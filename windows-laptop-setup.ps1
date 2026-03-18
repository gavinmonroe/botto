<#
.SYNOPSIS
    Botto secure setup for Windows. Run as Administrator.
.DESCRIPTION
    Installs Caddy, generates API key, configures firewall,
    and creates all config files needed to run Botto securely.
    Botto runs as a native binary (cargo build --release).
    Docker is optional (only needed for sandbox auto-fix).
#>

# ─── Self-elevate if not admin (must run BEFORE StrictMode) ──────────────────
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal(
    [Security.Principal.WindowsIdentity]::GetCurrent()
)
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Re-launching as Administrator..." -ForegroundColor Yellow
    Start-Process powershell.exe -Verb RunAs -ArgumentList (
        "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    )
    exit
}

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ─── Helper: Write file as UTF-8 without BOM (PS 5.1 Set-Content adds BOM) ──
function Write-Utf8NoBom {
    param([string]$Path, [string]$Content)
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

# ─── Config ──────────────────────────────────────────────────────────────────
$BOTTO_DIR      = "C:\botto"
$DATA_DIR       = "$BOTTO_DIR\data"
$CADDY_PORT     = 8443
$BOTTO_PORT     = 7700

# ─── Banner ──────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Botto Secure Setup for Windows            " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# ─── Step 1: Create directory structure ──────────────────────────────────────
Write-Host "[1/8] Creating directory structure..." -ForegroundColor Green

foreach ($dir in @($BOTTO_DIR, $DATA_DIR)) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "  Created $dir"
    } else {
        Write-Host "  Already exists: $dir"
    }
}

# ─── Step 2: Generate API key ───────────────────────────────────────────────
Write-Host ""
Write-Host "[2/8] Generating API key..." -ForegroundColor Green

$API_KEY_FILE = "$BOTTO_DIR\.api-key"
if (Test-Path $API_KEY_FILE) {
    $API_KEY = Get-Content $API_KEY_FILE -Raw
    $API_KEY = $API_KEY.Trim()
    Write-Host "  Using existing API key from $API_KEY_FILE"
} else {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    $API_KEY = ($bytes | ForEach-Object { $_.ToString("x2") }) -join ""
    $rng.Dispose()
    Set-Content -Path $API_KEY_FILE -Value $API_KEY -NoNewline -Encoding ASCII
    # Lock down the key file
    $acl = Get-Acl $API_KEY_FILE
    $acl.SetAccessRuleProtection($true, $false)
    $adminRule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        "BUILTIN\Administrators", "FullControl", "Allow"
    )
    $acl.AddAccessRule($adminRule)
    Set-Acl -Path $API_KEY_FILE -AclObject $acl
    Write-Host "  Generated and saved to $API_KEY_FILE (admin-only permissions)"
}

$maskedKey = $API_KEY.Substring(0, 8) + "..." + $API_KEY.Substring($API_KEY.Length - 4)
Write-Host "  API Key: $maskedKey" -ForegroundColor Yellow

# ─── Step 3: Collect credentials ─────────────────────────────────────────────
Write-Host ""
Write-Host "[3/8] Collecting credentials..." -ForegroundColor Green
Write-Host "  (Press Enter to skip optional fields)" -ForegroundColor DarkGray
Write-Host ""

$GITLAB_URL   = Read-Host "  GitLab URL (e.g. https://gitlab.com)"
$GITLAB_TOKEN = Read-Host "  GitLab Bot Token (glpat-...)"
$AI_URL       = Read-Host "  AI Endpoint URL (e.g. https://openrouter.ai/api/v1)"
$AI_KEY       = Read-Host "  AI API Key (sk-...)"
$WEBHOOK_SECRET = Read-Host "  GitLab Webhook Secret (optional)"

# Validate required fields
$missing = @()
if ([string]::IsNullOrWhiteSpace($GITLAB_URL))   { $missing += "GitLab URL" }
if ([string]::IsNullOrWhiteSpace($GITLAB_TOKEN))  { $missing += "GitLab Token" }
if ([string]::IsNullOrWhiteSpace($AI_URL))        { $missing += "AI URL" }
if ([string]::IsNullOrWhiteSpace($AI_KEY))        { $missing += "AI Key" }

if ($missing.Count -gt 0) {
    Write-Host ""
    Write-Host "  Missing required fields: $($missing -join ', ')" -ForegroundColor Red
    Write-Host "  You can edit $DATA_DIR\botto.toml later and re-run." -ForegroundColor Yellow
    Write-Host "  Continuing with placeholders..." -ForegroundColor Yellow
    if ([string]::IsNullOrWhiteSpace($GITLAB_URL))  { $GITLAB_URL  = "https://gitlab.com" }
    if ([string]::IsNullOrWhiteSpace($GITLAB_TOKEN)) { $GITLAB_TOKEN = "CHANGE_ME" }
    if ([string]::IsNullOrWhiteSpace($AI_URL))       { $AI_URL      = "https://openrouter.ai/api/v1" }
    if ([string]::IsNullOrWhiteSpace($AI_KEY))        { $AI_KEY      = "CHANGE_ME" }
}

# ─── Step 4: Network configuration ──────────────────────────────────────────
Write-Host ""
Write-Host "[4/8] Network configuration..." -ForegroundColor Green

# Detect local IP — use the default gateway's interface to avoid picking Hyper-V/VMware adapters
$localIP = $null
try {
    $defaultRoute = Get-NetRoute -DestinationPrefix "0.0.0.0/0" -ErrorAction SilentlyContinue |
        Sort-Object -Property RouteMetric |
        Select-Object -First 1
    if ($defaultRoute) {
        $localIP = (Get-NetIPAddress -InterfaceIndex $defaultRoute.InterfaceIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue |
            Where-Object { $_.PrefixOrigin -ne "WellKnown" } |
            Select-Object -First 1
        ).IPAddress
    }
} catch {}

# Fallback if route-based detection failed
if (-not $localIP) {
    $localIP = (
        Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object {
            $_.InterfaceAlias -notmatch "Loopback|vEthernet|VMware|VirtualBox|Hyper-V" -and
            ($_.PrefixOrigin -eq "Dhcp" -or $_.PrefixOrigin -eq "Manual")
        } |
        Select-Object -First 1
    ).IPAddress
}

if (-not $localIP) {
    $localIP = "UNKNOWN"
    Write-Host "  Could not detect local IP. You'll need to find it manually (ipconfig)." -ForegroundColor Red
} else {
    Write-Host "  Detected local IP: $localIP" -ForegroundColor Yellow
}

$defaultSubnet = if ($localIP -and $localIP -ne "UNKNOWN") {
    $parts = $localIP.Split(".")
    "$($parts[0]).$($parts[1]).$($parts[2]).0/24"
} else {
    "192.168.1.0/24"
}

Write-Host ""
$subnetInput = Read-Host "  Allowed subnet for team access (default: $defaultSubnet)"
if ([string]::IsNullOrWhiteSpace($subnetInput)) {
    $TEAM_SUBNET = $defaultSubnet
} else {
    $TEAM_SUBNET = $subnetInput
}

Write-Host ""
$installTailscale = Read-Host "  Install Tailscale for zero-trust networking? (y/N)"

# ─── Step 5: Write config files ─────────────────────────────────────────────
Write-Host ""
Write-Host "[5/8] Writing configuration files..." -ForegroundColor Green

# botto.toml
$bottoToml = @"
# Botto configuration — generated by setup script
# $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")

[auth]
api_key = "$API_KEY"

[gitlab]
url = "$GITLAB_URL"
bot_token = "$GITLAB_TOKEN"
$(if (-not [string]::IsNullOrWhiteSpace($WEBHOOK_SECRET)) { "webhook_secret = `"$WEBHOOK_SECRET`"" } else { "# webhook_secret = `"`"" })

[ai]
base_url = "$AI_URL"
api_key = "$AI_KEY"

[review]
auto_review_on_push = false

[sandbox]
enabled = true
max_concurrent = 2
timeout_seconds = 300

[cache]
review_ttl_days = 7
max_cached_reviews = 500
"@

Write-Utf8NoBom -Path "$DATA_DIR\botto.toml" -Content $bottoToml
Write-Host "  Wrote $DATA_DIR\botto.toml"

# docker-compose.yml (optional — for sandbox auto-fix if Docker is available)
$dockerCompose = @"
services:
  botto:
    image: botto:latest
    container_name: botto
    restart: unless-stopped
    ports:
      - "127.0.0.1:${BOTTO_PORT}:${BOTTO_PORT}"
    volumes:
      - ./data:/app/data
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - RUST_LOG=botto=info
    working_dir: /app
"@

Write-Utf8NoBom -Path "$BOTTO_DIR\docker-compose.yml" -Content $dockerCompose
Write-Host "  Wrote $BOTTO_DIR\docker-compose.yml (optional, for Docker-based sandbox)"

# Caddyfile
$caddyfile = @"
:${CADDY_PORT} {
    tls internal

    # ── WebSocket: Botto handles its own first-message auth ──
    @websocket {
        path /ws
    }
    handle @websocket {
        reverse_proxy localhost:${BOTTO_PORT}
    }

    # ── GitLab webhooks: authenticated by webhook_secret, not API key ──
    @webhooks {
        path /api/webhooks/*
    }
    handle @webhooks {
        reverse_proxy localhost:${BOTTO_PORT}
    }

    # ── Health checks from localhost only ──
    @local_health {
        remote_ip 127.0.0.1 ::1
        path /health /ready
    }
    handle @local_health {
        reverse_proxy localhost:${BOTTO_PORT}
    }

    # ── Reject anything else without a valid API key ──
    @unauthorized {
        not header X-API-Key "${API_KEY}"
        not header Authorization "Bearer ${API_KEY}"
        not query key=${API_KEY}
    }
    handle @unauthorized {
        respond "Unauthorized" 401
    }

    # ── Forward authorized traffic to Botto (fallback) ──
    handle {
        reverse_proxy localhost:${BOTTO_PORT}
    }
}
"@

Write-Utf8NoBom -Path "$BOTTO_DIR\Caddyfile" -Content $caddyfile
Write-Host "  Wrote $BOTTO_DIR\Caddyfile"

# ─── Step 6: Install software ───────────────────────────────────────────────
Write-Host ""
Write-Host "[6/8] Installing software..." -ForegroundColor Green

# Check winget
$wingetExists = Get-Command winget -ErrorAction SilentlyContinue
if (-not $wingetExists) {
    Write-Host "  ERROR: winget not found. Install App Installer from the Microsoft Store." -ForegroundColor Red
    Write-Host "  https://aka.ms/getwinget" -ForegroundColor Yellow
    Read-Host "  Press Enter to exit"
    exit 1
}

# Docker Desktop (optional — only needed for sandbox auto-fix)
$dockerExists = Get-Command docker -ErrorAction SilentlyContinue
if (-not $dockerExists) {
    Write-Host "  Docker not found. Sandbox auto-fix will be unavailable." -ForegroundColor DarkGray
    Write-Host "  Install later with: winget install Docker.DockerDesktop" -ForegroundColor DarkGray
} else {
    Write-Host "  Docker available (sandbox auto-fix enabled): $(docker --version)"
}

# Caddy
$caddyExists = Get-Command caddy -ErrorAction SilentlyContinue
if (-not $caddyExists) {
    Write-Host "  Installing Caddy..." -ForegroundColor Yellow
    winget install CaddyServer.Caddy --accept-source-agreements --accept-package-agreements --silent
    # Refresh PATH for this session
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("Path", "User")
    Write-Host "  Caddy installed."
} else {
    Write-Host "  Caddy already installed: $(caddy version)"
}

# Tailscale (optional)
if ($installTailscale -eq "y" -or $installTailscale -eq "Y") {
    $tailscaleExists = Get-Command tailscale -ErrorAction SilentlyContinue
    if (-not $tailscaleExists) {
        Write-Host "  Installing Tailscale..." -ForegroundColor Yellow
        winget install Tailscale.Tailscale --accept-source-agreements --accept-package-agreements --silent
        Write-Host "  Tailscale installed. Run 'tailscale up' after setup to join your tailnet."
    } else {
        Write-Host "  Tailscale already installed."
    }
}

# ─── Step 7: Firewall rules ─────────────────────────────────────────────────
Write-Host ""
Write-Host "[7/8] Configuring Windows Firewall..." -ForegroundColor Green

# Remove old rules if they exist (idempotent)
$ruleNames = @("Botto - Block Direct Access", "Botto - Allow Caddy (Team)")
if ($installTailscale -eq "y" -or $installTailscale -eq "Y") {
    $ruleNames += "Botto - Allow Caddy (Tailscale)"
}

foreach ($name in $ruleNames) {
    $existing = Get-NetFirewallRule -DisplayName $name -ErrorAction SilentlyContinue
    if ($existing) {
        Remove-NetFirewallRule -DisplayName $name
        Write-Host "  Removed existing rule: $name"
    }
}

# Rule 1: Block direct access to Botto port from network
New-NetFirewallRule `
    -DisplayName "Botto - Block Direct Access" `
    -Description "Prevents direct network access to Botto. All traffic must go through Caddy." `
    -Direction Inbound `
    -LocalPort $BOTTO_PORT `
    -Protocol TCP `
    -Action Block `
    -Profile Any `
    -Enabled True | Out-Null
Write-Host "  Blocked inbound port $BOTTO_PORT (direct Botto access)"

# Rule 2: Allow Caddy port from team subnet only
New-NetFirewallRule `
    -DisplayName "Botto - Allow Caddy (Team)" `
    -Description "Allows team subnet to reach Caddy reverse proxy." `
    -Direction Inbound `
    -LocalPort $CADDY_PORT `
    -Protocol TCP `
    -Action Allow `
    -Profile Any `
    -RemoteAddress $TEAM_SUBNET `
    -Enabled True | Out-Null
Write-Host "  Allowed inbound port $CADDY_PORT from $TEAM_SUBNET"

# Rule 3: Tailscale subnet (if opted in)
if ($installTailscale -eq "y" -or $installTailscale -eq "Y") {
    New-NetFirewallRule `
        -DisplayName "Botto - Allow Caddy (Tailscale)" `
        -Description "Allows Tailscale network to reach Caddy reverse proxy." `
        -Direction Inbound `
        -LocalPort $CADDY_PORT `
        -Protocol TCP `
        -Action Allow `
        -Profile Any `
        -RemoteAddress "100.64.0.0/10" `
        -Enabled True | Out-Null
    Write-Host "  Allowed inbound port $CADDY_PORT from Tailscale (100.64.0.0/10)"
}

# ─── Step 8: Create start/stop scripts ───────────────────────────────────────
Write-Host ""
Write-Host "[8/8] Creating management scripts..." -ForegroundColor Green

# start.ps1
$startScript = @"
# Start Botto + Caddy
`$ErrorActionPreference = "Stop"
Write-Host "Starting Botto..." -ForegroundColor Green

# Detect current local IP
`$localIP = "localhost"
try {
    `$route = Get-NetRoute -DestinationPrefix "0.0.0.0/0" -ErrorAction SilentlyContinue |
        Sort-Object -Property RouteMetric | Select-Object -First 1
    if (`$route) {
        `$localIP = (Get-NetIPAddress -InterfaceIndex `$route.InterfaceIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue |
            Where-Object { `$_.PrefixOrigin -ne "WellKnown" } | Select-Object -First 1).IPAddress
    }
} catch {}

Set-Location "$BOTTO_DIR"

# Start Botto as a background process
`$bottoExe = "$BOTTO_DIR\target\release\botto.exe"
if (-not (Test-Path `$bottoExe)) {
    Write-Host "ERROR: botto.exe not found at `$bottoExe" -ForegroundColor Red
    Write-Host "Build it first: cargo build --release" -ForegroundColor Yellow
    exit 1
}

`$bottoLog = "$BOTTO_DIR\data\botto.log"
`$bottoProcess = Start-Process -FilePath `$bottoExe -RedirectStandardOutput `$bottoLog -RedirectStandardError "$BOTTO_DIR\data\botto-error.log" -PassThru -WindowStyle Hidden
`$bottoProcess.Id | Set-Content "$BOTTO_DIR\.botto-pid"
Write-Host "Botto started (PID: `$(`$bottoProcess.Id))" -ForegroundColor Green

# Wait for Botto to be ready
Write-Host "Waiting for Botto to be ready..." -ForegroundColor Yellow
`$retries = 0
while (`$retries -lt 30) {
    try {
        `$response = Invoke-WebRequest -Uri "http://localhost:${BOTTO_PORT}/health" -UseBasicParsing -TimeoutSec 2
        if (`$response.StatusCode -eq 200) {
            Write-Host "Botto is ready." -ForegroundColor Green
            break
        }
    } catch {
        Start-Sleep -Seconds 2
        `$retries++
    }
}
if (`$retries -ge 30) {
    Write-Host "WARNING: Botto did not become ready within 60 seconds." -ForegroundColor Red
    Write-Host "Check logs: $BOTTO_DIR\data\botto.log" -ForegroundColor Yellow
    Write-Host "Check errors: $BOTTO_DIR\data\botto-error.log" -ForegroundColor Yellow
}

# Start Caddy
Write-Host "Starting Caddy reverse proxy..." -ForegroundColor Green
`$caddyProcess = Start-Process -FilePath "caddy" -ArgumentList "run --config `"$BOTTO_DIR\Caddyfile`"" -PassThru -WindowStyle Hidden
`$caddyProcess.Id | Set-Content "$BOTTO_DIR\.caddy-pid"
Write-Host "Caddy started (PID: `$(`$caddyProcess.Id))" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Botto is running securely" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Local:  https://localhost:${CADDY_PORT}" -ForegroundColor White
Write-Host "  Team:   https://`${localIP}:${CADDY_PORT}" -ForegroundColor White
Write-Host "  Admin:  https://`${localIP}:${CADDY_PORT}/admin?key=<api-key>" -ForegroundColor White
Write-Host "  Otto:   wss://`${localIP}:${CADDY_PORT}/ws" -ForegroundColor White
Write-Host ""
Write-Host "  Logs:   $BOTTO_DIR\data\botto.log" -ForegroundColor DarkGray
Write-Host "  API Key is in: $API_KEY_FILE" -ForegroundColor DarkGray
Write-Host ""
"@

Write-Utf8NoBom -Path "$BOTTO_DIR\start.ps1" -Content $startScript
Write-Host "  Wrote $BOTTO_DIR\start.ps1"

# stop.ps1
$stopScript = @"
# Stop Botto + Caddy
Write-Host "Stopping Caddy..." -ForegroundColor Yellow
`$pidFile = "$BOTTO_DIR\.caddy-pid"
if (Test-Path `$pidFile) {
    `$pid = Get-Content `$pidFile
    try {
        Stop-Process -Id `$pid -Force -ErrorAction SilentlyContinue
        Write-Host "  Caddy stopped (PID: `$pid)"
    } catch {
        Write-Host "  Caddy process not found (already stopped?)"
    }
    Remove-Item `$pidFile -Force
} else {
    Get-Process -Name "caddy" -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "  Caddy stopped."
}

Write-Host "Stopping Botto..." -ForegroundColor Yellow
`$bottoPidFile = "$BOTTO_DIR\.botto-pid"
if (Test-Path `$bottoPidFile) {
    `$pid = Get-Content `$bottoPidFile
    try {
        Stop-Process -Id `$pid -Force -ErrorAction SilentlyContinue
        Write-Host "  Botto stopped (PID: `$pid)"
    } catch {
        Write-Host "  Botto process not found (already stopped?)"
    }
    Remove-Item `$bottoPidFile -Force
} else {
    Get-Process -Name "botto" -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "  Botto stopped."
}

Write-Host "All services stopped." -ForegroundColor Green
"@

Write-Utf8NoBom -Path "$BOTTO_DIR\stop.ps1" -Content $stopScript
Write-Host "  Wrote $BOTTO_DIR\stop.ps1"

# status.ps1
$statusScript = @"
# Check Botto + Caddy status
Write-Host ""
Write-Host "=== Botto Status ===" -ForegroundColor Cyan

# Botto process
Write-Host ""
Write-Host "Botto:" -ForegroundColor Yellow
`$bottoPidFile = "$BOTTO_DIR\.botto-pid"
if (Test-Path `$bottoPidFile) {
    `$pid = (Get-Content `$bottoPidFile).Trim()
    `$proc = Get-Process -Id `$pid -ErrorAction SilentlyContinue
    if (`$proc) {
        Write-Host "  Running (PID: `$pid)" -ForegroundColor Green
    } else {
        Write-Host "  PID file exists but process is dead (stale)" -ForegroundColor Red
    }
} else {
    `$botto = Get-Process -Name "botto" -ErrorAction SilentlyContinue
    if (`$botto) {
        Write-Host "  Running (PID: `$(`$botto.Id)) — no PID file" -ForegroundColor Yellow
    } else {
        Write-Host "  Not running" -ForegroundColor Red
    }
}

# Caddy process
Write-Host ""
Write-Host "Caddy:" -ForegroundColor Yellow
`$caddy = Get-Process -Name "caddy" -ErrorAction SilentlyContinue
if (`$caddy) {
    Write-Host "  Running (PID: `$(`$caddy.Id))" -ForegroundColor Green
} else {
    Write-Host "  Not running" -ForegroundColor Red
}

# Health check
Write-Host ""
Write-Host "Health:" -ForegroundColor Yellow
try {
    `$r = Invoke-WebRequest -Uri "http://localhost:${BOTTO_PORT}/health" -UseBasicParsing -TimeoutSec 3
    Write-Host "  Botto direct:  OK (`$(`$r.StatusCode))" -ForegroundColor Green
} catch {
    Write-Host "  Botto direct:  UNREACHABLE" -ForegroundColor Red
}

# Firewall rules
Write-Host ""
Write-Host "Firewall Rules:" -ForegroundColor Yellow
Get-NetFirewallRule -DisplayName "Botto*" | Format-Table DisplayName, Enabled, Action, Direction -AutoSize
Write-Host ""
"@

Write-Utf8NoBom -Path "$BOTTO_DIR\status.ps1" -Content $statusScript
Write-Host "  Wrote $BOTTO_DIR\status.ps1"

# ─── Done ────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Setup complete!                           " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Files created in: $BOTTO_DIR" -ForegroundColor White
Write-Host ""
Write-Host "    botto.toml          Config (edit credentials if needed)" -ForegroundColor DarkGray
Write-Host "    Caddyfile           Reverse proxy config" -ForegroundColor DarkGray
Write-Host "    docker-compose.yml  Docker config (optional, for sandbox)" -ForegroundColor DarkGray
Write-Host "    .api-key            Your team API key" -ForegroundColor DarkGray
Write-Host "    start.ps1           Start Botto + Caddy" -ForegroundColor DarkGray
Write-Host "    stop.ps1            Stop everything" -ForegroundColor DarkGray
Write-Host "    status.ps1          Check status" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Your API key (share with team):" -ForegroundColor Yellow
Write-Host "  $API_KEY" -ForegroundColor White
Write-Host ""
Write-Host "  IMPORTANT: Save this key now. It's also stored in $API_KEY_FILE" -ForegroundColor Red
Write-Host ""

Write-Host ""
Write-Host "  Next steps:" -ForegroundColor Yellow

Write-Host ""
Write-Host "    1. Build Botto from source:" -ForegroundColor White
Write-Host "       cd $BOTTO_DIR && cargo build --release" -ForegroundColor Cyan
Write-Host ""
Write-Host "    2. Start everything:" -ForegroundColor White
Write-Host "       powershell -ExecutionPolicy Bypass -File $BOTTO_DIR\start.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "    3. Tell your team to connect Otto to:" -ForegroundColor White
Write-Host "       Server:  wss://${localIP}:${CADDY_PORT}/ws" -ForegroundColor Cyan
Write-Host "       API Key: (the key shown above)" -ForegroundColor Cyan
Write-Host ""
Write-Host "    For remote teams, run cloudflare-tunnel-setup.ps1 next." -ForegroundColor Yellow
Write-Host ""

if ($installTailscale -eq "y" -or $installTailscale -eq "Y") {
    Write-Host "    4. Run 'tailscale up' and share your Tailscale IP with the team" -ForegroundColor White
    Write-Host "       Team connects to: wss://<tailscale-ip>:${CADDY_PORT}/ws" -ForegroundColor Cyan
    Write-Host ""
}

Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Read-Host "Press Enter to exit"
