<#
.SYNOPSIS
    Cloudflare Tunnel setup for Botto. Exposes your local Botto instance to remote team members.
.DESCRIPTION
    Installs cloudflared and configures a tunnel pointing to your local Caddy reverse proxy.
    Supports quick tunnels (random URL, no account) and named tunnels (stable subdomain).
    Run after windows-laptop-setup.ps1 has already been executed.
#>

# ─── Self-elevate if not admin ───────────────────────────────────────────────
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

# ─── Helper ──────────────────────────────────────────────────────────────────
function Write-Utf8NoBom {
    param([string]$Path, [string]$Content)
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

# ─── Config ──────────────────────────────────────────────────────────────────
$BOTTO_DIR  = "C:\botto"
$CADDY_PORT = 8443
$TUNNEL_DIR = "$BOTTO_DIR\tunnel"

# ─── Verify prerequisites ───────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Cloudflare Tunnel Setup for Botto         " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Test-Path $BOTTO_DIR)) {
    Write-Host "ERROR: $BOTTO_DIR not found. Run windows-laptop-setup.ps1 first." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

$API_KEY_FILE = "$BOTTO_DIR\.api-key"
if (Test-Path $API_KEY_FILE) {
    $API_KEY = (Get-Content $API_KEY_FILE -Raw).Trim()
    $maskedKey = $API_KEY.Substring(0, 8) + "..." + $API_KEY.Substring($API_KEY.Length - 4)
    Write-Host "  Found API key: $maskedKey" -ForegroundColor Green
} else {
    Write-Host "  WARNING: No .api-key file found. Tunnel will still work but" -ForegroundColor Yellow
    Write-Host "  make sure Caddy and Botto are configured with a key." -ForegroundColor Yellow
}

# ─── Create tunnel directory ─────────────────────────────────────────────────
if (-not (Test-Path $TUNNEL_DIR)) {
    New-Item -ItemType Directory -Path $TUNNEL_DIR -Force | Out-Null
}

# ─── Install cloudflared ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "[1/3] Installing cloudflared..." -ForegroundColor Green

$cloudflaredExists = Get-Command cloudflared -ErrorAction SilentlyContinue
if (-not $cloudflaredExists) {
    $wingetExists = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $wingetExists) {
        Write-Host "  ERROR: winget not found. Install App Installer from the Microsoft Store." -ForegroundColor Red
        Read-Host "  Press Enter to exit"
        exit 1
    }
    winget install Cloudflare.cloudflared --accept-source-agreements --accept-package-agreements --silent
    # Refresh PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("Path", "User")
    Write-Host "  cloudflared installed." -ForegroundColor Green
} else {
    Write-Host "  cloudflared already installed: $(cloudflared --version)" -ForegroundColor Green
}

# ─── Choose tunnel mode ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "[2/3] Tunnel configuration..." -ForegroundColor Green
Write-Host ""
Write-Host "  Two options:" -ForegroundColor White
Write-Host ""
Write-Host "  [1] Quick tunnel  — random URL, no Cloudflare account needed" -ForegroundColor Cyan
Write-Host "                      URL changes every time you restart the tunnel" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  [2] Named tunnel — stable subdomain (e.g. botto.yourteam.com)" -ForegroundColor Cyan
Write-Host "                      requires a Cloudflare account + domain" -ForegroundColor DarkGray
Write-Host ""

$mode = Read-Host "  Choose mode (1 or 2, default: 1)"
if ([string]::IsNullOrWhiteSpace($mode)) { $mode = "1" }

# ─── Quick tunnel ────────────────────────────────────────────────────────────
if ($mode -eq "1") {
    Write-Host ""
    Write-Host "[3/3] Creating quick tunnel scripts..." -ForegroundColor Green

    $startTunnel = @"
# Start Cloudflare Quick Tunnel for Botto
`$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "Starting Cloudflare Quick Tunnel..." -ForegroundColor Green
Write-Host "  Pointing to https://localhost:${CADDY_PORT}" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  The tunnel URL will appear below (look for the .trycloudflare.com line)." -ForegroundColor Yellow
Write-Host "  Share that URL + the API key with your team." -ForegroundColor Yellow
Write-Host ""
Write-Host "  Team configures Otto with:" -ForegroundColor Cyan
Write-Host "    Server:  wss://<tunnel-url>/ws" -ForegroundColor White
Write-Host "    API Key: (from C:\botto\.api-key)" -ForegroundColor White
Write-Host ""
Write-Host "  Press Ctrl+C to stop the tunnel." -ForegroundColor DarkGray
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

cloudflared tunnel --url https://localhost:${CADDY_PORT} --no-tls-verify
"@

    Write-Utf8NoBom -Path "$TUNNEL_DIR\start-tunnel.ps1" -Content $startTunnel
    Write-Host "  Wrote $TUNNEL_DIR\start-tunnel.ps1"

    # Also create a .bat launcher for double-click convenience
    $batLauncher = @"
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-tunnel.ps1"
pause
"@

    Write-Utf8NoBom -Path "$TUNNEL_DIR\start-tunnel.bat" -Content $batLauncher
    Write-Host "  Wrote $TUNNEL_DIR\start-tunnel.bat"

# ─── Named tunnel ───────────────────────────────────────────────────────────
} elseif ($mode -eq "2") {
    Write-Host ""
    Write-Host "  Named tunnel setup requires authenticating with Cloudflare." -ForegroundColor Yellow
    Write-Host "  A browser window will open for you to log in." -ForegroundColor Yellow
    Write-Host ""

    $tunnelName = Read-Host "  Tunnel name (e.g. botto)"
    if ([string]::IsNullOrWhiteSpace($tunnelName)) { $tunnelName = "botto" }

    $hostname = Read-Host "  Hostname (e.g. botto.yourteam.com)"
    if ([string]::IsNullOrWhiteSpace($hostname)) {
        Write-Host "  ERROR: Hostname is required for named tunnels." -ForegroundColor Red
        Read-Host "  Press Enter to exit"
        exit 1
    }

    Write-Host ""
    Write-Host "  Logging in to Cloudflare..." -ForegroundColor Yellow
    cloudflared tunnel login

    Write-Host ""
    Write-Host "  Creating tunnel '$tunnelName'..." -ForegroundColor Yellow

    # Create the tunnel — capture output to extract tunnel ID
    $createOutput = cloudflared tunnel create $tunnelName 2>&1 | Out-String
    Write-Host $createOutput

    # Extract tunnel ID from output
    $tunnelId = $null
    if ($createOutput -match "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})") {
        $tunnelId = $Matches[1]
        Write-Host "  Tunnel ID: $tunnelId" -ForegroundColor Green
    } else {
        # Tunnel may already exist — try to get its ID
        $listOutput = cloudflared tunnel list -o json 2>&1 | Out-String
        try {
            $tunnels = $listOutput | ConvertFrom-Json
            $existing = $tunnels | Where-Object { $_.name -eq $tunnelName } | Select-Object -First 1
            if ($existing) {
                $tunnelId = $existing.id
                Write-Host "  Using existing tunnel: $tunnelId" -ForegroundColor Yellow
            }
        } catch {}
    }

    if (-not $tunnelId) {
        Write-Host "  ERROR: Could not determine tunnel ID." -ForegroundColor Red
        Write-Host "  Run 'cloudflared tunnel list' to check." -ForegroundColor Yellow
        Read-Host "  Press Enter to exit"
        exit 1
    }

    Write-Host ""
    Write-Host "[3/3] Writing tunnel configuration..." -ForegroundColor Green

    # Write config.yml
    $configYml = @"
tunnel: ${tunnelId}
credentials-file: $env:USERPROFILE\.cloudflared\${tunnelId}.json

ingress:
  - hostname: ${hostname}
    service: https://localhost:${CADDY_PORT}
    originRequest:
      noTLSVerify: true
  - service: http_status:404
"@

    Write-Utf8NoBom -Path "$TUNNEL_DIR\config.yml" -Content $configYml
    Write-Host "  Wrote $TUNNEL_DIR\config.yml"

    # Create DNS record
    Write-Host ""
    Write-Host "  Creating DNS record for $hostname..." -ForegroundColor Yellow
    cloudflared tunnel route dns $tunnelName $hostname 2>&1 | Write-Host

    # Start script
    $startTunnel = @"
# Start Cloudflare Named Tunnel for Botto
`$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "Starting Cloudflare Tunnel '${tunnelName}'..." -ForegroundColor Green
Write-Host "  URL: https://${hostname}" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Team configures Otto with:" -ForegroundColor Yellow
Write-Host "    Server:  wss://${hostname}/ws" -ForegroundColor White
Write-Host "    API Key: (from C:\botto\.api-key)" -ForegroundColor White
Write-Host ""
Write-Host "  Press Ctrl+C to stop the tunnel." -ForegroundColor DarkGray
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

cloudflared tunnel --config "$TUNNEL_DIR\config.yml" run ${tunnelName}
"@

    Write-Utf8NoBom -Path "$TUNNEL_DIR\start-tunnel.ps1" -Content $startTunnel
    Write-Host "  Wrote $TUNNEL_DIR\start-tunnel.ps1"

    # .bat launcher
    $batLauncher = @"
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-tunnel.ps1"
pause
"@

    Write-Utf8NoBom -Path "$TUNNEL_DIR\start-tunnel.bat" -Content $batLauncher
    Write-Host "  Wrote $TUNNEL_DIR\start-tunnel.bat"

} else {
    Write-Host "  Invalid choice. Exiting." -ForegroundColor Red
    Read-Host "  Press Enter to exit"
    exit 1
}

# ─── Install as Windows service (optional) ───────────────────────────────────
Write-Host ""
$installService = Read-Host "  Install tunnel as a Windows service (starts on boot)? (y/N)"
if ($installService -eq "y" -or $installService -eq "Y") {
    if ($mode -eq "2") {
        Write-Host "  Installing as service..." -ForegroundColor Yellow
        cloudflared service install --config "$TUNNEL_DIR\config.yml" 2>&1 | Write-Host
        Write-Host "  Service installed. It will start automatically on boot." -ForegroundColor Green
        Write-Host "  Manage with: 'sc start cloudflared' / 'sc stop cloudflared'" -ForegroundColor DarkGray
    } else {
        Write-Host "  Service install only works with named tunnels (mode 2)." -ForegroundColor Yellow
        Write-Host "  Quick tunnels generate a new URL each time, so a persistent" -ForegroundColor Yellow
        Write-Host "  service wouldn't give your team a stable address." -ForegroundColor Yellow
    }
}

# ─── Done ────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Tunnel setup complete!                    " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Files in: $TUNNEL_DIR" -ForegroundColor White
Write-Host ""

if ($mode -eq "1") {
    Write-Host "  To start the tunnel:" -ForegroundColor Yellow
    Write-Host "    Double-click: $TUNNEL_DIR\start-tunnel.bat" -ForegroundColor Cyan
    Write-Host "    Or run:       powershell -File $TUNNEL_DIR\start-tunnel.ps1" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  The tunnel URL (*.trycloudflare.com) will print in the console." -ForegroundColor White
    Write-Host "  Share it with your team each time you restart." -ForegroundColor White
} else {
    Write-Host "  Your stable URL: https://$hostname" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  To start the tunnel:" -ForegroundColor Yellow
    Write-Host "    Double-click: $TUNNEL_DIR\start-tunnel.bat" -ForegroundColor Cyan
    Write-Host "    Or run:       powershell -File $TUNNEL_DIR\start-tunnel.ps1" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "  Team configures Otto extension with:" -ForegroundColor Yellow
if ($mode -eq "1") {
    Write-Host "    Server:  wss://<tunnel-url>/ws" -ForegroundColor White
} else {
    Write-Host "    Server:  wss://${hostname}/ws" -ForegroundColor White
}
Write-Host "    API Key: (from $API_KEY_FILE)" -ForegroundColor White
Write-Host ""
Write-Host "  IMPORTANT: Make sure Botto + Caddy are running BEFORE" -ForegroundColor Red
Write-Host "  starting the tunnel (run start.ps1 first)." -ForegroundColor Red
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Read-Host "Press Enter to exit"
