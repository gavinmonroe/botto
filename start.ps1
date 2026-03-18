# Start Botto + Caddy
$ErrorActionPreference = "Stop"
Write-Host "Starting Botto..." -ForegroundColor Green

# Detect current local IP
$localIP = "localhost"
try {
    $route = Get-NetRoute -DestinationPrefix "0.0.0.0/0" -ErrorAction SilentlyContinue |
        Sort-Object -Property RouteMetric | Select-Object -First 1
    if ($route) {
        $localIP = (Get-NetIPAddress -InterfaceIndex $route.InterfaceIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue |
            Where-Object { $_.PrefixOrigin -ne "WellKnown" } | Select-Object -First 1).IPAddress
    }
} catch {}

Set-Location "C:\botto"

# Start Botto as a background process
$bottoExe = "C:\botto\target\release\botto.exe"
if (-not (Test-Path $bottoExe)) {
    Write-Host "ERROR: botto.exe not found at $bottoExe" -ForegroundColor Red
    Write-Host "Build it first: cargo build --release" -ForegroundColor Yellow
    exit 1
}

$bottoLog = "C:\botto\data\botto.log"
$bottoProcess = Start-Process -FilePath $bottoExe -WorkingDirectory "C:\botto" -RedirectStandardOutput $bottoLog -RedirectStandardError "C:\botto\data\botto-error.log" -PassThru -WindowStyle Hidden
$bottoProcess.Id | Set-Content "C:\botto\.botto-pid"
Write-Host "Botto started (PID: $($bottoProcess.Id))" -ForegroundColor Green

# Wait for Botto to be ready
Write-Host "Waiting for Botto to be ready..." -ForegroundColor Yellow
$retries = 0
while ($retries -lt 30) {
    try {
        $response = Invoke-WebRequest -Uri "http://127.0.0.1:7700/health" -UseBasicParsing -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            Write-Host "Botto is ready." -ForegroundColor Green
            break
        }
    } catch {
        Start-Sleep -Seconds 2
        $retries++
    }
}
if ($retries -ge 30) {
    Write-Host "WARNING: Botto did not become ready within 60 seconds." -ForegroundColor Red
    Write-Host "Check logs: C:\botto\data\botto.log" -ForegroundColor Yellow
    Write-Host "Check errors: C:\botto\data\botto-error.log" -ForegroundColor Yellow
}

# Start Caddy
Write-Host "Starting Caddy reverse proxy..." -ForegroundColor Green
$caddyProcess = Start-Process -FilePath "caddy" -ArgumentList "run --config `"C:\botto\Caddyfile`"" -PassThru -WindowStyle Hidden
$caddyProcess.Id | Set-Content "C:\botto\.caddy-pid"
Write-Host "Caddy started (PID: $($caddyProcess.Id))" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Botto is running securely" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Local:  https://localhost:8443" -ForegroundColor White
Write-Host "  Team:   https://${localIP}:8443" -ForegroundColor White
Write-Host "  Admin:  https://${localIP}:8443/admin?key=<api-key>" -ForegroundColor White
Write-Host "  Otto:   wss://${localIP}:8443/ws" -ForegroundColor White
Write-Host ""
Write-Host "  Logs:   C:\botto\data\botto.log" -ForegroundColor DarkGray
Write-Host "  API Key is in: C:\botto\.api-key" -ForegroundColor DarkGray
Write-Host ""
