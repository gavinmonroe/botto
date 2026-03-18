# Check Botto + Caddy status
Write-Host ""
Write-Host "=== Botto Status ===" -ForegroundColor Cyan

# Botto process
Write-Host ""
Write-Host "Botto:" -ForegroundColor Yellow
$bottoPidFile = "C:\botto\.botto-pid"
if (Test-Path $bottoPidFile) {
    $pid = (Get-Content $bottoPidFile).Trim()
    $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "  Running (PID: $pid)" -ForegroundColor Green
    } else {
        Write-Host "  PID file exists but process is dead (stale)" -ForegroundColor Red
    }
} else {
    $botto = Get-Process -Name "botto" -ErrorAction SilentlyContinue
    if ($botto) {
        Write-Host "  Running (PID: $($botto.Id)) — no PID file" -ForegroundColor Yellow
    } else {
        Write-Host "  Not running" -ForegroundColor Red
    }
}

# Caddy process
Write-Host ""
Write-Host "Caddy:" -ForegroundColor Yellow
$caddy = Get-Process -Name "caddy" -ErrorAction SilentlyContinue
if ($caddy) {
    Write-Host "  Running (PID: $($caddy.Id))" -ForegroundColor Green
} else {
    Write-Host "  Not running" -ForegroundColor Red
}

# Health check
Write-Host ""
Write-Host "Health:" -ForegroundColor Yellow
try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:7700/health" -UseBasicParsing -TimeoutSec 3
    Write-Host "  Botto direct:  OK ($($r.StatusCode))" -ForegroundColor Green
} catch {
    Write-Host "  Botto direct:  UNREACHABLE" -ForegroundColor Red
}

# Firewall rules
Write-Host ""
Write-Host "Firewall Rules:" -ForegroundColor Yellow
Get-NetFirewallRule -DisplayName "Botto*" | Format-Table DisplayName, Enabled, Action, Direction -AutoSize
Write-Host ""
