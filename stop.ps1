# Stop Botto + Caddy
Write-Host "Stopping Caddy..." -ForegroundColor Yellow
$pidFile = "C:\botto\.caddy-pid"
if (Test-Path $pidFile) {
    $pid = Get-Content $pidFile
    try {
        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        Write-Host "  Caddy stopped (PID: $pid)"
    } catch {
        Write-Host "  Caddy process not found (already stopped?)"
    }
    Remove-Item $pidFile -Force
} else {
    Get-Process -Name "caddy" -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "  Caddy stopped."
}

Write-Host "Stopping Botto..." -ForegroundColor Yellow
$bottoPidFile = "C:\botto\.botto-pid"
if (Test-Path $bottoPidFile) {
    $pid = Get-Content $bottoPidFile
    try {
        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        Write-Host "  Botto stopped (PID: $pid)"
    } catch {
        Write-Host "  Botto process not found (already stopped?)"
    }
    Remove-Item $bottoPidFile -Force
} else {
    Get-Process -Name "botto" -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "  Botto stopped."
}

Write-Host "All services stopped." -ForegroundColor Green
