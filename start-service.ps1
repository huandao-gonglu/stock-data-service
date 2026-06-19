$ErrorActionPreference = "Stop"

$ProjectRoot = $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

$HostAddress = "0.0.0.0"
$Port = 8000

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Python executable not found at $Python. Create the virtual environment first."
}

$listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
$processIds = $listeners | Select-Object -ExpandProperty OwningProcess -Unique

foreach ($processId in $processIds) {
    if ($processId -and $processId -ne $PID) {
        Write-Host "Stopping existing service on ${HostAddress}:$Port (PID: $processId)..."
        Stop-Process -Id $processId -Force
    }
}

Write-Host "Starting stock data service on http://${HostAddress}:$Port ..."
Push-Location -LiteralPath $ProjectRoot
try {
    & $Python -m uvicorn stock_data_service.main:app --host $HostAddress --port $Port
}
finally {
    Pop-Location
}
