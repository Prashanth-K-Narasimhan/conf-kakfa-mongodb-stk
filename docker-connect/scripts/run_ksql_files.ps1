# scripts/run_ksql_files.ps1
# Usage: .\scripts\run_ksql_files.ps1
Param(
    [string]$KsqlUrl = "http://localhost:8088/ksql",
    [string]$KsQlDir = ".\ksql"
)

if (-not (Test-Path $KsQlDir)) {
    Write-Error "Directory $KsQlDir not found"
    exit 1
}

Get-ChildItem -Path $KsQlDir -Filter *.ksql | Sort-Object Name | ForEach-Object {
    $file = $_.FullName
    Write-Host "==== Running $file ===="
    $content = Get-Content $file -Raw
    $payload = @{
        ksql = $content
        streamsProperties = @{}
    } | ConvertTo-Json -Compress
    $resp = Invoke-RestMethod -Uri $KsqlUrl -Method Post -Body $payload -ContentType "application/json" -ErrorAction SilentlyContinue
    if ($?) {
        Write-Host "OK"
    } else {
        Write-Host "ERROR response from ksql server (see below)"
        $resp | ConvertTo-Json -Depth 5
        exit 1
    }
}

Write-Host "All .ksql files processed."
