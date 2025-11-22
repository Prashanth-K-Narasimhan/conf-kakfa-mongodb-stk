<#
.SYNOPSIS
    Run KSQL pull or push queries from PowerShell. Allows interactive stop for streaming (push) queries.

.DESCRIPTION
    - Mode "pull" posts a pull query to /query and prints the JSON result.
    - Mode "push" posts a streaming push query to /query-stream and prints rows as they arrive.
    - For streaming, press Enter to stop the streaming job and return.

    The script references an uploaded file path (for tooling): /mnt/data/CreatePipelineFull.py

.EXAMPLE
    # Pull query (table/pull)
    .\ksql_query.ps1 -Mode pull -KsqlUrl "http://localhost:8088" -Query "SELECT * FROM VEHICLE_LATEST WHERE DID='1000';"

    # Push query (stream). This prints new rows as they arrive. Press Enter to stop.
    .\ksql_query.ps1 -Mode push -KsqlUrl "http://localhost:8088" -StreamSql "SELECT * FROM TELEMETRY_RAW EMIT CHANGES LIMIT 10;"
#>

param(
    [ValidateSet("pull","push")]
    [string]$Mode = "pull",

    [string]$KsqlUrl = "http://localhost:8088",

    # For pull mode: full SQL statement (SELECT .. FROM TABLE ..)
    [string]$Query = "SELECT * FROM VEHICLE_LATEST LIMIT 10;",

    # For push mode: SQL to stream (SELECT ... EMIT CHANGES ...)
    [string]$StreamSql = "SELECT * FROM TELEMETRY_RAW EMIT CHANGES LIMIT 5;",

    [int]$TimeoutSec = 15
)

# Local uploaded file path (for tooling integration)
$LocalUploadedFilePath = "/mnt/data/CreatePipelineFull.py"
Write-Host "Uploaded file (local path): $LocalUploadedFilePath" -ForegroundColor DarkCyan

function Invoke-KsqlPull {
    param($KsqlUrl, $ksql)
    $url = $KsqlUrl.TrimEnd('/') + "/query"
    $body = @{ ksql = $ksql; streamsProperties = @{} } | ConvertTo-Json
    Write-Host "POST $url"
    try {
        $resp = Invoke-RestMethod -Method Post -Uri $url -Body $body -ContentType "application/vnd.ksql.v1+json" -TimeoutSec $TimeoutSec
        Write-Host "Status: OK`n" -ForegroundColor Green
        # Pretty print JSON
        $resp | ConvertTo-Json -Depth 10 | Write-Host
    } catch {
        Write-Host "Pull query failed:" $_.Exception.Message -ForegroundColor Red
        if ($_.Exception.Response -ne $null) {
            try {
                $text = $_.Exception.Response.Content.ReadAsStringAsync().Result
                Write-Host "Response body: $text" -ForegroundColor Yellow
            } catch { }
        }
    }
}

function Start-KsqlStreamJob {
    param($KsqlUrl, $sql)

    $script = {
        param($KsqlUrlInner, $sqlInner)
        Add-Type -AssemblyName System.Net.Http
        $httpHandler = New-Object System.Net.Http.HttpClientHandler
        $httpHandler.AllowAutoRedirect = $true
        $client = New-Object System.Net.Http.HttpClient($httpHandler)
        $client.Timeout = [System.TimeSpan]::FromMilliseconds([System.Int64]::MaxValue)  # infinite timeout for streaming

        $req = New-Object System.Net.Http.HttpRequestMessage([System.Net.Http.HttpMethod]::Post, ($KsqlUrlInner.TrimEnd('/') + "/query-stream"))
        $payload = @{ sql = $sqlInner } | ConvertTo-Json
        $req.Content = New-Object System.Net.Http.StringContent($payload, [System.Text.Encoding]::UTF8, "application/json")

        try {
            $resp = $client.SendAsync($req, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).Result
        } catch {
            Write-Host "Stream request failed: $($_.Exception.Message)" -ForegroundColor Red
            return
        }

        if (-not $resp.IsSuccessStatusCode) {
            $body = $resp.Content.ReadAsStringAsync().Result
            Write-Host "Stream request returned status $($resp.StatusCode): $body" -ForegroundColor Yellow
            return
        }

        $stream = $resp.Content.ReadAsStreamAsync().Result
        $reader = New-Object System.IO.StreamReader($stream)

        Write-Host "Streaming started. Waiting for rows. (This job prints each JSON line received.)" -ForegroundColor Green
        try {
            while (-not $reader.EndOfStream) {
                $line = $reader.ReadLine()
                if ($line -and $line.Trim().Length -gt 0) {
                    # try to pretty print JSON if possible
                    try {
                        $obj = $line | ConvertFrom-Json
                        # If it's a KSQL "row" format, print the row content succinctly
                        if ($obj -is [System.Management.Automation.PSCustomObject] -and $obj.row) {
                            $cols = $obj.row.columns
                            Write-Host "[ksql row] " -NoNewline
                            $cols | ConvertTo-Json -Depth 5 | Write-Host
                        } else {
                            $line | Write-Host
                        }
                    } catch {
                        # not JSON, just print raw
                        Write-Host $line
                    }
                }
            }
        } catch {
            Write-Host "Streaming read exception: $($_.Exception.Message)" -ForegroundColor Yellow
        } finally {
            try { $reader.Close() } catch {}
            try { $stream.Close() } catch {}
            try { $resp.Dispose() } catch {}
            try { $client.Dispose() } catch {}
            Write-Host "Streaming job ended." -ForegroundColor Cyan
        }
    }

    # Start as a background job so main thread can wait for user input
    $job = Start-Job -ScriptBlock $script -ArgumentList $KsqlUrl, $sql
    return $job
}

# Main dispatcher
if ($Mode -eq "pull") {
    Write-Host "Running pull query (TABLE)..." -ForegroundColor Cyan
    Write-Host "SQL:" $Query
    Invoke-KsqlPull -KsqlUrl $KsqlUrl -ksql $Query
    exit 0
}

# Mode = push (stream)
if ($Mode -eq "push") {
    Write-Host "Starting push (streaming) query against $KsqlUrl" -ForegroundColor Cyan
    Write-Host "SQL:" $StreamSql

    $job = Start-KsqlStreamJob -KsqlUrl $KsqlUrl -sql $StreamSql
    if ($null -eq $job) {
        Write-Host "Failed to start streaming job." -ForegroundColor Red
        exit 2
    }

    Write-Host ""
    Write-Host "Streaming job started (JobId = $($job.Id)). Press ENTER to stop the stream and return." -ForegroundColor Yellow
    # Wait for user to press Enter. Also poll job status and print periodic heartbeat.
    try {
        while ($true) {
            if ($job.State -eq "Failed" -or $job.State -eq "Completed") {
                Write-Host "Streaming job ended with state: $($job.State)" -ForegroundColor Cyan
                break
            }
            if ([console]::KeyAvailable) {
                $key = [console]::ReadKey($true)
                if ($key.Key -eq "Enter") {
                    Write-Host "`nUser requested stop. Stopping job..." -ForegroundColor Yellow
                    break
                }
            }
            Start-Sleep -Milliseconds 250
        }
    } finally {
        # Attempt to stop job gracefully
        if ($job.State -ne "Completed" -and $job.State -ne "Failed") {
            Write-Host "Stopping job id $($job.Id)..." -ForegroundColor Gray
            try {
                Stop-Job -Job $job -Force -ErrorAction SilentlyContinue
            } catch {}
        }
        # Remove job
        try { Remove-Job -Job $job -Force -ErrorAction SilentlyContinue } catch {}
        Write-Host "Streaming stopped." -ForegroundColor Green
    }
    exit 0
}
