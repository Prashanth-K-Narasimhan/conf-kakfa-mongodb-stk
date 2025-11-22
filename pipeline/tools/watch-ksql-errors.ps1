<#
Watch ksqldb-server docker logs and show only high-signal events:
  - Errors and exceptions
  - Topic creation/failure
  - CREATE/DROP/ALTER statements (DDL)
  - Table/stream materialization messages
  - Connector registration messages
  - Startup/listening messages

Behavior:
  - Prints a short summary tag, timestamp, and the matching fragment
  - Deduplicates identical matches for a short cooldown window (configurable)
  - Optionally writes matches to an output file
  - Default service: ksqldb-server
#>

param(
    [string]$Service = "ksqldb-server",
    [string]$OutFile = "",
    [int]$DedupSeconds = 5,
    [int]$TailSeconds = 0   # if >0, uses `docker-compose logs --since=<TailSeconds>s` then follows
)

# Patterns mapped to a short tag and display color
$rules = @(
    @{ pat="ERROR|Exception|Fail|FATAL|SEVERE"; tag="ERROR"; color="Red" },
    @{ pat="UNKNOWN_TOPIC_OR_PARTITION|topic.*not found|Topic.*not found"; tag="TOPIC?"; color="Yellow" },
    @{ pat="Created topic|created topic|created new topic"; tag="TOPIC+CREATE"; color="Green" },
    @{ pat="CREATE STREAM|CREATE TABLE|CREATE TABLE AS|CREATE STREAM AS|DROP STREAM|DROP TABLE|ALTER TABLE"; tag="DDL"; color="Cyan" },
    @{ pat="Materialized|materialized|backing topic|changelog|repartition"; tag="MATERIALIZE"; color="Magenta" },
    @{ pat="Registered connector|Connector created|created connector|Failed to create connector"; tag="CONNECTOR"; color="Yellow" },
    @{ pat="Listening on|Started|Started ksqlDB|Started Server|Bound to|Listening for connections"; tag="STARTUP"; color="Green" },
    @{ pat="Deserialization error|deserialization|Malformed|bad record|Invalid timestamp"; tag="SERDE"; color="Yellow" },
    @{ pat="Task.*failed|Query terminated|Query exception|Dropped query"; tag="QUERY"; color="Red" }
)

# Build combined regex
$combined = ($rules | ForEach-Object { "($($_.pat))" }) -join "|"
$regex = [regex]::new($combined, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)

# dedupe map
$lastSeen = @{}

function Emit {
    param($tag, $line, $color)

    $ts = (Get-Date).ToString("HH:mm:ss")
    $short = $line.Trim()
    $out = "[$ts] [$tag] $short"

    # dedupe: skip if same short line seen within DedupSeconds
    $key = ($tag + "|" + $short)
    if ($lastSeen.ContainsKey($key)) {
        $elapsed = (Get-Date) - $lastSeen[$key]
        if ($elapsed.TotalSeconds -lt $DedupSeconds) {
            return
        }
    }
    $lastSeen[$key] = Get-Date

    if ($color -eq "Red") {
        Write-Host $out -ForegroundColor Red
    } elseif ($color -eq "Yellow") {
        Write-Host $out -ForegroundColor Yellow
    } elseif ($color -eq "Green") {
        Write-Host $out -ForegroundColor Green
    } elseif ($color -eq "Cyan") {
        Write-Host $out -ForegroundColor Cyan
    } elseif ($color -eq "Magenta") {
        Write-Host $out -ForegroundColor Magenta
    } else {
        Write-Host $out
    }

    if ($OutFile) {
        try { $out | Out-File -FilePath $OutFile -Append -Encoding utf8 } catch { Write-Host "Failed writing to $OutFile" -ForegroundColor Yellow }
    }
}

# Verify docker-compose present
try {
    $null = & docker-compose version 2>$null
} catch {
    Write-Host "docker-compose not found. Ensure Docker Compose is installed and in PATH." -ForegroundColor Red
    exit 2
}

# Build logs command
$logsCmd = "docker-compose logs"
if ($TailSeconds -gt 0) {
    $logsCmd = "$logsCmd --since=${TailSeconds}s"
}
$logsCmd = "$logsCmd -f $Service"

Write-Host "Watching $Service ($logsCmd). Showing only important lines. Dedup window: $DedupSeconds s" -ForegroundColor Cyan
if ($OutFile) { Write-Host "Logging matches to: $OutFile" -ForegroundColor Yellow }

# Start streaming
# Note: capturing both stdout/stderr with 2>&1
Invoke-Expression "$logsCmd 2>&1" | ForEach-Object {
    $line = $_.ToString()
    if ([string]::IsNullOrWhiteSpace($line)) { return }

    $m = $regex.Match($line)
    if ($m.Success) {
        # find which rule matched by checking each rule's regex
        foreach ($r in $rules) {
            if ([regex]::IsMatch($line, $r.pat, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)) {
                Emit $r.tag $line $r.color
                break
            }
        }
    }
}
