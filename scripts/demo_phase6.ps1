# Phase 6 demo helper: sample data for the web UI, a node failure to watch on the Cluster page,
# and a look inside Elasticsearch. Pauses so you can take screenshots in the browser.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_phase6.ps1 [-NoPause]
param([switch]$NoPause)

Push-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }
function Note($text) { Write-Host $text -ForegroundColor Yellow }
function Pause-Step($what) { if (-not $NoPause) { Read-Host "`n[$what, then press Enter]" | Out-Null } }

Step "1. Sample data: bucket docs (replication, 2,000 small tagged files) and archive (erasure coding, 2 disk images)"
& $py scripts\seed_ui_data.py
Note "`nOpen http://127.0.0.1:5173 in your browser."
Note "Files page: pick a bucket, upload a file, download one, open Details. Search page: try a few searches."
Pause-Step "take your Files and Search screenshots"

Step "2. Elasticsearch: the index really contains the object metadata"
Note "Number of indexed objects:"
curl.exe -s "http://127.0.0.1:9200/objects/_count"
Write-Host ""
Note "One document, found with a direct Elasticsearch query (key contains 'disk'):"
$query = Join-Path $env:TEMP "es-query.json"
[IO.File]::WriteAllText($query, '{"size": 1, "query": {"wildcard": {"key": {"value": "*disk*"}}}}')
curl.exe -s -H "Content-Type: application/json" "http://127.0.0.1:9200/objects/_search?pretty" --data-binary "@$query"
Remove-Item $query
Pause-Step "take your Elasticsearch screenshot"

Step "3. A node failure, live on the Cluster page"
Note "Open the Cluster page (http://127.0.0.1:5173/#/cluster) and keep it visible."
Pause-Step "when the Cluster page is open"
Note "Killing storage-node-4 ... watch it go from 'No heartbeat' to 'Failed', then the repair progress bar."
docker compose kill storage-node-4 2>&1 | Out-Null
& $py scripts\cli.py watch --until repaired --timeout 300
Pause-Step "take your 'failed node and repair' screenshots (and one after the repair)"

Note "Starting storage-node-4 again ..."
docker compose start storage-node-4 2>&1 | Out-Null
& $py scripts\cli.py watch --until healthy --timeout 120 | Select-Object -Last 1
Note "The node is back: the Cluster page shows all 7 nodes alive again."
Pop-Location
