# Phase 4 demo: replication vs erasure coding, quorum writes, failure detection and repair,
# erasure-coding fault tolerance, and the scrubber. Pauses after each step for a screenshot.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_phase4.ps1 [-NoPause]
# Needs: docker compose up -d (7 storage nodes) and the datasets.
param([switch]$NoPause)

Push-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$cli = "scripts\cli.py"
$api = "http://127.0.0.1:9000"
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }
function Note($text) { Write-Host $text -ForegroundColor Yellow }
function StoreCli { & $py $cli @args }
function Pause-Step { if (-not $NoPause) { Read-Host "`n[take your screenshot, then press Enter]" | Out-Null } }
function Random-File($path, $mb) {
    $buf = New-Object byte[] ($mb * 1MB)
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($buf)
    [IO.File]::WriteAllBytes($path, $buf)
}
function Wait-Healthy {
    StoreCli watch --until healthy --timeout 120 | Select-Object -Last 1
}

# Start from the same state every run.
foreach ($o in "demo-rep disk-02.img", "demo-ec disk-02.img", "demo-ec small-ec.bin", "demo-rep quorum-1.bin",
               "demo-rep quorum-2.bin") {
    & $py $cli rm @($o -split " ") 2>$null | Out-Null
}
$tmp = $env:TEMP

Step "1. Cluster: 7 storage nodes and the redundancy settings"
StoreCli health
Pause-Step

Step "2. The same 216 MB file in a replicated bucket and an erasure-coded bucket"
StoreCli mb demo-rep
StoreCli mb demo-ec --redundancy ec
StoreCli put demo-rep disk-02.img datasets\large\disk-02.img
StoreCli put demo-ec disk-02.img datasets\large\disk-02.img
Write-Host ""
StoreCli stats --bucket demo-rep
Write-Host ""
StoreCli stats --bucket demo-ec
Write-Host ""
StoreCli stat demo-ec disk-02.img --limit 6
Pause-Step

Step "3. Quorum write: N = 3 copies, the upload needs W = 2 of them"
$file = Join-Path $tmp "quorum-test.bin"
Random-File $file 1
$hash = (Get-FileHash $file -Algorithm SHA256).Hash.ToLower()
$plan = Invoke-RestMethod -Method Post -Uri "$api/plan" -ContentType "application/json" `
        -Body (@{ bucket = "demo-rep"; hashes = @($hash) } | ConvertTo-Json)
$targets = @($plan.chunks.$hash.nodes | ForEach-Object { $_.node_id })
Note "The hash ring places this 1 MB chunk on: $($targets -join ', ')"
Note "`nKill $($targets[1]) (1 of its 3 nodes down) and upload:"
docker compose kill $targets[1] 2>&1 | Out-Null
StoreCli put demo-rep quorum-1.bin $file
StoreCli rm demo-rep quorum-1.bin | Out-Null
Note "`nAlso kill $($targets[2]) (2 of its 3 nodes down) and upload again:"
docker compose kill $targets[2] 2>&1 | Out-Null
StoreCli put demo-rep quorum-2.bin $file
Note "`nRestarting $($targets[1]) and $($targets[2]) ..."
docker compose start $targets[1] $targets[2] 2>&1 | Out-Null
Remove-Item $file
Wait-Healthy
Pause-Step

Step "4. Node failure: detection after missed heartbeats, then automatic repair"
$since = (Invoke-RestMethod "$api/repair/stats").last_job_id
Note "Killing storage-node-3 (no more heartbeats) ..."
docker compose kill storage-node-3 2>&1 | Out-Null
StoreCli watch --until repaired --timeout 300
Write-Host ""
StoreCli repairs --since $since
Pause-Step

Step "5. Data is intact after the repair: download both copies and compare SHA-256"
StoreCli get demo-rep disk-02.img "$tmp\rep.img"
StoreCli get demo-ec disk-02.img "$tmp\ec.img"
Write-Host "Get-FileHash original: $((Get-FileHash datasets\large\disk-02.img).Hash.ToLower())"
Remove-Item "$tmp\rep.img", "$tmp\ec.img"
Note "`nStarting storage-node-3 again: it rejoins and is scrubbed automatically"
docker compose start storage-node-3 2>&1 | Out-Null
Wait-Healthy
Pause-Step

Step "6. Erasure coding 4 + 2 survives any 2 failed nodes, but not 3"
$file = Join-Path $tmp "small-ec.bin"
Random-File $file 24
StoreCli put demo-ec small-ec.bin $file
Note "`nKill storage-node-1 and storage-node-2, then download:"
docker compose kill storage-node-1 storage-node-2 2>&1 | Out-Null
StoreCli get demo-ec small-ec.bin "$tmp\small-ec.out"
Write-Host "Get-FileHash original: $((Get-FileHash $file).Hash.ToLower())"
Note "`nAlso kill storage-node-4 (3 nodes down), then download again:"
docker compose kill storage-node-4 2>&1 | Out-Null
StoreCli get demo-ec small-ec.bin "$tmp\small-ec.out"
Note "`nRestarting the three nodes ..."
docker compose start storage-node-1 storage-node-2 storage-node-4 2>&1 | Out-Null
Remove-Item $file, "$tmp\small-ec.out" -ErrorAction SilentlyContinue
Wait-Healthy
Pause-Step

Step "7. Scrubber: detect bit rot and repair it from a healthy copy"
$meta = StoreCli stat demo-rep disk-02.img --json | ConvertFrom-Json
$loc = $meta.chunks[0].locations[0]
$blob = $loc.blob_hash
$started = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Note "Corrupting the copy of chunk $($blob.Substring(0,12))... stored on $($loc.node_id) (writing over the file on its disk)"
docker compose exec -T $loc.node_id sh -c "echo bitrot > /data/chunks/$($blob.Substring(0,2))/$($blob.Substring(2,2))/$blob"
Write-Host "Reading that copy straight from the node:"
curl.exe -s -w "  [HTTP %{http_code}]`n" "$($loc.public_url)/chunks/$blob"
Note "`nRunning the scrubber on every node ..."
StoreCli scrub
do {
    Start-Sleep -Seconds 2
    $h = Invoke-RestMethod "$api/cluster"
} while ($h.scrub_jobs.pending + $h.scrub_jobs.running + $h.repair_jobs.pending + $h.repair_jobs.running -gt 0)
docker compose logs repair-worker --since $started 2>&1 | Select-String "scrub $($loc.node_id)|repaired chunk $($blob.Substring(0,12))|scrubbed $($loc.node_id)" |
    ForEach-Object { $_.Line -replace '^repair-worker-1\s+\|\s+', '' }
Write-Host "`nReading the copy from $($loc.node_id) again:"
curl.exe -s -o NUL -w "  [HTTP %{http_code}] the repaired copy is served again`n" "$($loc.public_url)/chunks/$blob"
Pause-Step

Step "8. Final state"
StoreCli health
Pop-Location
