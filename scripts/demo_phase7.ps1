# Phase 7 demo (Docker Compose part): load balancer, RabbitMQ workers, PostgreSQL failover,
# rebalancing onto a new node. Pauses after each step for a screenshot.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_phase7.ps1 [-NoPause]
# Needs: docker compose up -d (and the sample data: the script loads it if the cluster is empty).
param([switch]$NoPause)

Push-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$g = "http://127.0.0.1:8080"
$key = "X-API-Key: student-key-2026"
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }
function Note($text) { Write-Host $text -ForegroundColor Yellow }
function Pause-Step($what = "take your screenshot") { if (-not $NoPause) { Read-Host "`n[$what, then press Enter]" | Out-Null } }
function Prom($query) {
    (curl.exe -s "http://127.0.0.1:9090/api/v1/query?query=$([uri]::EscapeDataString($query))" | ConvertFrom-Json).data.result
}

Step "1. All services: 2 gateways behind nginx, 2 workers, RabbitMQ, 2 PostgreSQL nodes, Prometheus, Grafana"
docker compose ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}"
Pause-Step

Step "2. Load balancer: requests alternate between the two gateways"
1..6 | ForEach-Object { (curl.exe -s -D - -o NUL "$g/health" | Select-String "x-gateway-id").Line }
Note "`n40 requests while gateway-1 is killed half-way:"
$killer = Start-Job { Start-Sleep -Milliseconds 1500; docker compose -f $using:PWD\docker-compose.yml kill gateway-1 *> $null }
$answers = foreach ($i in 1..40) {
    $h = curl.exe -s -D - -o NUL -H $key "$g/buckets"
    "HTTP $(($h[0] -split ' ')[1]) from $((($h | Select-String 'x-gateway-id') -split ': ')[1])"
    Start-Sleep -Milliseconds 100
}
Wait-Job $killer | Out-Null
$answers | Group-Object | ForEach-Object { "{0,3} requests: {1}" -f $_.Count, $_.Name }
Note "No request failed: after gateway-1 died, nginx sent everything to gateway-2. Restarting gateway-1 ..."
docker compose start gateway-1 2>&1 | Out-Null
Pause-Step

$objects = (Invoke-RestMethod "http://127.0.0.1:9000/cluster").objects.total
if ($objects -lt 1000) {
    Note "`nLoading sample data first (2,000 files) ..."
    & $py scripts\seed_ui_data.py
}

Step "3. RabbitMQ: a node fails, the repair jobs are shared by the two workers"
Note "Open http://127.0.0.1:15672 (user dos, password dos) > Queues, to watch the jobs.* queues."
Pause-Step "when RabbitMQ is open"
$before = @{}; Prom 'sum by (instance) (dos_worker_jobs_total{kind="repair"})' | ForEach-Object { $before[$_.metric.instance] = [double]$_.value[1] }
Note "Killing storage-node-5 ..."
docker compose kill storage-node-5 2>&1 | Out-Null
& $py scripts\cli.py watch --until repaired --timeout 300
Start-Sleep -Seconds 6
Note "`nRepair jobs done by each worker (from Prometheus):"
Prom 'sum by (instance) (dos_worker_jobs_total{kind="repair"})' | ForEach-Object {
    "  worker {0,-18} {1,5} repairs" -f $_.metric.instance, ([double]$_.value[1] - [double]$before[$_.metric.instance])
}
Pause-Step

Step "4. The node comes back: scrub, garbage collection and rebalancing run through RabbitMQ"
docker compose start storage-node-5 2>&1 | Out-Null
Start-Sleep -Seconds 25
& $py scripts\cli.py jobs
docker compose logs metadata --since 1m 2>&1 | Select-String "is back" | ForEach-Object { $_.Line -replace '^metadata-1\s+\|\s+', '' }
docker compose logs repair-worker --since 1m 2>&1 | Select-String "rebalance plan|scrubbed storage-node-5|gc storage-node-5" |
    ForEach-Object { $_.Line -replace '^(repair-worker-\d)\s+\|\s+[\d\- :,]+INFO worker:', '$1:' }
Pause-Step

Step "5. Replicated PostgreSQL: pg-1 is the primary, pg-2 streams its changes"
docker compose exec -T pg-1 psql -U dos -d dos -c "SELECT application_name AS standby, state, sync_state FROM pg_stat_replication;"
Note "Killing the primary (pg-1), then uploading files while pg-2 takes over ..."
curl.exe -s -o NUL -X PUT -H $key "$g/buckets/failover"
docker compose kill pg-1 2>&1 | Out-Null
$t0 = Get-Date
foreach ($i in 1..3) {
    $r = curl.exe -s -o NUL -w "%{http_code} %{time_total}" -X PUT -H $key --data "written during failover $i" "$g/buckets/failover/objects/during-$i.txt"
    $code, $time = $r -split " "
    "upload {0}: HTTP {1} after {2:N1} s   (t={3:N0} s)" -f $i, $code, [double]$time, ((Get-Date) - $t0).TotalSeconds
}
docker compose logs pg-2 --since 1m 2>&1 | Select-String "\[ha|ready to accept connections" | ForEach-Object { $_.Line -replace '^pg-2-1\s+\|\s+', '' }
Note "The first upload waited for the promotion (about 10 s) instead of failing."
Pause-Step

Step "6. The old primary comes back as a standby (never as a second primary)"
docker compose start pg-1 2>&1 | Out-Null
Start-Sleep -Seconds 20
docker compose logs pg-1 --since 1m 2>&1 | Select-String "\[ha|streaming WAL" | ForEach-Object { $_.Line -replace '^pg-1-1\s+\|\s+', '' }
docker compose exec -T pg-2 psql -U dos -d dos -c "SELECT application_name AS standby, state FROM pg_stat_replication;"
Pause-Step

Step "7. Adding an 8th storage node: rebalancing moves its share of the data onto it"
docker compose --profile extra-node up -d storage-node-8 2>&1 | Out-Null
$t0 = Get-Date
do {
    Start-Sleep -Seconds 3
    $h = Invoke-RestMethod "http://127.0.0.1:9000/cluster"
    $rb = $h.rebalance_jobs
    $n8 = ($h.nodes.list | Where-Object node_id -eq "storage-node-8").assigned_blobs
    "t={0,3:N0}s  rebalance jobs: {1,4} waiting {2,3} running {3,5} done   storage-node-8 holds {4} blobs" -f `
        ((Get-Date) - $t0).TotalSeconds, $rb.pending, $rb.running, $rb.done, $n8
} while (((Get-Date) - $t0).TotalSeconds -lt 12 -or ($rb.pending + $rb.running -gt 0 -and ((Get-Date) - $t0).TotalSeconds -lt 300))
docker compose logs repair-worker --since 5m 2>&1 | Select-String "rebalance plan \(storage-node-8" | ForEach-Object { $_.Line -replace '^(repair-worker-\d)\s+\|\s+[\d\- :,]+INFO worker:', '$1:' }
& $py scripts\cli.py nodes
Pause-Step

Step "8. Monitoring"
Note "Open Grafana: http://127.0.0.1:3000 (the lab-dest dashboard opens directly)."
Note "Prometheus targets: http://127.0.0.1:9090/targets"
Pop-Location
