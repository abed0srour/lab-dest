# Phase 7 demo (Kubernetes part): storage nodes as a StatefulSet with persistent volumes and
# liveness/readiness probes. Pauses after each step for a screenshot.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_phase7_k8s.ps1 [-NoPause]
# Needs: Kubernetes enabled in Docker Desktop, and the images built (docker compose build).
param([switch]$NoPause)

Push-Location (Split-Path $PSScriptRoot -Parent)
$kc = @("--context", "docker-desktop", "-n", "dos")
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }
function Note($text) { Write-Host $text -ForegroundColor Yellow }
function Pause-Step { if (-not $NoPause) { Read-Host "`n[take your screenshot, then press Enter]" | Out-Null } }
function Blobs($pod) {
    kubectl @kc exec $pod -- python -c "import urllib.request,json; print(len(json.load(urllib.request.urlopen('http://localhost:8000/chunks'))['chunks']))" 2>$null
}

Step "1. Deploy the system to Kubernetes (namespace dos)"
kubectl --context docker-desktop apply -k deploy/k8s
kubectl @kc wait --for=condition=Ready pod --all --timeout=240s | Out-Null
kubectl @kc get statefulset,deployment,service
Pause-Step

Step "2. Storage nodes: a StatefulSet of 7 pods, each with its own persistent volume"
kubectl @kc get pods -o wide
kubectl @kc get pvc
Pause-Step

Step "3. Upload a file through the gateway Service"
$pf = Start-Process kubectl -ArgumentList "--context", "docker-desktop", "-n", "dos", "port-forward", "svc/gateway", "18080:8080" -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 4
curl.exe -s -o NUL -X PUT -H "X-API-Key: student-key-2026" "http://127.0.0.1:18080/buckets/k8s-demo"
curl.exe -s -T datasets\large\disk-00.img -H "X-API-Key: student-key-2026" -w "`n[HTTP %{http_code}] %{time_total} s`n" "http://127.0.0.1:18080/buckets/k8s-demo/objects/disk-00.img"
$pf | Stop-Process
Note "`nThe gateway Service spreads requests over the two gateway pods (20 requests from inside the cluster):"
kubectl @kc exec deploy/metadata -- python -c "import urllib.request, collections; c = collections.Counter(urllib.request.urlopen('http://gateway:8080/health').headers['X-Gateway-Id'] for _ in range(20)); [print(f'  {n:2d} answered by {g}') for g, n in c.items()]"
Pause-Step

Step "4. Liveness probe: freeze storage-node-1; Kubernetes restarts it"
Write-Host "storage-node-1 holds $(Blobs storage-node-1) blobs"
$initial = [int](kubectl @kc get pod storage-node-1 -o jsonpath="{.status.containerStatuses[0].restartCount}")
kubectl @kc exec storage-node-1 -- python -c "import urllib.request; urllib.request.urlopen(urllib.request.Request('http://localhost:8000/fault/hang?seconds=300', method='POST'), timeout=1)" 2>$null
Note "storage-node-1 is frozen; its probes now time out ..."
$t0 = Get-Date
do {
    Start-Sleep -Seconds 5
    $restarts = [int](kubectl @kc get pod storage-node-1 -o jsonpath="{.status.containerStatuses[0].restartCount}")
    $ready = kubectl @kc get pod storage-node-1 -o jsonpath="{.status.containerStatuses[0].ready}"
    "t={0,3:N0}s  ready={1}  restarts={2}" -f ((Get-Date) - $t0).TotalSeconds, $ready, $restarts
} while ($restarts -eq $initial -and ((Get-Date) - $t0).TotalSeconds -lt 150)
kubectl @kc get events --field-selector involvedObject.name=storage-node-1 --sort-by=.lastTimestamp |
    Select-String "Liveness probe failed|failed liveness probe" | Select-Object -Last 2 | ForEach-Object { $_.Line -replace '\s{2,}', '  ' }
do { Start-Sleep -Seconds 2 } until ((kubectl @kc get pod storage-node-1 -o jsonpath="{.status.containerStatuses[0].ready}") -eq "true")
Write-Host "after the restart storage-node-1 holds $(Blobs storage-node-1) blobs (its volume was kept)"
Pause-Step

Step "5. Delete a pod: the StatefulSet recreates it with the same name and the same volume"
Note "(The freeze lasted longer than the 15 s heartbeat timeout, so the metadata service also marked the node
failed, repaired its chunks elsewhere and rebalanced them back when it returned. Waiting for that to finish ...)"
do {
    Start-Sleep -Seconds 3
    $busy = kubectl @kc exec deploy/metadata -- python -c "import urllib.request, json; c = json.load(urllib.request.urlopen('http://localhost:8000/cluster')); print(sum(c[k]['pending'] + c[k]['running'] for k in ('repair_jobs', 'rebalance_jobs')))"
} while ([int]$busy -gt 0)
$volume = kubectl @kc get pvc data-storage-node-3 -o jsonpath="{.spec.volumeName}"
Write-Host "storage-node-3 holds $(Blobs storage-node-3) blobs on volume $volume"
kubectl @kc delete pod storage-node-3
kubectl @kc wait --for=condition=Ready pod/storage-node-3 --timeout=120s
Write-Host "the new storage-node-3 holds $(Blobs storage-node-3) blobs on volume $(kubectl @kc get pvc data-storage-node-3 -o jsonpath='{.spec.volumeName}')"
kubectl @kc get pods -l app=storage-node
Pause-Step

Note "To remove the Kubernetes deployment later: kubectl --context docker-desktop delete namespace dos"
Pop-Location
