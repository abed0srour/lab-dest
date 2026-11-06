# Phase 2 demo: exercises one storage node's chunk API with curl.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_storage_node.ps1 [-Port 8001] [-Service storage-node-1]
param([int]$Port = 8001, [string]$Service = "storage-node-1")

Push-Location (Split-Path $PSScriptRoot -Parent)
$base = "http://localhost:$Port"
$file = (New-TemporaryFile).FullName
[IO.File]::WriteAllText($file, "Hello from the distributed storage lab! $(Get-Date -Format o)")
$hash = (Get-FileHash $file -Algorithm SHA256).Hash.ToLower()
$diskPath = "/data/chunks/$($hash.Substring(0,2))/$($hash.Substring(2,2))/$hash"

function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }

Step "Chunk content SHA-256 = $hash"

Step "PUT /chunks/<hash>   store the chunk"
curl.exe -s -w "  [HTTP %{http_code}]`n" -X PUT --data-binary "@$file" "$base/chunks/$hash"

Step "GET /chunks/<hash>   read it back (hash is verified)"
curl.exe -s -w "  [HTTP %{http_code}]`n" "$base/chunks/$hash"

Step "HEAD /chunks/<hash>   does it exist, and how big is it?"
curl.exe -s -I "$base/chunks/$hash"

Step "PUT content that does not match the hash   rejected"
curl.exe -s -w "  [HTTP %{http_code}]`n" -X PUT --data-binary "tampered" "$base/chunks/$hash"

Step "Simulate bit rot: overwrite the chunk file on the node's disk ($diskPath)"
docker compose exec -T $Service sh -c "echo bitrot > $diskPath"

Step "GET after corruption   rejected, corrupted data is never served"
curl.exe -s -w "  [HTTP %{http_code}]`n" "$base/chunks/$hash"

Step "PUT the original content again   repairs the chunk"
curl.exe -s -w "  [HTTP %{http_code}]`n" -X PUT --data-binary "@$file" "$base/chunks/$hash"
curl.exe -s -w "  [HTTP %{http_code}]`n" "$base/chunks/$hash"

Step "DELETE /chunks/<hash>"
curl.exe -s -w "[HTTP %{http_code}]`n" -X DELETE "$base/chunks/$hash"
curl.exe -s -I "$base/chunks/$hash"

Step "Health of every storage node (node id, free space, chunk count)"
foreach ($p in 8001..8004) { curl.exe -s "http://localhost:$p/health"; "" }

Remove-Item $file
Pop-Location
