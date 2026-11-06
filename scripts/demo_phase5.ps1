# Phase 5 demo: the gateway API with curl. Pauses after each step for a screenshot.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_phase5.ps1 [-NoPause]
# Needs: docker compose up -d and the datasets.
param([switch]$NoPause)

Push-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$g = "http://127.0.0.1:8080"
$key = "X-API-Key: student-key-2026"
$resp = Join-Path $env:TEMP "gateway-response.txt"
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }
function Note($text) { Write-Host $text -ForegroundColor Yellow }
function Pause-Step { if (-not $NoPause) { Read-Host "`n[take your screenshot, then press Enter]" | Out-Null } }

# Runs curl, prints the JSON body nicely indented, then the status line.
function Api {
    $info = curl.exe -s -o $resp -w "%{http_code} %{time_total} %{size_upload} %{size_download}" @args
    $code, $time, $up, $down = $info -split " "
    $body = if (Test-Path $resp) { Get-Content $resp -Raw } else { "" }
    # json.tool reads the file itself: piping text into a program adds a BOM in Windows PowerShell 5.1
    if ($body -match '^\s*[\[{]') { & $py -m json.tool $resp } elseif ($body) { Write-Host $body }
    $line = "[HTTP $code]  $([math]::Round([double]$time, 2)) s"
    if ([double]$up -gt 1MB) { $line += "  uploaded $([math]::Round([double]$up / 1MB, 1)) MB at $([math]::Round([double]$up / [double]$time / 1MB, 1)) MB/s" }
    if ([double]$down -gt 1MB) { $line += "  downloaded $([math]::Round([double]$down / 1MB, 1)) MB at $([math]::Round([double]$down / [double]$time / 1MB, 1)) MB/s" }
    Write-Host $line -ForegroundColor Green
    Remove-Item $resp -ErrorAction SilentlyContinue
}

$notes = Get-ChildItem datasets\small\notes -File | Select-Object -First 3
$users = Get-ChildItem datasets\small\users -File | Select-Object -First 2
# Start from the same state every run.
$old = @("photos/objects/images/disk-02.img", "archive/objects/images/disk-02.img") +
       ($notes | ForEach-Object { "photos/objects/notes/$($_.Name)" }) +
       ($users | ForEach-Object { "photos/objects/users/$($_.Name)" })
foreach ($o in $old) { curl.exe -s -o NUL -X DELETE -H $key "$g/buckets/$o" }

Step "1. Authentication: an API key is required (interactive docs: http://127.0.0.1:8080/docs)"
Note "No key:"
Api "$g/buckets"
Note "Wrong key:"
Api -H "X-API-Key: not-a-real-key" "$g/buckets"
Note "Valid key (the health check needs no key):"
Api "$g/health"
Pause-Step

Step "2. Create and list buckets"
Api -X PUT -H $key "$g/buckets/photos"
Api -X PUT -H $key "$g/buckets/archive?redundancy=ec"
Api -H $key "$g/buckets"
Pause-Step

Step "3. Upload: a 216 MB file streamed into a replicated and an erasure-coded bucket, and small files with tags"
Api -T datasets\large\disk-02.img -H $key -H "Content-Type: application/octet-stream" `
    -H "X-Tags: type=disk-image,project=lab" "$g/buckets/photos/objects/images/disk-02.img"
Api -T datasets\large\disk-02.img -H $key -H "X-Tags: type=disk-image,project=lab" `
    "$g/buckets/archive/objects/images/disk-02.img"
foreach ($f in $notes) {
    curl.exe -s -o NUL -w "uploaded notes/$($f.Name) [HTTP %{http_code}]`n" -T $f.FullName -H $key `
        -H "Content-Type: text/plain" -H "X-Tags: type=note" "$g/buckets/photos/objects/notes/$($f.Name)"
}
foreach ($f in $users) {
    curl.exe -s -o NUL -w "uploaded users/$($f.Name) [HTTP %{http_code}]`n" -T $f.FullName -H $key `
        -H "Content-Type: application/json" -H "X-Tags: type=user" "$g/buckets/photos/objects/users/$($f.Name)"
}
Pause-Step

Step "4. Download: whole object (hash check) and byte ranges"
$out = Join-Path $env:TEMP "downloaded.img"
$code, $time, $size = (curl.exe -s -o $out -w "%{http_code} %{time_total} %{size_download}" -H $key `
    "$g/buckets/photos/objects/images/disk-02.img") -split " "
Write-Host "[HTTP $code]  downloaded $([math]::Round([double]$size / 1MB, 1)) MB in $([math]::Round([double]$time, 2)) s ($([math]::Round([double]$size / [double]$time / 1MB, 1)) MB/s)" -ForegroundColor Green
Write-Host "SHA-256 of the original:   $((Get-FileHash datasets\large\disk-02.img).Hash.ToLower())"
Write-Host "SHA-256 of the download:   $((Get-FileHash $out).Hash.ToLower())"
Remove-Item $out
Note "`nRange: bytes=0-99 (the first 100 bytes):"
curl.exe -s -D - -o NUL -r 0-99 -H $key "$g/buckets/photos/objects/images/disk-02.img" | Select-String "HTTP/|content-range|content-length"
Note "Range: bytes=4194300-4194399 (100 bytes across the boundary of chunks 0 and 1):"
curl.exe -s -D - -o NUL -r 4194300-4194399 -H $key "$g/buckets/photos/objects/images/disk-02.img" | Select-String "HTTP/|content-range|content-length"
Note "Range past the end of the object:"
curl.exe -s -D - -o NUL -r 999999999- -H $key "$g/buckets/photos/objects/images/disk-02.img" | Select-String "HTTP/|content-range"
Pause-Step

Step "5. Object metadata: HEAD (headers only) and ?metadata=true (JSON)"
curl.exe -s -I -H $key "$g/buckets/photos/objects/images/disk-02.img"
Api -H $key "$g/buckets/photos/objects/notes/$($notes[0].Name)?metadata=true"
Pause-Step

Step "6. List by prefix, and search by prefix, size, date and tag"
Note "List bucket photos, prefix notes/:"
Api -H $key "$g/buckets/photos/objects?prefix=notes/"
$today = (Get-Date).ToString("yyyy-MM-dd")
Note "Search: objects larger than 10 MB"
Api -H $key "$g/search?min_size=10000000"
Note "Search: tag type=user, created today or later, in bucket photos"
Api -H $key "$g/search?bucket=photos&tag=type=user&after=$today"
Pause-Step

Step "7. Delete an object, then try to read it"
Api -X DELETE -H $key "$g/buckets/photos/objects/notes/$($notes[2].Name)"
Api -H $key "$g/buckets/photos/objects/notes/$($notes[2].Name)"
Pause-Step

Step "8. Cluster status: nodes, free space, chunks per node, degraded objects"
Api -H $key "$g/cluster/status"
Pause-Step

Step "9. Rate limiting: 30 quick requests with the demo key (5 requests per second)"
$refused = $null
$codes = foreach ($i in 1..30) {
    $r = curl.exe -s -i -H "X-API-Key: demo-key-2026" "$g/buckets"
    $code = ($r[0] -split " ")[1]
    if ($code -eq "429" -and -not $refused) { $refused = $r }
    $code
}
$codes | Group-Object | ForEach-Object { Write-Host "HTTP $($_.Name): $($_.Count) requests" }
Note "`nOne of the refused requests:"
$refused | Select-String "HTTP/|retry-after|x-ratelimit|detail"
Pop-Location
