# Phase 3 demo: metadata, placement, deduplication and chunking on the running cluster.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\demo_phase3.ps1
# Needs: docker compose up -d, and the datasets (python scripts\generate_datasets.py).

Push-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$cli = "scripts\cli.py"
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }
function StoreCli { & $py $cli @args }

# Start from the same state every run: remove what this script created last time.
foreach ($o in "demo disk-01.img", "demo copy-1.json", "demo copy-2.json", "fixed-test disk-00.img",
               "fixed-test disk-05.img", "cdc-test disk-00.img", "cdc-test disk-05.img") {
    & $py $cli rm @($o -split " ") 2>$null | Out-Null
}

Step "1. Storage nodes registered through heartbeats"
StoreCli nodes

Step "2. Create buckets"
StoreCli mb demo
StoreCli mb fixed-test
StoreCli mb cdc-test
StoreCli buckets

Step "3. Upload a 194 MB file, download it back, compare SHA-256"
StoreCli put demo disk-01.img datasets\large\disk-01.img --tags "type=disk-image,project=lab"
$out = Join-Path $env:TEMP "disk-01.downloaded"
StoreCli get demo disk-01.img $out
Write-Host "Get-FileHash original:   $((Get-FileHash datasets\large\disk-01.img).Hash)"
Write-Host "Get-FileHash downloaded: $((Get-FileHash $out).Hash)"
Remove-Item $out

Step "4. Object metadata: ordered chunk list and the 3 nodes holding each chunk"
StoreCli stat demo disk-01.img

Step "5. Deduplication with reference counting"
$file = (Get-ChildItem datasets\small\users -File | Select-Object -First 1).FullName
Write-Host "Uploading the same file twice under two names: $file"
StoreCli put demo copy-1.json $file
StoreCli put demo copy-2.json $file
StoreCli stat demo copy-2.json
Write-Host "`nDelete the first copy: the chunk is still used by copy-2.json, so it stays" -ForegroundColor Yellow
StoreCli rm demo copy-1.json
StoreCli stat demo copy-2.json
Write-Host "`nDelete the second copy: reference count reaches 0, the chunk is removed from all 3 nodes" -ForegroundColor Yellow
StoreCli rm demo copy-2.json

Step "6. Fixed-size vs FastCDC: upload disk-00.img and its new version disk-05.img"
StoreCli put fixed-test disk-00.img datasets\large\disk-00.img --chunking fixed
StoreCli put fixed-test disk-05.img datasets\large\disk-05.img --chunking fixed
StoreCli put cdc-test disk-00.img datasets\large\disk-00.img --chunking cdc
StoreCli put cdc-test disk-05.img datasets\large\disk-05.img --chunking cdc
Write-Host ""
StoreCli stats --bucket fixed-test
Write-Host ""
StoreCli stats --bucket cdc-test

Pop-Location
