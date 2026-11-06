# lab-dest – Distributed Object-Tolerant Storage Framework

Distributed Systems midterm (S1 2026-2027). This system splits files into SHA-256 content-addressed chunks and spreads them across storage nodes with consistent hashing. Replication and erasure coding keep the data available, and an S3-like API gives access to it.

The phase-by-phase plan, screenshot checklist and report outline are in [docs/LAB_GUIDE.md](docs/LAB_GUIDE.md).

## Quick start (Windows / PowerShell)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -v          # unit tests

docker compose up -d --build                     # 4 storage nodes on ports 8001-8004
docker compose ps
powershell -ExecutionPolicy Bypass -File scripts\demo_storage_node.ps1
```
