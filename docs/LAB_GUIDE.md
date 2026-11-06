# Distributed Object Storage – Lab Guide

This guide splits the midterm into **8 phases**, one per task in the assignment. Phases 7 and 8 are the bonus tasks.
For every phase it lists:

- **What gets built:** the files and services.
- **Check it works:** the commands to run.
- **Screenshots:** what to capture, and the command that shows it.
- **In the report:** what your paragraph should explain.

At the end you will find the **3 flowcharts** the report needs, a **Word report outline**, and the **deliverables checklist**.

## Progress

| Phase | Assignment task | Status |
|---|---|---|
| 1 | Set up the environment | ✅ Done |
| 2 | Storage node module | ✅ Done |
| 3 | Metadata and placement | ✅ Done |
| 4 | Redundancy and recovery | ✅ Done |
| 5 | API gateway | ✅ Done |
| 6 | Web UI + Elasticsearch search | ✅ Done |
| 7 | Load balancing and fault tolerance (bonus) | ✅ Done |
| 8 | Cloud deployment (bonus) | ⬜ Next |

---

## How to work through a phase

1. **Build** the phase.
2. **Run** the "Check it works" commands.
3. **Take the screenshots** listed in the phase's table.
4. **Write the paragraph** in your Word report under that phase's heading. See [the report outline](#how-to-organize-the-word-report).
5. **Commit** the phase:
   ```powershell
   git add -A
   git commit -m "Phase N: <what was done>"
   git tag phase-N
   git push --follow-tags
   ```

### Taking good screenshots

- The assignment requires **full-screen** screenshots. Press **`Win + PrtScn`** to capture the whole screen. The image is saved to `Pictures\Screenshots`.
- Before you capture, maximize the window, clear old output with `cls`, and make the text bigger (`Ctrl` + `+` in the terminal) so it is easy to read in the report.
- Name each file after its figure number in this guide, for example `P3-07-chunking-comparison.png`, and keep them in `docs\screenshots\phase-3\`.
- Right after each screenshot, write one note to yourself about what it shows. Writing the report paragraph later is then much faster.
- In PowerShell, always type **`curl.exe`**, not `curl`. Plain `curl` is a different PowerShell command.

---

## Architecture (the finished system)

```mermaid
flowchart LR
    U([User]) --> UI[Web UI - React]
    U --> LB[Nginx load balancer - bonus]
    UI --> LB
    LB --> G1[Gateway 1]
    LB --> G2[Gateway 2]
    G1 & G2 --> M[Metadata service]
    M --> PG[(PostgreSQL)]
    M --> ES[(Elasticsearch)]
    G1 & G2 --> SN[Storage nodes 1-7<br/>each with its own volume]
    SN -- heartbeat every 5 s --> M
    M -- repair / rebalance / GC jobs --> Q[[RabbitMQ - bonus]]
    Q --> W[Repair workers]
    W --> SN
    W --> M
```

| Service | Folder | Built in | Job |
|---|---|---|---|
| storage-node (4, then 7) | `storage_node/` | Phase 2 | Stores chunks named by their SHA-256 on its own volume, checks the hash on every read, sends heartbeats |
| metadata | `metadata/` | Phase 3 | PostgreSQL tables for buckets, objects, chunk lists, reference counts and nodes. Runs the consistent hashing ring and detects failed nodes |
| repair-worker | `repair_worker/` | Phase 4 | Rebuilds lost chunks, runs the scrubber, rebalances data, garbage-collects unused chunks |
| gateway | `gateway/` | Phase 5 | S3-like REST API, authentication, rate limiting, chunking, quorum, erasure coding |
| ui | `ui/` | Phase 6 | React web interface |
| PostgreSQL, Elasticsearch | Docker images | Phases 3 and 6 | Metadata store and search |
| Nginx, RabbitMQ, Prometheus, Grafana | `deploy/` | Phase 7 | Bonus |

Code used by several services (chunking, hash ring, erasure coding) goes in `common/`.

**Ports on your PC:** storage nodes `8001`–`8007`, gateway `8080`, metadata `9000`, UI `5173`, Elasticsearch `9200`, RabbitMQ admin `15672`, Prometheus `9090`, Grafana `3000`.

### Project folders

```
lab-dest/
├── storage_node/        Phase 2  storage node service + Dockerfile
├── common/              Phase 3+ shared code: chunking, hash ring, erasure coding
├── metadata/            Phase 3  metadata service + Dockerfile
├── repair_worker/       Phase 4  repair, scrubber, rebalance, garbage collection
├── gateway/             Phase 5  public API
├── ui/                  Phase 6  React app
├── deploy/              Phase 7-8 nginx, prometheus, grafana, kubernetes files
├── scripts/             dataset generator, demo scripts, benchmarks, CLI
├── benchmarks/results/  benchmark output (CSV + charts)
├── tests/               pytest tests + Postman collection
├── docs/                this guide, flowcharts, screenshots
└── docker-compose.yml
```

---

## Phase 1 – Set up the environment ✅

**Goal:** have every tool installed, a Git repository, and one separate disk (Docker volume) per storage node.

**Done:** Python 3.12 with a virtual environment in `.venv`, a Git repository on branch `main`, Docker Desktop, Node.js, and a Docker volume for each storage node.
Kubernetes is also running: Docker Desktop in kind mode, Kubernetes v1.36.1.
**Repository:** https://github.com/abed0srour/lab-dest (first commit `ac2520d`, Phases 1–2).

| Fig | Screenshot | How to show it |
|---|---|---|
| 1.1 | Installed tool versions | `py -3.12 --version; git --version; docker --version; kubectl version --client; node --version` |
| 1.2 | Virtual environment active, with packages installed | `.\.venv\Scripts\Activate.ps1`, then `pip list` (the prompt shows `(.venv)`) |
| 1.3 | GitHub repository page | Browser, after the first push |
| 1.4 | Kubernetes running | Docker Desktop → Kubernetes page (green), plus `kubectl get nodes` in a terminal |
| 1.5 | One volume per storage node | `docker volume ls` |
| 1.6 | Project structure | VS Code Explorer panel with the folders expanded |

**In the report, explain:**

- What you installed and why:
  - Python 3.12 and a virtual environment, so the project's packages stay separate from the rest of the PC.
  - Git and GitHub, for version control.
  - Docker, so each storage node runs in its own container like a separate machine.
  - Kubernetes, for deployment later.
- Why each node gets its own Docker volume: each one acts as that node's private disk, so nodes never share data by accident.

---

## Phase 2 – Storage node module ✅

**Goal:** a service that stores chunks named by their SHA-256 hash and refuses to return corrupted data.

**What was built:** [storage_node/app.py](../storage_node/app.py), [storage_node/Dockerfile](../storage_node/Dockerfile), [docker-compose.yml](../docker-compose.yml) (4 nodes), [tests/test_storage_node.py](../tests/test_storage_node.py), [scripts/demo_storage_node.ps1](../scripts/demo_storage_node.ps1).

| Method | Path | What it does |
|---|---|---|
| PUT | `/chunks/{sha256}` | Stores a chunk. Rejected with **400** if the content's hash does not match the name |
| GET | `/chunks/{sha256}` | Returns the chunk after re-checking its hash. **409** if the chunk is corrupted |
| HEAD | `/chunks/{sha256}` | Tells whether the chunk exists, and its size |
| DELETE | `/chunks/{sha256}` | Removes the chunk |
| GET | `/chunks` | Lists every chunk on this node (used later by repair) |
| GET | `/health` | Node id, free space, chunk count |

- **Disk layout:** chunks are stored at `/data/chunks/ab/cd/<hash>`, where `ab` and `cd` are the first 4 characters of the hash. This keeps any one folder from holding thousands of files.
- **Atomic writes:** each chunk is written to a temporary file and then renamed into place, so a crash can never leave a half-written chunk.
- **Heartbeat:** every 5 seconds, each node sends its id, free space and chunk count to the metadata service. That service is built in Phase 3, so until then each node logs a warning that its heartbeats are failing.

**Check it works:**

```powershell
docker compose up -d --build
docker compose ps
powershell -ExecutionPolicy Bypass -File scripts\demo_storage_node.ps1
.\.venv\Scripts\python.exe -m pytest -v
```

| Fig | Screenshot | How to show it |
|---|---|---|
| 2.1 | Storage node code | `storage_node/app.py` open in VS Code |
| 2.2 | Automatic API documentation | Browser: http://localhost:8001/docs |
| 2.3 | 4 nodes running and healthy | `docker compose ps` |
| 2.4 | Storing, reading, checking and deleting a chunk | Demo script, top part |
| 2.5 | Corrupted chunk rejected | Demo script, "bit rot" part, plus `docker compose logs storage-node-1 --tail 5` |
| 2.6 | All tests pass | `.\.venv\Scripts\python.exe -m pytest -v` |
| 2.7 | Heartbeats arriving | **Take this in Phase 3**, once the metadata service exists: `.\.venv\Scripts\python.exe scripts\cli.py nodes` (same as Fig 3.2) |

**In the report, explain:**

- **Content addressing:** a chunk's name is the SHA-256 of its bytes. Identical data always gets the same name, and changing even one bit gives a different name. That is how corruption is detected.
- Why the node checks the hash on every read.
- The atomic write.
- The heartbeat contents.
- What each HTTP status code means.

---

## Phase 3 – Metadata and placement

**Goal:**

- Split files into chunks.
- Remember which chunks make up each object.
- Decide which nodes store each chunk.
- Store identical chunks only once.

**What was built:**

| File | What it does |
|---|---|
| [common/chunking.py](../common/chunking.py) | Fixed 4 MB chunks, and FastCDC (min 1 MB, average 4 MB, max 16 MB). FastCDC is fed 64 MB windows so it works on streams |
| [common/ring.py](../common/ring.py) | Consistent hashing ring, 128 virtual nodes per node. A chunk goes to the next 3 distinct nodes clockwise from its hash |
| [common/client.py](../common/client.py) | Write path (chunk → ask metadata → write 3 copies → commit) and read path (fetch each chunk, check its hash). Reused by the gateway in Phase 5 |
| [metadata/app.py](../metadata/app.py) | Metadata service (FastAPI + PostgreSQL) on port **9000**: heartbeats, buckets, placement and dedup check (`/plan`), object commit and versions, reference counts, garbage collection, `/stats` |
| [scripts/cli.py](../scripts/cli.py) | Command-line tool: `nodes`, `mb`, `buckets`, `put`, `put-dir`, `get`, `ls`, `stat`, `rm`, `stats` |
| [scripts/generate_datasets.py](../scripts/generate_datasets.py) | Creates the two workloads in `datasets/` |
| [scripts/bench_chunking.py](../scripts/bench_chunking.py), [scripts/bench_placement.py](../scripts/bench_placement.py) | Benchmarks; results in `benchmarks/results/*.csv` |
| [scripts/demo_phase3.ps1](../scripts/demo_phase3.ps1) | Runs the whole Phase 3 demo for the screenshots |
| `tests/test_ring.py`, `tests/test_chunking.py`, `tests/test_cluster.py` | 15 new tests (22 in total) |

**Database tables** (created automatically when the metadata service starts):

| Table | Holds |
|---|---|
| `buckets` | Name, redundancy mode (replication or erasure coding), creation date |
| `objects` | Bucket, key, version, size, chunking method, tags, creation date |
| `object_chunks` | The ordered list of chunk hashes for each object version |
| `chunks` | Hash, size, reference count |
| `chunk_locations` | Which nodes hold each chunk |
| `nodes` | Node id, URLs, last heartbeat, free space, chunk count |

**Datasets** (`python scripts\generate_datasets.py`, about 3 minutes):

- `datasets/small/`: 5,000 files, 121.8 MB, of which 539 are exact duplicates.
- `datasets/large/`: 10 disk images, 1.80 GB in total. `disk-05` to `disk-09` are new versions of `disk-00` to `disk-04`.

**Measured results:**

| Test | Fixed 4 MB | FastCDC |
|---|---|---|
| Large workload: space saved by dedup | 5.2% | **38.7%** |
| Small workload: space saved by dedup | 11.1% | 11.1% (each small file is a single chunk) |
| Insert 10 bytes into a 109 MB file: chunks changed | 15 of 28 (57 MB stored again) | **1 of 29 (1.8 MB)** |

| Scenario | Ring: copies moved | Modulo: copies moved | Ring: primary changed | Modulo: primary changed | Ideal |
|---|---|---|---|---|---|
| Add a node (4 → 5) | **20.1%** | 39.8% | 20.8% | 79.3% | 20% |
| Remove a node (5 → 4) | **18.2%** | 40.4% | 20.9% | 80.8% | 20% |

With 1 virtual node per node, the busiest node holds 27% more than average. With 128 it holds only **4.7%** more.

**Check it works:**

```powershell
docker compose up -d --build
.\.venv\Scripts\python.exe -m pytest -v
powershell -ExecutionPolicy Bypass -File scripts\demo_phase3.ps1
```

> To start again from an empty cluster: `docker compose down -v`, then `docker compose up -d`. This deletes every stored chunk and the whole database.

| Fig | Screenshot | How to show it |
|---|---|---|
| 3.1 | PostgreSQL + metadata service running | `docker compose ps` |
| 3.2 | All 4 nodes sending heartbeats (also counts as Fig 2.7) | `.\.venv\Scripts\python.exe scripts\cli.py nodes` |
| 3.3 | Database tables | `docker compose exec postgres psql -U dos -d dos -c "\dt"` |
| 3.4 | Datasets generated | `.\.venv\Scripts\python.exe scripts\generate_datasets.py` output |
| 3.5 | Upload and download a 194 MB file; SHA-256 identical | Demo script, step 3 |
| 3.6 | Object metadata with its ordered chunk list and 3 nodes per chunk | Demo script, step 4 |
| 3.7 | Deduplication: refcount 2 → 1 → chunk removed from 3 nodes | Demo script, step 5 |
| 3.8 | Fixed-size vs FastCDC on the cluster | Demo script, step 6 |
| 3.9 | Fixed-size vs FastCDC on both workloads + edit test | `.\.venv\Scripts\python.exe scripts\bench_chunking.py` |
| 3.10 | Data moved when adding or removing a node; balance by vnodes | `.\.venv\Scripts\python.exe scripts\bench_placement.py` |
| 3.11 | 5,000 small files uploaded, with dedup | `cli.py mb small`, then `cli.py put-dir small datasets\small`, then `cli.py stats --bucket small` |
| 3.12 | Chunks spread evenly over the 4 nodes | `cli.py nodes` (after Fig 3.11) |
| 3.13 | All tests pass | `.\.venv\Scripts\python.exe -m pytest -v` |

**In the report, explain:**

- Fixed-size vs content-defined chunking, and why FastCDC handles inserted bytes better.
- What metadata is stored, and why PostgreSQL. Searching by prefix, size, date and tag is plain SQL.
- How the ring works, with a drawing: a circle, virtual nodes, a chunk hash walking clockwise. Virtual nodes spread the data evenly.
- Your measured movement percentages compared with the expected ~20%.
- How reference counting makes deletion safe.

---

## Phase 4 – Redundancy and recovery

**Goal:** keep data available when nodes fail or disks corrupt data.

**What was built:**

| Part | What it does |
|---|---|
| **Quorum replication** | N = 3 copies, written in parallel. The upload succeeds once **W = 2** are stored; reads ask **R = 2** replicas (GET one, HEAD one). W + R = 4 > N = 3, so the replicas a reader asks always include one that acknowledged the write. The metadata service refuses to start if W + R ≤ N |
| **Erasure coding** ([common/erasure.py](../common/erasure.py)) | Reed–Solomon with `zfec`: each chunk becomes 4 data + 2 parity fragments on 6 different nodes, and any 4 rebuild it. An upload needs 5 of the 6. Chosen per bucket: `cli.py mb NAME --redundancy ec` |
| **7 storage nodes** | 6 for the fragments of a chunk, plus one spare so a lost fragment can be rebuilt on a node that does not already hold another fragment of the same chunk |
| **Failure detection** ([metadata/app.py](../metadata/app.py)) | Every 2 s, a node without a heartbeat for 15 s (3 missed beats) is marked **FAILED**, removed from the ring, and one repair job is queued per chunk it held. A node that comes back rejoins and is scrubbed |
| **Repair worker** ([repair_worker/worker.py](../repair_worker/worker.py)) | 8 threads take jobs from a PostgreSQL queue. Replication: copy a healthy replica to the next node on the ring. Erasure coding: read 4 fragments, decode, re-encode the missing one. Every job records the bytes it read and wrote |
| **Scrubber** | Each node re-hashes all its blobs (`POST /scrub`); corrupted or missing ones are recorded as bad copies and repaired from a healthy copy. Runs every 10 minutes, or now with `cli.py scrub` |
| **Sweep** | Every 30 s, any chunk still below full redundancy without a repair job gets one |
| **Read path** | A replica or fragment that is missing or fails its hash check is reported to the metadata service and repaired; the read carries on with another copy |
| [scripts/demo_phase4.ps1](../scripts/demo_phase4.ps1) | The whole Phase 4 demo in 8 steps, pausing after each one for a screenshot |
| [scripts/bench_redundancy.py](../scripts/bench_redundancy.py) | Replication vs erasure coding on both workloads: storage overhead, then kills a node and measures repair traffic |
| `tests/test_erasure.py`, `tests/test_cluster.py` | 10 new tests (32 in total) |

**Measured in the demo** (216 MB file in each scheme, `storage-node-3` killed):

| | Replication (3 copies) | Erasure coding (4 + 2) |
|---|---|---|
| Stored on the nodes | 648 MB (3.00×) in 162 blobs | 324 MB (1.50×) in 324 blobs |
| Failure detected after | 14–17 s | 14–17 s |
| Repair: chunks rebuilt | 22 | 53 |
| Repair: read / written | 88 MB / 88 MB (1×) | 212 MB / 53 MB (4×) |
| Repair time | about 2 s | about 2 s |
| Node failures survived | 2 | 2 (with 3 down, a chunk had only 3 of 4 fragments) |

**Benchmark** (`bench_redundancy.py`: both workloads, then `storage-node-1` killed while holding 6,203 blobs; FAILED after 13.6 s, fully repaired after 104 s):

| Workload | Scheme | Stored / unique | Blobs | Chunks rebuilt | Repair read | Repair written |
|---|---|---|---|---|---|---|
| Small (5,000 files, 108 MB unique) | Replication | 3.0× | 13,383 | 2,027 | 49.2 MB | 49.2 MB |
| Small | EC 4 + 2 | 1.5× | 26,766 | 3,939 | 95.3 MB | 23.8 MB |
| Large (4 images, 720 MB) | Replication | 3.0× | 546 | 81 | 319 MB | 319 MB |
| Large | EC 4 + 2 | 1.5× | 1,092 | 156 | 619 MB | 154.8 MB |

**Check it works:**

```powershell
docker compose down -v ; docker compose up -d --build
.\.venv\Scripts\python.exe -m pytest -v
powershell -ExecutionPolicy Bypass -File scripts\demo_phase4.ps1
.\.venv\Scripts\python.exe scripts\bench_redundancy.py
```

| Fig | Screenshot | How to show it |
|---|---|---|
| 4.1 | 7 nodes running + the repair worker | `docker compose ps` |
| 4.2 | Cluster settings and node health | Demo step 1 |
| 4.3 | Same file in both schemes: 3.00× vs 1.50×; 6 fragments on 6 nodes | Demo step 2 |
| 4.4 | Quorum write: OK with 1 node down, refused with 2 down | Demo step 3 |
| 4.5 | Node killed → FAILED after ~15 s → repaired; repair traffic table | Demo step 4 |
| 4.6 | Both downloads match the original SHA-256 after the failure | Demo step 5 |
| 4.7 | EC survives 2 dead nodes, not 3 | Demo step 6 |
| 4.8 | Scrubber finds the corrupted copy and repairs it (409 → 200) | Demo step 7 |
| 4.9 | Failure detection and repair in the logs | `docker compose logs metadata --since 15m \| Select-String "FAILED\|is back" -CaseSensitive`, then `docker compose logs repair-worker --since 15m \| Select-String "repaired chunk\|scrubbed" \| Select-Object -First 12` |
| 4.10 | Replication vs EC on both workloads | `.\.venv\Scripts\python.exe scripts\bench_redundancy.py` |
| 4.11 | All tests pass | `.\.venv\Scripts\python.exe -m pytest -v` |

**In the report, explain:**

- The quorum rule W + R > N.
- How Reed–Solomon k + m works: any k of the k + m pieces rebuild the data.
- The failure-detection timeout trade-off. Too short marks healthy but slow nodes as failed; too long leaves data unprotected for longer.
- The repair flowchart.
- Your comparison table, with a conclusion: EC uses half the space but needs more reads and more CPU to repair, and it is a poor fit for tiny files.

---

## Phase 5 – API gateway

**Goal:** an S3-like public API that hides chunks and nodes from the user.

**What was built:** [gateway/app.py](../gateway/app.py), a FastAPI service on port **8080**. Its interactive docs are at http://127.0.0.1:8080/docs (click **Authorize** and enter a key).

| Method | Path | Purpose |
|---|---|---|
| PUT | `/buckets/{bucket}?redundancy=replication\|ec` | Create a bucket |
| GET | `/buckets` | List buckets |
| PUT | `/buckets/{bucket}/objects/{key}` | Upload. The body is **streamed** through the chunker to the nodes, so any size works without filling memory. Headers: `X-Tags: type=video,project=lab`, `Content-Type`. `?chunking=cdc` for FastCDC. Same key again = new version |
| GET | `/buckets/{bucket}/objects/{key}` | Download (streamed). `Range: bytes=a-b`, `a-` or `-n` → **206** with `Content-Range`; past the end → **416**. `?version=N` |
| GET | `/buckets/{bucket}/objects/{key}?metadata=true` | Metadata as JSON, including the chunk list and where each chunk is stored |
| HEAD | `/buckets/{bucket}/objects/{key}` | Metadata in headers: `ETag` (SHA-256), `Last-Modified`, `X-Tags`, `X-Object-Version`, `X-Redundancy` |
| DELETE | `/buckets/{bucket}/objects/{key}` | Delete (all versions, or `?version=N`) |
| GET | `/buckets/{bucket}/objects?prefix=` | List objects |
| GET | `/search?bucket=&prefix=&min_size=&max_size=&after=&before=&tag=k=v` | Search; `tag` can be repeated |
| GET | `/cluster/status` | Nodes (status, free space, chunks per node), degraded objects, repair queue |
| GET | `/health` | Public health check |

- **Authentication:** every request except `/health` and the docs needs `X-API-Key`. Missing or wrong → **401**. Keys are set in `docker-compose.yml`:
  - `student-key-2026`: 200 requests/s;
  - `demo-key-2026`: 5 requests/s, to show the limit;
  - `bench-key-2026`: for the benchmark.
- **Rate limiting:** a token bucket per key, allowing 2 seconds of burst. Over the limit → **429** with `Retry-After`.
- **Every response** carries `X-Gateway-Id`, which Phase 7 uses to show the load balancer at work.
- **Tests:** [tests/test_gateway.py](../tests/test_gateway.py) (16 API tests, 48 in total) and a Postman collection, [tests/postman/lab-dest-gateway.postman_collection.json](../tests/postman/lab-dest-gateway.postman_collection.json) (14 requests with checks).
- **Demo:** [scripts/demo_phase5.ps1](../scripts/demo_phase5.ps1), every endpoint with curl, pausing after each step.
- **Workload benchmark:** [scripts/bench_workloads.py](../scripts/bench_workloads.py). Through the gateway, it uploads and downloads both workloads in both schemes with 8 parallel clients, checks every download against its SHA-256, and reports files/s, MB/s and latency.

**Measured** (`bench_workloads.py`, 8 parallel clients; all 10,010 downloads matched their SHA-256):

| Workload | Scheme | Upload | Download | Median latency (up / down) |
|---|---|---|---|---|
| Small: 5,000 files, 122 MB | Replication | 49 files/s | 91 files/s | 158 / 85 ms |
| Small | EC 4 + 2 | 47 files/s | 91 files/s | 159 / 84 ms |
| Large: 5 images, 923 MB | Replication | 98.9 MB/s | 133.5 MB/s | 9.5 / 7.1 s per file |
| Large | EC 4 + 2 | 92.3 MB/s | 109.0 MB/s | 10.2 / 8.7 s per file |

Small files are limited by requests (each upload means about 6 internal requests and a database commit). Large files are limited by bandwidth.

| Fig | Screenshot | How to show it |
|---|---|---|
| 5.1 | All endpoints, with the Authorize button | Browser: http://127.0.0.1:8080/docs |
| 5.2 | No or wrong key → 401 | Demo step 1 |
| 5.3 | Create and list buckets | Demo step 2 |
| 5.4 | Streamed upload of 216 MB (time, MB/s) into both schemes, plus small files with tags | Demo step 3 |
| 5.5 | Full download (SHA-256 matches); ranges → 206 and 416 | Demo step 4 |
| 5.6 | HEAD and metadata JSON | Demo step 5 |
| 5.7 | List by prefix; search by size, tag and date | Demo step 6 |
| 5.8 | Delete, then 404 | Demo step 7 |
| 5.9 | Cluster status | Demo step 8 |
| 5.10 | Rate limit → 429 with Retry-After | Demo step 9 |
| 5.11 | Trying an endpoint in Swagger (Authorize → upload or search → Execute) | Browser |
| 5.12 | Small vs large workload results | `.\.venv\Scripts\python.exe scripts\bench_workloads.py` (on an empty cluster) |
| 5.13 | All tests pass | `.\.venv\Scripts\python.exe -m pytest -v` |
| 5.14 (optional) | Postman collection run | Postman → Import the collection → Run |

**In the report, explain:**

- How the gateway turns one upload into chunks, placement and quorum writes. Refer to the write-path flowchart.
- How a range request reads only the chunks it needs.
- How authentication and rate limiting work.
- The workload results: small files are limited by the number of requests, large files by bandwidth.

---

## Phase 6 – Web UI + Elasticsearch

**Goal:** a browser interface for files and cluster health, and search backed by Elasticsearch.

**What was built:**

| Part | What it does |
|---|---|
| **React app** ([ui/src/](../ui/src/)), React 19 + Vite | Three pages: **Files**, **Search** and **Cluster**, with an API key box in the header. No UI library, just plain CSS with light and dark themes |
| **Files page** | Buckets list and "new bucket" (replication or EC). Drag-and-drop upload with folder, tags and chunking, and a progress bar per file. Object list with prefix filter. Download with progress, details (SHA-256, chunks, which nodes hold each chunk), delete |
| **Search page** | Free text (words, typos, any part of a key), bucket, prefix, size, date and tag filters. Facets (buckets, tags, content types) you can click. A switch to compare Elasticsearch with PostgreSQL |
| **Cluster page** | Refreshes every 2 s. Summary tiles (nodes, objects, degraded, unreadable, repair queue, free space, search index), a bar chart of chunks per node, a repair progress bar with repair traffic per scheme, a "Scrub now" button, node health (alive / no heartbeat / failed), chunk health per scheme |
| **Elasticsearch 9.2** | A single node in Docker, port 9200. The metadata service sends every commit and delete to the `objects` index in the background, in batches (bulk API), and rebuilds it from PostgreSQL if it's empty or behind ([metadata/search_index.py](../metadata/search_index.py)) |
| **nginx** ([ui/nginx.conf](../ui/nginx.conf)) | Serves the built app on port **5173** and forwards `/api/` to the gateway, with streaming for large uploads and downloads (no buffering, no size limit) |
| [scripts/seed_ui_data.py](../scripts/seed_ui_data.py), [scripts/demo_phase6.ps1](../scripts/demo_phase6.ps1) | Sample data for the screenshots (2,000 tagged files + 2 disk images), the Elasticsearch check, and a live node failure |
| `tests/test_search_es.py` | 8 search tests (56 in total) |

To work on the UI with live reload instead of Docker: `cd ui`, `npm install`, `npm run dev`. Vite forwards `/api` to the gateway the same way.

| Fig | Screenshot | How to show it |
|---|---|---|
| 6.1 | Files page: buckets and object list | http://127.0.0.1:5173 → Files → bucket `docs` |
| 6.2 | Uploading files with tags (progress bar) | Files → drop files → tags → Upload |
| 6.3 | Object details: SHA-256, chunks and their nodes | Files → **Details** on an object (try one in `archive` too, to see the fragments) |
| 6.4 | Downloading a file (progress, then the file in Downloads) | Files → **Download** |
| 6.5 | Free-text search with facets | Search → type `user` (or a typo like `ordr`) |
| 6.6 | Search with filters (size, tag, date) | Search → tags `type=disk-image`, min size `100000` KB |
| 6.7 | The metadata really is in Elasticsearch | Demo step 2 (`_count` + a direct query on port 9200) |
| 6.8 | Cluster page, healthy | Cluster |
| 6.9 | Cluster page during a failure: node "No heartbeat", then "Failed", repair progress moving | Demo step 3 kills storage-node-4 |
| 6.10 | Cluster page after the repair: 0 degraded | Cluster, when the repair finishes |
| 6.11 | All tests pass | `.\.venv\Scripts\python.exe -m pytest -v` |

**In the report, explain:**

- How the UI talks to the gateway.
- Why search uses Elasticsearch instead of PostgreSQL: full-text and combined filters stay fast on many objects.
- What the dashboard shows during a failure.

---

## Phase 7 – Bonus: load balancing and fault tolerance

**What was built:**

| Part | What it does |
|---|---|
| **nginx load balancer** ([deploy/nginx/lb.conf](../deploy/nginx/lb.conf)) | Port **8080** is now nginx, in front of `gateway-1` and `gateway-2` (round robin). A gateway that refuses a connection is skipped for 5 s and the request goes to the other one. `X-Gateway-Id` shows which gateway answered |
| **RabbitMQ** | Jobs are still recorded in PostgreSQL (state, bytes, progress). A dispatcher hands waiting jobs to four durable queues (`jobs.repair`, `jobs.rebalance`, `jobs.gc`, `jobs.scrub`). **Two worker replicas** take them, and a job is acknowledged only when it's finished, so a dying worker's job is delivered again. Management UI: http://127.0.0.1:15672 (`dos` / `dos`) |
| **Rebalancing** | When a node joins (or comes back after a failure), a plan job compares every chunk with the hash ring and queues a move for each misplaced chunk. The old copy is removed by a gc job |
| **Garbage collection** | Deleting an object queues a gc job for its blobs. An orphan gc lists each node's blobs older than 2 min and deletes those no chunk refers to. It runs when a node rejoins, after rebalancing, every 30 min, or with `cli.py gc`. A node refuses to delete a blob written seconds ago |
| **Replicated PostgreSQL** ([deploy/postgres/ha-entrypoint.sh](../deploy/postgres/ha-entrypoint.sh)) | `pg-1` and `pg-2`: a primary plus a streaming standby. If the primary doesn't answer for 10 s, the standby promotes itself. A node that finds the other one already primary rejoins as its standby (never two primaries). The services connect to whichever is primary (`target_session_attrs=read-write`) |
| **Prometheus + Grafana** ([deploy/prometheus/](../deploy/prometheus/), [deploy/grafana/](../deploy/grafana/)) | Every service has `/metrics`: requests, latency, bytes, disk per node, cluster gauges, jobs and repair traffic. Prometheus (http://127.0.0.1:9090) scrapes every 5 s. Grafana (http://127.0.0.1:3000) opens the 21-panel `lab-dest` dashboard directly |
| **Kubernetes** ([deploy/k8s/](../deploy/k8s/)) | Namespace `dos`. Storage nodes are a **StatefulSet** of 7 pods (stable names `storage-node-0…6`), each with its own 1 Gi persistent volume, plus a **liveness probe** (`/live`: restart if it hangs) and a **readiness probe** (`/ready`: no traffic unless its volume is writable). Also PostgreSQL, metadata, 2 gateways behind a Service, and 2 workers |
| [scripts/demo_phase7.ps1](../scripts/demo_phase7.ps1), [scripts/demo_phase7_k8s.ps1](../scripts/demo_phase7_k8s.ps1) | The Docker Compose demo and the Kubernetes demo, pausing after each step |

**Measured:**

| Test | Result |
|---|---|
| 40 requests while `gateway-1` is killed | 0 failed: 7 answered by gateway-1, then 33 by gateway-2 |
| Node killed: repair through RabbitMQ | 726 repairs split 366 / 360 between the two workers; repaired 19 s after the kill |
| Node back | Scrub, orphan gc and rebalancing ran on their own; 726 chunks moved back onto it |
| PostgreSQL primary killed | Standby promoted after 10 s; the upload in flight waited 15 s and succeeded (201); the old primary came back as a standby |
| `storage-node-8` added | 794 of 1,834 chunks moved (one copy each) in about 9 s; the new node holds about 14% of the blobs |
| Kubernetes: node frozen | Readiness took it out of service, liveness restarted it after about 50 s, and its blobs were kept |
| Kubernetes: pod deleted | Recreated in a few seconds with the same name, the same volume and the same blobs |

**Check it works:**

```powershell
docker compose down -v ; docker compose up -d --build
powershell -ExecutionPolicy Bypass -File scripts\demo_phase7.ps1
powershell -ExecutionPolicy Bypass -File scripts\demo_phase7_k8s.ps1
```

| Fig | Screenshot | How to show it |
|---|---|---|
| 7.1 | All the services running | Compose demo step 1 |
| 7.2 | Load balancing and a gateway failure with no failed request | Compose demo step 2 |
| 7.3 | RabbitMQ queues with messages and consumers | http://127.0.0.1:15672 → Queues, during demo step 3 |
| 7.4 | A node failure repaired by the two workers (repairs per worker) | Compose demo step 3 |
| 7.5 | The node rejoins: scrub, gc and rebalance jobs | Compose demo step 4 |
| 7.6 | PostgreSQL replication, primary killed, standby promoted | Compose demo step 5 |
| 7.7 | The old primary rejoins as a standby | Compose demo step 6 |
| 7.8 | A new node joins and rebalancing moves data onto it | Compose demo step 7 |
| 7.9 | Prometheus targets all UP | http://127.0.0.1:9090/targets |
| 7.10 | Grafana dashboard (best during demo steps 3 or 7) | http://127.0.0.1:3000 |
| 7.11 | Kubernetes: StatefulSet, Deployments, Services | Kubernetes demo step 1 |
| 7.12 | Kubernetes: 7 storage pods and their persistent volumes | Kubernetes demo step 2 |
| 7.13 | Kubernetes: upload through the Service; requests spread over 2 gateways | Kubernetes demo step 3 |
| 7.14 | Kubernetes: liveness probe restarts a frozen node | Kubernetes demo step 4 |
| 7.15 | Kubernetes: a deleted pod comes back with the same volume | Kubernetes demo step 5 |

---

## Phase 8 – Bonus: cloud deployment

**What gets built:**

- A Dockerfile for each service: storage node, metadata, gateway, repair worker, UI.
- Docker Compose for local testing.
- Kubernetes manifests in `deploy/k8s/`.
- A deployment on a cloud provider. **Azure for Students** gives free credit with a university email; check the current offer.
- **Auto-scaling** for the gateway (Horizontal Pod Autoscaler on CPU).
- A rebalancing demo: adding a storage node moves about 1/(n+1) of the chunks onto it.
- ⚠️ **Delete the cloud cluster as soon as your screenshots are done**, so it doesn't use up your credit.

| Fig | Screenshot | How to show it |
|---|---|---|
| 8.1 | Dockerfiles and built images | VS Code + `docker images` |
| 8.2 | Cluster in the cloud console | Azure / GCP / AWS portal |
| 8.3 | Everything running in the cloud | `kubectl get all` |
| 8.4 | App reachable on a public address | UI in the browser with the URL visible |
| 8.5 | Gateway auto-scaling under load | `kubectl get hpa -w` while a load script runs |
| 8.6 | New storage node triggers rebalancing | `kubectl scale statefulset storage-node --replicas=7`, then the rebalance logs and the new node's chunk count rising |

---

## Flowcharts (required in the report)

To turn a flowchart into an image for Word:

1. Copy everything inside its code block, without the ` ```mermaid ` line or the closing ` ``` `.
2. Paste it into https://mermaid.live.
3. Choose **Actions → PNG** and insert the image into Word.

GitHub also draws these diagrams automatically when you open this file on github.com.

### Write path

```mermaid
flowchart TD
    A([Client: PUT /buckets/b/objects/key]) --> B{Valid API key and<br/>under rate limit?}
    B -- No --> B1([401 Unauthorized / 429 Too Many Requests])
    B -- Yes --> C[Read the next chunk from the upload stream<br/>fixed 4 MB or FastCDC]
    C --> D[Chunk ID = SHA-256 of the chunk content]
    D --> E{Chunk already stored?<br/>ask metadata}
    E -- Yes: duplicate --> F[Reference count + 1<br/>no data is sent]
    E -- No --> G{Bucket redundancy}
    G -- Replication --> H[Hash ring picks N = 3 nodes<br/>send the chunk to all 3]
    G -- Erasure coding --> I[zfec encodes 4 data + 2 parity fragments<br/>hash ring picks 6 different nodes]
    H --> J{Enough nodes acknowledged?<br/>replication: W = 2 of 3<br/>EC: at least 5 of 6}
    I --> J
    J -- No --> X([503: upload fails, object is not committed<br/>orphan chunks are garbage-collected later])
    J -- Yes --> K[Save chunk locations<br/>reference count = 1]
    F --> L{More data in the stream?}
    K --> L
    L -- Yes --> C
    L -- No --> M[Metadata commits the object:<br/>bucket, key, version, size,<br/>ordered chunk list, tags, date]
    M --> N[Index the metadata in Elasticsearch]
    N --> O([200 OK + version])
```

### Read path

```mermaid
flowchart TD
    A([Client: GET /buckets/b/objects/key<br/>optional Range: bytes=start-end]) --> B{Valid API key and<br/>under rate limit?}
    B -- No --> B1([401 / 429])
    B -- Yes --> C{Object exists?<br/>ask metadata}
    C -- No --> C1([404 Not Found])
    C -- Yes --> D[Get the ordered chunk list<br/>keep only chunks that overlap the range]
    D --> E{Bucket redundancy}
    E -- Replication --> F[Read the chunk from R = 2 replicas in parallel]
    F --> G{A copy whose SHA-256<br/>matches the chunk ID?}
    G -- No --> H{Other replicas left?}
    H -- Yes --> H1[Read from the next replica] --> G
    H1 -.-> R[Report the bad or missing copy for repair]
    H -- No --> X([503: chunk unavailable<br/>object marked degraded])
    E -- Erasure coding --> I[Read fragments from the 6 nodes<br/>until any 4 valid ones arrive]
    I --> J{4 valid fragments?}
    J -- No --> X
    J -- Yes --> K[zfec decodes the chunk<br/>and its SHA-256 is checked]
    G -- Yes --> L[Cut the chunk to the requested range<br/>and stream it to the client]
    K --> L
    L --> M{More chunks?}
    M -- Yes --> E
    M -- No --> N([200 OK, or 206 Partial Content for a range])
```

### Failure and repair path (including scrubbing)

```mermaid
flowchart TD
    subgraph Detection
        A[Each storage node sends a heartbeat every 5 s<br/>node id, free space, chunk count] --> B[Metadata saves the last-seen time]
        B --> C{No heartbeat for 15 s?<br/>3 missed beats}
        C -- No --> A
        C -- Yes --> D[Mark the node FAILED<br/>remove it from the hash ring]
    end
    subgraph Scrubbing
        S1[Scrubber re-reads every chunk on a schedule] --> S2{SHA-256 still matches?}
        S2 -- Yes --> S1
        S2 -- No: bit rot --> S3[Create a repair job for this copy]
    end
    D --> E[Find every chunk or fragment<br/>that had a copy on the failed node]
    E --> F[Mark the affected objects DEGRADED<br/>queue one repair job per chunk]
    S3 --> G
    F --> G{Chunk redundancy}
    G -- Replication --> H[Read a healthy copy from a surviving replica]
    G -- Erasure coding --> I[Read any 4 healthy fragments<br/>rebuild the missing fragment]
    H --> J[Write it to the node the ring now assigns]
    I --> J
    J --> K[Update chunk locations in metadata]
    K --> L{All repair jobs done?}
    L -- No --> G
    L -- Yes --> M([Degraded objects = 0<br/>cluster healthy again])
```

> Update a flowchart if the final code ends up working differently from what it shows. The report must describe what the system really does.

---

## How to organize the Word report

### Formatting (set this up before you start writing)

- **Headings:** use the built-in styles from *Home → Styles*: **Heading 1** for chapters, **Heading 2** for sections. They are needed for step 2.
- **Table of contents:** *References → Table of Contents*. Right-click it → *Update Field* before you submit.
- **Figure captions:** right-click a picture → *Insert Caption* → label "Figure" → tick *Include chapter number*. Word numbers figures for you, for example "Figure 6.3". Then add *References → Insert Table of Figures* after the table of contents.
  - The figure numbers in this guide (3.7, 4.2, …) are only for naming your screenshot files. Let Word number the captions.
- **Pattern for every screenshot:**
  1. A paragraph **before** it: what you are doing and why.
  2. The screenshot at full page width (*Layout Options → In line with text*).
  3. The caption.
  4. One or two sentences **after** it: what the result proves.
- **Body text:** Calibri or Times New Roman 12 pt, 1.15 line spacing, justified.
- **Commands and code:** Consolas 10 pt, or *Insert → Text Box* with a light grey fill.
- **Benchmark numbers:** put them in Word tables (*Insert → Table*). Make bar charts with *Insert → Chart*, or paste the charts the benchmark scripts produce.
- **Page layout:** page numbers in the footer, and the course name in the header.

### Outline

| # | Chapter | Contents | Screenshots / figures |
|---|---|---|---|
| — | Cover page | University logo, faculty, course, project title, your name and ID, instructor, date | — |
| — | Table of contents + list of figures | Generated automatically | — |
| 1 | Introduction | The problem, the objectives (copy the list from the assignment), tools used | — |
| 2 | System architecture | Architecture diagram, services table, database tables, technology choices | Architecture diagram |
| 3 | System workflow | 3.1 Write path, 3.2 Read path, 3.3 Failure and repair path. Each flowchart with a paragraph that walks through it | The 3 flowcharts |
| 4 | Phase 1 – Environment | Objective, steps taken, screenshots, result | Figs 1.1–1.6 |
| 5 | Phase 2 – Storage node | Same structure | Figs 2.1–2.7 |
| 6 | Phase 3 – Metadata and placement | Same structure | Figs 3.1–3.10 |
| 7 | Phase 4 – Redundancy and recovery | Same structure | Figs 4.1–4.8 |
| 8 | Phase 5 – API gateway | Same structure | Figs 5.1–5.12 |
| 9 | Phase 6 – Web UI | Same structure | Figs 6.1–6.8 |
| 10 | Phase 7 – Load balancing and fault tolerance (bonus) | Same structure | Figs 7.1–7.9 |
| 11 | Phase 8 – Cloud deployment (bonus) | Same structure | Figs 8.1–8.6 |
| 12 | Workload evaluation | 12.1 The two datasets. 12.2 Fixed vs FastCDC. 12.3 Deduplication savings. 12.4 Data moved when adding or removing a node. 12.5 Replication vs EC: storage overhead and repair traffic. 12.6 Throughput and latency, small vs large files | Tables and charts from `benchmarks/results/` |
| 13 | Challenges and solutions | For example: the slow internet connection (downloads retried in the background); EC 4 + 2 needed 6 nodes | — |
| 14 | Conclusion | What works, what you learned, what you would improve | — |
| A | Appendix A – How to run | The setup and run commands | — |
| B | Appendix B – Repository | GitHub link and the list of delivered files | — |

**Each phase chapter uses the same 4 sections:**

1. **Objective:** one or two sentences, taken from the assignment.
2. **Implementation:** the paragraph explaining the steps you took. The assignment requires this.
3. **Testing and screenshots:** the figures, each with its before-and-after text.
4. **Results and observations:** numbers, problems you hit, how you solved them.

---

## Deliverables checklist

- [ ] Word report: full-screen screenshots and an explanatory paragraph for every phase, the 3 flowcharts, workload results for both workloads
- [ ] Source code: GitHub link, plus a zip of the repository
- [ ] Dockerfiles, `docker-compose.yml`, Kubernetes and Nginx/Prometheus configuration files
- [ ] Test scripts: pytest tests, the Postman collection, demo scripts
- [ ] Dataset generator script: deliver the script, not the gigabytes of generated data. It recreates the data
- [ ] Benchmark results: `benchmarks/results/*.csv` and the charts
