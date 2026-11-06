# Report content – Chapter 1 (Introduction), Chapter 4 (Phase 1), Chapter 5 (Phase 2)

**How to use this file.** The blocks below go into your Word document in this order.

- **H1 / H2 / H3** tells you which Word heading style to use: Heading 1, 2 or 3.
- Text in a **quote block** (lines starting with `>`) is draft paragraph text. Rewrite it in your own words.
- A **📷 box** means: insert that screenshot here, then add the caption it gives. Inside each box, *Before* is the text to put above the picture and *After* is the text to put below it.
- **Tables:** recreate them in Word with *Insert → Table*. Or select a table in VS Code's Markdown preview (`Ctrl+Shift+V`), copy it, and paste it into Word.
- **Commands and code:** use Consolas 10 pt.

Figure numbers are the ones Word gives when captions include the chapter number: Phase 1 is Chapter 4, so its figures are Figure 4.x.

---

## H1: 1 Introduction

### H2: 1.1 Background

> Modern applications store very large amounts of data, such as documents, images, videos and backups. A single disk or server cannot hold all of it, and it cannot guarantee that the data will never be lost. Distributed storage systems solve this problem by spreading data over many machines, called nodes, and by keeping redundant information, so the system keeps working even when some machines fail. Well-known examples are Amazon S3, Ceph and MinIO.

### H2: 1.2 Project Goal

> The goal of this project is to design and build a distributed, fault-tolerant object storage framework. Files uploaded by users are split into chunks. Each chunk is identified by the SHA-256 hash of its content, and the chunks are distributed across several storage nodes using consistent hashing. Replication and erasure coding keep the data available when nodes fail, and failed or corrupted data is detected and repaired automatically. Users access the system through an S3-like REST API and a web interface.

### H2: 1.3 Objectives

Bullet list, copied from the assignment:

- Implement a scalable storage layer distributed over at least 4 storage nodes.
- Implement data placement using consistent hashing, so that adding or removing a node moves only a fraction of the data.
- Ensure fault tolerance through replication and erasure coding, with automatic detection of failed nodes and repair of lost chunks.
- Ensure data integrity through content addressing (SHA-256) and periodic scrubbing.
- Reduce storage cost through chunk-level deduplication.
- Provide APIs for storing, retrieving, listing and searching objects.

### H2: 1.4 Technologies Used

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.12.10 | Programming language of all the services |
| FastAPI / Uvicorn | 0.162 / 0.54 | REST API framework / web server |
| Docker / Docker Compose | 29.8 / 5.5 | Runs every service and storage node in its own container |
| Kubernetes (Docker Desktop, kind) | 1.36.1 | Container orchestration (bonus phases) |
| Git / GitHub | 2.49 | Version control and code hosting |
| pytest | 9.2 | Automated tests |
| PostgreSQL | (Phase 3) | Metadata store |
| zfec | (Phase 4) | Reed–Solomon erasure coding |
| Node.js / React | 24.11 / (Phase 6) | Web user interface |
| Elasticsearch | (Phase 6) | Metadata search |

Fill in the "(Phase N)" versions when you reach those phases.

### H2: 1.5 Report Organization

> Chapter 2 describes the architecture of the system and Chapter 3 its main workflows: the write path, the read path, and the failure and repair path. Chapters 4 to 11 describe the eight implementation phases. Each of these chapters gives the objective, the steps taken, the screenshots that prove the result, and the observations. Chapter 12 evaluates the system on two workloads, many small files and a few large files. Chapter 13 discusses the challenges met during the project, and Chapter 14 concludes.

---

## H1: 4 Phase 1 – Environment Setup

### H2: 4.1 Objective

> The objective of this phase is to prepare the development environment. This means installing and verifying all the required tools, creating a Python virtual environment, setting up version control with Git and GitHub, installing Docker and Kubernetes, and giving every storage node its own separate data volume.

### H2: 4.2 Implementation

#### H3: 4.2.1 Development Machine

| Item | Value |
|---|---|
| Operating system | Windows 11 Pro for Workstations |
| Processor | Intel Xeon W-10885M @ 2.40 GHz |
| Memory | 32 GB RAM |
| Free disk space | 48 GB (drive C:) |

> All the work was done on a single Windows 11 machine. With 32 GB of RAM it can run all the storage nodes, PostgreSQL and Elasticsearch as containers at the same time. Free disk space is limited to 48 GB, so the large-file workload uses files of 100–300 MB instead of 1 GB. Stored three times, 1 GB files would almost fill the disk.

#### H3: 4.2.2 Python and Virtual Environment

> Python 3.12.10 was already installed. While checking it, I found that the `python` command pointed to a broken Python from Cygwin, which appears earlier in the system PATH. To avoid this problem, the official Python launcher `py` was used to select the correct version explicitly, and a virtual environment was created inside the project folder:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

> A virtual environment keeps the project's libraries separate from the rest of the system, so different projects cannot conflict. Anyone can also recreate exactly the same environment from the `requirements-dev.txt` file. The installed libraries are FastAPI (web framework), Uvicorn (web server), HTTPX (HTTP client, used for heartbeats and tests) and pytest (testing).

> The first installation attempt failed with a *ResolutionImpossible* error. Analysis showed that the real cause was a download interrupted by a very slow and unstable internet connection, measured at about 10 KB/s at that time, and not a real version conflict. The installation succeeded after retrying with `pip install --retries 10 --timeout 60 -r requirements-dev.txt`.

#### H3: 4.2.3 Version Control with Git and GitHub

> Git 2.49 was used for version control. The repository was initialized with `git init -b main`. A `.gitignore` file excludes the virtual environment, Python cache files, the local chunk data and the generated datasets. These can all be recreated by scripts, and they are too large for Git. The repository is published on GitHub at https://github.com/abed0srour/lab-dest. Each phase is committed separately, so the history shows the progress of the project.

#### H3: 4.2.4 Docker and Kubernetes

> Docker Desktop 4.91 (Docker Engine 29.8, Docker Compose 5.5) was installed. Docker runs each storage node in its own isolated container, which behaves like a small separate machine with its own file system and network address. Kubernetes was enabled from Docker Desktop (*Settings → Kubernetes → Enable Kubernetes*). Docker Desktop created a single-node cluster in *kind* mode ("Kubernetes in Docker") running Kubernetes v1.36.1, with one node named `desktop-control-plane`. Because of the slow connection, downloading the Kubernetes images took about 35 minutes. Kubernetes is used in the bonus phases (Phases 7 and 8).

#### H3: 4.2.5 Separate Storage for Each Node

> In `docker-compose.yml`, four named Docker volumes were declared (`node1-data` to `node4-data`). Each one is mounted at `/data` inside exactly one storage node container. A Docker volume is storage managed by Docker that lives outside the container, which has two consequences. First, the data survives when the container is restarted or rebuilt. Second, each node sees only its own data, just as real servers each have their own disk.

```yaml
storage-node-1:
  volumes: [node1-data:/data]
storage-node-2:
  volumes: [node2-data:/data]
# ... same for nodes 3 and 4
volumes:
  node1-data:
  node2-data:
  node3-data:
  node4-data:
```

### H2: 4.3 Testing and Screenshots

> 📷 **Figure 4.1: Versions of the installed development tools**
> Run: `py -3.12 --version; git --version; docker --version; kubectl version --client; node --version`
> Must be visible: all five version lines.
> *Before:* "The first step was to verify that every required tool is installed and reachable from the terminal."
> *After:* "The output confirms Python 3.12.10, Git 2.49.0, Docker 29.8.0, kubectl v1.36.1 and Node.js v24.11.1."

> 📷 **Figure 4.2: Activated virtual environment with the installed Python packages**
> Run: `.\.venv\Scripts\Activate.ps1`, then `pip list`. If activation is blocked, run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.
> Must be visible: the `(.venv)` prefix in the prompt, and fastapi, uvicorn, httpx and pytest in the list.
> *Before:* "Next, the virtual environment was activated and the installed packages were listed."
> *After:* "The `(.venv)` prefix shows that the virtual environment is active. The list contains fastapi 0.162.0, uvicorn 0.54.0, httpx 0.28.1 and pytest 9.2.0."

> 📷 **Figure 4.3: Project repository on GitHub**
> Open https://github.com/abed0srour/lab-dest in the browser.
> Must be visible: the address bar, the repository name, the file list and the last commit message.
> *Before:* "The project was then published on GitHub."
> *After:* "The repository contains the source code, Docker files, tests, scripts and documentation, and it is updated after every phase."

> 📷 **Figure 4.4: Local Kubernetes cluster running in Docker Desktop**
> Put Docker Desktop's Kubernetes page and a terminal side by side (`Win + ←` and `Win + →`). In the terminal, run `kubectl get nodes`.
> Must be visible: the cluster shown as running in Docker Desktop, and `desktop-control-plane   Ready` in the terminal.
> *Before:* "Kubernetes was enabled in Docker Desktop and the cluster status was checked."
> *After:* "The node `desktop-control-plane` is in the Ready state, so the cluster can run workloads."

> 📷 **Figure 4.5: One Docker volume per storage node**
> Run: `docker volume ls --filter name=lab-dest`
> Must be visible: `lab-dest_node1-data` to `lab-dest_node4-data`.
> *Before:* "The separate volumes created for the storage nodes were listed."
> *After:* "Docker adds the project name (`lab-dest_`) in front of each volume name. There are four volumes, one per storage node."

> 📷 **Figure 4.6: Project folder structure**
> In VS Code's Explorer panel, expand `storage_node`, `tests`, `scripts` and `docs`.
> *Before:* "The project is organized into folders by component."
> *After:* "Each service has its own folder. Tests, scripts and documentation are kept at the top level."

Below Figure 4.6, add this table:

| Folder / file | Contents |
|---|---|
| `storage_node/` | Storage node service and its Dockerfile |
| `tests/` | Automated pytest tests |
| `scripts/` | Demo and benchmark scripts |
| `docs/` | Lab guide and report material |
| `docker-compose.yml` | Definition of all containers and volumes |
| `requirements-dev.txt` | Python packages for development |

### H2: 4.4 Results and Observations

> At the end of this phase, all the tools were installed and verified, the repository was published on GitHub, the Kubernetes cluster was running, and a separate volume was defined for every storage node. The main difficulty was the slow and unstable internet connection. The table below summarizes the problems met and how they were solved.

| Problem | Cause | Solution |
|---|---|---|
| The `python` command did not work | A broken Cygwin Python came first in the PATH | Used the `py -3.12` launcher and a virtual environment |
| `pip install` failed with *ResolutionImpossible* | Downloads interrupted by the unstable connection | Retried with `--retries 10 --timeout 60` |
| Docker image build failed (*short read: unexpected EOF*) | The base image download was cut off | Ran `docker compose up --build` again on a better connection |
| Kubernetes took about 35 minutes to start | Several hundred MB of images over a slow connection | Waited for the download to finish instead of resetting the cluster |

---

## H1: 5 Phase 2 – Storage Node Module

### H2: 5.1 Objective

> The objective of this phase is to build the storage node, the component that actually stores the data on disk. It must offer PUT, GET, DELETE and HEAD operations on chunks, address each chunk by the SHA-256 hash of its content, verify the hash on every read, report its state to the metadata service through heartbeats, and run as at least four instances in separate containers.

### H2: 5.2 Implementation

#### H3: 5.2.1 Content Addressing

> A chunk is a piece of a file. Instead of an arbitrary name, each chunk is named by the SHA-256 hash of its content, written as 64 hexadecimal characters. This has three advantages:
> 1. **Integrity:** changing even a single bit gives a completely different hash, so a node can detect corruption by recomputing the hash.
> 2. **Deduplication:** identical chunks always get the same name, so each one needs to be stored only once (Phase 3).
> 3. **No name conflicts:** two different chunks practically never get the same name.
>
> The node accepts only names that match the pattern `^[0-9a-f]{64}$` and rejects anything else with status 422. This also blocks malicious names such as `../../etc/passwd`.

#### H3: 5.2.2 Storage Node API

> The service is written with FastAPI, which also generates interactive documentation automatically at `/docs` (Figure 5.2). The node offers the following endpoints:

| Method | Path | Description | Responses |
|---|---|---|---|
| PUT | `/chunks/{hash}` | Store a chunk | 201 created · 200 already stored · 400 hash mismatch |
| GET | `/chunks/{hash}` | Read a chunk (hash verified) | 200 · 404 not found · 409 corrupted |
| HEAD | `/chunks/{hash}` | Check existence and size | 200 with `Content-Length` · 404 |
| DELETE | `/chunks/{hash}` | Delete a chunk | 204 · 404 |
| GET | `/chunks` | List every chunk stored on the node | 200 |
| GET | `/health` | Node id, free space, chunk count | 200 |

#### H3: 5.2.3 Writing a Chunk

> When a chunk arrives, the node:
> 1. computes the SHA-256 of the received bytes;
> 2. compares it with the name in the URL, and returns **400** on a mismatch, which protects against data damaged in transit or sent under the wrong name;
> 3. writes the data to a temporary file and forces it to the physical disk (`fsync`);
> 4. renames the temporary file to its final location.
>
> The rename is **atomic**: other readers see either the old state or the complete new file, never a half-written chunk, even if the container crashes during the write. Writing the same chunk a second time simply replaces the file with identical content. This is also how a corrupted chunk is repaired.

#### H3: 5.2.4 Reading a Chunk and Detecting Corruption

> On every GET, the node reads the file and recomputes its SHA-256. If the hash no longer matches the name, the data was corrupted on disk. One example is *bit rot*, the slow, silent corruption of stored data caused by hardware faults. In that case the node does **not** return the data. It answers **409 Conflict** and writes an error to its log. In Phase 4, the scrubber and the repair worker use this response to find and repair corrupted copies.

#### H3: 5.2.5 On-Disk Layout

> Chunks are stored as `/data/chunks/<first 2 characters>/<next 2 characters>/<full hash>`. For example, the chunk `64e07fc8…` is stored at `/data/chunks/64/e0/64e07fc8…`. This spreads the chunks over up to 65,536 sub-folders, so no single folder holds a huge number of files, which would slow down the file system. Temporary files are written to `/data/tmp`.

#### H3: 5.2.6 Heartbeats

> A background task in every node sends a heartbeat to the metadata service (`POST /heartbeat`) every 5 seconds. The interval can be changed with the `HEARTBEAT_INTERVAL` setting. A heartbeat looks like this:

```json
{"node_id": "storage-node-1", "url": "http://storage-node-1:8000",
 "free_bytes": 1019687440384, "chunk_count": 0}
```

> The metadata service uses heartbeats to know which nodes are alive. It marks a node as failed after missing heartbeats (Phase 4). The metadata service is built in Phase 3, so until then each node logs a warning that its heartbeats are failing. The node logs only when heartbeats change between working and failing, so the log does not fill up with repeated messages.

#### H3: 5.2.7 Containerization

> The Dockerfile starts from the official `python:3.12-slim` image, installs the requirements, copies the code, declares `/data` as a volume, and starts Uvicorn on port 8000. `docker-compose.yml` defines four services, `storage-node-1` to `storage-node-4`, built from the same image. Each has its own `NODE_ID`, its own volume and its own port on the host. Docker calls `/health` every 10 seconds and marks a container *unhealthy* after 3 failures. The `restart: unless-stopped` policy restarts a crashed node automatically.

| Node | Host port | Volume |
|---|---|---|
| storage-node-1 | 8001 | lab-dest_node1-data |
| storage-node-2 | 8002 | lab-dest_node2-data |
| storage-node-3 | 8003 | lab-dest_node3-data |
| storage-node-4 | 8004 | lab-dest_node4-data |

#### H3: 5.2.8 Automated Tests

> Seven automated tests (`tests/test_storage_node.py`) check the node's behaviour. They use FastAPI's test client with a temporary folder, so they run without Docker.

| Test | What it checks |
|---|---|
| put_then_get_returns_same_bytes | A stored chunk is read back identically |
| put_same_chunk_twice_stores_it_once | The second PUT returns 200 and the chunk is stored only once |
| put_rejects_content_that_does_not_match_hash | Wrong content for the name is rejected with 400 |
| malformed_hash_is_rejected | A name that is not a valid SHA-256 is rejected with 422 |
| head_and_delete | HEAD gives the size; DELETE returns 204; afterwards 404 |
| corrupted_chunk_is_rejected_on_read_and_repaired_by_put | A chunk corrupted on disk gives 409; a new PUT repairs it |
| health_and_list_report_stored_chunks | `/health` and `/chunks` report the stored chunks |

### H2: 5.3 Testing and Screenshots

Start the nodes first with `docker compose up -d`.

> 📷 **Figure 5.1: Storage node source code (PUT and GET endpoints)**
> Open `storage_node/app.py` in VS Code and scroll to line 127, so that `put_chunk` and `get_chunk` are visible.
> *Before:* "The storage node is implemented in a single FastAPI module."
> *After:* "Both endpoints compute the SHA-256 of the data: PUT rejects a mismatch with 400, and GET refuses to return a corrupted chunk with 409."

> 📷 **Figure 5.2: Automatically generated API documentation of a storage node**
> Open http://localhost:8001/docs in the browser.
> Must be visible: all six endpoints.
> *Before:* "FastAPI generates interactive documentation for the node's API."
> *After:* "The page lists every endpoint of the node, and each one can be tested directly from the browser."

> 📷 **Figure 5.3: Four storage node containers running and healthy**
> Run: `docker compose ps`
> *Before:* "The four storage nodes were started with Docker Compose."
> *After:* "All four containers are *Up (healthy)*, mapped to host ports 8001 to 8004."

> 📷 **Figure 5.4: Storing, reading and checking a chunk with curl**
> Run: `powershell -ExecutionPolicy Bypass -File scripts\demo_storage_node.ps1`. Screenshot the **top** part of the output.
> *Before:* "A demo script then tested the API with real requests."
> *After:* "PUT stored the chunk (201, `created: true`). GET returned the same text (200). HEAD reported its size. Content that did not match the hash was rejected (400)."

> 📷 **Figure 5.5: Corrupted chunk detected and rejected, then repaired**
> Scroll down in the same output to the **bit rot** part, then also run `docker compose logs storage-node-1 --tail 8`.
> Must be visible: the 409, the `ERROR:storage_node:corrupted chunk` log line, the repair (200), and the delete (204 → 404).
> *Before:* "To simulate disk corruption, the chunk file was overwritten directly on the node's disk."
> *After:* "The next GET returned 409 and the node logged an error, so the corrupted data was never served. Storing the original content again repaired the chunk. DELETE then removed it, and HEAD returned 404."

> 📷 **Figure 5.6: All storage node tests passing**
> Run: `.\.venv\Scripts\python.exe -m pytest -v`
> *Before:* "Finally, the automated tests were run."
> *After:* "All 7 tests passed. The single warning is a deprecation notice from the testing library, not from the project's code."

### H2: 5.4 Results and Observations

| Test | Expected | Result |
|---|---|---|
| Store a new chunk | 201 Created | ✅ |
| Read it back | 200, same content | ✅ |
| HEAD request | 200, correct size | ✅ |
| Store with the wrong hash | 400 rejected | ✅ |
| Read after corrupting the file on disk | 409 rejected | ✅ |
| Store the original again (repair) | 200, readable again | ✅ |
| Delete, then HEAD | 204, then 404 | ✅ |
| Automated tests | 7 passed | ✅ |

The table below shows how each requirement of Task 2 is met:

| Task 2 requirement | How it is met | Evidence |
|---|---|---|
| FastAPI service with PUT, GET, DELETE, HEAD | `storage_node/app.py` | Figures 5.2, 5.4 |
| Chunks addressed by SHA-256 and stored on local disk | Name = hash, stored in the node's own volume | Figures 5.4, 4.5 |
| Hash verified on every read, corrupted chunks rejected | 409 on mismatch | Figure 5.5 |
| Heartbeat every few seconds (id, free space, chunk count) | Background task every 5 s | Chapter 6 (Phase 3), once the metadata service receives them |
| At least 4 nodes in separate containers | 4 containers, each with its own volume | Figures 5.3, 4.5 |

> **Observations:** every node reports about 950 GB of free space. This is the size of Docker Desktop's virtual disk, which all the containers share on this single machine, and not the real free space of drive C:. In a real deployment, each node would report its own disk. Each chunk is read fully into memory, which is fine because chunks are at most a few megabytes (4 MB in Phase 3).
