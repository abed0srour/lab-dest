# Chapter 8 – Phase 5: API Gateway

In Word, Phase 5 is **Chapter 8**, so its figures are **Figure 8.x**. Rewrite the paragraphs in your own words.

---

## 8.1 Objective

Until now only my own command-line tool could use the cluster. In this phase I built the public entrance to the system: an S3-like REST API that hides chunks, nodes and replicas from the user. It also controls who can use the storage (API keys) and how fast (rate limiting).

## 8.2 What I Did

**The gateway service.** This is a new FastAPI service on port 8080 that runs in its own container. It reuses the client library from Phases 3 and 4, so every upload goes through the same chunking, deduplication, placement and quorum writes, and every download through the same quorum reads and erasure decoding. FastAPI also generates interactive documentation at `/docs`, where every endpoint can be tried from the browser.

**Endpoints.** The API offers the following:
- creating and listing buckets, each with its own redundancy (replication or erasure coding);
- uploading an object with optional tags (`X-Tags` header) and a content type;
- downloading a whole object or only a byte range;
- getting an object's metadata, either as headers (HEAD) or as JSON;
- listing objects by key prefix, and deleting objects;
- searching by prefix, size, date and tags;
- a cluster status report with node health, free space, chunks per node and degraded objects.

**Streaming uploads.** A large file is never held in memory. The request body is fed into a small queue as it arrives, and a worker thread reads it from the queue, cuts it into 4 MB chunks and stores each chunk on its nodes straight away. Because the queue is bounded, the client can only send as fast as the cluster stores the data. I tested this with a 216 MB file and with bodies sent in many small pieces.

**Downloads and byte ranges.** Downloads are streamed chunk by chunk. For a `Range` request, the gateway uses the chunk offsets stored in the metadata to read only the chunks that overlap the range, then cuts out exactly the bytes asked for. It answers 206 Partial Content with a `Content-Range` header, or 416 if the range is past the end. Every response carries the object's SHA-256 as its ETag, so a client can check the data it received.

**Authentication and rate limiting.** Every request except the health check must carry an API key in the `X-API-Key` header, otherwise the answer is 401. Each key has its own token bucket: it refills at a fixed number of requests per second and allows a short burst. When it is empty, the gateway answers 429 Too Many Requests with a `Retry-After` header. I set up three keys: a normal one (200 requests per second), a slow demo key (5 per second, to show the limit) and a fast key for the benchmark.

**Search.** The metadata service gained a search query in PostgreSQL. It looks at the latest version of every object and filters by bucket, key prefix, minimum and maximum size, creation date, and any number of tags. Tags are stored as JSONB with an index, so "has these tags" is a single indexed containment check.

**Testing.** I wrote 16 automated API tests with pytest (48 tests in the whole project) and a Postman collection with 14 requests that check their own responses. A demo script calls every endpoint with curl.

**Problems I hit.** The benchmark sometimes failed with "server disconnected". The servers closed idle connections after 5 seconds, and a client reusing a connection at that exact moment lost its request. I raised the keep-alive time to 65 seconds and made the client retry once in that case. The retry is never applied to the final commit of an upload, which must not run twice.

---

## 8.3 Screenshots

Start from an empty cluster: `docker compose down -v`, then `docker compose up -d`. Wait about 30 seconds.

**Figure 8.1: Interactive API documentation of the gateway**
Open http://127.0.0.1:8080/docs in the browser.

**For Figures 8.2 to 8.10 run the demo; it stops after each step:**
`powershell -ExecutionPolicy Bypass -File scripts\demo_phase5.ps1`

**Figure 8.2: Requests without a valid API key are refused (401)** (step 1)

**Figure 8.3: Creating a replicated and an erasure-coded bucket, and listing the buckets** (step 2)

**Figure 8.4: Streaming a 216 MB file into both buckets, and uploading small files with tags** (step 3)

**Figure 8.5: Downloading the whole object (SHA-256 matches) and byte-range requests (206, 416)** (step 4)

**Figure 8.6: Object metadata with HEAD and as JSON** (step 5)

**Figure 8.7: Listing by prefix and searching by size, tag and date** (step 6)

**Figure 8.8: Deleting an object; reading it afterwards returns 404** (step 7)

**Figure 8.9: Cluster status: node health, free space, chunks per node and degraded objects** (step 8)

**Figure 8.10: Rate limiting: requests over the limit get 429 with Retry-After** (step 9)

**Figure 8.11: Trying the API from the browser**
In http://127.0.0.1:8080/docs click **Authorize**, enter `student-key-2026`, then open **GET /search**, click **Try it out**, type `type=disk-image` in the *tag* box and click **Execute**.

**Figure 8.12: Throughput and latency of both workloads through the gateway**
First `docker compose down -v` and `docker compose up -d` (an empty cluster), then run
`.\.venv\Scripts\python.exe scripts\bench_workloads.py` (a few minutes).

**Figure 8.13: All 48 tests passing**
Run: `.\.venv\Scripts\python.exe -m pytest -v`

**Figure 8.14 (optional): The Postman collection run**
In Postman: *Import* → `tests\postman\lab-dest-gateway.postman_collection.json` → *Run collection*.

---

## 8.4 Results

**Both workloads through the gateway** (`bench_workloads.py`, 8 parallel clients, empty cluster, everything running on one laptop):

| Workload | Scheme | Operation | Files | Time | Files/s | MB/s | Median latency | 95th percentile |
|---|---|---|---|---|---|---|---|---|
| Small (5,000 files, 121.8 MB) | Replication | upload | 5,000 | 101.4 s | 49.3 | 1.3 | 158 ms | 217 ms |
| | | download | 5,000 | 55.2 s | 90.6 | 2.3 | 85 ms | 120 ms |
| | EC 4 + 2 | upload | 5,000 | 106.5 s | 47.0 | 1.2 | 159 ms | 261 ms |
| | | download | 5,000 | 54.9 s | 91.1 | 2.3 | 84 ms | 118 ms |
| Large (5 images, 923 MB) | Replication | upload | 5 | 9.8 s | 0.5 | 98.9 | 9.5 s | 9.6 s |
| | | download | 5 | 7.3 s | 0.7 | 133.5 | 7.1 s | 7.2 s |
| | EC 4 + 2 | upload | 5 | 10.5 s | 0.5 | 92.3 | 10.2 s | 10.3 s |
| | | download | 5 | 8.9 s | 0.6 | 109.0 | 8.7 s | 8.8 s |

All 10,010 downloads matched their SHA-256 exactly.

**What this shows.** The two workloads hit completely different limits.

With **many small files**, the cost is per request, not per byte. Each 25 KB upload is one request to the gateway, but inside the cluster it becomes about six more: check the bucket, ask for a placement, store 3 replicas (6 fragments with EC) and commit to the database. So the cluster managed about 50 uploads and 90 downloads per second, and the data rate stayed tiny (1–2 MB/s).

With **large files**, the per-request overhead disappears and the limit becomes raw throughput: about 99 MB/s up and 134 MB/s down with replication. Replication actually writes three times the data to the nodes (about 300 MB/s inside the cluster). Erasure coding writes only 1.5 times the data, yet it was slightly slower (92 MB/s up, 109 MB/s down). It has to compute the parity, hash six fragments and send six requests per chunk, and on reads it fetches four fragments and joins them.

Every container shares the same laptop CPU and disk, so these numbers show where each workload's bottleneck is rather than the limits of real separate servers.

**Requirements check.**

| Task 5 requirement | Evidence |
|---|---|
| FastAPI gateway for user requests | Figures 8.1, 8.11 |
| Create and list buckets | Figure 8.3 |
| Upload (streaming) for large files | Figures 8.4, 8.12 |
| Download, including byte ranges | Figure 8.5 |
| List, delete and get object metadata | Figures 8.6, 8.7, 8.8 |
| Search by key prefix, size, date and user-defined tags | Figures 8.7, 8.11 |
| Cluster status (nodes, free space, chunks per node, degraded objects) | Figure 8.9 |
| Authentication (API keys) and rate limiting | Figures 8.2, 8.10 |
| Tests with pytest and Postman or curl | Figures 8.2–8.10 (curl), 8.13 (pytest), 8.14 (Postman) |
