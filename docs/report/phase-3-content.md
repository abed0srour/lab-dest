# Chapter 6 – Phase 3: Metadata and Placement

In Word, Phase 3 is **Chapter 6**, so its figures are **Figure 6.x**. Rewrite the paragraphs in your own words.

---

## 6.1 Objective

In this phase I built the "brain" of the system. The metadata service remembers which chunks make up each file, decides which storage nodes keep each chunk using consistent hashing, and makes sure identical chunks are stored only once.

## 6.2 What I Did

**Chunking.** I wrote two ways to split a file into chunks. *Fixed-size* chunking simply cuts every 4 MB. *Content-defined* chunking (FastCDC) runs a rolling hash over the bytes and cuts wherever the content matches a pattern, so the cut points move with the data. I used a minimum of 1 MB, an average of 4 MB and a maximum of 16 MB.

**Metadata service.** This is a new FastAPI service on port 9000, backed by PostgreSQL. It has six tables: `buckets`, `objects`, `object_chunks`, `chunks`, `chunk_locations` and `nodes`. For every object it stores the bucket, key, version, size, tags, creation date and the ordered list of chunk hashes. It also receives the heartbeats the storage nodes have been sending since Phase 2.

**Placement.** I implemented a consistent hashing ring where each storage node appears 128 times (virtual nodes). Each chunk is stored on the next 3 different nodes clockwise from its hash.

**Deduplication.** Before uploading, the client asks the metadata service which chunks already exist. Those chunks are not sent again; only their reference count goes up. When an object is deleted the counts go down, and a chunk is removed from the nodes only when its count reaches zero.

**Datasets and tools.** I generated the two workloads:
- small: 5,000 text and JSON files, 121.8 MB, of which 539 are duplicates;
- large: 10 disk images, 1.8 GB, where half of them are new versions of the other half.

I also wrote a command-line tool (`scripts/cli.py`) to upload and download files, two benchmark scripts, and 15 new tests.

**A problem I hit.** At first, uploads were slow (33 files/s) because every request took about 48 ms. On Windows, `localhost` goes to IPv6 first, and Docker Desktop's IPv6 forwarding adds about 40 ms per request. Switching to `127.0.0.1` raised the upload speed to 109 files/s.

---

## 6.3 Screenshots

Make sure the cluster is running first: `docker compose up -d`. Take the screenshots in this order.

**Figure 6.1: PostgreSQL and the metadata service running with the four storage nodes**
Run: `docker compose ps`

**Figure 6.2: Heartbeats received from all four storage nodes**
Run: `.\.venv\Scripts\python.exe scripts\cli.py nodes`
*(This also covers the heartbeat requirement of Phase 2. In Chapter 5 you can write "see Figure 6.2".)*

**Figure 6.3: Tables of the metadata database**
Run: `docker compose exec postgres psql -U dos -d dos -c "\dt"`

**Figure 6.4: Generating the two test workloads**
Run: `.\.venv\Scripts\python.exe scripts\generate_datasets.py` (takes about 3 minutes; it recreates exactly the same files)

**Run the demo script once, then scroll up to each step for Figures 6.5 to 6.8:**
`powershell -ExecutionPolicy Bypass -File scripts\demo_phase3.ps1`

**Figure 6.5: Uploading and downloading a 194 MB file; the SHA-256 hashes match**
Demo step 3.

**Figure 6.6: Ordered chunk list of an object and the three nodes storing each chunk**
Demo step 4 (the top part of the table is enough).

**Figure 6.7: Deduplication and reference counting**
Demo step 5: refcount 2 → 1 → chunk removed from 3 nodes.

**Figure 6.8: Fixed-size vs FastCDC on a file and its new version**
Demo step 6.

**Figure 6.9: Chunking comparison on both workloads, and the edit test**
Run: `.\.venv\Scripts\python.exe scripts\bench_chunking.py`

**Figure 6.10: Data moved when a node is added or removed, and load balance by number of virtual nodes**
Run: `.\.venv\Scripts\python.exe scripts\bench_placement.py`

**Figure 6.11: Uploading the 5,000 small files**
Run these three commands:
```
.\.venv\Scripts\python.exe scripts\cli.py mb small
.\.venv\Scripts\python.exe scripts\cli.py put-dir small datasets\small
.\.venv\Scripts\python.exe scripts\cli.py stats --bucket small
```

**Figure 6.12: Chunks spread across the four storage nodes**
Run: `.\.venv\Scripts\python.exe scripts\cli.py nodes`

**Figure 6.13: All 22 tests passing**
Run: `.\.venv\Scripts\python.exe -m pytest -v`

---

## 6.4 Results

**Chunking.**

| Test | Fixed 4 MB | FastCDC |
|---|---|---|
| Space saved on the large workload | 5.2% | 38.7% |
| Space saved on the small workload | 11.1% | 11.1% |
| Chunks changed after inserting 10 bytes into a 109 MB file | 15 of 28 (57 MB) | 1 of 29 (1.8 MB) |

With fixed-size chunks, inserting a few bytes shifts every cut point after it, so almost half the file looks new. FastCDC finds the same cut points again right after the change, so only one chunk is different. On the small files both methods give the same result, because each file is smaller than one chunk. There, deduplication only finds exact copies: the 539 duplicates, or 11.1% of the space.

**Placement.**

| | Consistent hashing ring | Modulo (hash mod n) | Ideal |
|---|---|---|---|
| Copies moved when adding a node (4 → 5) | 20.1% | 39.8% | 20% |
| Copies moved when removing a node (5 → 4) | 18.2% | 40.4% | 20% |
| Chunks whose main node changed (adding) | 20.8% | 79.3% | 20% |

The ring moves almost exactly the minimum: only the new node's fair share, about one fifth of the data. Modulo placement moves twice as many copies and changes the main node of about 80% of the chunks. Virtual nodes keep the load even. With 1 point per node the busiest node held 27% more than average; with 128 points it held only 4.7% more.

**Requirements check.**

| Task 3 requirement | Evidence |
|---|---|
| Fixed-size chunking, then FastCDC for comparison | Figures 6.8, 6.9 |
| Object metadata (bucket, key, size, version, ordered chunk list, date) in PostgreSQL | Figures 6.3, 6.6 |
| Consistent hashing ring with virtual nodes | Figures 6.6, 6.10, 6.12 |
| Deduplication with reference counting | Figures 6.7, 6.11 |
| Percentage of chunks that move when a node is added or removed | Figure 6.10 |
