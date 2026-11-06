# Chapter 7 – Phase 4: Redundancy and Recovery

In Word, Phase 4 is **Chapter 7**, so its figures are **Figure 7.x**. Rewrite the paragraphs in your own words.

---

## 7.1 Objective

The goal of this phase was to keep every file readable when storage nodes crash or disks silently corrupt data. That meant two ways of storing redundant data (replication and erasure coding), quorum writes and reads, automatic detection of failed nodes, automatic repair, and a scrubber that finds bit rot.

## 7.2 What I Did

**Quorum replication.** In a replicated bucket every chunk gets N = 3 copies on the next 3 nodes of the hash ring. The client sends the 3 copies in parallel, and the upload succeeds as soon as W = 2 of them are stored. A read asks R = 2 replicas at the same time: it downloads one and checks the other with a HEAD request. Since W + R = 4 is greater than N = 3, any 2 replicas I read always include at least one that confirmed the write. The metadata service refuses to start if the settings break this rule.

**Erasure coding.** For erasure-coded buckets I used Reed–Solomon coding from the zfec library. Each 4 MB chunk is cut into 4 data fragments of 1 MB, plus 2 parity fragments computed from them, and the 6 fragments go to 6 different nodes. Any 4 of the 6 are enough to rebuild the chunk. The redundancy scheme is chosen per bucket (`mb NAME --redundancy ec`).

**Seven storage nodes.** I added three more storage nodes (seven in total). Six are needed so that the 6 fragments of a chunk sit on 6 different machines. The seventh is a spare: after a failure, the missing fragment can be rebuilt on a node that doesn't already hold another fragment of the same chunk.

**Failure detection.** Nodes send a heartbeat every 5 seconds. Every 2 seconds the metadata service looks for nodes that have been silent for 15 seconds (3 missed beats), marks them FAILED, removes them from the ring, and queues one repair job for every chunk that had a copy on them. When a failed node comes back, it rejoins the ring and is scrubbed automatically.

**Repair worker.** This is a new service with 8 threads that takes jobs from a queue table in PostgreSQL. To repair a replicated chunk, it copies a healthy replica to the next node on the ring. To repair an erasure-coded chunk, it reads 4 surviving fragments, decodes the chunk, checks its SHA-256 and re-encodes the missing fragment. Every job records how many bytes it read and wrote, which is how I measured repair traffic.

**Scrubber.** Each storage node can re-hash every blob it stores. The repair worker asks every node to do this every 10 minutes, or right away with `cli.py scrub`. Any copy whose hash no longer matches is marked as bad and rebuilt from a healthy copy. Reads help too: if a client meets a missing or corrupted copy, it reports it and reads another one.

**One design decision.** A chunk is identified by its hash *and* its bucket's scheme. Without this, a file uploaded to an erasure-coded bucket could be deduplicated against a copy in a replicated bucket, and the bucket's chosen protection would be silently ignored.

**Problems I hit.**
- The demo download with 2 nodes down took 12.6 seconds for 24 MB: on Windows, every connection to a dead port takes about 2 seconds to fail. I made the client remember dead nodes for 30 seconds, which brought it down to 2.4 seconds.
- Some repairs were marked as permanently failed when a node was down but not yet detected. Now an unreachable node means "try again later", and a chunk is declared lost only when its data is really gone.

---

## 7.3 Screenshots

Start from an empty cluster: `docker compose down -v`, then `docker compose up -d --build`. Wait about 30 seconds.

**Figure 7.1: Seven storage nodes, PostgreSQL, the metadata service and the repair worker running**
Run: `docker compose ps`

**For Figures 7.2 to 7.8 run the demo. It stops after each step so you can take the screenshot, then press Enter:**
`powershell -ExecutionPolicy Bypass -File scripts\demo_phase4.ps1`

**Figure 7.2: Redundancy settings and health of the seven nodes** (step 1)

**Figure 7.3: The same 216 MB file stored with replication (3.00×) and erasure coding (1.50×), and the 6 fragments of each chunk on 6 different nodes** (step 2)

**Figure 7.4: Quorum write: the upload succeeds with one of the chunk's three nodes down and is refused with two down** (step 3)

**Figure 7.5: A killed node is marked FAILED after about 15 seconds and its data is rebuilt automatically** (step 4)

**Figure 7.6: Both copies of the file still match the original SHA-256 after the failure** (step 5)

**Figure 7.7: Erasure coding 4 + 2 survives two dead nodes but not three** (step 6)

**Figure 7.8: The scrubber detects a corrupted copy and repairs it** (step 7)

**Figure 7.9: Failure detection and repair in the service logs**
Run these two commands, one after the other:
```
docker compose logs metadata --since 15m | Select-String "FAILED|is back" -CaseSensitive
docker compose logs repair-worker --since 15m | Select-String "repaired chunk|scrubbed" | Select-Object -First 12
```

**Figure 7.10: Replication vs erasure coding on both workloads: storage overhead and repair traffic**
Run: `.\.venv\Scripts\python.exe scripts\bench_redundancy.py` (about 5 minutes; it kills one node and starts it again by itself)

**Figure 7.11: All 32 tests passing**
Run: `.\.venv\Scripts\python.exe -m pytest -v`

---

## 7.4 Results

**Demo (one 216 MB file in each scheme, one node killed).**

| | Replication (3 copies) | Erasure coding (4 + 2) |
|---|---|---|
| Stored on the nodes | 648 MB (3.00×), 162 blobs | 324 MB (1.50×), 324 blobs |
| Failure detected after | about 15 s | about 15 s |
| Chunks rebuilt | 22 | 53 |
| Read / written by the repair | 88 MB / 88 MB (1×) | 212 MB / 53 MB (4×) |
| Repair time | about 2 s | about 2 s |
| Dead nodes survived | 2 | 2 |

**Benchmark on both workloads** (`bench_redundancy.py`; storage-node-1 was killed while holding 6,203 blobs).

| Workload | Scheme | Unique data | Stored | Overhead | Blobs | Chunks rebuilt | Repair read | Repair written | Read / written |
|---|---|---|---|---|---|---|---|---|---|
| Small (5,000 files) | Replication | 108.3 MB | 324.8 MB | 3.0× | 13,383 | 2,027 | 49.2 MB | 49.2 MB | 1.0× |
| Small (5,000 files) | EC 4 + 2 | 108.3 MB | 162.4 MB | 1.5× | 26,766 | 3,939 | 95.3 MB | 23.8 MB | 4.0× |
| Large (4 disk images) | Replication | 720 MB | 2,160 MB | 3.0× | 546 | 81 | 319 MB | 319 MB | 1.0× |
| Large (4 disk images) | EC 4 + 2 | 720 MB | 1,080 MB | 1.5× | 1,092 | 156 | 619 MB | 154.8 MB | 4.0× |

The node was marked FAILED 13.6 s after it was killed. The whole cluster (about 6,180 repair jobs) was back to full redundancy 104 s after the kill.

**What this shows.** On both workloads, erasure coding stored the same data in exactly half the space of replication (1.5× instead of 3×), and it still survives two failed nodes. It pays for this at repair time. To rebuild a 1 MB fragment it has to read four fragments (4 MB), while replication reads exactly what it writes. EC also had about twice as many chunks to repair (3,939 vs 2,027 on the small files). That's because an EC chunk is spread over 6 of the 7 nodes, so 6 out of 7 chunks lose a fragment when a node dies, against only 3 out of 7 for replication. Altogether, the EC repair read about twice as many bytes as the replication repair (95 vs 49 MB, and 619 vs 319 MB), even though it wrote half as much.

The small files show the other cost of erasure coding: every 20 KB file becomes six 5 KB blobs, so the cluster held twice as many blobs (26,766 vs 13,383) and the repair needed many more small requests. Most of the 104 seconds went into those thousands of tiny small-file repairs. The large files, with only a few hundred 4 MB chunks, were repaired quickly. My conclusion: replication is the better fit for many small files, and erasure coding is the better fit for large files, where the space saving matters and the repair reads are large sequential transfers.

**Requirements check.**

| Task 4 requirement | Evidence |
|---|---|
| Replication with configurable N (default 3), quorum writes and reads with W + R > N | Figures 7.2, 7.4 |
| Erasure coding (Reed–Solomon 4 + 2, zfec), selectable per bucket | Figures 7.3, 7.7 |
| Node marked failed after missed heartbeats; repair job rebuilds its chunks on the remaining nodes | Figures 7.5, 7.6, 7.9 |
| Background scrubber that detects bit rot and repairs from a healthy copy | Figure 7.8 |
| Storage overhead and repair traffic, replication vs erasure coding, on both workloads | Figure 7.10 |
