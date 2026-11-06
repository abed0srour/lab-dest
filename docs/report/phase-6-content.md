# Chapter 9 – Phase 6: Web UI and Search

In Word, Phase 6 is **Chapter 9**, so its figures are **Figure 9.x**. Rewrite the paragraphs in your own words.

---

## 9.1 Objective

In this phase I gave the storage system a web interface. People can upload, download and browse their files in the browser, search them with Elasticsearch, and watch the health of the cluster: which nodes are alive, how the chunks are spread, and how a repair is progressing.

## 9.2 What I Did

**The web app.** I built a single-page app with React and Vite. It has three pages, Files, Search and Cluster, plus a box in the header for the API key. It talks only to the gateway from Phase 5, so it goes through the same authentication and rate limits as any other client. I didn't use a UI library: the layout and colors are plain CSS, with a light and a dark theme.

**Files page.** On the left are the buckets, with a form to create a new one (replication or erasure coding). On the right, files can be dropped into an upload area with an optional folder, tags and chunking method, and each file gets a progress bar. The object list can be filtered by key prefix. Each object can be downloaded (with progress), deleted, or opened to show its details: its SHA-256, its chunks, and the nodes holding each chunk or fragment.

**Search with Elasticsearch.** I added Elasticsearch 9.2 as a new container. Every time an object is uploaded or deleted, the metadata service puts its key in a queue. A background thread sends the changes to Elasticsearch in batches, so uploads never wait for the search index. PostgreSQL stays the source of truth: if the index is empty or behind, it is rebuilt from the database automatically.

The search page combines free text with filters. The free text matches words of the key (and tolerates typos), any part of the key, or an exact tag, and it can be combined with bucket, prefix, size, date and tag filters. Next to the results, facets count the matches per bucket, tag and content type, and clicking one narrows the search. A switch runs the same filters on PostgreSQL instead, to compare the two engines.

**Cluster page.** This page refreshes every 2 seconds. At the top, tiles show the number of alive nodes, objects, degraded and unreadable objects, the repair queue, the free space and the size of the search index. Below them:
- a bar chart shows how many blobs each node stores;
- a progress bar follows the current repair, with the bytes read and written per redundancy scheme;
- a table shows each node's state: alive, "no heartbeat for N seconds" while the failure detector waits, or failed.

Every state is shown with both an icon and a word, never by color alone.

**Serving it.** In Docker, the app is built once and served by nginx on port 5173. nginx also forwards every `/api/` request to the gateway, with buffering turned off, so large uploads and downloads stream straight through.

---

## 9.3 Screenshots

Start with: `docker compose up -d --build` (wait about a minute: Elasticsearch takes the longest).

Then run the helper: `powershell -ExecutionPolicy Bypass -File scripts\demo_phase6.ps1`. It uploads sample data, then **stops and waits** for you to take the browser screenshots before each next step.

**9.3.1 The Files page**
*Where:* browser → `http://127.0.0.1:5173` → Files → click the bucket **docs**.
*Before:* "The Files page lists the buckets on the left and the objects of the selected bucket on the right."
📷 **Figure 9.1: The Files page with the bucket list and the objects of a bucket**
*After:* "Each object shows its size, version, tags and upload date, and has buttons to download, inspect or delete it."

**9.3.2 Uploading files**
*Where:* Files → drop two or three files from your PC into the upload area, type tags such as `type=photo,project=lab`, click **Upload**. Take the screenshot while the bars move, or right after.
*Before:* "Files are uploaded by dropping them into the upload area, with optional tags."
📷 **Figure 9.2: Uploading files with tags and a progress bar per file**
*After:* "When an upload finishes, its line shows the version and how many chunks were new or already stored."

**9.3.3 Object details**
*Where:* Files → bucket **archive** → **Details** on `images/disk-00.img`.
*Before:* "The details of an object show how it is stored inside the cluster."
📷 **Figure 9.3: Object details: SHA-256, chunks and the nodes holding each fragment**
*After:* "This object is in an erasure-coded bucket, so each chunk has six fragments on six different nodes."

**9.3.4 Downloading a file**
*Where:* Files → **Download** on any object, then show the file in your Downloads folder (or the browser's download bar).
*Before:* "Downloads go through the gateway and are verified chunk by chunk."
📷 **Figure 9.4: Downloading a file from the web interface**
*After:* "The button shows the download progress, then the browser saves the file under its original name."

**9.3.5 Free-text search**
*Where:* Search → Elasticsearch → type `user`, press **Search**. Try a typo too, like `ordrs`.
*Before:* "The search page sends free-text queries to Elasticsearch."
📷 **Figure 9.5: Free-text search in Elasticsearch, with facets**
*After:* "Elasticsearch found the matching files in a few milliseconds, even with a typo, and counted the results per bucket, tag and content type."

**9.3.6 Search with filters**
*Where:* Search → clear the text → Tags `type=disk-image` → Min size `100000` KB → **Search**.
*Before:* "Filters can be combined: here, disk images larger than 100 MB."
📷 **Figure 9.6: Search with tag and size filters**
*After:* "Only the large disk images match. The same filters can also run on PostgreSQL with the switch next to the search box."

**9.3.7 Inside Elasticsearch**
*Where:* the terminal, at the **second stop** of the demo helper (step 2).
*Before:* "To check that the metadata really is in Elasticsearch, I queried it directly on port 9200."
📷 **Figure 9.7: The object metadata stored in the Elasticsearch index**
*After:* "The index counts every object, and each document holds the bucket, key, size, version, tags, redundancy and upload date."

**9.3.8 The Cluster page**
*Where:* browser → Cluster (`http://127.0.0.1:5173/#/cluster`), before the failure.
*Before:* "The Cluster page shows the state of the whole system and refreshes every two seconds."
📷 **Figure 9.8: The Cluster page with all nodes alive and the chunk distribution**
*After:* "All seven nodes are alive, no object is degraded, and the chart shows the blobs spread almost evenly over the nodes."

**9.3.9 A node failure, live**
*Where:* keep the Cluster page open, then press Enter at the **third stop** of the helper. It kills storage-node-4. Take one screenshot when the node shows **No heartbeat for … s** (in the first 15 seconds), and one when it shows **Failed** while the repair bar moves.
*Before:* "To see the failure handling in the interface, I killed one storage node."
📷 **Figure 9.9: The node stops sending heartbeats, is marked failed, and the repair starts**
*After:* "The page shows the missed heartbeats, then the failed node, the degraded objects and the repair progress bar."

**9.3.10 After the repair**
*Where:* Cluster page, when the helper prints "Repair finished".
*Before:* "A few seconds later the repair worker had rebuilt everything."
📷 **Figure 9.10: The cluster after the repair: no degraded objects**
*After:* "Degraded objects are back to zero and the repair table shows the bytes read and written for each scheme."

**9.3.11 Automated tests**
*Where:* terminal: `.\.venv\Scripts\python.exe -m pytest -v`
📷 **Figure 9.11: All 56 tests passing**
*After:* "The 8 new tests check free-text search, typos, partial keys, filters, facets and that deleted objects leave the index."

---

## 9.4 Results and Observations

Every Task 6 requirement works: uploading, downloading and browsing in the browser, the cluster state with node health, chunk distribution and repair progress, and search with Elasticsearch.

Elasticsearch answered searches over about 2,000 objects in a few milliseconds and handled typos and partial words, which plain SQL `LIKE` queries can't do. In return, a new upload only becomes searchable about a second later, because the index refreshes once per second. PostgreSQL search is always exactly up to date. That's why PostgreSQL stays the source of truth, and the index can always be rebuilt from it.

During a node failure, the Cluster page made the failure detector visible. For 15 seconds the node showed "no heartbeat", then it turned "failed", and the repair bar ran until the degraded objects were back to zero.

| Task 6 requirement | Evidence |
|---|---|
| React/Vue web interface | Figures 9.1–9.6, 9.8 |
| Upload, download and browse through the gateway | Figures 9.1, 9.2, 9.3, 9.4 |
| Cluster state: node health, chunk distribution, repair progress | Figures 9.8, 9.9, 9.10 |
| Object search with Elasticsearch over the metadata | Figures 9.5, 9.6, 9.7 |
