"""Elasticsearch index of the object metadata (latest version of every object).

PostgreSQL stays the source of truth. After every commit or delete the metadata
service calls touch(bucket, key); a background thread batches those keys, reads
their latest version from PostgreSQL and sends one bulk request to Elasticsearch,
so uploads never wait for the index. If Elasticsearch is down, the keys are kept
and retried; reindex() rebuilds the whole index from PostgreSQL.
"""

import json
import logging
import queue
import threading
import time

import httpx

log = logging.getLogger("search-index")

INDEX = "objects"
MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            # "photos/2026/cat-01.jpg" -> photos, 2026, cat, 01, jpg
            "tokenizer": {"key_parts": {"type": "pattern", "pattern": "[/._\\-\\s]+"}},
            "analyzer": {"key_text": {"type": "custom", "tokenizer": "key_parts", "filter": ["lowercase"]}},
        },
    },
    "mappings": {
        "properties": {
            "bucket": {"type": "keyword"},
            "key": {"type": "keyword", "fields": {"text": {"type": "text", "analyzer": "key_text"}}},
            "size": {"type": "long"},
            "version": {"type": "integer"},
            "content_type": {"type": "keyword"},
            "redundancy": {"type": "keyword"},
            "chunking": {"type": "keyword"},
            "sha256": {"type": "keyword"},
            "created_at": {"type": "date"},
            "tags": {"type": "flattened"},
            "tag_list": {"type": "keyword"},  # "type=video": for facets and exact tag search
        }
    },
}

LATEST = """SELECT DISTINCT ON (o.bucket, o.key) o.bucket, o.key, o.version, o.size, o.chunking, o.tags,
                   o.sha256, o.content_type, o.created_at, b.redundancy
            FROM objects o JOIN buckets b ON b.name = o.bucket"""


def doc_id(bucket: str, key: str) -> str:
    return f"{bucket}/{key}"


def to_doc(row: dict) -> dict:
    return {"bucket": row["bucket"], "key": row["key"], "version": row["version"], "size": row["size"],
            "chunking": row["chunking"], "content_type": row["content_type"], "redundancy": row["redundancy"],
            "sha256": row["sha256"], "created_at": row["created_at"].isoformat(), "tags": row["tags"],
            "tag_list": [f"{k}={v}" for k, v in row["tags"].items()]}


class SearchIndex:
    def __init__(self, pool, url: str):
        self.pool = pool
        self.url = url.rstrip("/")
        self.http = httpx.Client(timeout=30)
        self.pending: queue.Queue = queue.Queue()
        self.ready = False

    # ---- writing ---------------------------------------------------------------

    def touch(self, bucket: str, key: str) -> None:
        """This object changed: (re)index its latest version, or remove it if it has none."""
        self.pending.put((bucket, key))

    def _bulk(self, lines: list[dict]) -> None:
        body = "".join(json.dumps(line) + "\n" for line in lines)
        r = self.http.post(f"{self.url}/_bulk", content=body, headers={"Content-Type": "application/x-ndjson"})
        r.raise_for_status()
        if r.json().get("errors"):
            bad = [i for i in r.json()["items"] if next(iter(i.values())).get("error")]
            log.warning("%d bulk items failed, first: %s", len(bad), bad[0])

    def _sync(self, keys: set[tuple[str, str]]) -> None:
        buckets, names = zip(*keys)
        with self.pool.connection() as conn:
            rows = conn.execute(f"""{LATEST} WHERE (o.bucket, o.key) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
                                    ORDER BY o.bucket, o.key, o.version DESC""", (list(buckets), list(names))).fetchall()
        found = {(r["bucket"], r["key"]): r for r in rows}
        lines = []
        for bucket, key in keys:
            if (bucket, key) in found:
                lines += [{"index": {"_index": INDEX, "_id": doc_id(bucket, key)}}, to_doc(found[(bucket, key)])]
            else:
                lines.append({"delete": {"_index": INDEX, "_id": doc_id(bucket, key)}})
        self._bulk(lines)

    def ensure_index(self) -> None:
        if self.http.head(f"{self.url}/{INDEX}").status_code == 404:
            self.http.put(f"{self.url}/{INDEX}", json=MAPPING).raise_for_status()
            log.info("created Elasticsearch index %r", INDEX)

    def reindex(self) -> int:
        """Rebuild the index from PostgreSQL."""
        self.http.delete(f"{self.url}/{INDEX}")
        self.ensure_index()
        with self.pool.connection() as conn:
            rows = conn.execute(f"{LATEST} ORDER BY o.bucket, o.key, o.version DESC").fetchall()
        for i in range(0, len(rows), 1000):
            lines = []
            for row in rows[i:i + 1000]:
                lines += [{"index": {"_index": INDEX, "_id": doc_id(row["bucket"], row["key"])}}, to_doc(row)]
            self._bulk(lines)
        self.http.post(f"{self.url}/{INDEX}/_refresh")
        log.info("reindexed %d objects", len(rows))
        return len(rows)

    def run(self) -> None:
        """Background thread: connect, catch up if the index was lost, then sync changes in batches."""
        while not self.ready:
            try:
                self.ensure_index()
                indexed = self.http.get(f"{self.url}/{INDEX}/_count").json()["count"]
                with self.pool.connection() as conn:
                    stored = conn.execute("SELECT count(DISTINCT (bucket, key)) AS n FROM objects").fetchone()["n"]
                if indexed != stored:
                    log.info("index has %d objects, database has %d: reindexing", indexed, stored)
                    self.reindex()
                self.ready = True
                log.info("search index ready at %s", self.url)
            except (httpx.HTTPError, KeyError) as e:
                log.warning("Elasticsearch not reachable yet (%r); retrying in 5 s", e)
                time.sleep(5)
        while True:
            keys = {self.pending.get()}
            time.sleep(0.2)  # let a burst of uploads collect into one bulk request
            while len(keys) < 1000:
                try:
                    keys.add(self.pending.get_nowait())
                except queue.Empty:
                    break
            try:
                self._sync(keys)
            except Exception as e:
                log.warning("indexing %d objects failed (%r); retrying in 5 s", len(keys), e)
                for k in keys:
                    self.pending.put(k)
                time.sleep(5)

    def start(self) -> None:
        threading.Thread(target=self.run, daemon=True).start()

    # ---- searching -------------------------------------------------------------

    def status(self) -> dict:
        try:
            health = self.http.get(f"{self.url}/_cluster/health").json()["status"]
            count = self.http.get(f"{self.url}/{INDEX}/_count").json().get("count", 0)
        except (httpx.HTTPError, KeyError, ValueError):
            health, count = "unreachable", 0
        return {"elasticsearch": health, "indexed_objects": count, "waiting_to_index": self.pending.qsize(),
                "url": self.url}

    def search(self, q: str | None, bucket: str | None, prefix: str, min_size: int | None,
               max_size: int | None, after: str | None, before: str | None, tags: dict, limit: int) -> dict:
        filters: list[dict] = []
        if bucket:
            filters.append({"term": {"bucket": bucket}})
        if prefix:
            filters.append({"prefix": {"key": prefix}})
        size = {k: v for k, v in (("gte", min_size), ("lte", max_size)) if v is not None}
        if size:
            filters.append({"range": {"size": size}})
        dates = {k: v for k, v in (("gte", after), ("lt", before)) if v}
        if dates:
            filters.append({"range": {"created_at": dates}})
        for k, v in tags.items():
            filters.append({"term": {f"tags.{k}": v}})
        must: list[dict] = [{"match_all": {}}]
        if q:
            pattern = q.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?")
            must = [{"bool": {"minimum_should_match": 1, "should": [
                {"match": {"key.text": {"query": q, "operator": "and", "fuzziness": "AUTO"}}},  # words, typos ok
                {"wildcard": {"key": {"value": f"*{pattern}*", "case_insensitive": True}}},     # any part of the key
                {"term": {"tag_list": q}},                                                     # an exact tag
            ]}}]
        body = {
            "query": {"bool": {"must": must, "filter": filters}},
            "size": limit,
            "sort": (["_score"] if q else []) + [{"bucket": "asc"}, {"key": "asc"}],
            "track_scores": True,
            "track_total_hits": True,
            "aggs": {"buckets": {"terms": {"field": "bucket", "size": 20}},
                     "content_types": {"terms": {"field": "content_type", "size": 10}},
                     "tags": {"terms": {"field": "tag_list", "size": 15}}},
        }
        r = self.http.post(f"{self.url}/{INDEX}/_search", json=body)
        r.raise_for_status()
        res = r.json()
        return {
            "engine": "elasticsearch", "took_ms": res["took"], "count": res["hits"]["total"]["value"],
            "results": [{**h["_source"], "score": h["_score"]} for h in res["hits"]["hits"]],
            "facets": {name: [{"value": b["key"], "count": b["doc_count"]} for b in agg["buckets"]]
                       for name, agg in res["aggregations"].items()},
        }
