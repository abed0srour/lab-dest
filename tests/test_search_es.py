"""Elasticsearch search through the gateway. Skipped if the gateway or Elasticsearch is not running."""

import os
import time
import uuid

import httpx
import pytest

GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080")
KEY = {"X-API-Key": os.environ.get("API_KEY", "bench-key-2026")}


def es_ready() -> bool:
    try:
        return httpx.get(f"{GATEWAY}/search/status", headers=KEY, timeout=3).json()["elasticsearch"] in ("green", "yellow")
    except (httpx.HTTPError, KeyError, ValueError):
        return False


pytestmark = pytest.mark.skipif(not es_ready(), reason="gateway or Elasticsearch is not running")


@pytest.fixture(scope="module")
def run():
    """Upload a few objects with a unique tag, wait until Elasticsearch has them, delete them afterwards."""
    tag = uuid.uuid4().hex[:8]
    files = {f"es-{tag}/reports/quarterly-summary.pdf": (b"x" * 5000, "kind=report"),
             f"es-{tag}/photos/holiday-beach.jpg": (b"y" * 300_000, "kind=photo"),
             f"es-{tag}/photos/mountain-trip.jpg": (b"z" * 200_000, "kind=photo")}
    with httpx.Client(base_url=GATEWAY, headers=KEY, timeout=60) as c:
        c.put("/buckets/es-test").raise_for_status()
        for key, (data, kind) in files.items():
            c.put(f"/buckets/es-test/objects/{key}", content=data, headers={"X-Tags": f"run={tag},{kind}"}).raise_for_status()
        for _ in range(50):  # new documents become searchable after the index refreshes (~1 s)
            if find(c, tag=[f"run={tag}"])["count"] == 3:
                break
            time.sleep(0.2)
        yield c, tag
        for key in files:
            c.delete(f"/buckets/es-test/objects/{key}")


def find(c, **params):
    r = c.get("/search", params={"engine": "elasticsearch", **params})
    r.raise_for_status()
    return r.json()


def names(result):
    return sorted(o["key"].split("/")[-1] for o in result["results"])


def test_all_uploads_are_indexed(run):
    c, tag = run
    assert find(c, tag=[f"run={tag}"])["count"] == 3


def test_free_text_matches_words_of_the_key(run):
    c, tag = run
    assert names(find(c, q="beach", tag=[f"run={tag}"])) == ["holiday-beach.jpg"]


def test_free_text_tolerates_typos(run):
    c, tag = run
    assert names(find(c, q="mountian", tag=[f"run={tag}"])) == ["mountain-trip.jpg"]


def test_free_text_matches_any_part_of_the_key(run):
    c, tag = run
    assert names(find(c, q="terly-sum", tag=[f"run={tag}"])) == ["quarterly-summary.pdf"]


def test_filters_combine_with_free_text(run):
    c, tag = run
    assert names(find(c, q="photos", tag=[f"run={tag}"], min_size=250_000)) == ["holiday-beach.jpg"]
    assert names(find(c, prefix=f"es-{tag}/reports/")) == ["quarterly-summary.pdf"]


def test_facets_count_tags(run):
    c, tag = run
    facets = {f["value"]: f["count"] for f in find(c, tag=[f"run={tag}"])["facets"]["tags"]}
    assert facets["kind=photo"] == 2 and facets["kind=report"] == 1


def test_deleted_objects_leave_the_index(run):
    c, tag = run
    key = f"es-{tag}/temporary.txt"
    c.put(f"/buckets/es-test/objects/{key}", content=b"bye", headers={"X-Tags": f"run={tag}"}).raise_for_status()
    c.delete(f"/buckets/es-test/objects/{key}").raise_for_status()
    for _ in range(50):
        if "temporary.txt" not in names(find(c, tag=[f"run={tag}"])):
            break
        time.sleep(0.2)
    assert "temporary.txt" not in names(find(c, tag=[f"run={tag}"]))


def test_free_text_needs_elasticsearch(run):
    c, _ = run
    assert c.get("/search", params={"q": "beach"}).status_code == 400
