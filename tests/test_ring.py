from collections import Counter

from common.chunking import sha256
from common.ring import HashRing

NODES = [f"storage-node-{i}" for i in range(1, 5)]
KEYS = [sha256(str(i).encode()) for i in range(20_000)]


def test_replicas_are_distinct_and_deterministic():
    ring = HashRing(NODES)
    for key in KEYS[:1000]:
        nodes = ring.nodes_for(key, 3)
        assert len(set(nodes)) == 3
        assert nodes == HashRing(reversed(NODES)).nodes_for(key, 3)


def test_asking_for_more_replicas_than_nodes_returns_every_node():
    assert sorted(HashRing(NODES[:2]).nodes_for(KEYS[0], 3)) == NODES[:2]


def test_adding_a_node_moves_only_its_fair_share_to_the_new_node():
    before, after = HashRing(NODES), HashRing(NODES + ["storage-node-5"])
    moved = 0
    for key in KEYS:
        new_copies = set(after.nodes_for(key, 3)) - set(before.nodes_for(key, 3))
        assert new_copies <= {"storage-node-5"}  # data only ever moves onto the new node
        moved += len(new_copies)
    assert 0.17 < moved / (3 * len(KEYS)) < 0.23  # ideal: 1/5 of all copies


def test_removing_a_node_keeps_every_surviving_copy_in_place():
    five = NODES + ["storage-node-5"]
    before, after = HashRing(five), HashRing(five)
    after.remove("storage-node-3")
    for key in KEYS[:5000]:
        assert set(before.nodes_for(key, 3)) - {"storage-node-3"} <= set(after.nodes_for(key, 3))


def test_virtual_nodes_spread_copies_evenly():
    ring = HashRing(NODES, vnodes=128)
    load = Counter(n for key in KEYS for n in ring.nodes_for(key, 3))
    mean = 3 * len(KEYS) / len(NODES)
    assert all(abs(load[n] - mean) / mean < 0.10 for n in NODES)
