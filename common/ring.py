"""Consistent hashing ring with virtual nodes.

Every physical node is placed on the ring `vnodes` times. A key is owned by the
first point clockwise from the key's position; its replicas are the next
distinct physical nodes after that. Adding or removing a node therefore only
moves the keys next to that node's points, about 1/n of the data.
"""

import bisect
import hashlib
from collections.abc import Iterable


def _position(value: str) -> int:
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], "big")


class HashRing:
    def __init__(self, nodes: Iterable[str] = (), vnodes: int = 128):
        self.vnodes = vnodes
        self._nodes: set[str] = set()
        self._points: list[int] = []
        self._owners: list[str] = []
        for node in nodes:
            self.add(node)

    @property
    def nodes(self) -> set[str]:
        return set(self._nodes)

    def add(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.vnodes):
            p = _position(f"{node}#{i}")
            idx = bisect.bisect(self._points, p)
            self._points.insert(idx, p)
            self._owners.insert(idx, node)

    def remove(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        keep = [(p, o) for p, o in zip(self._points, self._owners) if o != node]
        self._points = [p for p, _ in keep]
        self._owners = [o for _, o in keep]

    def nodes_for(self, key: str, n: int = 1) -> list[str]:
        """The n distinct nodes responsible for key, primary first."""
        n = min(n, len(self._nodes))
        out: list[str] = []
        i = bisect.bisect(self._points, _position(key))
        while len(out) < n:
            owner = self._owners[i % len(self._points)]
            if owner not in out:
                out.append(owner)
            i += 1
        return out
