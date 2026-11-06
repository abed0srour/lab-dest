"""Reed-Solomon erasure coding with zfec: k data fragments + m parity fragments.

Any k of the k + m fragments rebuild the chunk. The code is systematic: fragments
0..k-1 are the chunk itself cut into k equal parts (the last one zero-padded),
so a read that gets all data fragments only has to join them.
"""

import zfec


def fragment_size(size: int, k: int) -> int:
    return -(-size // k)  # ceil(size / k)


def encode(data: bytes, k: int, m: int) -> list[bytes]:
    """Split data into k + m fragments of equal size."""
    n = fragment_size(len(data), k)
    padded = data.ljust(n * k, b"\0")
    blocks = [padded[i * n:(i + 1) * n] for i in range(k)]
    return zfec.Encoder(k, k + m).encode(blocks)


def decode(fragments: dict[int, bytes], k: int, m: int, size: int) -> bytes:
    """Rebuild the original `size` bytes from any k fragments {index: bytes}."""
    if len(fragments) < k:
        raise ValueError(f"need {k} fragments to decode, have {len(fragments)}")
    nums = tuple(sorted(fragments)[:k])
    if nums == tuple(range(k)):  # all data fragments present: no decoding needed
        return b"".join(fragments[i] for i in nums)[:size]
    blocks = zfec.Decoder(k, k + m).decode(tuple(fragments[i] for i in nums), nums)
    return b"".join(blocks)[:size]
