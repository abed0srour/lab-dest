"""Split a byte stream into chunks: fixed-size or content-defined (FastCDC)."""

import hashlib
from collections.abc import Iterator
from typing import BinaryIO

MiB = 1024 * 1024
FIXED_SIZE = 4 * MiB
CDC_MIN, CDC_AVG, CDC_MAX = 1 * MiB, 4 * MiB, 16 * MiB
METHODS = ("fixed", "cdc")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_full(stream: BinaryIO, n: int) -> bytes:
    """Read exactly n bytes unless the stream ends first (network streams return short reads)."""
    parts, got = [], 0
    while got < n and (data := stream.read(n - got)):
        parts.append(data)
        got += len(data)
    return b"".join(parts)


def fixed_chunks(stream: BinaryIO, size: int = FIXED_SIZE) -> Iterator[bytes]:
    """Cut every `size` bytes. A single insertion shifts every later boundary."""
    while chunk := _read_full(stream, size):
        yield chunk


def cdc_chunks(stream: BinaryIO, min_size: int = CDC_MIN, avg_size: int = CDC_AVG,
               max_size: int = CDC_MAX) -> Iterator[bytes]:
    """Cut where a rolling (gear) hash of the content matches a pattern, so
    boundaries move with the content and an insertion only changes nearby chunks.

    FastCDC needs the data in memory, so the stream is processed in windows of
    4 * max_size. The last chunk of a window may only end because the window
    ended, so it is carried into the next window; every other boundary depends
    only on content, which gives the same chunks as chunking the whole file at once.
    """
    from fastcdc import fastcdc

    window = 4 * max_size
    buf = b""
    while True:
        buf += _read_full(stream, window - len(buf))
        if not buf:
            return
        at_end = len(buf) < window
        cuts = [(c.offset, c.length) for c in fastcdc(buf, min_size, avg_size, max_size, fat=False)]
        if not at_end:
            cuts = cuts[:-1]  # a full window always holds >= 4 chunks, so cuts is not empty
        for offset, length in cuts:
            yield buf[offset:offset + length]
        if at_end:
            return
        offset, length = cuts[-1]
        buf = buf[offset + length:]


def chunk_stream(stream: BinaryIO, method: str = "fixed") -> Iterator[bytes]:
    if method == "fixed":
        return fixed_chunks(stream)
    if method == "cdc":
        return cdc_chunks(stream)
    raise ValueError(f"unknown chunking method {method!r}, expected one of {METHODS}")
