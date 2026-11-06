import io
import os

from fastcdc import fastcdc

from common.chunking import CDC_AVG, CDC_MAX, CDC_MIN, FIXED_SIZE, MiB, cdc_chunks, fixed_chunks, sha256


class Trickle(io.RawIOBase):
    """A stream that returns at most 1000 bytes per read, like a slow network upload."""

    def __init__(self, data: bytes):
        self.inner = io.BytesIO(data)

    def readable(self):
        return True

    def read(self, n=-1):
        return self.inner.read(min(n, 1000) if n > 0 else 1000)


def test_fixed_chunks_have_fixed_size_and_rejoin_to_the_original():
    data = os.urandom(10 * MiB + 123)
    chunks = list(fixed_chunks(io.BytesIO(data)))
    assert [len(c) for c in chunks] == [FIXED_SIZE, FIXED_SIZE, 2 * MiB + 123]
    assert b"".join(chunks) == data


def test_fixed_chunks_are_the_same_for_short_reads():
    data = os.urandom(5 * MiB)
    assert list(fixed_chunks(Trickle(data))) == list(fixed_chunks(io.BytesIO(data)))


def test_cdc_chunks_respect_size_limits_and_rejoin_to_the_original():
    data = os.urandom(50 * MiB)
    chunks = list(cdc_chunks(io.BytesIO(data)))
    assert b"".join(chunks) == data
    assert all(CDC_MIN <= len(c) <= CDC_MAX for c in chunks[:-1])


def test_windowed_cdc_gives_the_same_chunks_as_chunking_the_whole_file():
    data = os.urandom(100 * MiB)  # larger than one 64 MB window
    whole = [bytes(c.data) for c in fastcdc(data, CDC_MIN, CDC_AVG, CDC_MAX, fat=True)]
    assert list(cdc_chunks(io.BytesIO(data))) == whole


def test_an_insertion_changes_most_fixed_chunks_but_few_cdc_chunks():
    base = os.urandom(32 * MiB)
    edited = base[:len(base) // 2] + b"hello" + base[len(base) // 2:]
    for method, max_changed in ((fixed_chunks, None), (cdc_chunks, 2)):
        before = {sha256(c) for c in method(io.BytesIO(base))}
        after = [sha256(c) for c in method(io.BytesIO(edited))]
        changed = sum(h not in before for h in after)
        if max_changed is None:
            assert changed >= len(after) // 2  # every chunk after the insertion shifts
        else:
            assert changed <= max_changed


def test_empty_stream_has_no_chunks():
    assert list(fixed_chunks(io.BytesIO(b""))) == []
    assert list(cdc_chunks(io.BytesIO(b""))) == []
