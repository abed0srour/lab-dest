import itertools
import os

import pytest

from common.erasure import decode, encode, fragment_size


@pytest.mark.parametrize("size", [1, 1000, 4 * 1024 * 1024 + 3])
def test_any_4_of_6_fragments_rebuild_the_data(size):
    data = os.urandom(size)
    fragments = encode(data, 4, 2)
    assert len(fragments) == 6
    assert {len(f) for f in fragments} == {fragment_size(size, 4)}
    for keep in itertools.combinations(range(6), 4):
        assert decode({i: fragments[i] for i in keep}, 4, 2, size) == data


def test_data_fragments_are_the_original_bytes():
    data = os.urandom(4000)
    fragments = encode(data, 4, 2)
    assert b"".join(fragments[:4]) == data


def test_fragments_take_one_and_a_half_times_the_space():
    data = os.urandom(4 * 1024 * 1024)
    assert sum(len(f) for f in encode(data, 4, 2)) == 1.5 * len(data)


def test_three_fragments_are_not_enough():
    data = os.urandom(1000)
    fragments = encode(data, 4, 2)
    with pytest.raises(ValueError):
        decode({0: fragments[0], 3: fragments[3], 5: fragments[5]}, 4, 2, len(data))
