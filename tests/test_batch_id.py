"""Verify the batch-id formula (PARSING_SPEC §8): batch_id = floor((ord-1)/N) + 1."""
from __future__ import annotations


def batch_id(ordinal: int, n: int) -> int:
    return ((ordinal - 1) // n) + 1


def test_batch_id_n10_ordinals_1_to_25():
    expected = [1] * 10 + [2] * 10 + [3] * 5
    actual = [batch_id(o, 10) for o in range(1, 26)]
    assert actual == expected


def test_batch_id_first_in_batch_is_one_indexed():
    assert batch_id(1, 100_000) == 1
    assert batch_id(100_000, 100_000) == 1
    assert batch_id(100_001, 100_000) == 2


def test_batch_id_partial_last_batch_still_counts():
    # 17 records, N=5  →  batches: [1..5]=1, [6..10]=2, [11..15]=3, [16,17]=4
    last_ordinal = 17
    last_batch = batch_id(last_ordinal, 5)
    assert last_batch == 4
    # Total non-empty batches = max(batch_id) = 4 (PARSING_SPEC §8)


def test_batch_id_n_equal_to_total():
    # N = total records → exactly one batch
    for ord_ in range(1, 1001):
        assert batch_id(ord_, 1000) == 1


def test_batch_id_n_one():
    # N = 1 → every record in its own batch
    for ord_ in range(1, 11):
        assert batch_id(ord_, 1) == ord_
