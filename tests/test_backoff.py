import pytest

from jobqueue.backoff import compute_backoff


def test_delay_doubles_each_attempt_without_jitter():
    delays = [compute_backoff(n, base=2, cap=300, jitter=False) for n in range(1, 6)]
    assert delays == [2, 4, 8, 16, 32]


def test_delay_is_capped():
    assert compute_backoff(20, base=2, cap=300, jitter=False) == 300


def test_huge_attempt_numbers_do_not_overflow():
    assert compute_backoff(5000, base=2, cap=300, jitter=False) == 300


def test_jitter_stays_between_half_and_full_delay():
    for attempt in range(1, 11):
        full = compute_backoff(attempt, base=2, cap=300, jitter=False)
        for _ in range(200):
            delay = compute_backoff(attempt, base=2, cap=300)
            assert full / 2 <= delay <= full


def test_jitter_actually_randomises():
    samples = {compute_backoff(5, base=2, cap=300) for _ in range(50)}
    assert len(samples) > 1


def test_attempt_must_be_positive():
    with pytest.raises(ValueError):
        compute_backoff(0)