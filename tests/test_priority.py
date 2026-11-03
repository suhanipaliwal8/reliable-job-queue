import pytest

from jobqueue.core import claim_job, enqueue, get_job
from jobqueue.registry import handler
from jobqueue.worker import Worker


# ---------- priority ----------

def test_higher_priority_is_claimed_first():
    low = enqueue("t", priority=0)
    high = enqueue("t", priority=10)

    assert claim_job("w1")["id"] == high
    assert claim_job("w1")["id"] == low


def test_same_priority_is_first_in_first_out():
    first = enqueue("t", priority=5)
    second = enqueue("t", priority=5)

    assert claim_job("w1")["id"] == first
    assert claim_job("w1")["id"] == second


def test_negative_priority_runs_after_default_priority():
    bulk = enqueue("t", priority=-5)
    normal = enqueue("t")

    assert claim_job("w1")["id"] == normal
    assert claim_job("w1")["id"] == bulk


def test_recovered_job_still_respects_priority(expire_lease):
    low = enqueue("t", priority=0)
    claim_job("dead-worker")          # low job is claimed, then the worker "crashes"
    expire_lease(low)
    high = enqueue("t", priority=10)

    assert claim_job("w2")["id"] == high
    assert claim_job("w2")["id"] == low


# ---------- delayed jobs ----------

def test_delayed_job_is_not_claimed_before_it_is_due():
    job_id = enqueue("t", delay_seconds=60)

    assert claim_job("w1") is None
    assert get_job(job_id)["status"] == "pending"


def test_delayed_job_is_claimed_once_due(make_due):
    job_id = enqueue("t", delay_seconds=60)
    assert claim_job("w1") is None

    make_due(job_id)
    assert claim_job("w1")["id"] == job_id


def test_delay_is_stored_in_run_at():
    job = get_job(enqueue("t", delay_seconds=60))

    gap = (job["run_at"] - job["created_at"]).total_seconds()
    assert gap == pytest.approx(60, abs=0.01)


def test_future_urgent_job_does_not_block_due_low_priority_job():
    enqueue("t", priority=100, delay_seconds=60)
    low = enqueue("t", priority=0)

    assert claim_job("w1")["id"] == low


def test_delayed_urgent_job_jumps_the_queue_once_due(make_due):
    urgent = enqueue("t", priority=10, delay_seconds=60)
    normal = enqueue("t")
    make_due(urgent)

    assert claim_job("w1")["id"] == urgent
    assert claim_job("w1")["id"] == normal


# ---------- validation ----------

def test_negative_delay_is_rejected():
    with pytest.raises(ValueError):
        enqueue("t", delay_seconds=-1)


def test_max_attempts_below_one_is_rejected():
    with pytest.raises(ValueError):
        enqueue("t", max_attempts=0)


# ---------- worker integration ----------

order = []


@handler("test_prio")
def _record(job):
    order.append(job["payload"]["name"])


def test_worker_processes_jobs_in_priority_order():
    order.clear()
    enqueue("test_prio", {"name": "low"}, priority=0)
    enqueue("test_prio", {"name": "high"}, priority=9)
    enqueue("test_prio", {"name": "mid"}, priority=5)

    Worker("w1").run_forever(drain=True)

    assert order == ["high", "mid", "low"]


def test_drain_leaves_jobs_that_are_not_due_yet():
    order.clear()
    job_id = enqueue("test_prio", {"name": "later"}, delay_seconds=60)

    Worker("w1").run_forever(drain=True)

    assert order == []
    assert get_job(job_id)["status"] == "pending"