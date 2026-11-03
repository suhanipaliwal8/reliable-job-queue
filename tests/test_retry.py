from jobqueue.core import claim_job, enqueue, fail_job, get_job
from jobqueue.registry import handler
from jobqueue.worker import Worker


def _gap(job_id) -> float:
    """Seconds between the moment of failure and the scheduled retry time.
    Both values are set by the same SQL statement, so this is exactly the delay."""
    job = get_job(job_id)
    return (job["run_at"] - job["updated_at"]).total_seconds()


# ---------- backoff scheduling ----------

def test_failed_job_is_not_immediately_claimable():
    job_id = enqueue("t", max_attempts=3)
    claim_job("w1")

    assert fail_job(job_id, "w1", "boom") == "pending"
    assert claim_job("w2") is None


def test_retry_is_scheduled_within_the_jittered_window():
    job_id = enqueue("t", max_attempts=3)
    claim_job("w1")
    fail_job(job_id, "w1", "boom", backoff_base=100, backoff_cap=1000)

    assert 50 <= _gap(job_id) <= 100      # attempt 1: 100s, jitter -> 50..100


def test_backoff_grows_with_each_attempt(make_due):
    job_id = enqueue("t", max_attempts=5)

    claim_job("w1")
    fail_job(job_id, "w1", "boom", backoff_base=100, backoff_cap=1000)
    first = _gap(job_id)                  # 50..100
    make_due(job_id)

    claim_job("w1")
    fail_job(job_id, "w1", "boom", backoff_base=100, backoff_cap=1000)
    second = _gap(job_id)                 # 100..200

    assert first <= 100 <= second


def test_backoff_is_capped(make_due):
    job_id = enqueue("t", max_attempts=5)
    for attempt in range(3):              # uncapped, attempt 3 would be 400s
        claim_job("w1")
        fail_job(job_id, "w1", "boom", backoff_base=100, backoff_cap=120)
        if attempt < 2:
            make_due(job_id)

    assert _gap(job_id) <= 120


def test_job_is_retried_once_backoff_has_passed(make_due):
    job_id = enqueue("t", max_attempts=3)
    claim_job("w1")
    fail_job(job_id, "w1", "boom")
    make_due(job_id)

    job = claim_job("w2")
    assert job["id"] == job_id
    assert job["attempts"] == 2


def test_stale_worker_cannot_fail_a_job_it_lost(expire_lease):
    job_id = enqueue("t")
    claim_job("w1")
    expire_lease(job_id)
    claim_job("w2")                       # w2 takes over

    assert fail_job(job_id, "w1", "late failure") is None
    job = get_job(job_id)
    assert job["status"] == "in_flight"
    assert job["locked_by"] == "w2"
    assert job["last_error"] is None


# ---------- poison jobs (crash loops) ----------

def test_job_that_keeps_killing_workers_goes_dead(expire_lease):
    job_id = enqueue("t", max_attempts=1)
    claim_job("w1")                       # claims, then "crashes"
    expire_lease(job_id)

    assert claim_job("w2") is None        # not run again
    job = get_job(job_id)
    assert job["status"] == "dead"
    assert "lease expired" in job["last_error"].lower()


def test_crash_recovery_still_works_while_attempts_remain(expire_lease):
    job_id = enqueue("t", max_attempts=2)
    claim_job("w1")
    expire_lease(job_id)

    assert claim_job("w2")["attempts"] == 2    # attempt 2 of 2 is allowed
    expire_lease(job_id)
    assert claim_job("w3") is None             # but there is no attempt 3
    assert get_job(job_id)["status"] == "dead"


def test_dead_poison_job_does_not_block_healthy_jobs(expire_lease):
    poison = enqueue("t", max_attempts=1)
    claim_job("w1")
    expire_lease(poison)
    healthy = enqueue("t")

    assert claim_job("w2")["id"] == healthy
    assert get_job(poison)["status"] == "dead"


# ---------- worker integration ----------

@handler("test_retry_boom")
def _boom(job):
    raise RuntimeError("boom")


def test_worker_waits_for_backoff_before_retrying():
    job_id = enqueue("test_retry_boom", max_attempts=3)
    worker = Worker("w1")

    assert worker.run_once() is True      # first attempt fails
    assert worker.run_once() is False     # job is waiting out its backoff
    assert get_job(job_id)["status"] == "pending"