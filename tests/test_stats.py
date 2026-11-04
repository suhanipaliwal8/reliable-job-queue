import pytest

from jobqueue.core import claim_job, complete_job, enqueue, list_in_flight_jobs
from jobqueue.stats import get_stats, get_stats_by_type


# ---------- get_stats ----------

def test_empty_queue_has_zero_counts_and_no_timings():
    stats = get_stats()

    for key in ("pending", "pending_ready", "pending_scheduled", "in_flight",
                "in_flight_active", "in_flight_expired", "done", "dead", "total"):
        assert stats[key] == 0
    assert stats["oldest_ready_age_seconds"] is None
    assert stats["next_scheduled_in_seconds"] is None


def test_stats_separate_ready_and_scheduled_pending_jobs():
    enqueue("t")
    enqueue("t", delay_seconds=60)

    stats = get_stats()
    assert stats["pending_ready"] == 1
    assert stats["pending_scheduled"] == 1
    assert stats["pending"] == 2
    assert 50 < stats["next_scheduled_in_seconds"] <= 60


def test_stats_separate_healthy_and_expired_leases(expire_lease):
    first = enqueue("t")
    enqueue("t")
    claim_job("w1")                      # takes the oldest job, `first`
    claim_job("w2")
    expire_lease(first)

    stats = get_stats()
    assert stats["in_flight"] == 2
    assert stats["in_flight_active"] == 1
    assert stats["in_flight_expired"] == 1


def test_stats_count_done_and_dead_jobs_and_total(make_dead_job):
    make_dead_job()
    done_id = enqueue("t")
    claim_job("w1")
    complete_job(done_id, "w1")

    stats = get_stats()
    assert stats["done"] == 1
    assert stats["dead"] == 1
    assert stats["total"] == 2


def test_oldest_ready_age_reflects_how_long_a_job_has_waited(make_due):
    job_id = enqueue("t")
    make_due(job_id)                     # pretend it became runnable 1 second ago

    age = get_stats()["oldest_ready_age_seconds"]
    assert 1.0 <= age < 30


def test_scheduled_jobs_are_not_counted_as_waiting():
    enqueue("t", delay_seconds=60)

    stats = get_stats()
    assert stats["pending_ready"] == 0
    assert stats["oldest_ready_age_seconds"] is None


def test_stats_by_type_groups_counts(make_dead_job):
    make_dead_job("email")
    enqueue("email")
    enqueue("sms")

    assert get_stats_by_type() == [
        {"job_type": "email", "pending": 1, "waiting": 0, "in_flight": 0, "done": 0, "dead": 1},
        {"job_type": "sms", "pending": 1, "waiting": 0, "in_flight": 0, "done": 0, "dead": 0},
    ]


# ---------- list_in_flight_jobs ----------

def test_list_in_flight_returns_only_jobs_held_by_workers():
    first = enqueue("t")
    enqueue("t")
    claim_job("w1")                      # claims `first`

    jobs = list_in_flight_jobs()
    assert [j["id"] for j in jobs] == [first]
    assert jobs[0]["locked_by"] == "w1"


def test_in_flight_shows_remaining_lease_time():
    enqueue("t")
    claim_job("w1", lease_seconds=60)

    remaining = list_in_flight_jobs()[0]["lease_remaining_seconds"]
    assert 50 < remaining <= 60


def test_expired_lease_has_negative_remaining_time(expire_lease):
    job_id = enqueue("t")
    claim_job("w1")
    expire_lease(job_id)

    assert list_in_flight_jobs()[0]["lease_remaining_seconds"] < 0


def test_in_flight_jobs_are_sorted_by_soonest_expiry():
    slow = enqueue("t")
    quick = enqueue("t")
    claim_job("w1", lease_seconds=100)   # takes `slow`
    claim_job("w2", lease_seconds=10)    # takes `quick`

    assert [j["id"] for j in list_in_flight_jobs()] == [quick, slow]


def test_in_flight_limit_must_be_positive():
    with pytest.raises(ValueError):
        list_in_flight_jobs(limit=0)

def test_stats_count_waiting_and_blocked_jobs(make_dead_job):
    dead_id = make_dead_job()
    enqueue("t", depends_on=[dead_id])        # waiting, blocked by a dead job
    root = enqueue("t")
    enqueue("t", depends_on=[root])           # waiting, but healthy

    stats = get_stats()
    assert stats["waiting"] == 2
    assert stats["waiting_blocked"] == 1
    assert stats["pending_ready"] == 1
    assert stats["total"] == 4