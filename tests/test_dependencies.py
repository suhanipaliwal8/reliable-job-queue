import threading

import pytest

from jobqueue.core import (
    cancel_job,
    claim_job,
    complete_job,
    enqueue,
    fail_job,
    get_job,
    list_dependencies,
    purge_dead_jobs,
    purge_job,
    replay_job,
)
from jobqueue.db import transaction
from jobqueue.registry import handler
from jobqueue.stats import get_stats
from jobqueue.worker import Worker


def count(table: str) -> int:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            return cur.fetchone()["n"]


# ---------- creating dependent jobs ----------

def test_job_waits_for_unfinished_dependency():
    a = enqueue("t")
    b = enqueue("t", depends_on=[a])

    assert get_job(b)["status"] == "waiting"


def test_waiting_job_is_not_claimable():
    a = enqueue("t")
    b = enqueue("t", depends_on=[a], priority=100)    # even with top priority

    assert claim_job("w1")["id"] == a
    assert claim_job("w2") is None                    # b must keep waiting
    assert get_job(b)["status"] == "waiting"


def test_job_depending_on_finished_job_is_pending_immediately():
    a = enqueue("t")
    claim_job("w1")
    complete_job(a, "w1")

    b = enqueue("t", depends_on=[a])
    assert get_job(b)["status"] == "pending"


def test_unknown_dependency_is_rejected():
    with pytest.raises(ValueError):
        enqueue("t", depends_on=[999_999])

    assert count("jobs") == 0                         # nothing half-created


# ---------- releasing ----------

def test_completing_dependency_releases_waiting_job():
    a = enqueue("t")
    b = enqueue("t", depends_on=[a])

    claim_job("w1")
    assert complete_job(a, "w1") is True

    assert get_job(b)["status"] == "pending"
    assert claim_job("w2")["id"] == b


def test_job_with_two_dependencies_waits_for_both():
    a1 = enqueue("t")
    a2 = enqueue("t")
    b = enqueue("t", depends_on=[a1, a2])

    claim_job("w1")                                   # a1
    complete_job(a1, "w1")
    assert get_job(b)["status"] == "waiting"          # a2 is still unfinished

    claim_job("w1")                                   # a2
    complete_job(a2, "w1")
    assert get_job(b)["status"] == "pending"


order = []


@handler("test_chain")
def _record(job):
    order.append(job["payload"]["name"])


def test_chain_runs_in_dependency_order_regardless_of_priority():
    order.clear()
    a = enqueue("test_chain", {"name": "a"})
    b = enqueue("test_chain", {"name": "b"}, depends_on=[a], priority=5)
    c = enqueue("test_chain", {"name": "c"}, depends_on=[b], priority=10)

    Worker("w1").run_forever(drain=True)

    assert order == ["a", "b", "c"]
    assert all(get_job(j)["status"] == "done" for j in (a, b, c))


def test_retrying_dependency_keeps_dependent_waiting():
    a = enqueue("t", max_attempts=3)
    b = enqueue("t", depends_on=[a])

    claim_job("w1")
    assert fail_job(a, "w1", "boom") == "pending"     # will be retried later

    assert get_job(b)["status"] == "waiting"


def test_released_job_still_honours_its_own_delay():
    a = enqueue("t")
    b = enqueue("t", delay_seconds=60, depends_on=[a])

    claim_job("w1")
    complete_job(a, "w1")

    assert get_job(b)["status"] == "pending"
    assert claim_job("w2") is None                    # released, but not due yet


def test_waiting_time_does_not_count_as_queue_latency():
    a = enqueue("t")
    b = enqueue("t", depends_on=[a])
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET run_at = NOW(6) - INTERVAL 1 HOUR WHERE id = %s", (b,))

    claim_job("w1")
    complete_job(a, "w1")

    age = get_stats()["oldest_ready_age_seconds"]
    assert age is not None and age < 30


def test_concurrent_completions_release_the_dependent():
    """Two dependencies finish at the same instant. Without row locks, each
    transaction would see the other as unfinished and the job would wait forever."""
    for _ in range(10):
        a1 = enqueue("t")
        a2 = enqueue("t")
        b = enqueue("t", depends_on=[a1, a2])
        claim_job("w1")                               # a1
        claim_job("w2")                               # a2

        barrier = threading.Barrier(2)
        errors = []

        def finish(job_id, worker):
            try:
                barrier.wait()
                complete_job(job_id, worker)
            except Exception as exc:                  # exceptions in threads would vanish
                errors.append(exc)

        threads = [threading.Thread(target=finish, args=(a1, "w1")),
                   threading.Thread(target=finish, args=(a2, "w2"))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert get_job(b)["status"] == "pending"

        job = claim_job("w3")                         # tidy up for the next round
        assert job["id"] == b
        complete_job(b, "w3")


# ---------- failure handling ----------

def test_dead_dependency_blocks_dependent(make_dead_job):
    dead_id = make_dead_job()
    b = enqueue("t", depends_on=[dead_id])

    assert get_job(b)["status"] == "waiting"
    assert claim_job("w1") is None


def test_replaying_dead_dependency_releases_dependent_after_success(make_dead_job):
    dead_id = make_dead_job()
    b = enqueue("t", depends_on=[dead_id])

    assert replay_job(dead_id) is True
    assert get_job(b)["status"] == "waiting"          # replay alone releases nothing

    assert claim_job("w1")["id"] == dead_id
    assert complete_job(dead_id, "w1") is True
    assert get_job(b)["status"] == "pending"


# ---------- inspecting ----------

def test_list_dependencies_reports_status():
    a = enqueue("email")
    b = enqueue("t", depends_on=[a])

    assert list_dependencies(b) == [{"id": a, "job_type": "email", "status": "pending"}]


# ---------- cancel ----------

def test_cancel_removes_waiting_job_and_its_dependency_rows():
    a = enqueue("t")
    b = enqueue("t", depends_on=[a])

    assert cancel_job(b) is True
    assert get_job(b) is None
    assert count("job_dependencies") == 0


def test_cancel_refuses_jobs_that_are_not_waiting():
    a = enqueue("t")

    assert cancel_job(a) is False                     # pending, not waiting
    assert get_job(a) is not None
    assert cancel_job(999_999) is False


def test_cancel_refuses_a_job_that_others_depend_on():
    a = enqueue("t")
    b = enqueue("t", depends_on=[a])
    c = enqueue("t", depends_on=[b])

    assert cancel_job(b) is False                     # c still depends on b
    assert cancel_job(c) is True                      # cancel from the end backwards
    assert cancel_job(b) is True


# ---------- purge safety ----------

def test_purge_job_refuses_dead_job_with_dependents(make_dead_job):
    dead_id = make_dead_job()
    b = enqueue("t", depends_on=[dead_id])

    assert purge_job(dead_id) is False
    assert get_job(dead_id) is not None

    assert cancel_job(b) is True
    assert purge_job(dead_id) is True


def test_purge_all_skips_dead_jobs_with_dependents(make_dead_job):
    keep = make_dead_job()
    free = make_dead_job()
    enqueue("t", depends_on=[keep])

    assert purge_dead_jobs() == 1
    assert get_job(keep) is not None
    assert get_job(free) is None