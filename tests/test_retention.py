import pytest

from jobqueue.cli import main
from jobqueue.core import (
    claim_job,
    complete_job,
    count_prunable_done_jobs,
    enqueue,
    get_job,
    prune_done_jobs,
)
from jobqueue.db import transaction
from jobqueue.effects import run_once


def count(table: str) -> int:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            return cur.fetchone()["n"]


def finish_job(job_type="t") -> int:
    """Enqueue a job and run it to 'done' via the real claim -> complete path.
    Only call this while no other job is claimable."""
    job_id = enqueue(job_type)
    job = claim_job("fixture-worker")
    assert job is not None and job["id"] == job_id
    assert complete_job(job_id, "fixture-worker") is True
    return job_id


def test_prune_removes_only_old_done_jobs(make_dead_job, age_job):
    old_done = finish_job()
    recent_done = finish_job()
    dead = make_dead_job()
    in_flight = enqueue("t")
    claim_job("w1")                       # takes in_flight (the only claimable job)
    pending = enqueue("t")
    for job_id in (old_done, dead, in_flight, pending):
        age_job(job_id, 3600)             # everything is old, but only 'done' may go

    assert prune_done_jobs(older_than_seconds=60) == 1

    assert get_job(old_done) is None
    for job_id in (recent_done, dead, in_flight, pending):
        assert get_job(job_id) is not None


def test_prune_returns_zero_when_nothing_is_old_enough():
    finish_job()

    assert prune_done_jobs(older_than_seconds=3600) == 0
    assert count("jobs") == 1


def test_prune_works_in_batches(age_job):
    for job_id in [finish_job() for _ in range(5)]:
        age_job(job_id, 3600)

    assert prune_done_jobs(older_than_seconds=60, batch_size=2) == 5
    assert count("jobs") == 0


def test_count_prunable_matches_and_deletes_nothing(age_job):
    old = [finish_job(), finish_job()]
    finish_job()                          # recent
    for job_id in old:
        age_job(job_id, 3600)

    assert count_prunable_done_jobs(older_than_seconds=60) == 2
    assert count("jobs") == 3


def test_prune_cleans_dependency_rows_and_keeps_dependents_working(age_job):
    parent = finish_job()
    child = enqueue("t", depends_on=[parent])        # parent is done, so child is pending
    assert count("job_dependencies") == 1
    age_job(parent, 3600)

    assert prune_done_jobs(older_than_seconds=60) == 1

    assert count("job_dependencies") == 0
    assert claim_job("w1")["id"] == child
    assert complete_job(child, "w1") is True


def test_pruning_a_finished_parent_does_not_strand_a_waiting_child(age_job):
    a1 = finish_job()
    a2 = enqueue("t")
    child = enqueue("t", depends_on=[a1, a2])        # waits for a2
    assert get_job(child)["status"] == "waiting"
    age_job(a1, 3600)

    assert prune_done_jobs(older_than_seconds=60) == 1

    assert claim_job("w1")["id"] == a2
    complete_job(a2, "w1")
    assert get_job(child)["status"] == "pending"     # released despite a1 being gone


def test_prune_does_not_touch_idempotency_keys(age_job):
    job_id = finish_job()
    run_once("charge:order-1", lambda cur: None, job_id)
    age_job(job_id, 3600)

    assert prune_done_jobs(older_than_seconds=60) == 1
    assert count("side_effects") == 1


def test_prune_rejects_invalid_arguments():
    with pytest.raises(ValueError):
        prune_done_jobs(-1)
    with pytest.raises(ValueError):
        prune_done_jobs(60, batch_size=0)
    with pytest.raises(ValueError):
        count_prunable_done_jobs(-1)


def test_cli_prune_dry_run_deletes_nothing(age_job, capsys):
    job_id = finish_job()
    age_job(job_id, 8 * 86400)

    main(["prune", "--days", "7", "--dry-run"])

    assert "would delete 1" in capsys.readouterr().out
    assert get_job(job_id) is not None


def test_cli_prune_deletes_old_done_jobs(age_job, capsys):
    job_id = finish_job()
    age_job(job_id, 8 * 86400)

    main(["prune", "--days", "7"])

    assert "Deleted 1" in capsys.readouterr().out
    assert get_job(job_id) is None