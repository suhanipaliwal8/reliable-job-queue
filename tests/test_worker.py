from jobqueue.core import claim_job, enqueue, get_job
from jobqueue.registry import handler
from jobqueue.worker import Worker

calls = []


@handler("test_ok")
def _ok(job):
    calls.append(job["id"])


@handler("test_boom")
def _boom(job):
    raise RuntimeError("boom")


def test_worker_runs_job_and_marks_it_done():
    calls.clear()
    job_id = enqueue("test_ok")

    assert Worker("w1").run_once() is True
    assert calls == [job_id]
    assert get_job(job_id)["status"] == "done"


def test_run_once_returns_false_when_queue_is_empty():
    assert Worker("w1").run_once() is False


def test_failing_handler_requeues_job_and_stores_error():
    job_id = enqueue("test_boom", max_attempts=3)
    Worker("w1").run_once()

    job = get_job(job_id)
    assert job["status"] == "pending"
    assert "boom" in job["last_error"]


def test_failing_job_becomes_dead_after_max_attempts(make_due):
    job_id = enqueue("test_boom", max_attempts=2)
    worker = Worker("w1")
    worker.run_once()
    make_due(job_id)                      # skip the backoff wait
    worker.run_once()

    assert get_job(job_id)["status"] == "dead"
    assert worker.run_once() is False     # dead jobs are never picked up again


def test_unknown_job_type_fails_cleanly():
    job_id = enqueue("no_such_type", max_attempts=1)
    Worker("w1").run_once()

    job = get_job(job_id)
    assert job["status"] == "dead"
    assert "No handler" in job["last_error"]


def test_job_left_behind_by_a_dead_worker_is_recovered(expire_lease):
    calls.clear()
    job_id = enqueue("test_ok")

    claim_job("dead-worker")      # claims the job... and "crashes" (never finishes)
    expire_lease(job_id)          # time passes, lease expires

    Worker("w2").run_once()
    assert calls == [job_id]
    assert get_job(job_id)["status"] == "done"