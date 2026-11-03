import threading

from jobqueue.core import (
    claim_job,
    complete_job,
    enqueue,
    get_job,
    list_dead_jobs,
    purge_dead_jobs,
    purge_job,
    replay_dead_jobs,
    replay_job,
)
from jobqueue.registry import handler
from jobqueue.worker import Worker

import pytest


# ---------- listing ----------

def test_list_dead_jobs_returns_only_dead_jobs(make_dead_job):
    dead_id = make_dead_job()
    enqueue("t")                                   # a pending job: must not appear

    assert [j["id"] for j in list_dead_jobs()] == [dead_id]


def test_list_dead_jobs_can_filter_by_job_type(make_dead_job):
    make_dead_job("email")
    sms_id = make_dead_job("sms")

    assert [j["id"] for j in list_dead_jobs(job_type="sms")] == [sms_id]


def test_list_dead_jobs_is_newest_first_and_respects_limit(make_dead_job):
    ids = [make_dead_job() for _ in range(3)]

    listed = [j["id"] for j in list_dead_jobs(limit=2)]
    assert listed == [ids[2], ids[1]]


def test_list_limit_must_be_positive():
    with pytest.raises(ValueError):
        list_dead_jobs(limit=0)


# ---------- replaying one job ----------

def test_replay_resets_dead_job_to_pending(make_dead_job):
    job_id = make_dead_job()

    assert replay_job(job_id) is True
    job = get_job(job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["locked_by"] is None
    assert job["lease_expires_at"] is None


def test_replayed_job_is_claimable_immediately_with_fresh_attempts(make_dead_job):
    job_id = make_dead_job()
    replay_job(job_id)

    job = claim_job("w2")
    assert job["id"] == job_id
    assert job["attempts"] == 1


def test_replay_keeps_payload_priority_and_last_error(make_dead_job):
    job_id = make_dead_job("email", {"to": "a@b.com"}, priority=7)
    replay_job(job_id)

    job = get_job(job_id)
    assert job["payload"] == {"to": "a@b.com"}
    assert job["priority"] == 7
    assert job["last_error"] == "boom"


def test_replay_refuses_jobs_that_are_not_dead():
    job_id = enqueue("t")
    assert replay_job(job_id) is False             # pending

    claim_job("w1")
    assert replay_job(job_id) is False             # in_flight
    assert get_job(job_id)["locked_by"] == "w1"    # untouched

    complete_job(job_id, "w1")
    assert replay_job(job_id) is False             # done
    assert get_job(job_id)["status"] == "done"


def test_replay_of_unknown_job_returns_false():
    assert replay_job(999_999) is False


def test_concurrent_replays_only_one_succeeds(make_dead_job):
    job_id = make_dead_job()
    results = []
    lock = threading.Lock()

    def attempt():
        ok = replay_job(job_id)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1
    assert results.count(False) == 7
    assert get_job(job_id)["status"] == "pending"


# ---------- replaying many ----------

def test_replay_dead_jobs_replays_everything_and_returns_count(make_dead_job):
    a = make_dead_job()
    b = make_dead_job()

    assert replay_dead_jobs() == 2
    assert get_job(a)["status"] == "pending"
    assert get_job(b)["status"] == "pending"
    assert replay_dead_jobs() == 0                 # nothing left to replay


def test_replay_dead_jobs_can_be_limited_to_one_job_type(make_dead_job):
    email = make_dead_job("email")
    sms = make_dead_job("sms")

    assert replay_dead_jobs(job_type="email") == 1
    assert get_job(email)["status"] == "pending"
    assert get_job(sms)["status"] == "dead"


# ---------- purging ----------

def test_purge_job_deletes_only_dead_jobs(make_dead_job):
    dead_id = make_dead_job()
    pending_id = enqueue("t")

    assert purge_job(dead_id) is True
    assert get_job(dead_id) is None

    assert purge_job(pending_id) is False          # safety: never deletes live jobs
    assert get_job(pending_id) is not None


def test_purge_dead_jobs_returns_count_and_keeps_other_jobs(make_dead_job):
    make_dead_job()
    make_dead_job()
    pending_id = enqueue("t")

    assert purge_dead_jobs() == 2
    assert list_dead_jobs() == []
    assert get_job(pending_id) is not None


# ---------- end to end ----------

service = {"healthy": False}


@handler("test_flaky_service")
def _flaky(job):
    if not service["healthy"]:
        raise RuntimeError("service down")


def test_dead_job_can_be_fixed_and_replayed_to_success():
    service["healthy"] = False
    job_id = enqueue("test_flaky_service", max_attempts=1)

    Worker("w1").run_once()
    assert get_job(job_id)["status"] == "dead"       # the outage killed it

    service["healthy"] = True                        # the "bug" is fixed
    assert replay_job(job_id) is True
    Worker("w1").run_once()

    assert get_job(job_id)["status"] == "done"