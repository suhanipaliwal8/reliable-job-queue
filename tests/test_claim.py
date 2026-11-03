import threading

from jobqueue.core import claim_job, complete_job, enqueue, extend_lease, fail_job, get_job


def test_claim_marks_job_in_flight():
    job_id = enqueue("t")
    job = claim_job("w1", lease_seconds=30)

    assert job["id"] == job_id
    assert job["status"] == "in_flight"
    assert job["locked_by"] == "w1"
    assert job["attempts"] == 1
    assert job["lease_expires_at"] is not None


def test_claim_returns_none_when_queue_is_empty():
    assert claim_job("w1") is None


def test_second_worker_cannot_claim_an_in_flight_job():
    enqueue("t")
    assert claim_job("w1") is not None
    assert claim_job("w2") is None


def test_jobs_are_claimed_oldest_first():
    first = enqueue("t")
    second = enqueue("t")
    assert claim_job("w1")["id"] == first
    assert claim_job("w1")["id"] == second


def test_expired_lease_can_be_reclaimed_by_another_worker(expire_lease):
    job_id = enqueue("t")
    claim_job("w1")
    expire_lease(job_id)

    job = claim_job("w2")
    assert job["id"] == job_id
    assert job["locked_by"] == "w2"
    assert job["attempts"] == 2


def test_stale_worker_cannot_complete_after_losing_lease(expire_lease):
    job_id = enqueue("t")
    claim_job("w1")
    expire_lease(job_id)
    claim_job("w2")  # w2 takes over

    assert complete_job(job_id, "w1") is False          # w1 is rejected
    assert get_job(job_id)["status"] == "in_flight"

    assert complete_job(job_id, "w2") is True           # the real owner succeeds
    job = get_job(job_id)
    assert job["status"] == "done"
    assert job["locked_by"] is None


def test_fail_job_requeues_until_max_attempts_then_dead():
    job_id = enqueue("t", max_attempts=2)

    claim_job("w1")
    assert fail_job(job_id, "w1", "boom") == "pending"

    claim_job("w1")
    assert fail_job(job_id, "w1", "boom") == "dead"
    assert get_job(job_id)["last_error"] == "boom"


def test_extend_lease_only_works_for_the_owner():
    job_id = enqueue("t")
    claim_job("w1", lease_seconds=5)
    before = get_job(job_id)["lease_expires_at"]

    assert extend_lease(job_id, "w1", lease_seconds=60) is True
    assert get_job(job_id)["lease_expires_at"] > before
    assert extend_lease(job_id, "w2", lease_seconds=60) is False


def test_many_workers_never_claim_the_same_job():
    total = 40
    for i in range(total):
        enqueue("t", {"n": i})

    claimed = []
    lock = threading.Lock()  # protects the shared list

    def work(name):
        while True:
            job = claim_job(name)
            if job is None:
                break
            with lock:
                claimed.append(job["id"])

    threads = [threading.Thread(target=work, args=(f"w{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claimed) == total          # every job was claimed
    assert len(set(claimed)) == total     # and none was claimed twice