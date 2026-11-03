import threading

import pytest

from jobqueue import handlers
from jobqueue.core import enqueue, get_job, replay_job
from jobqueue.db import transaction
from jobqueue.effects import run_once
from jobqueue.handlers import charge_and_receipt
from jobqueue.worker import Worker


def count(table: str) -> int:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            return cur.fetchone()["n"]


def order(order_id="o1", **extra) -> dict:
    return {"order_id": order_id, "customer": "asha", "amount_cents": "49900", **extra}


def insert_one_charge(cur):
    cur.execute(
        "INSERT INTO charges (order_id, customer, amount_cents) VALUES (%s, %s, %s)",
        ("o1", "asha", 100),
    )


# ---------- run_once ----------

def test_run_once_runs_the_action_the_first_time():
    ran = []
    assert run_once("k1", lambda cur: ran.append(1)) is True
    assert ran == [1]


def test_run_once_skips_the_action_the_second_time():
    ran = []
    run_once("k1", lambda cur: ran.append(1))

    assert run_once("k1", lambda cur: ran.append(2)) is False
    assert ran == [1]


def test_different_keys_are_independent():
    assert run_once("k1", lambda cur: None) is True
    assert run_once("k2", lambda cur: None) is True


def test_failed_action_rolls_back_the_key_too():
    def explode(cur):
        insert_one_charge(cur)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        run_once("k1", explode)

    assert count("charges") == 0            # the effect was rolled back
    assert count("side_effects") == 0       # and so was the key
    assert run_once("k1", lambda cur: None) is True   # a retry may try again


def test_concurrent_callers_only_one_wins():
    results = []
    lock = threading.Lock()

    def attempt():
        ok = run_once("same-key", insert_one_charge)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1
    assert results.count(False) == 7
    assert count("charges") == 1


def test_run_once_records_the_job_id():
    run_once("k1", lambda cur: None, job_id=42)

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT job_id FROM side_effects WHERE idempotency_key = 'k1'")
            assert cur.fetchone()["job_id"] == 42


def test_empty_or_oversized_key_is_rejected():
    with pytest.raises(ValueError):
        run_once("", lambda cur: None)
    with pytest.raises(ValueError):
        run_once("x" * 201, lambda cur: None)


# ---------- the payment demo, end to end ----------

def test_naive_handler_double_charges_after_a_crash(make_due):
    job_id = enqueue("naive_charge_and_receipt",
                     order(fail_after_charge="1"), max_attempts=3)
    worker = Worker("w1")

    worker.run_once()                       # attempt 1: charges, then crashes
    make_due(job_id)                        # skip the backoff wait
    worker.run_once()                       # attempt 2: charges AGAIN

    assert get_job(job_id)["status"] == "done"
    assert count("charges") == 2            # <- the bug
    assert count("receipts") == 1


def test_idempotent_handler_charges_once_even_after_a_crash(make_due):
    job_id = enqueue("charge_and_receipt",
                     order(fail_after_charge="1"), max_attempts=3)
    worker = Worker("w1")

    worker.run_once()                       # attempt 1: charges, then crashes
    assert count("charges") == 1
    assert count("receipts") == 0           # the crash left the job half-done

    make_due(job_id)
    worker.run_once()                       # attempt 2: skips charge, sends receipt

    assert get_job(job_id)["status"] == "done"
    assert count("charges") == 1            # not double-applied
    assert count("receipts") == 1


def test_calling_the_handler_twice_applies_effects_once():
    job = {"id": 1, "attempts": 2, "payload": order()}

    charge_and_receipt(job)
    charge_and_receipt(job)                 # duplicate delivery

    assert count("charges") == 1
    assert count("receipts") == 1


def test_two_jobs_for_the_same_order_charge_once():
    first = enqueue("charge_and_receipt", order("o1"))
    second = enqueue("charge_and_receipt", order("o1"))   # e.g. a double-clicked "Pay"

    Worker("w1").run_forever(drain=True)

    assert get_job(first)["status"] == "done"
    assert get_job(second)["status"] == "done"
    assert count("charges") == 1
    assert count("receipts") == 1


def test_different_orders_are_charged_separately():
    enqueue("charge_and_receipt", order("o1"))
    enqueue("charge_and_receipt", order("o2"))

    Worker("w1").run_forever(drain=True)

    assert count("charges") == 2
    assert count("receipts") == 2


def test_replayed_dead_job_does_not_double_charge(monkeypatch):
    job_id = enqueue("charge_and_receipt",
                     order(fail_after_charge="1"), max_attempts=1)

    Worker("w1").run_once()                 # charges, crashes, no attempts left
    assert get_job(job_id)["status"] == "dead"
    assert count("charges") == 1

    # "Fix the bug": stop injecting the fault, then replay (Phase 5).
    monkeypatch.setattr(handlers, "_simulate_trouble_between_steps", lambda job: None)
    assert replay_job(job_id) is True
    Worker("w1").run_once()

    assert get_job(job_id)["status"] == "done"
    assert count("charges") == 1            # the replay did not charge again
    assert count("receipts") == 1