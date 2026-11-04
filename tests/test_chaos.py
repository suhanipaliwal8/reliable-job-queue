import pytest

from jobqueue.chaos import ChaosError, render_report, run_chaos
from jobqueue.core import enqueue
from jobqueue.db import transaction
from jobqueue.handlers import chaos_order, chaos_order_naive


def count(table: str) -> int:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            return cur.fetchone()["n"]


def make_job(job_id=1) -> dict:
    return {
        "id": job_id,
        "attempts": 1,
        "payload": {"order_id": "o1", "customer": "asha",
                    "amount_cents": 100, "step_delay": 0},
    }


def sample_report(**overrides) -> dict:
    report = {
        "run_id": "abc12345", "mode": "idempotent", "jobs": 10, "workers": 2,
        "kills": 3, "interrupted": 2, "elapsed_seconds": 5.0, "timed_out": False,
        "done": 10, "dead": 0, "unfinished": 0,
        "retried_jobs": 2, "max_attempts_used": 3,
        "charges": 10, "charge_orders": 10, "receipts": 10, "receipt_orders": 10,
        "checks": {"every job completed (none lost, stuck or dead)": True,
                   "exactly one charge per order": True,
                   "exactly one receipt per order": True},
        "ok": True,
    }
    report.update(overrides)
    return report


# ---------- the real thing: separate processes, hard kills ----------

def test_chaos_run_preserves_every_invariant():
    report = run_chaos(jobs=40, workers=3, kills=3, kill_interval=0.7,
                       step_delay=0.1, lease_seconds=2, timeout=90, seed=1)

    assert report["ok"], render_report(report)
    assert report["done"] == 40
    assert report["charges"] == 40
    assert report["receipts"] == 40
    assert report["kills"] >= 1               # the chaos really happened


def test_chaos_cleanup_removes_only_its_own_rows():
    report = run_chaos(jobs=10, workers=2, kills=0, step_delay=0.05,
                       lease_seconds=3, timeout=60, cleanup=True)

    assert report["ok"], render_report(report)
    for table in ("jobs", "charges", "receipts", "side_effects"):
        assert count(table) == 0


# ---------- guard rails ----------

def test_chaos_refuses_to_start_when_the_queue_is_busy():
    enqueue("t")

    with pytest.raises(ChaosError):
        run_chaos(jobs=1, workers=1, kills=0)


def test_chaos_rejects_invalid_arguments():
    with pytest.raises(ChaosError):
        run_chaos(jobs=0)
    with pytest.raises(ChaosError):
        run_chaos(workers=0)
    with pytest.raises(ChaosError):
        run_chaos(kills=-1)
    with pytest.raises(ChaosError):
        run_chaos(lease_seconds=0)


# ---------- the two handlers, without any processes (deterministic) ----------

def test_idempotent_chaos_handler_is_safe_to_run_twice():
    chaos_order(make_job())
    chaos_order(make_job())              # what a retry after a crash looks like

    assert count("charges") == 1
    assert count("receipts") == 1


def test_naive_chaos_handler_double_applies_when_run_twice():
    chaos_order_naive(make_job())
    chaos_order_naive(make_job())        # same retry, but no idempotency keys

    assert count("charges") == 2
    assert count("receipts") == 2


# ---------- report rendering ----------

def test_render_report_for_a_passing_run():
    text = render_report(sample_report())

    assert "RESULT: PASS" in text
    assert "[FAIL]" not in text


def test_render_report_for_a_failing_naive_run():
    failing = sample_report(
        mode="naive", charges=14, charge_orders=10, ok=False,
        checks={"every job completed (none lost, stuck or dead)": True,
                "exactly one charge per order": False,
                "exactly one receipt per order": True},
    )
    text = render_report(failing)

    assert "RESULT: FAIL" in text
    assert "[FAIL] exactly one charge per order" in text
    assert "expected" in text            # explains that naive failing is intended