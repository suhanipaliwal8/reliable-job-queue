import pytest

from jobqueue.core import enqueue
from jobqueue.db import transaction
from jobqueue.demo import DemoError, run_demo


def count(table: str) -> int:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            return cur.fetchone()["n"]


def test_demo_runs_end_to_end_and_cleans_up_after_itself(capsys):
    run_demo(lambda stats: "STATS")

    output = capsys.readouterr().out
    assert "Charges recorded for this order: 1" in output   # idempotency held
    assert "dead job" in output                             # the doomed job died
    for table in ("jobs", "job_dependencies", "charges", "receipts", "side_effects"):
        assert count(table) == 0


def test_demo_refuses_to_run_on_a_busy_queue():
    enqueue("t")

    with pytest.raises(DemoError):
        run_demo(lambda stats: "")