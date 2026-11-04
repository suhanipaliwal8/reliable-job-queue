"""A guided, self-cleaning tour of the whole system (python -m jobqueue demo)."""
import logging
import uuid

from .core import enqueue, list_dead_jobs
from .db import transaction
from .stats import get_stats
from .worker import Worker


class DemoError(Exception):
    """The demo could not run (for example, the queue is busy)."""


def _section(title: str) -> None:
    print(f"\n=== {title} ===")


def _cleanup(job_ids: list[int], run_id: str) -> None:
    """Delete everything the demo created, and nothing else."""
    marks = ", ".join(["%s"] * len(job_ids))
    like = f"{run_id}-%"
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM job_dependencies "
                f"WHERE job_id IN ({marks}) OR depends_on_id IN ({marks})",
                tuple(job_ids) * 2,
            )
            cur.execute(f"DELETE FROM jobs WHERE id IN ({marks})", tuple(job_ids))
            cur.execute("DELETE FROM charges WHERE order_id LIKE %s", (like,))
            cur.execute("DELETE FROM receipts WHERE order_id LIKE %s", (like,))
            cur.execute(
                "DELETE FROM side_effects "
                "WHERE idempotency_key LIKE %s OR idempotency_key LIKE %s",
                (f"charge:{like}", f"receipt:{like}"),
            )


def run_demo(render_stats) -> None:
    """Enqueue a mix of jobs, run a worker, show the result, then clean up.
    `render_stats` turns a stats dict into printable text (supplied by the CLI)."""
    stats = get_stats()
    if stats["pending"] + stats["in_flight"] > 0:
        raise DemoError(
            "The queue is not idle (pending or in-flight jobs exist). The demo worker "
            "would process them too. Let them finish, or clear them first."
        )

    run_id = uuid.uuid4().hex[:8]
    order_id = f"{run_id}-demo"
    job_ids: list[int] = []

    def add(*args, **kwargs) -> int:
        job_id = enqueue(*args, **kwargs)
        job_ids.append(job_id)
        return job_id

    payment = {"order_id": order_id, "customer": "asha", "amount_cents": 49900}

    try:
        _section("1. Enqueue a mix of jobs")
        low = add("send_email", {"to": "low@example.com"})
        urgent = add("send_email", {"to": "URGENT@example.com"}, priority=10)
        later = add("send_email", {"to": "later@example.com"}, delay_seconds=3600)
        doomed = add("always_fail", max_attempts=1)
        charge = add("charge_and_receipt", payment)
        notify = add("send_email", {"to": "asha@example.com", "subject": "Receipt"},
                     depends_on=[charge])
        double_click = add("charge_and_receipt", payment)
        print(f"  job {low:>4}  low-priority email")
        print(f"  job {urgent:>4}  URGENT email (priority 10: should run first)")
        print(f"  job {later:>4}  email delayed by 1 hour (stays scheduled)")
        print(f"  job {doomed:>4}  job that always fails, 1 attempt (will go dead)")
        print(f"  job {charge:>4}  charge a customer for one order")
        print(f"  job {notify:>4}  receipt email, waits for job {charge} (dependency)")
        print(f"  job {double_click:>4}  the SAME order again (a double-clicked 'Pay')")

        _section("2. Queue state before any worker runs")
        print(render_stats(get_stats()))

        _section("3. Run one worker until nothing is due")
        logging.basicConfig(level=logging.INFO, format="    worker> %(message)s")
        Worker("demo-worker", lease_seconds=30, poll_interval=0.1).run_forever(drain=True)
        print("  (read the log above: the URGENT email ran first, and the receipt "
              "email only after the charge)")

        _section("4. Queue state afterwards")
        print(render_stats(get_stats()))
        dead = [j for j in list_dead_jobs(limit=50) if j["id"] in job_ids]
        for job in dead:
            print(f"\n  dead job {job['id']} ({job['job_type']}): {job['last_error']}")

        _section("5. Idempotency check")
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM charges WHERE order_id = %s",
                            (order_id,))
                charges = cur.fetchone()["n"]
        print(f"  Charges recorded for this order: {charges}")
        print("  Two jobs ran for the same order, but the customer was charged once.")
    finally:
        _cleanup(job_ids, run_id)
        print("\nDemo data removed (its jobs, charges and idempotency keys).")