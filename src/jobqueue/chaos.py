"""Chaos test: hard-kill real worker processes mid-job, then verify nothing broke."""
import random
import subprocess
import sys
import time
import uuid

from .core import enqueue
from .db import transaction
from .stats import get_stats


class ChaosError(Exception):
    """The chaos test could not run (bad arguments, busy queue, broken worker)."""


def _in_list(count: int) -> str:
    """Build '%s, %s, %s' for an IN (...) clause. Only placeholders are built here;
    the actual values are still passed separately, so this is injection-safe."""
    return ", ".join(["%s"] * count)


def _spawn_worker(worker_id: str, lease_seconds: float) -> subprocess.Popen:
    """Start a real worker process, exactly as you would from a terminal."""
    return subprocess.Popen(
        [
            sys.executable, "-m", "jobqueue", "worker",
            "--worker-id", worker_id,
            "--lease-seconds", str(lease_seconds),
            "--poll-interval", "0.2",
            "--backoff-base", "0.5",
            "--backoff-cap", "5",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _status_counts(job_ids: list[int]) -> dict:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status, COUNT(*) AS n FROM jobs "
                f"WHERE id IN ({_in_list(len(job_ids))}) GROUP BY status",
                tuple(job_ids),
            )
            return {row["status"]: int(row["n"]) for row in cur.fetchall()}


def _all_finished(job_ids: list[int]) -> bool:
    counts = _status_counts(job_ids)
    return counts.get("done", 0) + counts.get("dead", 0) == len(job_ids)


def _jobs_held_by(worker_id: str) -> int:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE locked_by = %s AND status = 'in_flight'",
                (worker_id,),
            )
            return int(cur.fetchone()["n"])


def _cleanup(job_ids: list[int], run_id: str) -> None:
    """Delete everything this run created (and nothing else)."""
    like = f"{run_id}-%"
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM jobs WHERE id IN ({_in_list(len(job_ids))})",
                        tuple(job_ids))
            cur.execute("DELETE FROM charges WHERE order_id LIKE %s", (like,))
            cur.execute("DELETE FROM receipts WHERE order_id LIKE %s", (like,))
            cur.execute(
                "DELETE FROM side_effects "
                "WHERE idempotency_key LIKE %s OR idempotency_key LIKE %s",
                (f"charge:{like}", f"receipt:{like}"),
            )


def run_chaos(
    jobs: int = 100,
    workers: int = 4,
    kills: int = 6,
    kill_interval: float = 1.0,
    step_delay: float = 0.1,
    lease_seconds: float = 3.0,
    max_attempts: int = 50,
    timeout: float = 120.0,
    naive: bool = False,
    seed: int | None = None,
    cleanup: bool = False,
) -> dict:
    """Enqueue `jobs` payment jobs, run `workers` real worker processes, hard-kill
    up to `kills` of them at random moments (each is replaced by a fresh worker),
    wait until every job is finished, then check the invariants.

    Returns a report dict; report["ok"] is True when every invariant held.
    """
    if jobs < 1 or workers < 1:
        raise ChaosError("jobs and workers must each be at least 1")
    if kills < 0:
        raise ChaosError("kills must not be negative")
    if kill_interval <= 0 or lease_seconds <= 0 or timeout <= 0:
        raise ChaosError("kill_interval, lease_seconds and timeout must be positive")
    if step_delay < 0:
        raise ChaosError("step_delay must not be negative")

    stats = get_stats()
    if stats["pending"] + stats["in_flight"] > 0:
        raise ChaosError(
            "The queue is not idle (pending or in-flight jobs exist). The chaos "
            "workers would process them too. Let them finish, or clear them first."
        )

    run_id = uuid.uuid4().hex[:8]
    rng = random.Random(seed)
    job_type = "chaos_order_naive" if naive else "chaos_order"

    job_ids = [
        enqueue(
            job_type,
            {"order_id": f"{run_id}-{i}", "customer": "chaos",
             "amount_cents": 100, "step_delay": step_delay},
            max_attempts=max_attempts,
        )
        for i in range(jobs)
    ]

    spawned = 0

    def spawn() -> tuple[str, subprocess.Popen]:
        nonlocal spawned
        spawned += 1
        worker_id = f"chaos-{run_id}-{spawned}"
        return worker_id, _spawn_worker(worker_id, lease_seconds)

    fleet = [spawn() for _ in range(workers)]
    kills_done = 0
    interrupted = 0          # kills that hit a worker which was holding a job
    timed_out = False
    started = time.monotonic()
    next_kill = started + kill_interval

    try:
        while True:
            if _all_finished(job_ids):
                break
            now = time.monotonic()
            if now - started > timeout:
                timed_out = True
                break

            if kills_done < kills and now >= next_kill:
                index = rng.randrange(len(fleet))
                worker_id, proc = fleet[index]
                if _jobs_held_by(worker_id) > 0:
                    interrupted += 1
                proc.kill()                  # HARD kill: no cleanup code runs
                proc.wait()
                fleet[index] = spawn()       # a fresh worker takes its place
                kills_done += 1
                next_kill = now + kill_interval * rng.uniform(0.5, 1.5)

            for worker_id, proc in fleet:    # a worker dying by itself is a setup bug
                if proc.poll() is not None:
                    raise ChaosError(
                        f"Worker {worker_id} exited unexpectedly (code {proc.returncode}). "
                        "Run `python -m jobqueue worker` by hand to see the error."
                    )
            time.sleep(0.1)
    finally:
        for _, proc in fleet:
            if proc.poll() is None:
                proc.kill()
        for _, proc in fleet:
            proc.wait()

    elapsed = time.monotonic() - started

    # ---- verification ----
    counts = _status_counts(job_ids)
    like = f"{run_id}-%"
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(attempts > 1), 0) AS retried, "
                "COALESCE(MAX(attempts), 0) AS most "
                f"FROM jobs WHERE id IN ({_in_list(len(job_ids))})",
                tuple(job_ids),
            )
            attempts = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) AS total, COUNT(DISTINCT order_id) AS orders "
                "FROM charges WHERE order_id LIKE %s", (like,))
            charges = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) AS total, COUNT(DISTINCT order_id) AS orders "
                "FROM receipts WHERE order_id LIKE %s", (like,))
            receipts = cur.fetchone()

    done = counts.get("done", 0)
    dead = counts.get("dead", 0)
    checks = {
        "every job completed (none lost, stuck or dead)": done == jobs,
        "exactly one charge per order": charges["total"] == jobs and charges["orders"] == jobs,
        "exactly one receipt per order": receipts["total"] == jobs and receipts["orders"] == jobs,
    }

    report = {
        "run_id": run_id,
        "mode": "naive" if naive else "idempotent",
        "jobs": jobs,
        "workers": workers,
        "kills": kills_done,
        "interrupted": interrupted,
        "elapsed_seconds": elapsed,
        "timed_out": timed_out,
        "done": done,
        "dead": dead,
        "unfinished": jobs - done - dead,
        "retried_jobs": int(attempts["retried"]),
        "max_attempts_used": int(attempts["most"]),
        "charges": int(charges["total"]),
        "charge_orders": int(charges["orders"]),
        "receipts": int(receipts["total"]),
        "receipt_orders": int(receipts["orders"]),
        "checks": checks,
        "ok": all(checks.values()),
    }

    if cleanup:
        _cleanup(job_ids, run_id)
    return report


def render_report(report: dict) -> str:
    jobs = report["jobs"]
    elapsed = f"{report['elapsed_seconds']:.1f}s" + ("  (TIMED OUT)" if report["timed_out"] else "")
    lines = [
        f"CHAOS TEST  (mode: {report['mode']}, run {report['run_id']})",
        "",
        f"  workers                        {report['workers']}",
        f"  hard kills                     {report['kills']}"
        f"  ({report['interrupted']} hit a worker that was holding a job)",
        f"  jobs that needed a retry       {report['retried_jobs']}"
        f"  (most attempts on one job: {report['max_attempts_used']})",
        f"  elapsed                        {elapsed}",
        "",
        f"  jobs done / dead / unfinished  {report['done']} / {report['dead']} / {report['unfinished']}"
        f"  (of {jobs})",
        f"  charges                        {report['charges']}  (expected {jobs})",
        f"  receipts                       {report['receipts']}  (expected {jobs})",
        "",
    ]
    for name, passed in report["checks"].items():
        lines.append(f"  [{'PASS' if passed else 'FAIL'}] {name}")
    lines.append("")

    if report["ok"]:
        lines.append("RESULT: PASS. Jobs survived every crash and no effect was applied twice.")
    else:
        lines.append("RESULT: FAIL")
        if report["mode"] == "naive":
            lines.append("(expected: the naive handler is broken on purpose, so crashes "
                         "followed by retries apply effects twice)")
    return "\n".join(lines)