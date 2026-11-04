import json

from .db import transaction
from .backoff import compute_backoff

def _decode_payload(job: dict | None) -> dict | None:
    """MySQL returns JSON columns as text; turn the payload back into a dict."""
    if job and isinstance(job["payload"], str):
        job["payload"] = json.loads(job["payload"])
    return job

def enqueue(
    job_type: str,
    payload: dict | None = None,
    max_attempts: int = 5,
    priority: int = 0,
    delay_seconds: float = 0,
    depends_on: list[int] | None = None,
) -> int:
    """Add a job to the queue. Returns the new job's id.

    priority:      higher number = more urgent (default 0, negatives allowed).
    delay_seconds: the job will not run before NOW + this many seconds
                   (measured by the DATABASE clock).
    depends_on:    ids of jobs that must be 'done' first. If any of them is not
                   done yet, the job starts as 'waiting'. Dependencies must
                   already exist, which is what makes cycles impossible.
    """
    if not job_type:
        raise ValueError("job_type must not be empty")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must not be negative")
    payload = payload or {}
    dependency_ids = sorted(set(depends_on or []))

    with transaction() as conn:
        with conn.cursor() as cur:
            status = "pending"
            if dependency_ids:
                # Lock the dependency rows (in id order, to avoid deadlocks) so a
                # dependency can't finish between our check and our INSERT.
                marks = ", ".join(["%s"] * len(dependency_ids))
                cur.execute(
                    f"SELECT id, status FROM jobs WHERE id IN ({marks}) ORDER BY id FOR UPDATE",
                    tuple(dependency_ids),
                )
                found = {row["id"]: row["status"] for row in cur.fetchall()}
                missing = [i for i in dependency_ids if i not in found]
                if missing:
                    raise ValueError(f"depends_on refers to unknown job id(s): {missing}")
                if any(s != "done" for s in found.values()):
                    status = "waiting"

            cur.execute(
                """
                INSERT INTO jobs (job_type, payload, max_attempts, priority, status, run_at)
                VALUES (%s, %s, %s, %s, %s, NOW(6) + INTERVAL %s SECOND)
                """,
                (job_type, json.dumps(payload), max_attempts, priority, status, delay_seconds),
            )
            job_id = cur.lastrowid

            if dependency_ids:
                cur.executemany(
                    "INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (%s, %s)",
                    [(job_id, dep) for dep in dependency_ids],
                )
            return job_id

        
def get_job(job_id: int) -> dict | None:
    """Fetch one job by id, or None if it doesn't exist."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()
    return _decode_payload(job)

CLAIM_CANDIDATE_SQL = """
SELECT id, status, attempts, max_attempts FROM jobs
WHERE (status = 'pending'   AND run_at <= NOW(6))
   OR (status = 'in_flight' AND lease_expires_at <= NOW(6))  -- expired lease
ORDER BY priority DESC, run_at, id
LIMIT 1
FOR UPDATE SKIP LOCKED
"""

LEASE_EXPIRED_ERROR = "Lease expired on the final attempt (worker likely crashed)"


def claim_job(worker_id: str, lease_seconds: float = 30) -> dict | None:
    """Atomically take ONE job for this worker, or return None if none is available.

    The job is either:
      - pending and due (run_at has passed), or
      - in_flight but its lease expired (the previous worker is presumed dead).

    An expired-lease job that has used all its attempts is moved to 'dead'
    instead of being run again (it probably keeps crashing workers).
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            while True:
                cur.execute(CLAIM_CANDIDATE_SQL)
                candidate = cur.fetchone()
                if candidate is None:
                    return None

                exhausted = (
                    candidate["status"] == "in_flight"
                    and candidate["attempts"] >= candidate["max_attempts"]
                )
                if exhausted:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'dead', locked_by = NULL,
                            lease_expires_at = NULL, last_error = %s
                        WHERE id = %s
                        """,
                        (LEASE_EXPIRED_ERROR, candidate["id"]),
                    )
                    continue  # look for the next candidate

                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'in_flight',
                        locked_by = %s,
                        lease_expires_at = NOW(6) + INTERVAL %s SECOND,
                        attempts = attempts + 1
                    WHERE id = %s
                    """,
                    (worker_id, lease_seconds, candidate["id"]),
                )
                cur.execute("SELECT * FROM jobs WHERE id = %s", (candidate["id"],))
                job = cur.fetchone()
                return _decode_payload(job)
            

def _release_dependents(cur, finished_job_id: int) -> int:
    """After a job finishes, move each waiting dependent to 'pending' if ALL of
    its dependencies are now done. Runs inside the caller's transaction.

    Each dependent is locked BEFORE we check it. Without that, two dependencies
    finishing at the same moment would each see the other as unfinished (the
    other's change is not committed yet), and the dependent would wait forever.
    """
    cur.execute(
        "SELECT job_id FROM job_dependencies WHERE depends_on_id = %s ORDER BY job_id",
        (finished_job_id,),
    )
    released = 0
    for row in cur.fetchall():
        dependent_id = row["job_id"]

        cur.execute("SELECT status FROM jobs WHERE id = %s FOR UPDATE", (dependent_id,))
        dependent = cur.fetchone()
        if dependent is None or dependent["status"] != "waiting":
            continue

        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM job_dependencies d
            JOIN jobs p ON p.id = d.depends_on_id
            WHERE d.job_id = %s AND p.status <> 'done'
            """,
            (dependent_id,),
        )
        if cur.fetchone()["n"] == 0:
            # GREATEST: waiting time must not count as queue latency.
            cur.execute(
                "UPDATE jobs SET status = 'pending', run_at = GREATEST(run_at, NOW(6)) "
                "WHERE id = %s",
                (dependent_id,),
            )
            released += 1
    return released


def complete_job(job_id: int, worker_id: str) -> bool:
    """Mark a job done and release any jobs that were waiting for it.
    Returns False if this worker no longer owns the job."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'done', locked_by = NULL, lease_expires_at = NULL
                WHERE id = %s AND locked_by = %s AND status = 'in_flight'
                """,
                (job_id, worker_id),
            )
            if cur.rowcount != 1:
                return False
            _release_dependents(cur, job_id)
            return True

        
def fail_job(
    job_id: int,
    worker_id: str,
    error: str,
    backoff_base: float = 2.0,
    backoff_cap: float = 300.0,
) -> str | None:
    """Record a failure. Returns the new status ('pending' or 'dead'),
    or None if this worker no longer owns the job.

    If attempts remain, the job is rescheduled with exponential backoff
    (run_at = now + delay). Otherwise it moves to 'dead'.
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            # Lock the row first: we READ attempts and then WRITE based on it.
            cur.execute(
                """
                SELECT attempts, max_attempts FROM jobs
                WHERE id = %s AND locked_by = %s AND status = 'in_flight'
                FOR UPDATE
                """,
                (job_id, worker_id),
            )
            job = cur.fetchone()
            if job is None:
                return None  # we lost the lease; someone else owns this job now

            if job["attempts"] >= job["max_attempts"]:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'dead', locked_by = NULL, lease_expires_at = NULL,
                        last_error = %s
                    WHERE id = %s
                    """,
                    (error[:5000], job_id),
                )
                return "dead"

            delay = round(compute_backoff(job["attempts"], backoff_base, backoff_cap), 3)
            cur.execute(
                """
                UPDATE jobs
                SET status = 'pending', locked_by = NULL, lease_expires_at = NULL,
                    last_error = %s,
                    run_at = NOW(6) + INTERVAL %s SECOND
                WHERE id = %s
                """,
                (error[:5000], delay, job_id),
            )
            return "pending"

def extend_lease(job_id: int, worker_id: str, lease_seconds: float = 30) -> bool:
    """Heartbeat: push the lease further into the future for a long-running job."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET lease_expires_at = NOW(6) + INTERVAL %s SECOND
                WHERE id = %s AND locked_by = %s AND status = 'in_flight'
                """,
                (lease_seconds, job_id, worker_id),
            )
            return cur.rowcount == 1

# ---------------------------------------------------------------------------
# Dead-letter queue: inspect, replay, purge
# ---------------------------------------------------------------------------

def list_dead_jobs(limit: int = 50, job_type: str | None = None) -> list[dict]:
    """Dead jobs, most recently dead first. Optionally filter by job type."""
    if limit < 1:
        raise ValueError("limit must be at least 1")

    sql = "SELECT * FROM jobs WHERE status = 'dead'"
    params: list = []
    if job_type:
        sql += " AND job_type = %s"
        params.append(job_type)
    sql += " ORDER BY updated_at DESC, id DESC LIMIT %s"
    params.append(limit)

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            jobs = cur.fetchall()
    return [_decode_payload(job) for job in jobs]


def replay_job(job_id: int) -> bool:
    """Send ONE dead job back to the queue with a fresh set of attempts.

    Returns False if the job doesn't exist or isn't dead. A single conditional
    UPDATE makes this safe even if several people replay at the same moment:
    only one of them will match `status = 'dead'`.
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'pending', attempts = 0, run_at = NOW(6),
                    locked_by = NULL, lease_expires_at = NULL
                WHERE id = %s AND status = 'dead'
                """,
                (job_id,),
            )
            return cur.rowcount == 1


def replay_dead_jobs(job_type: str | None = None) -> int:
    """Replay every dead job (optionally only one job type). Returns the count."""
    sql = """
        UPDATE jobs
        SET status = 'pending', attempts = 0, run_at = NOW(6),
            locked_by = NULL, lease_expires_at = NULL
        WHERE status = 'dead'
    """
    params: list = []
    if job_type:
        sql += " AND job_type = %s"
        params.append(job_type)

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

def _remove_orphan_dependencies(cur) -> None:
    """Delete dependency rows whose job OR whose parent no longer exists.

    A missing parent is safe to forget: parents are only ever deleted when
    they are 'done' (retention) and 'done' is permanent, so it can no longer block.
    """
    cur.execute(
        "DELETE d FROM job_dependencies d "
        "LEFT JOIN jobs j ON j.id = d.job_id WHERE j.id IS NULL"
    )
    cur.execute(
        "DELETE d FROM job_dependencies d "
        "LEFT JOIN jobs p ON p.id = d.depends_on_id WHERE p.id IS NULL"
    )

def purge_job(job_id: int) -> bool:
    """Permanently delete ONE dead job. Never touches jobs in any other status,
    and refuses a job that other jobs still depend on."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM jobs
                WHERE id = %s AND status = 'dead'
                  AND NOT EXISTS (SELECT 1 FROM job_dependencies d
                                  WHERE d.depends_on_id = jobs.id)
                """,
                (job_id,),
            )
            deleted = cur.rowcount == 1
            if deleted:
                _remove_orphan_dependencies(cur)
            return deleted


def purge_dead_jobs(job_type: str | None = None) -> int:
    """Permanently delete dead jobs (optionally one job type), except those that
    other jobs depend on. Returns the count."""
    sql = """
        DELETE FROM jobs
        WHERE status = 'dead'
          AND NOT EXISTS (SELECT 1 FROM job_dependencies d WHERE d.depends_on_id = jobs.id)
    """
    params: list = []
    if job_type:
        sql += " AND job_type = %s"
        params.append(job_type)

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            deleted = cur.rowcount
            _remove_orphan_dependencies(cur)
            return deleted


def cancel_job(job_id: int) -> bool:
    """Delete a WAITING job that no other job depends on.
    To cancel a whole chain, cancel from the end of the chain backwards."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM jobs
                WHERE id = %s AND status = 'waiting'
                  AND NOT EXISTS (SELECT 1 FROM job_dependencies d
                                  WHERE d.depends_on_id = jobs.id)
                """,
                (job_id,),
            )
            cancelled = cur.rowcount == 1
            if cancelled:
                _remove_orphan_dependencies(cur)
            return cancelled


def list_dependencies(job_id: int) -> list[dict]:
    """The jobs this job waits for, with their current status."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.job_type, p.status
                FROM job_dependencies d
                JOIN jobs p ON p.id = d.depends_on_id
                WHERE d.job_id = %s
                ORDER BY p.id
                """,
                (job_id,),
            )
            return list(cur.fetchall())

def list_in_flight_jobs(limit: int = 50) -> list[dict]:
    """Jobs currently held by workers, soonest-expiring lease first.

    Each job gets `lease_remaining_seconds` (computed with the DB clock);
    it is NEGATIVE when the lease has already expired.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *, TIMESTAMPDIFF(MICROSECOND, NOW(6), lease_expires_at) AS lease_micros
                FROM jobs
                WHERE status = 'in_flight'
                ORDER BY lease_expires_at, id
                LIMIT %s
                """,
                (limit,),
            )
            jobs = cur.fetchall()

    result = []
    for job in jobs:
        micros = job.pop("lease_micros")
        job["lease_remaining_seconds"] = None if micros is None else round(micros / 1_000_000, 3)
        result.append(_decode_payload(job))
    return result

# ---------------------------------------------------------------------------
# Retention: delete old finished jobs
# ---------------------------------------------------------------------------

def _check_retention_args(older_than_seconds: float, batch_size: int = 1) -> None:
    if older_than_seconds < 0:
        raise ValueError("older_than_seconds must not be negative")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")


def count_prunable_done_jobs(older_than_seconds: float) -> int:
    """How many 'done' jobs prune_done_jobs would delete right now (dry run)."""
    _check_retention_args(older_than_seconds)
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM jobs "
                "WHERE status = 'done' AND updated_at < NOW(6) - INTERVAL %s SECOND",
                (older_than_seconds,),
            )
            return int(cur.fetchone()["n"])


def prune_done_jobs(older_than_seconds: float, batch_size: int = 1000) -> int:
    """Delete 'done' jobs that finished more than `older_than_seconds` ago.

    Works in small batches (one transaction each) so workers are never blocked by
    one huge DELETE. Never touches pending, in-flight, waiting or dead jobs, and
    never touches idempotency keys. Returns the number of jobs deleted.
    """
    _check_retention_args(older_than_seconds, batch_size)

    total = 0
    while True:
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM jobs WHERE status = 'done' "
                    "AND updated_at < NOW(6) - INTERVAL %s SECOND LIMIT %s",
                    (older_than_seconds, batch_size),
                )
                deleted = cur.rowcount
        total += deleted
        if deleted < batch_size:
            break

    if total:
        with transaction() as conn:
            with conn.cursor() as cur:
                _remove_orphan_dependencies(cur)
    return total