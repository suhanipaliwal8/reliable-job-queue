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
) -> int:
    """Add a job to the queue. Returns the new job's id.

    priority:      higher number = more urgent (default 0, negatives allowed).
    delay_seconds: the job will not run before NOW + this many seconds
                   (measured by the DATABASE clock).
    """
    if not job_type:
        raise ValueError("job_type must not be empty")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must not be negative")
    payload = payload or {}

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs (job_type, payload, max_attempts, priority, run_at)
                VALUES (%s, %s, %s, %s, NOW(6) + INTERVAL %s SECOND)
                """,
                (job_type, json.dumps(payload), max_attempts, priority, delay_seconds),
            )
            return cur.lastrowid


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
            

def complete_job(job_id: int, worker_id: str) -> bool:
    """Mark a job done. Returns False if this worker no longer owns the job."""
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
            return cur.rowcount == 1


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


def purge_job(job_id: int) -> bool:
    """Permanently delete ONE dead job. Never touches jobs in any other status."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM jobs WHERE id = %s AND status = 'dead'", (job_id,)
            )
            return cur.rowcount == 1


def purge_dead_jobs(job_type: str | None = None) -> int:
    """Permanently delete all dead jobs (optionally one job type). Returns the count."""
    sql = "DELETE FROM jobs WHERE status = 'dead'"
    params: list = []
    if job_type:
        sql += " AND job_type = %s"
        params.append(job_type)

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

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