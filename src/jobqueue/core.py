import json

from .db import transaction

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
SELECT id FROM jobs
WHERE (status = 'pending'   AND run_at <= NOW(6))
   OR (status = 'in_flight' AND lease_expires_at <= NOW(6))  -- expired lease
ORDER BY priority DESC, run_at, id
LIMIT 1
FOR UPDATE SKIP LOCKED
"""


def claim_job(worker_id: str, lease_seconds: float = 30) -> dict | None:
    """Atomically take ONE job for this worker, or return None if none is available.

    The job is either:
      - pending and due (run_at has passed), or
      - in_flight but its lease expired (the previous worker is presumed dead).
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            # 1. Find a candidate and lock its row (skipping rows others locked).
            cur.execute(CLAIM_CANDIDATE_SQL)
            candidate = cur.fetchone()
            if candidate is None:
                return None

            # 2. Mark it ours, in the SAME transaction, so no one can sneak in.
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

            # 3. Read the updated row back to return it.
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


def fail_job(job_id: int, worker_id: str, error: str) -> str | None:
    """Record a failure. Returns the new status ('pending' or 'dead'),
    or None if this worker no longer owns the job.

    NOTE: simple version for now. A failed job is retried immediately.
    Phase 4 adds exponential backoff; Phase 5 adds inspect/replay for dead jobs.
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = IF(attempts >= max_attempts, 'dead', 'pending'),
                    locked_by = NULL,
                    lease_expires_at = NULL,
                    last_error = %s
                WHERE id = %s AND locked_by = %s AND status = 'in_flight'
                """,
                (error[:5000], job_id, worker_id),
            )
            if cur.rowcount != 1:
                return None
            cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
            return cur.fetchone()["status"]


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