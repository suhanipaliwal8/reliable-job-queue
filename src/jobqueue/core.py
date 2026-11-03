import json

from .db import transaction

def _decode_payload(job: dict | None) -> dict | None:
    """MySQL returns JSON columns as text; turn the payload back into a dict."""
    if job and isinstance(job["payload"], str):
        job["payload"] = json.loads(job["payload"])
    return job

def enqueue(job_type: str, payload: dict | None = None, max_attempts: int = 5) -> int:
    """Add a job to the queue. Returns the new job's id."""
    if not job_type:
        raise ValueError("job_type must not be empty")
    payload = payload or {}

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO jobs (job_type, payload, max_attempts) VALUES (%s, %s, %s)",
                (job_type, json.dumps(payload), max_attempts),
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
ORDER BY run_at, id
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