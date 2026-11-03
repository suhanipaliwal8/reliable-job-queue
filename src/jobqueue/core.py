import json

from .db import transaction


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

    if job and isinstance(job["payload"], str):
        job["payload"] = json.loads(job["payload"])
    return job