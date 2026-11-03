import pytest

from jobqueue.db import transaction
from jobqueue.schema import init_db


@pytest.fixture(autouse=True)
def clean_jobs_table():
    """Before every test: make sure the table exists and is empty."""
    init_db()
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE jobs")
    yield

@pytest.fixture
def expire_lease():
    """Return a function that forces a job's lease into the past (simulates time passing)."""
    def _expire(job_id):
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND "
                    "WHERE id = %s",
                    (job_id,),
                )
    return _expire

@pytest.fixture
def make_due():
    """Return a function that makes a delayed job runnable NOW (instead of sleeping)."""
    def _make_due(job_id):
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET run_at = NOW(6) - INTERVAL 1 SECOND WHERE id = %s",
                    (job_id,),
                )
    return _make_due

@pytest.fixture
def make_dead_job():
    """Return a function that creates a job and takes it all the way to 'dead'
    through the real claim -> fail path (no SQL shortcuts)."""
    from jobqueue.core import claim_job, enqueue, fail_job

    def _make(job_type="t", payload=None, **kwargs):
        job_id = enqueue(job_type, payload, max_attempts=1, **kwargs)
        job = claim_job("fixture-worker")
        assert job is not None and job["id"] == job_id
        assert fail_job(job_id, "fixture-worker", "boom") == "dead"
        return job_id

    return _make

@pytest.fixture(autouse=True)
def clean_tables():
    """Before every test: make sure the tables exist and are empty."""
    init_db()
    with transaction() as conn:
        with conn.cursor() as cur:
            for table in ("jobs", "side_effects", "charges", "receipts"):
                cur.execute(f"TRUNCATE TABLE {table}")
    yield