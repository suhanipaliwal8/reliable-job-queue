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