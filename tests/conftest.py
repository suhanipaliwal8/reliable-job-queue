import os

# Tests TRUNCATE tables, so they must NEVER touch your development database.
# This line must run before anything connects. Child processes (the chaos
# test's workers, for example) inherit it automatically.
os.environ["JQ_DB_NAME"] = os.getenv("JQ_TEST_DB_NAME", "jobqueue_test")

import pymysql
import pytest

from jobqueue.config import get_db_config
from jobqueue.db import connect, transaction
from jobqueue.schema import init_db

CREATE_HINT = (
    "Create it once with:\n"
    "  docker exec -it jobqueue-mysql mysql -uroot -prootpw -e "
    "\"CREATE DATABASE IF NOT EXISTS jobqueue_test; "
    "GRANT ALL PRIVILEGES ON jobqueue_test.* TO 'jobqueue'@'%';\""
)


@pytest.fixture(scope="session", autouse=True)
def require_test_database():
    """Refuse to run against anything that isn't clearly a test database."""
    name = get_db_config()["database"]
    if not name.endswith("_test"):
        pytest.exit(f"Refusing to run: database '{name}' does not end with '_test'.",
                    returncode=2)
    try:
        connect().close()
    except pymysql.err.OperationalError as exc:
        pytest.exit(f"Cannot connect to the test database '{name}': {exc}\n{CREATE_HINT}",
                    returncode=2)


@pytest.fixture(autouse=True)
def clean_tables(require_test_database):
    """Before every test: make sure the tables exist and are empty."""
    init_db()
    with transaction() as conn:
        with conn.cursor() as cur:
            for table in ("jobs", "job_dependencies", "side_effects", "charges", "receipts"):
                cur.execute(f"TRUNCATE TABLE {table}")
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

@pytest.fixture
def age_job():
    """Return a function that pretends a job was last updated `seconds` ago."""
    def _age(job_id, seconds):
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET updated_at = NOW(6) - INTERVAL %s SECOND WHERE id = %s",
                    (seconds, job_id),
                )
    return _age