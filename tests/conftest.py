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