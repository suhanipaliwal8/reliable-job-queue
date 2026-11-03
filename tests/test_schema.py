from jobqueue.db import transaction
from jobqueue.schema import init_db


def _index_names() -> set[str]:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW INDEX FROM jobs")
            return {row["Key_name"] for row in cur.fetchall()}


def test_init_db_creates_claim_indexes_and_drops_old_one():
    names = _index_names()
    assert "idx_claim" in names
    assert "idx_lease" in names
    assert "idx_fetch" not in names


def test_init_db_can_run_repeatedly():
    init_db()
    init_db()
    assert "idx_claim" in _index_names()