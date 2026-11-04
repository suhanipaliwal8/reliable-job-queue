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

def test_init_db_creates_idempotency_and_demo_tables():
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            names = {list(row.values())[0] for row in cur.fetchall()}
    assert {"jobs", "side_effects", "charges", "receipts"} <= names

def _status_type() -> str:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_TYPE AS column_type FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'jobs' "
                "AND column_name = 'status'"
            )
            value = cur.fetchone()["column_type"]
    return value.decode() if isinstance(value, bytes) else value


def test_job_dependencies_table_exists():
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            names = {list(row.values())[0] for row in cur.fetchall()}
    assert "job_dependencies" in names


def test_status_column_knows_waiting():
    assert "'waiting'" in _status_type()


def test_init_db_upgrades_a_status_column_from_an_older_version():
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE jobs MODIFY COLUMN status "
                        "ENUM('pending','in_flight','done','dead') NOT NULL DEFAULT 'pending'")
    assert "'waiting'" not in _status_type()

    init_db()                                         # the migration under test

    assert "'waiting'" in _status_type()

def test_init_db_creates_retention_index():
    assert "idx_retention" in _index_names()