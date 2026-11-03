from .db import transaction

# Note: indexes are NOT declared here. They are managed below so the same code
# works for brand-new databases AND for databases created in earlier phases.
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id               BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    job_type         VARCHAR(100) NOT NULL,
    payload          JSON NOT NULL,
    status           ENUM('pending','in_flight','done','dead') NOT NULL DEFAULT 'pending',
    priority         INT NOT NULL DEFAULT 0,
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 5,
    run_at           DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    locked_by        VARCHAR(100) NULL,
    lease_expires_at DATETIME(6) NULL,
    last_error       TEXT NULL,
    created_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                     ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Tables for Phase 6 (idempotency) and the demo payment job.
EXTRA_TABLES = [
    # The ledger of "this effect already happened". The PRIMARY KEY is the referee.
    """
    CREATE TABLE IF NOT EXISTS side_effects (
        idempotency_key VARCHAR(200) NOT NULL PRIMARY KEY,
        job_id          BIGINT UNSIGNED NULL,
        created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # Demo "payment provider". No UNIQUE on order_id ON PURPOSE: the protection
    # must come from run_once(), so the naive handler can show what goes wrong.
    """
    CREATE TABLE IF NOT EXISTS charges (
        id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        order_id     VARCHAR(100) NOT NULL,
        customer     VARCHAR(100) NOT NULL,
        amount_cents INT NOT NULL,
        created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # Demo "email service".
    """
    CREATE TABLE IF NOT EXISTS receipts (
        id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        order_id   VARCHAR(100) NOT NULL,
        customer   VARCHAR(100) NOT NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]

# index name -> column list
INDEXES = {
    # claim order: pending jobs by priority (high first), then oldest run_at
    "idx_claim": "(status, priority DESC, run_at)",
    # find in_flight jobs whose lease has expired
    "idx_lease": "(status, lease_expires_at)",
}

# indexes from earlier phases that are replaced
OBSOLETE_INDEXES = ["idx_fetch"]


def _index_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM information_schema.statistics
        WHERE table_schema = DATABASE() AND table_name = 'jobs' AND index_name = %s
        """,
        (name,),
    )
    return cur.fetchone()["n"] > 0


def init_db() -> None:
    """Create the table and bring its indexes up to date. Safe to run many times."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)

            for statement in EXTRA_TABLES:
                cur.execute(statement)

            for name in OBSOLETE_INDEXES:
                if _index_exists(cur, name):
                    cur.execute(f"DROP INDEX {name} ON jobs")

            for name, columns in INDEXES.items():
                if not _index_exists(cur, name):
                    cur.execute(f"CREATE INDEX {name} ON jobs {columns}")