from .db import transaction

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
                     ON UPDATE CURRENT_TIMESTAMP(6),
    KEY idx_fetch (status, run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def init_db() -> None:
    """Create tables if they don't exist yet. Safe to run many times."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)