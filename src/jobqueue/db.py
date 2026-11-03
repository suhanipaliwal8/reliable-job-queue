from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from .config import get_db_config


def connect():
    return pymysql.connect(
        **get_db_config(),
        cursorclass=DictCursor,
        autocommit=False,
        charset="utf8mb4",
        # Queue tables work best with READ COMMITTED (fewer row/gap locks).
        init_command="SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
    )


@contextmanager
def transaction():
    """Open a connection, run a block as ONE transaction, commit or roll back."""
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()