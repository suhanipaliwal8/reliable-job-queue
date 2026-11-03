from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from .config import get_db_config


def connect():
    return pymysql.connect(
        **get_db_config(),
        cursorclass=DictCursor,   # rows come back as dicts: row["status"]
        autocommit=False,         # we control transactions explicitly
        charset="utf8mb4",
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