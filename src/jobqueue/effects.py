from typing import Callable

import pymysql

from .db import transaction

MAX_KEY_LENGTH = 200          # matches the side_effects.idempotency_key column
DUPLICATE_KEY_ERROR = 1062    # MySQL's error code for "duplicate entry"


def run_once(key: str, action: Callable, job_id: int | None = None) -> bool:
    """Run `action(cursor)` at most once per idempotency `key`.

    Returns True if the action ran now, False if the key was already used
    (meaning the effect already happened earlier).

    The key and the action's database writes commit in ONE transaction:
      - action raises  -> everything rolls back, the key is NOT consumed,
                          so a retry may try again
      - process dies   -> MySQL rolls the transaction back automatically
      - two callers at once -> the second waits on the PRIMARY KEY, then sees
                          the duplicate and skips

    Choose keys from business identity (e.g. "charge:order-123"), not from
    job ids, so that two different jobs for the same order are also protected.
    """
    if not key:
        raise ValueError("idempotency key must not be empty")
    if len(key) > MAX_KEY_LENGTH:
        raise ValueError(f"idempotency key must be at most {MAX_KEY_LENGTH} characters")

    with transaction() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "INSERT INTO side_effects (idempotency_key, job_id) VALUES (%s, %s)",
                    (key, job_id),
                )
            except pymysql.err.IntegrityError as exc:
                if exc.args[0] != DUPLICATE_KEY_ERROR:
                    raise
                return False          # already done earlier: skip the action

            action(cur)               # same transaction as the key insert
            return True