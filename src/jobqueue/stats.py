from .db import transaction

# ONE statement = ONE consistent snapshot. SUM(<condition>) counts matching rows.
# COALESCE turns "no rows at all" (NULL) into 0.
STATS_SQL = """
SELECT
    COALESCE(SUM(status = 'pending'   AND run_at <= NOW(6)), 0)            AS pending_ready,
    COALESCE(SUM(status = 'pending'   AND run_at >  NOW(6)), 0)            AS pending_scheduled,
    COALESCE(SUM(status = 'in_flight' AND lease_expires_at >  NOW(6)), 0)  AS in_flight_active,
    COALESCE(SUM(status = 'in_flight' AND lease_expires_at <= NOW(6)), 0)  AS in_flight_expired,
    COALESCE(SUM(status = 'done'), 0)                                      AS done,
    COALESCE(SUM(status = 'dead'), 0)                                      AS dead,
    TIMESTAMPDIFF(MICROSECOND,
        MIN(CASE WHEN status = 'pending' AND run_at <= NOW(6) THEN run_at END),
        NOW(6))                                                            AS oldest_ready_micros,
    TIMESTAMPDIFF(MICROSECOND, NOW(6),
        MIN(CASE WHEN status = 'pending' AND run_at > NOW(6) THEN run_at END))
                                                                           AS next_scheduled_micros
FROM jobs
"""

BY_TYPE_SQL = """
SELECT
    job_type,
    COALESCE(SUM(status = 'pending'), 0)   AS pending,
    COALESCE(SUM(status = 'in_flight'), 0) AS in_flight,
    COALESCE(SUM(status = 'done'), 0)      AS done,
    COALESCE(SUM(status = 'dead'), 0)      AS dead
FROM jobs
GROUP BY job_type
ORDER BY job_type
"""


def _seconds(micros) -> float | None:
    return None if micros is None else round(int(micros) / 1_000_000, 3)


def get_stats() -> dict:
    """Queue health in one consistent snapshot.

    pending            all jobs waiting (ready + scheduled)
    pending_ready      waiting AND due now: the real backlog
    pending_scheduled  waiting for a future time (delayed jobs, retry backoff)
    in_flight_active   held by a worker, lease still valid
    in_flight_expired  lease ran out: worker died or is very slow
    oldest_ready_age_seconds   how long the longest-waiting ready job has waited
    next_scheduled_in_seconds  time until the next delayed job becomes ready
    """
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(STATS_SQL)
            row = cur.fetchone()

    counts = {
        key: int(row[key])
        for key in ("pending_ready", "pending_scheduled", "in_flight_active",
                    "in_flight_expired", "done", "dead")
    }
    return {
        "pending": counts["pending_ready"] + counts["pending_scheduled"],
        "pending_ready": counts["pending_ready"],
        "pending_scheduled": counts["pending_scheduled"],
        "in_flight": counts["in_flight_active"] + counts["in_flight_expired"],
        "in_flight_active": counts["in_flight_active"],
        "in_flight_expired": counts["in_flight_expired"],
        "done": counts["done"],
        "dead": counts["dead"],
        "total": sum(counts.values()),
        "oldest_ready_age_seconds": _seconds(row["oldest_ready_micros"]),
        "next_scheduled_in_seconds": _seconds(row["next_scheduled_micros"]),
    }


def get_stats_by_type() -> list[dict]:
    """The same counts, one row per job type (a GROUP BY query)."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(BY_TYPE_SQL)
            rows = cur.fetchall()
    return [
        {
            "job_type": row["job_type"],
            "pending": int(row["pending"]),
            "in_flight": int(row["in_flight"]),
            "done": int(row["done"]),
            "dead": int(row["dead"]),
        }
        for row in rows
    ]