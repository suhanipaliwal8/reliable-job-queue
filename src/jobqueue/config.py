import os


def get_db_config() -> dict:
    """Read DB settings from environment variables, with local-dev defaults."""
    return {
        "host": os.getenv("JQ_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("JQ_DB_PORT", "3307")),
        "user": os.getenv("JQ_DB_USER", "jobqueue"),
        "password": os.getenv("JQ_DB_PASSWORD", "jobqueue_pw"),
        "database": os.getenv("JQ_DB_NAME", "jobqueue"),
    }