import subprocess
import sys

import pytest

from jobqueue.core import enqueue, get_job


def test_enqueue_returns_id_and_stores_job():
    job_id = enqueue("send_email", {"to": "a@b.com"})
    job = get_job(job_id)

    assert job is not None
    assert job["job_type"] == "send_email"
    assert job["payload"] == {"to": "a@b.com"}


def test_new_job_has_correct_defaults():
    job = get_job(enqueue("send_email"))

    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["max_attempts"] == 5
    assert job["locked_by"] is None


def test_empty_job_type_is_rejected():
    with pytest.raises(ValueError):
        enqueue("")


def test_job_survives_process_exit():
    """Enqueue in a separate Python process that then exits.
    The job must still be readable from this process."""
    code = "from jobqueue.core import enqueue; print(enqueue('email', {'x': 1}))"
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    job_id = int(result.stdout.strip())

    job = get_job(job_id)
    assert job is not None
    assert job["status"] == "pending"