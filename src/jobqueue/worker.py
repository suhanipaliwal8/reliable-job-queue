import logging
import os
import socket
import time
import uuid

from . import handlers  # noqa: F401  (importing registers the demo handlers)
from .core import claim_job, complete_job, fail_job
from .registry import get_handler

log = logging.getLogger(__name__)


def make_worker_id() -> str:
    """Unique id like 'my-pc-4321-a1b2c3'. Shows who holds which job."""
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


class Worker:
    def __init__(self, worker_id=None, lease_seconds=30, poll_interval=1.0):
        self.worker_id = worker_id or make_worker_id()
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self._stop = False

    def stop(self):
        self._stop = True

    def run_once(self) -> bool:
        """Claim and process at most one job. Returns True if a job was found."""
        job = claim_job(self.worker_id, self.lease_seconds)
        if job is None:
            return False

        log.info("[%s] claimed job %s (%s, priority %s), attempt %s",
                 self.worker_id, job["id"], job["job_type"],
                 job["priority"], job["attempts"])

        try:
            func = get_handler(job["job_type"])
            if func is None:
                raise LookupError(f"No handler registered for job type '{job['job_type']}'")
            func(job)
        except Exception as exc:
            status = fail_job(job["id"], self.worker_id, f"{type(exc).__name__}: {exc}")
            log.warning("[%s] job %s failed (%s) -> %s",
                        self.worker_id, job["id"], exc, status or "lease lost")
        else:
            if complete_job(job["id"], self.worker_id):
                log.info("[%s] job %s done", self.worker_id, job["id"])
            else:
                log.warning("[%s] job %s finished but lease was lost; "
                            "another worker owns it now", self.worker_id, job["id"])
        return True

    def run_forever(self, drain: bool = False):
        """Keep processing jobs. If drain=True, exit when the queue is empty."""
        log.info("[%s] worker started", self.worker_id)
        while not self._stop:
            if not self.run_once():
                if drain:
                    break
                time.sleep(self.poll_interval)