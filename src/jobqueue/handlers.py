import logging
import time

from .registry import handler

log = logging.getLogger(__name__)


@handler("send_email")
def send_email(job):
    payload = job["payload"]
    log.info("Sending email to %s (subject=%s)", payload.get("to"), payload.get("subject"))


@handler("sleep")
def sleep(job):
    """Simulates a slow job. Useful for crash experiments."""
    time.sleep(float(job["payload"].get("seconds", 5)))


@handler("always_fail")
def always_fail(job):
    raise RuntimeError("this job always fails")