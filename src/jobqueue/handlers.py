import logging
import time

from .db import transaction
from .effects import run_once
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


# ---------------------------------------------------------------------------
# Demo: "charge a customer, then send a receipt"
# ---------------------------------------------------------------------------

def _insert_charge(cur, payload):
    cur.execute(
        "INSERT INTO charges (order_id, customer, amount_cents) VALUES (%s, %s, %s)",
        (payload["order_id"], payload["customer"], int(payload["amount_cents"])),
    )


def _insert_receipt(cur, payload):
    cur.execute(
        "INSERT INTO receipts (order_id, customer) VALUES (%s, %s)",
        (payload["order_id"], payload["customer"]),
    )


def _simulate_trouble_between_steps(job):
    """Fault injection for demos and tests. Only acts on the FIRST attempt.

    pause_after_charge=<seconds>  sleep here, so you can press Ctrl+C (a real crash)
    fail_after_charge=1           raise an error here (a simulated crash)
    """
    if job["attempts"] != 1:
        return
    payload = job["payload"]

    pause = float(payload.get("pause_after_charge", 0))
    if pause:
        time.sleep(pause)

    if str(payload.get("fail_after_charge", "")) == "1":
        raise RuntimeError("simulated crash between charge and receipt")


@handler("naive_charge_and_receipt")
def naive_charge_and_receipt(job):
    """BROKEN ON PURPOSE. Teaching exhibit: a retry charges the customer again."""
    payload = job["payload"]

    with transaction() as conn:
        with conn.cursor() as cur:
            _insert_charge(cur, payload)

    _simulate_trouble_between_steps(job)

    with transaction() as conn:
        with conn.cursor() as cur:
            _insert_receipt(cur, payload)


@handler("charge_and_receipt")
def charge_and_receipt(job):
    """Safe version: every step is guarded by an idempotency key."""
    payload = job["payload"]
    order = payload["order_id"]

    run_once(f"charge:{order}", lambda cur: _insert_charge(cur, payload), job["id"])
    _simulate_trouble_between_steps(job)
    run_once(f"receipt:{order}", lambda cur: _insert_receipt(cur, payload), job["id"])

# ---------------------------------------------------------------------------
# Chaos-test handlers (Phase 8): a slow two-step job, so hard kills land mid-job
# ---------------------------------------------------------------------------

def _chaos_order(job, idempotent: bool):
    payload = job["payload"]
    order = payload["order_id"]
    pause = float(payload.get("step_delay", 0.1))

    # Step 1: charge
    if idempotent:
        run_once(f"charge:{order}", lambda cur: _insert_charge(cur, payload), job["id"])
    else:
        with transaction() as conn:
            with conn.cursor() as cur:
                _insert_charge(cur, payload)

    time.sleep(pause)        # kill window 1: charged, receipt not sent yet

    # Step 2: receipt
    if idempotent:
        run_once(f"receipt:{order}", lambda cur: _insert_receipt(cur, payload), job["id"])
    else:
        with transaction() as conn:
            with conn.cursor() as cur:
                _insert_receipt(cur, payload)

    time.sleep(pause / 2)    # kill window 2: every effect done, job not yet marked done


@handler("chaos_order")
def chaos_order(job):
    """Safe: every step is guarded by an idempotency key."""
    _chaos_order(job, idempotent=True)


@handler("chaos_order_naive")
def chaos_order_naive(job):
    """BROKEN ON PURPOSE: no idempotency keys. A kill + retry applies effects twice."""
    _chaos_order(job, idempotent=False)