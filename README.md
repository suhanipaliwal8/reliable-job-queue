# Reliable Job Queue

A background-job engine built from scratch in Python + MySQL.
No Celery, no Sidekiq: just SQL, transactions, and careful design.

> Status: ✅ Phase 8 complete (crash demo) → next: Phase 9 (stretch goals)

## Features (planned)
- Durable queue: jobs survive crashes and restarts
- Priorities and delayed jobs
- Retries with exponential backoff
- Dead-letter queue with replay
- Lease-based locking: no job runs on two workers at once
- Inspection CLI/API
- Idempotent job handling

## Tech
Python 3.10+, MySQL 8.0 (Docker), PyMySQL, pytest, FastAPI (later)

## Quick start
```bash
# 1. Start MySQL
docker compose up -d

# 2. Install
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev,api]"

# 3. Create tables and add jobs
python -m jobqueue init-db
python -m jobqueue enqueue send_email to=a@b.com subject=Hello

# 4. Run a worker (Ctrl+C to stop). Run several in separate terminals!
python -m jobqueue worker
# or process everything and exit:
python -m jobqueue worker --drain

# 5. Run tests (stop workers first: tests truncate the jobs table)
pytest -v
```

## Usage

```bash
# normal job
python -m jobqueue enqueue send_email to=a@b.com

# urgent job (higher number = more urgent)
python -m jobqueue enqueue send_email to=boss@b.com --priority 10

# delayed job: not runnable for 10 minutes
python -m jobqueue enqueue send_email to=a@b.com --delay 600

# run a worker (--drain exits when nothing is due right now)
python -m jobqueue worker
```
## Priority and scheduling

- **Claim order:** `priority DESC, run_at ASC, id ASC`: the most urgent job goes first,
  and equal-priority jobs are first-in-first-out.
- **Delayed jobs** set `run_at = NOW() + delay` using the *database* clock. A job is
  never claimed before its `run_at`. It runs at-or-after that time, depending on
  worker polling and availability.
- Priority is not preemptive: a running job is never interrupted.
- **Known trade-off:** strict priority can starve low-priority jobs under constant
  high-priority load. Priority aging is a possible future improvement.
- Retry backoff (Phase 4) will reuse `run_at`: a failed job is simply rescheduled.


## Writing a handler
```python
from jobqueue.registry import handler

@handler("send_email")
def send_email(job):
    payload = job["payload"]
    ...
```

## Indexes
| Index | Columns | Purpose |
|---|---|---|
| `idx_claim` | `(status, priority DESC, run_at)` | fast "next pending job" lookup in claim order |
| `idx_lease` | `(status, lease_expires_at)` | find in_flight jobs with expired leases |

`init_db()` is an idempotent migration: it creates missing indexes and drops obsolete
ones, so it can safely be re-run after pulling new code.


## How reliability works

### Claiming a job (no double-processing)
A worker claims a job inside ONE transaction:
```sql
SELECT id FROM jobs WHERE <due> ORDER BY run_at, id LIMIT 1
FOR UPDATE SKIP LOCKED;
UPDATE jobs SET status='in_flight', locked_by=?, lease_expires_at=NOW()+lease ...
```
`FOR UPDATE` locks the row; `SKIP LOCKED` makes other workers pick a different one
instead of waiting.

### Leases (surviving dead workers)
Every claim comes with a lease. If a worker crashes, its lease expires and another
worker reclaims the job. Only the current lock owner can complete/fail a job
(fencing), so a slow "zombie" worker can't overwrite the new owner's result.

### Delivery guarantee
**At-least-once delivery plus idempotent handlers (run_once) gives exactly-once effects.** A job is never lost, but may run more than once (e.g. a worker
is slow and loses its lease). Handlers must therefore be idempotent (Phase 6).

## Configuration
Environment variables (defaults work with the included docker-compose):

| Variable | Default |
|---|---|
| JQ_DB_HOST | 127.0.0.1 |
| JQ_DB_PORT | 3307 |
| JQ_DB_USER | jobqueue |
| JQ_DB_PASSWORD | jobqueue_pw |
| JQ_DB_NAME | jobqueue |

## Retries and backoff

When a handler raises an exception, the job is retried with **exponential backoff
and jitter**:

`delay = min(cap, base × 2^(attempt−1))`, then randomised to 50–100% of that value.

| Attempt that failed | Delay (base=2s, no jitter) | With jitter |
|---|---|---|
| 1 | 2s | 1–2s |
| 2 | 4s | 2–4s |
| 3 | 8s | 4–8s |
| 4 | 16s | 8–16s |
| … | capped at 300s | |

- **Why backoff?** Instant retries cause *retry storms* against a struggling service.
- **Why jitter?** Without it, jobs that failed together retry together (thundering herd).
- Retries reuse `run_at`: a failed job is simply invisible to workers until it is due.
- After `max_attempts` failures the job moves to `dead` (see Phase 5).
- Tune with `python -m jobqueue worker --backoff-base 2 --backoff-cap 300`.

### Crash loops ("poison jobs")
`attempts` is counted when a job is *claimed*, so a job that crashes its worker still
uses up attempts. When such a job's lease expires and no attempts remain, it goes to
`dead` instead of being claimed again forever.

## Job lifecycle
`pending → in_flight → done`  
Failed jobs return to `pending` with a delay, or move to `dead` after max attempts.

## Dead-letter queue (DLQ)

A job that fails `max_attempts` times (or keeps crashing its worker) moves to
`status = 'dead'`. Dead jobs are kept, not deleted, so a human can find out why they
failed and decide what to do.

```bash
python -m jobqueue dead list                     # what died, and why
python -m jobqueue dead list --job-type email    # filter by type

python -m jobqueue dead replay 42                # send one job back to the queue
python -m jobqueue dead replay --all --job-type email   # replay a whole type

python -m jobqueue dead purge 42                 # delete one dead job
python -m jobqueue dead purge --all --yes        # delete all dead jobs
```

### How replay works
- Replay is ONE conditional `UPDATE ... WHERE id = ? AND status = 'dead'`, so two
  people replaying the same job at once cannot both succeed.
- It resets `attempts` to 0 and `run_at` to now. It keeps the payload, the priority,
  and `last_error` (useful debugging context).
- Only `dead` jobs can be replayed or purged. Live jobs are never touched.
- **Fix the cause before replaying**, or the job will simply die again. Replaying a
  huge number of jobs at once can overload the service that was failing, so prefer
  `--job-type` and replay in batches.
- A replayed job may have partly run before it died, so handlers must be idempotent
  (Phase 6).

## Idempotency (safe re-runs)

Delivery is **at-least-once**, so a job can run more than once: a slow worker loses
its lease, a worker crashes mid-job, or a dead job is replayed. Handlers must
therefore make their side effects **idempotent**.

`run_once(key, action)` guarantees `action` happens at most once per key:

```python
from jobqueue.effects import run_once

@handler("charge_and_receipt")
def charge_and_receipt(job):
    p = job["payload"]
    run_once(f"charge:{p['order_id']}",  lambda cur: insert_charge(cur, p),  job["id"])
    run_once(f"receipt:{p['order_id']}", lambda cur: insert_receipt(cur, p), job["id"])
```

How it works:
- The key is inserted into `side_effects` (PRIMARY KEY) **in the same transaction** as
  the effect, so they commit or roll back together.
- Duplicate key means the effect already happened, so the action is skipped.
- If the action fails or the process dies, the transaction rolls back and the key is
  not consumed, so the retry can try again.
- Two concurrent callers are serialized by the PRIMARY KEY: exactly one wins.

Key design rules:
- Derive keys from **business identity** (`charge:order-123`), not job ids: two jobs
  for the same order must not both charge.
- Use **one key per step**, so a retry resumes after the last finished step.

### Demo: a crash between two steps
```bash
# BROKEN on purpose: double-charges after the simulated crash
python -m jobqueue enqueue naive_charge_and_receipt order_id=A1 customer=asha amount_cents=49900 fail_after_charge=1
# SAFE: charges once, retry only sends the receipt
python -m jobqueue enqueue charge_and_receipt order_id=B2 customer=asha amount_cents=49900 fail_after_charge=1
python -m jobqueue worker --backoff-base 1
```
`pause_after_charge=30` lets you press Ctrl+C for a real crash instead.

### Limits
- Here the "payment provider" is a table in the same database, so the effect and the key
  share one transaction. A real external API cannot join that transaction: pass the key
  to the provider too (e.g. Stripe's `Idempotency-Key` header).
- `side_effects` grows forever; a production system would expire old keys.

## Inspecting the queue

```bash
python -m jobqueue stats                # queue depth and health
python -m jobqueue stats --by-type      # ...broken down per job type
python -m jobqueue stats --json         # machine-readable
python -m jobqueue in-flight            # who holds which job, lease time left
python -m jobqueue dead list            # dead-letter contents (Phase 5)
```

### What the numbers mean
| Field | Meaning | Worry when |
|---|---|---|
| `pending_ready` | Waiting and due now (the backlog) | keeps growing |
| `pending_scheduled` | Waiting for a future `run_at` (delays, retry backoff) | n/a |
| `oldest_ready_age_seconds` | How long the longest-waiting ready job has waited (**latency**) | keeps growing |
| `in_flight_active` | Held by a worker, lease still valid | n/a |
| `in_flight_expired` | Lease ran out: worker died **or is just slow** | stays above 0 |
| `dead` | Jobs that exhausted their retries | above 0 |

Depth alone can mislead: 1,000 jobs that drain in a second are fine, 3 jobs that have
waited 20 minutes are not. Watch latency too.

### HTTP API (read-only)
```bash
python -m jobqueue serve        # http://127.0.0.1:8000  (interactive docs at /docs)
```
| Endpoint | Returns |
|---|---|
| `GET /health` | `200` if the API can reach the database, else `503` |
| `GET /stats` | the numbers above |
| `GET /stats/by-type` | counts per job type |
| `GET /jobs/in-flight?limit=` | jobs held by workers, with `lease_remaining_seconds` |
| `GET /jobs/dead?limit=&job_type=` | dead-letter contents with `last_error` |
| `GET /jobs/{id}` | one job (`404` if missing) |

## Chaos test: proving reliability

Simulated failures aren't convincing, so the project can hard-kill its own workers:

```bash
python -m jobqueue chaos                      # idempotent handler: should PASS
python -m jobqueue chaos --naive --seed 7     # broken handler: should FAIL (on purpose)
python -m jobqueue chaos --jobs 300 --workers 6 --kills 15
```

What it does:
1. Enqueues N two-step jobs ("charge, pause, send receipt, pause").
2. Starts real `worker` **processes** and, at random moments, **hard-kills** them
   (`SIGKILL` on Linux/macOS, `TerminateProcess` on Windows): no `finally` blocks, no
   `fail_job`, nothing rolled back by Python. Each killed worker is replaced.
3. Waits until every job has finished, then checks three **invariants**:
   - every job completed (none lost, stuck or dead)
   - exactly one charge per order
   - exactly one receipt per order

Sample report:
```
CHAOS TEST  (mode: idempotent, run a1b2c3d4)
  hard kills                     6  (5 hit a worker that was holding a job)
  jobs that needed a retry       9  (most attempts on one job: 3)
  jobs done / dead / unfinished  100 / 0 / 0  (of 100)
  charges                        100  (expected 100)
  [PASS] exactly one charge per order
RESULT: PASS. Jobs survived every crash and no effect was applied twice.
```

Why it works:
- **Leases** (Phase 2): a killed worker's job becomes claimable once its lease expires.
- **Idempotency keys** (Phase 6): the retry skips steps that already happened,
  including the case where *every* effect was done but the worker died before
  reporting success.
- `--naive` swaps in a handler without keys, to prove the test can actually fail.

Notes:
- The runner refuses to start unless the queue is idle, and it only counts its own
  rows (order ids are prefixed with a random run id). `--cleanup` deletes them.
- `max_attempts` defaults to 50 here: a crash uses up an attempt, and with the normal
  default of 5, unlucky random kills could trip the crash-loop protection (Phase 4).
- It exits with a non-zero code on failure, so it can run in CI.
- `--seed` repeats the kill schedule, not the exact outcome (timing is up to the OS).
- It kills workers only, not MySQL or the network.



## Design notes
- **MySQL/InnoDB** gives transactions and row-level locks, which are required for safe multi-worker locking.
- **Schema is designed up-front** (priority, run_at, lease columns) so later phases need no migrations.
- **Config via environment variables** so the same code runs anywhere.
- **One SQL statement, one snapshot.** All counts come from a single query using
  conditional aggregation, so they can't contradict each other mid-count.
- **Database clock** for every age and countdown, like leases and delays.
- **Read-only on purpose.** The API has no authentication, so it cannot replay or purge.
  Those stay in the CLI. It binds to `127.0.0.1` by default; don't expose it publicly.
- **Limit:** `stats` scans the table. With millions of finished jobs you would add
  retention (delete old `done` jobs) or maintain counters.

### Other design notes
- Time comes from the database clock (`NOW(6)`), never from worker machines.
- `READ COMMITTED` isolation keeps row locking narrow and avoids needless blocking.
- `attempts` is incremented at claim time, so jobs that crash workers still count.



## Roadmap
- [x] Phase 0: Project setup
- [x] Phase 1: Durable queue core
- [x] Phase 2: Worker + lease
- [x] Phase 3: Priority + delayed jobs
- [x] Phase 4: Retry + exponential backoff
- [x] Phase 5: Dead-letter queue + replay
- [x] Phase 6: Idempotency
- [x] Phase 7: Inspect CLI + API
- [x] Phase 8: Crash demo
- [ ] Phase 9: Stretch (job dependencies, dashboard)
- [ ] Phase 10: Polish