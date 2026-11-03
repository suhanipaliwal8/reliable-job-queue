# Reliable Job Queue

A background-job engine built from scratch in Python + MySQL.
No Celery, no Sidekiq: just SQL, transactions, and careful design.

> Status: ✅ Phase 5 complete (dead-letter queue + replay) → next: Phase 6 (idempotency)

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
pip install -e ".[dev]"

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
**At-least-once.** A job is never lost, but may run more than once (e.g. a worker
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

## Design notes
- **MySQL/InnoDB** gives transactions and row-level locks, which are required for safe multi-worker locking.
- **Schema is designed up-front** (priority, run_at, lease columns) so later phases need no migrations.
- **Config via environment variables** so the same code runs anywhere.

### Other design notes
- Time comes from the database clock (`NOW(6)`), never from worker machines.
- `READ COMMITTED` isolation keeps row locking narrow and avoids needless blocking.
- `attempts` is incremented at claim time, so jobs that crash workers still count.

## Writing a handler
```python
from jobqueue.registry import handler

@handler("send_email")
def send_email(job):
    payload = job["payload"]
    ...
```


## Roadmap
- [x] Phase 0: Project setup
- [x] Phase 1: Durable queue core
- [x] Phase 2: Worker + lease
- [x] Phase 3: Priority + delayed jobs
- [x] Phase 4: Retry + exponential backoff
- [x] Phase 5: Dead-letter queue + replay
- [ ] Phase 6: Idempotency
- [ ] Phase 7: Inspect CLI + API
- [ ] Phase 8: Crash demo