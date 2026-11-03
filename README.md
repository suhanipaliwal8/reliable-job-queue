# Reliable Job Queue

A background-job engine built from scratch in Python + MySQL.
No Celery, no Sidekiq: just SQL, transactions, and careful design.

> Status: ✅ Phase 2 complete (workers + leases) → next: Phase 3 (priority + delayed jobs)

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

## Job lifecycle
`pending → in_flight → done`  
Failed jobs return to `pending` with a delay, or move to `dead` after max attempts.

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
- [ ] Phase 3: Priority + delayed jobs
- [ ] Phase 4: Retry + exponential backoff
- [ ] Phase 5: Dead-letter queue + replay
- [ ] Phase 6: Idempotency
- [ ] Phase 7: Inspect CLI + API
- [ ] Phase 8: Crash demo