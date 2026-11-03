# Reliable Job Queue

A background-job engine built from scratch in Python + MySQL.
No Celery, no Sidekiq: just SQL, transactions, and careful design.

> Status: ✅ Phase 1 complete (durable queue core) → next: Phase 2 (workers + leases)

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

# 3. Create tables and add a job
python -m jobqueue init-db
python -m jobqueue enqueue send_email to=a@b.com subject=Hello
python -m jobqueue show 1

# 4. Run tests
pytest -v
```

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

## Roadmap
- [x] Phase 0: Project setup
- [x] Phase 1: Durable queue core
- [ ] Phase 2: Worker + lease
- [ ] Phase 3: Priority + delayed jobs
- [ ] Phase 4: Retry + exponential backoff
- [ ] Phase 5: Dead-letter queue + replay
- [ ] Phase 6: Idempotency
- [ ] Phase 7: Inspect CLI + API
- [ ] Phase 8: Crash demo