# Reliable Job Queue

A background-job engine built from scratch in Python + SQLite.

> Status: ✅ Phase 0 complete (setup) → next: Phase 1 (durable queue core)

## Setup
    python -m venv .venv
    source .venv/bin/activate      # Windows: .venv\Scripts\activate
    pip install -e ".[dev]"
    pytest

## Roadmap
- [x] Phase 0: Project setup
- [ ] Phase 1: Durable queue core
- [ ] Phase 2: Worker + lease
- [ ] Phase 3: Priority + delayed jobs
- [ ] Phase 4: Retry + exponential backoff
- [ ] Phase 5: Dead-letter queue + replay
- [ ] Phase 6: Idempotency
- [ ] Phase 7: Inspect CLI + API
- [ ] Phase 8: Crash demo

## Goals
- Durable queue: jobs survive crashes
- Priorities and delayed jobs
- Retries with exponential backoff
- Dead-letter queue with replay
- Lease-based locking: no job runs on two workers at once
- Inspection CLI/API
- Idempotent job handling

## Tech
Python 3.10+, SQLite, pytest, FastAPI (later)