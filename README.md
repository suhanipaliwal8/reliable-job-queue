# Reliable Job Queue

A background-job engine built from scratch in Python + SQLite.

> Status: 🚧 Under construction (Phase 0: project setup)

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