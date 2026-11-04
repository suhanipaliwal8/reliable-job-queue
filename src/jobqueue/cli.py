import argparse
import json
import logging

from .core import (
    enqueue,
    get_job,
    list_dead_jobs,
    list_in_flight_jobs,
    purge_dead_jobs,
    purge_job,
    replay_dead_jobs,
    replay_job,
)
from .chaos import ChaosError, render_report, run_chaos
from .schema import init_db
from .stats import get_stats, get_stats_by_type
from .worker import Worker


def parse_key_values(pairs: list[str]) -> dict:
    data = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"Invalid argument '{pair}'. Use key=value.")
        key, value = pair.split("=", 1)
        data[key] = value
    return data


# ---------------------------------------------------------------------------
# dead-letter queue commands
# ---------------------------------------------------------------------------

def run_dead_command(args) -> None:
    """Handle `jobqueue dead list|replay|purge`."""
    if args.dead_command == "list":
        try:
            jobs = list_dead_jobs(limit=args.limit, job_type=args.job_type)
        except ValueError as exc:
            raise SystemExit(f"Error: {exc}")
        if not jobs:
            print("Dead-letter queue is empty.")
            return
        print(f"{'ID':>6}  {'TYPE':<20} {'TRIES':>5} {'PRIO':>4}  {'DIED AT':<26}  ERROR")
        for job in jobs:
            error = (job["last_error"] or "").replace("\n", " ")
            if len(error) > 60:
                error = error[:57] + "..."
            print(f"{job['id']:>6}  {job['job_type']:<20} {job['attempts']:>5} "
                  f"{job['priority']:>4}  {str(job['updated_at']):<26}  {error}")

    elif args.dead_command == "replay":
        if args.all and args.job_id is not None:
            raise SystemExit("Error: give either a job id or --all, not both")
        if args.all:
            print(f"Replayed {replay_dead_jobs(args.job_type)} dead job(s).")
        elif args.job_id is not None:
            if replay_job(args.job_id):
                print(f"Job {args.job_id} sent back to the queue.")
            else:
                raise SystemExit(f"Job {args.job_id} not found or not dead; nothing replayed.")
        else:
            raise SystemExit("Error: give a job id or --all")

    elif args.dead_command == "purge":
        if args.all and args.job_id is not None:
            raise SystemExit("Error: give either a job id or --all, not both")
        if args.all:
            if not args.yes:
                raise SystemExit("Refusing to delete all dead jobs without --yes")
            print(f"Purged {purge_dead_jobs(args.job_type)} dead job(s).")
        elif args.job_id is not None:
            if purge_job(args.job_id):
                print(f"Job {args.job_id} deleted.")
            else:
                raise SystemExit(f"Job {args.job_id} not found or not dead; nothing deleted.")
        else:
            raise SystemExit("Error: give a job id or --all")


# ---------------------------------------------------------------------------
# inspection commands
# ---------------------------------------------------------------------------

def format_seconds(value) -> str:
    return "-" if value is None else f"{value:.1f}s"


def render_stats(stats: dict) -> str:
    rows = [
        ("pending, ready now", stats["pending_ready"]),
        ("pending, scheduled", stats["pending_scheduled"]),
        ("in flight, lease valid", stats["in_flight_active"]),
        ("in flight, lease expired", stats["in_flight_expired"]),
        ("done", stats["done"]),
        ("dead", stats["dead"]),
        ("total", stats["total"]),
    ]
    lines = ["QUEUE HEALTH"]
    lines += [f"  {label:<26}{value:>8}" for label, value in rows]
    lines.append("")
    oldest = format_seconds(stats["oldest_ready_age_seconds"])
    upcoming = format_seconds(stats["next_scheduled_in_seconds"])
    lines.append(f"  {'oldest ready job waiting':<26}{oldest:>8}")
    lines.append(f"  {'next scheduled job in':<26}{upcoming:>8}")

    if stats["in_flight_expired"]:
        lines += [
            "",
            "! Some leases have expired. A worker may have died (or is just slow);",
            "  another worker will reclaim these jobs on its next poll.",
        ]
    if stats["dead"]:
        lines += ["", "! Dead jobs need attention: python -m jobqueue dead list"]
    return "\n".join(lines)


def render_by_type(rows: list[dict]) -> str:
    if not rows:
        return "No jobs yet."
    lines = [f"{'TYPE':<24}{'PENDING':>9}{'IN FLIGHT':>11}{'DONE':>8}{'DEAD':>7}"]
    for row in rows:
        lines.append(f"{row['job_type']:<24}{row['pending']:>9}{row['in_flight']:>11}"
                     f"{row['done']:>8}{row['dead']:>7}")
    return "\n".join(lines)


def run_stats_command(args) -> None:
    stats = get_stats()
    # The per-type table is a second query, so on a very busy queue its numbers
    # can differ from the totals by a job or two.
    rows = get_stats_by_type() if args.by_type else None

    if args.json:
        data = {**stats, "by_type": rows} if rows is not None else stats
        print(json.dumps(data, indent=2))
        return

    print(render_stats(stats))
    if rows is not None:
        print()
        print(render_by_type(rows))


def run_in_flight_command(args) -> None:
    try:
        jobs = list_in_flight_jobs(limit=args.limit)
    except ValueError as exc:
        raise SystemExit(f"Error: {exc}")
    if not jobs:
        print("No jobs are in flight.")
        return

    print(f"{'ID':>6}  {'TYPE':<20} {'WORKER':<32} {'TRY':>5}  LEASE")
    for job in jobs:
        left = job["lease_remaining_seconds"]
        if left is None:
            lease = "-"
        elif left >= 0:
            lease = f"{left:.1f}s left"
        else:
            lease = f"EXPIRED {-left:.1f}s ago"
        tries = f"{job['attempts']}/{job['max_attempts']}"
        print(f"{job['id']:>6}  {job['job_type']:<20} {(job['locked_by'] or '-'):<32} "
              f"{tries:>5}  {lease}")


def run_serve_command(args) -> None:
    try:
        import uvicorn
        from .api import app
    except ImportError as exc:
        raise SystemExit(
            f'The API needs extra packages ({exc}). Run: pip install -e ".[api]"'
        )
    if args.host not in ("127.0.0.1", "localhost"):
        print("WARNING: the API has no authentication. Do not expose it to a network "
              "you don't trust.")
    uvicorn.run(app, host=args.host, port=args.port)

def run_chaos_command(args) -> None:
    print(f"Chaos test: {args.jobs} jobs, {args.workers} workers, up to {args.kills} "
          f"hard kills ({'NAIVE' if args.naive else 'idempotent'} handler). "
          "This takes a little while...")
    try:
        report = run_chaos(
            jobs=args.jobs,
            workers=args.workers,
            kills=args.kills,
            kill_interval=args.kill_interval,
            step_delay=args.step_delay,
            lease_seconds=args.lease_seconds,
            max_attempts=args.max_attempts,
            timeout=args.timeout,
            naive=args.naive,
            seed=args.seed,
            cleanup=args.cleanup,
        )
    except ChaosError as exc:
        raise SystemExit(f"Error: {exc}")

    print()
    print(render_report(report))
    if not report["ok"]:
        raise SystemExit(1)     # a non-zero exit code lets scripts and CI detect failure

# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="jobqueue", description="Reliable job queue")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create tables/indexes if they don't exist")

    p_enqueue = sub.add_parser("enqueue", help="Add a job to the queue")
    p_enqueue.add_argument("job_type")
    p_enqueue.add_argument("data", nargs="*", help="payload as key=value pairs")
    p_enqueue.add_argument("--max-attempts", type=int, default=5)
    p_enqueue.add_argument("--priority", type=int, default=0,
                           help="higher number runs first (default 0)")
    p_enqueue.add_argument("--delay", type=float, default=0,
                           help="seconds to wait before the job becomes runnable")

    p_show = sub.add_parser("show", help="Show one job")
    p_show.add_argument("job_id", type=int)

    p_worker = sub.add_parser("worker", help="Run a worker that processes jobs")
    p_worker.add_argument("--worker-id", default=None)
    p_worker.add_argument("--lease-seconds", type=float, default=30)
    p_worker.add_argument("--poll-interval", type=float, default=1.0)
    p_worker.add_argument("--backoff-base", type=float, default=2.0,
                          help="first retry delay in seconds (doubles each attempt)")
    p_worker.add_argument("--backoff-cap", type=float, default=300.0,
                          help="maximum retry delay in seconds")
    p_worker.add_argument("--drain", action="store_true",
                          help="exit when no job is currently due")

    # ---- inspection ----
    p_stats = sub.add_parser("stats", help="Show queue depth and health")
    p_stats.add_argument("--json", action="store_true", help="machine-readable output")
    p_stats.add_argument("--by-type", action="store_true",
                         help="also break the counts down per job type")

    p_inflight = sub.add_parser("in-flight", help="Show jobs currently held by workers")
    p_inflight.add_argument("--limit", type=int, default=50)

    p_serve = sub.add_parser("serve", help="Run the read-only HTTP API")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)

        # ---- chaos test ----
    p_chaos = sub.add_parser("chaos", help="Hard-kill workers mid-job and verify nothing breaks")
    p_chaos.add_argument("--jobs", type=int, default=100)
    p_chaos.add_argument("--workers", type=int, default=4)
    p_chaos.add_argument("--kills", type=int, default=6, help="max number of hard kills")
    p_chaos.add_argument("--kill-interval", type=float, default=1.0,
                         help="average seconds between kills")
    p_chaos.add_argument("--step-delay", type=float, default=0.1,
                         help="pause between a job's steps (the kill window), in seconds")
    p_chaos.add_argument("--lease-seconds", type=float, default=3.0)
    p_chaos.add_argument("--max-attempts", type=int, default=50)
    p_chaos.add_argument("--timeout", type=float, default=120.0)
    p_chaos.add_argument("--naive", action="store_true",
                         help="use the BROKEN handler (no idempotency) to see the failure")
    p_chaos.add_argument("--seed", type=int, default=None, help="repeatable kill schedule")
    p_chaos.add_argument("--cleanup", action="store_true",
                         help="delete this run's rows afterwards")

    # ---- dead-letter queue ----
    p_dead = sub.add_parser("dead", help="Inspect, replay or purge dead jobs")
    dead_sub = p_dead.add_subparsers(dest="dead_command", required=True)

    p_dlist = dead_sub.add_parser("list", help="List dead jobs and why they failed")
    p_dlist.add_argument("--limit", type=int, default=50)
    p_dlist.add_argument("--job-type", default=None)

    p_dreplay = dead_sub.add_parser("replay", help="Send dead job(s) back to the queue")
    p_dreplay.add_argument("job_id", type=int, nargs="?", default=None)
    p_dreplay.add_argument("--all", action="store_true", help="replay every dead job")
    p_dreplay.add_argument("--job-type", default=None, help="with --all: only this type")

    p_dpurge = dead_sub.add_parser("purge", help="Permanently delete dead job(s)")
    p_dpurge.add_argument("job_id", type=int, nargs="?", default=None)
    p_dpurge.add_argument("--all", action="store_true", help="delete every dead job")
    p_dpurge.add_argument("--job-type", default=None, help="with --all: only this type")
    p_dpurge.add_argument("--yes", action="store_true",
                          help="required confirmation for --all")

    args = parser.parse_args(argv)

    if args.command == "init-db":
        init_db()
        print("Database initialised.")
    elif args.command == "enqueue":
        try:
            job_id = enqueue(
                args.job_type,
                parse_key_values(args.data),
                max_attempts=args.max_attempts,
                priority=args.priority,
                delay_seconds=args.delay,
            )
        except ValueError as exc:
            raise SystemExit(f"Error: {exc}")
        print(f"Enqueued job {job_id}")
    elif args.command == "show":
        job = get_job(args.job_id)
        print(json.dumps(job, indent=2, default=str) if job else "Job not found")
    elif args.command == "worker":
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(message)s")
        worker = Worker(args.worker_id, args.lease_seconds, args.poll_interval,
                        args.backoff_base, args.backoff_cap)
        try:
            worker.run_forever(drain=args.drain)
        except KeyboardInterrupt:
            print("\nWorker stopped.")
    elif args.command == "stats":
        run_stats_command(args)
    elif args.command == "in-flight":
        run_in_flight_command(args)
    elif args.command == "serve":
        run_serve_command(args)
    elif args.command == "dead":
        run_dead_command(args)
    elif args.command == "chaos":
        run_chaos_command(args)