import argparse
import json
import logging

from .core import (
    enqueue,
    get_job,
    list_dead_jobs,
    purge_dead_jobs,
    purge_job,
    replay_dead_jobs,
    replay_job,
)
from .schema import init_db
from .worker import Worker


def parse_key_values(pairs: list[str]) -> dict:
    data = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"Invalid argument '{pair}'. Use key=value.")
        key, value = pair.split("=", 1)
        data[key] = value
    return data


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
    elif args.command == "dead":
        run_dead_command(args)