import argparse
import json
import logging

from .core import enqueue, get_job
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
    p_worker.add_argument("--drain", action="store_true",
                          help="exit when no job is currently due")
    p_worker.add_argument("--backoff-base", type=float, default=2.0,
                          help="first retry delay in seconds (doubles each attempt)")
    p_worker.add_argument("--backoff-cap", type=float, default=300.0,
                          help="maximum retry delay in seconds")

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