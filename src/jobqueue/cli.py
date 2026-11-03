import argparse
import json

from .core import enqueue, get_job
from .schema import init_db


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

    sub.add_parser("init-db", help="Create tables if they don't exist")

    p_enqueue = sub.add_parser("enqueue", help="Add a job to the queue")
    p_enqueue.add_argument("job_type")
    p_enqueue.add_argument("data", nargs="*", help="payload as key=value pairs")
    p_enqueue.add_argument("--max-attempts", type=int, default=5)

    p_show = sub.add_parser("show", help="Show one job")
    p_show.add_argument("job_id", type=int)

    args = parser.parse_args(argv)

    if args.command == "init-db":
        init_db()
        print("Database initialised.")
    elif args.command == "enqueue":
        job_id = enqueue(args.job_type, parse_key_values(args.data), args.max_attempts)
        print(f"Enqueued job {job_id}")
    elif args.command == "show":
        job = get_job(args.job_id)
        print(json.dumps(job, indent=2, default=str) if job else "Job not found")