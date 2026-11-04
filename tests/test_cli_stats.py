import json

from jobqueue.cli import main
from jobqueue.core import claim_job, enqueue


def test_cli_stats_prints_counts_and_points_to_dead_jobs(make_dead_job, capsys):
    make_dead_job()
    enqueue("t")

    main(["stats"])
    output = capsys.readouterr().out

    assert "QUEUE HEALTH" in output
    assert "jobqueue dead list" in output


def test_cli_stats_json_is_valid_json(capsys):
    enqueue("t")

    main(["stats", "--json"])
    data = json.loads(capsys.readouterr().out)

    assert data["pending_ready"] == 1
    assert data["total"] == 1


def test_cli_stats_by_type_prints_a_per_type_table(capsys):
    enqueue("email")

    main(["stats", "--by-type"])
    output = capsys.readouterr().out

    assert "TYPE" in output
    assert "email" in output


def test_cli_stats_warns_about_expired_leases(expire_lease, capsys):
    job_id = enqueue("t")
    claim_job("w1")
    expire_lease(job_id)

    main(["stats"])

    assert "reclaim" in capsys.readouterr().out


def test_cli_in_flight_flags_expired_leases(expire_lease, capsys):
    job_id = enqueue("t")
    claim_job("w1")
    expire_lease(job_id)

    main(["in-flight"])
    output = capsys.readouterr().out

    assert "w1" in output
    assert "EXPIRED" in output


def test_cli_in_flight_with_nothing_running(capsys):
    main(["in-flight"])

    assert "No jobs are in flight." in capsys.readouterr().out