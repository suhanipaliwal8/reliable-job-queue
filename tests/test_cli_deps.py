import pytest

from jobqueue.cli import main
from jobqueue.core import enqueue, get_job


def test_cli_enqueue_with_depends_on_creates_a_waiting_job(capsys):
    a = enqueue("t")

    main(["enqueue", "t", "--depends-on", str(a)])
    b = int(capsys.readouterr().out.split()[-1])      # "Enqueued job 2"

    assert get_job(b)["status"] == "waiting"



def test_cli_enqueue_with_unknown_dependency_exits_with_an_error():
    with pytest.raises(SystemExit):
        main(["enqueue", "t", "--depends-on", "999999"])


def test_cli_cancel_removes_a_waiting_job(capsys):
    a = enqueue("t")
    b = enqueue("t", depends_on=[a])

    main(["cancel", str(b)])

    assert get_job(b) is None
    assert "cancelled" in capsys.readouterr().out


def test_cli_cancel_refuses_a_job_that_is_not_waiting():
    a = enqueue("t")

    with pytest.raises(SystemExit):
        main(["cancel", str(a)])
    assert get_job(a) is not None


def test_cli_stats_warns_about_jobs_blocked_by_a_dead_job(make_dead_job, capsys):
    dead_id = make_dead_job()
    enqueue("t", depends_on=[dead_id])

    main(["stats"])
    output = capsys.readouterr().out

    assert "waiting on dependencies" in output
    assert "jobqueue cancel" in output