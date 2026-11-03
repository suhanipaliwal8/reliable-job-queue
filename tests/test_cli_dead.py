import pytest

from jobqueue.cli import main
from jobqueue.core import get_job


def test_cli_dead_list_then_replay(make_dead_job, capsys):
    job_id = make_dead_job()

    main(["dead", "list"])
    output = capsys.readouterr().out
    assert str(job_id) in output
    assert "boom" in output

    main(["dead", "replay", str(job_id)])
    assert get_job(job_id)["status"] == "pending"


def test_cli_purge_all_requires_confirmation(make_dead_job):
    job_id = make_dead_job()

    with pytest.raises(SystemExit):
        main(["dead", "purge", "--all"])             # refused without --yes
    assert get_job(job_id) is not None

    main(["dead", "purge", "--all", "--yes"])
    assert get_job(job_id) is None