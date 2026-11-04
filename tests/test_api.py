import pymysql
from fastapi.testclient import TestClient

from jobqueue import api
from jobqueue.core import claim_job, enqueue

client = TestClient(api.app)


def test_health_is_ok_when_database_is_reachable():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_returns_503_when_database_is_down(monkeypatch):
    def broken():
        raise pymysql.err.OperationalError(2003, "cannot connect")

    monkeypatch.setattr(api, "transaction", broken)

    assert client.get("/health").status_code == 503


def test_stats_endpoint_returns_counts():
    enqueue("t")

    body = client.get("/stats").json()
    assert body["pending_ready"] == 1
    assert body["total"] == 1


def test_stats_by_type_endpoint_returns_rows():
    enqueue("email")

    body = client.get("/stats/by-type").json()
    assert body[0]["job_type"] == "email"
    assert body[0]["pending"] == 1


def test_in_flight_endpoint_lists_claimed_jobs():
    job_id = enqueue("t")
    claim_job("w1")

    response = client.get("/jobs/in-flight")   # must not be mistaken for a job id

    assert response.status_code == 200
    body = response.json()
    assert body[0]["id"] == job_id
    assert body[0]["locked_by"] == "w1"
    assert body[0]["lease_remaining_seconds"] > 0


def test_dead_endpoint_lists_dead_jobs_with_their_error(make_dead_job):
    job_id = make_dead_job()

    body = client.get("/jobs/dead").json()
    assert [j["id"] for j in body] == [job_id]
    assert body[0]["last_error"] == "boom"


def test_dead_endpoint_filters_by_job_type(make_dead_job):
    make_dead_job("email")
    sms_id = make_dead_job("sms")

    body = client.get("/jobs/dead", params={"job_type": "sms"}).json()
    assert [j["id"] for j in body] == [sms_id]


def test_job_detail_endpoint_returns_one_job():
    job_id = enqueue("email", {"to": "a@b.com"}, priority=3)

    body = client.get(f"/jobs/{job_id}").json()
    assert body["payload"] == {"to": "a@b.com"}
    assert body["priority"] == 3
    assert body["status"] == "pending"


def test_unknown_job_returns_404():
    assert client.get("/jobs/999999").status_code == 404


def test_invalid_limit_is_rejected_with_422():
    assert client.get("/jobs/dead", params={"limit": 0}).status_code == 422


def test_api_is_read_only():
    assert client.post("/stats").status_code == 405
    assert client.delete("/jobs/1").status_code == 405

def test_job_detail_lists_dependencies():
    a = enqueue("email")
    b = enqueue("t", depends_on=[a])

    body = client.get(f"/jobs/{b}").json()
    assert body["status"] == "waiting"
    assert body["depends_on"] == [{"id": a, "job_type": "email", "status": "pending"}]