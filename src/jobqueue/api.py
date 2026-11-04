import pymysql
from fastapi import FastAPI, HTTPException, Query
import secrets

from . import __version__
from .core import get_job, list_dead_jobs, list_in_flight_jobs, list_dependencies
from .db import transaction
from .stats import get_stats, get_stats_by_type
from pathlib import Path

from fastapi.responses import HTMLResponse

app = FastAPI(
    title="Reliable Job Queue",
    description="Read-only inspection API. Changes (replay, purge) go through the CLI.",
    version=__version__,
)

# NOTE: these are plain `def`, not `async def`. Our DB driver blocks, and FastAPI
# runs plain `def` endpoints in a thread pool so one slow query can't freeze the server.

DASHBOARD_FILE = Path(__file__).parent / "static" / "dashboard.html"


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    """The live dashboard. A fresh random nonce is put into the page and into the
    Content-Security-Policy header, so the browser runs ONLY our own inline
    script/style and refuses anything injected, and may talk only to this server."""
    nonce = secrets.token_urlsafe(16)
    html = DASHBOARD_FILE.read_text(encoding="utf-8").replace("__NONCE__", nonce)
    headers = {
        "Content-Security-Policy": (
            f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
            "connect-src 'self'; base-uri 'none'; form-action 'none'"
        ),
        "Cache-Control": "no-store",
    }
    return HTMLResponse(html, headers=headers)


@app.get("/health")
def health():
    """Is the API up AND can it reach the database?"""
    try:
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except pymysql.MySQLError:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}


@app.get("/stats")
def stats():
    return get_stats()


@app.get("/stats/by-type")
def stats_by_type():
    return get_stats_by_type()


# Static paths must come BEFORE "/jobs/{job_id}", or "in-flight" and "dead"
# would be treated as job ids.
@app.get("/jobs/in-flight")
def in_flight(limit: int = Query(50, ge=1, le=500)):
    return list_in_flight_jobs(limit=limit)


@app.get("/jobs/dead")
def dead(limit: int = Query(50, ge=1, le=500), job_type: str | None = None):
    return list_dead_jobs(limit=limit, job_type=job_type)


@app.get("/jobs/{job_id}")
def job_detail(job_id: int):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    job["depends_on"] = list_dependencies(job_id)
    return job