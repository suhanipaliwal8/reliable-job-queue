import re

from fastapi.testclient import TestClient

from jobqueue import api

client = TestClient(api.app)


def test_dashboard_is_served_as_html_and_is_read_only():
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Job Queue" in response.text
    assert client.post("/").status_code == 405


def test_dashboard_uses_a_fresh_csp_nonce_on_every_load():
    first = client.get("/")
    second = client.get("/")

    nonce_1 = re.search(r"nonce-([\w-]+)", first.headers["content-security-policy"]).group(1)
    nonce_2 = re.search(r"nonce-([\w-]+)", second.headers["content-security-policy"]).group(1)

    assert nonce_1 != nonce_2
    assert f'nonce="{nonce_1}"' in first.text          # the page carries the same nonce
    assert "__NONCE__" not in first.text               # the placeholder was replaced
    assert "default-src 'none'" in first.headers["content-security-policy"]


def test_dashboard_never_builds_markup_from_strings():
    """Regression guard against XSS: job types and errors are untrusted text, so the
    page must only use textContent, never HTML-string APIs."""
    html = client.get("/").text

    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"):
        assert forbidden not in html


def test_dashboard_only_polls_existing_read_only_endpoints():
    html = client.get("/").text

    paths = re.findall(r'getJson\("([^"]+)"\)', html)
    assert paths                                       # we found the polled endpoints
    for path in paths:
        assert client.get(path).status_code == 200     # each one really exists
    assert "method:" not in html                       # no POST/DELETE calls from the page