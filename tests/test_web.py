import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from http.cookiejar import CookieJar
from urllib.parse import parse_qs, urlparse

import pytest

from nesi import auth, mailer
from nesi.web import parse_range, start_web_server

TODAY = date(2026, 9, 24)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


@pytest.fixture
def site(settings, store, monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "send", lambda s, subject, html, text, to=(): sent.append((to, text)))
    server = start_web_server(settings, store)
    base = f"http://127.0.0.1:{server.server_address[1]}"
    jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    raw = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), NoRedirect)

    def get(path):
        return opener.open(base + path).read().decode()

    def post(path, follow=True, **data):
        body = urllib.parse.urlencode(data).encode()
        try:
            resp = (opener if follow else raw).open(base + path, data=body)
            return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    yield get, post, sent, jar
    server.shutdown()


def _sign_in(get, post, sent, email="ops@raven.example"):
    post("/login", email=email)
    token = parse_qs(urlparse(sent[-1][1].split("\n")[-1]).query)["token"][0]
    assert "Continue" in get(f"/auth?token={token}")  # GET alone must not sign in
    return post("/auth", token=token)


def _csrf(page: str) -> str:
    return re.search(r'name="csrf" value="([0-9a-f]+)"', page).group(1)


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("2026-09-01", "2026-09-30", "future"),
        ("2026-09-10", "2026-09-01", "order"),
        ("2026-06-01", "2026-09-01", "too_long"),
        ("2019-12-31", "2020-01-01", "too_old"),
        ("bad", "2026-09-01", "dates"),
        ("2026-08-01", "2026-08-31", (date(2026, 8, 1), date(2026, 8, 31))),
    ],
)
def test_parse_range(start, end, expected):
    assert parse_range(start, end, TODAY) == expected


def test_dashboard_requires_sign_in(site):
    get, *_ = site
    assert "Email me a sign-in link" in get("/")


def test_unknown_email_gets_no_link(site):
    get, post, sent, _ = site
    status, page = post("/login", email="stranger@evil.example")
    assert status == 200 and "Check your email" in page
    assert sent == []


def test_sign_in_and_queue_reruns(site, store):
    get, post, sent, _ = site
    status, page = _sign_in(get, post, sent)
    assert status == 200 and "Re-run latest" in page
    assert sent[0][0] == ("ops@raven.example",)

    status, page = post("/runs", csrf=_csrf(page), kind="dates", **{"from": "2026-09-01", "to": "2026-09-03"})
    assert "Queued: GENCO · 1 Sep 2026 – 3 Sep 2026" in page
    run = store.next_active()
    assert (run.start_date, run.end_date, run.requested_by) == (
        date(2026, 9, 1),
        date(2026, 9, 3),
        "ops@raven.example",
    )


def test_failed_send_allows_immediate_retry(site, monkeypatch):
    get, post, sent, _ = site

    def boom(*a, **k):
        raise OSError("resend down")

    monkeypatch.setattr(mailer, "send", boom)
    post("/login", email="ops@raven.example")
    monkeypatch.setattr(mailer, "send", lambda s, subject, html, text, to=(): sent.append((to, text)))
    post("/login", email="ops@raven.example")
    assert len(sent) == 1  # not blocked by the one-per-minute limit


def test_sign_in_link_works_once(site):
    get, post, sent, _ = site
    post("/login", email="ops@raven.example")
    token = parse_qs(urlparse(sent[-1][1].split("\n")[-1]).query)["token"][0]
    assert post("/auth", token=token)[0] == 200
    assert post("/auth", token=token)[0] == 403


def test_post_without_csrf_is_rejected(site, store):
    get, post, sent, _ = site
    _sign_in(get, post, sent)
    status, _ = post("/runs", follow=False, csrf="wrong", kind="latest")
    assert status == 303 and store.next_active() is None


def test_email_link_queues_latest_without_sign_in(site, settings, store):
    get, post, *_ = site
    q = parse_qs(urlparse(auth.rerun_link(settings)).query)
    exp, sig = q["exp"][0], q["sig"][0]
    assert "Confirm re-run" in get(f"/rerun?exp={exp}&sig={sig}")
    assert store.next_active() is None  # opening the link alone must not trigger
    status, page = post("/rerun", exp=exp, sig=sig)
    assert "Re-run started" in page and store.next_active().kind == "latest"
    assert post("/rerun", exp=exp, sig="forged")[0] == 403


def test_people_can_add_and_remove_others(site, store):
    get, post, sent, jar = site
    _, page = _sign_in(get, post, sent)
    csrf = _csrf(page)

    _, page = post("/people", csrf=csrf, action="add", email="New.Person@Raven.example", reports="1")
    assert "Added new.person@raven.example" in page
    assert sent[-1][0] == ("new.person@raven.example",)  # welcome email
    assert "new.person@raven.example" in [u.email for u in store.users()]

    _, page = post("/people", csrf=csrf, action="reports-off", email="new.person@raven.example")
    assert "new.person@raven.example" not in store.report_recipients()

    # The new person can now sign in themselves.
    jar.clear()
    status, page = _sign_in(get, post, sent, email="new.person@raven.example")
    assert status == 200 and "People with access" in page
    csrf = _csrf(page)

    _, page = post("/people", csrf=csrf, action="remove", email="ops@raven.example")
    assert "Owners are set in GitHub" in page
    _, page = post("/people", csrf=csrf, action="remove", email="new.person@raven.example")
    assert "You can&#x27;t remove yourself" in page
    _, page = post("/people", csrf=csrf, action="add", email="not-an-email", reports="1")
    assert "Enter a valid email" in page


def test_removed_person_loses_access(site, store):
    get, post, sent, jar = site
    store.add_user("temp@raven.example", "ops@raven.example")
    _sign_in(get, post, sent, email="temp@raven.example")
    assert "Re-run latest" in get("/")
    store.remove_user("temp@raven.example")
    assert "Email me a sign-in link" in get("/")  # existing session no longer works


def test_forged_notice_is_not_shown(site):
    get, post, sent, _ = site
    _sign_in(get, post, sent)
    page = get("/?done=removed&email=ops@raven.example")  # ops still has access: not true
    assert "Removed" not in page


def test_session_signature_and_expiry():
    value = auth.make_session("k", "a@x", now=1000)
    assert auth.read_session("k", value, now=1001) == "a@x"
    assert auth.read_session("other", value, now=1001) is None
    assert auth.read_session("k", value, now=1000 + auth.SESSION_TTL + 1) is None
    assert auth.read_session("k", "garbage", now=1001) is None
