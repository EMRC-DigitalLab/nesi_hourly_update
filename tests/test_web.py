import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from http.cookiejar import CookieJar
from urllib.parse import parse_qs, urlparse

import pytest

from nesi import auth, mailer
from nesi.store import MAX_FAILURES
from nesi.web import parse_range, start_web_server

TODAY = date(2026, 9, 24)
OWNER = "ops@raven.example"
PASSWORD = "correct horse battery"


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
        try:
            return opener.open(base + path).read().decode()
        except urllib.error.HTTPError as e:
            return e.read().decode()

    def post(path, follow=True, **data):
        body = urllib.parse.urlencode(data).encode()
        try:
            resp = (opener if follow else raw).open(base + path, data=body)
            return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    yield get, post, sent, jar
    server.shutdown()


def _token(sent) -> str:
    return re.search(r"token=([\w-]+)", sent[-1][1]).group(1)


def _set_password(get, post, sent, email=OWNER, password=PASSWORD):
    """First-time flow: ask for a link, open it, save a password (signs you in)."""
    post("/forgot", email=email)
    token = _token(sent)
    assert "Set your password" in get(f"/set-password?token={token}")
    return post("/set-password", token=token, password=password, confirm=password)


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
    page = get("/")
    assert 'type="password"' in page and "First time here" in page


def test_first_time_set_password_then_sign_in_without_email(site, store):
    get, post, sent, jar = site
    status, page = _set_password(get, post, sent)
    assert status == 200 and "Re-run latest" in page
    assert len(sent) == 1  # the one set-password email

    jar.clear()
    status, page = post("/login", email=OWNER, password=PASSWORD)
    assert status == 200 and "Re-run latest" in page
    assert len(sent) == 1  # signing in sent nothing


def test_wrong_password_and_unknown_email_look_the_same(site):
    get, post, sent, _ = site
    _set_password(get, post, sent)
    wrong = post("/login", email=OWNER, password="nope nope nope")
    unknown = post("/login", email="stranger@evil.example", password="whatever123")
    assert wrong[0] == unknown[0] == 401
    assert "Email or password is incorrect" in wrong[1] and "Email or password is incorrect" in unknown[1]


def test_lockout_after_repeated_failures_and_reset_unlocks(site, monkeypatch):
    get, post, sent, jar = site
    monkeypatch.setattr("nesi.store.LOGIN_EMAIL_INTERVAL", 0)  # allow a second link right away
    _set_password(get, post, sent)
    jar.clear()
    for _ in range(MAX_FAILURES):
        post("/login", email=OWNER, password="wrong password!")
    status, page = post("/login", email=OWNER, password=PASSWORD)
    assert status == 429 and "Too many wrong attempts" in page

    sent.clear()
    status, page = _set_password(get, post, sent, password="a brand new password")
    assert status == 200 and "Re-run latest" in page


def test_set_password_validation_keeps_link_usable(site):
    get, post, sent, _ = site
    post("/forgot", email=OWNER)
    token = _token(sent)
    assert post("/set-password", token=token, password="short", confirm="short")[0] == 400
    assert post("/set-password", token=token, password=PASSWORD, confirm="different one")[0] == 400
    assert post("/set-password", token=token, password=PASSWORD, confirm=PASSWORD)[0] == 200
    assert post("/set-password", token=token, password=PASSWORD, confirm=PASSWORD)[0] == 403  # used


def test_unknown_email_gets_no_link(site):
    get, post, sent, _ = site
    status, page = post("/forgot", email="stranger@evil.example")
    assert status == 200 and "Check your email" in page
    assert sent == []


def test_failed_send_allows_immediate_retry(site, monkeypatch):
    get, post, sent, _ = site

    def boom(*a, **k):
        raise OSError("resend down")

    monkeypatch.setattr(mailer, "send", boom)
    post("/forgot", email=OWNER)
    monkeypatch.setattr(mailer, "send", lambda s, subject, html, text, to=(): sent.append((to, text)))
    post("/forgot", email=OWNER)
    assert len(sent) == 1  # not blocked by the one-per-minute limit


def test_queue_reruns(site, store):
    get, post, sent, _ = site
    _, page = _set_password(get, post, sent)
    status, page = post("/runs", csrf=_csrf(page), kind="dates", **{"from": "2026-09-01", "to": "2026-09-03"})
    assert "Queued: GENCO · 1 Sep 2026 – 3 Sep 2026" in page
    run = store.next_active()
    assert (run.start_date, run.end_date, run.requested_by) == (date(2026, 9, 1), date(2026, 9, 3), OWNER)


def test_post_without_csrf_is_rejected(site, store):
    get, post, sent, _ = site
    _set_password(get, post, sent)
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
    _, page = _set_password(get, post, sent)
    csrf = _csrf(page)

    _, page = post("/people", csrf=csrf, action="add", email="New.Person@Raven.example", reports="1")
    assert "Added new.person@raven.example" in page
    assert sent[-1][0] == ("new.person@raven.example",)  # invite with set-password link
    assert "set a password yet" in page and "Resend link" in page

    _, page = post("/people", csrf=csrf, action="reports-off", email="new.person@raven.example")
    assert "new.person@raven.example" not in store.report_recipients()

    # The invite link lets the new person set a password and get in.
    invite = _token(sent)
    jar.clear()
    status, page = post("/set-password", token=invite, password=PASSWORD, confirm=PASSWORD)
    assert status == 200 and "People with access" in page
    csrf = _csrf(page)

    _, page = post("/people", csrf=csrf, action="remove", email=OWNER)
    assert "Owners are set in GitHub" in page
    _, page = post("/people", csrf=csrf, action="remove", email="new.person@raven.example")
    assert "You can&#x27;t remove yourself" in page
    _, page = post("/people", csrf=csrf, action="add", email="not-an-email", reports="1")
    assert "Enter a valid email" in page


def test_removed_person_loses_access(site, store):
    get, post, sent, jar = site
    store.add_user("temp@raven.example", OWNER)
    _set_password(get, post, sent, email="temp@raven.example")
    assert "Re-run latest" in get("/")
    store.remove_user("temp@raven.example")
    assert 'type="password"' in get("/")  # existing session no longer works


def test_forged_notice_is_not_shown(site):
    get, post, sent, _ = site
    _set_password(get, post, sent)
    page = get(f"/?done=removed&email={OWNER}")  # owner still has access: not true
    assert "Removed" not in page


def test_session_signature_and_expiry():
    value = auth.make_session("k", "a@x", now=1000)
    assert auth.read_session("k", value, now=1001) == "a@x"
    assert auth.read_session("other", value, now=1001) is None
    assert auth.read_session("k", value, now=1000 + auth.SESSION_TTL + 1) is None
    assert auth.read_session("k", "garbage", now=1001) is None
