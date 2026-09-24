"""The re-run site at PUBLIC_BASE_URL.

Routes:
  /login, /logout          email + password, 30-day signed session cookie
  /forgot, /set-password   emailed one-time link to set or reset a password
  /                        re-run latest data or GENCO for chosen dates, history, people
  /runs, /people           POST: queue a re-run / manage people (signed in + CSRF token)
  /rerun                   one-click link from the report email (signed, 24 h)
  /healthz                 liveness

The only emails sent are the set-password links: when someone is added, and
when they ask to reset. Signing in never sends email.

Runs in a thread of the scheduler process; it only queues work in the Store,
and the scheduler loop executes it, so jobs never overlap.
"""

from __future__ import annotations

import html
import logging
import re
import threading
from datetime import date
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

from nesi import auth, mailer
from nesi.clock import now_wat
from nesi.config import Settings
from nesi.store import INVITE_TOKEN_TTL, RESET_TOKEN_TTL, Run, Store, User, check_password

log = logging.getLogger(__name__)

COOKIE = "nesi_session"
MIN_PASSWORD = 10
MAX_DAYS = 62
EARLIEST = date(2020, 1, 1)
e = html.escape

ERRORS = {
    "dates": "Pick a valid From and To date.",
    "order": "The From date must be on or before the To date.",
    "future": "Dates can't be in the future.",
    "too_old": "Dates before 2020 aren't available.",
    "too_long": f"Pick at most {MAX_DAYS} days per re-run.",
    "csrf": "Your session changed. Please try again.",
    "email": "Enter a valid email address.",
    "owner": "Owners are set in GitHub and can't be removed here.",
    "self": "You can't remove yourself.",
}
NOTICES = {
    "added": "Added {email}. They've been emailed a link to set their password.",
    "invited": "Sent {email} a new link to set their password.",
    "exists": "{email} already has access.",
    "removed": "Removed {email}.",
    "reports-on": "Report emails on for {email}.",
    "reports-off": "Report emails off for {email}.",
}
EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]+\.[^@\s]{2,}$")


def parse_range(start: str, end: str, today: date) -> tuple[date, date] | str:
    """Validated (start, end) or an ERRORS key."""
    try:
        a, b = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError:
        return "dates"
    if a > b:
        return "order"
    if b > today:
        return "future"
    if a < EARLIEST:
        return "too_old"
    if (b - a).days + 1 > MAX_DAYS:
        return "too_long"
    return a, b


def _fmt_day(d: date) -> str:
    return f"{d.day} {d:%b %Y}"


def describe(run: Run) -> str:
    if run.kind == "latest":
        return "Latest data (GENCO + DISCO)"
    if run.start_date == run.end_date:
        return f"GENCO · {_fmt_day(run.start_date)}"
    return f"GENCO · {_fmt_day(run.start_date)} – {_fmt_day(run.end_date)}"


def status_text(run: Run) -> str:
    if run.status == "queued":
        return "Queued"
    if run.status == "running":
        return f"Running · day {run.days_done + 1} of {run.days_total}" if run.kind == "dates" else "Running"
    if run.status == "done":
        return "Done"
    if run.failed_days:
        days = ", ".join(_fmt_day(date.fromisoformat(d))[:-5] for d in run.failed_days[:5])
        more = f" +{len(run.failed_days) - 5} more" if len(run.failed_days) > 5 else ""
        return f"Failed · {len(run.failed_days)} of {run.days_total} days ({days}{more})"
    return "Failed"


# --- presentation --------------------------------------------------------------

CSS = """
:root{--ink:#1f2328;--muted:#656d76;--line:#d0d7de;--bg:#f6f8fa;--accent:#0b6e4f;--warn:#9a6700;--bad:#cf222e;--ok:#1a7f37}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
header{background:#fff;border-bottom:1px solid var(--line)}
.bar,main{max-width:780px;margin:0 auto;padding-left:16px;padding-right:16px}
.bar{display:flex;align-items:center;justify-content:space-between;height:52px}
.brand{font-weight:600}.brand span{color:var(--muted);font-weight:400;margin-left:6px}
.who{color:var(--muted);display:flex;gap:10px;align-items:center}
main{padding-top:24px;padding-bottom:48px}
section{background:#fff;border:1px solid var(--line);border-radius:6px;padding:20px;margin-bottom:16px}
h1{font-size:18px;margin:0 0 6px}h2{font-size:15px;margin:0 0 4px}
.help{color:var(--muted);margin:0 0 14px}
.btn{border:1px solid var(--accent);background:var(--accent);color:#fff;border-radius:6px;padding:8px 14px;font:inherit;font-weight:600;cursor:pointer}
.btn:hover{filter:brightness(.95)}
.chip{border:1px solid var(--line);background:#fff;color:var(--ink);border-radius:6px;padding:4px 10px;font:inherit;font-size:13px;cursor:pointer}
.chip:hover{background:var(--bg)}
.linkbtn{background:none;border:0;color:var(--muted);font:inherit;cursor:pointer;padding:0;text-decoration:underline}
.presets{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}
.fields{display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end;margin-bottom:16px}
.or{color:var(--muted);padding-bottom:8px}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
input{font:inherit;padding:7px 9px;border:1px solid var(--line);border-radius:6px;background:#fff;color:var(--ink)}
input[type=email]{width:100%}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:8px 6px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--muted);font-weight:600}
tr:last-child td{border-bottom:0}
.muted{color:var(--muted)}.nowrap{white-space:nowrap}
.sub{color:var(--muted);font-size:12px;margin-top:2px;word-break:break-word}
.st::before{content:"";display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px;background:var(--muted)}
.st.running::before{background:var(--warn)}.st.done::before{background:var(--ok)}.st.failed::before{background:var(--bad)}
.note{padding:10px 12px;border-radius:6px;margin-bottom:16px;border:1px solid}
.note.ok{background:#dafbe1;border-color:#aceebb}.note.err{background:#ffebe9;border-color:#ffcecb}
.narrow{max-width:420px;margin:56px auto}
form.add{margin-top:18px;padding-top:16px;border-top:1px solid var(--line)}
.grow{flex:1;min-width:220px}
label.check{display:flex;gap:6px;align-items:center;color:var(--ink);font-size:14px;margin:0 0 8px}
td form{margin:0}form.inline{display:inline}
.field{margin-bottom:12px}.field input{width:100%}
.links{margin-top:14px;font-size:13px}.links a{color:var(--muted)}
@media (max-width:600px){.hide-sm{display:none}}
"""

JS = """
(function () {
  var f = document.getElementById('from'), t = document.getElementById('to'),
      m = document.getElementById('month'), btn = document.getElementById('dates-btn');
  if (!f) return;
  var today = new Date(document.body.dataset.today + 'T00:00:00Z');
  function iso(d) { return d.toISOString().slice(0, 10); }
  function day(y, mo, d) { return new Date(Date.UTC(y, mo, d)); }
  function add(d, n) { var x = new Date(d); x.setUTCDate(x.getUTCDate() + n); return x; }
  function set(a, b) { f.value = iso(a); t.value = iso(b > today ? today : b); }
  function update() {
    var n = f.value && t.value ? Math.round((new Date(t.value) - new Date(f.value)) / 864e5) + 1 : 0;
    btn.textContent = n > 0 ? 'Re-run ' + n + ' day' + (n > 1 ? 's' : '') : 'Re-run selected dates';
  }
  var y = today.getUTCFullYear(), mo = today.getUTCMonth();
  var presets = {
    'yesterday': function () { var d = add(today, -1); set(d, d); },
    'week': function () { set(add(today, -6), today); },
    'this-month': function () { set(day(y, mo, 1), today); },
    'last-month': function () { set(day(y, mo - 1, 1), day(y, mo, 0)); }
  };
  document.querySelectorAll('[data-preset]').forEach(function (b) {
    b.addEventListener('click', function () { presets[b.dataset.preset](); m.value = ''; update(); });
  });
  m.addEventListener('change', function () {
    if (!m.value) return;
    var p = m.value.split('-');
    set(day(+p[0], +p[1] - 1, 1), day(+p[0], +p[1], 0));
    update();
  });
  [f, t].forEach(function (el) { el.addEventListener('change', function () { m.value = ''; update(); }); });
  update();
})();
"""


def page(title: str, body: str, *, user: str | None = None, csrf: str = "", refresh: bool = False) -> bytes:
    who = ""
    if user:
        who = (
            f'<div class="who">{e(user)}<form method="post" action="/logout">'
            f'<input type="hidden" name="csrf" value="{e(csrf)}">'
            '<button class="linkbtn" type="submit">Sign out</button></form></div>'
        )
    meta = '<meta http-equiv="refresh" content="10">' if refresh else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">{meta}
<title>{e(title)} · NESI</title>
<link rel="stylesheet" href="/static/app.css">
</head><body data-today="{now_wat().date().isoformat()}">
<header><div class="bar"><div class="brand">NESI<span>Re-run</span></div>{who}</div></header>
<main>{body}</main>
<script src="/static/app.js"></script>
</body></html>""".encode()


def _runs_table(runs: list[Run]) -> str:
    if not runs:
        return '<p class="muted">No re-runs yet.</p>'
    rows = "".join(
        f"<tr><td>{e(describe(r))}"
        f'<div class="sub">{e(r.requested_by)} · {e(r.requested_at)}</div></td>'
        f'<td><span class="st {e(r.status)}">{e(status_text(r))}</span></td>'
        f'<td class="hide-sm muted nowrap">{e(r.finished_at or "")}</td></tr>'
        for r in runs
    )
    return f"<table><tr><th>Re-run</th><th>Status</th><th class=hide-sm>Finished</th></tr>{rows}</table>"


def _people(users: list[User], owners: frozenset[str], me: str, csrf: str) -> str:
    rows = []
    for u in users:
        hidden = f'<input type="hidden" name="csrf" value="{e(csrf)}"><input type="hidden" name="email" value="{e(u.email)}">'
        toggle = (
            f'<form method="post" action="/people">{hidden}'
            f'<input type="hidden" name="action" value="{"reports-off" if u.reports else "reports-on"}">'
            f'<button class="chip" type="submit">{"On" if u.reports else "Off"}</button></form>'
        )
        if u.email in owners:
            remove = '<span class="muted">Owner</span>'
        elif u.email == me:
            remove = '<span class="muted">You</span>'
        else:
            remove = (
                f'<form method="post" action="/people">{hidden}<input type="hidden" name="action" value="remove">'
                '<button class="linkbtn" type="submit">Remove</button></form>'
            )
        details = [] if u.added_by == "setup" else [f"Added by {e(u.added_by)} · {e(u.added_at)}"]
        if not u.has_password:
            details.append(
                "Hasn't set a password yet · "
                f'<form method="post" action="/people" class="inline">{hidden}'
                '<input type="hidden" name="action" value="invite">'
                '<button class="linkbtn" type="submit">Resend link</button></form>'
            )
        sub = "".join(f'<div class="sub">{d}</div>' for d in details)
        rows.append(f"<tr><td>{e(u.email)}{sub}</td><td>{toggle}</td><td>{remove}</td></tr>")
    return f"""
  <table><tr><th>Email</th><th>Report emails</th><th></th></tr>{"".join(rows)}</table>
  <form method="post" action="/people" class="add">
    <input type="hidden" name="csrf" value="{e(csrf)}"><input type="hidden" name="action" value="add">
    <div class="fields">
      <div class="grow"><label for="new-email">Add someone</label>
        <input type="email" id="new-email" name="email" placeholder="name@company.com" required></div>
      <label class="check"><input type="checkbox" name="reports" value="1" checked> Send them report emails</label>
    </div>
    <button class="btn" type="submit">Add</button>
  </form>"""


def dashboard(
    user: str,
    csrf: str,
    runs: list[Run],
    users: list[User],
    owners: frozenset[str],
    notice: str = "",
    error: str = "",
) -> bytes:
    today = now_wat().date().isoformat()
    note = ""
    if notice:
        note = f'<div class="note ok">{e(notice)}</div>'
    elif error in ERRORS:
        note = f'<div class="note err">{e(ERRORS[error])}</div>'
    body = f"""{note}
<section>
  <h2>Re-run latest data</h2>
  <p class="help">Pulls today's GENCO readings (plus yesterday before 07:00) and the live DISCO
  allocation from niggrid.org, then emails a fresh report.</p>
  <form method="post" action="/runs">
    <input type="hidden" name="csrf" value="{e(csrf)}"><input type="hidden" name="kind" value="latest">
    <button class="btn" type="submit">Re-run latest</button>
  </form>
</section>
<section>
  <h2>Re-run GENCO for past dates</h2>
  <p class="help">Re-pulls every hour for the chosen days and fixes missing or changed readings.
  Up to {MAX_DAYS} days at a time; each day takes 1–2 minutes. DISCO can't be re-run for past
  dates because niggrid.org only shows live values.</p>
  <div class="presets">
    <button class="chip" type="button" data-preset="yesterday">Yesterday</button>
    <button class="chip" type="button" data-preset="week">Last 7 days</button>
    <button class="chip" type="button" data-preset="this-month">This month</button>
    <button class="chip" type="button" data-preset="last-month">Last month</button>
  </div>
  <form method="post" action="/runs">
    <input type="hidden" name="csrf" value="{e(csrf)}"><input type="hidden" name="kind" value="dates">
    <div class="fields">
      <div><label for="from">From</label><input type="date" id="from" name="from" max="{today}" min="{EARLIEST}" required></div>
      <div><label for="to">To</label><input type="date" id="to" name="to" max="{today}" min="{EARLIEST}" required></div>
      <div class="or">or</div>
      <div><label for="month">Whole month</label><input type="month" id="month" max="{today[:7]}"></div>
    </div>
    <button class="btn" type="submit" id="dates-btn">Re-run selected dates</button>
  </form>
</section>
<section>
  <h2>Recent re-runs</h2>
  <p class="help">Refreshes automatically while a re-run is in progress.</p>
  {_runs_table(runs)}
</section>
<section>
  <h2>People with access</h2>
  <p class="help">Everyone here can sign in, run re-runs and add or remove people. New people get
  one email with a link to set their password.</p>
  {_people(users, owners, user, csrf)}
</section>"""
    return page("Re-run", body, user=user, csrf=csrf, refresh=any(r.active for r in runs))


def _card(title: str, text: str, extra: str = "") -> str:
    help_text = f'<p class="help">{text}</p>' if text else ""
    return f'<section class="narrow"><h1>{e(title)}</h1>{help_text}{extra}</section>'


def _error(message: str) -> str:
    return f'<div class="note err">{e(message)}</div>' if message else ""


def login_page(email: str = "", error: str = "") -> bytes:
    form = f"""{_error(error)}<form method="post" action="/login">
  <div class="field"><label for="email">Email</label>
    <input type="email" id="email" name="email" value="{e(email)}" required autocomplete="username" autofocus></div>
  <div class="field"><label for="password">Password</label>
    <input type="password" id="password" name="password" required autocomplete="current-password"></div>
  <button class="btn" type="submit">Sign in</button></form>
<p class="links"><a href="/forgot">First time here, or forgot your password?</a></p>"""
    return page("Sign in", _card("Sign in", "", form))


def forgot_page(error: str = "") -> bytes:
    form = f"""{_error(error)}<form method="post" action="/forgot">
  <div class="field"><label for="email">Email</label>
    <input type="email" id="email" name="email" required autocomplete="email" autofocus></div>
  <button class="btn" type="submit">Email me a link</button></form>
<p class="links"><a href="/login">Back to sign in</a></p>"""
    text = "We'll email you a link to set your password. You only need this once."
    return page("Set your password", _card("Set your password", text, form))


def set_password_page(email: str, token: str, error: str = "") -> bytes:
    form = f"""{_error(error)}<form method="post" action="/set-password">
  <input type="hidden" name="token" value="{e(token)}">
  <input type="email" name="email" value="{e(email)}" autocomplete="username" hidden>
  <div class="field"><label for="password">New password</label>
    <input type="password" id="password" name="password" required minlength="{MIN_PASSWORD}"
      autocomplete="new-password" autofocus></div>
  <div class="field"><label for="confirm">Type it again</label>
    <input type="password" id="confirm" name="confirm" required autocomplete="new-password"></div>
  <button class="btn" type="submit">Save password and sign in</button></form>"""
    text = f"For <b>{e(email)}</b>. At least {MIN_PASSWORD} characters."
    return page("Set your password", _card("Set your password", text, form))


# --- HTTP ----------------------------------------------------------------------


def _handler(settings: Settings, store: Store) -> type[BaseHTTPRequestHandler]:
    secret = settings.rerun_secret
    secure = "; Secure" if settings.public_base_url.startswith("https://") else ""
    owners = settings.dashboard_users
    store.ensure_owners(owners)

    def allowed(email: str) -> bool:
        return email in owners or store.has_user(email)

    class Handler(BaseHTTPRequestHandler):
        server_version = "nesi"

        def log_message(self, fmt, *args) -> None:
            log.info("web %s %s", self.headers.get("X-Real-IP", self.client_address[0]), fmt % args)

        # -- helpers --

        def _send(self, status: int, body: bytes, ctype: str = "text/html; charset=utf-8", headers=()):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; style-src 'self'; script-src 'self'; form-action 'self'; "
                "base-uri 'none'; frame-ancestors 'none'",
            )
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def _redirect(self, location: str, headers=()) -> None:
            self._send(303, b"", headers=(("Location", location), *headers))

        def _form(self) -> dict[str, str]:
            length = min(int(self.headers.get("Content-Length") or 0), 8192)
            raw = self.rfile.read(length).decode(errors="replace")
            return {k: v[0] for k, v in parse_qs(raw).items()}

        def _session(self) -> tuple[str | None, str]:
            cookie = SimpleCookie(self.headers.get("Cookie", ""))
            value = cookie[COOKIE].value if COOKIE in cookie else ""
            user = auth.read_session(secret, value) if value else None
            if user and not allowed(user):
                user = None  # removed from the people list since signing in
            return user, value

        def _set_cookie(self, value: str, max_age: int) -> tuple[str, str]:
            return (
                "Set-Cookie",
                f"{COOKIE}={value}; Path=/; Max-Age={max_age}; HttpOnly; SameSite=Lax{secure}",
            )

        def _sign_in(self, email: str) -> None:
            session = auth.make_session(secret, email)
            self._redirect("/", (self._set_cookie(session, auth.SESSION_TTL),))

        def _link_expired(self) -> None:
            text = "This link has expired or was already used. Ask for a new one below."
            link = '<p class="links"><a href="/forgot">Email me a new link</a></p>'
            self._send(403, page("Link expired", _card("Link expired", text, link)))

        def _expired_link(self) -> None:
            self._send(
                403,
                page(
                    "Link expired",
                    _card(
                        "Link expired or invalid",
                        "Re-run links in report emails work for 24 hours.",
                        '<p><a href="/">Open the re-run page</a></p>',
                    ),
                ),
            )

        # -- routes --

        def do_GET(self) -> None:
            url = urlparse(self.path)
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            if url.path == "/healthz":
                return self._send(200, b"ok", "text/plain")
            if url.path == "/static/app.css":
                return self._send(200, CSS.encode(), "text/css; charset=utf-8")
            if url.path == "/static/app.js":
                return self._send(200, JS.encode(), "text/javascript; charset=utf-8")
            if url.path == "/login":
                if self._session()[0]:
                    return self._redirect("/")
                return self._send(200, login_page())
            if url.path == "/forgot":
                return self._send(200, forgot_page())
            if url.path == "/set-password":
                # Viewing the link doesn't use it up (mail scanners open links); saving does.
                token = query.get("token", "")
                email = store.peek_login_token(token)
                if not email or not allowed(email):
                    return self._link_expired()
                return self._send(200, set_password_page(email, token))
            if url.path == "/rerun":
                if not auth.verify_link(secret, query.get("exp", ""), query.get("sig", "")):
                    return self._expired_link()
                form = (
                    '<form method="post" action="/rerun">'
                    f'<input type="hidden" name="exp" value="{e(query["exp"])}">'
                    f'<input type="hidden" name="sig" value="{e(query["sig"])}">'
                    '<button class="btn" type="submit">Confirm re-run</button></form>'
                )
                text = "Pulls the latest GENCO and DISCO data again and emails a fresh report, usually within 5 minutes."
                return self._send(200, page("Re-run", _card("Re-run the scrapers?", text, form)))
            if url.path == "/":
                user, session = self._session()
                if not user:
                    return self._redirect("/login")
                queued = query.get("queued", "")
                notice = ""
                done, email = query.get("done", ""), query.get("email", "")
                # Only confirm what actually happened, so the URL can't carry made-up messages.
                if done in NOTICES and EMAIL_RE.match(email) and store.has_user(email) != (done == "removed"):
                    notice = NOTICES[done].format(email=email)
                if queued.isdigit():
                    notice = f"Queued: {describe(store.get(int(queued)))}. It starts within a few seconds."
                csrf = auth.csrf_token(secret, session)
                body = dashboard(
                    user, csrf, store.recent(), store.users(), owners, notice, query.get("error", "")
                )
                return self._send(200, body)
            self._send(
                404, page("Not found", _card("Not found", "", '<p><a href="/">Go to the re-run page</a></p>'))
            )

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            form = self._form()

            if path == "/login":
                email = form.get("email", "").strip().lower()
                if store.is_locked(email):
                    log.warning("Sign-in blocked for %s: too many wrong passwords", email)
                    msg = "Too many wrong attempts. Wait 15 minutes, or set a new password below."
                    return self._send(429, login_page(email, msg))
                # Always run the password check so timing doesn't reveal who has access.
                ok = check_password(form.get("password", ""), store.password_hash(email)) and allowed(email)
                if not ok:
                    store.record_failure(email)
                    log.info("Failed sign-in for %s", email)
                    return self._send(401, login_page(email, "Email or password is incorrect."))
                store.clear_failures(email)
                log.info("Signed in: %s", email)
                return self._sign_in(email)

            if path == "/forgot":
                email = form.get("email", "").strip().lower()
                if not allowed(email):
                    log.warning("Password link requested for an email not on the people list")
                elif not settings.reports_enabled:
                    log.error("Can't send password link: set RESEND_API_KEY and REPORT_FROM")
                elif token := store.create_login_token(email, RESET_TOKEN_TTL):
                    self._email_set_password(email, token)
                else:
                    log.info("Password link for %s not re-sent: one was sent less than a minute ago", email)
                text = (
                    f"If <b>{e(email)}</b> has access, a link to set your password is on its way. "
                    "It works once and expires in 1 hour."
                )
                return self._send(200, page("Check your email", _card("Check your email", text)))

            if path == "/set-password":
                token = form.get("token", "")
                email = store.peek_login_token(token)
                if not email or not allowed(email):
                    return self._link_expired()
                password = form.get("password", "")
                if len(password) < MIN_PASSWORD:
                    error = f"Use at least {MIN_PASSWORD} characters."
                    return self._send(400, set_password_page(email, token, error))
                if password != form.get("confirm", ""):
                    return self._send(400, set_password_page(email, token, "The two passwords don't match."))
                if store.consume_login_token(token) != email:
                    return self._link_expired()
                store.set_password(email, password)
                log.info("Password set for %s", email)
                return self._sign_in(email)

            if path == "/rerun":
                if not auth.verify_link(secret, form.get("exp", ""), form.get("sig", "")):
                    return self._expired_link()
                store.enqueue_latest("report email link")
                text = "The scrapers are running now. A fresh report will arrive by email when they finish."
                return self._send(
                    200, page("Re-run", _card("Re-run started", text, '<p><a href="/">Track it here</a></p>'))
                )

            user, session = self._session()
            if not user:
                return self._redirect("/login")
            if form.get("csrf") != auth.csrf_token(secret, session):
                return self._redirect("/?error=csrf")

            if path == "/logout":
                return self._redirect("/login", (self._set_cookie("", 0),))

            if path == "/runs":
                if form.get("kind") == "latest":
                    run = store.enqueue_latest(user)
                else:
                    parsed = parse_range(form.get("from", ""), form.get("to", ""), now_wat().date())
                    if isinstance(parsed, str):
                        return self._redirect("/?" + urlencode({"error": parsed}))
                    run = store.enqueue_dates(*parsed, requested_by=user)
                log.info("Re-run #%d queued by %s: %s", run.id, user, describe(run))
                return self._redirect(f"/?queued={run.id}")

            if path == "/people":
                return self._people(user, form)

            self._send(404, page("Not found", _card("Not found", "")))

        def _people(self, user: str, form: dict[str, str]) -> None:
            email = form.get("email", "").strip().lower()
            action = form.get("action")
            if not EMAIL_RE.match(email) or len(email) > 254:
                return self._redirect("/?error=email")
            if action == "add":
                if store.add_user(email, user, reports=form.get("reports") == "1"):
                    log.info("%s added %s", user, email)
                    self._invite(email, user)
                    done = "added"
                else:
                    done = "exists"
            elif action == "invite":
                self._invite(email, user)
                done = "invited"
            elif action == "remove":
                if email in owners:
                    return self._redirect("/?error=owner")
                if email == user:
                    return self._redirect("/?error=self")
                store.remove_user(email)
                log.info("%s removed %s", user, email)
                done = "removed"
            elif action in ("reports-on", "reports-off"):
                store.set_reports(email, action == "reports-on")
                done = action
            else:
                return self._redirect("/")
            self._redirect("/?" + urlencode({"done": done, "email": email}))

        def _invite(self, email: str, added_by: str) -> None:
            if not settings.reports_enabled:
                log.error("Can't email %s a password link: set RESEND_API_KEY and REPORT_FROM", email)
            elif token := store.create_login_token(email, INVITE_TOKEN_TTL):
                self._email_set_password(email, token, added_by=added_by)

        def _email_set_password(self, email: str, token: str, added_by: str | None = None) -> None:
            link = f"{settings.public_base_url}/set-password?{urlencode({'token': token})}"
            if added_by:
                subject = "You now have access to NESI Re-run"
                intro = (
                    f"{e(added_by)} gave you access to NESI Re-run, where you can re-pull GENCO and "
                    "DISCO data from niggrid.org. Set your password to get started; after that you "
                    "sign in with your email and password."
                )
                expiry = "This link works once and expires in 7 days."
            else:
                subject = "Set your NESI Re-run password"
                intro = "Use this link to set your NESI Re-run password."
                expiry = "It works once and expires in 1 hour. If you didn't ask for this, ignore this email."
            html_body = (
                '<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#1f2328">'
                f"<p>{intro}</p>"
                f'<p><a href="{e(link)}" style="display:inline-block;background:#0b6e4f;color:#fff;'
                'text-decoration:none;padding:10px 18px;border-radius:6px;font-weight:600">Set password</a></p>'
                f'<p style="color:#656d76;font-size:12px">{expiry}</p></div>'
            )
            text = f"{html.unescape(intro)}\n\nSet your password: {link}\n\n{expiry}"
            try:
                mailer.send(settings, subject, html_body, text, to=(email,))
            except Exception:
                log.exception("Could not send password email to %s", email)
                store.discard_login_token(token)  # so they can retry straight away

    return Handler


def start_web_server(settings: Settings, store: Store) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", settings.web_port), _handler(settings, store))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="web", daemon=True).start()
    log.info("Re-run site on :%d (%s)", settings.web_port, settings.public_base_url)
    return server
