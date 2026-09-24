import pytest

from nesi.config import Settings
from nesi.store import Store


@pytest.fixture
def settings(monkeypatch, tmp_path) -> Settings:
    env = {
        "DB_HOST": "h",
        "DB_NAME": "d",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
        "STATUS_FILE": str(tmp_path / "status.json"),
        "STATE_DB": str(tmp_path / "state.sqlite3"),
        # http, so the test client sends the (non-Secure) session cookie.
        "PUBLIC_BASE_URL": "http://nesi.example",
        "RERUN_SECRET": "s3cret",
        "RESEND_API_KEY": "re_test",
        "REPORT_FROM": "NESI <r@nesi.example>",
        "REPORT_TO": "Ops@Raven.example, boss@raven.example",
        "WEB_PORT": "0",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return Settings.from_env()


@pytest.fixture
def store(settings) -> Store:
    s = Store(settings.state_db)
    s.ensure_owners(settings.dashboard_users)
    return s
