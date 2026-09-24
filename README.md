# NESI hourly update

Scrapes [niggrid.org](https://www.niggrid.org) every hour and writes to MySQL (`jksutauf_nesidb`) on the VPS:

| Job | Source page | Table | Notes |
|---|---|---|---|
| `genco` | Genco Generation Readings | `combined_hourly_energy_generated_mwh` | Re-scrapes the whole day each run; before 07:00 WAT also re-scrapes yesterday (for `24:00` and late corrections). Hours that haven't happened yet are never written. |
| `disco` | `/discoloadprofile` | `discoloadprofile` | Live values only, one row per company per hour. Past hours cannot be backfilled. |

## How it runs

One long-running container (`nesi_scraper`) on the VPS runs a scheduler that fires at `RUN_MINUTE` (default :05 UTC) and runs `genco` then `disco`, each in its own process with a hard timeout. Only one Chromium runs at a time. Writes are idempotent upserts guarded by unique keys, so re-runs never create duplicates.

```
push to main → GitHub Actions: lint + tests → build image → push to GHCR → SSH to VPS → check-db → restart → wait for healthy
```

The container reaches MySQL on the host through `host.docker.internal`, from the fixed Docker network `nesi_net` (172.30.0.0/24). The `ufw` rule and the `nesi_scraper` MySQL user are scoped to that subnet; port 3306 stays closed to the internet.

## Email reports and re-runs

At 00:00, 09:00, 12:00, 15:00, 18:00 and 21:00 WAT (after that hour's scrape) a report is emailed via Resend: missing hours, energy so far, peak hour, top GENCOs, latest DISCO allocation and scraper status. A report is also sent the first time a job starts failing, and after every re-run.

The **Re-run now** button in the email opens a signed link (valid 24 hours). Opening it only shows a Confirm button, so mail scanners that pre-open links trigger nothing; confirming re-runs the latest data and emails a fresh report.

## Re-run site: https://nesi-alert.raven-emrc.com

- **Sign in** with email + password; sessions last 30 days. Signing in never sends email. The only emails are a one-time set-password link: in the welcome email when someone is added (valid 7 days), or from "First time here, or forgot your password?" (valid 1 hour). After 5 wrong passwords an account is locked for 15 minutes (setting a new password unlocks it).
- **People with access**: owners come from `DASHBOARD_USERS` and can't be removed on the site. Anyone signed in can add or remove other people (who get a welcome email) and switch report emails on or off per person. Report emails go to `REPORT_TO` plus everyone with reports on.
- **Re-run latest**: GENCO for today + live DISCO, then a report email.
- **Re-run GENCO for past dates**: a day, a range or a whole month (up to 62 days). Runs one day at a time between the hourly scrapes; the requester gets an email when it finishes, listing any days that failed.
- **Recent re-runs**: queued / running (day N of M) / done / failed, auto-refreshing.

All re-runs go through one queue in the scheduler process (SQLite on the `nesi_data` volume), so they never overlap with each other or the hourly jobs. The site is served on `127.0.0.1:8101` and published by the host nginx ([deploy/nginx/](deploy/nginx/)). A latest re-run can also be started from GitHub (Actions → **Re-run scrapers**) or on the VPS with `docker compose exec scraper python -m nesi rerun`.

## Operating it (on the VPS)

```bash
sudo -iu nesi-deploy                                   # switch to the deploy user first
cd ~/nesi
docker compose ps                                      # status and health
docker compose logs -f --tail 100                      # live logs
docker compose run --rm scraper check-db               # DB connectivity + unique keys
docker compose run --rm scraper genco                  # run GENCO now
docker compose run --rm scraper disco                  # run DISCO now
docker compose run --rm scraper genco --from 2026-09-01 --to 2026-09-10   # backfill GENCO days
```

## One-time setup

### VPS
1. Network (the deploy also creates it if missing): `docker network create --subnet 172.30.0.0/24 nesi_net`
2. Firewall: `sudo ufw allow from 172.30.0.0/24 to any port 3306 proto tcp comment 'nesi-scraper'`
3. MySQL user, as admin (`sudo mysql --defaults-file=/etc/mysql/debian.cnf`):
   ```sql
   CREATE USER 'nesi_scraper'@'172.30.0.%' IDENTIFIED BY '<openssl rand -hex 20>';
   GRANT SELECT, INSERT, UPDATE, DELETE ON jksutauf_nesidb.combined_hourly_energy_generated_mwh TO 'nesi_scraper'@'172.30.0.%';
   GRANT SELECT, INSERT, UPDATE, DELETE ON jksutauf_nesidb.discoloadprofile TO 'nesi_scraper'@'172.30.0.%';
   ```
4. Migrations, as admin, in order: `migrations/001_*.sql`, `migrations/002_*.sql`.
5. A dedicated deploy user `nesi-deploy` (in the `docker` group) whose `authorized_keys` holds the GitHub Actions public key. The app lives in `/home/nesi-deploy/nesi`.

### GitHub (Settings → Environments → `production` → secrets)
| Secret | Value |
|---|---|
| `VPS_HOST` | VPS IP address |
| `VPS_USER` | `nesi-deploy` |
| `VPS_SSH_KEY` | Private key of the deploy key pair |
| `VPS_KNOWN_HOSTS` | The VPS's SSH host keys, as `<ip> <key>` lines |
| `DB_PASSWORD` | Password of the `nesi_scraper` MySQL user |
| `HEALTHCHECK_URL_GENCO` / `_DISCO` | Optional healthchecks.io ping URLs |
| `RESEND_API_KEY` | Resend API key (sending access) |
| `RERUN_SECRET` | `openssl rand -hex 32`; signs the re-run links |

Environment **variables** (same page, not secret):

| Variable | Value |
|---|---|
| `REPORT_FROM` | e.g. `NESI Reports <reports@raven-emrc.com>` (domain verified in Resend) |
| `REPORT_TO` | Comma-separated recipient emails |
| `PUBLIC_BASE_URL` | `https://nesi-alert.raven-emrc.com` |
| `DASHBOARD_USERS` | Owner email(s), comma-separated; they add everyone else on the site |
| `REPORT_TO` | Optional extra report recipients who don't need site access |

GitHub is the single source of truth for configuration: every deploy rewrites `~/nesi/.env` on the VPS (mode 600) from these secrets, so rotating a password is "update the secret, re-run the workflow". `.env.example` documents the variables for manual runs.

The deploy pulls from GHCR with the workflow's short-lived `GITHUB_TOKEN`, using a private `DOCKER_CONFIG` in `~/nesi/.docker`, so no registry credentials are stored on the VPS and other services' logins are untouched.

## Development

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
ruff check . && ruff format --check . && pytest
```
