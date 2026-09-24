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

GitHub is the single source of truth for configuration: every deploy rewrites `~/nesi/.env` on the VPS (mode 600) from these secrets, so rotating a password is "update the secret, re-run the workflow". `.env.example` documents the variables for manual runs.

The deploy pulls from GHCR with the workflow's short-lived `GITHUB_TOKEN`, using a private `DOCKER_CONFIG` in `~/nesi/.docker`, so no registry credentials are stored on the VPS and other services' logins are untouched.

## Development

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
ruff check . && ruff format --check . && pytest
```
