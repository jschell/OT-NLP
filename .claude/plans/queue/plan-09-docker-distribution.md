# Plan 09 — Docker Distribution & New-User Onboarding

## Goal

Make the project cleanly distributable so a new user can go from `git clone` to a running
Streamlit explorer in under 15 minutes with no manual SQL or PowerShell required.

## Context

The pipeline is complete (Stages 0–7). The current distribution has three blocking friction
points for new users:
1. Schema must be applied manually via `docker exec … psql`
2. ULT/UST translation directories are empty; download is Windows-only PowerShell
3. No cross-platform pre-flight checker (check-env.ps1 is Windows-only)

## Tasks

### Task 1 — Auto-initialize schema via compose init container

Add a `db-init` service to `docker-compose.yml` that applies `init_schema.sql` (idempotent
due to `CREATE TABLE IF NOT EXISTS`) before the pipeline can run:

```yaml
db-init:
  image: postgres:16
  depends_on:
    db:
      condition: service_healthy
  command: >
    psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db/${POSTGRES_DB}
    -f /pipeline/init_schema.sql
  volumes:
    - ./pipeline:/pipeline:ro
  networks:
    - psalms_net
  profiles: [pipeline]
  restart: "no"
```

Update `psalms_pipeline` service `depends_on` to include `db-init`.

Force credential validation in compose (replace insecure defaults):
```yaml
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env}
  JUPYTER_TOKEN: ${JUPYTER_TOKEN:?Set JUPYTER_TOKEN in .env}
```

### Task 2 — Write `scripts/download_data.sh` (cross-platform Bash)

Port the logic from `scripts/download_data.ps1` to Bash. The script should:
1. Create `data/translations/` if missing
2. Download KJV, YLT, NHEB SQLite files from scrollmapper GitHub releases
3. Clone or update ULT and UST from git.door43.org into `data/translations/ult/` and `ust/`
4. Print checksum for each downloaded file
5. Exit non-zero on any download failure

```bash
#!/usr/bin/env bash
set -euo pipefail
# Usage: bash scripts/download_data.sh
```

### Task 3 — Write `scripts/check-env.sh` (cross-platform pre-flight)

Bash equivalent of `check-env.ps1`. Check:
- Docker daemon is running (`docker info`)
- Docker Compose v2 is available (`docker compose version`)
- `.env` file exists and contains required vars (POSTGRES_PASSWORD, JUPYTER_TOKEN)
- `data/translations/` contains at least KJV.db
- Ports 5432, 8888, 8501 are not already in use
- Print pass/fail for each check; exit 0 only if all pass

### Task 4 — Update README Quick Start

Replace the current 6-step quick start with the improved 8-step flow:

```
1. git clone + cd
2. cp .env.example .env  →  edit POSTGRES_PASSWORD and JUPYTER_TOKEN
3. bash scripts/check-env.sh       ← pre-flight (new)
4. bash scripts/download_data.sh   ← fetch all 5 translations (new)
5. docker compose up -d db
6. docker compose --profile pipeline run --rm db-init   ← schema init (new)
7. docker compose up -d            ← start Streamlit + JupyterLab
8. docker compose --profile pipeline run --rm pipeline python run.py
   (first run: downloads ~200 MB BHSA corpus automatically, 5–10 min)
```

Add sections:
- **First-run note:** BHSA auto-downloads ~200 MB via text-fabric on Stage 1; resumable if interrupted
- **Verification:** SQL queries to confirm row counts after pipeline completes
- **Troubleshooting:** Three most common failure modes with fixes

### Task 5 — Update `.env.example` with security guidance

Add inline comments clarifying which vars are required vs optional:
```bash
# REQUIRED — must be set before running docker compose
POSTGRES_PASSWORD=          # choose a strong password
JUPYTER_TOKEN=              # choose a strong token

# OPTIONAL — pipeline runs fully offline without any of these
LLM_PROVIDER=none
...
```

## Acceptance Criteria

- Fresh `git clone` → `check-env.sh` → `download_data.sh` → compose up → pipeline run
  completes without any manual SQL or PowerShell
- `check-env.sh` exits 0 on a clean Mac/Linux system with Docker installed
- `download_data.sh` populates all 5 translation sources (KJV, YLT, NHEB, ULT, UST)
- `docker compose run --rm db-init` applies schema idempotently (safe to run twice)
- Compose fails loudly if `.env` is missing POSTGRES_PASSWORD or JUPYTER_TOKEN
- README quick start accurately describes the new 8-step flow

## Files to Create/Modify

- `docker-compose.yml` — add `db-init` service; force credential env var validation
- `scripts/download_data.sh` (new) — cross-platform translation downloader
- `scripts/check-env.sh` (new) — Mac/Linux pre-flight checker
- `README.md` — expanded Quick Start + First-run note + Verification + Troubleshooting
- `.env.example` — add required/optional comments
