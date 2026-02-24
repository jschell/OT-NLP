# Plan: Stage 0 — Foundation & Infrastructure

> **No dependencies. This is the first plan.**
> **Folder:** active

## Goal

Establish the complete project scaffold, Docker infrastructure, and PostgreSQL schema so
that every subsequent stage has a healthy, fully-initialized environment to build on.

## Architecture

Docker Compose orchestrates four services: a pgvector-enabled PostgreSQL instance (`db`),
a JupyterLab notebook server (`jupyter`), a Streamlit visualization app (`streamlit`), and
an on-demand pipeline runner (`pipeline`). All Python source lives under `pipeline/` on the
host and is volume-mounted at `/pipeline/` inside containers, keeping a single source of
truth. The database schema is defined once in `init_schema.sql` and applied idempotently via
`CREATE TABLE IF NOT EXISTS`; no stage ever alters the schema destructively.

## Tech Stack

- Python 3.11-slim Docker image for pipeline and Streamlit services
- `pgvector/pgvector:pg16` for PostgreSQL with vector-search extension
- `jupyter/base-notebook:python-3.11` for JupyterLab
- `uv` for host-side Python tooling (pytest, ruff, pyright)
- Typst v0.12.0 (pinned binary) for PDF generation in the pipeline image
- Pre-commit with ruff hooks for lint/format enforcement

## Acceptance Criteria

All of the following must be true before Stage 0 is considered complete:

- `docker compose ps` shows `db`, `jupyter`, and `streamlit` all running with no restart
  loops
- `\dt` in PostgreSQL returns exactly 11 tables: `books`, `verses`, `translations`,
  `word_tokens`, `verse_fingerprints`, `chiasm_candidates`, `syllable_tokens`,
  `breath_profiles`, `translation_scores`, `suggestions`, `pipeline_runs`
- `books` table contains exactly 6 seed rows (Genesis, Exodus, Psalms, Isaiah, Job,
  Lamentations)
- pgvector extension is active (`\dx` confirms version 0.7.x or later)
- JupyterLab UI reachable at http://localhost:8888
- Streamlit placeholder reachable at http://localhost:8501
- Pipeline container builds successfully under `--profile pipeline`
- No credentials appear in any committed file (all from environment variables or defaults)
- `validate_infrastructure.py` exits 0 with all checks passing

Verification commands:

```bash
# Container health
docker compose ps

# PostgreSQL connectivity
docker exec psalms_db pg_isready -U psalms -d psalms

# pgvector present
docker exec psalms_db psql -U psalms -d psalms -c "\dx pgvector"

# All 11 tables
docker exec psalms_db psql -U psalms -d psalms -c "\dt"

# 6 seed rows
docker exec psalms_db psql -U psalms -d psalms \
  -c "SELECT book_num, book_name FROM books ORDER BY book_num;"

# HTTP checks
curl -s -o /dev/null -w "%{http_code}" http://localhost:8888
curl -s -o /dev/null -w "%{http_code}" http://localhost:8501

# Pipeline image build
docker compose --profile pipeline build pipeline
```

---

## Tasks

### Task 1: Project Scaffold

**Files:**
- `/home/user/OT-NLP/pyproject.toml`
- `/home/user/OT-NLP/conftest.py`
- `/home/user/OT-NLP/.pre-commit-config.yaml`
- `/home/user/OT-NLP/.gitignore` (additions)

**Steps:**

1. Write test in `tests/test_scaffold.py`:

   ```python
   """Tests that the project scaffold is correctly configured."""

   import sys
   from pathlib import Path


   def test_pipeline_on_sys_path() -> None:
       """conftest.py must add pipeline/ to sys.path."""
       pipeline_dir = str(Path(__file__).parent.parent / "pipeline")
       assert pipeline_dir in sys.path, (
           f"pipeline/ not found in sys.path.\n"
           f"Expected: {pipeline_dir}\n"
           f"sys.path: {sys.path}"
       )


   def test_pyproject_toml_exists() -> None:
       """pyproject.toml must exist at repo root."""
       toml_path = Path(__file__).parent.parent / "pyproject.toml"
       assert toml_path.exists(), "pyproject.toml not found at repo root"


   def test_pyproject_has_pytest_section() -> None:
       """pyproject.toml must declare pytest as a dev dependency."""
       toml_path = Path(__file__).parent.parent / "pyproject.toml"
       content = toml_path.read_text()
       assert "pytest" in content, "pytest not found in pyproject.toml"


   def test_pre_commit_config_exists() -> None:
       """.pre-commit-config.yaml must exist at repo root."""
       config_path = Path(__file__).parent.parent / ".pre-commit-config.yaml"
       assert config_path.exists(), ".pre-commit-config.yaml not found at repo root"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_scaffold.py -v
   # Expected: FAILED — pyproject.toml and conftest.py do not exist yet
   ```

3. Create `pyproject.toml` at `/home/user/OT-NLP/pyproject.toml`:

   ```toml
   [project]
   name = "psalms-nlp"
   version = "0.1.0"
   description = "Biblical Hebrew NLP analysis pipeline"
   requires-python = ">=3.11"

   [tool.uv]
   dev-dependencies = [
     "pytest>=8.2",
     "ruff>=0.4",
     "pyright>=1.1",
     "psycopg2-binary>=2.9",
     "pre-commit>=3.7",
   ]

   [tool.pytest.ini_options]
   testpaths = ["tests"]
   python_files = ["test_*.py"]
   python_functions = ["test_*"]

   [tool.ruff]
   line-length = 88
   target-version = "py311"

   [tool.ruff.lint]
   select = ["E", "F", "I", "UP"]

   [tool.pyright]
   pythonVersion = "3.11"
   include = ["pipeline", "tests"]
   typeCheckingMode = "basic"
   ```

4. Create `conftest.py` at `/home/user/OT-NLP/conftest.py`:

   ```python
   """Root conftest.py — adds pipeline/ to sys.path for all tests."""

   import sys
   from pathlib import Path

   sys.path.insert(0, str(Path(__file__).parent / "pipeline"))
   ```

5. Create `.pre-commit-config.yaml` at `/home/user/OT-NLP/.pre-commit-config.yaml`:

   ```yaml
   repos:
     - repo: https://github.com/astral-sh/ruff-pre-commit
       rev: v0.4.4
       hooks:
         - id: ruff
           args: [--fix]
         - id: ruff-format
   ```

6. Add to `/home/user/OT-NLP/.gitignore` (create if absent):

   ```
   # Python
   __pycache__/
   *.pyc
   *.pyo
   .venv/
   .uv/

   # Environment / secrets
   .env

   # Data (large binaries — never commit)
   data/bhsa/
   data/translations/*.db
   data/translations/ult/
   data/translations/ust/
   data/outputs/

   # Docker
   .docker/

   # Editor
   .vscode/
   .idea/
   *.swp
   ```

7. Create `tests/` directory with empty `__init__.py`:

   ```bash
   mkdir -p /home/user/OT-NLP/tests
   touch /home/user/OT-NLP/tests/__init__.py
   ```

8. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_scaffold.py -v
   # Expected: 4 passed
   ```

9. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

10. Commit: `"chore: project scaffold — pyproject.toml, conftest, pre-commit, gitignore"`

---

### Task 2: Write validate_infrastructure.py

**Files:**
- `/home/user/OT-NLP/pipeline/validate_infrastructure.py`

**Steps:**

1. Write the validation script first — it acts as the "test" for Task 3 and 4. Running it
   before containers exist must fail with a clear error:

   ```python
   #!/usr/bin/env python3
   """
   Infrastructure validation script.

   Asserts:
     1. PostgreSQL container is reachable and healthy
     2. pgvector extension is active
     3. All 11 expected tables are present
     4. books table contains exactly 6 seed rows
     5. Cross-container connectivity (pipeline -> db)

   # TODO: environment check — add Docker daemon health check here
   #       (nice-to-have: subprocess.run(['docker', 'info']) and assert returncode 0)

   Usage:
       python pipeline/validate_infrastructure.py
   """

   from __future__ import annotations

   import os
   import sys
   import logging

   import psycopg2

   logging.basicConfig(
       level=logging.INFO,
       format="%(levelname)s  %(message)s",
   )
   logger = logging.getLogger(__name__)

   EXPECTED_TABLES = {
       "books",
       "verses",
       "translations",
       "word_tokens",
       "verse_fingerprints",
       "chiasm_candidates",
       "syllable_tokens",
       "breath_profiles",
       "translation_scores",
       "suggestions",
       "pipeline_runs",
   }

   EXPECTED_BOOKS = {
       (1, "Genesis"),
       (2, "Exodus"),
       (18, "Job"),
       (19, "Psalms"),
       (23, "Isaiah"),
       (25, "Lamentations"),
   }


   def get_connection() -> psycopg2.extensions.connection:
       """Open a psycopg2 connection using environment variables."""
       return psycopg2.connect(
           host=os.environ.get("POSTGRES_HOST", "localhost"),
           dbname=os.environ.get("POSTGRES_DB", "psalms"),
           user=os.environ.get("POSTGRES_USER", "psalms"),
           password=os.environ.get("POSTGRES_PASSWORD", "psalms_dev"),
           connect_timeout=5,
       )


   def check_connection(conn: psycopg2.extensions.connection) -> None:
       """Assert the database connection is alive."""
       with conn.cursor() as cur:
           cur.execute("SELECT 1")
           row = cur.fetchone()
       assert row == (1,), f"Unexpected result from SELECT 1: {row}"
       logger.info("CHECK  database connection: OK")


   def check_pgvector(conn: psycopg2.extensions.connection) -> None:
       """Assert pgvector extension is installed."""
       with conn.cursor() as cur:
           cur.execute(
               "SELECT extname FROM pg_extension WHERE extname = 'vector'"
           )
           row = cur.fetchone()
       assert row is not None, (
           "pgvector extension not found. "
           "Run: CREATE EXTENSION IF NOT EXISTS pgvector;"
       )
       logger.info("CHECK  pgvector extension: OK")


   def check_tables(conn: psycopg2.extensions.connection) -> None:
       """Assert all expected tables exist in the public schema."""
       with conn.cursor() as cur:
           cur.execute(
               """
               SELECT tablename
               FROM pg_tables
               WHERE schemaname = 'public'
               ORDER BY tablename
               """
           )
           found = {row[0] for row in cur.fetchall()}

       missing = EXPECTED_TABLES - found
       assert not missing, (
           f"Missing tables: {sorted(missing)}\n"
           f"Found tables:   {sorted(found)}"
       )
       logger.info(f"CHECK  all {len(EXPECTED_TABLES)} tables present: OK")


   def check_books_seed(conn: psycopg2.extensions.connection) -> None:
       """Assert books table contains exactly the 6 expected seed rows."""
       with conn.cursor() as cur:
           cur.execute("SELECT book_num, book_name FROM books ORDER BY book_num")
           rows = {(r[0], r[1]) for r in cur.fetchall()}

       assert rows == EXPECTED_BOOKS, (
           f"books table mismatch.\n"
           f"Expected: {sorted(EXPECTED_BOOKS)}\n"
           f"Got:      {sorted(rows)}"
       )
       logger.info(f"CHECK  books seed data ({len(rows)} rows): OK")


   def check_indices(conn: psycopg2.extensions.connection) -> None:
       """Assert the 9 expected indices exist."""
       expected_indices = {
           "idx_verses_book_chapter",
           "idx_translations_verse",
           "idx_translations_key",
           "idx_word_tokens_verse",
           "idx_syllable_tokens_verse",
           "idx_syllable_tokens_token",
           "idx_translation_scores_verse",
           "idx_translation_scores_key",
           "idx_chiasm_candidates_start",
       }
       with conn.cursor() as cur:
           cur.execute(
               """
               SELECT indexname
               FROM pg_indexes
               WHERE schemaname = 'public'
               """
           )
           found = {row[0] for row in cur.fetchall()}

       missing = expected_indices - found
       assert not missing, (
           f"Missing indices: {sorted(missing)}\n"
           f"Found indices:   {sorted(found & expected_indices)}"
       )
       logger.info(f"CHECK  all {len(expected_indices)} indices present: OK")


   def main() -> int:
       """Run all checks. Returns 0 on success, 1 on failure."""
       failures: list[str] = []

       try:
           conn = get_connection()
       except psycopg2.OperationalError as exc:
           logger.error(
               "Cannot connect to PostgreSQL. "
               "Is the db container running?\n"
               f"  POSTGRES_HOST={os.environ.get('POSTGRES_HOST', 'localhost')}\n"
               f"  Error: {exc}"
           )
           return 1

       checks = [
           ("connection",    check_connection),
           ("pgvector",      check_pgvector),
           ("tables",        check_tables),
           ("books seed",    check_books_seed),
           ("indices",       check_indices),
       ]

       for name, fn in checks:
           try:
               fn(conn)
           except AssertionError as exc:
               logger.error(f"FAIL   {name}: {exc}")
               failures.append(name)

       conn.close()

       if failures:
           logger.error(f"\n{len(failures)} check(s) FAILED: {failures}")
           return 1

       logger.info("\nAll infrastructure checks PASSED.")
       return 0


   if __name__ == "__main__":
       sys.exit(main())
   ```

2. Run and confirm it fails without containers:

   ```bash
   python /home/user/OT-NLP/pipeline/validate_infrastructure.py
   # Expected: ERROR  Cannot connect to PostgreSQL. Is the db container running?
   # Exit code 1
   ```

3. This script is the acceptance gate for Tasks 3, 4, and 5. It will pass after containers
   are up and the schema is applied (Task 7).

4. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

5. Commit: `"feat: add validate_infrastructure.py — infrastructure acceptance gate"`

---

### Task 3: Docker Build Files

**Files:**
- `/home/user/OT-NLP/pipeline/Dockerfile.pipeline`
- `/home/user/OT-NLP/pipeline/requirements.txt`
- `/home/user/OT-NLP/streamlit/Dockerfile.streamlit`
- `/home/user/OT-NLP/streamlit/requirements_streamlit.txt`

**Steps:**

1. Write test in `tests/test_docker_files.py`:

   ```python
   """Tests that required Docker build artifacts exist and are well-formed."""

   from pathlib import Path


   REPO_ROOT = Path(__file__).parent.parent


   def test_pipeline_dockerfile_exists() -> None:
       """pipeline/Dockerfile.pipeline must exist."""
       assert (REPO_ROOT / "pipeline" / "Dockerfile.pipeline").exists()


   def test_pipeline_dockerfile_uses_python311() -> None:
       """Pipeline Dockerfile must use python:3.11-slim base image."""
       content = (REPO_ROOT / "pipeline" / "Dockerfile.pipeline").read_text()
       assert "python:3.11-slim" in content


   def test_pipeline_dockerfile_installs_typst() -> None:
       """Pipeline Dockerfile must install Typst v0.12.0."""
       content = (REPO_ROOT / "pipeline" / "Dockerfile.pipeline").read_text()
       assert "typst" in content.lower()
       assert "0.12.0" in content


   def test_pipeline_requirements_exists() -> None:
       """pipeline/requirements.txt must exist."""
       assert (REPO_ROOT / "pipeline" / "requirements.txt").exists()


   def test_pipeline_requirements_pins_psycopg2() -> None:
       """requirements.txt must pin psycopg2-binary."""
       content = (REPO_ROOT / "pipeline" / "requirements.txt").read_text()
       assert "psycopg2-binary" in content


   def test_pipeline_requirements_pins_text_fabric() -> None:
       """requirements.txt must include text-fabric for BHSA access."""
       content = (REPO_ROOT / "pipeline" / "requirements.txt").read_text()
       assert "text-fabric" in content


   def test_streamlit_dockerfile_exists() -> None:
       """streamlit/Dockerfile.streamlit must exist."""
       assert (REPO_ROOT / "streamlit" / "Dockerfile.streamlit").exists()


   def test_streamlit_requirements_exists() -> None:
       """streamlit/requirements_streamlit.txt must exist."""
       assert (REPO_ROOT / "streamlit" / "requirements_streamlit.txt").exists()


   def test_streamlit_requirements_pins_streamlit() -> None:
       """streamlit/requirements_streamlit.txt must pin streamlit."""
       content = (
           REPO_ROOT / "streamlit" / "requirements_streamlit.txt"
       ).read_text()
       assert "streamlit" in content
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_docker_files.py -v
   # Expected: FAILED — Dockerfile.pipeline and requirements.txt do not exist yet
   ```

3. Create `pipeline/Dockerfile.pipeline` at
   `/home/user/OT-NLP/pipeline/Dockerfile.pipeline`:

   ```dockerfile
   FROM python:3.11-slim

   # Typst — pinned single-binary PDF renderer
   ARG TYPST_VERSION=0.12.0
   RUN apt-get update && apt-get install -y --no-install-recommends \
         curl xz-utils pandoc \
       && curl -L \
          "https://github.com/typst/typst/releases/download/v${TYPST_VERSION}/typst-x86_64-unknown-linux-musl.tar.xz" \
          | tar -xJ --strip-components=1 -C /usr/local/bin \
                 "typst-x86_64-unknown-linux-musl/typst" \
       && apt-get clean && rm -rf /var/lib/apt/lists/*

   WORKDIR /pipeline

   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt

   # Default: run the orchestrator; override for one-shot stage execution
   CMD ["python", "run.py"]
   ```

4. Create `pipeline/requirements.txt` at
   `/home/user/OT-NLP/pipeline/requirements.txt`:

   ```text
   # ── Database ──────────────────────────────────────────────
   psycopg2-binary==2.9.9
   pgvector==0.2.5

   # ── Configuration ──────────────────────────────────────────
   pyyaml==6.0.1

   # ── Data / Hebrew NLP ──────────────────────────────────────
   text-fabric==12.0.0
   numpy==1.26.4
   pandas==2.2.2

   # ── Publication ────────────────────────────────────────────
   sphinx==8.1.3
   myst-nb==1.3.0
   sphinx-book-theme==1.1.3
   sphinxcontrib-bibtex==2.6.3
   nbconvert==7.16.4
   ipykernel==6.29.5

   # ── Visualization ──────────────────────────────────────────
   plotly==5.22.0
   kaleido==0.2.1

   # ── Streamlit ──────────────────────────────────────────────
   streamlit==1.35.0

   # ── Translation adapters ───────────────────────────────────
   usfm-grammar==2.3.0

   # ── Phoneme / NLP ──────────────────────────────────────────
   pronouncing==0.2.0

   # ── LLM adapters (all optional at runtime) ─────────────────
   anthropic==0.28.1
   openai==1.35.0
   google-generativeai==0.7.2
   requests==2.32.3
   ```

5. Create directory and `streamlit/Dockerfile.streamlit` at
   `/home/user/OT-NLP/streamlit/Dockerfile.streamlit`:

   ```dockerfile
   FROM python:3.11-slim

   WORKDIR /app

   COPY requirements_streamlit.txt .
   RUN pip install --no-cache-dir -r requirements_streamlit.txt

   EXPOSE 8501

   CMD ["streamlit", "run", "app.py", \
        "--server.port=8501", "--server.address=0.0.0.0"]
   ```

6. Create `streamlit/requirements_streamlit.txt` at
   `/home/user/OT-NLP/streamlit/requirements_streamlit.txt`:

   ```text
   streamlit==1.35.0
   plotly==5.22.0
   psycopg2-binary==2.9.9
   pandas==2.2.2
   numpy==1.26.4
   ```

7. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_docker_files.py -v
   # Expected: 9 passed
   ```

8. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

9. Commit: `"feat: Docker build files — pipeline and streamlit Dockerfiles + requirements"`

---

### Task 4: docker-compose.yml + .env.example

**Files:**
- `/home/user/OT-NLP/docker-compose.yml`
- `/home/user/OT-NLP/.env.example`

**Steps:**

1. Write test in `tests/test_compose.py`:

   ```python
   """Tests that docker-compose.yml is well-formed and contains required services."""

   from pathlib import Path

   import yaml


   REPO_ROOT = Path(__file__).parent.parent


   def _load_compose() -> dict:
       with open(REPO_ROOT / "docker-compose.yml") as f:
           return yaml.safe_load(f)


   def test_compose_file_exists() -> None:
       """docker-compose.yml must exist at repo root."""
       assert (REPO_ROOT / "docker-compose.yml").exists()


   def test_compose_has_db_service() -> None:
       """docker-compose.yml must define a 'db' service."""
       compose = _load_compose()
       assert "db" in compose["services"]


   def test_compose_db_uses_pgvector_image() -> None:
       """db service must use pgvector/pgvector:pg16 image."""
       compose = _load_compose()
       assert compose["services"]["db"]["image"] == "pgvector/pgvector:pg16"


   def test_compose_db_has_healthcheck() -> None:
       """db service must define a healthcheck."""
       compose = _load_compose()
       assert "healthcheck" in compose["services"]["db"]


   def test_compose_has_jupyter_service() -> None:
       """docker-compose.yml must define a 'jupyter' service."""
       compose = _load_compose()
       assert "jupyter" in compose["services"]


   def test_compose_has_streamlit_service() -> None:
       """docker-compose.yml must define a 'streamlit' service."""
       compose = _load_compose()
       assert "streamlit" in compose["services"]


   def test_compose_has_pipeline_service() -> None:
       """docker-compose.yml must define a 'pipeline' service."""
       compose = _load_compose()
       assert "pipeline" in compose["services"]


   def test_compose_pipeline_has_profile() -> None:
       """pipeline service must use the 'pipeline' profile so it doesn't start by default."""
       compose = _load_compose()
       profiles = compose["services"]["pipeline"].get("profiles", [])
       assert "pipeline" in profiles


   def test_compose_no_hardcoded_passwords() -> None:
       """docker-compose.yml must not contain hardcoded credential strings."""
       content = (REPO_ROOT / "docker-compose.yml").read_text()
       # Credentials must be referenced as ${VAR} env vars, not hardcoded
       assert "POSTGRES_PASSWORD:" not in content or "${POSTGRES_PASSWORD" in content


   def test_env_example_exists() -> None:
       """.env.example must exist."""
       assert (REPO_ROOT / ".env.example").exists()


   def test_env_example_has_required_vars() -> None:
       """.env.example must document all required environment variables."""
       content = (REPO_ROOT / ".env.example").read_text()
       for var in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "LLM_PROVIDER"]:
           assert var in content, f"{var} missing from .env.example"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_compose.py -v
   # Expected: FAILED — docker-compose.yml does not exist yet
   ```

3. Create `docker-compose.yml` at `/home/user/OT-NLP/docker-compose.yml`:

   ```yaml
   # ═══════════════════════════════════════════════════════════════
   # Psalms NLP Pipeline — Docker Compose
   # Host paths (Windows) appear ONLY in volume mount definitions.
   # All container paths use Linux convention (/pipeline, /data, etc.)
   # ═══════════════════════════════════════════════════════════════

   networks:
     psalms_net:
       driver: bridge

   volumes:
     pg_data:

   services:

     # PostgreSQL with pgvector extension
     db:
       image: pgvector/pgvector:pg16
       container_name: psalms_db
       restart: unless-stopped
       environment:
         POSTGRES_DB:       ${POSTGRES_DB:-psalms}
         POSTGRES_USER:     ${POSTGRES_USER:-psalms}
         POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
       volumes:
         - pg_data:/var/lib/postgresql/data
         - ./pipeline:/pipeline:ro
       ports:
         - "5432:5432"
       networks:
         - psalms_net
       healthcheck:
         test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-psalms} -d ${POSTGRES_DB:-psalms}"]
         interval: 10s
         timeout: 5s
         retries: 5

     # JupyterLab — analysis and notebook development
     jupyter:
       image: jupyter/base-notebook:python-3.11
       container_name: psalms_jupyter
       restart: unless-stopped
       environment:
         JUPYTER_ENABLE_LAB: "yes"
         JUPYTER_TOKEN:     ${JUPYTER_TOKEN:-psalms_dev}
         POSTGRES_HOST:     db
         POSTGRES_DB:       ${POSTGRES_DB:-psalms}
         POSTGRES_USER:     ${POSTGRES_USER:-psalms}
         POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
       volumes:
         - ./notebooks:/home/jovyan/work
         - ./data:/home/jovyan/data
         - ./pipeline:/pipeline:ro
       ports:
         - "8888:8888"
       networks:
         - psalms_net
       depends_on:
         db:
           condition: service_healthy

     # Streamlit — interactive visualization explorer
     streamlit:
       build:
         context: ./streamlit
         dockerfile: Dockerfile.streamlit
       container_name: psalms_streamlit
       restart: unless-stopped
       profiles:
         - ui
       environment:
         POSTGRES_HOST:     db
         POSTGRES_DB:       ${POSTGRES_DB:-psalms}
         POSTGRES_USER:     ${POSTGRES_USER:-psalms}
         POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
       volumes:
         - ./streamlit:/app
         - ./pipeline:/pipeline:ro
       ports:
         - "8501:8501"
       networks:
         - psalms_net
       depends_on:
         db:
           condition: service_healthy

     # Pipeline runner — on-demand via: docker compose --profile pipeline run
     pipeline:
       build:
         context: ./pipeline
         dockerfile: Dockerfile.pipeline
       container_name: psalms_pipeline
       profiles:
         - pipeline
       environment:
         POSTGRES_HOST:     db
         POSTGRES_DB:       ${POSTGRES_DB:-psalms}
         POSTGRES_USER:     ${POSTGRES_USER:-psalms}
         POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
         LLM_PROVIDER:      ${LLM_PROVIDER:-none}
         LLM_API_KEY:       ${LLM_API_KEY:-}
         LLM_MODEL:         ${LLM_MODEL:-}
         OLLAMA_HOST:       ${OLLAMA_HOST:-}
       volumes:
         - ./pipeline:/pipeline
         - ./data:/data
       networks:
         - psalms_net
       depends_on:
         db:
           condition: service_healthy

     # Ollama — optional local LLM (uncomment profile: llm to enable)
     # ollama:
     #   image: ollama/ollama:latest
     #   container_name: psalms_ollama
     #   profiles:
     #     - llm
     #   volumes:
     #     - ollama_data:/root/.ollama
     #   ports:
     #     - "11434:11434"
     #   networks:
     #     - psalms_net
   ```

4. Create `.env.example` at `/home/user/OT-NLP/.env.example`:

   ```bash
   # Copy this file to .env and fill in real values.
   # .env is git-ignored and must never be committed.

   # PostgreSQL credentials
   POSTGRES_USER=psalms
   POSTGRES_PASSWORD=change_me_in_production
   POSTGRES_DB=psalms

   # JupyterLab access token
   JUPYTER_TOKEN=change_me_in_production

   # LLM provider: none | anthropic | openai | gemini | openrouter | ollama
   LLM_PROVIDER=none

   # API keys (leave blank if LLM_PROVIDER=none)
   LLM_API_KEY=
   LLM_MODEL=

   # Anthropic (if LLM_PROVIDER=anthropic)
   ANTHROPIC_API_KEY=

   # OpenAI (if LLM_PROVIDER=openai)
   OPENAI_API_KEY=

   # Google (if LLM_PROVIDER=gemini)
   GOOGLE_API_KEY=

   # ESV Bible API (optional — for API adapter in Stage 1)
   ESV_API_KEY=

   # Ollama host (if LLM_PROVIDER=ollama, typically http://host.docker.internal:11434)
   OLLAMA_HOST=
   ```

5. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_compose.py -v
   # Expected: 11 passed
   ```

6. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

7. Commit: `"feat: docker-compose.yml and .env.example — service topology defined"`

---

### Task 5: init_schema.sql

**Files:**
- `/home/user/OT-NLP/pipeline/init_schema.sql`

**Steps:**

1. Write test in `tests/test_schema_sql.py`:

   ```python
   """Tests that init_schema.sql is well-formed and defines all required objects."""

   import re
   from pathlib import Path


   REPO_ROOT = Path(__file__).parent.parent
   SQL_PATH = REPO_ROOT / "pipeline" / "init_schema.sql"


   def _sql() -> str:
       return SQL_PATH.read_text()


   def test_schema_sql_exists() -> None:
       """init_schema.sql must exist in pipeline/."""
       assert SQL_PATH.exists()


   def test_schema_sql_uses_if_not_exists() -> None:
       """Every CREATE TABLE must use IF NOT EXISTS for idempotency."""
       sql = _sql()
       tables = re.findall(r"CREATE TABLE\s+(\w+)", sql, re.IGNORECASE)
       safe_tables = re.findall(
           r"CREATE TABLE IF NOT EXISTS\s+(\w+)", sql, re.IGNORECASE
       )
       assert sorted(tables) == sorted(safe_tables), (
           f"These tables lack IF NOT EXISTS: "
           f"{set(tables) - set(safe_tables)}"
       )


   def test_schema_sql_defines_all_11_tables() -> None:
       """init_schema.sql must define all 11 required tables."""
       required = {
           "books", "verses", "translations", "word_tokens",
           "verse_fingerprints", "chiasm_candidates", "syllable_tokens",
           "breath_profiles", "translation_scores", "suggestions",
           "pipeline_runs",
       }
       sql = _sql()
       found = {
           m.lower()
           for m in re.findall(
               r"CREATE TABLE IF NOT EXISTS\s+(\w+)", sql, re.IGNORECASE
           )
       }
       missing = required - found
       assert not missing, f"Missing table definitions: {missing}"


   def test_schema_sql_defines_pgvector_extension() -> None:
       """init_schema.sql must enable pgvector extension."""
       assert "pgvector" in _sql().lower()


   def test_schema_sql_defines_9_indices() -> None:
       """init_schema.sql must define all 9 expected indices."""
       required_indices = {
           "idx_verses_book_chapter",
           "idx_translations_verse",
           "idx_translations_key",
           "idx_word_tokens_verse",
           "idx_syllable_tokens_verse",
           "idx_syllable_tokens_token",
           "idx_translation_scores_verse",
           "idx_translation_scores_key",
           "idx_chiasm_candidates_start",
       }
       sql = _sql()
       found = {
           m.lower()
           for m in re.findall(r"CREATE INDEX IF NOT EXISTS\s+(\w+)", sql, re.IGNORECASE)
       }
       missing = required_indices - found
       assert not missing, f"Missing index definitions: {missing}"


   def test_schema_sql_seeds_6_books() -> None:
       """init_schema.sql must INSERT the 6 expected books."""
       sql = _sql()
       # Each book has a distinct book_num in the INSERT block
       for book_num in [1, 2, 18, 19, 23, 25]:
           assert f"({book_num}," in sql or f"({book_num} " in sql, (
               f"Book number {book_num} not found in INSERT INTO books block"
           )


   def test_schema_sql_has_on_conflict_for_books() -> None:
       """Books INSERT must be idempotent via ON CONFLICT DO NOTHING."""
       assert "ON CONFLICT" in _sql().upper()
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_schema_sql.py -v
   # Expected: FAILED — init_schema.sql does not exist yet
   ```

3. Create `pipeline/init_schema.sql` at `/home/user/OT-NLP/pipeline/init_schema.sql`:

   ```sql
   -- ═══════════════════════════════════════════════════════════════
   -- Psalms NLP Pipeline — Complete Database Schema
   -- Run once via:
   --   docker exec psalms_db psql -U psalms -d psalms \
   --     -f /pipeline/init_schema.sql
   -- All CREATE TABLE statements use IF NOT EXISTS — fully idempotent.
   -- ═══════════════════════════════════════════════════════════════

   -- Extensions
   CREATE EXTENSION IF NOT EXISTS vector;       -- pgvector
   CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- text search trigrams

   -- ───────────────────────────────────────────────────────────────
   -- Core reference: books in the corpus
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS books (
       book_num   INTEGER PRIMARY KEY,
       book_name  TEXT    NOT NULL,
       testament  TEXT    NOT NULL CHECK (testament IN ('OT', 'NT')),
       genre      TEXT,
       language   TEXT    NOT NULL DEFAULT 'hebrew'
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 1 / 2: Source verses and translation text
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS verses (
       verse_id    SERIAL  PRIMARY KEY,
       book_num    INTEGER NOT NULL REFERENCES books(book_num),
       chapter     INTEGER NOT NULL,
       verse_num   INTEGER NOT NULL,
       hebrew_text TEXT    NOT NULL,
       word_count  INTEGER DEFAULT 0,
       colon_count INTEGER DEFAULT 0,
       is_aramaic  BOOLEAN NOT NULL DEFAULT FALSE,
       is_acrostic BOOLEAN NOT NULL DEFAULT FALSE,
       UNIQUE (book_num, chapter, verse_num)
   );

   CREATE TABLE IF NOT EXISTS translations (
       translation_id SERIAL  PRIMARY KEY,
       verse_id       INTEGER NOT NULL REFERENCES verses(verse_id),
       translation_key TEXT   NOT NULL,
       verse_text      TEXT   NOT NULL,
       word_count      INTEGER DEFAULT 0,
       UNIQUE (verse_id, translation_key)
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 2: Morphological tokens and style fingerprints
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS word_tokens (
       token_id       SERIAL  PRIMARY KEY,
       verse_id       INTEGER NOT NULL REFERENCES verses(verse_id),
       position       INTEGER NOT NULL,
       surface_form   TEXT    NOT NULL,
       lexeme         TEXT,
       part_of_speech TEXT,
       morpheme_count INTEGER,
       is_verb        BOOLEAN,
       is_noun        BOOLEAN,
       stem           TEXT,
       colon_index    INTEGER DEFAULT 0,
       UNIQUE (verse_id, position)
   );

   CREATE TABLE IF NOT EXISTS verse_fingerprints (
       fingerprint_id     SERIAL   PRIMARY KEY,
       verse_id           INTEGER  NOT NULL UNIQUE REFERENCES verses(verse_id),
       syllable_density   FLOAT,
       morpheme_ratio     FLOAT,
       sonority_score     FLOAT,
       clause_compression FLOAT,
       colon_fingerprints JSONB,
       computed_at        TIMESTAMP DEFAULT NOW()
   );

   CREATE TABLE IF NOT EXISTS chiasm_candidates (
       chiasm_id      SERIAL  PRIMARY KEY,
       verse_id_start INTEGER NOT NULL REFERENCES verses(verse_id),
       verse_id_end   INTEGER NOT NULL REFERENCES verses(verse_id),
       pattern_type   TEXT    NOT NULL
                      CHECK (pattern_type IN ('ABBA', 'ABCBA', 'AB')),
       colon_matches  JSONB   NOT NULL,
       confidence     FLOAT   NOT NULL,
       is_reviewed    BOOLEAN NOT NULL DEFAULT FALSE,
       reviewer_note  TEXT,
       computed_at    TIMESTAMP DEFAULT NOW()
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 3: Syllable tokens and breath profiles
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS syllable_tokens (
       syllable_id     SERIAL  PRIMARY KEY,
       token_id        INTEGER NOT NULL REFERENCES word_tokens(token_id),
       verse_id        INTEGER NOT NULL REFERENCES verses(verse_id),
       syllable_index  INTEGER NOT NULL,
       syllable_text   TEXT    NOT NULL,
       nucleus_vowel   TEXT,
       vowel_openness  FLOAT,
       vowel_length    TEXT CHECK (vowel_length IN ('full', 'half', 'none')),
       is_open         BOOLEAN,
       onset_class     TEXT CHECK (
           onset_class IN ('guttural', 'sibilant', 'liquid', 'nasal', 'stop', 'none')
       ),
       breath_weight   FLOAT,
       stress_position BOOLEAN DEFAULT FALSE,
       colon_index     INTEGER DEFAULT 0,
       UNIQUE (token_id, syllable_index)
   );

   CREATE TABLE IF NOT EXISTS breath_profiles (
       profile_id       SERIAL  PRIMARY KEY,
       verse_id         INTEGER NOT NULL UNIQUE REFERENCES verses(verse_id),
       mean_weight      FLOAT,
       open_ratio       FLOAT,
       guttural_density FLOAT,
       colon_count      INTEGER,
       colon_boundaries INTEGER[],
       stress_positions NUMERIC[],
       breath_curve     NUMERIC[],
       computed_at      TIMESTAMP DEFAULT NOW()
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 4: Translation scoring
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS translation_scores (
       score_id              SERIAL  PRIMARY KEY,
       verse_id              INTEGER NOT NULL REFERENCES verses(verse_id),
       translation_key       TEXT    NOT NULL,
       density_deviation     FLOAT,
       morpheme_deviation    FLOAT,
       sonority_deviation    FLOAT,
       compression_deviation FLOAT,
       composite_deviation   FLOAT,
       stress_alignment      FLOAT,
       weight_match          FLOAT,
       breath_alignment      FLOAT,
       scored_at             TIMESTAMP DEFAULT NOW(),
       UNIQUE (verse_id, translation_key)
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 5: LLM suggestions
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS suggestions (
       suggestion_id       SERIAL  PRIMARY KEY,
       verse_id            INTEGER NOT NULL REFERENCES verses(verse_id),
       translation_key     TEXT    NOT NULL,
       suggested_text      TEXT    NOT NULL,
       composite_deviation FLOAT,
       breath_alignment    FLOAT,
       improvement_delta   FLOAT,
       llm_provider        TEXT    NOT NULL,
       llm_model           TEXT    NOT NULL,
       prompt_version      TEXT    NOT NULL,
       generated_at        TIMESTAMP DEFAULT NOW()
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 7: Pipeline run log
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS pipeline_runs (
       run_id        SERIAL PRIMARY KEY,
       started_at    TIMESTAMP NOT NULL DEFAULT NOW(),
       finished_at   TIMESTAMP,
       status        TEXT NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running', 'ok', 'error')),
       stages_run    TEXT[],
       error_message TEXT,
       row_counts    JSONB
   );

   -- ───────────────────────────────────────────────────────────────
   -- Indices
   -- ───────────────────────────────────────────────────────────────

   CREATE INDEX IF NOT EXISTS idx_verses_book_chapter
       ON verses(book_num, chapter);

   CREATE INDEX IF NOT EXISTS idx_translations_verse
       ON translations(verse_id);

   CREATE INDEX IF NOT EXISTS idx_translations_key
       ON translations(translation_key);

   CREATE INDEX IF NOT EXISTS idx_word_tokens_verse
       ON word_tokens(verse_id);

   CREATE INDEX IF NOT EXISTS idx_syllable_tokens_verse
       ON syllable_tokens(verse_id);

   CREATE INDEX IF NOT EXISTS idx_syllable_tokens_token
       ON syllable_tokens(token_id);

   CREATE INDEX IF NOT EXISTS idx_translation_scores_verse
       ON translation_scores(verse_id);

   CREATE INDEX IF NOT EXISTS idx_translation_scores_key
       ON translation_scores(translation_key);

   CREATE INDEX IF NOT EXISTS idx_chiasm_candidates_start
       ON chiasm_candidates(verse_id_start);

   -- ───────────────────────────────────────────────────────────────
   -- Seed data: corpus books
   -- ───────────────────────────────────────────────────────────────

   INSERT INTO books (book_num, book_name, testament, language) VALUES
     (1,  'Genesis',      'OT', 'hebrew'),
     (2,  'Exodus',       'OT', 'hebrew'),
     (18, 'Job',          'OT', 'hebrew'),
     (19, 'Psalms',       'OT', 'hebrew'),
     (23, 'Isaiah',       'OT', 'hebrew'),
     (25, 'Lamentations', 'OT', 'hebrew')
   ON CONFLICT (book_num) DO NOTHING;
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_schema_sql.py -v
   # Expected: 7 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat: init_schema.sql — complete 11-table schema with indices and seed data"`

---

### Task 6: config.yml Skeleton + Streamlit Placeholder

**Files:**
- `/home/user/OT-NLP/pipeline/config.yml`
- `/home/user/OT-NLP/streamlit/app.py`

**Steps:**

1. Write test in `tests/test_config.py`:

   ```python
   """Tests that config.yml is present and structurally valid."""

   from pathlib import Path

   import yaml


   REPO_ROOT = Path(__file__).parent.parent
   CONFIG_PATH = REPO_ROOT / "pipeline" / "config.yml"


   def _cfg() -> dict:
       with open(CONFIG_PATH) as f:
           return yaml.safe_load(f)


   def test_config_yml_exists() -> None:
       """pipeline/config.yml must exist."""
       assert CONFIG_PATH.exists()


   def test_config_has_pipeline_section() -> None:
       """config.yml must have a 'pipeline' top-level key."""
       assert "pipeline" in _cfg()


   def test_config_has_translations_section() -> None:
       """config.yml must have a 'translations' section with sources."""
       cfg = _cfg()
       assert "translations" in cfg
       assert "sources" in cfg["translations"]


   def test_config_translations_include_kjv() -> None:
       """config.yml translations sources must include KJV."""
       cfg = _cfg()
       ids = [s["id"] for s in cfg["translations"]["sources"]]
       assert "KJV" in ids, f"KJV not found in translation sources: {ids}"


   def test_config_has_bhsa_section() -> None:
       """config.yml must define bhsa.data_path."""
       cfg = _cfg()
       assert "bhsa" in cfg
       assert "data_path" in cfg["bhsa"]


   def test_config_has_corpus_section() -> None:
       """config.yml must define corpus.books with at least Psalms (19)."""
       cfg = _cfg()
       assert "corpus" in cfg
       book_nums = [b["book_num"] for b in cfg["corpus"]["books"]]
       assert 19 in book_nums, f"Psalms (19) not in corpus books: {book_nums}"


   def test_config_has_llm_section() -> None:
       """config.yml must have an 'llm' section."""
       assert "llm" in _cfg()


   def test_config_llm_default_provider_is_none() -> None:
       """Default LLM provider must be 'none' for offline operation."""
       assert _cfg()["llm"]["provider"] == "none"


   def test_streamlit_app_exists() -> None:
       """streamlit/app.py must exist as a placeholder."""
       assert (REPO_ROOT / "streamlit" / "app.py").exists()


   def test_streamlit_app_has_title() -> None:
       """streamlit/app.py must call st.title."""
       content = (REPO_ROOT / "streamlit" / "app.py").read_text()
       assert "st.title" in content
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_config.py -v
   # Expected: FAILED — config.yml does not exist yet
   ```

3. Create `pipeline/config.yml` at `/home/user/OT-NLP/pipeline/config.yml`:

   ```yaml
   # ═══════════════════════════════════════════════════════════════
   # Psalms NLP Pipeline — Master Configuration
   # All paths are container-internal (Linux).
   # Host paths appear ONLY in docker-compose.yml volume mounts.
   # ═══════════════════════════════════════════════════════════════

   pipeline:
     name: psalms_nlp
     version: "0.1.0"
     stages:
       - ingest
       - fingerprint
       - breath
       - chiasm
       - score
       - suggest
       - export
     on_error: stop   # stop | warn_continue

   corpus:
     books:
       - book_num: 19
         name: Psalms
     debug_chapters: []   # e.g. [23] to restrict to Psalm 23

   bhsa:
     data_path: /data/bhsa

   translations:
     sources:
       - id:     KJV
         format: sqlite_scrollmapper
         path:   /data/translations/t_kjv.db

       - id:     YLT
         format: sqlite_scrollmapper
         path:   /data/translations/t_ylt.db

       - id:     WEB
         format: sqlite_scrollmapper
         path:   /data/translations/t_web.db

       - id:     ULT
         format: usfm
         path:   /data/translations/ult/
         book_map: usfm_standard

       - id:     UST
         format: usfm
         path:   /data/translations/ust/
         book_map: usfm_standard

   fingerprint:
     batch_size: 100
     conflict_mode: skip   # skip | upsert | rebuild

   breath:
     batch_size: 100
     conflict_mode: skip

   chiasm:
     similarity_threshold: 0.75
     min_confidence: 0.65
     max_stanza_verses: 8

   scoring:
     batch_size: 100
     conflict_mode: skip
     deviation_weights:
       density:     0.35
       morpheme:    0.25
       sonority:    0.20
       compression: 0.20
     breath_alignment_weights:
       stress: 0.60
       weight: 0.40

   llm:
     provider: none   # none | anthropic | openai | gemini | openrouter | ollama
     model: ""
     max_tokens: 256
     temperature: 0.3
     suggestion_filter:
       min_composite_deviation: 0.15
       max_suggestions_per_verse: 3

   export:
     output_dir: /data/outputs
     report_dir: /data/outputs/report
     pdf_path:   /data/outputs/report.pdf
     typst_version: "0.12.0"

   genre:
     # Genre tags used for Stage 8 expansion; populated per book
     psalms:     lyric
     isaiah:     prophetic
     job:        wisdom
     lamentations: lyric
   ```

4. Create `streamlit/app.py` at `/home/user/OT-NLP/streamlit/app.py`:

   ```python
   """Streamlit placeholder — full implementation in Stage 6."""

   import streamlit as st

   st.set_page_config(page_title="Psalms NLP Explorer", layout="wide")
   st.title("Psalms NLP Explorer")
   st.info("Pipeline not yet run. Return after Stage 4 completes.")
   ```

5. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_config.py -v
   # Expected: 10 passed
   ```

6. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

7. Commit: `"feat: config.yml skeleton + streamlit placeholder"`

---

### Task 7: Bring Up Containers and Run Validation (Integration Step)

**Note:** This task is not a pytest test — it is the integration verification step that
confirms the Docker stack and schema are correctly configured. Run these commands from the
repo root on the host machine.

**Steps:**

1. Create required host directories (if not already present):

   ```bash
   mkdir -p /home/user/OT-NLP/data/bhsa
   mkdir -p /home/user/OT-NLP/data/translations
   mkdir -p /home/user/OT-NLP/data/outputs/report
   mkdir -p /home/user/OT-NLP/notebooks
   mkdir -p /home/user/OT-NLP/pipeline/modules
   mkdir -p /home/user/OT-NLP/pipeline/adapters
   mkdir -p /home/user/OT-NLP/pipeline/visualize
   ```

2. Start core services:

   ```bash
   cd /home/user/OT-NLP
   docker compose up -d
   ```

   Expected: `psalms_db` and `psalms_jupyter` start; `psalms_streamlit` starts only if
   `--profile ui` is passed (it now has `profiles: [ui]`).

3. Wait for db to be healthy, then apply the schema:

   ```bash
   docker compose exec db \
     psql -U psalms -d psalms -f /pipeline/init_schema.sql
   ```

   Expected final lines:
   ```
   CREATE INDEX
   INSERT 0 6
   ```

4. Verify containers:

   ```bash
   docker compose ps
   # Expected: db (healthy), jupyter (running)

   docker exec psalms_db pg_isready -U psalms -d psalms
   # Expected: /var/run/postgresql:5432 - accepting connections

   docker exec psalms_db psql -U psalms -d psalms -c "\dt"
   # Expected: 11 rows (one per table)

   docker exec psalms_db psql -U psalms -d psalms \
     -c "SELECT book_num, book_name FROM books ORDER BY book_num;"
   # Expected: 6 rows
   ```

5. Run the pipeline container build to confirm it succeeds:

   ```bash
   docker compose --profile pipeline build pipeline
   # Expected: exits 0
   ```

6. Run `validate_infrastructure.py` inside the pipeline container:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python /pipeline/validate_infrastructure.py
   # Expected:
   # INFO  CHECK  database connection: OK
   # INFO  CHECK  pgvector extension: OK
   # INFO  CHECK  all 11 tables present: OK
   # INFO  CHECK  books seed data (6 rows): OK
   # INFO  CHECK  all 9 indices present: OK
   # INFO
   # INFO  All infrastructure checks PASSED.
   # Exit code: 0
   ```

7. Run the full pytest suite from the host to confirm all unit tests still pass:

   ```bash
   uv run --frozen pytest -v
   # Expected: all tests pass
   ```

8. Commit: `"feat: Stage 0 complete — Docker stack healthy, schema initialized, validation passing"`

---

## Resumability Verification

Running `init_schema.sql` a second time must be idempotent — no errors, no duplicate rows:

```bash
# Run schema again
docker compose exec db \
  psql -U psalms -d psalms -f /pipeline/init_schema.sql

# Confirm books still has exactly 6 rows
docker exec psalms_db psql -U psalms -d psalms \
  -c "SELECT COUNT(*) FROM books;"
# Expected: 6

# Confirm table count is still 11
docker exec psalms_db psql -U psalms -d psalms -c "\dt" | wc -l
```

All `CREATE TABLE IF NOT EXISTS` and `ON CONFLICT DO NOTHING` clauses guarantee this.
