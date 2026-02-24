# Plan: Stage 0 — Foundation & Infrastructure

> **Depends on:** No dependencies.
> **Status:** active

## Goal

Bootstrap the complete local Docker stack with PostgreSQL (pgvector), JupyterLab, Streamlit,
and the pipeline runner service; initialize the full database schema with all 11 tables,
9 indices, and 6 seed rows; and validate every service and the schema are healthy before
any data pipeline work begins.

## Acceptance Criteria

- `docker compose ps` shows `db` (healthy), `jupyter` (running) with no restart loops;
  `streamlit` runs under `--profile ui`, `pipeline` runs under `--profile pipeline`
- `\dt` in PostgreSQL returns exactly 11 tables: `books`, `verses`, `translations`,
  `word_tokens`, `verse_fingerprints`, `chiasm_candidates`, `syllable_tokens`,
  `breath_profiles`, `translation_scores`, `suggestions`, `pipeline_runs`
- pgvector extension is active (`\dx` confirms version 0.7.x or later)
- `books` table contains exactly 6 seed rows (Genesis, Exodus, Job, Psalms, Isaiah,
  Lamentations)
- JupyterLab UI reachable at http://localhost:8888
- Streamlit placeholder reachable at http://localhost:8501 (when started with `--profile ui`)
- Pipeline container builds successfully under `--profile pipeline` (exits 0)
- No credentials appear in any committed file (all sourced from environment variables or
  compile-time defaults)
- `pipeline/validate_infrastructure.py` exits 0 with all assertions passing when run
  inside the pipeline container

## Architecture

All services run under Docker Compose on a single bridge network (`psalms_net`). The
database service uses the official `pgvector/pgvector:pg16` image with a named volume for
data persistence. The pipeline service is built from a `python:3.11-slim` base image,
installs Python dependencies via `uv sync --frozen` from `pyproject.toml` + `uv.lock`, and downloads the Typst v0.12.0
binary at build time for Stage 6 PDF generation. All Python source is bind-mounted from
the host at `./pipeline:/pipeline` so edits are reflected without rebuilds. Credentials
are never hard-coded: `docker-compose.yml` uses `${VAR:-default}` syntax so the stack
runs without any `.env` file in local development.

## Tech Stack

- Docker Compose v2, bridge network `psalms_net`, named volume `pg_data`
- PostgreSQL 16 via `pgvector/pgvector:pg16` image (pgvector + pg_trgm extensions)
- Python 3.11-slim for the pipeline runner and Streamlit services
- Typst v0.12.0 (pinned single binary, downloaded at image build time)
- `jupyter/base-notebook:python-3.11` for JupyterLab
- `psycopg2-binary` for all Python-to-PostgreSQL connectivity
- `uv` on the host for test/lint/typecheck tooling (never `pip`)
- `pytest`, `ruff`, `pyright`, `pre-commit` as dev dependencies

---

<!-- TODO (nice-to-have): add a check_environment.sh script that verifies Docker Desktop
     is version >= 24.x, WSL2 is the active backend, and at least 8 GB RAM is free.
     This would improve onboarding. Not part of the TDD tasks below. -->

---

## Tasks

### Task 1: Project scaffold — conftest, pyproject.toml, pre-commit

**Files:**
- `tests/conftest.py`
- `pyproject.toml`
- `.pre-commit-config.yaml`

**Steps:**

1. Write test in `tests/test_scaffold.py`:

   ```python
   # tests/test_scaffold.py
   """Verify that the project scaffold is correctly configured."""
   import sys
   from pathlib import Path


   def test_pipeline_on_sys_path() -> None:
       """conftest.py must add pipeline/ to sys.path so pipeline modules import."""
       pipeline_dir = str(Path(__file__).parent.parent / "pipeline")
       assert pipeline_dir in sys.path, (
           f"pipeline/ not in sys.path.\nExpected: {pipeline_dir}\nGot: {sys.path}"
       )


   def test_pyproject_toml_exists() -> None:
       """pyproject.toml must exist at repo root."""
       assert (Path(__file__).parent.parent / "pyproject.toml").exists()


   def test_pyproject_has_pytest() -> None:
       """pyproject.toml must declare pytest as a dev dependency."""
       content = (Path(__file__).parent.parent / "pyproject.toml").read_text()
       assert "pytest" in content


   def test_pre_commit_config_exists() -> None:
       """.pre-commit-config.yaml must exist at repo root."""
       assert (Path(__file__).parent.parent / ".pre-commit-config.yaml").exists()


   def test_pre_commit_uses_ruff() -> None:
       """.pre-commit-config.yaml must configure ruff hooks."""
       content = (
           Path(__file__).parent.parent / ".pre-commit-config.yaml"
       ).read_text()
       assert "ruff" in content
   ```

2. Run and confirm FAILED:

   ```bash
   uv run pytest tests/test_scaffold.py -v
   # Expected: FAILED — conftest.py and pyproject.toml do not exist yet
   # Note: do NOT use --frozen here; uv.lock does not exist until step 4
   ```

3. Implement in the files listed above.

   `tests/conftest.py`:

   ```python
   # tests/conftest.py
   """
   Root conftest — adds pipeline/ to sys.path for all tests.

   Every test module can import pipeline source directly, e.g.:
       from adapters.translation_adapter import SQLiteScrollmapperAdapter
   """
   import sys
   from pathlib import Path

   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
   ```

   `pyproject.toml` (repo root):

   ```toml
   [project]
   name = "ot-nlp"
   version = "0.1.0"
   description = "Biblical Hebrew NLP analysis pipeline"
   requires-python = ">=3.11"
   dependencies = [
       "psycopg2-binary==2.9.9",
       "pgvector==0.2.5",
       "pyyaml==6.0.1",
       "text-fabric==12.0.0",
       "numpy==1.26.4",
       "pandas==2.2.2",
       "sphinx==8.1.3",
       "myst-nb==1.3.0",
       "sphinx-book-theme==1.1.3",
       "sphinxcontrib-bibtex==2.6.3",
       "nbconvert==7.16.4",
       "ipykernel==6.29.5",
       "plotly==5.22.0",
       "kaleido==0.2.1",
       "streamlit==1.35.0",
       "usfm-grammar==2.3.0",
       "pronouncing==0.2.0",
       "anthropic==0.28.1",
       "openai==1.35.0",
       "google-generativeai==0.7.2",
       "requests==2.32.3",
   ]

   [tool.uv]
   dev-dependencies = [
       "pytest>=8.2",
       "ruff>=0.4",
       "pyright>=1.1",
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
   select = ["E", "F", "I", "UP", "B", "SIM"]

   [tool.pyright]
   pythonVersion = "3.11"
   typeCheckingMode = "basic"
   include = ["pipeline", "tests"]
   ```

   `.pre-commit-config.yaml` (repo root):

   ```yaml
   repos:
     - repo: https://github.com/astral-sh/ruff-pre-commit
       rev: v0.4.4
       hooks:
         - id: ruff
           args: [--fix]
         - id: ruff-format
   ```

4. Generate lockfile and wire pre-commit hooks:

   ```bash
   uv lock                      # creates uv.lock; required before any --frozen command
   uv run pre-commit install    # wire ruff hooks into .git/hooks/pre-commit
   ```

5. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_scaffold.py -v
   # Expected: PASSED (5 tests)
   ```

6. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

7. Commit: `"scaffold: conftest.py, pyproject.toml, pre-commit config"`

---

### Task 2: Pipeline configuration file

**Files:**
- `pipeline/config.yml`

**Steps:**

1. Write test in `tests/test_config.py`:

   ```python
   # tests/test_config.py
   """Verify config.yml is valid YAML and contains all required top-level sections."""
   from pathlib import Path

   import yaml

   CONFIG_PATH = Path(__file__).parent.parent / "pipeline" / "config.yml"

   REQUIRED_SECTIONS = [
       "pipeline",
       "corpus",
       "bhsa",
       "translations",
       "fingerprint",
       "breath",
       "chiasm",
       "scoring",
       "llm",
       "export",
   ]


   def test_config_file_exists() -> None:
       """pipeline/config.yml must exist."""
       assert CONFIG_PATH.exists(), f"config.yml not found at {CONFIG_PATH}"


   def test_config_is_valid_yaml() -> None:
       """config.yml must parse without error."""
       data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
       assert isinstance(data, dict), "config.yml root must be a mapping"


   def test_config_has_required_sections() -> None:
       """config.yml must contain all required top-level sections."""
       data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
       for section in REQUIRED_SECTIONS:
           assert section in data, f"Missing required section: '{section}'"


   def test_config_corpus_has_psalms() -> None:
       """corpus.books must include Psalms (book_num=19)."""
       data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
       book_nums = [b["book_num"] for b in data["corpus"]["books"]]
       assert 19 in book_nums, f"Psalms (book_num=19) missing from corpus.books"


   def test_config_translations_have_kjv() -> None:
       """translations.sources must include a KJV entry."""
       data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
       ids = [s["id"] for s in data["translations"]["sources"]]
       assert "KJV" in ids, f"KJV not in translation sources: {ids}"


   def test_config_llm_provider_default() -> None:
       """llm.provider must default to 'none' for fully offline operation."""
       data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
       assert data["llm"]["provider"] == "none"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_config.py -v
   # Expected: FAILED — pipeline/config.yml does not exist yet
   ```

3. Implement `pipeline/config.yml`:

   ```yaml
   # ═══════════════════════════════════════════════════════════════
   # Psalms NLP Pipeline — Master Configuration
   # All paths are container-internal (Linux). Host paths appear
   # ONLY in docker-compose.yml volume mount definitions.
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
     debug_chapters: []   # e.g. [23] to run only Psalm 23 during development

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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_config.py -v
   # Expected: PASSED (6 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"config: add pipeline/config.yml skeleton with all required sections"`

---

### Task 3: Database schema — init_schema.sql

**Files:**
- `pipeline/init_schema.sql`

**Steps:**

1. Write test in `tests/test_schema_sql.py`:

   ```python
   # tests/test_schema_sql.py
   """
   Static analysis of init_schema.sql.

   Verifies all required objects are defined without connecting to a database.
   Live database assertions are performed by validate_infrastructure.py.
   """
   import re
   from pathlib import Path

   SCHEMA_PATH = Path(__file__).parent.parent / "pipeline" / "init_schema.sql"

   REQUIRED_TABLES = [
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
   ]

   REQUIRED_INDICES = [
       "idx_verses_book_chapter",
       "idx_translations_verse",
       "idx_translations_key",
       "idx_word_tokens_verse",
       "idx_syllable_tokens_verse",
       "idx_syllable_tokens_token",
       "idx_translation_scores_verse",
       "idx_translation_scores_key",
       "idx_chiasm_candidates_start",
   ]

   SEED_BOOK_NUMS = [1, 2, 18, 19, 23, 25]


   def _sql() -> str:
       return SCHEMA_PATH.read_text(encoding="utf-8")


   def test_schema_file_exists() -> None:
       """init_schema.sql must exist at pipeline/init_schema.sql."""
       assert SCHEMA_PATH.exists(), f"Schema file not found at {SCHEMA_PATH}"


   def test_schema_enables_pgvector() -> None:
       """Schema must enable the pgvector extension."""
       sql = _sql().lower()
       assert "create extension if not exists" in sql
       # pgvector is registered as 'vector' internally
       assert "pgvector" in sql or "vector" in sql


   def test_schema_has_all_tables() -> None:
       """Schema must define all 11 required tables with IF NOT EXISTS."""
       sql = _sql().lower()
       for table in REQUIRED_TABLES:
           pattern = rf"create table if not exists\s+{table}\b"
           assert re.search(pattern, sql), (
               f"Missing 'CREATE TABLE IF NOT EXISTS {table}' in init_schema.sql"
           )


   def test_schema_has_all_indices() -> None:
       """Schema must define all 9 required indices."""
       sql = _sql().lower()
       for idx in REQUIRED_INDICES:
           assert idx in sql, f"Missing index '{idx}' in init_schema.sql"


   def test_schema_has_books_seed_insert() -> None:
       """Schema must contain the INSERT INTO books statement."""
       assert "INSERT INTO books" in _sql()


   def test_schema_seeds_all_six_books() -> None:
       """All 6 expected book_num values must appear in the INSERT block."""
       sql = _sql()
       for book_num in SEED_BOOK_NUMS:
           assert str(book_num) in sql, (
               f"book_num={book_num} not found in init_schema.sql seed data"
           )
       for name in ["Genesis", "Exodus", "Job", "Psalms", "Isaiah", "Lamentations"]:
           assert name in sql, f"Book name '{name}' missing from seed data"


   def test_schema_books_insert_is_idempotent() -> None:
       """Books INSERT must use ON CONFLICT DO NOTHING for idempotency."""
       assert "ON CONFLICT (book_num) DO NOTHING" in _sql()


   def test_schema_pipeline_runs_has_status_check() -> None:
       """pipeline_runs.status must have a CHECK constraint with all 3 values."""
       sql = _sql()
       assert "'running'" in sql
       assert "'ok'" in sql
       assert "'error'" in sql
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_schema_sql.py -v
   # Expected: FAILED — pipeline/init_schema.sql does not exist yet
   ```

3. Implement `pipeline/init_schema.sql`:

   ```sql
   -- ═══════════════════════════════════════════════════════════════
   -- Psalms NLP Pipeline — Complete Database Schema
   -- Run once via:
   --   docker exec psalms_db psql -U psalms -d psalms \
   --     -f /pipeline/init_schema.sql
   -- All CREATE statements use IF NOT EXISTS — fully idempotent.
   -- ═══════════════════════════════════════════════════════════════

   -- Extensions
   CREATE EXTENSION IF NOT EXISTS pgvector;
   CREATE EXTENSION IF NOT EXISTS pg_trgm;

   -- ───────────────────────────────────────────────────────────────
   -- Corpus reference table
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
       word_count  INTEGER,
       colon_count INTEGER,
       is_aramaic  BOOLEAN NOT NULL DEFAULT FALSE,
       is_acrostic BOOLEAN NOT NULL DEFAULT FALSE,
       UNIQUE (book_num, chapter, verse_num)
   );

   CREATE TABLE IF NOT EXISTS translations (
       translation_id  SERIAL  PRIMARY KEY,
       verse_id        INTEGER NOT NULL REFERENCES verses(verse_id),
       translation_key TEXT    NOT NULL,
       verse_text      TEXT    NOT NULL,
       word_count      INTEGER,
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
       colon_index    INTEGER,
       UNIQUE (verse_id, position)
   );

   CREATE TABLE IF NOT EXISTS verse_fingerprints (
       fingerprint_id     SERIAL    PRIMARY KEY,
       verse_id           INTEGER   NOT NULL UNIQUE REFERENCES verses(verse_id),
       syllable_density   NUMERIC(6,4),
       morpheme_ratio     NUMERIC(6,4),
       sonority_score     NUMERIC(6,4),
       clause_compression NUMERIC(6,4),
       colon_fingerprints JSONB,
       computed_at        TIMESTAMP NOT NULL DEFAULT NOW()
   );

   CREATE TABLE IF NOT EXISTS chiasm_candidates (
       chiasm_id      SERIAL    PRIMARY KEY,
       verse_id_start INTEGER   NOT NULL REFERENCES verses(verse_id),
       verse_id_end   INTEGER   NOT NULL REFERENCES verses(verse_id),
       pattern_type   TEXT      NOT NULL
                          CHECK (pattern_type IN ('ABBA', 'ABCBA', 'AB')),
       colon_matches  JSONB     NOT NULL,
       confidence     NUMERIC(5,4) NOT NULL,
       is_reviewed    BOOLEAN   NOT NULL DEFAULT FALSE,
       reviewer_note  TEXT,
       computed_at    TIMESTAMP NOT NULL DEFAULT NOW()
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 3: Syllable tokens and breath profiles
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS syllable_tokens (
       syllable_id     SERIAL    PRIMARY KEY,
       token_id        INTEGER   NOT NULL REFERENCES word_tokens(token_id),
       verse_id        INTEGER   NOT NULL REFERENCES verses(verse_id),
       syllable_index  INTEGER   NOT NULL,
       syllable_text   TEXT      NOT NULL,
       nucleus_vowel   TEXT,
       vowel_openness  NUMERIC(4,3),
       vowel_length    TEXT
                           CHECK (vowel_length IN
                               ('long', 'short', 'ultra-short', 'shewa')),
       is_open         BOOLEAN,
       onset_class     TEXT
                           CHECK (onset_class IN
                               ('guttural', 'sibilant', 'liquid', 'stop',
                                'nasal', 'none')),
       breath_weight   NUMERIC(5,4),
       stress_position NUMERIC(5,4),
       colon_index     INTEGER   NOT NULL,
       UNIQUE (token_id, syllable_index)
   );

   CREATE TABLE IF NOT EXISTS breath_profiles (
       profile_id       SERIAL    PRIMARY KEY,
       verse_id         INTEGER   NOT NULL UNIQUE REFERENCES verses(verse_id),
       mean_weight      NUMERIC(5,4),
       open_ratio       NUMERIC(5,4),
       guttural_density NUMERIC(5,4),
       colon_count      INTEGER,
       colon_boundaries INTEGER[],
       stress_positions NUMERIC(5,4)[],
       breath_curve     NUMERIC(5,4)[],
       computed_at      TIMESTAMP NOT NULL DEFAULT NOW()
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 4: Translation scoring
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS translation_scores (
       score_id              SERIAL    PRIMARY KEY,
       verse_id              INTEGER   NOT NULL REFERENCES verses(verse_id),
       translation_key       TEXT      NOT NULL,
       density_deviation     NUMERIC(7,4),
       morpheme_deviation    NUMERIC(7,4),
       sonority_deviation    NUMERIC(7,4),
       compression_deviation NUMERIC(7,4),
       composite_deviation   NUMERIC(7,4),
       stress_alignment      NUMERIC(5,4),
       weight_match          NUMERIC(5,4),
       breath_alignment      NUMERIC(5,4),
       scored_at             TIMESTAMP NOT NULL DEFAULT NOW(),
       UNIQUE (verse_id, translation_key)
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 5: LLM-generated translation suggestions
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS suggestions (
       suggestion_id       SERIAL    PRIMARY KEY,
       verse_id            INTEGER   NOT NULL REFERENCES verses(verse_id),
       translation_key     TEXT      NOT NULL,
       suggested_text      TEXT      NOT NULL,
       composite_deviation NUMERIC(7,4),
       breath_alignment    NUMERIC(5,4),
       improvement_delta   NUMERIC(7,4),
       llm_provider        TEXT      NOT NULL,
       llm_model           TEXT      NOT NULL,
       prompt_version      TEXT      NOT NULL,
       generated_at        TIMESTAMP NOT NULL DEFAULT NOW()
   );

   -- ───────────────────────────────────────────────────────────────
   -- Stage 7: Pipeline run audit log
   -- ───────────────────────────────────────────────────────────────

   CREATE TABLE IF NOT EXISTS pipeline_runs (
       run_id        SERIAL    PRIMARY KEY,
       started_at    TIMESTAMP NOT NULL DEFAULT NOW(),
       finished_at   TIMESTAMP,
       status        TEXT      NOT NULL DEFAULT 'running'
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
       ( 1, 'Genesis',      'OT', 'hebrew'),
       ( 2, 'Exodus',       'OT', 'hebrew'),
       (18, 'Job',          'OT', 'hebrew'),
       (19, 'Psalms',       'OT', 'hebrew'),
       (23, 'Isaiah',       'OT', 'hebrew'),
       (25, 'Lamentations', 'OT', 'hebrew')
   ON CONFLICT (book_num) DO NOTHING;
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_schema_sql.py -v
   # Expected: PASSED (8 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"schema: init_schema.sql — 11 tables, 9 indices, 6 seed rows"`

---

### Task 4: Pipeline Dockerfile

**Files:**
- `pipeline/Dockerfile.pipeline`

> **Note:** Dependencies are declared in the repo-root `pyproject.toml`
> (added in Task 1) and locked in `uv.lock`.  The Docker build context is
> the repo root so both files are accessible at build time.  No separate
> `requirements.txt` is needed.

**Steps:**

1. Write test in `tests/test_dockerfile.py`:

   ```python
   # tests/test_dockerfile.py
   """Static verification of pipeline/Dockerfile.pipeline."""
   from pathlib import Path

   DOCKERFILE = Path(__file__).parent.parent / "pipeline" / "Dockerfile.pipeline"


   def test_dockerfile_exists() -> None:
       assert DOCKERFILE.exists()


   def test_dockerfile_uses_python311_slim() -> None:
       assert "FROM python:3.11-slim" in DOCKERFILE.read_text()


   def test_dockerfile_installs_typst_pinned() -> None:
       content = DOCKERFILE.read_text()
       assert "typst" in content.lower()
       assert "0.12.0" in content


   def test_dockerfile_workdir_is_pipeline() -> None:
       assert "WORKDIR /pipeline" in DOCKERFILE.read_text()


   def test_dockerfile_copies_lockfile() -> None:
       """Dockerfile must copy pyproject.toml and uv.lock, not a requirements.txt."""
       content = DOCKERFILE.read_text()
       assert "pyproject.toml" in content
       assert "uv.lock" in content


   def test_dockerfile_uses_uv_sync() -> None:
       """Dockerfile must install via uv sync — not pip or uv pip install."""
       content = DOCKERFILE.read_text()
       assert "uv sync" in content, "Dockerfile must use 'uv sync' to install deps"
       assert "pip install" not in content, (
           "Dockerfile must not call pip install in any form"
       )


   def test_dockerfile_sets_venv_on_path() -> None:
       """The venv bin dir must be on PATH so 'python' resolves correctly."""
       assert "/pipeline/.venv/bin" in DOCKERFILE.read_text()


   def test_dockerfile_cmd_is_run_py() -> None:
       assert "run.py" in DOCKERFILE.read_text()
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_dockerfile.py -v
   # Expected: FAILED — pipeline/Dockerfile.pipeline does not exist yet
   ```

3. Implement `pipeline/Dockerfile.pipeline`:

   ```dockerfile
   FROM python:3.11-slim

   # Typst — pinned single-binary PDF renderer (used in Stage 6)
   ARG TYPST_VERSION=0.12.0
   RUN apt-get update && apt-get install -y --no-install-recommends \
         curl xz-utils pandoc \
       && curl -L \
         "https://github.com/typst/typst/releases/download/v${TYPST_VERSION}/typst-x86_64-unknown-linux-musl.tar.xz" \
         | tar -xJ --strip-components=1 -C /usr/local/bin \
               "typst-x86_64-unknown-linux-musl/typst" \
       && apt-get clean \
       && rm -rf /var/lib/apt/lists/*

   WORKDIR /pipeline

   # uv is the project's only allowed package manager
   COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

   # Copy dependency specs from the repo root (build context must be '.')
   COPY pyproject.toml uv.lock ./

   # Install production deps into a venv; --no-dev excludes pytest/ruff/pyright
   RUN uv sync --frozen --no-dev

   # Put the venv on PATH so 'python' and entry-points work without 'uv run'
   ENV PATH="/pipeline/.venv/bin:$PATH"

   # Default entrypoint: full pipeline orchestrator.
   # Override CMD for single-stage execution, e.g.:
   #   docker compose --profile pipeline run pipeline python modules/ingest.py
   CMD ["python", "run.py"]
   ```

   > **docker-compose.yml build context:** because `pyproject.toml` and
   > `uv.lock` live at the repo root, the pipeline service must use context
   > `.` with an explicit `dockerfile:` path.  This is reflected in Task 6.

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_dockerfile.py -v
   # Expected: PASSED (7 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"docker: Dockerfile.pipeline using uv sync"`

---

### Task 5: Streamlit placeholder service

**Files:**
- `streamlit/Dockerfile.streamlit`
- `streamlit/requirements_streamlit.txt`
- `streamlit/app.py`

**Steps:**

1. Write test in `tests/test_streamlit_placeholder.py`:

   ```python
   # tests/test_streamlit_placeholder.py
   """Static verification of Streamlit service files."""
   from pathlib import Path

   STREAMLIT_DIR = Path(__file__).parent.parent / "streamlit"


   def test_dockerfile_exists() -> None:
       assert (STREAMLIT_DIR / "Dockerfile.streamlit").exists()


   def test_dockerfile_uses_python311_slim() -> None:
       content = (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()
       assert "FROM python:3.11-slim" in content


   def test_dockerfile_exposes_8501() -> None:
       content = (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()
       assert "EXPOSE 8501" in content


   def test_requirements_exists() -> None:
       assert (STREAMLIT_DIR / "requirements_streamlit.txt").exists()


   def test_streamlit_pyproject_exists() -> None:
       """streamlit/pyproject.toml must exist (uv sync source for the container)."""
       assert (STREAMLIT_DIR / "pyproject.toml").exists()


   def test_streamlit_pyproject_pins_streamlit() -> None:
       content = (STREAMLIT_DIR / "pyproject.toml").read_text()
       assert "streamlit==" in content


   def test_dockerfile_uses_uv_sync() -> None:
       """Streamlit Dockerfile must use 'uv sync' — not pip or uv pip install."""
       content = (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()
       assert "uv sync" in content, "Dockerfile.streamlit must use 'uv sync'"
       assert "pip install" not in content, (
           "Dockerfile.streamlit must not call pip install in any form"
       )


   def test_dockerfile_sets_venv_on_path() -> None:
       assert "/app/.venv/bin" in (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()


   def test_app_exists() -> None:
       assert (STREAMLIT_DIR / "app.py").exists()


   def test_app_is_placeholder() -> None:
       content = (STREAMLIT_DIR / "app.py").read_text()
       assert "Coming in Stage 6" in content


   def test_app_has_set_page_config() -> None:
       content = (STREAMLIT_DIR / "app.py").read_text()
       assert "st.set_page_config" in content
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_streamlit_placeholder.py -v
   # Expected: FAILED — streamlit/ files do not exist yet
   ```

3. Implement the four files.

   `streamlit/pyproject.toml`:

   ```toml
   [project]
   name = "ot-nlp-streamlit"
   version = "0.1.0"
   description = "Psalms NLP Explorer — Streamlit UI"
   requires-python = ">=3.11"
   dependencies = [
       "streamlit==1.35.0",
       "plotly==5.22.0",
       "psycopg2-binary==2.9.9",
       "pandas==2.2.2",
       "numpy==1.26.4",
   ]
   ```

3b. Generate the streamlit lockfile (required by `uv sync --frozen` inside the Docker image):

   ```bash
   cd streamlit && uv lock && cd ..
   # Creates streamlit/uv.lock — must be committed before docker build
   ```

   `streamlit/Dockerfile.streamlit`:

   ```dockerfile
   FROM python:3.11-slim

   WORKDIR /app

   # uv is the project's only allowed package manager
   COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

   # Copy dependency specs (build context is ./streamlit)
   COPY pyproject.toml uv.lock ./

   # Install into a venv; uv.lock guarantees exact pinned versions
   RUN uv sync --frozen

   # Put the venv on PATH so 'streamlit' resolves without 'uv run'
   ENV PATH="/app/.venv/bin:$PATH"

   EXPOSE 8501

   CMD ["streamlit", "run", "app.py", \
        "--server.port=8501", \
        "--server.address=0.0.0.0"]
   ```

   `streamlit/app.py`:

   ```python
   """
   Psalms NLP Explorer — Streamlit application.

   This is a placeholder. Full implementation arrives in Stage 6.
   Coming in Stage 6: interactive visualization of style fingerprints,
   breath profiles, chiasm candidates, and translation scores.
   """
   import streamlit as st

   st.set_page_config(page_title="Psalms NLP Explorer", layout="wide")
   st.title("Psalms NLP Explorer")
   st.info(
       "Pipeline not yet run. Return after Stage 4 completes. "
       "Coming in Stage 6: full interactive analysis."
   )
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_streamlit_placeholder.py -v
   # Expected: PASSED (9 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"streamlit: placeholder Dockerfile (uv sync), pyproject.toml, and app.py"`

---

### Task 6: docker-compose.yml

**Files:**
- `docker-compose.yml`
- `.env.example`

**Steps:**

1. Write test in `tests/test_docker_compose.py`:

   ```python
   # tests/test_docker_compose.py
   """
   Static verification that docker-compose.yml is valid YAML and defines all
   required services, networks, volumes, and safety constraints.
   """
   import re
   from pathlib import Path

   import yaml

   REPO_ROOT = Path(__file__).parent.parent
   COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
   ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"


   def _compose() -> dict:
       return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


   def test_compose_file_exists() -> None:
       assert COMPOSE_PATH.exists()


   def test_compose_is_valid_yaml() -> None:
       assert isinstance(_compose(), dict)


   def test_compose_has_all_services() -> None:
       services = _compose().get("services", {})
       for svc in ["db", "jupyter", "streamlit", "pipeline"]:
           assert svc in services, f"Missing service '{svc}'"


   def test_compose_db_uses_pgvector_image() -> None:
       assert "pgvector" in _compose()["services"]["db"]["image"]


   def test_compose_db_has_healthcheck() -> None:
       assert "healthcheck" in _compose()["services"]["db"]


   def test_compose_pipeline_has_pipeline_profile() -> None:
       profiles = _compose()["services"]["pipeline"].get("profiles", [])
       assert "pipeline" in profiles


   def test_compose_has_psalms_net_network() -> None:
       assert "psalms_net" in _compose().get("networks", {})


   def test_compose_has_pg_data_volume() -> None:
       assert "pg_data" in _compose().get("volumes", {})


   def test_compose_no_hardcoded_passwords() -> None:
       """Credentials must use ${VAR:-default} syntax, not bare literals."""
       raw = COMPOSE_PATH.read_text()
       # Reject POSTGRES_PASSWORD: <bare_value> not wrapped in ${}
       bad = re.findall(r"POSTGRES_PASSWORD:\s+(?!\$\{)[^\s#]", raw)
       assert not bad, f"Hard-coded password found: {bad}"


   def test_compose_jupyter_depends_on_db() -> None:
       depends = _compose()["services"]["jupyter"].get("depends_on", {})
       assert "db" in depends


   def test_env_example_exists() -> None:
       assert ENV_EXAMPLE_PATH.exists()


   def test_env_example_documents_required_vars() -> None:
       content = ENV_EXAMPLE_PATH.read_text()
       for var in ["POSTGRES_PASSWORD", "POSTGRES_USER", "POSTGRES_DB", "LLM_PROVIDER"]:
           assert var in content, f"'{var}' missing from .env.example"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_docker_compose.py -v
   # Expected: FAILED — docker-compose.yml does not exist yet
   ```

3. Implement `docker-compose.yml` at repo root:

   ```yaml
   # ═══════════════════════════════════════════════════════════════
   # Psalms NLP Pipeline — Docker Compose
   #
   # Host paths (relative, Linux convention) appear ONLY in volume
   # mount definitions. All other config uses container paths.
   # Credentials: use a .env file (see .env.example). Never commit
   # real secrets. The ${VAR:-default} syntax makes the stack work
   # without any .env file for local development.
   # ═══════════════════════════════════════════════════════════════

   networks:
     psalms_net:
       driver: bridge

   volumes:
     pg_data:

   services:

     # ── PostgreSQL 16 with pgvector ───────────────────────────
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
         test: ["CMD-SHELL", "pg_isready -U psalms -d psalms"]
         interval: 10s
         timeout: 5s
         retries: 5

     # ── JupyterLab ────────────────────────────────────────────
     jupyter:
       image: jupyter/base-notebook:python-3.11
       container_name: psalms_jupyter
       restart: unless-stopped
       environment:
         JUPYTER_ENABLE_LAB: "yes"
         JUPYTER_TOKEN:      ${JUPYTER_TOKEN:-psalms_dev}
         POSTGRES_HOST:      db
         POSTGRES_DB:        ${POSTGRES_DB:-psalms}
         POSTGRES_USER:      ${POSTGRES_USER:-psalms}
         POSTGRES_PASSWORD:  ${POSTGRES_PASSWORD:-psalms_dev}
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

     # ── Streamlit explorer (start with: --profile ui) ─────────
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

     # ── Pipeline runner (start with: --profile pipeline run) ──
     pipeline:
       build:
         context: .                          # repo root — needs pyproject.toml + uv.lock
         dockerfile: pipeline/Dockerfile.pipeline
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

     # Ollama — optional local LLM (uncomment to enable)
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

   Implement `.env.example` at repo root:

   ```bash
   # Copy this file to .env and fill in real values.
   # .env is git-ignored — never commit it.

   # PostgreSQL
   POSTGRES_USER=psalms
   POSTGRES_PASSWORD=change_me_in_production
   POSTGRES_DB=psalms

   # JupyterLab access token
   JUPYTER_TOKEN=change_me_in_production

   # LLM — none | anthropic | openai | gemini | openrouter | ollama
   LLM_PROVIDER=none
   LLM_API_KEY=
   LLM_MODEL=

   # Provider-specific API keys (only needed when LLM_PROVIDER is set)
   ANTHROPIC_API_KEY=
   OPENAI_API_KEY=
   GOOGLE_API_KEY=

   # ESV Bible API (optional, for API adapter in Stage 1)
   ESV_API_KEY=

   # Ollama (when LLM_PROVIDER=ollama)
   OLLAMA_HOST=
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_docker_compose.py -v
   # Expected: PASSED (12 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"docker: add docker-compose.yml with all 4 services and .env.example"`

---

### Task 7: Infrastructure validation script

**Files:**
- `pipeline/validate_infrastructure.py`

**Steps:**

1. Write the script first. It acts as the TDD "test" for the entire Docker stack.
   Running it before containers are up must fail with a clear error.

   Implement `pipeline/validate_infrastructure.py`:

   ```python
   # pipeline/validate_infrastructure.py
   """
   Stage 0 infrastructure validator.

   Asserts that the full Docker stack is correctly configured:
     - PostgreSQL is reachable and accepting connections
     - pgvector extension is installed
     - All 11 required tables exist
     - books table has exactly 6 seed rows
     - All 9 expected indices exist

   Run from inside the pipeline container:
     docker compose --profile pipeline run --rm pipeline \\
       python validate_infrastructure.py

   Or from the host (requires psycopg2 installed):
     POSTGRES_HOST=localhost python pipeline/validate_infrastructure.py
   """

   from __future__ import annotations

   import logging
   import os
   import sys

   import psycopg2
   import psycopg2.extensions

   logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
   logger = logging.getLogger(__name__)

   REQUIRED_TABLES = {
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

   REQUIRED_INDICES = {
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

   EXPECTED_BOOKS = {
       (1, "Genesis"),
       (2, "Exodus"),
       (18, "Job"),
       (19, "Psalms"),
       (23, "Isaiah"),
       (25, "Lamentations"),
   }


   def _conn() -> psycopg2.extensions.connection:
       """Open a connection using environment variables."""
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
           result = cur.fetchone()
       assert result == (1,), f"SELECT 1 returned unexpected result: {result}"
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
       """Assert all 11 required tables exist in the public schema."""
       with conn.cursor() as cur:
           cur.execute(
               "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
           )
           found = {row[0] for row in cur.fetchall()}
       missing = REQUIRED_TABLES - found
       assert not missing, (
           f"Missing tables: {sorted(missing)}\n"
           f"Found: {sorted(found)}"
       )
       logger.info(f"CHECK  all {len(REQUIRED_TABLES)} tables present: OK")


   def check_indices(conn: psycopg2.extensions.connection) -> None:
       """Assert all 9 required indices exist."""
       with conn.cursor() as cur:
           cur.execute(
               "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
           )
           found = {row[0] for row in cur.fetchall()}
       missing = REQUIRED_INDICES - found
       assert not missing, (
           f"Missing indices: {sorted(missing)}\n"
           f"Found: {sorted(found & REQUIRED_INDICES)}"
       )
       logger.info(f"CHECK  all {len(REQUIRED_INDICES)} indices present: OK")


   def check_books_seed(conn: psycopg2.extensions.connection) -> None:
       """Assert books table contains exactly the 6 expected seed rows."""
       with conn.cursor() as cur:
           cur.execute("SELECT book_num, book_name FROM books ORDER BY book_num")
           rows = {(r[0], r[1]) for r in cur.fetchall()}
       assert rows == EXPECTED_BOOKS, (
           f"books mismatch.\nExpected: {sorted(EXPECTED_BOOKS)}\nGot: {sorted(rows)}"
       )
       logger.info(f"CHECK  books seed data ({len(rows)} rows): OK")


   def main() -> int:
       """Run all checks. Return 0 on success, 1 on any failure."""
       failures: list[str] = []

       try:
           conn = _conn()
       except psycopg2.OperationalError:
           logger.exception(
               "Cannot connect to PostgreSQL. Is the db container running?\n"
               f"  POSTGRES_HOST={os.environ.get('POSTGRES_HOST', 'localhost')}"
           )
           return 1

       checks = [
           ("connection",  check_connection),
           ("pgvector",    check_pgvector),
           ("tables",      check_tables),
           ("indices",     check_indices),
           ("books seed",  check_books_seed),
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

2. Run and confirm it fails before containers exist:

   ```bash
   python /home/user/OT-NLP/pipeline/validate_infrastructure.py
   # Expected: exits 1
   # ERROR  Cannot connect to PostgreSQL. Is the db container running?
   ```

3. Start the Docker stack and initialize the schema (this is the "implementation"
   step that makes the script pass):

   ```bash
   # Create required host-side directories
   mkdir -p /home/user/OT-NLP/data/bhsa
   mkdir -p /home/user/OT-NLP/data/translations
   mkdir -p /home/user/OT-NLP/data/outputs/report
   mkdir -p /home/user/OT-NLP/notebooks
   mkdir -p /home/user/OT-NLP/pipeline/modules
   mkdir -p /home/user/OT-NLP/pipeline/adapters
   mkdir -p /home/user/OT-NLP/pipeline/visualize

   # Bring up core services
   docker compose up -d db jupyter

   # Wait for db healthcheck (watch until 'healthy' appears)
   watch docker compose ps

   # Apply schema
   docker compose exec db \
     psql -U psalms -d psalms -f /pipeline/init_schema.sql

   # Expected final output lines:
   # CREATE INDEX
   # INSERT 0 6
   ```

4. Run the validator inside the pipeline container and confirm it passes:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python validate_infrastructure.py
   # Expected: exits 0
   # INFO  CHECK  database connection: OK
   # INFO  CHECK  pgvector extension: OK
   # INFO  CHECK  all 11 tables present: OK
   # INFO  CHECK  all 9 indices present: OK
   # INFO  CHECK  books seed data (6 rows): OK
   # INFO
   # INFO  All infrastructure checks PASSED.
   ```

5. Verify idempotency (re-running schema must produce no errors and no duplicate rows):

   ```bash
   docker compose exec db \
     psql -U psalms -d psalms -f /pipeline/init_schema.sql

   docker compose exec db \
     psql -U psalms -d psalms -c "SELECT COUNT(*) FROM books;"
   # Expected: 6
   ```

6. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

7. Commit: `"validate: validate_infrastructure.py — Stage 0 acceptance gate"`

---

### Task 8: Full test suite green + final verification

**Steps:**

1. Run the complete test suite:

   ```bash
   uv run --frozen pytest tests/ -v
   # Expected: all tests PASSED
   # test_scaffold.py                — 5 tests
   # test_config.py                  — 6 tests
   # test_schema_sql.py              — 8 tests
   # test_dockerfile.py              — 9 tests
   # test_streamlit_placeholder.py   — 8 tests
   # test_docker_compose.py          — 12 tests
   ```

2. Run lint and format checks:

   ```bash
   uv run --frozen ruff check . --fix
   uv run --frozen ruff format .
   uv run --frozen pyright
   ```

3. Manual acceptance verification:

   ```bash
   # Container status
   docker compose ps
   # Expected: psalms_db (healthy), psalms_jupyter (running)

   # pgvector
   docker exec psalms_db psql -U psalms -d psalms -c "\dx" | grep vector

   # 11 tables
   docker exec psalms_db psql -U psalms -d psalms -c "\dt"

   # 6 seed rows
   docker exec psalms_db psql -U psalms -d psalms \
     -c "SELECT book_num, book_name FROM books ORDER BY book_num;"

   # Pipeline image build
   docker compose --profile pipeline build pipeline
   # Expected: exits 0
   ```

4. Commit: `"stage-0: complete — all tests pass, infrastructure validated"`
