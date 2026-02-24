# Stage 0 — Foundation & Infrastructure
## Detailed Implementation Plan

> **Depends on:** Nothing  
> **Produces:** Running Docker stack with all services healthy; complete database schema initialized; connectivity validated  
> **Estimated time:** 2–4 hours on first setup

---

## Objectives

1. Install and configure WSL2 + Docker Desktop on Windows host
2. Create the full project directory tree on the Windows host
3. Author `docker-compose.yml` with all services and correct volume mounts
4. Write `init_schema.sql` defining every table for all stages
5. Build and start all containers
6. Validate cross-container connectivity and schema correctness

---

## Prerequisites

- Windows 10 version 2004+ or Windows 11
- Administrator access to the machine
- Internet access for initial Docker image pulls (offline after setup)
- Intel NUC or equivalent host with ≥16 GB RAM and ≥50 GB free disk

---

## Step 1 — Enable WSL2 and Install Docker Desktop

Run the following in an elevated PowerShell:

```powershell
# Enable WSL2
wsl --install
# Reboot if prompted, then set WSL2 as default
wsl --set-default-version 2
```

Install Docker Desktop from https://www.docker.com/products/docker-desktop — select "Use WSL2 based engine" during setup.

Verify Docker is operational:
```powershell
docker --version       # expect 24.x or later
docker compose version # expect 2.x or later
```

---

## Step 2 — Create Project Directory Tree

Run in PowerShell (creates entire tree in one block):

```powershell
$base = "C:\psalms-nlp"
@(
  "$base\pipeline\modules",
  "$base\pipeline\adapters",
  "$base\pipeline\visualize",
  "$base\pipeline\docs",
  "$base\data\bhsa",
  "$base\data\translations",
  "$base\data\outputs\report",
  "$base\notebooks",
  "$base\streamlit"
) | ForEach-Object { New-Item -ItemType Directory -Force -Path $_ }
```

After this command the host tree is:
```
C:\psalms-nlp\
  pipeline\
    modules\
    adapters\
    visualize\
    docs\
  data\
    bhsa\
    translations\
    outputs\
      report\
  notebooks\
  streamlit\
```

---

## Step 3 — File: `docker-compose.yml`

Save to `C:\psalms-nlp\docker-compose.yml`:

```yaml
version: "3.9"

# ─── NETWORKS ────────────────────────────────────────────────────
networks:
  psalms_net:
    driver: bridge

# ─── VOLUMES (named — managed by Docker) ────────────────────────
volumes:
  pg_data:

# ─── SERVICES ────────────────────────────────────────────────────
services:

  # PostgreSQL with pgvector extension
  db:
    image: pgvector/pgvector:pg16
    container_name: psalms_db
    restart: unless-stopped
    environment:
      POSTGRES_DB:       psalms
      POSTGRES_USER:     psalms
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
    volumes:
      - pg_data:/var/lib/postgresql/data
      - C:\psalms-nlp\pipeline:/pipeline:ro
    ports:
      - "5432:5432"
    networks:
      - psalms_net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U psalms -d psalms"]
      interval: 10s
      timeout: 5s
      retries: 5

  # JupyterLab — analysis and notebook development
  jupyter:
    image: jupyter/scipy-notebook:python-3.11
    container_name: psalms_jupyter
    restart: unless-stopped
    environment:
      JUPYTER_ENABLE_LAB: "yes"
      JUPYTER_TOKEN: ${JUPYTER_TOKEN:-psalms_dev}
      POSTGRES_HOST:     db
      POSTGRES_DB:       psalms
      POSTGRES_USER:     psalms
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
    volumes:
      - C:\psalms-nlp\notebooks:/home/jovyan/work
      - C:\psalms-nlp\data:/home/jovyan/data
      - C:\psalms-nlp\pipeline:/pipeline:ro
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
      context: C:\psalms-nlp\streamlit
      dockerfile: Dockerfile.streamlit
    container_name: psalms_streamlit
    restart: unless-stopped
    environment:
      POSTGRES_HOST:     db
      POSTGRES_DB:       psalms
      POSTGRES_USER:     psalms
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
    volumes:
      - C:\psalms-nlp\streamlit:/app
      - C:\psalms-nlp\pipeline:/pipeline:ro
    ports:
      - "8501:8501"
    networks:
      - psalms_net
    depends_on:
      db:
        condition: service_healthy

  # Pipeline runner — starts only via 'docker compose --profile pipeline run'
  pipeline:
    build:
      context: C:\psalms-nlp\pipeline
      dockerfile: Dockerfile.pipeline
    container_name: psalms_pipeline
    profiles:
      - pipeline
    environment:
      POSTGRES_HOST:     db
      POSTGRES_DB:       psalms
      POSTGRES_USER:     psalms
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-psalms_dev}
      LLM_PROVIDER:      ${LLM_PROVIDER:-none}
      LLM_API_KEY:       ${LLM_API_KEY:-}
      LLM_MODEL:         ${LLM_MODEL:-}
      OLLAMA_HOST:       ${OLLAMA_HOST:-}
    volumes:
      - C:\psalms-nlp\pipeline:/pipeline
      - C:\psalms-nlp\data:/data
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

> **Security note:** Never commit credentials to source control. For production use, replace default values with a `.env` file. The compose file uses `${VAR:-default}` syntax so it runs without any `.env` for local development.

---

## Step 4 — File: `pipeline/Dockerfile.pipeline`

```dockerfile
FROM python:3.11-slim

# Typst — pinned single-binary PDF renderer
ARG TYPST_VERSION=0.12.0
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl xz-utils pandoc \
    && curl -L "https://github.com/typst/typst/releases/download/v${TYPST_VERSION}/typst-x86_64-unknown-linux-musl.tar.xz" \
       | tar -xJ --strip-components=1 -C /usr/local/bin \
              "typst-x86_64-unknown-linux-musl/typst" \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /pipeline

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Default: run the orchestrator; override for one-shot stage execution
CMD ["python", "run.py"]
```

---

## Step 5 — File: `pipeline/requirements.txt`

Pin all versions. These are the Stage 0 baseline; later stages append to this file.

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

---

## Step 6 — File: `streamlit/Dockerfile.streamlit`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements_streamlit.txt .
RUN pip install --no-cache-dir -r requirements_streamlit.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

`streamlit/requirements_streamlit.txt`:
```text
streamlit==1.35.0
plotly==5.22.0
psycopg2-binary==2.9.9
pandas==2.2.2
numpy==1.26.4
```

`streamlit/app.py` (placeholder — full implementation in Stage 6):
```python
import streamlit as st

st.set_page_config(page_title="Psalms NLP Explorer", layout="wide")
st.title("Psalms NLP Explorer")
st.info("Pipeline not yet run. Return after Stage 4 completes.")
```

---

## Step 7 — File: `init_schema.sql`

Save to `C:\psalms-nlp\pipeline\init_schema.sql`. This is the complete schema for all stages — written once, never destructively altered.

```sql
-- ═══════════════════════════════════════════════════════════════
-- Psalms NLP Pipeline — Complete Database Schema
-- Run once via: docker exec psalms_db psql -U psalms -d psalms -f /pipeline/init_schema.sql
-- ═══════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS pgvector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- for text search

-- ───────────────────────────────────────────────────────────────
-- STAGE 1: Source verses and translations
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS books (
    book_num        INTEGER PRIMARY KEY,  -- 19 = Psalms
    book_name       TEXT NOT NULL,
    testament       TEXT NOT NULL CHECK (testament IN ('OT','NT')),
    genre           TEXT,                 -- populated Stage 8
    language        TEXT NOT NULL DEFAULT 'hebrew'
);

CREATE TABLE IF NOT EXISTS verses (
    verse_id        SERIAL PRIMARY KEY,
    book_num        INTEGER NOT NULL REFERENCES books(book_num),
    chapter         INTEGER NOT NULL,
    verse_num       INTEGER NOT NULL,
    hebrew_text     TEXT NOT NULL,        -- raw BHSA Unicode text
    word_count      INTEGER,              -- populated Stage 2
    colon_count     INTEGER,              -- populated Stage 3
    is_aramaic      BOOLEAN NOT NULL DEFAULT FALSE,  -- Daniel/Ezra flag
    is_acrostic     BOOLEAN NOT NULL DEFAULT FALSE,  -- Psalm 119 etc.
    UNIQUE (book_num, chapter, verse_num)
);

CREATE TABLE IF NOT EXISTS translations (
    translation_id  SERIAL PRIMARY KEY,
    verse_id        INTEGER NOT NULL REFERENCES verses(verse_id),
    translation_key TEXT NOT NULL,        -- 'KJV', 'YLT', 'ULT', etc.
    verse_text      TEXT NOT NULL,
    word_count      INTEGER,              -- populated Stage 4
    UNIQUE (verse_id, translation_key)
);

-- ───────────────────────────────────────────────────────────────
-- STAGE 2: Morphological tokens and style fingerprints
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS word_tokens (
    token_id        SERIAL PRIMARY KEY,
    verse_id        INTEGER NOT NULL REFERENCES verses(verse_id),
    position        INTEGER NOT NULL,     -- 1-based word position in verse
    surface_form    TEXT NOT NULL,        -- Hebrew surface form with niqqud
    lexeme          TEXT,                 -- dictionary form (root)
    part_of_speech  TEXT,                 -- BHSA POS tag
    morpheme_count  INTEGER,              -- prefix + stem + suffix morphemes
    is_verb         BOOLEAN,
    is_noun         BOOLEAN,
    stem            TEXT,                 -- verb stem (qal, niphal, etc.)
    colon_index     INTEGER,              -- which colon this word belongs to (Stage 3)
    UNIQUE (verse_id, position)
);

CREATE TABLE IF NOT EXISTS verse_fingerprints (
    fingerprint_id  SERIAL PRIMARY KEY,
    verse_id        INTEGER NOT NULL UNIQUE REFERENCES verses(verse_id),
    -- 4-dimensional style fingerprint
    syllable_density    NUMERIC(6,4),     -- syllables per word
    morpheme_ratio      NUMERIC(6,4),     -- morphemes per word
    sonority_score      NUMERIC(6,4),     -- mean consonant sonority
    clause_compression  NUMERIC(6,4),     -- words per clause boundary
    -- colon-level arrays (populated Stage 2 second pass after Stage 3)
    colon_fingerprints  JSONB,            -- [{colon:1, density:x, ...}, ...]
    computed_at         TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Chiasm candidates (populated by modules/chiasm.py after Stage 3)
CREATE TABLE IF NOT EXISTS chiasm_candidates (
    chiasm_id       SERIAL PRIMARY KEY,
    verse_id_start  INTEGER NOT NULL REFERENCES verses(verse_id),
    verse_id_end    INTEGER NOT NULL REFERENCES verses(verse_id),
    pattern_type    TEXT NOT NULL CHECK (pattern_type IN ('ABBA','ABCBA','AB')),
    colon_matches   JSONB NOT NULL,       -- [{a:1,b:4,similarity:0.92}, ...]
    confidence      NUMERIC(5,4) NOT NULL,
    is_reviewed     BOOLEAN NOT NULL DEFAULT FALSE,
    reviewer_note   TEXT,
    computed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- STAGE 3: Syllable tokens and breath profiles
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS syllable_tokens (
    syllable_id     SERIAL PRIMARY KEY,
    token_id        INTEGER NOT NULL REFERENCES word_tokens(token_id),
    verse_id        INTEGER NOT NULL REFERENCES verses(verse_id),
    syllable_index  INTEGER NOT NULL,     -- position within word (1-based)
    syllable_text   TEXT NOT NULL,        -- phonetic representation
    nucleus_vowel   TEXT,                 -- primary vowel character
    vowel_openness  NUMERIC(4,3),         -- 0.0–1.0
    vowel_length    TEXT CHECK (vowel_length IN ('long','short','ultra-short','shewa')),
    is_open         BOOLEAN,              -- CV vs CVC syllable type
    onset_class     TEXT CHECK (onset_class IN ('guttural','sibilant','liquid','stop','nasal','none')),
    breath_weight   NUMERIC(5,4),         -- composite score 0.0–1.0
    stress_position NUMERIC(5,4),         -- normalized 0–1 within verse
    colon_index     INTEGER NOT NULL,     -- which colon (from accent boundaries)
    UNIQUE (token_id, syllable_index)
);

CREATE TABLE IF NOT EXISTS breath_profiles (
    profile_id      SERIAL PRIMARY KEY,
    verse_id        INTEGER NOT NULL UNIQUE REFERENCES verses(verse_id),
    mean_weight     NUMERIC(5,4),
    open_ratio      NUMERIC(5,4),         -- proportion of open syllables
    guttural_density NUMERIC(5,4),        -- proportion of guttural-onset syllables
    colon_count     INTEGER,
    colon_boundaries INTEGER[],           -- syllable indices where colons begin
    stress_positions NUMERIC(5,4)[],      -- normalized stress peak positions
    breath_curve    NUMERIC(5,4)[],       -- full syllable-by-syllable weight array
    computed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- STAGE 4: Translation scoring
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS translation_scores (
    score_id            SERIAL PRIMARY KEY,
    verse_id            INTEGER NOT NULL REFERENCES verses(verse_id),
    translation_key     TEXT NOT NULL,
    -- Style deviation (distance from Hebrew fingerprint)
    density_deviation   NUMERIC(7,4),
    morpheme_deviation  NUMERIC(7,4),
    sonority_deviation  NUMERIC(7,4),
    compression_deviation NUMERIC(7,4),
    composite_deviation NUMERIC(7,4),     -- weighted mean
    -- Breath alignment
    stress_alignment    NUMERIC(5,4),     -- 0–1, higher = better aligned
    weight_match        NUMERIC(5,4),     -- 0–1
    breath_alignment    NUMERIC(5,4),     -- composite: 60% stress + 40% weight
    -- Computed
    scored_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (verse_id, translation_key)
);

-- ───────────────────────────────────────────────────────────────
-- STAGE 5: LLM suggestions
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS suggestions (
    suggestion_id       SERIAL PRIMARY KEY,
    verse_id            INTEGER NOT NULL REFERENCES verses(verse_id),
    translation_key     TEXT NOT NULL,    -- which translation this improves
    suggested_text      TEXT NOT NULL,
    -- Scores (computed same as translation_scores after generation)
    composite_deviation NUMERIC(7,4),
    breath_alignment    NUMERIC(5,4),
    improvement_delta   NUMERIC(7,4),     -- vs original translation score
    -- Provenance
    llm_provider        TEXT NOT NULL,
    llm_model           TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    generated_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- STAGE 7: Pipeline run log
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP,
    status          TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running','ok','error')),
    stages_run      TEXT[],               -- e.g. ['ingest','fingerprint',...]
    error_message   TEXT,
    row_counts      JSONB                 -- {verses: 2527, tokens: 43000, ...}
);

-- ───────────────────────────────────────────────────────────────
-- INDICES
-- ───────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_verses_book_chapter    ON verses(book_num, chapter);
CREATE INDEX IF NOT EXISTS idx_translations_verse     ON translations(verse_id);
CREATE INDEX IF NOT EXISTS idx_translations_key       ON translations(translation_key);
CREATE INDEX IF NOT EXISTS idx_word_tokens_verse      ON word_tokens(verse_id);
CREATE INDEX IF NOT EXISTS idx_syllable_tokens_verse  ON syllable_tokens(verse_id);
CREATE INDEX IF NOT EXISTS idx_syllable_tokens_token  ON syllable_tokens(token_id);
CREATE INDEX IF NOT EXISTS idx_translation_scores_verse ON translation_scores(verse_id);
CREATE INDEX IF NOT EXISTS idx_translation_scores_key   ON translation_scores(translation_key);
CREATE INDEX IF NOT EXISTS idx_chiasm_candidates_start  ON chiasm_candidates(verse_id_start);

-- ───────────────────────────────────────────────────────────────
-- SEED DATA
-- ───────────────────────────────────────────────────────────────

INSERT INTO books (book_num, book_name, testament, language) VALUES
  (1,  'Genesis',        'OT', 'hebrew'),
  (2,  'Exodus',         'OT', 'hebrew'),
  (19, 'Psalms',         'OT', 'hebrew'),
  (23, 'Isaiah',         'OT', 'hebrew'),
  (18, 'Job',            'OT', 'hebrew'),
  (25, 'Lamentations',   'OT', 'hebrew')
ON CONFLICT (book_num) DO NOTHING;
```

---

## Step 8 — File: `pipeline/config.yml` (baseline)

```yaml
# ═══════════════════════════════════════════════════════════════
# Psalms NLP Pipeline — Master Configuration
# All paths are container-internal (Linux). Host paths only in
# docker-compose.yml volume mounts.
# ═══════════════════════════════════════════════════════════════

pipeline:
  name: psalms_nlp
  version: "0.1.0"
  # Stages to run when executing run.py with no arguments
  stages:
    - ingest
    - fingerprint
    - breath
    - chiasm          # second pass — requires breath to complete first
    - score
    - suggest
    - export
  on_error: stop      # stop | warn_continue

corpus:
  books:
    - book_num: 19
      name: Psalms
  # Set to true to restrict to specific chapters for development
  debug_chapters: []  # e.g. [23] to run only Psalm 23

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
  similarity_threshold: 0.75   # cosine similarity floor for colon match
  min_confidence: 0.65         # minimum pattern confidence to store
  max_stanza_verses: 8         # do not search patterns spanning > N verses

scoring:
  batch_size: 100
  conflict_mode: skip
  deviation_weights:
    density:     0.35
    morpheme:    0.25
    sonority:    0.20
    compression: 0.20
  breath_alignment_weights:
    stress:  0.60
    weight:  0.40

llm:
  provider: none   # none | anthropic | openai | gemini | openrouter | ollama
  model: ""
  max_tokens: 256
  temperature: 0.3
  # Verses filtered in for suggestion generation
  suggestion_filter:
    min_composite_deviation: 0.15
    max_suggestions_per_verse: 3

export:
  output_dir: /data/outputs
  report_dir: /data/outputs/report
  pdf_path:   /data/outputs/report.pdf
  typst_version: "0.12.0"
```

---

## Step 9 — Initialize the Database

```bash
# Start core services (db + jupyter + streamlit)
cd C:\psalms-nlp
docker compose up -d

# Wait for db to be healthy, then run schema
docker exec psalms_db psql -U psalms -d psalms -f /pipeline/init_schema.sql
```

Expected output ends with:
```
CREATE INDEX
INSERT 0 6
```

---

## Step 10 — Validation Checklist

Run each command and verify expected output:

```bash
# 1. All non-pipeline services healthy
docker compose ps
# Expected: db (healthy), jupyter (running), streamlit (running)

# 2. PostgreSQL accepting connections
docker exec psalms_db pg_isready -U psalms -d psalms
# Expected: /var/run/postgresql:5432 - accepting connections

# 3. pgvector extension present
docker exec psalms_db psql -U psalms -d psalms -c "\dx pgvector"
# Expected: pgvector | 0.7.x | ...

# 4. All tables created
docker exec psalms_db psql -U psalms -d psalms -c "\dt"
# Expected: 10 tables listed

# 5. Books seed data present
docker exec psalms_db psql -U psalms -d psalms -c "SELECT book_num, book_name FROM books ORDER BY book_num;"
# Expected: 6 rows

# 6. JupyterLab accessible
curl -s -o /dev/null -w "%{http_code}" http://localhost:8888
# Expected: 200 or 302

# 7. Streamlit placeholder accessible
curl -s -o /dev/null -w "%{http_code}" http://localhost:8501
# Expected: 200

# 8. Pipeline container builds without error
docker compose --profile pipeline build pipeline
# Expected: exits 0, no error lines
```

---

## Acceptance Criteria

All of the following must be true before Stage 0 is considered complete:

- [ ] `docker compose ps` shows `db`, `jupyter`, `streamlit` all running with no restart loops
- [ ] `\dt` in PostgreSQL returns exactly the 10 expected tables: `books`, `verses`, `translations`, `word_tokens`, `verse_fingerprints`, `chiasm_candidates`, `syllable_tokens`, `breath_profiles`, `translation_scores`, `suggestions`, `pipeline_runs`
- [ ] `books` table contains 6 seed rows
- [ ] pgvector extension is active (`\dx` confirms)
- [ ] JupyterLab UI reachable at http://localhost:8888
- [ ] Streamlit placeholder reachable at http://localhost:8501
- [ ] Pipeline container builds successfully under `--profile pipeline`
- [ ] No credentials appear in any committed file (all from environment variables or defaults)

---

## Troubleshooting

**`docker compose up` fails with port conflict:** Another service is using 5432, 8888, or 8501. Change the left side of the port mapping in `docker-compose.yml` (e.g., `"5433:5432"`).

**`init_schema.sql` reports "already exists":** The schema was partially run. This is safe — all `CREATE TABLE IF NOT EXISTS` statements are idempotent. Re-run the script; it will only create what is missing.

**Streamlit fails to build:** Check that `streamlit/Dockerfile.streamlit` and `streamlit/requirements_streamlit.txt` exist. The `streamlit/app.py` placeholder must also exist.

**Pipeline build fails on Typst download:** The build requires internet access at image-build time. If the machine is air-gapped after initial setup, download the Typst binary to a local path and `COPY` it into the Dockerfile instead of fetching from GitHub.
