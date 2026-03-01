# Psalms NLP Analysis Pipeline

A self-hosted, locally-run pipeline that ingests morphologically-tagged Biblical Hebrew text,
computes multi-dimensional style and phonetic fingerprints, scores existing English translations
against those fingerprints, and generates constrained translation suggestions informed by
quantified linguistic data.

---

## What It Does

1. **Ingest** — Loads BHSA morphological data for all 150 Psalms (2,527 verses) and selected
   English translations from public-domain sources.
2. **Fingerprint** — Computes per-verse style fingerprints from morphological features
   (syntactic depth, lexical density, clause structure, word-order patterns).
3. **Breath & phonetics** — Profiles syllable structure, stress patterns, and pause markers
   from the Hebrew cantillation system.
4. **Score** — Measures how well each English translation preserves the Hebrew fingerprint
   across style and phonetic dimensions.
5. **Suggest** — Optionally queries an LLM (local or cloud) for constrained translation
   suggestions guided by the quantified fingerprint data.
6. **Report** — Publishes results as an interactive Streamlit explorer, a Sphinx HTML report
   site, and a Typst-rendered archival PDF.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE DATA                                                     │
│  BHSA Hebrew Bible · Translation files (KJV, ASV, YLT, WEB,   │
│  ULT, UST + extensible adapter for any format)                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│  PIPELINE  /pipeline/run.py  (Docker — python:3.11-slim)        │
│  ingest → fingerprint → breath → score → suggest → export      │
└───────────┬─────────────────────────────────┬───────────────────┘
            │                                 │
┌───────────▼───────────┐     ┌───────────────▼───────────────────┐
│  PostgreSQL + pgvector │     │  LLM Adapter (optional)           │
│  psalms_db container  │     │  anthropic | openai | gemini      │
└───────────┬───────────┘     │  openrouter | ollama | none       │
            │                 └───────────────────────────────────┘
┌───────────▼───────────────────────────────────────────────────┐
│  VISUALIZATION & PUBLICATION                                   │
│  JupyterLab (analysis) · Sphinx/myst-nb (HTML report site)    │
│  Typst (PDF archival) · Streamlit (interactive exploration)   │
└───────────────────────────────────────────────────────────────┘
```

---

## Pipeline Stages

| Stage | Title | Primary Output |
|-------|-------|----------------|
| 0 | Foundation & Infrastructure | Docker stack running, full schema initialized |
| 1 | Data Acquisition & Configuration | BHSA downloaded, translations loaded, `config.yml` complete |
| 2 | Morphology & Fingerprinting | 2,527 verse rows · ~43,000 token rows · 2,527 fingerprints |
| 3 | Breath & Phonetic Analysis | ~120,000 syllable tokens · 2,527 breath profiles |
| 4 | Translation Scoring | All translations scored across style + breath metrics |
| 5 | LLM Integration & Suggestions | Optional suggestions stored in DB, queryable from JupyterLab |
| 6 | Visualization & Reporting | HTML report site · archival PDF |
| 7 | Pipeline Orchestration | Hands-off automated runner, exits 0, log written |
| 8 | Corpus Expansion | Isaiah output validated as first expansion target |

---

## Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11 (slim Docker image) |
| Database | PostgreSQL 15 + pgvector |
| Orchestration | Docker Compose |
| Hebrew data | BHSA via text-fabric (~200 MB) |
| Visualization | Plotly · Sphinx + myst-nb · Typst · Streamlit |
| LLM | Provider-agnostic adapter (anthropic / openai / gemini / openrouter / ollama / none) |
| Package manager | `uv` |

---

## Prerequisites

- **Windows host:** WSL2 + Docker Desktop with WSL2 backend
- **macOS / Linux host:** Docker Engine + Docker Compose v2
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) — for running tests and dev tools locally
- ~4 GB disk space (BHSA corpus + translation files + PostgreSQL data)
- Optional: API key for a cloud LLM provider (the pipeline runs fully without one)

> **Windows users:** Run `check-env.ps1` to verify all prerequisites before starting.

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/jschell/OT-NLP.git
cd OT-NLP

# 2. Copy and edit the environment file
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD and any optional LLM API keys

# 3. Start persistent services (database + JupyterLab + Streamlit)
docker compose up -d

# 4. Run the full pipeline
docker compose run --rm pipeline python run.py

# 5. Open JupyterLab
#    http://localhost:8888

# 6. Open Streamlit explorer
#    http://localhost:8501
```

---

## Translation Sources

### Tier 1 — Programmatically Ingestible (Public Domain / CC-BY)

| ID | Name | Date | Character |
|----|------|------|-----------|
| KJV | King James Version | 1611 | Victorian literary |
| ASV | American Standard Version | 1901 | Formal equivalence |
| YLT | Young's Literal Translation | 1862 | Maximally literal |
| WEB | World English Bible | 2000 | Modern public domain |
| NHEB | New Heart English Bible | 2008 | Modern public domain |
| DBY | Darby Translation | 1890 | Literal, analytical |
| ULT | unfoldingWord Literal Text | 2022 | Linguistically literal (CC-BY) |
| UST | unfoldingWord Simplified Text | 2022 | Dynamic equivalence (CC-BY) |
| NET | New English Translation | 2005 | Functional equivalence |

**Recommended initial set:** KJV · YLT · WEB · ULT · UST — spanning the full literal-to-dynamic spectrum.

### Tier 2 — Licensed (Manual Reference Only)

NIV, ESV, NASB, NLT, The Message, Robert Alter's *The Hebrew Bible* — cannot be ingested
programmatically without a commercial license.

### Adding a Translation

Adding any new Tier 1 translation requires only a `config.yml` entry — no code changes:

```yaml
translations:
  sources:
    - id:     WEB
      format: sqlite_scrollmapper
      path:   /data/translations/web.db
```

---

## Project Structure

```
OT-NLP/
├── pipeline/
│   ├── run.py                    # Top-level orchestrator
│   ├── config.yml                # Pipeline configuration
│   ├── init_schema.sql           # Full DB schema (schema-first)
│   ├── validate_data.py          # Data validation helpers
│   ├── validate_infrastructure.py
│   ├── Dockerfile.pipeline
│   ├── modules/                  # One module per stage
│   │   ├── ingest.py
│   │   ├── ingest_translations.py
│   │   ├── fingerprint.py
│   │   ├── breath.py
│   │   ├── chiasm.py
│   │   ├── score.py
│   │   ├── suggest.py
│   │   ├── export.py
│   │   └── logger.py
│   ├── adapters/                 # Translation + LLM adapters
│   │   ├── db_adapter.py
│   │   ├── llm_adapter.py
│   │   ├── phoneme_adapter.py
│   │   └── translation_adapter.py
│   └── visualize/                # Streamlit app + report generator
│       ├── arcs.py
│       ├── breath_curves.py
│       ├── heatmaps.py
│       ├── radar.py
│       └── report.py
├── streamlit/
│   ├── app.py
│   └── Dockerfile.streamlit
├── data/
│   ├── bhsa/                     # BHSA Hebrew corpus (downloaded at runtime)
│   ├── translations/             # SQLite translation files
│   └── outputs/                  # Generated reports, figures, notebooks
├── tests/                        # pytest suite (one file per module)
├── docs/                         # Stage design documents
│   ├── psalms_nlp_highlevel_plan.md
│   ├── stage_00_foundation.md
│   └── stage_01_data_acquisition.md … stage_08_corpus_expansion.md
├── scripts/
│   └── download_data.ps1         # Data download helper (Windows)
├── .claude/
│   ├── CLAUDE.md                 # Claude Code project instructions
│   └── skills/                   # 13 autonomous-work skills
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── .pre-commit-config.yaml
├── check-env.ps1                 # Windows prerequisite checker
└── AGENTS.md                     # Agent instructions (Cursor/Windsurf/Copilot)
```

---

## Development

```bash
# Run all tests
uv run --frozen pytest

# Run specific module tests
uv run --frozen pytest tests/test_<module>.py -v

# Lint
uv run --frozen ruff check .

# Fix lint issues automatically
uv run --frozen ruff check . --fix

# Format
uv run --frozen ruff format .

# Type check
uv run --frozen pyright
```

All pipeline stages follow a uniform module interface:

```python
def run(conn: psycopg2.Connection, config: dict) -> dict:
    """Run this stage. Returns {"rows_written": int, "elapsed_s": float}."""
    ...
```

---

## Design Principles

- **Fully local and offline** after initial data download — no cloud dependencies for core analysis
- **Schema-first** — the complete database schema (`init_schema.sql`) is written before any data
- **Resumable** — every stage queries which verses lack target rows before processing
- **Config-driven** — adding a translation or expanding the corpus is a `config.yml` change only
- **Credentials via environment variables** — never in source files

---

## Status

| Item | Status |
|------|--------|
| High-level plan | Complete |
| Stage design docs (0–8) | Complete |
| Docker configuration | Complete |
| Database schema | Complete |
| Pipeline code (Stages 0–7) | Complete |
| Streamlit explorer | Complete |
| Tests | Complete (24 test files) |
| Corpus expansion (Stage 8) | In progress |

---

## License

MIT — see [LICENSE](LICENSE).
