# Psalms NLP Analysis Pipeline
## High-Level Implementation Plan

> **Purpose:** This document defines the full scope of the project across all implementation stages. Each stage is a discrete, reviewable deliverable that builds on the previous. Detailed implementation plans for each stage will be produced after this document is approved.

---

## Project Summary

A self-hosted, locally-run pipeline that ingests morphologically-tagged Biblical Hebrew text, computes multi-dimensional style and phonetic fingerprints, scores existing English translations against those fingerprints, and generates constrained translation suggestions informed by quantified linguistic data.

**Core design principles:**
- Fully local and offline after initial setup
- LLM integration optional and provider-agnostic at every stage
- Modular — each stage is independently runnable and resumable
- No cloud dependencies for core analysis
- Schema-first — database structure defined before any data is written
- Path-portable — host OS paths isolated to `docker-compose.yml` volume mounts only; all pipeline code uses container-internal Linux paths exclusively

---

## Path Convention

All paths in this document follow a two-layer convention that keeps the implementation OS-agnostic while providing concrete Windows guidance for initial setup.

**Host paths (Windows)** appear only in `docker-compose.yml` volume mount definitions. These use Windows syntax and are the only place the host OS matters:

```
C:\psalms-nlp\
  docker-compose.yml
  pipeline\
  data\
    bhsa\
    translations\
    outputs\
      report\       ← Sphinx HTML site written here each run
      report.pdf    ← Typst PDF written here each run
  notebooks\
  streamlit\        ← Streamlit explorer app
```

**Container paths (Linux)** are used everywhere else — in all Python code, config files, Dockerfiles, and documentation. These are identical regardless of whether the host is Windows, macOS, or Linux:

```
/pipeline/
/data/
  bhsa/
  translations/
  outputs/
/home/jovyan/work/
/home/jovyan/data/
```

The volume mount block in `docker-compose.yml` is the only translation layer between the two:

```yaml
volumes:
  - C:\psalms-nlp\pipeline:/pipeline
  - C:\psalms-nlp\data:/data
  - C:\psalms-nlp\notebooks:/home/jovyan/work
  - C:\psalms-nlp\data:/home/jovyan/data
```

On macOS or Linux, only these volume mount lines change — replace `C:\psalms-nlp\` with the equivalent host path. No other files require modification.

---

## System Overview

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

## Translation Sources

Translation sourcing has licensing implications that affect what can be ingested automatically, what must be referenced manually, and how the loader is designed.

### Tier 1 — Free, Programmatically Ingestible

These translations are public domain or openly licensed and can be downloaded and ingested without restriction.

| ID | Name | Date | Character | Format | Source |
|---|---|---|---|---|---|
| KJV | King James Version | 1611 | Victorian literary | SQLite | scrollmapper/bible_databases |
| ASV | American Standard Version | 1901 | Formal equivalence | SQLite | scrollmapper/bible_databases |
| YLT | Young's Literal Translation | 1862 | Maximally literal | SQLite | scrollmapper/bible_databases |
| WEB | World English Bible | 2000 | Modern public domain | SQLite | scrollmapper/bible_databases |
| DBY | Darby Translation | 1890 | Literal, analytical | SQLite | scrollmapper/bible_databases |
| ULT | unfoldingWord Literal Text | 2022 | Linguistically literal (CC-BY) | USFM | github.com/unfoldingWord/en_ult |
| UST | unfoldingWord Simplified Text | 2022 | Dynamic equivalence (CC-BY) | USFM | github.com/unfoldingWord/en_ust |
| NET | New English Translation | 2005 | Functional equivalence | USFM | github.com/BibleOrg/eBible |

**Recommended initial set:** KJV, YLT, WEB, ULT, UST. This spans the full literal-to-dynamic spectrum and includes both a Victorian benchmark and two modern options — one linguistically literal, one readable paraphrase.

### Tier 2 — Licensed (Manual Reference Only)

These translations cannot be ingested programmatically without a commercial license. They serve as manual validation references only.

| Name | Publisher | Notes |
|---|---|---|
| NIV | Zondervan / HarperCollins | Restricted API |
| ESV | Crossway | Free non-commercial API available (see note) |
| NASB | Lockman Foundation | No free API |
| NLT | Tyndale House | No free API |
| Robert Alter (Hebrew Bible) | W.W. Norton | Gold standard for style-aware translation; manual reference only |
| The Message | NavPress | Paraphrase; no API |

> **ESV note:** Crossway provides a free API for non-commercial use at `api.esv.org`. This is architecturally supportable as an optional API-format adapter that queries at score time rather than ingesting the full corpus. This introduces a runtime network dependency and requires a free registration key. Treated as a deferred optional extension, not a default.

### Adding New Translations

The pipeline uses a translation adapter pattern. Any translation can be added by one of three methods:

**Method 1 — SQLite (scrollmapper format):** Add one entry to `config.yml`. No code changes.
```yaml
translations:
  sources:
    - id:     WEB
      format: sqlite_scrollmapper
      path:   /data/translations/web.db
```
The scrollmapper schema is a single table: `CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)` where b=book, c=chapter, v=verse, t=text. Book 19 = Psalms.

**Method 2 — USFM (unfoldingWord / eBible format):** Add one entry to `config.yml` pointing to the directory of USFM files. The USFM parser normalizes to internal schema.
```yaml
translations:
  sources:
    - id:     ULT
      format: usfm
      path:   /data/translations/ult/
      book_map: usfm_standard
```

**Method 3 — API adapter:** For licensed translations with API access. Key loaded from environment variable.
```yaml
translations:
  sources:
    - id:         ESV
      format:     api
      provider:   esv
      api_key_env: ESV_API_KEY
      rate_limit: 500
```

**Method 4 — Custom format:** Implement one class in `adapters/translation_adapter.py` with a single method:
```python
def get_verse(self, book_num: int, chapter: int, verse: int) -> str | None:
    ...
```
Register the class in the adapter loader map. No other changes needed.

### Translation Download Locations

```
# scrollmapper SQLite files
https://github.com/scrollmapper/bible_databases/raw/master/sqlite/t_kjv.db
https://github.com/scrollmapper/bible_databases/raw/master/sqlite/t_asv.db
https://github.com/scrollmapper/bible_databases/raw/master/sqlite/t_ylt.db
https://github.com/scrollmapper/bible_databases/raw/master/sqlite/t_web.db
https://github.com/scrollmapper/bible_databases/raw/master/sqlite/t_dby.db

# unfoldingWord USFM repositories
https://github.com/unfoldingWord/en_ult
https://github.com/unfoldingWord/en_ust

# NET Bible via eBible corpus
https://github.com/BibleOrg/eBible  (navigate to eng-netfree)
```

---

## Stage Overview

| Stage | Title | Depends On | Primary Output |
|---|---|---|---|
| 0 | Foundation & Infrastructure | Nothing | Docker stack running, full schema initialized |
| 1 | Data Acquisition & Configuration | Stage 0 | Source data downloaded, `config.yml` complete, translation loader tested |
| 2 | Core Pipeline — Morphology & Fingerprinting | Stage 1 | `verses`, `word_tokens`, `verse_fingerprints` populated |
| 3 | Breath & Phonetic Analysis | Stage 2 | `syllable_tokens`, `breath_profiles` populated |
| 4 | Translation Scoring | Stage 3 | `translation_scores` with style + breath metrics |
| 5 | LLM Integration & Suggestions | Stage 4 | Optional suggestion generation, provider-agnostic |
| 6 | Visualization & Reporting | Stage 4 | Sphinx HTML report site, Typst PDF, Streamlit explorer, Plotly charts |
| 7 | Pipeline Orchestration | Stages 2–6 | Hands-off automated pipeline runner |
| 8 | Corpus Expansion | Stage 7 | Extended coverage beyond Psalms |

---

## Stage Descriptions

---

### Stage 0 — Foundation & Infrastructure

**Goal:** A running, validated local Docker environment on the Intel NUC with all base services healthy.

**Scope:**
- Enable WSL2 on Windows host
- Install and configure Docker Desktop with WSL2 backend
- Create project directory structure on Windows host under `C:\psalms-nlp\`
- Author `docker-compose.yml` with Windows volume mount paths for PostgreSQL (pgvector), JupyterLab, Streamlit, and pipeline runner services
- Write and execute `init_schema.sql` — complete database schema for all stages, run once at setup
- Validate connectivity between all containers

**Key decisions:**
- PostgreSQL as single storage layer — pgvector covers future semantic vectors without a separate vector database
- Pipeline runner uses `profiles: pipeline` — does not start on routine `docker compose up -d`
- All credentials as environment variables in `docker-compose.yml`, never in source files
- Windows host paths appear only in `docker-compose.yml` volume mounts; all other files use Linux container paths

**Host directory structure (Windows):**
```
C:\psalms-nlp\
  docker-compose.yml
  pipeline\
    config.yml
    run.py
    requirements.txt
    Dockerfile.pipeline
    modules\
    adapters\
    visualize\
  data\
    bhsa\
    translations\
    outputs\
      report\       ← Sphinx HTML site written here each run
      report.pdf    ← Typst PDF written here each run
  notebooks\
  streamlit\        ← Streamlit explorer app
    app.py
```

**Deliverables:**
- `docker-compose.yml` with Windows volume mount paths and all services defined (PostgreSQL, JupyterLab, Streamlit, pipeline runner)
- `init_schema.sql` — complete schema for all stages
- Checkpoint: all containers healthy, all tables created, cross-container connectivity verified

---

### Stage 1 — Data Acquisition & Configuration

**Goal:** All source data downloaded and accessible inside containers, translation adapter tested against all configured sources, `config.yml` complete.

**Scope:**
- Download BHSA dataset via text-fabric (~200 MB) to `C:\psalms-nlp\data\bhsa\` → `/data/bhsa/`
- Download Tier 1 translation corpus to `C:\psalms-nlp\data\translations\` → `/data/translations/`
  - KJV, ASV, YLT, WEB via scrollmapper (SQLite)
  - ULT, UST via unfoldingWord GitHub (USFM)
- Implement `adapters/translation_adapter.py` with SQLite and USFM loaders and adapter registry
- Author `config.yml` as single source of truth
- Install all Python dependencies in pipeline container via `requirements.txt`
- Validate: each configured translation returns correct text for Psalm 23:1

**Key decisions:**
- **Single database:** PostgreSQL is the only persistent store. The SQLite files distributed by scrollmapper and the USFM directories from unfoldingWord are source formats only — each is read once during this stage, the verse text extracted, and the result written into the `translations` table in PostgreSQL. After ingest completes, the source files are inert downloads. No stage after Stage 1 queries SQLite or USFM directly.
- Adapter pattern established here — all future translations added via `config.yml`, not code
- USFM parser normalizes unfoldingWord format to internal schema at load time
- Data validation confirms expected verse counts against known values before pipeline runs

**Deliverables:**
- `adapters/translation_adapter.py` with SQLite, USFM, and API adapter classes
- `config.yml` with full schema and inline documentation
- Data validation script
- `requirements.txt`

---

### Stage 2 — Core Pipeline: Morphology & Fingerprinting

**Goal:** Every verse in the configured corpus has morphological token records, a 4-dimensional style fingerprint, colon-level fingerprints, and chiastic structure annotations in the database.

**Scope:**
- `modules/ingest.py` — BHSA extraction to `verses` and `word_tokens`
- `modules/fingerprint.py` — syllable density, morpheme ratio, sonority score, clause compression at verse level; colon-level fingerprint vectors derived from Stage 3 colon boundaries (populated in a second pass after Stage 3)
- `modules/chiasm.py` — ABBA and ABCBA pattern detection across colon sequences within a stanza
  - Colon similarity: cosine distance between colon fingerprint vectors, configurable threshold in `config.yml`
  - ABBA: colons 1 and 4 match, colons 2 and 3 match, within a four-colon unit
  - ABCBA: colons 1 and 5 match, 2 and 4 match, colon 3 is the pivot, within a five-colon unit
  - Stores match pairs, pattern type, and confidence score in `chiasm_candidates` table
  - All candidates flagged for interpretive review — not asserted as findings
- `adapters/db_adapter.py` — upsert/skip/rebuild, batch commits, resumability
- Hebrew syllable counter with Unicode vowel point parsing
- All modules: `run(conn, config) -> dict` interface

**Key decisions:**
- Resumability via `verse_ids_for_stage()` — queries which verses lack target table rows
- Batch size and conflict mode configurable in `config.yml`
- Colon-level fingerprints depend on colon boundary data from Stage 3; `modules/chiasm.py` runs as a second pass after Stage 3 completes — `run.py` sequencing handles this
- Chiasm detection intentionally conservative: outputs candidates with confidence scores, not assertions; threshold tunable without code changes

**Deliverables:**
- `modules/ingest.py`, `modules/fingerprint.py`, `adapters/db_adapter.py`
- `modules/chiasm.py` with ABBA and ABCBA detectors
- `chiasm_candidates` table in schema (added to `init_schema.sql`)
- Unit tests for syllable counter, fingerprint calculator, and chiasm matcher
- Validated: 2,527 verse rows, ~43,000 token rows, 2,527 fingerprint rows (Psalms)

---

### Stage 3 — Breath & Phonetic Analysis

**Goal:** Every verse has syllable-level phonetic annotation and a verse-level breath profile.

**Scope:**
- `modules/breath.py` — Hebrew syllable parser, vowel openness scoring, consonant sonority classification, Masoretic accent detection
- Composite breath weight per syllable: vowel openness (40%), vowel length (25%), syllable openness (20%), onset class (15%)
- Verse-level profile: mean weight, open ratio, guttural density, colon boundaries, stress positions, full breath curve
- Populates `syllable_tokens` and `breath_profiles`

**Key decisions:**
- Colon boundaries from disjunctive accent positions — no manual annotation
- Stage independently re-runnable without affecting ingest or fingerprint data

**Deliverables:**
- `modules/breath.py` with vowel map and consonant classification constants
- Unit tests for syllable parser
- Validated: ~120,000 syllable token rows, 2,527 breath profiles (Psalms)

---

### Stage 4 — Translation Scoring

**Goal:** Every verse × translation pair has quantified style deviation and breath alignment scores.

**Scope:**
- `modules/score.py` — English phoneme parsing, style deviation, breath alignment scoring
- `adapters/phoneme_adapter.py` — CMU Pronouncing Dictionary lookup with heuristic fallback
- Stress alignment: normalized peak position comparison (0–1 scale)
- Composite breath alignment: stress alignment (60%) + weight match (40%)
- Scores all translations in `config.yml` — no hardcoded translation IDs
- Extends `translation_scores` with breath columns

**Key decisions:**
- Normalized position scale handles differing word counts between Hebrew and English
- No external API calls — CMU dictionary is bundled via `pronouncing` library

**Deliverables:**
- `modules/score.py`, `adapters/phoneme_adapter.py`
- Unit tests for stress alignment scorer
- Validated: all configured translations scored with all columns populated

---

### Stage 5 — LLM Integration & Suggestions

**Goal:** Provider-agnostic LLM hook for constrained suggestion generation, with full graceful degradation when no provider is configured.

**Scope:**
- `adapters/llm_adapter.py` — `ask(prompt, max_tokens) -> str` covering anthropic, openai, gemini, openrouter, ollama, none
- `modules/suggest.py` — breath-aware prompt builder, suggestion storage
- Filtering via configurable thresholds in `config.yml`
- Ollama service as optional commented block in `docker-compose.yml`

**Key decisions:**
- Provider via environment variables in `docker-compose.yml` — no code changes to switch
- Suggestions stored in database, not ephemeral — queryable from JupyterLab or Streamlit explorer

**Deliverables:**
- `adapters/llm_adapter.py`, `modules/suggest.py`
- Suggestion storage schema extension
- Provider switching documentation

---

### Stage 6 — Visualization & Reporting

**Goal:** Auto-generated HTML report site and PDF rebuilt after every pipeline run, plus a persistent Streamlit explorer for interactive analysis. No Node.js or external toolchain beyond a single pinned Typst binary.

**Tool rationale:**
- **Sphinx + myst-nb** — pure Python (`pip install sphinx myst-nb sphinx-book-theme sphinxcontrib-bibtex`), already present as a JupyterLab transitive dependency. Produces a navigable multi-page HTML site with cross-references, citations, and numbered figures from the same `.ipynb` notebooks used in JupyterLab. Version-pinned entirely in `requirements.txt` alongside all other Python packages. No separate toolchain.
- **Typst** — single statically-linked Rust binary (~10 MB) pinned via a one-line Dockerfile `ARG`. Produces PDF from `.typ` source files without a TeX distribution. Appropriate for self-hosted archival and Zenodo deposit. Fast (sub-second compile for a document of this size). Not yet accepted for formal journal submission — LaTeX export from Sphinx handles that path if needed.
- **Streamlit** — Python library added to `requirements.txt`. Wraps existing Plotly figures from `visualize/` into a persistent web app with a Psalm selector. No new framework or component architecture; `app.py` is a thin caller of existing visualization modules.
- **JupyterLab Plotly** — all specialized chart types (breath curves, arc diagrams, radar charts) developed here first; both Sphinx documents and the Streamlit app consume them from `visualize/` modules.

**Scope:**

*Sphinx HTML report site (auto-generated per pipeline run):*
- `docs/conf.py` — Sphinx configuration with myst-nb extension and book theme
- `docs/index.md` — table of contents linking to notebook chapters
- `docs/analysis.ipynb` — primary analysis notebook (frozen outputs, not re-executed at build time)
- Stage summary, row counts, timing, worst-performing verses by combined deviation score, Psalm-level summary table, embedded Plotly figures as static HTML
- Built by `modules/export.py` calling `sphinx-build` via subprocess; output written to `/data/outputs/report/`

*Typst PDF (auto-generated per pipeline run):*
- `docs/report.typ` — Typst source referencing pre-generated PNG/SVG chart exports
- Charts exported as static images by `visualize/` modules using `kaleido` (Plotly static export)
- PDF written to `/data/outputs/report.pdf`
- `TYPST_VERSION` pinned as `ARG` in `Dockerfile.pipeline`

*Streamlit explorer (persistent service):*
- `streamlit/app.py` — Psalm selector driving live Plotly charts from PostgreSQL
- Breath curve overlay: Hebrew vs. translations vs. suggestions
- Translation comparison table for selected verse range
- Suggestion browser with score breakdown
- Chiasm viewer: arc diagram for selected Psalm showing candidate ABBA/ABCBA patterns with confidence scores
- Runs as a separate Docker service on a fixed port; accesses same PostgreSQL container

*JupyterLab Plotly (interactive development):*
- Syllable openness heatmap
- Stress alignment comparison
- Arc diagram for parallelism, root repetition, and chiastic structure — arcs connect matched colon pairs with color encoding for pattern type (ABBA vs ABCBA) and line weight encoding for confidence score
- Radar chart for style fingerprint comparison
- All charts developed in `visualize/` as importable functions; called by both Sphinx notebooks and Streamlit

**Freeze pattern:**
Sphinx renders the notebook with frozen outputs — the pipeline executes notebooks via `jupyter nbconvert --execute` at export time, saves the executed `.ipynb` with outputs, then `sphinx-build` reads those static outputs without re-executing. This decouples rendering from live database access and keeps build time under ten seconds.

**Files:**
- `visualize/breath_curves.py`, `visualize/heatmaps.py`, `visualize/arcs.py`, `visualize/radar.py`, `visualize/report.py`
- `modules/export.py` — orchestrates nbconvert execute → sphinx-build → typst compile
- `docs/conf.py`, `docs/index.md`, `docs/analysis.ipynb`
- `docs/report.typ`
- `streamlit/app.py`
- Typst binary version pinned in `Dockerfile.pipeline` via `ARG TYPST_VERSION`

**Key decisions:**
- Sphinx + myst-nb pins entirely in `requirements.txt` — no separate toolchain version management
- Typst replaces LaTeX as PDF engine; single binary, zero dependency tree
- Streamlit is the only persistent visualization service beyond JupyterLab — no Grafana, no Dash, no Metabase
- `visualize/` modules are the single source of all chart logic; no duplication between Sphinx and Streamlit
- PDF is a secondary artifact suitable for archival; HTML site is the primary deliverable

**Deliverables:**
- `visualize/` module directory with all chart types
- `modules/export.py`
- `docs/` directory with Sphinx configuration and analysis notebook
- `streamlit/app.py`
- Sample HTML report site and PDF
- Updated `Dockerfile.pipeline` with pinned Typst binary

---

### Stage 7 — Pipeline Orchestration

**Goal:** Single-command hands-off runner with sequencing, failure recovery, resumability, structured logging, and scheduling.

**Scope:**
- `run.py` — stage sequencing, error handling, exit codes
- Structured logging to stdout and `/data/outputs/pipeline.log`
- Docker Compose `profiles: pipeline`
- WSL cron template for scheduled runs on Windows host
- Partial rebuild via `config.yml` stages list

**Windows scheduling via WSL cron:**
```bash
# WSL crontab — runs full pipeline every Sunday at 2am
0 2 * * 0 cd /mnt/c/psalms-nlp && docker compose --profile pipeline run --rm pipeline >> /mnt/c/psalms-nlp/data/outputs/cron.log 2>&1
```

**Key decisions:**
- Exit code 0 = ok, 1 = error — enables scripted monitoring
- `on_error: stop | warn_continue` configurable in `config.yml`
- All inter-stage state in database — no temp files between stages

**Deliverables:**
- `run.py`, `Dockerfile.pipeline`
- Updated `docker-compose.yml` with pipeline and optional Ollama services
- WSL cron configuration example
- Operations reference documentation

---

### Stage 8 — Corpus Expansion

**Goal:** Extend to additional Biblical books with genre-aware baseline fingerprints.

**Scope:**
- Genre cluster configuration in `config.yml`: `hebrew_poetry`, `hebrew_prophecy`, `hebrew_narrative`, `hebrew_law`, `late_hebrew`, `aramaic_sections`
- Genre baseline fingerprints stored as reference rows in database
- Aramaic section flagging (Daniel 2–7, Ezra 4–7)
- Acrostic poetry flagging (Psalm 119, Lamentations 1–4)
- Priority expansion: Isaiah, Job, Lamentations

**Key decisions:**
- Corpus expansion is a `config.yml` change only — no code changes
- Aramaic and acrostic flags pre-provisioned as columns in `verses` table from Stage 0
- Greek NT and Akkadian tracks remain deferred

**Deliverables:**
- Genre cluster baseline calculator
- `config.yml` expansion documentation
- Validated Isaiah output as first expansion target

---

## Dependency Map

```
Stage 0 ──► Stage 1 ──► Stage 2 ──► Stage 3 ──► Stage 4
                                                      │
                                          ┌───────────┤
                                          │           │
                                       Stage 5    Stage 6
                                          │           │
                                          └─────┬─────┘
                                                │
                                            Stage 7
                                           (wraps 2–6)
                                                │
                                            Stage 8
                                         (config only)
```

---

## Deferred Items

- **Greek NT track** — requires MorphGNT adapter and Koine Greek phoneme mapping
- **Akkadian/Gilgamesh track** — eBL API adapter; lower data maturity
- **Semantic vectors** — pgvector infrastructure ready; sentence-transformers embedding generation deferred
- **NetworkX graphs and UMAP clustering** — deferred pending semantic vectors
- **ESV API adapter** — optional Tier 2 extension, not default
- **Web UI** — all visualization via Streamlit, JupyterLab, Sphinx HTML reports, and Typst PDF; no custom application framework in scope
- **Phonosemantic annotation** — detection of words whose phonetic shape echoes the natural sound or acoustic character of their referent (e.g., sibilant+liquid clusters in words for serpent or wind; guttural-heavy roots in words for breath, crying, and animal exhalation). Requires two components not yet in scope: (1) a referential layer mapping root meanings to semantic categories (animal, sound-event, physical-action), sourced from a curated Hebrew lexicon or LLM root-meaning query; (2) a phoneme-to-natural-sound mapping (sibilants → hiss/wind/water, gutturals → breath/roar, labials → low animal sounds). The Stage 3 consonant classification and guttural density outputs are the necessary raw inputs. All candidate matches must be flagged for interpretive review — phonosemantics in Biblical Hebrew is a live scholarly debate, and automated detection produces candidates, not findings. Natural integration point is alongside or after semantic vectors.

---

## Document Status

| Document | Status |
|---|---|
| High-Level Plan (this document) | **Pending review** |
| Stage 0 — Foundation & Infrastructure | Not started |
| Stage 1 — Data Acquisition & Configuration | Not started |
| Stage 2 — Morphology & Fingerprinting | Not started |
| Stage 3 — Breath & Phonetic Analysis | Not started |
| Stage 4 — Translation Scoring | Not started |
| Stage 5 — LLM Integration & Suggestions | Not started |
| Stage 6 — Visualization & Reporting | Not started |
| Stage 7 — Pipeline Orchestration | Not started |
| Stage 8 — Corpus Expansion | Not started |

---

*Detailed implementation plans for each stage will be produced after this document is reviewed and approved. Each detailed plan will include: full file structure, complete code specifications, acceptance criteria, test cases, and agent/developer instructions.*
