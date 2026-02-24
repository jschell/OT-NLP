# Project: Psalms NLP Analysis Pipeline

A self-hosted, locally-run pipeline that ingests morphologically-tagged Biblical Hebrew text,
computes multi-dimensional style and phonetic fingerprints, scores existing English translations
against those fingerprints, and generates constrained translation suggestions.

---

## Stack

- **Language:** Python 3.11 (slim Docker image)
- **Database:** PostgreSQL + pgvector (`psalms_db` container)
- **Orchestration:** Docker Compose — pipeline runner, JupyterLab, Streamlit, PostgreSQL
- **Hebrew data:** BHSA via text-fabric (~200 MB)
- **Visualization:** Plotly, Sphinx + myst-nb, Typst, Streamlit
- **LLM:** Provider-agnostic adapter (anthropic | openai | gemini | openrouter | ollama | none)
- **Package manager:** `uv` — never `pip`

---

## Path Convention

**Container paths (Linux)** are used in ALL Python code, config files, Dockerfiles, and docs:

```
/pipeline/       ← all Python source
/data/
  bhsa/
  translations/
  outputs/
/home/jovyan/work/    ← JupyterLab notebooks
```

**Host paths (Windows)** appear ONLY in `docker-compose.yml` volume mount definitions.
No other file references the host OS path.

---

## Module Interface

Every pipeline stage module exports a single entry point:

```python
def run(conn: psycopg2.Connection, config: dict) -> dict:
    ...
```

- `conn` — live PostgreSQL connection
- `config` — full parsed `config.yml`
- Returns a dict with at minimum `{"rows_written": int, "elapsed_s": float}`

---

## Commands

```bash
# Run all tests
uv run --frozen pytest

# Run specific module tests
uv run --frozen pytest tests/test_<module>.py -v

# Lint
uv run --frozen ruff check .

# Format
uv run --frozen ruff format .

# Fix lint issues
uv run --frozen ruff check . --fix

# Type check
uv run --frozen pyright
```

---

## Package Management

- **Install:** `uv add <package>`
- **Run tools:** `uv run <tool>`
- **Upgrade:** `uv lock --upgrade-package <package>`
- **Forbidden:** `uv pip install`, `pip install`, `@latest` syntax

---

## Code Quality Rules

- All code requires **type hints**
- Public APIs must include **docstrings**
- Functions should be focused and small
- Match existing code patterns precisely
- Maximum line length: **88 characters** (ruff enforced)
- Imports at the **top of files** — never inside functions

---

## Testing Rules

- Framework: `uv run --frozen pytest`
- Write **functions**, not `Test`-prefixed classes
- Cover edge cases and error conditions
- New features need tests; bug fixes need regression tests
- **Watch each test fail before implementing** — TDD is mandatory

### Stage Acceptance Criteria

Verification must confirm these row counts before a stage is marked complete:

| Stage | Required Evidence |
|-------|-------------------|
| 0 | All containers healthy, all tables created, cross-container connectivity verified |
| 1 | Each configured translation returns correct text for Psalm 23:1 |
| 2 | 2,527 verse rows · ~43,000 token rows · 2,527 fingerprint rows |
| 3 | ~120,000 syllable token rows · 2,527 breath profiles |
| 4 | All configured translations scored · all score columns populated |
| 5 | Suggestions stored in DB · queryable from JupyterLab |
| 6 | HTML report site built · PDF generated |
| 7 | Full pipeline run exits 0 · log written |
| 8 | Isaiah output validated as first expansion target |

---

## Formatting

Ruff is the formatter and linter. Pre-commit runs on git commit.

**CI failure resolution order:**
1. Fix formatting issues
2. Resolve type errors
3. Address linting problems

---

## Exception Handling

- Use `logger.exception()` — **not** `logger.error()` — when catching exceptions
- Do **not** include the exception in the message string (logger captures it automatically)
- Catch **specific** exceptions: `OSError`, `json.JSONDecodeError`, `psycopg2.DatabaseError`, etc.
- Never use bare `except Exception:` except in top-level pipeline handlers (`run.py`)

---

## Key Design Constraints

- **Fully local and offline** after initial setup — no cloud dependencies for core analysis
- **Schema-first** — database structure defined before any data is written (`init_schema.sql`)
- **Resumable** — every stage queries which verses lack target rows before processing
- **Credentials** as environment variables in `docker-compose.yml` — never in source files
- **Config-driven** — adding a translation or expanding corpus is a `config.yml` change only; no code changes

---

## Autonomous Work

```
Plans: docs/plans/{1_backlog,2_active,3_complete}/
Active: max 1 plan at a time
```

Stop and consult the human when:
- 3 consecutive failures on the same step
- Breaking changes to the module interface (`run(conn, config) -> dict`)
- Database schema changes affecting existing stages
- Security implications discovered
- Scope significantly larger than expected

---

## Skills

Skills are in `.claude/skills/`. The `using-superpowers` skill is always active:
check whether any skill applies before responding or acting.

| Tier | Skills |
|------|--------|
| 0 — Always | `using-superpowers` |
| 1 — Essential | `project-setup` · `writing-plans` · `executing-plans` · `autonomous-work` |
| 2 — Per stage | `test-driven-development` · `verification-before-completion` · `finishing-a-development-branch` |
| 3 — Targeted | `dispatching-parallel-agents` · `subagent-driven-development` · `systematic-debugging` · `feature-backlog` · `read-arxiv-paper` |
