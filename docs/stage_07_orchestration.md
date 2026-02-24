# Stage 7 — Pipeline Orchestration
## Detailed Implementation Plan

> **Depends on:** Stages 2–6 implemented  
> **Produces:** Single-command automated pipeline runner with sequencing, failure handling, resumability, structured logging, and optional scheduling via WSL cron  
> **Estimated time:** 2–3 hours

---

## Objectives

1. Implement `run.py` as the single orchestration entry point
2. Implement structured JSON logging to stdout and file
3. Configure `docker-compose.yml` pipeline profile correctly
4. Document and test partial rebuild via stage list in `config.yml`
5. Provide WSL cron scheduling template

---

## Design Principles

- **Idempotent:** Running the full pipeline twice produces the same database state
- **Resumable:** Any stage can be re-run by modifying the `stages` list in `config.yml` — no code changes
- **Transparent:** Every stage logs start time, end time, row counts, and errors as structured JSON
- **Fail-safe:** `on_error: stop` halts on first error (default); `on_error: warn_continue` logs and proceeds
- **Record-keeping:** Each run creates a row in `pipeline_runs` for auditability

---

## Stage Sequencing

```
ingest             (Stage 2: BHSA → verses + word_tokens)
fingerprint        (Stage 2: verses → verse_fingerprints)
breath             (Stage 3: word_tokens → syllable_tokens + breath_profiles)
chiasm             (Stage 2 second pass: breath data → chiasm_candidates)
translate_ingest   (Stage 1 completion: source files → translations table)
score              (Stage 4: translations + fingerprints → translation_scores)
suggest            (Stage 5: high-deviation verses → suggestions)
export             (Stage 6: database → HTML site + PDF)
```

The `chiasm` pass is placed after `breath` because it requires colon boundary data.
`translate_ingest` is placed after `fingerprint` because it requires `verses` to exist.

---

## File Structure

```
pipeline/
  run.py             ← main orchestrator
  modules/
    logger.py        ← structured logging setup
  tests/
    test_run.py
```

---

## Step 1 — File: `modules/logger.py`

```python
"""
Structured logging setup for the pipeline.

Outputs JSON-formatted log lines to stdout and a file.
Each line: timestamp, level, logger name, message, and optional stage/data fields.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "ts":    datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "name":  record.name,
            "msg":   record.getMessage(),
        }
        for key in ("stage", "data", "duration_s", "run_id"):
            if hasattr(record, key):
                log_data[key] = getattr(record, key)
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def setup_logging(log_path: str = "/data/outputs/pipeline.log") -> None:
    """Configure root logger with JSON formatter to stdout and file."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = JSONFormatter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(fmt)
    root.addHandler(stdout_handler)

    try:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except (PermissionError, OSError):
        pass  # Non-fatal; stdout logging continues
```

---

## Step 2 — File: `run.py`

```python
#!/usr/bin/env python3
"""
Psalms NLP Pipeline Orchestrator

Entry point for the pipeline container. Runs all configured stages in order,
with structured JSON logging, failure handling, and run recording.

Usage:
    python run.py                              # run all stages in config.yml
    python run.py --stages ingest,fingerprint  # run specific stages
    python run.py --check                      # connectivity check only
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import yaml

sys.path.insert(0, "/pipeline")
from modules.logger import setup_logging

setup_logging()
logger = logging.getLogger("run")

# ─── Stage registry ───────────────────────────────────────────────
# Keys must match the names used in config.yml stages list.
# Each module must expose: run(conn, config) -> dict

STAGE_REGISTRY = {
    "ingest":           "modules.ingest",
    "fingerprint":      "modules.fingerprint",
    "breath":           "modules.breath",
    "chiasm":           "modules.chiasm",
    "translate_ingest": "modules.ingest_translations",
    "score":            "modules.score",
    "suggest":          "modules.suggest",
    "export":           "modules.export",
}


def get_connection(config: dict) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "db"),
        dbname=os.environ.get("POSTGRES_DB", "psalms"),
        user=os.environ.get("POSTGRES_USER", "psalms"),
        password=os.environ.get("POSTGRES_PASSWORD", "psalms_dev"),
        connect_timeout=10,
    )


def load_config(path: str = "/pipeline/config.yml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def start_run(conn: psycopg2.extensions.connection, stages: list) -> int:
    """Insert a pipeline_runs row and return the run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (stages_run, status)
            VALUES (%s, 'running')
            RETURNING run_id
            """,
            (stages,)
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(
    conn: psycopg2.extensions.connection,
    run_id: int,
    status: str,
    row_counts: dict,
    error_message: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = NOW(), status = %s,
                row_counts = %s, error_message = %s
            WHERE run_id = %s
            """,
            (status, json.dumps(row_counts), error_message, run_id)
        )
    conn.commit()


def run_stage(name: str, conn, config: dict) -> dict:
    """Import and execute a single stage module. Returns its summary dict."""
    module_path = STAGE_REGISTRY.get(name)
    if module_path is None:
        raise ValueError(f"Unknown stage: '{name}'. Valid stages: {list(STAGE_REGISTRY)}")

    mod = importlib.import_module(module_path)
    if not hasattr(mod, "run"):
        raise AttributeError(f"Module '{module_path}' has no run() function")

    return mod.run(conn, config)


def check_connectivity(conn) -> bool:
    """Verify all expected tables exist."""
    required_tables = [
        "books", "verses", "translations", "word_tokens",
        "verse_fingerprints", "chiasm_candidates",
        "syllable_tokens", "breath_profiles",
        "translation_scores", "suggestions", "pipeline_runs",
    ]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        existing = {row[0] for row in cur.fetchall()}

    missing = [t for t in required_tables if t not in existing]
    if missing:
        logger.error(f"Missing tables: {missing}. Run init_schema.sql first.")
        return False
    logger.info("Connectivity check passed: all tables present")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Psalms NLP Pipeline Runner")
    parser.add_argument("--stages", help="Comma-separated list of stages to run")
    parser.add_argument("--check", action="store_true", help="Connectivity check only")
    parser.add_argument("--config", default="/pipeline/config.yml")
    args = parser.parse_args()

    config = load_config(args.config)
    pipeline_cfg = config.get("pipeline", {})
    on_error = pipeline_cfg.get("on_error", "stop")

    try:
        conn = get_connection(config)
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Cannot connect to database: {e}")
        return 1

    if args.check:
        return 0 if check_connectivity(conn) else 1

    if not check_connectivity(conn):
        return 1

    # Determine which stages to run
    if args.stages:
        stages = [s.strip() for s in args.stages.split(",")]
    else:
        stages = pipeline_cfg.get("stages", list(STAGE_REGISTRY.keys()))

    logger.info(f"Pipeline starting — stages: {stages}")

    run_id = start_run(conn, stages)
    all_row_counts: dict = {}
    pipeline_status = "ok"
    error_msg = None

    for stage_name in stages:
        t_start = time.monotonic()
        logger.info(f"Stage starting: {stage_name}", extra={"stage": stage_name, "run_id": run_id})

        try:
            summary = run_stage(stage_name, conn, config)
            duration = round(time.monotonic() - t_start, 2)
            all_row_counts[stage_name] = summary
            logger.info(
                f"Stage complete: {stage_name}",
                extra={"stage": stage_name, "run_id": run_id, "duration_s": duration, "data": summary}
            )
        except Exception as exc:
            duration = round(time.monotonic() - t_start, 2)
            error_msg = f"{stage_name}: {type(exc).__name__}: {exc}"
            logger.error(
                f"Stage failed: {stage_name} — {exc}",
                extra={"stage": stage_name, "run_id": run_id, "duration_s": duration},
                exc_info=True,
            )

            if on_error == "stop":
                pipeline_status = "error"
                finish_run(conn, run_id, pipeline_status, all_row_counts, error_msg)
                logger.error(f"Pipeline halted at stage '{stage_name}' (on_error=stop)")
                return 1
            else:
                logger.warning(f"Continuing after error in '{stage_name}' (on_error=warn_continue)")

    finish_run(conn, run_id, pipeline_status, all_row_counts, error_msg)
    logger.info(
        f"Pipeline complete — run_id={run_id}, status={pipeline_status}",
        extra={"run_id": run_id, "data": all_row_counts}
    )
    return 0 if pipeline_status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
```

---

## Step 3 — Updated `docker-compose.yml` pipeline service block

This is a reminder of the final shape of the pipeline service, including the optional Ollama block:

```yaml
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
    command: ["python", "run.py"]

  # ── Optional: local Ollama LLM ───────────────────────────────
  # Uncomment entire block to enable. Then set:
  #   LLM_PROVIDER=ollama, LLM_MODEL=llama3, OLLAMA_HOST=http://ollama:11434
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

---

## Step 4 — Operational Usage Reference

### Run the full pipeline
```bash
docker compose --profile pipeline run --rm pipeline
```

### Run specific stages only
```bash
# Re-score only (after adding a new translation to config.yml)
docker compose --profile pipeline run --rm pipeline \
  python run.py --stages translate_ingest,score

# Re-run export only (after editing visualization code)
docker compose --profile pipeline run --rm pipeline \
  python run.py --stages export

# Force breath + chiasm rebuild
docker compose --profile pipeline run --rm pipeline \
  python run.py --stages breath,chiasm
```

### Connectivity check
```bash
docker compose --profile pipeline run --rm pipeline python run.py --check
# Expected: exits 0 with log line: "Connectivity check passed"
```

### Watch live log output
```bash
# During a run, in a second terminal:
docker logs -f psalms_pipeline

# Or directly on the log file from Windows host:
Get-Content C:\psalms-nlp\data\outputs\pipeline.log -Wait
```

### View recent runs
```bash
docker exec psalms_db psql -U psalms -d psalms -c \
  "SELECT run_id, started_at, finished_at, status, stages_run FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;"
```

---

## Step 5 — WSL Cron Scheduling

For automated scheduled runs on Windows via WSL2:

```bash
# Open WSL crontab
wsl crontab -e
```

Add these lines:

```cron
# Psalms NLP Pipeline — runs every Sunday at 2:00 AM
# Logs to cron.log on Windows host
0 2 * * 0 cd /mnt/c/psalms-nlp && docker compose --profile pipeline run --rm pipeline \
  >> /mnt/c/psalms-nlp/data/outputs/cron.log 2>&1

# Optional: run export only every night to keep reports fresh (no re-analysis)
0 3 * * * cd /mnt/c/psalms-nlp && docker compose --profile pipeline run --rm pipeline \
  python run.py --stages export \
  >> /mnt/c/psalms-nlp/data/outputs/cron_export.log 2>&1
```

Verify WSL cron is running:
```bash
# In WSL
service cron status
# If not running: sudo service cron start
```

---

## Step 6 — `config.yml` Stage Control Reference

The `stages` list in `config.yml` controls which stages run by default:

```yaml
pipeline:
  stages:
    - ingest
    - fingerprint
    - breath
    - chiasm
    - translate_ingest
    - score
    - suggest
    - export
  on_error: stop   # stop | warn_continue
```

Common configurations:

```yaml
# Development: analysis only, no export or suggestions
pipeline:
  stages: [ingest, fingerprint, breath, chiasm, translate_ingest, score]
  on_error: warn_continue

# Production: full pipeline, halt on error
pipeline:
  stages: [ingest, fingerprint, breath, chiasm, translate_ingest, score, suggest, export]
  on_error: stop

# Quick re-score after config change (translations already ingested)
pipeline:
  stages: [score, export]
  on_error: stop

# Debug single Psalm
pipeline:
  stages: [ingest, fingerprint, breath, chiasm, translate_ingest, score]
  on_error: warn_continue
corpus:
  debug_chapters: [23]
```

---

## Step 7 — Test Cases

```python
# tests/test_run.py

import sys
import json
import logging
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestJSONFormatter:

    def test_formats_as_json(self):
        from modules.logger import JSONFormatter
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO,
            pathname="", lineno=0,
            msg="hello world", args=(), exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["msg"] == "hello world"
        assert data["level"] == "INFO"
        assert "ts" in data

    def test_includes_extra_fields(self):
        from modules.logger import JSONFormatter
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO,
            pathname="", lineno=0,
            msg="stage done", args=(), exc_info=None,
        )
        record.stage = "fingerprint"
        record.duration_s = 12.3
        output = formatter.format(record)
        data = json.loads(output)
        assert data["stage"] == "fingerprint"
        assert data["duration_s"] == 12.3


class TestStageRegistry:

    def test_all_expected_stages_registered(self):
        import run as r
        expected = [
            "ingest", "fingerprint", "breath", "chiasm",
            "translate_ingest", "score", "suggest", "export",
        ]
        for stage in expected:
            assert stage in r.STAGE_REGISTRY, f"Stage '{stage}' missing from registry"

    def test_unknown_stage_raises(self):
        import run as r

        class MockConn:
            pass

        with pytest.raises(ValueError, match="Unknown stage"):
            r.run_stage("not_a_real_stage", MockConn(), {})


class TestLoadConfig:

    def test_load_config_returns_dict(self, tmp_path):
        cfg_file = tmp_path / "config.yml"
        cfg_file.write_text("pipeline:\n  stages: [ingest]\n  on_error: stop\n")
        import run as r
        config = r.load_config(str(cfg_file))
        assert isinstance(config, dict)
        assert config["pipeline"]["on_error"] == "stop"
```

Run:
```bash
docker compose --profile pipeline run --rm pipeline python -m pytest /pipeline/tests/test_run.py -v
```

---

## Acceptance Criteria

- [ ] `python run.py --check` exits 0 when all tables present
- [ ] `python run.py --check` exits 1 and logs missing table names when schema incomplete
- [ ] `python run.py` runs all configured stages, exits 0 on success, 1 on stage failure
- [ ] `python run.py --stages export` runs only the export stage
- [ ] On stage failure with `on_error: stop`, pipeline halts and `pipeline_runs` row shows `status = 'error'`
- [ ] On stage failure with `on_error: warn_continue`, remaining stages execute and final status is still set correctly
- [ ] Log file written to `/data/outputs/pipeline.log` after each run
- [ ] Each log line is valid JSON with `ts`, `level`, `name`, `msg` keys
- [ ] `pipeline_runs` table has one row per invocation
- [ ] All unit tests pass

---

## SQL Validation Queries

```sql
-- Recent pipeline runs
SELECT run_id, started_at, finished_at,
       EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds,
       status, stages_run, error_message
FROM pipeline_runs
ORDER BY started_at DESC LIMIT 10;

-- Row counts from last successful run
SELECT run_id, row_counts
FROM pipeline_runs
WHERE status = 'ok'
ORDER BY started_at DESC LIMIT 1;
```
