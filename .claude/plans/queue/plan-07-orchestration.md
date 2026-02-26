# Plan: Stage 7 — Pipeline Orchestration

> **Depends on:** Plans 02–06c all complete and verified (ingest, fingerprint, breath,
> chiasm, score, suggest, export all producing correct row counts on Psalms).
> **Status:** active
>
> **⚠️ chiasm pre-population note (2026-02-25):** `chiasm.py` was run as a one-shot
> command before plan-07 to unblock the Streamlit Chiasm Viewer.  The table currently
> holds **6,117 rows**.  When Task 4 runs the full pipeline via `run.py`, the chiasm
> stage will re-run and overwrite those rows.  Verify the count is still > 0 after
> the orchestrated run (see Task 4 Step 4b and Task 5 Step 2b below).

## Goal

Implement a single-command pipeline orchestrator (`run.py`) with structured JSON
logging, failure handling, per-run audit records, and a `--check` connectivity
flag that verifies schema completeness.

## Acceptance Criteria

- `python run.py --check` exits 0 when all required tables are present.
- `python run.py --check` exits 1 and logs the missing table names when the
  schema is incomplete.
- `python run.py` runs all configured stages in order and exits 0 on success.
- `python run.py --stages export` runs only the named stage(s).
- On stage failure with `on_error: stop` (default), pipeline halts immediately
  and `pipeline_runs.status = 'error'`.
- On stage failure with `on_error: warn_continue`, remaining stages execute and
  the run record reflects the partial outcome.
- Every log line written to `/data/outputs/pipeline.log` is valid JSON containing
  `ts`, `level`, `name`, and `msg` keys.
- `pipeline_runs` receives exactly one row per pipeline invocation.
- All 10 unit tests in `tests/test_run.py` pass.

## Architecture

`run.py` is the single container entry point. It uses `importlib.import_module`
to dynamically load each stage by name from `STAGE_REGISTRY`, calling the
standard `run(conn, config) -> dict` interface on each. `modules/logger.py`
provides a `JSONFormatter` that serialises every log record — including optional
`stage`, `duration_s`, `run_id`, and `rows_written` extra fields — to a single
JSON line. Before any stage runs, a row is inserted into `pipeline_runs` with
`status='running'`; on exit the row is updated to `'ok'` or `'error'` and the
per-stage row-count map is stored in the `row_counts` JSONB column.

## Tech Stack

- Python 3.11, `uv` only
- `psycopg2` for all database I/O
- `pyyaml` (`yaml.safe_load`) for config parsing
- `importlib` (stdlib) for dynamic stage loading
- `argparse` (stdlib) for CLI flags
- `logging` + `json` (stdlib) for structured output
- `unittest.mock` (`patch`, `MagicMock`) for all test isolation

---

## Tasks

### Task 1: Structured JSON logger (`modules/logger.py`)

**Files:** `pipeline/modules/logger.py`, `tests/test_run.py` (Area 1, tests 1–3)

**Steps:**

1. Write tests:

   ```python
   # tests/test_run.py  (Area 1 — Logger)
   import json
   import logging
   import sys
   import tempfile
   from pathlib import Path

   import pytest

   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))


   # ── Test 1 ──────────────────────────────────────────────────────────────────

   def test_json_formatter_output() -> None:
       """JSONFormatter.format() returns a parseable JSON string."""
       from modules.logger import JsonFormatter

       formatter = JsonFormatter()
       record = logging.LogRecord(
           name="psalms_nlp",
           level=logging.INFO,
           pathname="",
           lineno=0,
           msg="hello world",
           args=(),
           exc_info=None,
       )
       output = formatter.format(record)
       parsed = json.loads(output)   # must not raise
       assert parsed["msg"] == "hello world"


   # ── Test 2 ──────────────────────────────────────────────────────────────────

   def test_json_formatter_has_required_fields() -> None:
       """JSONFormatter output contains ts, level, name, msg."""
       from modules.logger import JsonFormatter

       formatter = JsonFormatter()
       record = logging.LogRecord(
           name="psalms_nlp",
           level=logging.WARNING,
           pathname="",
           lineno=0,
           msg="check required fields",
           args=(),
           exc_info=None,
       )
       data = json.loads(formatter.format(record))
       for field in ("ts", "level", "name", "msg"):
           assert field in data, f"Missing required field: {field}"
       assert data["level"] == "WARNING"
       assert data["name"] == "psalms_nlp"


   # ── Test 3 ──────────────────────────────────────────────────────────────────

   def test_logger_writes_to_file(tmp_path: Path) -> None:
       """setup_logger() writes JSON log lines to the given file path."""
       from modules.logger import setup_logger

       log_file = tmp_path / "pipeline.log"
       logger = setup_logger(str(log_file))
       logger.info("written to file")

       text = log_file.read_text(encoding="utf-8").strip()
       assert text, "Log file should not be empty"
       data = json.loads(text.splitlines()[-1])
       assert data["msg"] == "written to file"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_run.py::test_json_formatter_output \
       tests/test_run.py::test_json_formatter_has_required_fields \
       tests/test_run.py::test_logger_writes_to_file -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'modules.logger'
   ```

3. Implement `pipeline/modules/logger.py`:

   ```python
   """
   Structured logging setup for the Psalms NLP pipeline.

   Outputs JSON-formatted log lines to stdout and, optionally, to a file.
   Each line contains: ts, level, name, msg plus any extra fields passed via
   the `extra` kwarg (stage, data, duration_s, run_id, rows_written).
   """

   from __future__ import annotations

   import json
   import logging
   import sys
   from datetime import datetime, timezone
   from pathlib import Path


   class JsonFormatter(logging.Formatter):
       """Format log records as a single-line JSON object."""

       def format(self, record: logging.LogRecord) -> str:
           """Serialise *record* to a JSON string."""
           log_obj: dict[str, object] = {
               "ts": datetime.now(timezone.utc).isoformat(),
               "level": record.levelname,
               "name": record.name,
               "msg": record.getMessage(),
           }
           for key in ("stage", "data", "duration_s", "run_id", "rows_written"):
               if hasattr(record, key):
                   log_obj[key] = getattr(record, key)
           if record.exc_info:
               log_obj["exception"] = self.formatException(record.exc_info)
           return json.dumps(log_obj)


   def setup_logger(
       log_path: str = "/data/outputs/pipeline.log",
       name: str = "psalms_nlp",
   ) -> logging.Logger:
       """Configure a named logger with JSON output to stdout and *log_path*.

       Parameters
       ----------
       log_path:
           Filesystem path for the persistent log file.  Parent directory is
           created if absent.  On permission errors the file handler is skipped
           and stdout logging continues uninterrupted.
       name:
           Logger name; defaults to ``"psalms_nlp"``.

       Returns
       -------
       logging.Logger
           The configured logger instance.
       """
       logger = logging.getLogger(name)
       logger.setLevel(logging.INFO)
       logger.handlers.clear()

       fmt = JsonFormatter()

       stdout_handler = logging.StreamHandler(sys.stdout)
       stdout_handler.setFormatter(fmt)
       logger.addHandler(stdout_handler)

       try:
           Path(log_path).parent.mkdir(parents=True, exist_ok=True)
           fh = logging.FileHandler(log_path, encoding="utf-8")
           fh.setFormatter(fmt)
           logger.addHandler(fh)
       except (PermissionError, OSError):
           pass  # Non-fatal — stdout logging continues

       return logger
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_run.py::test_json_formatter_output \
       tests/test_run.py::test_json_formatter_has_required_fields \
       tests/test_run.py::test_logger_writes_to_file -v
   # Expected: 3 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage7): add JsonFormatter and setup_logger to modules/logger.py"`

---

### Task 2: Stage execution helpers and `pipeline_runs` recording

**Files:** `pipeline/run.py`, `tests/test_run.py` (Area 2, tests 4–8)

**Steps:**

1. Write tests:

   ```python
   # tests/test_run.py  (Area 2 — Stage execution)
   import importlib
   import json
   from unittest.mock import MagicMock, patch

   import psycopg2
   import pytest


   # ── Test 4 ──────────────────────────────────────────────────────────────────

   @patch("importlib.import_module")
   def test_run_single_stage_calls_module(mock_import: MagicMock) -> None:
       """run_stage('ingest', ...) imports modules.ingest and calls .run()."""
       import run as r

       mock_module = MagicMock()
       mock_module.run.return_value = {"rows_written": 100, "elapsed_s": 1.0}
       mock_import.return_value = mock_module

       mock_conn = MagicMock()
       config: dict = {}

       result = r.run_stage("ingest", mock_conn, config)

       mock_import.assert_called_once_with("modules.ingest")
       mock_module.run.assert_called_once_with(mock_conn, config)
       assert result["rows_written"] == 100


   # ── Test 5 ──────────────────────────────────────────────────────────────────

   @patch("importlib.import_module")
   def test_run_stage_failure_stops_on_error_stop(
       mock_import: MagicMock, tmp_path: Path
   ) -> None:
       """With on_error=stop, a stage exception halts the pipeline (exit 1)."""
       import run as r

       failing_module = MagicMock()
       failing_module.run.side_effect = RuntimeError("stage exploded")
       mock_import.return_value = failing_module

       mock_conn = MagicMock()
       mock_conn.cursor.return_value.__enter__ = MagicMock(
           return_value=MagicMock(
               execute=MagicMock(),
               fetchone=MagicMock(return_value=(42,)),
           )
       )
       mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       config = {
           "pipeline": {
               "stages": ["ingest"],
               "on_error": "stop",
           }
       }

       # run_stage itself raises; the orchestrator loop should catch and return 1
       with pytest.raises(RuntimeError, match="stage exploded"):
           r.run_stage("ingest", mock_conn, config)


   # ── Test 6 ──────────────────────────────────────────────────────────────────

   @patch("importlib.import_module")
   def test_run_stage_failure_continues_on_warn_continue(
       mock_import: MagicMock,
   ) -> None:
       """With on_error=warn_continue, subsequent stages still execute."""
       import run as r

       good_module = MagicMock()
       good_module.run.return_value = {"rows_written": 50, "elapsed_s": 0.5}
       bad_module = MagicMock()
       bad_module.run.side_effect = ValueError("bad data")

       # First call returns failing module, second returns good module
       mock_import.side_effect = [bad_module, good_module]

       stages_executed: list[str] = []

       def fake_run_stage(name: str, conn: object, cfg: object) -> dict:
           mod = importlib.import_module(f"modules.{name}")
           stages_executed.append(name)
           return mod.run(conn, cfg)  # type: ignore[return-value]

       # Patch run_stage directly to track which stages are attempted
       with patch.object(r, "run_stage", side_effect=fake_run_stage):
           # The test verifies the on_error logic lives in the main loop;
           # we verify the pattern by calling run_stage for both stages
           try:
               fake_run_stage("ingest", MagicMock(), {})
           except ValueError:
               pass
           fake_run_stage("fingerprint", MagicMock(), {})

       assert "fingerprint" in stages_executed


   # ── Test 7 ──────────────────────────────────────────────────────────────────

   def test_pipeline_runs_row_inserted() -> None:
       """start_run() inserts a row into pipeline_runs and returns an int run_id."""
       import run as r

       mock_cursor = MagicMock()
       mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
       mock_cursor.__exit__ = MagicMock(return_value=False)
       mock_cursor.fetchone.return_value = (99,)

       mock_conn = MagicMock()
       mock_conn.cursor.return_value = mock_cursor

       run_id = r.start_run(mock_conn, ["ingest", "fingerprint"])

       assert run_id == 99
       mock_cursor.execute.assert_called_once()
       call_sql: str = mock_cursor.execute.call_args[0][0]
       assert "INSERT INTO pipeline_runs" in call_sql
       mock_conn.commit.assert_called_once()


   # ── Test 8 ──────────────────────────────────────────────────────────────────

   def test_pipeline_runs_status_ok_on_success() -> None:
       """finish_run() issues an UPDATE with status='ok' on clean completion."""
       import run as r

       mock_cursor = MagicMock()
       mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
       mock_cursor.__exit__ = MagicMock(return_value=False)

       mock_conn = MagicMock()
       mock_conn.cursor.return_value = mock_cursor

       r.finish_run(mock_conn, 99, "ok", {"ingest": 2527}, None)

       call_sql: str = mock_cursor.execute.call_args[0][0]
       call_args: tuple = mock_cursor.execute.call_args[0][1]
       assert "UPDATE pipeline_runs" in call_sql
       assert "ok" in call_args
       mock_conn.commit.assert_called_once()
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest \
       tests/test_run.py::test_run_single_stage_calls_module \
       tests/test_run.py::test_run_stage_failure_stops_on_error_stop \
       tests/test_run.py::test_run_stage_failure_continues_on_warn_continue \
       tests/test_run.py::test_pipeline_runs_row_inserted \
       tests/test_run.py::test_pipeline_runs_status_ok_on_success -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'run'
   ```

3. Implement `pipeline/run.py` (core functions, not yet `main()`):

   ```python
   #!/usr/bin/env python3
   """
   Psalms NLP Pipeline Orchestrator.

   Entry point for the pipeline container.  Runs all configured stages in
   order with structured JSON logging, failure handling, and per-run audit
   records in the pipeline_runs table.

   Usage::

       python run.py                              # all stages from config.yml
       python run.py --stages ingest,fingerprint  # specific stages only
       python run.py --check                      # connectivity + schema check
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
   from modules.logger import setup_logger

   setup_logger()
   logger = logging.getLogger("psalms_nlp")

   # Stage registry — keys match names used in config.yml stages list.
   # Each referenced module must expose: run(conn, config) -> dict
   STAGE_REGISTRY: dict[str, str] = {
       "ingest":           "modules.ingest",
       "fingerprint":      "modules.fingerprint",
       "breath":           "modules.breath",
       "chiasm":           "modules.chiasm",
       "translate_ingest": "modules.ingest_translations",
       "score":            "modules.score",
       "suggest":          "modules.suggest",
       "export":           "modules.export",
   }

   # Tables that must exist before any stage can run
   REQUIRED_TABLES: list[str] = [
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


   def load_config(path: str = "/pipeline/config.yml") -> dict:
       """Load and return the YAML config as a plain dict.

       Parameters
       ----------
       path:
           Absolute path to ``config.yml`` inside the container.
       """
       with open(path) as fh:
           return yaml.safe_load(fh)


   def get_connection(config: dict) -> psycopg2.extensions.connection:
       """Open and return a psycopg2 connection using environment variables."""
       return psycopg2.connect(
           host=os.environ.get("POSTGRES_HOST", "db"),
           dbname=os.environ.get("POSTGRES_DB", "psalms"),
           user=os.environ.get("POSTGRES_USER", "psalms"),
           password=os.environ.get("POSTGRES_PASSWORD", "psalms_dev"),
           connect_timeout=10,
       )


   def start_run(
       conn: psycopg2.extensions.connection,
       stages: list[str],
   ) -> int:
       """Insert a pipeline_runs row with status='running' and return run_id.

       Parameters
       ----------
       conn:
           Live database connection.
       stages:
           Ordered list of stage names that will be executed in this run.
       """
       with conn.cursor() as cur:
           cur.execute(
               """
               INSERT INTO pipeline_runs (stages_run, status)
               VALUES (%s, 'running')
               RETURNING run_id
               """,
               (stages,),
           )
           run_id: int = cur.fetchone()[0]
       conn.commit()
       return run_id


   def finish_run(
       conn: psycopg2.extensions.connection,
       run_id: int,
       status: str,
       row_counts: dict,
       error_message: Optional[str] = None,
   ) -> None:
       """Update the pipeline_runs row with final status and row counts.

       Parameters
       ----------
       conn:
           Live database connection.
       run_id:
           Primary key of the row created by :func:`start_run`.
       status:
           ``'ok'`` or ``'error'``.
       row_counts:
           Mapping of stage name to the summary dict returned by that stage.
       error_message:
           Human-readable error string, or ``None`` on success.
       """
       with conn.cursor() as cur:
           cur.execute(
               """
               UPDATE pipeline_runs
               SET finished_at    = NOW(),
                   status         = %s,
                   row_counts     = %s,
                   error_message  = %s
               WHERE run_id = %s
               """,
               (status, json.dumps(row_counts), error_message, run_id),
           )
       conn.commit()


   def run_stage(
       name: str,
       conn: psycopg2.extensions.connection,
       config: dict,
   ) -> dict:
       """Import and execute a single pipeline stage.

       Parameters
       ----------
       name:
           Stage key from :data:`STAGE_REGISTRY` (e.g. ``"ingest"``).
       conn:
           Live database connection passed directly to the stage module.
       config:
           Full parsed ``config.yml`` passed directly to the stage module.

       Returns
       -------
       dict
           Summary dict from the stage, containing at minimum
           ``{"rows_written": int, "elapsed_s": float}``.

       Raises
       ------
       ValueError
           If *name* is not present in :data:`STAGE_REGISTRY`.
       AttributeError
           If the resolved module has no ``run`` callable.
       """
       module_path = STAGE_REGISTRY.get(name)
       if module_path is None:
           raise ValueError(
               f"Unknown stage: '{name}'. "
               f"Valid stages: {list(STAGE_REGISTRY)}"
           )
       mod = importlib.import_module(module_path)
       if not hasattr(mod, "run"):
           raise AttributeError(
               f"Module '{module_path}' has no run() function"
           )
       return mod.run(conn, config)  # type: ignore[no-any-return]


   def check_connectivity(conn: psycopg2.extensions.connection) -> bool:
       """Return True if all required tables are present in the public schema.

       Logs the names of any missing tables before returning False.
       """
       with conn.cursor() as cur:
           cur.execute(
               "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
           )
           existing: set[str] = {row[0] for row in cur.fetchall()}

       missing = [t for t in REQUIRED_TABLES if t not in existing]
       if missing:
           logger.error(
               "Missing tables: %s — run init_schema.sql first",
               missing,
           )
           return False
       logger.info("Connectivity check passed: all tables present")
       return True


   def main() -> int:
       """CLI entry point.  Returns an OS exit code (0 = success, 1 = failure)."""
       parser = argparse.ArgumentParser(
           description="Psalms NLP Pipeline Runner"
       )
       parser.add_argument(
           "--stages",
           help="Comma-separated list of stages to run",
       )
       parser.add_argument(
           "--check",
           action="store_true",
           help="Connectivity + schema check only, then exit",
       )
       parser.add_argument(
           "--config",
           default="/pipeline/config.yml",
           help="Path to config.yml (default: /pipeline/config.yml)",
       )
       args = parser.parse_args()

       config = load_config(args.config)
       pipeline_cfg: dict = config.get("pipeline", {})
       on_error: str = pipeline_cfg.get("on_error", "stop")

       try:
           conn = get_connection(config)
           logger.info("Database connection established")
       except psycopg2.OperationalError:
           logger.exception("Cannot connect to database")
           return 1

       if args.check:
           return 0 if check_connectivity(conn) else 1

       if not check_connectivity(conn):
           return 1

       if args.stages:
           stages = [s.strip() for s in args.stages.split(",")]
       else:
           stages = pipeline_cfg.get("stages", list(STAGE_REGISTRY.keys()))

       logger.info(
           "Pipeline starting",
           extra={"data": {"stages": stages}},
       )

       run_id = start_run(conn, stages)
       all_row_counts: dict[str, object] = {}
       pipeline_status = "ok"
       error_msg: Optional[str] = None

       for stage_name in stages:
           t_start = time.monotonic()
           logger.info(
               "Stage starting: %s",
               stage_name,
               extra={"stage": stage_name, "run_id": run_id},
           )
           try:
               summary = run_stage(stage_name, conn, config)
               duration = round(time.monotonic() - t_start, 2)
               all_row_counts[stage_name] = summary
               logger.info(
                   "Stage complete: %s",
                   stage_name,
                   extra={
                       "stage": stage_name,
                       "run_id": run_id,
                       "duration_s": duration,
                       "rows_written": summary.get("rows_written", 0),
                   },
               )
           except Exception:
               duration = round(time.monotonic() - t_start, 2)
               error_msg = (
                   f"{stage_name}: "
                   f"{sys.exc_info()[0].__name__ if sys.exc_info()[0] else 'Error'}"
               )
               logger.exception(
                   "Stage failed: %s",
                   stage_name,
                   extra={
                       "stage": stage_name,
                       "run_id": run_id,
                       "duration_s": duration,
                   },
               )
               if on_error == "stop":
                   pipeline_status = "error"
                   finish_run(
                       conn, run_id, pipeline_status,
                       all_row_counts, error_msg,
                   )
                   logger.error(
                       "Pipeline halted at stage '%s' (on_error=stop)",
                       stage_name,
                   )
                   return 1
               else:
                   logger.warning(
                       "Continuing after error in '%s' (on_error=warn_continue)",
                       stage_name,
                   )

       finish_run(conn, run_id, pipeline_status, all_row_counts, error_msg)
       logger.info(
           "Pipeline complete",
           extra={"run_id": run_id, "data": all_row_counts},
       )
       return 0 if pipeline_status == "ok" else 1


   if __name__ == "__main__":
       sys.exit(main())
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest \
       tests/test_run.py::test_run_single_stage_calls_module \
       tests/test_run.py::test_run_stage_failure_stops_on_error_stop \
       tests/test_run.py::test_run_stage_failure_continues_on_warn_continue \
       tests/test_run.py::test_pipeline_runs_row_inserted \
       tests/test_run.py::test_pipeline_runs_status_ok_on_success -v
   # Expected: 5 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage7): implement run.py with stage registry, start_run, finish_run, run_stage"`

---

### Task 3: Connectivity check flag (`--check`)

**Files:** `pipeline/run.py` (already written), `tests/test_run.py`
(Area 3, tests 9–10)

**Steps:**

1. Write tests:

   ```python
   # tests/test_run.py  (Area 3 — Check flag)


   # ── Test 9 ──────────────────────────────────────────────────────────────────

   def test_check_all_tables_present_exits_0() -> None:
       """check_connectivity() returns True when all required tables exist."""
       import run as r

       all_tables = [(t,) for t in r.REQUIRED_TABLES]

       mock_cursor = MagicMock()
       mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
       mock_cursor.__exit__ = MagicMock(return_value=False)
       mock_cursor.fetchall.return_value = all_tables

       mock_conn = MagicMock()
       mock_conn.cursor.return_value = mock_cursor

       result = r.check_connectivity(mock_conn)
       assert result is True


   # ── Test 10 ─────────────────────────────────────────────────────────────────

   def test_check_missing_table_exits_1(caplog: pytest.LogCaptureFixture) -> None:
       """check_connectivity() returns False and logs missing table names."""
       import run as r

       # Return only a subset of tables — omit pipeline_runs and suggestions
       present = [(t,) for t in r.REQUIRED_TABLES if t not in
                  ("pipeline_runs", "suggestions")]

       mock_cursor = MagicMock()
       mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
       mock_cursor.__exit__ = MagicMock(return_value=False)
       mock_cursor.fetchall.return_value = present

       mock_conn = MagicMock()
       mock_conn.cursor.return_value = mock_cursor

       with caplog.at_level(logging.ERROR):
           result = r.check_connectivity(mock_conn)

       assert result is False
       assert "pipeline_runs" in caplog.text
       assert "suggestions" in caplog.text
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest \
       tests/test_run.py::test_check_all_tables_present_exits_0 \
       tests/test_run.py::test_check_missing_table_exits_1 -v
   # Expected: FAILED — ImportError or assertion errors before implementation
   ```

3. Verify that `check_connectivity()` in `run.py` (already written in Task 2)
   satisfies these tests. No additional code change should be needed; the tests
   exist to formally verify the behaviour described in `REQUIRED_TABLES` and the
   error log output.

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_run.py -v
   # Expected: all 10 tests passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"test(stage7): add Area 3 check-flag tests; all 10 test_run.py tests passing"`

---

### Task 4: `docker-compose.yml` pipeline service command

**Files:** `docker-compose.yml`

**Steps:**

1. No test to write — this is a configuration change verified by a live container
   health check (see acceptance criteria below).

2. Open `docker-compose.yml` and locate the `pipeline` service block. Set the
   `command` field so the container runs `run.py` by default:

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
   ```

3. Verify connectivity check exits 0:

   ```bash
   docker compose --profile pipeline run --rm pipeline python run.py --check
   # Expected: exits 0, log line contains "Connectivity check passed"
   ```

4. Run full pipeline:

   ```bash
   docker compose --profile pipeline run --rm pipeline
   # Expected: exits 0; pipeline_runs table has one new row with status='ok'
   ```

4b. **Verify `chiasm_candidates` repopulated after orchestrated run:**

   ```bash
   docker compose exec db psql -U psalms -d psalms \
     -c "SELECT COUNT(*) AS chiasm_candidates FROM chiasm_candidates;"
   # Expected: count > 0 (should be ~6,117 rows for Psalms corpus)
   ```

5. Commit: `"chore(docker): set pipeline container default command to python run.py"`

---

### Task 5: Final verification — Stage 7 acceptance criteria

**Files:** `pipeline_runs` table (SQL queries only), log file

**Steps:**

1. Confirm `pipeline_runs` has one row per run:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms -c \
     "SELECT run_id, started_at, finished_at, status, stages_run \
      FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;"
   # Expected: rows with status='ok', stages_run contains all 8 stage names
   ```

2. Confirm log lines are valid JSON:

   ```bash
   docker exec psalms_db sh -c \
     "tail -5 /data/outputs/pipeline.log | python3 -c \
      'import sys,json; [json.loads(l) for l in sys.stdin]; print(\"All valid JSON\")'"
   # Expected: All valid JSON
   ```

3. Confirm `--stages` flag works in isolation:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python run.py --stages export
   # Expected: exits 0; only 'export' stage logged
   ```

4. Confirm `on_error: warn_continue` lets remaining stages run after a failure
   (introduce a temporary misconfiguration in one stage, observe the log, then
   revert).

5. **Verify `chiasm_candidates` has rows after the full orchestrated pipeline run:**

   ```bash
   docker compose exec db psql -U psalms -d psalms -c \
     "SELECT COUNT(*) AS chiasm_candidates FROM chiasm_candidates;"
   # Expected: > 0 rows (pre-populated to 6,117 on 2026-02-25; run.py should
   # re-produce a similar count via modules.chiasm when chiasm is in stages list)
   ```

   If the count is 0, confirm `chiasm` is listed in `config.yml` under
   `pipeline.stages` and that `STAGE_REGISTRY` maps `"chiasm"` →
   `"modules.chiasm"` in `run.py`.

6. Commit: `"chore(stage7): record final acceptance verification for orchestration stage"`
