#!/usr/bin/env python3
# pipeline/run.py
"""Psalms NLP Pipeline Orchestrator.

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
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from modules.logger import setup_logger  # noqa: E402

setup_logger()
logger = logging.getLogger("psalms_nlp")

# Stage registry — keys match names used in config.yml stages list.
# Each referenced module must expose: run(conn, config) -> dict
STAGE_REGISTRY: dict[str, str] = {
    "ingest": "modules.ingest",
    "fingerprint": "modules.fingerprint",
    "breath": "modules.breath",
    "chiasm": "modules.chiasm",
    "translate_ingest": "modules.ingest_translations",
    "score": "modules.score",
    "suggest": "modules.suggest",
    "export": "modules.export",
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


def get_connection(config: dict) -> psycopg2.extensions.connection:  # noqa: ARG001
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
        row = cur.fetchone()
        run_id: int = row[0] if row else 0
    conn.commit()
    return run_id


def finish_run(
    conn: psycopg2.extensions.connection,
    run_id: int,
    status: str,
    row_counts: dict,
    error_message: str | None = None,
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
            f"Unknown stage: '{name}'. Valid stages: {list(STAGE_REGISTRY)}"
        )
    mod = importlib.import_module(module_path)
    if not hasattr(mod, "run"):
        raise AttributeError(f"Module '{module_path}' has no run() function")
    return mod.run(conn, config)  # type: ignore[no-any-return]


def check_connectivity(conn: psycopg2.extensions.connection) -> bool:
    """Return True if all required tables are present in the public schema.

    Logs the names of any missing tables before returning False.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
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
    parser = argparse.ArgumentParser(description="Psalms NLP Pipeline Runner")
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
    error_msg: str | None = None

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
            exc_type = sys.exc_info()[0]
            error_msg = f"{stage_name}: {exc_type.__name__ if exc_type else 'Error'}"
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
                    conn,
                    run_id,
                    pipeline_status,
                    all_row_counts,
                    error_msg,
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
