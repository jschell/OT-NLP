# pipeline/run.py
"""
Psalms NLP Pipeline — Full orchestrator.

Runs all implemented stages in order:
  Stage 2a  ingest              BHSA → verses + word_tokens
  Stage 2b  fingerprint         verses → verse_fingerprints
  Stage 1   ingest_translations translation files → translations
  Stage 1   validate_data       spot-check known verses in translations

Usage (from inside the pipeline container):
    python run.py

Usage (single stage override via docker compose):
    docker compose --profile pipeline run --rm pipeline \
        python -c "
import sys, os, psycopg2, yaml
sys.path.insert(0, '/pipeline')
cfg = yaml.safe_load(open('/pipeline/config.yml'))
conn = psycopg2.connect(host=os.environ['POSTGRES_HOST'],
    dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'])
from modules import ingest
print(ingest.run(conn, cfg))
"
"""

from __future__ import annotations

import logging
import os
import sys

import psycopg2
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run")

# ── Adjust import path so modules can import from adapters ────────────────────
sys.path.insert(0, "/pipeline")


def _conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        dbname=os.environ.get("POSTGRES_DB", "psalms"),
        user=os.environ.get("POSTGRES_USER", "psalms"),
        password=os.environ.get("POSTGRES_PASSWORD", "psalms_dev"),
        connect_timeout=10,
    )


def _load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "config.yml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def main() -> int:
    logger.info("═══════════════════════════════════════════════")
    logger.info("  Psalms NLP Pipeline — starting")
    logger.info("═══════════════════════════════════════════════")

    config = _load_config()

    try:
        conn = _conn()
    except psycopg2.OperationalError:
        logger.exception(
            "Cannot connect to PostgreSQL (POSTGRES_HOST=%s).",
            os.environ.get("POSTGRES_HOST", "localhost"),
        )
        return 1

    failures: list[str] = []

    # ── Stage 2a: BHSA ingest ─────────────────────────────────────────────────
    logger.info("─── Stage 2a: BHSA ingest ───────────────────────────────────")
    try:
        from modules import ingest as _ingest
        result = _ingest.run(conn, config)
        logger.info(
            "ingest: %d verses, %d tokens in %.1fs",
            result["verses"],
            result["word_tokens"],
            result["elapsed_s"],
        )
    except Exception:
        logger.exception("Stage 2a FAILED")
        failures.append("ingest")

    # ── Stage 2b: fingerprint ─────────────────────────────────────────────────
    logger.info("─── Stage 2b: fingerprint ───────────────────────────────────")
    try:
        from modules import fingerprint as _fp
        result = _fp.run(conn, config)
        logger.info(
            "fingerprint: %d rows in %.1fs",
            result.get("rows_written", 0),
            result.get("elapsed_s", 0.0),
        )
    except Exception:
        logger.exception("Stage 2b FAILED")
        failures.append("fingerprint")

    # ── Stage 1: translation ingest ───────────────────────────────────────────
    logger.info("─── Stage 1: translation ingest ─────────────────────────────")
    try:
        from modules import ingest_translations as _it
        result = _it.run(conn, config)
        logger.info(
            "ingest_translations: %d rows in %.1fs",
            result["rows_written"],
            result["elapsed_s"],
        )
    except Exception:
        logger.exception("Stage 1 translation ingest FAILED")
        failures.append("ingest_translations")

    # ── Stage 1: validate data ────────────────────────────────────────────────
    logger.info("─── Stage 1: validate_data ──────────────────────────────────")
    try:
        import validate_data as _vd
        result = _vd.run(conn, config)
        logger.info(
            "validate_data: %d/%d checks passed",
            result["passed"],
            result["passed"] + result["failed"],
        )
    except AssertionError as exc:
        logger.error("validate_data FAILED:\n%s", exc)
        failures.append("validate_data")
    except Exception:
        logger.exception("validate_data FAILED (unexpected error)")
        failures.append("validate_data")

    # ── Stage 3: breath / phonetic analysis ───────────────────────────────────
    logger.info("─── Stage 3: breath ─────────────────────────────────────────")
    try:
        from modules import breath as _breath
        result = _breath.run(conn, config)
        logger.info(
            "breath: %d profiles, %d syllable_tokens in %.1fs",
            result.get("breath_profiles", 0),
            result.get("syllable_tokens", 0),
            result.get("elapsed_s", 0.0),
        )
    except Exception:
        logger.exception("Stage 3 FAILED")
        failures.append("breath")

    # ── Stage 4: translation scoring ──────────────────────────────────────────
    logger.info("─── Stage 4: score ──────────────────────────────────────────")
    try:
        from modules import score as _score
        result = _score.run(conn, config)
        logger.info(
            "score: %d pairs in %.1fs",
            result.get("scored", 0),
            result.get("elapsed_s", 0.0),
        )
    except Exception:
        logger.exception("Stage 4 FAILED")
        failures.append("score")

    # ── Stage 5: LLM suggestions ──────────────────────────────────────────────
    logger.info("─── Stage 5: suggest ────────────────────────────────────────")
    try:
        from modules import suggest as _suggest
        result = _suggest.run(conn, config)
        logger.info(
            "suggest: generated=%s skipped=%s",
            result.get("generated", 0),
            result.get("skipped", 0),
        )
    except Exception:
        logger.exception("Stage 5 FAILED")
        failures.append("suggest")

    conn.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("═══════════════════════════════════════════════")
    if failures:
        logger.error("Pipeline FAILED — %d stage(s): %s", len(failures), failures)
        logger.info("═══════════════════════════════════════════════")
        return 1

    logger.info("Pipeline COMPLETE — all stages passed.")
    logger.info("═══════════════════════════════════════════════")
    return 0


if __name__ == "__main__":
    sys.exit(main())
