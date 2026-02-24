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
        cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
        row = cur.fetchone()
    assert (
        row is not None
    ), "pgvector extension not found. Run: CREATE EXTENSION IF NOT EXISTS pgvector;"
    logger.info("CHECK  pgvector extension: OK")


def check_tables(conn: psycopg2.extensions.connection) -> None:
    """Assert all 11 required tables exist in the public schema."""
    with conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        found = {row[0] for row in cur.fetchall()}
    missing = REQUIRED_TABLES - found
    assert not missing, f"Missing tables: {sorted(missing)}\nFound: {sorted(found)}"
    logger.info(f"CHECK  all {len(REQUIRED_TABLES)} tables present: OK")


def check_indices(conn: psycopg2.extensions.connection) -> None:
    """Assert all 9 required indices exist."""
    with conn.cursor() as cur:
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
        found = {row[0] for row in cur.fetchall()}
    missing = REQUIRED_INDICES - found
    assert (
        not missing
    ), f"Missing indices: {sorted(missing)}\nFound: {sorted(found & REQUIRED_INDICES)}"
    logger.info(f"CHECK  all {len(REQUIRED_INDICES)} indices present: OK")


def check_books_seed(conn: psycopg2.extensions.connection) -> None:
    """Assert books table contains exactly the 6 expected seed rows."""
    with conn.cursor() as cur:
        cur.execute("SELECT book_num, book_name FROM books ORDER BY book_num")
        rows = {(r[0], r[1]) for r in cur.fetchall()}
    assert (
        rows == EXPECTED_BOOKS
    ), f"books mismatch.\nExpected: {sorted(EXPECTED_BOOKS)}\nGot: {sorted(rows)}"
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
        ("connection", check_connection),
        ("pgvector", check_pgvector),
        ("tables", check_tables),
        ("indices", check_indices),
        ("books seed", check_books_seed),
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
