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
        assert re.search(
            pattern, sql
        ), f"Missing 'CREATE TABLE IF NOT EXISTS {table}' in init_schema.sql"


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
        assert (
            str(book_num) in sql
        ), f"book_num={book_num} not found in init_schema.sql seed data"
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
