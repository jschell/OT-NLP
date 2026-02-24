# pipeline/validate_data.py
"""
Stage 1 — Post-ingest data validation.

Checks that known verses return expected text from the translations table.
Raises AssertionError with a clear diagnostic message if any check fails.

Entry point:
    run(conn, config) -> {"passed": int, "failed": int}
"""

from __future__ import annotations

import logging

import psycopg2.extensions

logger = logging.getLogger(__name__)

# Known-good checks: (book_num, chapter, verse_num, translation_key, expected_prefix)
# These are immutable facts — if they fail, the data is wrong.
CHECKS: list[tuple[int, int, int, str, str]] = [
    # Psalm 23:1 — primary fixture across all configured translations
    (19, 23, 1, "KJV", "The LORD is my shepherd"),
    (19, 23, 1, "YLT", "Jehovah"),
    (19, 23, 1, "WEB", "Yahweh"),
    (19, 23, 1, "ULT", "Yahweh"),
    (19, 23, 1, "UST", "God"),
    # Psalm 1:1 — first verse of the book
    (19, 1, 1, "KJV", "Blessed"),
    # Psalm 150:6 — last verse of the book
    (19, 150, 6, "KJV", "Let every thing that hath breath"),
]


def run(
    conn: psycopg2.extensions.connection,
    config: dict,
) -> dict:
    """
    Validate that known verses return expected text from the translations table.

    Args:
        conn:   Live psycopg2 connection.
        config: Full parsed config.yml dict (unused; present for module interface).

    Returns:
        {"passed": int, "failed": int}

    Raises:
        AssertionError: If any check fails. Message includes all failures.
    """
    failures: list[str] = []
    passed = 0

    for book_num, chapter, verse_num, key, expected_prefix in CHECKS:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.verse_text
                FROM translations t
                JOIN verses v ON t.verse_id = v.verse_id
                WHERE v.book_num = %s
                  AND v.chapter = %s
                  AND v.verse_num = %s
                  AND t.translation_key = %s
                """,
                (book_num, chapter, verse_num, key),
            )
            row = cur.fetchone()

        ref = f"{key} {book_num}:{chapter}:{verse_num}"

        if row is None:
            failures.append(f"MISSING: {ref}")
        elif not row[0].startswith(expected_prefix):
            failures.append(
                f"WRONG TEXT: {ref}\n"
                f"  Expected prefix: '{expected_prefix}'\n"
                f"  Got:             '{row[0][:80]}'"
            )
        else:
            passed += 1

    if failures:
        msg = (
            f"Data validation FAILED ({len(failures)} check(s)):\n"
            + "\n".join(failures)
        )
        logger.error(msg)
        raise AssertionError(msg)

    logger.info("Data validation passed: %d/%d checks", passed, len(CHECKS))
    return {"passed": passed, "failed": 0}
