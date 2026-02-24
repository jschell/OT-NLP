# pipeline/modules/ingest_translations.py
"""
Stage 1 — Translation ingest.

Reads all translation sources configured in config.yml via their adapters,
then upserts verse text into the `translations` PostgreSQL table.

Entry point:
    run(conn, config) -> {"rows_written": int, "elapsed_s": float}

Sequencing note:
    This module writes to `translations`, which has a foreign key on `verses`.
    If `verses` is empty (pre-Stage-2), the module logs a warning and returns
    rows_written=0 without error. Re-run after Stage 2 populates `verses`.
"""

from __future__ import annotations

import logging
import time

import psycopg2.extensions
import psycopg2.extras
from adapters.translation_adapter import adapter_factory

logger = logging.getLogger(__name__)


def run(
    conn: psycopg2.extensions.connection,
    config: dict,
) -> dict:
    """
    Ingest all configured translations into the translations table.

    Args:
        conn:   Live psycopg2 connection to the psalms database.
        config: Full parsed config.yml dict.

    Returns:
        {"rows_written": int, "elapsed_s": float}
    """
    t0 = time.monotonic()
    sources = config.get("translations", {}).get("sources", [])
    corpus_books = [b["book_num"] for b in config.get("corpus", {}).get("books", [])]

    if not sources:
        logger.warning("No translation sources configured. Skipping.")
        return {"rows_written": 0, "elapsed_s": 0.0}

    total_written = 0

    for source in sources:
        t_id: str = source["id"]
        logger.info("Ingesting translation: %s", t_id)

        # adapter_factory raises ValueError for unknown formats —
        # let it propagate so the caller knows about misconfiguration
        adapter = adapter_factory(source)

        for book_num in corpus_books:
            rows = _ingest_book(conn, adapter, t_id, book_num)
            total_written += rows
            logger.info("  %s book %s: %d rows written", t_id, book_num, rows)

    elapsed = time.monotonic() - t0
    logger.info("Translation ingest complete: %d rows in %.2fs", total_written, elapsed)
    return {"rows_written": total_written, "elapsed_s": round(elapsed, 3)}


def _ingest_book(
    conn: psycopg2.extensions.connection,
    adapter: object,
    translation_key: str,
    book_num: int,
) -> int:
    """
    Fetch verses for one book from the adapter and upsert into translations.

    Args:
        conn:            Live database connection.
        adapter:         TranslationAdapter instance.
        translation_key: Short ID string e.g. 'KJV'.
        book_num:        BHSA book number.

    Returns:
        Number of rows upserted.
    """
    # Look up (chapter, verse_num) -> verse_id for this book
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chapter, verse_num, verse_id FROM verses WHERE book_num = %s",
            (book_num,),
        )
        verse_lookup: dict[tuple[int, int], int] = {
            (int(r[0]), int(r[1])): int(r[2]) for r in cur.fetchall()
        }

    if not verse_lookup:
        logger.warning(
            "verses table is empty for book_num=%s. "
            "Re-run after Stage 2 populates verses.",
            book_num,
        )
        return 0

    rows_to_insert: list[tuple[int, str, str]] = []

    for (chapter, verse_num), verse_id in verse_lookup.items():
        text = adapter.get_verse(book_num, chapter, verse_num)  # type: ignore[attr-defined]
        if text is None:
            logger.debug(
                "  %s %s:%s:%s — not found in source, skipping",
                translation_key,
                book_num,
                chapter,
                verse_num,
            )
            continue
        rows_to_insert.append((verse_id, translation_key, text))

    if not rows_to_insert:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO translations (verse_id, translation_key, verse_text)
            VALUES %s
            ON CONFLICT (verse_id, translation_key)
            DO UPDATE SET verse_text = EXCLUDED.verse_text
            """,
            rows_to_insert,
        )
    conn.commit()
    return len(rows_to_insert)
