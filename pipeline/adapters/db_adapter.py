# pipeline/adapters/db_adapter.py
"""
Database adapter utilities.

Provides resumable upsert operations used by all pipeline modules.
The core pattern: before processing, query which items already have
target table rows, then skip those. This makes every stage
idempotent and resumable after interruption.
"""

from __future__ import annotations

import logging

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_processed_verse_ids(
    conn: psycopg2.extensions.connection,
    table: str,
    id_column: str = "verse_id",
) -> set[int]:
    """Return the set of verse_ids that already have rows in table.

    Used to skip already-computed verses when resuming a pipeline
    stage after interruption.

    Args:
        conn: Live psycopg2 connection.
        table: Target table name to query.
        id_column: Column name to read (default "verse_id").

    Returns:
        Set of integer verse IDs already present in the table.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT {id_column} FROM {table}")
        return {row[0] for row in cur.fetchall()}


def get_all_verse_ids(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
    debug_chapters: list[int] | None = None,
) -> list[int]:
    """Return all verse_ids for the given books, optionally filtered.

    Args:
        conn: Live psycopg2 connection.
        book_nums: List of book numbers to include.
        debug_chapters: If provided, restrict to these chapter numbers.

    Returns:
        Sorted list of verse_id integers.
    """
    with conn.cursor() as cur:
        if debug_chapters:
            cur.execute(
                """
                SELECT verse_id FROM verses
                WHERE book_num = ANY(%s) AND chapter = ANY(%s)
                ORDER BY verse_id
                """,
                (book_nums, debug_chapters),
            )
        else:
            cur.execute(
                """
                SELECT verse_id FROM verses
                WHERE book_num = ANY(%s)
                ORDER BY verse_id
                """,
                (book_nums,),
            )
        return [row[0] for row in cur.fetchall()]


def verse_ids_for_stage(
    conn: psycopg2.extensions.connection,
    target_table: str,
    book_nums: list[int],
    debug_chapters: list[int] | None = None,
) -> list[int]:
    """Return verse_ids that need processing for a given stage.

    Subtracts already-processed IDs from the full corpus set. Call
    this at the start of each module's run() to implement resumability.

    Args:
        conn: Live psycopg2 connection.
        target_table: Table to check for existing rows.
        book_nums: Books in scope.
        debug_chapters: Optional chapter filter for faster dev runs.

    Returns:
        Sorted list of verse_ids that still need processing.
    """
    all_ids = set(get_all_verse_ids(conn, book_nums, debug_chapters))
    done_ids = get_processed_verse_ids(conn, target_table)
    pending = sorted(all_ids - done_ids)
    logger.info(
        "Stage targeting %s: %d total, %d done, %d pending",
        target_table,
        len(all_ids),
        len(done_ids),
        len(pending),
    )
    return pending


def batch_upsert(
    conn: psycopg2.extensions.connection,
    query: str,
    rows: list[tuple],
    batch_size: int = 100,
) -> int:
    """Execute a psycopg2 execute_values upsert in batches.

    Commits after each batch so progress is saved incrementally.

    Args:
        conn: Live psycopg2 connection.
        query: SQL with VALUES %s placeholder.
        rows: List of tuples to insert.
        batch_size: Rows per commit batch.

    Returns:
        Total number of rows processed.
    """
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, batch)
        conn.commit()
        total += len(batch)
    return total
