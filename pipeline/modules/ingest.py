# pipeline/modules/ingest.py
"""
Stage 2 — BHSA ingest.

Extracts morphological data from the BHSA text-fabric corpus and writes
to `verses` and `word_tokens` tables. Designed to be resumable: if
interrupted, re-running will skip already-ingested verses.
"""

from __future__ import annotations

import logging

import psycopg2
import psycopg2.extras
from adapters.db_adapter import batch_upsert

logger = logging.getLogger(__name__)

# BHSA part-of-speech codes → human-readable labels
POS_MAP: dict[str, str] = {
    "subs": "noun",
    "verb": "verb",
    "adjv": "adjective",
    "advb": "adverb",
    "prep": "preposition",
    "conj": "conjunction",
    "prps": "pronoun_personal",
    "prde": "pronoun_demonstrative",
    "prin": "pronoun_interrogative",
    "nmpr": "proper_noun",
    "intj": "interjection",
    "nega": "negative_particle",
    "inrg": "interrogative_particle",
    "art": "article",
}

# BHSA verbal stem codes → human-readable labels
STEM_MAP: dict[str, str] = {
    "qal": "qal",
    "nif": "niphal",
    "piel": "piel",
    "pual": "pual",
    "hif": "hiphil",
    "hof": "hophal",
    "hit": "hitpael",
    "htpo": "hitpolel",
    "nit": "nitpael",
}

# BHSA book name → book number for supported corpus books
BOOK_NUM_MAP: dict[str, int] = {
    "Psalms": 19,
    "Isaiah": 23,
    "Job": 18,
    "Lamentations": 25,
}


def _count_prefixes_from_pfm(pfm: str | None) -> int:
    """Return 1 if pfm indicates a prefix morpheme, else 0.

    Args:
        pfm: The pfm feature value from BHSA (may be None, empty, or
            the string 'absent' when no prefix is present).

    Returns:
        1 if a prefix morpheme is indicated, 0 otherwise.
    """
    if not pfm or pfm == "absent":
        return 0
    return 1


def run(
    conn: psycopg2.extensions.connection,
    config: dict,
) -> dict:
    """Ingest BHSA morphological data into `verses` and `word_tokens`.

    Reads the BHSA corpus via text-fabric and upserts verse rows and
    per-word token rows. Uses ON CONFLICT DO UPDATE so re-runs are safe.

    Args:
        conn: Live psycopg2 connection to psalms_db.
        config: Full parsed config.yml dict.

    Returns:
        Dict with keys "rows_written" (total token rows), "elapsed_s",
        "verses" (verse row count), "word_tokens" (token row count).
    """
    import time

    from tf.fabric import Fabric

    t0 = time.perf_counter()

    bhsa_path: str = config["bhsa"]["data_path"]
    corpus = config.get("corpus", {})
    book_nums: list[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: list[int] = corpus.get("debug_chapters", [])
    batch_size: int = config.get("fingerprint", {}).get("batch_size", 100)

    logger.info("Loading BHSA from %s", bhsa_path)
    TF = Fabric(
        locations=[f"{bhsa_path}/github/ETCBC/bhsa/tf/c"],
        silent=True,  # type: ignore[arg-type]
    )
    api = TF.load(
        "book chapter verse "
        "g_word_utf8 lex sp "
        "prs uvf vs "
        "pfm",
        silent=True,  # type: ignore[arg-type]
    )
    # Reverse map: book_num → BHSA book name
    num_to_name = {v: k for k, v in BOOK_NUM_MAP.items()}

    total_verses = 0
    total_tokens = 0

    for book_num in book_nums:
        bhsa_name = num_to_name.get(book_num)
        if not bhsa_name:
            logger.warning("No BHSA name mapping for book_num=%d", book_num)
            continue
        v, t = _ingest_book(
            conn, api, book_num, bhsa_name, debug_chapters, batch_size
        )
        total_verses += v
        total_tokens += t

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        "Ingest complete: %d verses, %d tokens in %.1fs",
        total_verses,
        total_tokens,
        elapsed,
    )
    return {
        "rows_written": total_tokens,
        "elapsed_s": elapsed,
        "verses": total_verses,
        "word_tokens": total_tokens,
    }


def _ingest_book(
    conn: psycopg2.extensions.connection,
    api: object,
    book_num: int,
    bhsa_book_name: str,
    debug_chapters: list[int],
    batch_size: int,
) -> tuple[int, int]:
    """Ingest one book from the BHSA corpus.

    Args:
        conn: Live psycopg2 connection.
        api: Loaded text-fabric API object (has .F, .T, .L).
        book_num: Numeric book identifier for the DB.
        bhsa_book_name: BHSA internal book name string.
        debug_chapters: If non-empty, only ingest these chapters.
        batch_size: Rows per commit batch.

    Returns:
        Tuple of (verse_count, token_count) inserted/updated.
    """
    F, T, L = api.F, api.T, api.L  # type: ignore[attr-defined]

    book_node = next(
        n for n in F.otype.s("book") if T.bookName(n) == bhsa_book_name
    )
    verse_nodes = L.d(book_node, "verse")

    verse_rows: list[tuple] = []
    token_rows_by_verse: dict[tuple[int, int], list[tuple]] = {}

    for v_node in verse_nodes:
        chapter = int(F.chapter.v(v_node))
        verse_num = int(F.verse.v(v_node))

        if debug_chapters and chapter not in debug_chapters:
            continue

        words = L.d(v_node, "word")
        surface_forms = [F.g_word_utf8.v(w) for w in words]
        hebrew_text = " ".join(surface_forms)
        verse_rows.append((book_num, chapter, verse_num, hebrew_text))

        tokens: list[tuple] = []
        for pos, w_node in enumerate(words, start=1):
            pos_tag = F.sp.v(w_node) or ""
            human_pos = POS_MAP.get(pos_tag, pos_tag)
            is_verb = pos_tag == "verb"
            is_noun = pos_tag in ("subs", "nmpr")
            stem: str | None = (
                STEM_MAP.get(F.vs.v(w_node)) if is_verb else None
            )
            pfm_val = F.pfm.v(w_node)
            prefix_count = _count_prefixes_from_pfm(pfm_val)
            has_suffix = bool(F.prs.v(w_node) or F.uvf.v(w_node))
            morpheme_count = prefix_count + 1 + (1 if has_suffix else 0)

            tokens.append((
                pos,
                F.g_word_utf8.v(w_node),
                F.lex.v(w_node),
                human_pos,
                morpheme_count,
                is_verb,
                is_noun,
                stem,
            ))
        token_rows_by_verse[(chapter, verse_num)] = tokens

    # Upsert verses, capture generated verse_ids.
    # execute_values(fetch=True) returns the RETURNING rows itself and exhausts
    # the cursor — capture the return value rather than calling cur.fetchall().
    logger.info("Upserting %d verse rows for book %d", len(verse_rows), book_num)
    with conn.cursor() as cur:
        returned = psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO verses (book_num, chapter, verse_num, hebrew_text)
            VALUES %s
            ON CONFLICT (book_num, chapter, verse_num) DO UPDATE
              SET hebrew_text = EXCLUDED.hebrew_text
            RETURNING chapter, verse_num, verse_id
            """,
            verse_rows,
            fetch=True,
        )
    verse_id_map: dict[tuple[int, int], int] = {
        (r[0], r[1]): r[2] for r in returned
    }
    conn.commit()

    # Build flat token rows with verse_id
    all_token_rows: list[tuple] = []
    for (chapter, verse_num), tokens in token_rows_by_verse.items():
        verse_id = verse_id_map.get((chapter, verse_num))
        if verse_id is None:
            continue
        for t in tokens:
            all_token_rows.append((verse_id, *t))

    logger.info(
        "Upserting %d token rows for book %d", len(all_token_rows), book_num
    )
    batch_upsert(
        conn,
        """
        INSERT INTO word_tokens
          (verse_id, position, surface_form, lexeme, part_of_speech,
           morpheme_count, is_verb, is_noun, stem)
        VALUES %s
        ON CONFLICT (verse_id, position) DO UPDATE
          SET surface_form   = EXCLUDED.surface_form,
              lexeme         = EXCLUDED.lexeme,
              part_of_speech = EXCLUDED.part_of_speech,
              morpheme_count = EXCLUDED.morpheme_count
        """,
        all_token_rows,
        batch_size=batch_size,
    )

    logger.info("Book %d ingested.", book_num)
    return len(verse_rows), len(all_token_rows)
