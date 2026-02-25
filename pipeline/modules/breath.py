# pipeline/modules/breath.py
"""
Stage 3 — Breath and phonetic analysis.

Parses Hebrew words into syllables, assigns breath weights based on
vowel and consonant properties, detects colon boundaries from Masoretic
disjunctive accent positions, and stores verse-level breath profiles.

After this module runs, back-populates colon_fingerprints on
verse_fingerprints (required before Stage 2 chiasm detection can run).
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict

import psycopg2
import psycopg2.extras
from adapters.db_adapter import batch_upsert, verse_ids_for_stage

logger = logging.getLogger(__name__)

# ── Unicode character sets ────────────────────────────────────────────────

# Full vowel points: (name, length_class, openness_score)
# dagesh and diacriticals have openness=None and are not syllable nuclei.
FULL_VOWELS: dict[str, tuple[str, str | None, float | None]] = {
    "\u05B7": ("patah",     "short",  0.85),
    "\u05B8": ("qamets",    "long",   1.00),
    "\u05B5": ("tsere",     "long",   0.65),
    "\u05B6": ("segol",     "short",  0.70),
    "\u05B4": ("hiriq",     "short",  0.45),
    "\u05B9": ("holam",     "long",   0.75),
    "\u05BA": ("holam_waw", "long",   0.75),
    "\u05BB": ("qibbuts",   "long",   0.40),
    "\u05BC": ("dagesh",    None,     None),
    "\u05C1": ("shin_dot",  None,     None),
    "\u05C2": ("sin_dot",   None,     None),
}

# Half-vowels: (name, length_class, openness_score)
HALF_VOWELS: dict[str, tuple[str, str, float]] = {
    "\u05B0": ("shewa",        "ultra-short", 0.10),
    "\u05B1": ("hataf_segol",  "ultra-short", 0.35),
    "\u05B2": ("hataf_patah",  "ultra-short", 0.40),
    "\u05B3": ("hataf_qamets", "ultra-short", 0.45),
}

ALL_VOWELS: dict[str, tuple] = {**FULL_VOWELS, **HALF_VOWELS}

# Hebrew consonants including final (sofit) forms
CONSONANTS: frozenset[str] = frozenset(
    "אבגדהוזחטיכלמנסעפצקרשת" + "ךםןףץ"
)

# Guttural consonants (affect following vowel, used for guttural_density)
GUTTURALS: frozenset[str] = frozenset("אהחע")

# Consonant onset classes: consonant → (class_name, onset_weight)
ONSET_CLASS: dict[str, tuple[str, float]] = {
    # Gutturals
    "א": ("guttural", 0.65),
    "ה": ("guttural", 0.70),
    "ח": ("guttural", 0.55),
    "ע": ("guttural", 0.60),
    # Sibilants
    "ש": ("sibilant", 0.40),
    "ס": ("sibilant", 0.40),
    "ז": ("sibilant", 0.45),
    "צ": ("sibilant", 0.35),
    # Liquids
    "ל": ("liquid",   0.90),
    "ר": ("liquid",   0.85),
    # Nasals
    "מ": ("nasal",    0.80),
    "נ": ("nasal",    0.80),
    # Approximants
    "י": ("liquid",   0.75),
    "ו": ("liquid",   0.70),
    # Voiced stops
    "ב": ("stop",     0.30),
    "ג": ("stop",     0.30),
    "ד": ("stop",     0.30),
    # Voiceless stops
    "כ": ("stop",     0.20),
    "ך": ("stop",     0.20),
    "פ": ("stop",     0.20),
    "ף": ("stop",     0.20),
    "ת": ("stop",     0.20),
    "ק": ("stop",     0.15),
    "ט": ("stop",     0.20),
}
DEFAULT_ONSET: tuple[str, float] = ("stop", 0.25)

# Masoretic disjunctive accents — each signals a colon boundary
DISJUNCTIVE_ACCENTS: frozenset[str] = frozenset({
    "\u0591",  # etnahta — primary verse-medial pause
    "\u05C3",  # sof pasuq — end of verse
    "\u0592",  # segolta
    "\u0593",  # shalshelet
    "\u0594",  # zaqef qatan
    "\u0595",  # zaqef gadol
    "\u0596",  # tifha
    "\u059A",  # yetiv
    "\u059C",  # geresh
    "\u059D",  # geresh muqdam
    "\u059E",  # gershayim
    "\u05A1",  # pazer
    "\u05A8",  # qadma
    "\u05A9",  # telisha qetana
    "\u05AC",  # ole
    "\u05AE",  # zinor
})

# ── Main entry point ──────────────────────────────────────────────────────


def run(
    conn: psycopg2.extensions.connection,
    config: dict,
) -> dict:
    """Process all pending verses: parse syllables and compute breath profiles.

    After writing syllable_tokens and breath_profiles, calls
    _backpopulate_colon_fingerprints to update verse_fingerprints.colon_fingerprints,
    which is required before Stage 2 chiasm detection can run.

    Args:
        conn: Live psycopg2 connection.
        config: Full parsed config.yml.

    Returns:
        Dict with "rows_written", "elapsed_s", "breath_profiles",
        "syllable_tokens".
    """
    t0 = time.perf_counter()
    corpus = config.get("corpus", {})
    book_nums: list[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: list[int] = corpus.get("debug_chapters", [])
    batch_size: int = config.get("breath", {}).get("batch_size", 100)

    pending = verse_ids_for_stage(
        conn, "breath_profiles", book_nums, debug_chapters
    )
    if not pending:
        logger.info("All breath profiles already computed.")
        _backpopulate_colon_fingerprints(conn, book_nums)
        return {
            "rows_written": 0,
            "elapsed_s": round(time.perf_counter() - t0, 2),
            "breath_profiles": 0,
            "syllable_tokens": 0,
        }

    logger.info("Computing breath profiles for %d verses", len(pending))

    # Fetch word tokens for pending verses (surface_form is all we need)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT wt.verse_id, wt.token_id, wt.position,
                   wt.surface_form, wt.part_of_speech
            FROM word_tokens wt
            WHERE wt.verse_id = ANY(%s)
            ORDER BY wt.verse_id, wt.position
            """,
            (pending,),
        )
        token_rows = cur.fetchall()

    by_verse: dict[int, list] = defaultdict(list)
    for row in token_rows:
        by_verse[row[0]].append(row)

    syllable_rows_all: list[tuple] = []
    profile_rows: list[tuple] = []

    for verse_id in pending:
        tokens = by_verse.get(verse_id, [])
        if not tokens:
            continue
        syllable_data, profile = _process_verse(tokens)
        syllable_rows_all.extend((verse_id,) + s for s in syllable_data)
        profile_rows.append((verse_id,) + profile)

    # Store syllable tokens
    batch_upsert(
        conn,
        """
        INSERT INTO syllable_tokens
          (verse_id, token_id, syllable_index, syllable_text, nucleus_vowel,
           vowel_openness, vowel_length, is_open, onset_class, breath_weight,
           stress_position, colon_index)
        VALUES %s
        ON CONFLICT (token_id, syllable_index) DO NOTHING
        """,
        syllable_rows_all,
        batch_size=batch_size,
    )

    # Store breath profiles
    batch_upsert(
        conn,
        """
        INSERT INTO breath_profiles
          (verse_id, mean_weight, open_ratio, guttural_density,
           colon_count, colon_boundaries, stress_positions, breath_curve)
        VALUES %s
        ON CONFLICT (verse_id) DO UPDATE
          SET mean_weight      = EXCLUDED.mean_weight,
              open_ratio       = EXCLUDED.open_ratio,
              guttural_density = EXCLUDED.guttural_density,
              colon_count      = EXCLUDED.colon_count,
              colon_boundaries = EXCLUDED.colon_boundaries,
              stress_positions = EXCLUDED.stress_positions,
              breath_curve     = EXCLUDED.breath_curve,
              computed_at      = NOW()
        """,
        profile_rows,
        batch_size=batch_size,
    )

    # Update colon_count on verses table
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE verses v
            SET colon_count = bp.colon_count
            FROM breath_profiles bp
            WHERE v.verse_id = bp.verse_id
              AND v.verse_id = ANY(%s)
            """,
            (pending,),
        )
    conn.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        "Breath profiles stored: %d, syllable tokens: %d in %.1fs",
        len(profile_rows),
        len(syllable_rows_all),
        elapsed,
    )

    _backpopulate_colon_fingerprints(conn, book_nums)

    return {
        "rows_written": len(profile_rows),
        "elapsed_s": elapsed,
        "breath_profiles": len(profile_rows),
        "syllable_tokens": len(syllable_rows_all),
    }


# ── Verse processing ──────────────────────────────────────────────────────


def _process_verse(
    tokens: list,
) -> tuple[list[tuple], tuple]:
    """Process all tokens in a verse into syllables and a breath profile.

    Args:
        tokens: List of rows from word_tokens:
            (verse_id, token_id, position, surface_form, part_of_speech).

    Returns:
        Tuple of (syllable_rows, profile_tuple) where:
            syllable_rows: list of tuples for syllable_tokens insert
                (without verse_id prefix — caller prepends it).
            profile_tuple: tuple for breath_profiles insert
                (mean_weight, open_ratio, guttural_density, colon_count,
                 colon_boundaries, stress_positions, breath_curve).
    """
    all_syllables: list[dict] = []
    colon_boundaries: list[int] = [0]
    current_colon = 1
    n_tokens = len(tokens)

    for idx, token_row in enumerate(tokens):
        _verse_id, token_id, position, surface_form, _pos_tag = token_row
        has_disjunctive = any(c in DISJUNCTIVE_ACCENTS for c in surface_form)
        syls = parse_syllables(surface_form)

        for syl in syls:
            syl["token_id"] = token_id
            syl["colon_index"] = current_colon
            syl["global_idx"] = len(all_syllables)
            all_syllables.append(syl)

        # Advance colon counter after the word that carries the accent,
        # but not after the very last token (that would create an empty colon)
        if has_disjunctive and idx < n_tokens - 1:
            current_colon += 1
            colon_boundaries.append(len(all_syllables))

    if not all_syllables:
        return [], (
            0.0, 0.0, 0.0, 1,
            [0],  # colon_boundaries — PostgreSQL INTEGER[]
            [],   # stress_positions — PostgreSQL NUMERIC[]
            [],   # breath_curve    — PostgreSQL NUMERIC[]
        )

    n_syls = len(all_syllables)
    weights = [s["breath_weight"] for s in all_syllables]
    mean_weight = round(sum(weights) / n_syls, 4)
    open_ratio = round(
        sum(1 for s in all_syllables if s.get("is_open")) / n_syls, 4
    )
    guttural_density = round(
        sum(1 for s in all_syllables if s.get("onset_class") == "guttural") / n_syls,
        4,
    )
    colon_count = current_colon

    # Normalize stress positions to [0, 1] scale
    stress_positions: list[float] = [
        round(s["global_idx"] / max(n_syls - 1, 1), 4)
        for s in all_syllables
        if s.get("is_stressed")
    ]
    breath_curve: list[float] = [
        round(s["breath_weight"], 4) for s in all_syllables
    ]

    # Build syllable_tokens rows (verse_id prepended by caller)
    syllable_rows: list[tuple] = []
    for syl in all_syllables:
        syllable_rows.append((
            syl["token_id"],
            syl["syl_idx"],
            syl["text"],
            syl.get("nucleus_vowel"),
            syl.get("vowel_openness"),
            syl.get("vowel_length"),
            syl.get("is_open"),
            syl.get("onset_class"),
            syl["breath_weight"],
            round(syl["global_idx"] / max(n_syls - 1, 1), 4),
            syl["colon_index"],
        ))

    profile: tuple = (
        mean_weight,
        open_ratio,
        guttural_density,
        colon_count,
        colon_boundaries,   # PostgreSQL INTEGER[]
        stress_positions,   # PostgreSQL NUMERIC[]
        breath_curve,       # PostgreSQL NUMERIC[]
    )
    return syllable_rows, profile


# ── Syllable parser ───────────────────────────────────────────────────────


def parse_syllables(word: str) -> list[dict]:
    """Parse a Hebrew word (with niqqud) into syllable structures.

    Each syllable is identified by its onset consonant. The parser scans
    left-to-right: on each consonant, it collects dagesh and vowel combining
    characters that follow, then determines syllable openness and computes
    a composite breath weight.

    Args:
        word: Hebrew word string, potentially with niqqud combining chars.

    Returns:
        Non-empty list of syllable dicts. Each dict has keys:
            syl_idx (int), text (str), nucleus_vowel (str|None),
            vowel_openness (float), vowel_length (str|None),
            is_open (bool), onset_class (str), breath_weight (float),
            is_stressed (bool).
    """
    syllables: list[dict] = []
    chars = list(word)
    n = len(chars)
    i = 0
    syl_idx = 0

    while i < n:
        ch = chars[i]

        # Skip combining marks and accent characters that are not consonants
        if ch not in CONSONANTS:
            i += 1
            continue

        # Onset consonant
        onset_char = ch
        onset_class, onset_weight = ONSET_CLASS.get(onset_char, DEFAULT_ONSET)
        syl_text = ch
        i += 1

        # Consume dagesh, shin dot, sin dot immediately after onset
        while i < n and chars[i] in ("\u05BC", "\u05C1", "\u05C2", "\u05BD"):
            syl_text += chars[i]
            i += 1

        # Collect nucleus vowel
        nucleus_vowel: str | None = None
        vowel_openness: float = 0.0
        vowel_length: str | None = None

        if i < n and chars[i] in ALL_VOWELS:
            vowel_ch = chars[i]
            syl_text += vowel_ch
            vdata = ALL_VOWELS[vowel_ch]
            if vdata[2] is not None:  # skip dagesh/diacritical entries
                vowel_name, vowel_length, vowel_openness = vdata
                nucleus_vowel = vowel_ch
            i += 1

        # Consume any trailing non-consonant, non-accent characters
        while (
            i < n
            and chars[i] not in CONSONANTS
            and chars[i] not in DISJUNCTIVE_ACCENTS
        ):
            if chars[i] in ALL_VOWELS:
                vdata = ALL_VOWELS[chars[i]]
                # Accept a second vowel indicator only if no vowel found yet
                if vdata[2] is not None and vowel_openness == 0.0:
                    _vname, vowel_length, vowel_openness = vdata
                    nucleus_vowel = chars[i]
            syl_text += chars[i]
            i += 1

        # Open syllable: ends in a vowel (next char is consonant or end of word)
        is_open: bool = vowel_length in ("long", "short")

        breath_weight = _compute_breath_weight(
            vowel_openness=vowel_openness,
            vowel_length=vowel_length,
            is_open=is_open,
            onset_weight=onset_weight,
        )

        syllables.append({
            "syl_idx":        syl_idx,
            "text":           syl_text,
            "nucleus_vowel":  nucleus_vowel,
            "vowel_openness": vowel_openness,
            "vowel_length":   vowel_length,
            "is_open":        is_open,
            "onset_class":    onset_class,
            "breath_weight":  breath_weight,
            "is_stressed":    vowel_length == "long",
        })
        syl_idx += 1

    # Guarantee at least one syllable for single-consonant / unvocalized words
    if not syllables:
        first_consonant = next((c for c in word if c in CONSONANTS), "")
        onset_class, onset_weight = ONSET_CLASS.get(
            first_consonant, DEFAULT_ONSET
        )
        syllables.append({
            "syl_idx":        0,
            "text":           word,
            "nucleus_vowel":  None,
            "vowel_openness": 0.3,
            "vowel_length":   "short",
            "is_open":        True,
            "onset_class":    onset_class,
            "breath_weight":  _compute_breath_weight(
                0.3, "short", True, onset_weight
            ),
            "is_stressed":    False,
        })

    return syllables


def _compute_breath_weight(
    vowel_openness: float,
    vowel_length: str | None,
    is_open: bool,
    onset_weight: float,
) -> float:
    """Compute the composite breath weight for a syllable.

    Formula:
        breath_weight = 0.40 * vowel_openness
                      + 0.25 * vowel_length_score
                      + 0.20 * syllable_openness_score
                      + 0.15 * onset_weight

    Args:
        vowel_openness: Openness score of the nucleus vowel (0.0–1.0).
        vowel_length: Length class string ("long", "short", "ultra-short").
        is_open: True if the syllable is open (CV), False if closed (CVC).
        onset_weight: Sonority weight of the onset consonant.

    Returns:
        Float breath weight in [0.0, 1.0].
    """
    length_score = _vowel_length_score(vowel_length)
    openness_score = 1.0 if is_open else 0.4
    weight = (
        0.40 * vowel_openness
        + 0.25 * length_score
        + 0.20 * openness_score
        + 0.15 * onset_weight
    )
    return round(min(max(weight, 0.0), 1.0), 4)


def _vowel_length_score(length: str | None) -> float:
    """Return a normalized score for a vowel length class.

    Args:
        length: Length class string or None.

    Returns:
        Float in (0.0, 1.0].
    """
    return {
        "long":        1.00,
        "short":       0.65,
        "ultra-short": 0.25,
        "shewa":       0.10,
    }.get(length or "short", 0.50)


# ── Back-population of colon fingerprints ─────────────────────────────────


def _backpopulate_colon_fingerprints(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
) -> None:
    """Aggregate syllable data per colon and update verse_fingerprints.

    Computes colon-level summary statistics from syllable_tokens and writes
    them into verse_fingerprints.colon_fingerprints as a JSONB array. This
    data is required by modules/chiasm.py before it can run.

    Each element of the JSON array has the structure:
        {"colon": int, "density": float, "sonority": float, "mean_weight": float}

    Args:
        conn: Live psycopg2 connection.
        book_nums: Books to process (filter).
    """
    logger.info("Back-populating colon fingerprints for book_nums=%s", book_nums)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT st.verse_id,
                   st.colon_index,
                   AVG(st.vowel_openness) AS mean_openness,
                   AVG(st.breath_weight)  AS mean_weight,
                   COUNT(*)               AS syllable_count
            FROM syllable_tokens st
            JOIN verses v ON st.verse_id = v.verse_id
            WHERE v.book_num = ANY(%s)
            GROUP BY st.verse_id, st.colon_index
            ORDER BY st.verse_id, st.colon_index
            """,
            (book_nums,),
        )
        rows = cur.fetchall()

    by_verse: dict[int, list[dict]] = defaultdict(list)
    for verse_id, colon_idx, mean_openness, mean_weight, syl_count in rows:
        by_verse[verse_id].append({
            "colon":       colon_idx,
            "density":     round(float(syl_count), 2),
            "sonority":    round(float(mean_openness or 0.0), 4),
            "mean_weight": round(float(mean_weight or 0.0), 4),
        })

    if not by_verse:
        logger.info("No colon data found for back-population.")
        return

    update_rows = [
        (json.dumps(colons), verse_id)
        for verse_id, colons in by_verse.items()
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE verse_fingerprints
            SET colon_fingerprints = %s
            WHERE verse_id = %s
            """,
            update_rows,
        )
    conn.commit()
    logger.info(
        "Colon fingerprints back-populated for %d verses", len(update_rows)
    )
