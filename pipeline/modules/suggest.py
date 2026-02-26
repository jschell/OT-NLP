# pipeline/modules/suggest.py
"""
Stage 5 — LLM-assisted suggestion generation.

For each verse exceeding the configured composite_deviation threshold,
generates one or more alternative English translations that better preserve
the Hebrew phonetic and structural texture.  Each suggestion is scored with
the same metrics as Stage 4 translations.

If LLM_PROVIDER is 'none' (the default), the stage exits cleanly with
{"generated": 0, "skipped": "no_provider"} and writes no rows.
"""

from __future__ import annotations

import logging
import time

import psycopg2
import psycopg2.extras
from adapters.llm_adapter import LLMAdapter
from adapters.phoneme_adapter import english_fingerprint

from modules.score import _compute_breath_alignment

logger = logging.getLogger(__name__)

# Bump this string whenever the prompt template changes to preserve provenance.
PROMPT_VERSION = "v1"


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Generate and store LLM suggestions for high-deviation verses.

    Returns {"generated": int, "skipped": int | str, "elapsed_s": float}.
    Skips cleanly (no DB writes) if LLM provider is 'none'.
    """
    t0 = time.monotonic()

    adapter = LLMAdapter.from_config(config)
    if not adapter.is_enabled():
        logger.info("LLM provider is 'none' — suggestion stage skipped.")
        return {"generated": 0, "skipped": "no_provider"}

    llm_cfg = config.get("llm", {})
    max_tokens: int = llm_cfg.get("max_tokens", 256)
    filter_cfg = llm_cfg.get("suggestion_filter", {})
    min_deviation: float = filter_cfg.get("min_composite_deviation", 0.15)
    max_per_verse: int = filter_cfg.get("max_suggestions_per_verse", 3)

    corpus = config.get("corpus", {})
    book_nums: list[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: list[int] = corpus.get("debug_chapters", [])

    w = config.get("scoring", {}).get("deviation_weights", {})
    dev_weights: dict[str, float] = {
        "density": w.get("density", 0.35),
        "morpheme": w.get("morpheme", 0.25),
        "sonority": w.get("sonority", 0.20),
        "compression": w.get("compression", 0.20),
    }

    candidates = _get_suggestion_candidates(
        conn, book_nums, debug_chapters, min_deviation
    )
    logger.info(
        "Found %d verse × translation pairs above threshold %.3f",
        len(candidates),
        min_deviation,
    )

    generated = 0
    skipped = 0

    for candidate in candidates:
        verse_id = candidate["verse_id"]
        translation_key = candidate["translation_key"]

        if _existing_suggestion_count(conn, verse_id, translation_key) >= max_per_verse:
            continue

        prompt = _build_prompt(candidate)

        try:
            suggestion_text = adapter.ask(prompt, max_tokens=max_tokens)
        except Exception:
            logger.exception(
                "LLM error for verse %d / %s — skipping", verse_id, translation_key
            )
            skipped += 1
            continue

        if not suggestion_text:
            skipped += 1
            continue

        heb_fp = candidate["heb_fingerprint"]
        heb_breath = candidate["heb_breath"]
        eng_fp = english_fingerprint(suggestion_text)

        composite_deviation = _weighted_deviation(heb_fp, eng_fp, dev_weights)
        stress_align, weight_match = _compute_breath_alignment(
            heb_breath, suggestion_text
        )
        breath_alignment = round(stress_align * 0.60 + weight_match * 0.40, 4)
        improvement_delta = round(
            candidate["current_composite_deviation"] - composite_deviation, 4
        )

        _store_suggestion(
            conn,
            {
                "verse_id": verse_id,
                "translation_key": translation_key,
                "suggested_text": suggestion_text,
                "composite_deviation": round(composite_deviation, 4),
                "breath_alignment": breath_alignment,
                "improvement_delta": improvement_delta,
                "llm_provider": adapter.provider,
                "llm_model": adapter.model,
                "prompt_version": PROMPT_VERSION,
            },
        )
        generated += 1

    elapsed = round(time.monotonic() - t0, 2)
    logger.info(
        "Suggestions generated: %d, skipped: %d in %.2fs",
        generated,
        skipped,
        elapsed,
    )
    return {"generated": generated, "skipped": skipped, "elapsed_s": elapsed}


# ── Prompt construction ───────────────────────────────────────────────────────


def _build_prompt(candidate: dict) -> str:
    """
    Build a breath-aware translation improvement prompt.

    Provides the LLM with the Hebrew verse reference, existing translation,
    deviation score, and specific phonetic guidance derived from the Hebrew
    breath profile.
    """
    ref = f"{candidate['book_name']} {candidate['chapter']}:{candidate['verse_num']}"
    hebrew = candidate["hebrew_text"]
    translation_key = candidate["translation_key"]
    existing_text = candidate["existing_translation"]
    deviation = candidate["current_composite_deviation"]
    mean_weight: float = candidate["heb_breath"].get("mean_weight", 0.5)
    guttural_density: float = candidate.get("guttural_density", 0.0)

    phonetic_notes: list[str] = []
    if mean_weight > 0.65:
        phonetic_notes.append(
            "predominantly open, resonant vowels (high phonetic weight)"
        )
    elif mean_weight < 0.40:
        phonetic_notes.append("compressed, closed syllables (low phonetic weight)")
    else:
        phonetic_notes.append("balanced mix of open and closed syllables")

    if guttural_density > 0.25:
        phonetic_notes.append(
            "frequent guttural consonants (breathy, aspirated quality)"
        )

    phonetic_desc = (
        "; ".join(phonetic_notes) if phonetic_notes else ("standard phonetic texture")
    )

    if mean_weight > 0.55:
        word_preference = "resonant, open-vowel words (praise, glory, flowing)"
    else:
        word_preference = "crisp, closed-syllable words"

    prompt = (
        f"You are a Biblical Hebrew translator with expertise in phonetic"
        f" and rhythmic accuracy.\n\n"
        f"TASK: Suggest a single improved English translation of the following"
        f" verse that better preserves the phonetic and rhythmic texture of"
        f" the original Hebrew.\n\n"
        f"VERSE: {ref}\n"
        f"HEBREW TEXT: {hebrew}\n"
        f"CURRENT TRANSLATION ({translation_key}): {existing_text}\n"
        f"CURRENT DEVIATION SCORE: {deviation:.3f}"
        f" (lower is better; 0.0 = perfect structural match)\n"
        f"HEBREW PHONETIC CHARACTER: {phonetic_desc}\n\n"
        f"REQUIREMENTS:\n"
        f"1. Preserve the meaning of the original translation faithfully"
        f" — do not add or remove ideas\n"
        f"2. Where meaning allows, prefer {word_preference}\n"
        f"3. Maintain similar clause structure and rhythm to the Hebrew\n"
        f"4. Do not use archaic language unless it appears in the existing"
        f" translation\n"
        f"5. Return ONLY the translation text with no commentary,"
        f" explanation, or quotation marks\n\n"
        f"TRANSLATION:"
    )
    return prompt


# ── Database helpers ──────────────────────────────────────────────────────────


def _get_suggestion_candidates(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
    debug_chapters: list[int],
    min_deviation: float,
) -> list[dict]:
    """Fetch high-deviation verse × translation pairs with full context."""
    q = """
        SELECT
            ts.verse_id, ts.translation_key,
            ts.composite_deviation,
            v.hebrew_text, v.chapter, v.verse_num, b.book_name,
            t.verse_text AS existing_translation,
            vf.syllable_density, vf.morpheme_ratio,
            vf.sonority_score, vf.clause_compression,
            bp.breath_curve, bp.stress_positions,
            bp.mean_weight, bp.guttural_density
        FROM translation_scores ts
        JOIN verses v ON ts.verse_id = v.verse_id
        JOIN books b ON v.book_num = b.book_num
        JOIN translations t
          ON t.verse_id = ts.verse_id
         AND t.translation_key = ts.translation_key
        JOIN verse_fingerprints vf ON vf.verse_id = ts.verse_id
        JOIN breath_profiles bp ON bp.verse_id = ts.verse_id
        WHERE v.book_num = ANY(%s)
          AND ts.composite_deviation >= %s
    """
    params: list = [book_nums, min_deviation]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    q += " ORDER BY ts.composite_deviation DESC"

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()

    candidates: list[dict] = []
    for row in rows:
        d = dict(zip(cols, row, strict=True))
        candidates.append(
            {
                "verse_id": d["verse_id"],
                "translation_key": d["translation_key"],
                "current_composite_deviation": float(d["composite_deviation"]),
                "hebrew_text": d["hebrew_text"],
                "chapter": d["chapter"],
                "verse_num": d["verse_num"],
                "book_name": d["book_name"],
                "existing_translation": d["existing_translation"],
                "guttural_density": float(d.get("guttural_density") or 0),
                "heb_fingerprint": {
                    "syllable_density": float(d["syllable_density"] or 0),
                    "morpheme_ratio": float(d["morpheme_ratio"] or 0),
                    "sonority_score": float(d["sonority_score"] or 0),
                    "clause_compression": float(d["clause_compression"] or 0),
                },
                "heb_breath": {
                    "breath_curve": d["breath_curve"] or [],
                    "stress_positions": [
                        float(x) for x in (d["stress_positions"] or [])
                    ],
                    "mean_weight": float(d["mean_weight"] or 0),
                },
            }
        )
    return candidates


def _existing_suggestion_count(
    conn: psycopg2.extensions.connection,
    verse_id: int,
    translation_key: str,
) -> int:
    """Return the number of suggestions already stored for this pair."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM suggestions"
            " WHERE verse_id = %s AND translation_key = %s",
            (verse_id, translation_key),
        )
        result = cur.fetchone()
        return int(result[0]) if result else 0


def _weighted_deviation(
    heb_fp: dict[str, float],
    eng_fp: dict[str, float],
    weights: dict[str, float],
) -> float:
    """
    Compute weighted L1 deviation between Hebrew and English fingerprints.

    Uses the same formula as Stage 4: sum of w_i * |heb_i - eng_i|.
    """
    dims = [
        "syllable_density",
        "morpheme_ratio",
        "sonority_score",
        "clause_compression",
    ]
    weight_keys = ["density", "morpheme", "sonority", "compression"]
    total = 0.0
    for dim, wk in zip(dims, weight_keys, strict=True):
        total += abs(heb_fp.get(dim, 0.0) - eng_fp.get(dim, 0.0)) * weights.get(
            wk, 0.25
        )
    return total


def _store_suggestion(conn: psycopg2.extensions.connection, s: dict) -> None:
    """Insert one suggestion row; commits immediately."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO suggestions
              (verse_id, translation_key, suggested_text,
               composite_deviation, breath_alignment, improvement_delta,
               llm_provider, llm_model, prompt_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                s["verse_id"],
                s["translation_key"],
                s["suggested_text"],
                s["composite_deviation"],
                s["breath_alignment"],
                s["improvement_delta"],
                s["llm_provider"],
                s["llm_model"],
                s["prompt_version"],
            ),
        )
    conn.commit()
