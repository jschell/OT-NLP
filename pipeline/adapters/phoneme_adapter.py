# pipeline/adapters/phoneme_adapter.py
"""
English phoneme adapter.

Uses the CMU Pronouncing Dictionary via the `pronouncing` library for
known words, with a heuristic syllable counter fallback for unknown words.

No external API calls — CMU dictionary is bundled with the library.
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache

import pronouncing

logger = logging.getLogger(__name__)

# CMU ARPABET vowel codes → openness score (approximate IPA mapping).
# Stress digit stripped before lookup.
VOWEL_OPENNESS: dict[str, float] = {
    # Open vowels (low, back)
    "AA": 1.00,  # father
    "AE": 0.85,  # cat
    "AH": 0.80,  # strut / schwa
    # Mid vowels
    "AO": 0.75,  # thought
    "AW": 0.72,  # mouth
    "AY": 0.78,  # price
    "EH": 0.65,  # dress
    "EY": 0.60,  # face
    "OW": 0.70,  # goat
    "OY": 0.68,  # choice
    # Close vowels (high, front/back)
    "IH": 0.45,  # kit
    "IY": 0.40,  # fleece
    "UH": 0.42,  # foot
    "UW": 0.38,  # goose
    # Reduced vowel
    "ER": 0.50,  # nurse
}
DEFAULT_VOWEL_OPENNESS: float = 0.55

# Simplified English onset sonority by first character
_ENG_SONORITY: dict[str, float] = {
    "l": 0.90,
    "r": 0.85,
    "m": 0.80,
    "n": 0.78,
    "w": 0.75,
    "y": 0.70,
    "h": 0.65,
    "v": 0.60,
    "z": 0.55,
    "f": 0.50,
    "s": 0.48,
    "b": 0.35,
    "d": 0.30,
    "g": 0.25,
    "p": 0.20,
    "t": 0.18,
    "k": 0.15,
}


def get_syllables_and_stress(word: str) -> tuple[int, list[float]]:
    """Return (syllable_count, stress_weights) for an English word.

    stress_weights is a list of per-syllable breath weights derived from
    vowel openness and CMU stress marks (0=unstressed, 1=primary,
    2=secondary). Falls back to heuristic counter for unknown words.

    Args:
        word: A single English word (punctuation stripped internally).

    Returns:
        Tuple of (syllable_count, per_syllable_weight_list).
    """
    word_clean = re.sub(r"[^\w]", "", word.lower())
    phones_list = pronouncing.phones_for_word(word_clean)
    if phones_list:
        return _parse_cmu_phones(phones_list[0])
    return _heuristic_syllables(word_clean)


def _parse_cmu_phones(phones_str: str) -> tuple[int, list[float]]:
    """Parse a CMU phones string into syllable count and per-syllable weights.

    Args:
        phones_str: Space-separated ARPABET phoneme string with stress digits.

    Returns:
        Tuple of (syllable_count, weight_list).
    """
    tokens = phones_str.split()
    syllable_weights: list[float] = []

    for token in tokens:
        if token and token[-1].isdigit():
            stress = int(token[-1])
            vowel_code = token[:-1]
            openness = VOWEL_OPENNESS.get(vowel_code, DEFAULT_VOWEL_OPENNESS)
            stress_multiplier = {0: 0.7, 1: 1.0, 2: 0.85}.get(stress, 0.75)
            syllable_weights.append(round(openness * stress_multiplier, 4))

    if not syllable_weights:
        return 1, [DEFAULT_VOWEL_OPENNESS]

    return len(syllable_weights), syllable_weights


def _heuristic_syllables(word: str) -> tuple[int, list[float]]:
    """Heuristic syllable counter for words not in the CMU dictionary.

    Rules: count vowel groups (aeiouy), adjusting for silent-e endings.

    Args:
        word: Lowercase alphabetic word string.

    Returns:
        Tuple of (syllable_count, weight_list).
    """
    word = word.lower()
    if len(word) > 2 and word.endswith("e") and word[-2] not in "aeiouy":
        word = word[:-1]
    if len(word) > 3 and word.endswith("ed") and word[-3] not in "aeiouy":
        word = word[:-2]
    vowel_groups = re.findall(r"[aeiouy]+", word)
    count = max(1, len(vowel_groups))
    return count, [DEFAULT_VOWEL_OPENNESS] * count


@lru_cache(maxsize=10_000)
def syllable_count(word: str) -> int:
    """Return cached syllable count for a single word.

    Args:
        word: English word string.

    Returns:
        Integer syllable count (>= 1).
    """
    return get_syllables_and_stress(word)[0]


def english_fingerprint(text: str) -> dict[str, float]:
    """Compute the 4-dimensional style fingerprint for an English verse.

    Returns a dict with keys: syllable_density, morpheme_ratio,
    sonority_score, clause_compression.

    Args:
        text: Full English verse string.

    Returns:
        Dict with four float dimensions. Returns all-zero dict for empty input.
    """
    words = _tokenize(text)
    zero: dict[str, float] = {
        "syllable_density": 0.0,
        "morpheme_ratio": 0.0,
        "sonority_score": 0.0,
        "clause_compression": 0.0,
    }
    if not words:
        return zero

    n = len(words)

    syl_counts = [syllable_count(w) for w in words]
    syllable_density = sum(syl_counts) / n

    # Morpheme ratio proxy: mean word character length / 4.0
    morpheme_ratio = min(sum(len(w) for w in words) / (n * 4.0), 4.0)

    sonority_values = [_english_onset_sonority(w) for w in words]
    sonority_score = sum(sonority_values) / len(sonority_values)

    clause_separators = text.count(",") + text.count(";") + 1
    clause_compression = n / clause_separators

    return {
        "syllable_density": round(syllable_density, 4),
        "morpheme_ratio": round(morpheme_ratio, 4),
        "sonority_score": round(sonority_score, 4),
        "clause_compression": round(clause_compression, 4),
    }


def english_breath_weights(text: str) -> list[float]:
    """Return a flat list of per-syllable breath weights for an English verse.

    Used by score.py for breath alignment scoring.

    Args:
        text: Full English verse string.

    Returns:
        Flat list of per-syllable float weights.
    """
    words = _tokenize(text)
    weights: list[float] = []
    for w in words:
        _, syl_weights = get_syllables_and_stress(w)
        weights.extend(syl_weights)
    return weights


def _tokenize(text: str) -> list[str]:
    """Split English verse text into alphabetic word tokens.

    Args:
        text: English verse string.

    Returns:
        List of lowercase alphabetic tokens.
    """
    return [
        w for w in re.split(r"\s+", re.sub(r"[^\w\s]", " ", text)) if w and w.isalpha()
    ]


def _english_onset_sonority(word: str) -> float:
    """Return approximate onset sonority for a word based on its first letter.

    Args:
        word: English word string.

    Returns:
        Float sonority score (0.0–1.0).
    """
    first = word[0].lower() if word else ""
    return _ENG_SONORITY.get(first, 0.30)
