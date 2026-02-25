# pipeline/adapters/translation_adapter.py
"""
Translation adapter layer.

Each adapter implements a single interface method:
    get_verse(book_num, chapter, verse) -> str | None

This module is used during Stage 1 ingest only. After ingest completes,
all translation text is read exclusively from PostgreSQL.

Adapter classes:
    SQLiteScrollmapperAdapter  — scrollmapper SQLite format (.db files)
    USFMAdapter                — USFM directory format (unfoldingWord / eBible)
    APIAdapter                 — placeholder; returns None (not yet implemented)

Factory function:
    adapter_factory(source_config)  — returns the correct adapter instance
"""

from __future__ import annotations

import contextlib
import re
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path

# ─────────────────────────────────────────────────────────────────
# Type alias
# ─────────────────────────────────────────────────────────────────

# Internal cache: {(chapter, verse_num): verse_text}
_VerseCache = dict[tuple[int, int], str]


# ─────────────────────────────────────────────────────────────────
# Abstract base
# ─────────────────────────────────────────────────────────────────


class TranslationAdapter(ABC):
    """Base class for all translation source adapters."""

    def __init__(self, source_config: dict) -> None:
        """
        Initialize the adapter with a source config block from config.yml.

        Args:
            source_config: Dict with at minimum 'id', 'format', 'path' keys.
        """
        self.id: str = source_config["id"]
        self.config: dict = source_config

    @abstractmethod
    def get_verse(self, book_num: int, chapter: int, verse: int) -> str | None:
        """
        Return the verse text for the given coordinates, or None if not found.

        Args:
            book_num: BHSA book number (e.g. 19 for Psalms).
            chapter:  Chapter number (1-based).
            verse:    Verse number (1-based).

        Returns:
            Verse text string, or None if the verse does not exist in this source.
        """
        ...


# ─────────────────────────────────────────────────────────────────
# SQLite adapter — scrollmapper format
# Schema: CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)
# where b = book_num (19 = Psalms), c = chapter, v = verse, t = text
# ─────────────────────────────────────────────────────────────────


class SQLiteScrollmapperAdapter(TranslationAdapter):
    """
    Adapter for scrollmapper-format SQLite Bible databases.

    Downloads available at:
    https://github.com/scrollmapper/bible_databases/tree/master/sqlite
    """

    def get_verse(self, book_num: int, chapter: int, verse: int) -> str | None:
        """Return the verse text from the SQLite file, or None if not found."""
        path = Path(self.config["path"])
        if not path.exists():
            raise FileNotFoundError(f"SQLite translation file not found: {path}")
        conn = sqlite3.connect(str(path))
        try:
            cur = conn.execute(
                "SELECT t FROM t WHERE b = ? AND c = ? AND v = ?",
                (book_num, chapter, verse),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return str(row[0]).strip()
        finally:
            conn.close()


# ─────────────────────────────────────────────────────────────────
# USFM adapter — unfoldingWord / eBible format
# Reads a directory of .usfm files; locates the book file by the
# two-digit book number prefix (e.g. 19PSA.usfm for Psalms).
# ─────────────────────────────────────────────────────────────────

# USFM book codes: book_num -> two-digit string prefix
_USFM_BOOK_CODES: dict[int, str] = {num: f"{num:02d}" for num in range(1, 40)}


class USFMAdapter(TranslationAdapter):
    """
    Adapter for USFM-format Bible directories (unfoldingWord ULT/UST).

    Parses USFM files line by line. Handles:
      \\c <chapter>     — chapter marker
      \\v <verse> <text> — verse marker with inline text
      \\p \\q \\q2 \\m  — paragraph markers (ignored)
      \\w...\\w*        — word-level markup (stripped, word retained)
      \\f...\\f*        — footnote spans (stripped entirely)
      \\x...\\x*        — cross-reference spans (stripped entirely)
    """

    # Per-instance cache: populated on first access per book file
    _cache: dict[str, _VerseCache]

    def __init__(self, source_config: dict) -> None:
        super().__init__(source_config)
        self._cache = {}

    def get_verse(self, book_num: int, chapter: int, verse: int) -> str | None:
        """Return the verse text from the USFM directory, or None if not found."""
        cache_key = str(book_num)
        if cache_key not in self._cache:
            self._cache[cache_key] = self._load_book(book_num)
        return self._cache[cache_key].get((chapter, verse))

    def _load_book(self, book_num: int) -> _VerseCache:
        """Locate and parse the USFM file for book_num. Returns full verse map."""
        usfm_dir = Path(self.config["path"])
        code = _USFM_BOOK_CODES.get(book_num)
        if code is None:
            raise ValueError(f"No USFM book code for book_num={book_num}")
        candidates = list(usfm_dir.glob(f"{code}*.usfm"))
        if not candidates:
            raise FileNotFoundError(
                f"No USFM file matching '{code}*.usfm' in {usfm_dir}"
            )
        text = candidates[0].read_text(encoding="utf-8")
        return _parse_usfm(text)


def _parse_usfm(text: str) -> _VerseCache:
    """
    Parse a USFM string into a verse map.

    Args:
        text: Full USFM file content as a string.

    Returns:
        Dict mapping (chapter, verse_num) -> cleaned verse text.
    """
    verses: _VerseCache = {}
    chapter = 0
    verse_num = 0
    parts: list[str] = []

    def _flush() -> None:
        if chapter and verse_num and parts:
            raw = " ".join(parts).strip()
            verses[(chapter, verse_num)] = re.sub(r"\s+", " ", raw)

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith(r"\c "):
            _flush()
            parts = []
            verse_num = 0
            with contextlib.suppress(IndexError, ValueError):
                chapter = int(line.split()[1])
            continue

        if line.startswith(r"\v "):
            _flush()
            parts = []
            tokens = line.split(None, 2)
            try:
                verse_num = int(tokens[1])
            except (IndexError, ValueError):
                verse_num = 0
            if len(tokens) > 2:
                parts.append(_strip_usfm_inline(tokens[2]))
            continue

        # Continuation text within the current verse
        if chapter and verse_num:
            # Skip structural markers that start new blocks
            if line.startswith(r"\c ") or line.startswith(r"\v "):
                continue
            # Skip section headings (\s, \ms, \mt)
            if re.match(r"\\(s|ms|mt)\d?\s", line):
                continue
            # Paragraph markers (\p, \q, \q2, \m, etc.) — take any trailing text
            if re.match(r"\\[pqmb]\d?\s*", line):
                tail = re.sub(r"^\\[pqmb]\d?\s*", "", line)
                if tail:
                    parts.append(_strip_usfm_inline(tail))
            elif not line.startswith("\\"):
                parts.append(_strip_usfm_inline(line))

    _flush()
    return verses


def _strip_usfm_inline(text: str) -> str:
    """
    Remove USFM inline character markers from a text fragment.

    Removes:
      - Footnote spans:        \\f ... \\f*
      - Cross-ref spans:       \\x ... \\x*
      - Word markup:           \\w word|attrs\\w*  -> word
      - Other inline markers:  \\nd\\* \\add\\* etc.
    """
    # Remove footnote and cross-reference spans (may be multi-token)
    text = re.sub(r"\\[fx]\s.*?\\[fx]\*", "", text)
    # Strip word markup: keep the word, discard the attribute block
    text = re.sub(r"\\w\s+(.*?)\|[^\\]*?\\w\*", r"\1", text)
    # Remove remaining inline markers like \nd \add \bk etc.
    text = re.sub(r"\\[a-zA-Z0-9]+\*?", "", text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────
# API adapter — stub (not yet implemented)
# ─────────────────────────────────────────────────────────────────


class APIAdapter(TranslationAdapter):
    """
    Stub adapter for API-based translations (e.g. ESV API).

    Not yet implemented. Returns None for all verse lookups.
    Full implementation planned for a future stage if API access is configured.
    """

    def get_verse(self, book_num: int, chapter: int, verse: int) -> str | None:
        """Return None — API adapter is not yet implemented."""
        return None


# ─────────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────────

_ADAPTER_MAP: dict[str, type[TranslationAdapter]] = {
    "sqlite_scrollmapper": SQLiteScrollmapperAdapter,
    "usfm": USFMAdapter,
    "api": APIAdapter,
}


def adapter_factory(source_config: dict) -> TranslationAdapter:
    """
    Return the correct adapter instance for a source config block.

    Args:
        source_config: Dict with at minimum 'id' and 'format' keys.
                       'format' must be one of: sqlite_scrollmapper, usfm, api.

    Returns:
        Configured TranslationAdapter instance.

    Raises:
        ValueError: If 'format' is not a recognized adapter type.
    """
    fmt: str = source_config.get("format", "")
    cls = _ADAPTER_MAP.get(fmt)
    if cls is None:
        raise ValueError(
            f"Unknown translation format '{fmt}'. "
            f"Valid options: {sorted(_ADAPTER_MAP.keys())}"
        )
    return cls(source_config)
