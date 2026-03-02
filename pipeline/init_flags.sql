-- ═══════════════════════════════════════════════════════════════
-- Psalms NLP Pipeline — Genre and Flag Assignments
-- Run once after init_schema.sql:
--   docker exec psalms_db psql -U psalms -d psalms \
--     -f /pipeline/init_flags.sql
-- All statements are idempotent (UPDATE WHERE is safe to re-run).
-- ═══════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────
-- Genre assignments for seeded books
-- ───────────────────────────────────────────────────────────────

-- Hebrew wisdom poetry
UPDATE books SET genre = 'hebrew_poetry'
WHERE book_num IN (18, 19, 20, 21, 22, 25);
-- Job (18), Psalms (19), Proverbs (20), Ecclesiastes (21),
-- Song of Songs (22), Lamentations (25)

-- Hebrew prophecy
UPDATE books SET genre = 'hebrew_prophecy'
WHERE book_num = 23;
-- Isaiah (23)

-- Hebrew narrative (seed books not yet in corpus — set for completeness)
UPDATE books SET genre = 'hebrew_narrative'
WHERE book_num IN (1, 2);
-- Genesis (1), Exodus (2)

-- ───────────────────────────────────────────────────────────────
-- Acrostic verse flags
-- ───────────────────────────────────────────────────────────────

-- Psalm 119 — full 22-stanza acrostic (all 176 verses)
UPDATE verses SET is_acrostic = TRUE
WHERE book_num = 19 AND chapter = 119;

-- Psalms 9, 10 — partial acrostic (paired)
UPDATE verses SET is_acrostic = TRUE
WHERE book_num = 19 AND chapter IN (9, 10);

-- Psalm 25, 34, 37, 111, 112, 145 — individual acrostics
UPDATE verses SET is_acrostic = TRUE
WHERE book_num = 19 AND chapter IN (25, 34, 37, 111, 112, 145);

-- Lamentations chapters 1–4 — acrostic (chapter 5 is not)
UPDATE verses SET is_acrostic = TRUE
WHERE book_num = 25 AND chapter IN (1, 2, 3, 4);

-- ───────────────────────────────────────────────────────────────
-- Aramaic verse flags (Psalms has no Aramaic — Isaiah has none
-- in the chapters present; placeholder for future books)
-- ───────────────────────────────────────────────────────────────

-- No Aramaic verses in Psalms or Isaiah.
-- Daniel 2:4b–7:28 and Ezra 4:8–6:18, 7:12–26 would be flagged
-- when those books are added to the corpus.
