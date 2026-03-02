-- ═══════════════════════════════════════════════════════════════
-- Psalms NLP Pipeline — Complete Database Schema
-- Run once via:
--   docker exec psalms_db psql -U psalms -d psalms \
--     -f /pipeline/init_schema.sql
-- All CREATE statements use IF NOT EXISTS — fully idempotent.
-- ═══════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ───────────────────────────────────────────────────────────────
-- Corpus reference table
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS books (
    book_num   INTEGER PRIMARY KEY,
    book_name  TEXT    NOT NULL,
    testament  TEXT    NOT NULL CHECK (testament IN ('OT', 'NT')),
    genre      TEXT,
    language   TEXT    NOT NULL DEFAULT 'hebrew'
);

-- ───────────────────────────────────────────────────────────────
-- Stage 1 / 2: Source verses and translation text
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS verses (
    verse_id    SERIAL  PRIMARY KEY,
    book_num    INTEGER NOT NULL REFERENCES books(book_num),
    chapter     INTEGER NOT NULL,
    verse_num   INTEGER NOT NULL,
    hebrew_text TEXT    NOT NULL,
    word_count  INTEGER,
    colon_count INTEGER,
    is_aramaic  BOOLEAN NOT NULL DEFAULT FALSE,
    is_acrostic BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (book_num, chapter, verse_num)
);

CREATE TABLE IF NOT EXISTS translations (
    translation_id  SERIAL  PRIMARY KEY,
    verse_id        INTEGER NOT NULL REFERENCES verses(verse_id),
    translation_key TEXT    NOT NULL,
    verse_text      TEXT    NOT NULL,
    word_count      INTEGER,
    UNIQUE (verse_id, translation_key)
);

-- ───────────────────────────────────────────────────────────────
-- Stage 2: Morphological tokens and style fingerprints
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS word_tokens (
    token_id       SERIAL  PRIMARY KEY,
    verse_id       INTEGER NOT NULL REFERENCES verses(verse_id),
    position       INTEGER NOT NULL,
    surface_form   TEXT    NOT NULL,
    lexeme         TEXT,
    part_of_speech TEXT,
    morpheme_count INTEGER,
    is_verb        BOOLEAN,
    is_noun        BOOLEAN,
    stem           TEXT,
    colon_index    INTEGER,
    UNIQUE (verse_id, position)
);

CREATE TABLE IF NOT EXISTS verse_fingerprints (
    fingerprint_id     SERIAL    PRIMARY KEY,
    verse_id           INTEGER   NOT NULL UNIQUE REFERENCES verses(verse_id),
    syllable_density   NUMERIC(6,4),
    morpheme_ratio     NUMERIC(6,4),
    sonority_score     NUMERIC(6,4),
    clause_compression NUMERIC(6,4),
    colon_fingerprints JSONB,
    computed_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chiasm_candidates (
    chiasm_id      SERIAL    PRIMARY KEY,
    verse_id_start INTEGER   NOT NULL REFERENCES verses(verse_id),
    verse_id_end   INTEGER   NOT NULL REFERENCES verses(verse_id),
    pattern_type   TEXT      NOT NULL
                       CHECK (pattern_type IN ('ABBA', 'ABCBA', 'AB')),
    colon_matches  JSONB     NOT NULL,
    confidence     NUMERIC(5,4) NOT NULL,
    is_reviewed    BOOLEAN   NOT NULL DEFAULT FALSE,
    reviewer_note  TEXT,
    computed_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- Stage 3: Syllable tokens and breath profiles
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS syllable_tokens (
    syllable_id     SERIAL    PRIMARY KEY,
    token_id        INTEGER   NOT NULL REFERENCES word_tokens(token_id),
    verse_id        INTEGER   NOT NULL REFERENCES verses(verse_id),
    syllable_index  INTEGER   NOT NULL,
    syllable_text   TEXT      NOT NULL,
    nucleus_vowel   TEXT,
    vowel_openness  NUMERIC(4,3),
    vowel_length    TEXT
                        CHECK (vowel_length IN
                            ('long', 'short', 'ultra-short', 'shewa')),
    is_open         BOOLEAN,
    onset_class     TEXT
                        CHECK (onset_class IN
                            ('guttural', 'sibilant', 'liquid', 'stop',
                             'nasal', 'none')),
    breath_weight   NUMERIC(5,4),
    stress_position NUMERIC(5,4),
    colon_index     INTEGER   NOT NULL,
    UNIQUE (token_id, syllable_index)
);

CREATE TABLE IF NOT EXISTS breath_profiles (
    profile_id       SERIAL    PRIMARY KEY,
    verse_id         INTEGER   NOT NULL UNIQUE REFERENCES verses(verse_id),
    mean_weight      NUMERIC(5,4),
    open_ratio       NUMERIC(5,4),
    guttural_density NUMERIC(5,4),
    colon_count      INTEGER,
    colon_boundaries INTEGER[],
    stress_positions NUMERIC(5,4)[],
    breath_curve     NUMERIC(5,4)[],
    computed_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- Stage 4: Translation scoring
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS translation_scores (
    score_id              SERIAL    PRIMARY KEY,
    verse_id              INTEGER   NOT NULL REFERENCES verses(verse_id),
    translation_key       TEXT      NOT NULL,
    density_deviation     NUMERIC(7,4),
    morpheme_deviation    NUMERIC(7,4),
    sonority_deviation    NUMERIC(7,4),
    compression_deviation NUMERIC(7,4),
    composite_deviation   NUMERIC(7,4),
    stress_alignment      NUMERIC(5,4),
    weight_match          NUMERIC(5,4),
    breath_alignment      NUMERIC(5,4),
    scored_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (verse_id, translation_key)
);

-- ───────────────────────────────────────────────────────────────
-- Stage 5: LLM-generated translation suggestions
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS suggestions (
    suggestion_id       SERIAL    PRIMARY KEY,
    verse_id            INTEGER   NOT NULL REFERENCES verses(verse_id),
    translation_key     TEXT      NOT NULL,
    suggested_text      TEXT      NOT NULL,
    composite_deviation NUMERIC(7,4),
    breath_alignment    NUMERIC(5,4),
    improvement_delta   NUMERIC(7,4),
    llm_provider        TEXT      NOT NULL,
    llm_model           TEXT      NOT NULL,
    prompt_version      TEXT      NOT NULL,
    generated_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- Stage 8: Genre-level aggregate baselines
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS genre_baselines (
    baseline_id               SERIAL    PRIMARY KEY,
    genre                     TEXT      NOT NULL UNIQUE,
    verse_count               INTEGER   NOT NULL,
    syllable_density_mean     NUMERIC(6,4),
    syllable_density_stddev   NUMERIC(6,4),
    morpheme_ratio_mean       NUMERIC(6,4),
    morpheme_ratio_stddev     NUMERIC(6,4),
    sonority_mean             NUMERIC(6,4),
    sonority_stddev           NUMERIC(6,4),
    clause_compression_mean   NUMERIC(6,4),
    clause_compression_stddev NUMERIC(6,4),
    computed_at               TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- Stage 7: Pipeline run audit log
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        SERIAL    PRIMARY KEY,
    started_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMP,
    status        TEXT      NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'ok', 'error')),
    stages_run    TEXT[],
    error_message TEXT,
    row_counts    JSONB
);

-- ───────────────────────────────────────────────────────────────
-- Indices
-- ───────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_verses_book_chapter
    ON verses(book_num, chapter);

CREATE INDEX IF NOT EXISTS idx_translations_verse
    ON translations(verse_id);

CREATE INDEX IF NOT EXISTS idx_translations_key
    ON translations(translation_key);

CREATE INDEX IF NOT EXISTS idx_word_tokens_verse
    ON word_tokens(verse_id);

CREATE INDEX IF NOT EXISTS idx_syllable_tokens_verse
    ON syllable_tokens(verse_id);

CREATE INDEX IF NOT EXISTS idx_syllable_tokens_token
    ON syllable_tokens(token_id);

CREATE INDEX IF NOT EXISTS idx_translation_scores_verse
    ON translation_scores(verse_id);

CREATE INDEX IF NOT EXISTS idx_translation_scores_key
    ON translation_scores(translation_key);

CREATE INDEX IF NOT EXISTS idx_chiasm_candidates_start
    ON chiasm_candidates(verse_id_start);

-- ───────────────────────────────────────────────────────────────
-- Seed data: corpus books
-- ───────────────────────────────────────────────────────────────

INSERT INTO books (book_num, book_name, testament, language) VALUES
    ( 1, 'Genesis',      'OT', 'hebrew'),
    ( 2, 'Exodus',       'OT', 'hebrew'),
    (18, 'Job',          'OT', 'hebrew'),
    (19, 'Psalms',       'OT', 'hebrew'),
    (23, 'Isaiah',       'OT', 'hebrew'),
    (25, 'Lamentations', 'OT', 'hebrew')
ON CONFLICT (book_num) DO NOTHING;
