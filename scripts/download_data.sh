#!/usr/bin/env bash
# scripts/download_data.sh — Psalms NLP Pipeline translation downloader
#
# Downloads all five translation sources needed by the pipeline:
#   KJV, YLT, NHEB — SQLite files from scrollmapper/bible_databases
#   ULT, UST       — USFM repositories from git.door43.org
#
# Usage:
#   bash scripts/download_data.sh
#
# Requirements:
#   curl (or wget), git, sha256sum (or shasum on macOS)
#
# All downloads are idempotent: existing files are skipped.
# Exits non-zero if any download fails.

set -euo pipefail

TRANSLATIONS_DIR="${TRANSLATIONS_DIR:-data/translations}"
SCROLLMAPPER_BASE="https://github.com/scrollmapper/bible_databases/raw/master/sqlite"

# ── Helpers ──────────────────────────────────────────────────────────────────

log()  { printf '[download_data] %s\n' "$*"; }
skip() { printf '[download_data] SKIP %s (already exists)\n' "$*"; }
fail() { printf '[download_data] ERROR %s\n' "$*" >&2; exit 1; }

checksum() {
    local file="$1"
    if command -v sha256sum &>/dev/null; then
        sha256sum "$file" | awk '{print $1}'
    elif command -v shasum &>/dev/null; then
        shasum -a 256 "$file" | awk '{print $1}'
    else
        echo "(checksum tool not available)"
    fi
}

download_file() {
    local url="$1"
    local dest="$2"
    local label="$3"

    if [[ -f "$dest" ]]; then
        skip "$label"
        return 0
    fi

    log "Downloading $label..."
    if command -v curl &>/dev/null; then
        curl --fail --silent --show-error --location --output "$dest" "$url" \
            || fail "curl failed for $label ($url)"
    elif command -v wget &>/dev/null; then
        wget --quiet --output-document="$dest" "$url" \
            || fail "wget failed for $label ($url)"
    else
        fail "Neither curl nor wget found — install one and retry"
    fi

    log "Done $label — sha256: $(checksum "$dest")"
}

clone_or_update() {
    local url="$1"
    local dest="$2"
    local label="$3"

    if [[ -d "$dest/.git" ]]; then
        skip "$label (pulling latest)"
        git -C "$dest" pull --ff-only --quiet \
            || log "WARNING: pull failed for $label — using cached version"
        return 0
    fi

    log "Cloning $label..."
    git clone --depth 1 --quiet "$url" "$dest" \
        || fail "git clone failed for $label ($url)"
    log "Done $label"
}

# ── Setup ─────────────────────────────────────────────────────────────────────

mkdir -p "$TRANSLATIONS_DIR"

# ── SQLite scrollmapper translations ─────────────────────────────────────────

declare -A SQLITE_SOURCES=(
    ["KJV"]="t_kjv"
    ["YLT"]="t_ylt"
    ["NHEB"]="t_nheb"
)

for id in "${!SQLITE_SOURCES[@]}"; do
    remote="${SQLITE_SOURCES[$id]}"
    dest="$TRANSLATIONS_DIR/${id}.db"
    url="$SCROLLMAPPER_BASE/${remote}.db"
    download_file "$url" "$dest" "$id"
done

# ── USFM repositories ─────────────────────────────────────────────────────────

clone_or_update \
    "https://git.door43.org/unfoldingWord/en_ult" \
    "$TRANSLATIONS_DIR/ult" \
    "ULT (unfoldingWord Literal Text)"

clone_or_update \
    "https://git.door43.org/unfoldingWord/en_ust" \
    "$TRANSLATIONS_DIR/ust" \
    "UST (unfoldingWord Simplified Text)"

# ── Summary ──────────────────────────────────────────────────────────────────

log "All translation sources ready in $TRANSLATIONS_DIR/"
log "  KJV.db   YLT.db   NHEB.db   ult/   ust/"
