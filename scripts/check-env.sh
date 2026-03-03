#!/usr/bin/env bash
# scripts/check-env.sh — Psalms NLP Pipeline pre-flight checker
#
# Verifies that all prerequisites are met before running the pipeline.
# Prints PASS/FAIL for each check; exits 0 only when all checks pass.
#
# Usage:
#   bash scripts/check-env.sh

set -uo pipefail

PASS=0
FAIL=0

# Ports the pipeline stack expects to own
REQUIRED_PORTS=(5432 8888 8501)

# ── Helpers ───────────────────────────────────────────────────────────────────

pass() { printf '  [PASS] %s\n' "$*"; ((PASS++)); }
fail() { printf '  [FAIL] %s\n' "$*" >&2; ((FAIL++)); }

port_in_use() {
    local port="$1"
    if command -v ss &>/dev/null; then
        ss -tln "sport = :$port" 2>/dev/null | grep -q ":$port"
    elif command -v lsof &>/dev/null; then
        lsof -iTCP:"$port" -sTCP:LISTEN &>/dev/null
    elif command -v netstat &>/dev/null; then
        netstat -tln 2>/dev/null | grep -q ":$port "
    else
        return 1   # cannot check — assume free
    fi
}

# ── Checks ────────────────────────────────────────────────────────────────────

printf '\nPsalms NLP — pre-flight check\n'
printf '══════════════════════════════\n\n'

# 1. Docker daemon
printf 'Docker\n'
if docker info &>/dev/null; then
    pass "Docker daemon is running"
else
    fail "Docker daemon is not running — start Docker and retry"
fi

# 2. Docker Compose v2
if docker compose version &>/dev/null; then
    version=$(docker compose version --short 2>/dev/null || echo "v2")
    pass "Docker Compose v2 available ($version)"
else
    fail "Docker Compose v2 not found — upgrade Docker Desktop or install the compose plugin"
fi

# 3. .env file
printf '\nEnvironment\n'
if [[ -f ".env" ]]; then
    pass ".env file exists"
else
    fail ".env file missing — run: cp .env.example .env  then edit it"
fi

# 4. Required env vars
for var in POSTGRES_PASSWORD JUPYTER_TOKEN; do
    # Source .env quietly if present
    value=""
    if [[ -f ".env" ]]; then
        value=$(grep -E "^${var}=" .env 2>/dev/null | cut -d= -f2- | tr -d '"'"'" | xargs)
    fi
    if [[ -n "$value" && "$value" != "change_me_in_production" ]]; then
        pass "$var is set"
    elif [[ "$value" == "change_me_in_production" ]]; then
        fail "$var is still the default placeholder — set a real value in .env"
    else
        fail "$var is missing or empty in .env"
    fi
done

# 5. Translation data
printf '\nTranslation data\n'
if [[ -f "data/translations/KJV.db" ]]; then
    pass "data/translations/KJV.db exists"
else
    fail "data/translations/KJV.db missing — run: bash scripts/download_data.sh"
fi

# 6. Port availability
printf '\nPorts\n'
for port in "${REQUIRED_PORTS[@]}"; do
    if port_in_use "$port"; then
        fail "Port $port is already in use — stop the conflicting process before starting the stack"
    else
        pass "Port $port is free"
    fi
done

# ── Summary ───────────────────────────────────────────────────────────────────

printf '\n══════════════════════════════\n'
printf 'Results: %d passed, %d failed\n' "$PASS" "$FAIL"

if ((FAIL == 0)); then
    printf 'All checks passed — ready to run the pipeline.\n\n'
    exit 0
else
    printf 'Fix the failures above, then re-run this script.\n\n'
    exit 1
fi
