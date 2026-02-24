| name | description |
|------|-------------|
| verification-before-completion | Use when about to claim work is complete, fixed, or passing - requires running verification commands and confirming output before making any success claims |

# Verification Before Completion

## Overview

Claiming work is complete without verification is dishonesty, not efficiency.

**Core principle:** "Evidence before claims, always."

## The Iron Law

```
NO COMPLETION CLAIMS WITHOUT FRESH VERIFICATION EVIDENCE
```

If you haven't run the verification command in this message, you cannot claim it passes.

## The Gate Function

```
BEFORE claiming any status:

1. IDENTIFY: What command proves this claim?
2. RUN: Execute the FULL command (fresh, complete)
3. READ: Full output, check exit code, count failures
4. VERIFY: Does output confirm the claim?
   - If NO: State actual status with evidence
   - If YES: State claim WITH evidence
5. ONLY THEN: Make the claim

Skip any step = lying, not verifying
```

## Stage Acceptance Criteria (Psalms Pipeline)

Each stage has quantified exit criteria. Verification must confirm:

| Stage | Required Evidence |
|-------|-------------------|
| Stage 0 | All containers healthy, all tables created |
| Stage 1 | Each translation returns correct text for Psalm 23:1 |
| Stage 2 | 2,527 verse rows, ~43,000 token rows, 2,527 fingerprint rows |
| Stage 3 | ~120,000 syllable token rows, 2,527 breath profiles |
| Stage 4 | All configured translations scored, all columns populated |
| Stage 5 | Suggestions stored in database, queryable |
| Stage 6 | HTML report site built, PDF generated |
| Stage 7 | Pipeline runs end-to-end, exit code 0 |
| Stage 8 | Isaiah output validated as first expansion target |

## What Requires Verification

| Claim | Requires | Not Sufficient |
|-------|----------|----------------|
| Tests pass | Test command output: 0 failures | Previous run, "should pass" |
| Linter clean | Linter output: 0 errors | Partial check |
| Build succeeds | Build command: exit 0 | Linter passing |
| Bug fixed | Test original symptom: passes | Code changed |
| Row counts correct | DB query output | Code looks right |

## Red Flags — STOP

- Using "should", "probably", "seems to"
- Expressing satisfaction before verification ("Great!", "Done!")
- About to commit/push without verification
- Relying on partial verification
- Thinking "just this once"

## Correct Patterns

**Tests:**
```
✅ [Run pytest] [See: 34/34 pass] "All tests pass"
❌ "Should pass now" / "Looks correct"
```

**Row counts:**
```
✅ [Run SELECT COUNT(*) FROM verses] [See: 2527] "2,527 verse rows confirmed"
❌ "Ingest completed successfully"
```

## The Bottom Line

Run the command. Read the output. THEN claim the result.

**No shortcuts for verification.**
