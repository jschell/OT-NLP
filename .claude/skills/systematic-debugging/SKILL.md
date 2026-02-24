| name | description |
|------|-------------|
| systematic-debugging | Use when debugging issues, before attempting any fix - requires finding root cause first through investigation, not symptom-fixing |

# Systematic Debugging

## Overview

Find root cause before attempting fixes. Symptom fixes are failure.

**Core principle:** "NO FIXES WITHOUT ROOT CAUSE INVESTIGATION FIRST."

## The Four Phases

### Phase 1: Root Cause Investigation

1. **Read error messages carefully** — they often tell you exactly what's wrong
2. **Reproduce consistently** — if you can't reproduce, you can't verify the fix
3. **Examine recent changes** — what changed since it last worked?
4. **Gather evidence** — logs, stack traces, data states
5. **Trace backward** — from symptom to origin

### Phase 2: Pattern Analysis

1. **Find working examples** — what's similar that does work?
2. **Study complete implementations** — don't guess at how things work
3. **Identify specific differences** — between broken and working
4. **Map dependencies** — what does this rely on?

### Phase 3: Hypothesis and Testing

1. **Form explicit hypothesis** — "I believe X causes Y because Z"
2. **Test with minimal change** — one variable at a time
3. **If wrong, form new hypothesis** — don't randomly try things
4. **Document what you learn** — even dead ends teach something

### Phase 4: Implementation

1. **Write failing test first** — captures the bug
2. **Apply single targeted fix** — address root cause only
3. **Verify test passes** — confirms fix works
4. **Check for regressions** — run full test suite

## Pipeline-Specific Failure Surfaces

| Surface | Common Issues |
|---------|---------------|
| Docker networking | Container can't reach PostgreSQL — check service names in compose |
| Hebrew Unicode | Silent sheva vs. vocal sheva — check vowel point codepoints |
| pgvector queries | Dimension mismatch — check vector size in schema vs. code |
| CMU dictionary | Word not found — check heuristic fallback path |
| USFM parser | Book number mapping — verify book_map config |
| Typst binary | PATH not set in container — check Dockerfile ARG |
| pipeline.log | Stage exit code non-zero — check `on_error` config |

## The Three-Attempt Rule

```
IF three fix attempts each reveal different problems elsewhere:
  STOP - this signals architectural issues, not isolated bugs
  Discuss design with your human partner
  Don't keep patching
```

## Warning Signs You're Violating the Process

- Proposing solutions before tracing data flow
- Attempting multiple changes simultaneously
- "Just one more fix" after multiple failures
- Guessing instead of investigating
- Not reproducing before fixing

**When you see these:** Return to Phase 1.

## Quick Debugging Questions

1. What exactly is the error/symptom?
2. When did it start happening?
3. What changed since it worked?
4. Can I reproduce it consistently?
5. What does the error message say?
6. What's the data state at failure point?
7. Is there a working example to compare?

## The Bottom Line

**Investigate → Understand → Hypothesize → Test → Fix**

Never: Fix → Hope → Repeat
