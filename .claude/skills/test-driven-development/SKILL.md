| name | description |
|------|-------------|
| test-driven-development | Use when implementing any feature or bugfix, before writing implementation code - requires writing failing test first, watching it fail, then implementing minimal code to pass |

# Test-Driven Development

## Overview

TDD is mandatory for all feature development and bug fixes.

**Core principle:** "If you didn't watch the test fail, you don't know if it tests the right thing."

## The Iron Law

```
NO PRODUCTION CODE WITHOUT A FAILING TEST FIRST
```

Write code before test? Delete it. Start over.

**No exceptions:**
- Don't keep it as "reference"
- Don't "adapt" it while writing tests
- Delete means delete

## The Cycle: RED-GREEN-REFACTOR

### RED: Write Failing Test

```
1. Write minimal test for ONE behavior
2. Run test - must FAIL
3. Verify failure is for RIGHT reason (missing feature, not syntax error)
```

### GREEN: Make It Pass

```
1. Write MINIMAL code to pass test
2. Run test - must PASS
3. No extra code "while you're there"
```

### REFACTOR: Clean Up

```
1. Improve code structure
2. Run tests - must still PASS
3. Commit
```

## Test Commands for This Project

```bash
# Run all tests
uv run --frozen pytest

# Run specific module tests
uv run --frozen pytest tests/test_<module>.py -v

# Run with coverage
uv run --frozen pytest --cov=pipeline
```

## What Makes a Good Test

**One behavior per test:**

```
❌ test_validates_fingerprint_and_tokens_and_syllables
✅ test_syllable_counter_handles_sheva
✅ test_fingerprint_returns_four_dimensions
✅ test_chiasm_detects_abba_pattern
```

**Descriptive names:**

```
❌ test_syllable_1
✅ test_syllable_counter_skips_silent_sheva
```

**Real code, not mocks** (when possible):
```
❌ Mock everything, test nothing real
✅ Test actual behavior with minimal isolation
```

## Common Rationalizations

| Excuse | Reality |
|--------|---------|
| "Tests after achieve same goals" | Tests-after pass immediately, proving nothing |
| "Too simple to test" | Simple code breaks. Test takes 30 seconds. |
| "Manual testing is sufficient" | Not repeatable, not systematic |
| "Deleting hours of work is wasteful" | Sunk cost fallacy. Unverified code = debt. |
| "I'll test after" | You won't. And passing tests prove nothing. |

## Red Flags — STOP and Start Over

- Code before test
- "I already manually tested it"
- "Tests after achieve the same purpose"
- Test passes on first run (didn't see it fail)

**All of these mean: Delete code. Start over with TDD.**

## Verification Checklist

Before claiming work complete:

- [ ] Each new function has corresponding test
- [ ] Watched each test fail before implementing
- [ ] Failure was for expected reason (not syntax/typo)
- [ ] Wrote minimal code to pass
- [ ] All tests pass with clean output
- [ ] Used real code (mocks only when unavoidable)
