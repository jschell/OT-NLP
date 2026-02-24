| name | description |
|------|-------------|
| autonomous-work | Use to orchestrate plan-then-execute workflow across the full session - separates planning (human-reviewed) from execution (autonomous) |

# Autonomous Work

## Overview

Orchestrate a plan-then-execute workflow for feature implementation.

**Core principle:** Separate planning (human-reviewed) from execution (autonomous).

**Announce at start:** "I'm using the autonomous-work skill to start this session."

## Plan Directories

```
docs/plans/
├── 1_backlog/     # New plans awaiting review
├── 2_active/      # Approved plan being executed (max 1)
└── 3_complete/    # Verified complete plans
```

**State transitions:**
- `1_backlog → 2_active` (human approves)
- `2_active → 3_complete` (tests pass, verified)
- `2_active → 1_backlog` (blocked, needs rework)

**Rule:** Only ONE plan in `2_active/` at a time.

## The Four Phases

### Phase 1: Baseline Check

1. Verify clean git state (no uncommitted changes)
2. Run test suite — MUST pass before new work
3. Check for plan in `docs/plans/2_active/`

If tests fail, fix before proceeding.

### Phase 2: Planning

If no active plan exists:
1. Identify next feature from backlog
2. Use **writing-plans** skill
3. Save to `docs/plans/1_backlog/[feature-name].md`
4. Present summary for human review
5. WAIT — Human moves to `2_active/` when approved

### Phase 3: Execution

1. Read plan from `docs/plans/2_active/`
2. Use **executing-plans** skill
3. Test and commit after each step
4. Upon completion: run **verification-before-completion**, move plan to `docs/plans/3_complete/`, use **finishing-a-development-branch** skill

### Phase 4: Continue or Stop

| Condition | Action |
|-----------|--------|
| More features | Return to Phase 2 |
| Blocker | Stop, report |
| All done | Summarize session |

## Stop Conditions

Halt and consult the human when:
- 3 consecutive failures on the same step
- Breaking API changes required
- Security implications discovered
- Scope significantly larger than expected
- Architectural decisions needed

## Quick Reference

| State | Action |
|-------|--------|
| Tests failing | Fix before new work |
| Nothing in 2_active | Create plan → 1_backlog |
| Plan in 1_backlog | Wait for human to approve |
| Plan in 2_active | Execute with executing-plans |
| Execution complete | Verify → move to 3_complete |

## Integration

**Uses:**
- **writing-plans** — create plans
- **executing-plans** — execute plans
- **finishing-a-development-branch** — complete work
- **verification-before-completion** — validate steps

**Prerequisite:**
- **project-setup** — CLAUDE.md with commands and conventions
