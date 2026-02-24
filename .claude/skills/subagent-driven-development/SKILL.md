| name | description |
|------|-------------|
| subagent-driven-development | Use when executing implementation plans with independent tasks - dispatches fresh subagent per task with two-stage review |

# Subagent-Driven Development

## Overview

Execute implementation plans by dispatching independent subagents for each task.

**Core principle:** Fresh subagent per task + two-stage review (spec then quality) = high quality, fast iteration.

## When to Use

Use when:
- You have a finalized plan in `docs/plans/2_active/`
- Tasks are mostly independent
- Want to stay in current session

## The Process

For each task:

```
1. IMPLEMENT: Subagent implements, tests, commits, self-reviews
2. SPEC REVIEW: Second subagent confirms code matches spec
3. QUALITY REVIEW: Third subagent assesses code quality
4. ITERATE: Any issues loop back to implementer
```

### Controller Role

1. Read entire plan once
2. Extract all tasks with context
3. Create task list (TodoWrite)
4. Dispatch fresh subagent per task

### Why Fresh Subagents

- No context pollution between tasks
- Each starts with clear scope
- Parallel work when possible

## Two-Stage Review

**Stage 1: Spec Compliance**
- Does code match the specification?
- Are all requirements addressed?
- Any gaps or deviations?

**Stage 2: Code Quality**
- Is code well-structured?
- Are there bugs or issues?
- Suggestions for improvement?

## Critical Guidelines

**Never:**
- Skip reviews
- Wrong review sequence (quality before spec)
- Start on main/master without consent
- Accept "close enough" on spec compliance

**Always:**
- Answer subagent questions before implementation
- Re-review after any fixes
- Use TDD within subagents

## Integration

**Requires:**
- **test-driven-development** — Within subagents

**Leads to:**
- **finishing-a-development-branch** — After all tasks complete
