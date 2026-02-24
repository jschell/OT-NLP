# Using Superpowers

## Overview

Invoke relevant skills BEFORE any response or action.

**Core principle:** Skill-check first, even if there's only 1% chance a skill applies.

## The Protocol

```
User message received
  ↓
Check: Does any skill apply?
  ↓
If yes → Invoke skill FIRST
  ↓
Then respond/act
```

## Red Flags (Rationalizations to Avoid)

If you catch yourself thinking:

- "This is just a simple question"
- "I need more context first"
- "I remember this skill"
- "The skill is overkill"
- "I'll invoke it after I understand better"
- "This doesn't quite match"

**STOP** - These are signs you're bypassing the protocol.

## Skill Priority

When multiple skills apply:

1. **Process skills first** (brainstorming, debugging)
2. **Then implementation skills** (TDD, executing-plans)

Ensure the *approach* is sound before execution begins.

## Practical Application

- If a skill exists and could plausibly apply, invoke it
- If it turns out inapplicable, no harm done
- Skipping the check risks missing essential guidance

## Available Skills

**Tier 0 — Always Active:**
- using-superpowers (this skill)

**Planning:**
- writing-plans
- autonomous-work

**Execution:**
- executing-plans
- subagent-driven-development
- dispatching-parallel-agents

**Quality:**
- test-driven-development
- systematic-debugging
- verification-before-completion

**Git Workflow:**
- finishing-a-development-branch

**Project:**
- project-setup
- feature-backlog

**Research:**
- read-arxiv-paper
