| name | description |
|------|-------------|
| executing-plans | Use when you have a written implementation plan to execute - load, review critically, execute in batches with checkpoints |

# Executing Plans

## Overview

Load plan, review critically, execute tasks in batches, report for review between batches.

**Core principle:** Batch execution with checkpoints for architect review.

**Announce at start:** "I'm using the executing-plans skill to implement this plan."

## The Process

### Step 1: Load and Review Plan

1. Read plan file from `docs/plans/2_active/`
2. Review critically - identify questions or concerns
3. If concerns: Raise them before starting
4. If no concerns: Create TodoWrite and proceed

### Step 2: Execute Batch

**Default: First 3 tasks**

For each task:
1. Mark as in_progress
2. Follow each step exactly
3. Run verifications as specified
4. Mark as completed

### Step 3: Report

When batch complete:
- Show what was implemented
- Show verification output
- Say: "Ready for feedback."

### Step 4: Continue

Based on feedback:
- Apply changes if needed
- Execute next batch
- Repeat until complete

### Step 5: Complete Development

After all tasks complete and verified:
- Use **finishing-a-development-branch** skill
- Follow that skill to verify tests, present options, execute choice

## When to Stop and Ask

**STOP executing immediately when:**
- Hit a blocker mid-batch
- Plan has critical gaps
- You don't understand an instruction
- Verification fails repeatedly

**Ask for clarification rather than guessing.**

## Red Flags

**Never:**
- Skip verifications
- Guess when blocked
- Start on main/master without consent

**Always:**
- Review plan critically first
- Follow plan steps exactly
- Stop when blocked, ask

## Integration

**Required:**
- **finishing-a-development-branch** - Complete after all tasks
