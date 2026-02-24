| name | description |
|------|-------------|
| dispatching-parallel-agents | Use when facing 2+ independent tasks that can be worked on without shared state or sequential dependencies |

# Dispatching Parallel Agents

## Overview

When multiple unrelated problems exist, investigating them sequentially wastes time.

**Core principle:** Dispatch one agent per independent problem domain. Let them work concurrently.

## When to Use

**Use when:**
- Multiple independent modules to implement (e.g., `visualize/breath_curves.py`, `visualize/heatmaps.py`, `visualize/arcs.py`)
- Stages 5 and 6 both ready (both depend only on Stage 4, independent of each other)
- Multiple test files failing with different root causes
- Problems can be understood without context from others

**Don't use when:**
- Tasks have sequential dependencies
- Agents would share database state in conflicting ways
- Still exploring what's broken

## Pipeline-Specific Parallel Opportunities

| Parallel Group | Members | Dependency |
|----------------|---------|------------|
| Visualization modules | breath_curves, heatmaps, arcs, radar | All depend only on Stage 3 output |
| Stage 5 + Stage 6 | LLM suggestions + Reporting | Both depend only on Stage 4 |
| Unit test suites | syllable, fingerprint, chiasm | Independent modules |

## The Pattern

### Step 1: Identify Independent Domains

Group tasks by what they touch:
- Domain A: `visualize/breath_curves.py`
- Domain B: `visualize/arcs.py`
- Domain C: `visualize/radar.py`

Each domain stands alone.

### Step 2: Create Focused Agent Tasks

Each agent receives:
- Specific scope (one domain only)
- Clear goal
- Constraints
- Expected output

**Example prompt:**
```
Implement visualize/breath_curves.py

Scope: Only this file and its tests
Goal: All breath curve chart functions passing tests
Constraints: Don't modify other visualize/ files
Output: Summary of functions implemented and test results
```

### Step 3: Dispatch in Parallel

Use Task tool to create multiple agents simultaneously in a single message.

### Step 4: Review and Integrate

1. Read summaries from each agent
2. Verify fixes don't conflict
3. Run full test suite
4. Integrate changes

## Agent Prompt Structure

```
[Problem domain and scope]

Scope: [specific files/area]
Goal: [clear success criteria]
Constraints: [what NOT to do]
Output: [what to report back]
```

**Good:** "Implement visualize/breath_curves.py"
**Bad:** "Implement all visualization modules"

## Integration

**Pairs with:**
- **subagent-driven-development** — For per-task review cycles within each agent
