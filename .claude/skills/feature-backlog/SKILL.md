| name | description |
|------|-------------|
| feature-backlog | Use to track features and deferred items across all pipeline stages - one feature in progress at a time |

# Feature Backlog

## Overview

Simple format for tracking features through autonomous work phases.

**Core principle:** One feature in progress at a time.

## Format

```markdown
# Feature Backlog

## In Progress
- [ ] Current stage → docs/plans/2_active/stage-name.md

## Todo — Pipeline Stages
- [ ] Stage 0: Foundation & Infrastructure
- [ ] Stage 1: Data Acquisition & Configuration
- [ ] Stage 2: Morphology & Fingerprinting
- [ ] Stage 3: Breath & Phonetic Analysis
- [ ] Stage 4: Translation Scoring
- [ ] Stage 5: LLM Integration & Suggestions
- [ ] Stage 6: Visualization & Reporting
- [ ] Stage 7: Pipeline Orchestration
- [ ] Stage 8: Corpus Expansion

## Todo — Deferred Items
- [ ] ESV API adapter (optional Tier 2 extension)
- [ ] Semantic vectors (pgvector infrastructure ready)
- [ ] Phonosemantic annotation (requires lexical + phoneme-sound mapping layers)
- [ ] NetworkX graphs and UMAP clustering (pending semantic vectors)
- [ ] Greek NT track (requires MorphGNT adapter + Koine phoneme mapping)
- [ ] Akkadian/Gilgamesh track (eBL API adapter)

## Done
- [x] High-level plan document (2026-02-24)
- [x] Move docs to docs/ folder (2026-02-24)
```

## Rules

| Rule | Why |
|------|-----|
| One in-progress at a time | Prevents scope creep, context loss |
| Link to plan in 2_active/ | Connects backlog to execution |
| Mark done only after tests pass | Ensures quality gate |
| Add completion date | Tracks velocity |

## State Transitions

```
Todo → In Progress (when plan moves to 2_active/)
  Link to docs/plans/2_active/stage-name.md

In Progress → Done (when plan moves to 3_complete/)
  Add completion date
```

## Integration

**Used by:**
- **autonomous-work** — Identifies next feature to work on
