| name | description |
|------|-------------|
| read-arxiv-paper | Use when implementation decisions require grounding in linguistics or NLP research - fetches and reads arXiv papers from LaTeX source |

# Read arXiv Paper

Fetch and read arXiv papers from their LaTeX source (not PDF).

## Quick Reference

| Step | Action |
|------|--------|
| 1. Normalize | Convert URL to source format |
| 2. Fetch | Download and extract LaTeX source |
| 3. Find entry | Locate main .tex file |
| 4. Read | Follow includes recursively |
| 5. Summarize | Extract key points |

## URL Normalization

Input formats:
- arxiv.org/abs/2601.07372
- arxiv.org/pdf/2601.07372
- arxiv.org/abs/2601.07372v2

Output format:
- arxiv.org/src/2601.07372

Extract ID: `2601.07372` (ignore version suffix)

## Relevant Research Areas for This Project

**Stage 2 — Chiasm Detection:**
- Biblical Hebrew parallelism detection
- Discourse structure in ancient texts
- Chiastic structure computational methods

**Stage 3 — Breath & Phonetic Analysis:**
- Biblical Hebrew phonology and prosody
- Masoretic accent systems
- Vowel openness and sonority in Semitic languages

**Stage 5 — LLM Translation:**
- LLM-assisted literary translation
- Constrained generation for style preservation
- Evaluation metrics for translation quality

## Output Format

```markdown
# Paper: [Title]

**Authors:** [names]
**arXiv:** [id]

## Abstract
[extracted abstract]

## Key Contributions
- [point 1]
- [point 2]

## Method
[summary]

## Results
[key findings]

## Relevance
[how this applies to Psalms NLP pipeline]
```

## Error Handling

| Issue | Solution |
|-------|----------|
| Invalid URL | Show expected format |
| 404 error | Check arxiv ID exists |
| No .tex files | May be PDF-only submission |
| Multiple entrypoints | List files, ask user |
| Encoding issues | Try utf-8, then latin-1 |
