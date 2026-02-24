| name | description |
|------|-------------|
| repo-init | Use when bootstrapping a new repository from scratch — creates pyproject.toml, .gitignore, .env.example, docker-compose.yml skeleton, pre-commit config, README.md, and CLAUDE.md |

# Repo Init

## Overview

Bootstrap a new repository so it is immediately usable: version-controlled,
lintable, testable, and documented before the first line of product code.

**Core principle:** "Green before you grow — every tool must pass on an empty
codebase before feature work begins."

---

## When to Use

- Starting a brand-new Python project
- Converting an ad-hoc directory into a proper repo
- Rebuilding a project scaffold after structural changes

---

## Checklist

Work through these in order. Each item should be committed before the next.

### 1. Git

```bash
git init
git branch -M main
```

### 2. Python package (uv)

```bash
uv init --name <project-name> --python 3.11
```

Creates `pyproject.toml`, `.python-version`, and a minimal `src/` layout.

Add core dev dependencies:

```bash
uv add --dev pytest ruff pyright pre-commit
```

Verify the toolchain works on an empty codebase:

```bash
uv run --frozen pytest          # 0 tests collected, exit 0
uv run --frozen ruff check .    # no issues
uv run --frozen pyright         # 0 errors
```

### 3. `.gitignore`

Include at minimum:

```
# Python
__pycache__/
*.pyc
.venv/
.uv/
dist/

# Env
.env
*.env

# Data (large binary / downloaded corpus)
/data/bhsa/
/data/translations/*.db

# Editor
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db
```

### 4. `.env.example`

Document every environment variable the project needs. Actual `.env` is git-ignored.

```bash
# Database
POSTGRES_USER=psalms
POSTGRES_PASSWORD=changeme
POSTGRES_DB=psalms_db

# LLM (all optional — set provider=none to skip)
LLM_PROVIDER=none
ANTHROPIC_API_KEY=
OPENAI_API_KEY=
GEMINI_API_KEY=
OPENROUTER_API_KEY=
ESV_API_KEY=
```

### 5. `docker-compose.yml` skeleton

Provide a minimal working skeleton with all services named and port-mapped.
Credentials must come from environment variables — never hardcoded.

```yaml
services:
  db:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  pipeline:
    build:
      context: ./pipeline
      dockerfile: Dockerfile.pipeline
    depends_on: [db]
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - ./pipeline:/pipeline
      - ./data:/data
    profiles: [pipeline]

  jupyter:
    image: jupyter/base-notebook:python-3.11
    ports:
      - "8888:8888"
    volumes:
      - ./notebooks:/home/jovyan/work
      - ./data:/home/jovyan/data

  streamlit:
    build:
      context: ./streamlit
    ports:
      - "8501:8501"
    depends_on: [db]
    profiles: [ui]

volumes:
  postgres_data:
```

### 6. Pre-commit config

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.9.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
```

Install hooks:

```bash
uv run pre-commit install
uv run pre-commit run --all-files   # must pass clean
```

### 7. `README.md`

Minimum sections:

- One-line project description
- What it does (numbered steps)
- Architecture diagram (ASCII)
- Prerequisites
- Quick-start commands
- Development commands (test / lint / format / typecheck)
- Project status table

### 8. Claude Code context files

Create `.claude/CLAUDE.md` and root `AGENTS.md` using the **project-setup** skill.

### 9. Initial commit

```bash
git add .
git commit -m "chore: bootstrap repo scaffold"
```

---

## Verification Checklist

Before marking init complete, confirm all pass:

```bash
uv run --frozen pytest              # exit 0
uv run --frozen ruff check .        # no issues
uv run --frozen ruff format --check . # no diffs
uv run --frozen pyright             # 0 errors
uv run pre-commit run --all-files   # all hooks pass
docker compose config               # valid YAML, no missing vars
```

---

## Integration

**Followed by:**
- **project-setup** — Create `.claude/CLAUDE.md` and `AGENTS.md`
- **writing-plans** — Write Stage 0 implementation plan
- **test-driven-development** — First feature work begins here

**Do not use repo-init on an existing project with uncommitted changes.**
Read and understand the current state first, then consult the human.
