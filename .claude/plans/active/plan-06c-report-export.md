# Plan: Stage 06c — Report Export

> **Depends on:** Plan 06a (visualization library complete). Plan 05 optional (suggestions
> enhance report content but are not required for the export pipeline to run).
> **Status:** active

## Goal

Implement `pipeline/modules/export.py` — the Stage 6 pipeline entry point — which
orchestrates notebook execution (nbconvert), HTML site generation (Sphinx), and PDF
compilation (Typst), plus author the supporting docs and notebook stubs that the
export module reads.

## Acceptance Criteria

- `uv run --frozen pytest tests/test_export.py -v` reports 4 tests passed, 0 failed
- `export.run(conn, config)` returns a dict with at minimum
  `{"rows_written": 0, "elapsed_s": float}` plus sub-step status keys
- `pipeline/docs/conf.py`, `index.md`, `analysis.ipynb`, `report.typ`, and
  `references.bib` all exist at the expected paths
- Running `export.run(conn, config)` with a minimal config inside the pipeline container
  produces HTML at `/data/outputs/report/index.html` (Sphinx step) and does not raise
  an unhandled exception regardless of whether Typst is installed
- `uv run --frozen ruff check .` and `uv run --frozen pyright` report no errors

## Architecture

`modules/export.py` follows the standard module interface (`run(conn, config) -> dict`)
and orchestrates three sequential subprocess calls: `jupyter nbconvert` to execute the
analysis notebook and freeze its outputs, `sphinx-build` to convert the executed
notebook and Markdown source into a navigable HTML site, and `typst compile` to render
a structured PDF from a `.typ` template. Typst is optional — the step is skipped
gracefully via `shutil.which("typst")` so the module works even in environments where
the binary is absent. All subprocess calls are mocked in unit tests so neither Sphinx,
nbconvert, nor Typst need to be installed in the test environment.

## Tech Stack

- Python 3.11
- `subprocess` (stdlib) — external process orchestration
- `shutil` (stdlib) — `which()` for optional binary detection, `copy()`
- `pathlib.Path` (stdlib) — filesystem paths
- `psycopg2` — connection type annotation only (no DB queries in export.py)
- `jupyter nbconvert` — notebook execution
- `sphinx-build` — HTML site generation
- `myst-nb` — notebook rendering inside Sphinx
- `sphinx-book-theme` — HTML theme
- `typst` binary — PDF compilation (optional)
- `pytest` + `unittest.mock` — unit tests

---

## Tasks

### Task 1: Write failing tests for `pipeline/modules/export.py`

**Files:** `tests/test_export.py`

**Steps:**

1. Write the test file. All four tests mock `subprocess.run` and `shutil.which` so that
   neither Sphinx, nbconvert, nor Typst need to be installed. The file will fail to
   import until `pipeline/modules/export.py` exists.

   ```python
   # tests/test_export.py
   """Unit tests for pipeline/modules/export.py.

   All subprocess calls and filesystem side-effects are mocked.
   No live DB, Sphinx, nbconvert, or Typst installation required.
   """
   from __future__ import annotations

   import sys
   import time
   from pathlib import Path
   from unittest.mock import MagicMock, patch, call
   import subprocess

   import pytest

   # Ensure pipeline/ is on the path
   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   import modules.export as export_module


   # ── Fixtures ─────────────────────────────────────────────────────

   def _minimal_config(tmp_path: Path) -> dict:
       """Return a minimal config dict with export paths pointing to tmp_path."""
       output_dir = str(tmp_path / "outputs")
       return {
           "export": {
               "output_dir": output_dir,
               "report_dir": str(tmp_path / "outputs" / "report"),
               "pdf_path": str(tmp_path / "outputs" / "report.pdf"),
               "typst_version": "0.12.0",
           }
       }


   # ── test_export_returns_dict ──────────────────────────────────────


   def test_export_returns_dict(tmp_path: Path) -> None:
       """run() returns a dict with at minimum rows_written and elapsed_s."""
       mock_conn = MagicMock()
       config = _minimal_config(tmp_path)

       # All subprocess calls succeed; typst is not available
       mock_result_ok = MagicMock()
       mock_result_ok.returncode = 0
       mock_result_ok.stderr = ""

       with (
           patch("modules.export.subprocess.run", return_value=mock_result_ok),
           patch("modules.export.shutil.which", return_value=None),  # no typst
       ):
           result = export_module.run(mock_conn, config)

       assert isinstance(result, dict)
       assert "rows_written" in result
       assert "elapsed_s" in result
       assert isinstance(result["rows_written"], int)
       assert isinstance(result["elapsed_s"], float)


   # ── test_export_creates_output_dir ───────────────────────────────


   def test_export_creates_output_dir(tmp_path: Path) -> None:
       """run() creates output_dir and report_dir if they do not exist."""
       mock_conn = MagicMock()
       config = _minimal_config(tmp_path)

       output_dir = Path(config["export"]["output_dir"])
       report_dir = Path(config["export"]["report_dir"])

       # Directories must NOT pre-exist for the test to be meaningful
       assert not output_dir.exists()
       assert not report_dir.exists()

       mock_result = MagicMock()
       mock_result.returncode = 0
       mock_result.stderr = ""

       with (
           patch("modules.export.subprocess.run", return_value=mock_result),
           patch("modules.export.shutil.which", return_value=None),
       ):
           export_module.run(mock_conn, config)

       assert output_dir.exists(), "output_dir was not created"
       assert report_dir.exists(), "report_dir was not created"


   # ── test_export_skips_typst_gracefully ────────────────────────────


   def test_export_skips_typst_gracefully(tmp_path: Path) -> None:
       """When typst binary is not found, run() logs a warning and continues.

       The result dict must contain typst: 'skipped' (not raise an exception).
       """
       mock_conn = MagicMock()
       config = _minimal_config(tmp_path)

       mock_result = MagicMock()
       mock_result.returncode = 0
       mock_result.stderr = ""

       with (
           patch("modules.export.subprocess.run", return_value=mock_result),
           patch("modules.export.shutil.which", return_value=None),  # typst absent
       ):
           result = export_module.run(mock_conn, config)

       assert result.get("typst") == "skipped", (
           f"Expected typst='skipped', got {result.get('typst')!r}"
       )


   # ── test_sphinx_build_called ──────────────────────────────────────


   def test_sphinx_build_called(tmp_path: Path) -> None:
       """run() invokes subprocess.run with sphinx-build as the first arg."""
       mock_conn = MagicMock()
       config = _minimal_config(tmp_path)

       mock_result = MagicMock()
       mock_result.returncode = 0
       mock_result.stderr = ""

       with (
           patch("modules.export.subprocess.run", return_value=mock_result) as mock_run,
           patch("modules.export.shutil.which", return_value=None),
       ):
           export_module.run(mock_conn, config)

       # Collect the first positional arg (list) of every subprocess.run call
       invoked_commands = [
           c.args[0][0]  # first element of the command list
           for c in mock_run.call_args_list
           if c.args and isinstance(c.args[0], list)
       ]
       assert "sphinx-build" in invoked_commands, (
           f"sphinx-build not found in called commands: {invoked_commands}"
       )
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_export.py -v
   # Expected: ERROR — ModuleNotFoundError: No module named 'modules.export'
   ```

3. No implementation yet.

4. N/A.

5. N/A.

6. Commit: `"test: add 4 failing tests for export module (TDD red phase)"`

---

### Task 2: Implement `pipeline/modules/export.py`

**Files:** `pipeline/modules/export.py`

**Steps:**

1. Tests already written.

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_export.py -v
   # Expected: ERROR — ModuleNotFoundError: No module named 'modules.export'
   ```

3. Implement:

   ```python
   # pipeline/modules/export.py
   """Stage 6 — Report export orchestrator.

   Sequence:
     1. Execute analysis notebook via jupyter nbconvert (freezes outputs).
     2. Build Sphinx HTML site from frozen notebook + Markdown sources.
     3. Compile Typst PDF from docs/report.typ (skipped if typst not found).

   Entry point follows the standard pipeline module interface:
       run(conn, config) -> dict
   """
   from __future__ import annotations

   import logging
   import shutil
   import subprocess
   import time
   from pathlib import Path

   import psycopg2

   logger = logging.getLogger(__name__)


   def run(
       conn: psycopg2.extensions.connection,
       config: dict,
   ) -> dict:
       """Orchestrate: execute notebook -> build Sphinx HTML -> compile Typst PDF.

       Args:
           conn: Live psycopg2 connection (not used for queries in this stage;
               kept for interface consistency).
           config: Full parsed config.yml.  Reads config["export"] sub-dict with
               keys output_dir, report_dir, pdf_path, typst_version.

       Returns:
           Dict with keys rows_written (always 0), elapsed_s, notebook_execute,
           sphinx, and typst. Each step value is 'ok', 'failed', or 'skipped'.
       """
       t0 = time.monotonic()
       export_cfg: dict = config.get("export", {})

       output_dir = Path(export_cfg.get("output_dir", "/data/outputs"))
       report_dir = Path(export_cfg.get("report_dir", "/data/outputs/report"))
       pdf_path = Path(export_cfg.get("pdf_path", "/data/outputs/report.pdf"))

       docs_dir = Path("/pipeline/docs")
       notebook = docs_dir / "analysis.ipynb"

       output_dir.mkdir(parents=True, exist_ok=True)
       report_dir.mkdir(parents=True, exist_ok=True)

       results: dict[str, object] = {}

       # ── Step 1: Execute notebook ─────────────────────────────────
       executed_notebook = output_dir / "analysis_executed.ipynb"
       logger.info("Executing analysis notebook ...")
       nb_result = subprocess.run(
           [
               "jupyter",
               "nbconvert",
               "--to",
               "notebook",
               "--execute",
               "--ExecutePreprocessor.timeout=300",
               "--output",
               str(executed_notebook),
               str(notebook),
           ],
           capture_output=True,
           text=True,
       )
       if nb_result.returncode != 0:
           logger.exception(
               "Notebook execution failed (returncode %d)",
               nb_result.returncode,
           )
           results["notebook_execute"] = "failed"
       else:
           logger.info("Notebook executed successfully -> %s", executed_notebook)
           results["notebook_execute"] = "ok"
           # Copy executed notebook into docs dir so Sphinx can pick it up
           shutil.copy(executed_notebook, docs_dir / "analysis_executed.ipynb")

       # ── Step 2: Sphinx HTML build ────────────────────────────────
       logger.info("Building Sphinx HTML site ...")
       sphinx_result = subprocess.run(
           [
               "sphinx-build",
               "-b",
               "html",
               "-q",
               str(docs_dir),
               str(report_dir),
           ],
           capture_output=True,
           text=True,
       )
       if sphinx_result.returncode != 0:
           logger.exception(
               "Sphinx build failed (returncode %d)",
               sphinx_result.returncode,
           )
           results["sphinx"] = "failed"
       else:
           logger.info("Sphinx HTML site written to %s", report_dir)
           results["sphinx"] = "ok"

       # ── Step 3: Typst PDF ─────────────────────────────────────────
       typ_file = docs_dir / "report.typ"
       if typ_file.exists() and shutil.which("typst"):
           logger.info("Compiling Typst PDF ...")
           typst_result = subprocess.run(
               ["typst", "compile", str(typ_file), str(pdf_path)],
               capture_output=True,
               text=True,
               timeout=120,
           )
           if typst_result.returncode != 0:
               logger.exception(
                   "Typst compilation failed (returncode %d)",
                   typst_result.returncode,
               )
               results["typst"] = "failed"
           else:
               logger.info("PDF written to %s", pdf_path)
               results["typst"] = "ok"
       else:
           logger.info(
               "Typst not available or report.typ not found — skipping PDF"
           )
           results["typst"] = "skipped"

       elapsed = time.monotonic() - t0
       return {
           "rows_written": 0,
           "elapsed_s": round(elapsed, 3),
           **results,
       }
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_export.py -v
   # Expected:
   # tests/test_export.py::test_export_returns_dict PASSED
   # tests/test_export.py::test_export_creates_output_dir PASSED
   # tests/test_export.py::test_export_skips_typst_gracefully PASSED
   # tests/test_export.py::test_sphinx_build_called PASSED
   # 4 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check pipeline/modules/export.py --fix
   uv run --frozen pyright pipeline/modules/export.py
   ```

6. Commit: `"feat: implement modules/export.py — 4 tests green"`

---

### Task 3: Create `pipeline/docs/conf.py` — Sphinx configuration

**Files:** `pipeline/docs/conf.py`

**Steps:**

1. No automated tests for this file — it is a Sphinx configuration module, not Python
   application code. Integration testing is covered by the Sphinx build in Task 6.

2. N/A.

3. Create the file:

   ```python
   # pipeline/docs/conf.py
   """Sphinx configuration for the Psalms NLP Analysis report site."""
   from __future__ import annotations

   project = "Psalms NLP Analysis"
   author = "Psalms NLP Pipeline"
   release = "0.1"

   extensions = [
       "myst_nb",
       "sphinx.ext.autodoc",
       "sphinxcontrib.bibtex",
   ]

   myst_enable_extensions = ["colon_fence", "deflist", "dollarmath"]

   # Notebooks are pre-executed by nbconvert; do not re-execute inside Sphinx.
   nb_execution_mode = "off"

   html_theme = "sphinx_book_theme"
   html_theme_options = {
       "repository_url": "",
       "use_repository_button": False,
       "show_navbar_depth": 2,
   }

   bibtex_bibfiles = ["references.bib"]

   suppress_warnings = ["myst.header"]

   # Ensure the pipeline source is importable for autodoc
   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent))
   ```

4. Verify the file is syntactically valid:

   ```bash
   python -c "import ast; ast.parse(open('pipeline/docs/conf.py').read()); print('OK')"
   # Expected: OK
   ```

5. Lint:

   ```bash
   uv run --frozen ruff check pipeline/docs/conf.py --fix
   ```

6. Commit: `"feat: add pipeline/docs/conf.py Sphinx configuration"`

---

### Task 4: Create `pipeline/docs/index.md` and `pipeline/docs/references.bib`

**Files:** `pipeline/docs/index.md`, `pipeline/docs/references.bib`

**Steps:**

1. No automated tests needed for these static files.

2. N/A.

3. Create `pipeline/docs/index.md`:

   ```markdown
   # Psalms NLP Analysis

   A computational analysis of phonetic and structural properties in the Hebrew Psalms
   and their English translations.

   This report was generated by the Psalms NLP pipeline using the BHSA morphological
   database and a four-dimensional style fingerprint:
   syllable density, morpheme ratio, sonority score, and clause compression.

   ```{toctree}
   :maxdepth: 2
   :caption: Contents

   analysis_executed
   ```

   ---

   *Generated by the Psalms NLP Pipeline.*
   ```

   Create `pipeline/docs/references.bib`:

   ```bibtex
   @book{alter2007,
     author    = {Alter, Robert},
     title     = {The Book of Psalms: A Translation with Commentary},
     publisher = {W.W. Norton},
     year      = {2007}
   }

   @online{bhsa2021,
     author = {{ETCBC}},
     title  = {Biblia Hebraica Stuttgartensia Amstelodamensis},
     year   = {2021},
     url    = {https://github.com/ETCBC/bhsa}
   }

   @book{watson1984,
     author    = {Watson, Wilfred G. E.},
     title     = {Classical Hebrew Poetry: A Guide to its Techniques},
     publisher = {JSOT Press},
     year      = {1984}
   }
   ```

4. Verify files exist:

   ```bash
   ls pipeline/docs/index.md pipeline/docs/references.bib
   # Expected: both files listed
   ```

5. N/A (not Python source).

6. Commit: `"feat: add Sphinx index.md and references.bib"`

---

### Task 5: Create `pipeline/docs/analysis.ipynb` — notebook stub

**Files:** `pipeline/docs/analysis.ipynb`

**Steps:**

1. No automated tests — the notebook is executed by nbconvert at export time.
   The key requirement is that the notebook is valid JSON and has at least three cells.

2. Validate after creation:

   ```bash
   python -c "import json; nb=json.load(open('pipeline/docs/analysis.ipynb')); \
   print(f\"cells: {len(nb['cells'])}, format: {nb['nbformat']}.{nb['nbformat_minor']}\")"
   # Expected: cells: 3, format: 4.5 (or similar valid nbformat version)
   ```

3. Create the notebook file. The notebook uses nbformat 4.5. The code cells contain
   placeholder DB connection code that works when run inside the pipeline container
   where `/pipeline` is on `sys.path` and environment variables are set.

   ```json
   {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
     "kernelspec": {
      "display_name": "Python 3",
      "language": "python",
      "name": "python3"
     },
     "language_info": {
      "name": "python",
      "version": "3.11.0"
     }
    },
    "cells": [
     {
      "cell_type": "markdown",
      "id": "a1b2c3d4-0001",
      "metadata": {},
      "source": [
       "# Psalms NLP Analysis\n",
       "\n",
       "Computational analysis of phonetic and structural properties in the Hebrew Psalms.\n",
       "Generated by the Psalms NLP pipeline."
      ]
     },
     {
      "cell_type": "code",
      "execution_count": null,
      "id": "a1b2c3d4-0002",
      "metadata": {},
      "outputs": [],
      "source": [
       "import os\n",
       "import sys\n",
       "sys.path.insert(0, '/pipeline')\n",
       "\n",
       "import psycopg2\n",
       "import psycopg2.extras\n",
       "import pandas as pd\n",
       "import plotly.io as pio\n",
       "pio.renderers.default = 'notebook'\n",
       "\n",
       "conn = psycopg2.connect(\n",
       "    host=os.environ.get('POSTGRES_HOST', 'db'),\n",
       "    dbname=os.environ.get('POSTGRES_DB', 'psalms'),\n",
       "    user=os.environ.get('POSTGRES_USER', 'psalms'),\n",
       "    password=os.environ.get('POSTGRES_PASSWORD', 'psalms_dev'),\n",
       ")\n",
       "\n",
       "def query(sql, params=()):\n",
       "    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:\n",
       "        cur.execute(sql, params)\n",
       "        return [dict(r) for r in cur.fetchall()]\n",
       "\n",
       "print('Connected.')"
      ]
     },
     {
      "cell_type": "code",
      "execution_count": null,
      "id": "a1b2c3d4-0003",
      "metadata": {},
      "outputs": [],
      "source": [
       "from visualize.heatmaps import deviation_heatmap\n",
       "from visualize.breath_curves import breath_curve_figure\n",
       "from visualize.radar import fingerprint_radar\n",
       "from visualize.arcs import chiasm_arc_figure\n",
       "from visualize.report import pipeline_summary_chart\n",
       "\n",
       "# ── Deviation heatmap across all Psalms ──────────────────────────\n",
       "score_rows = query(\"\"\"\n",
       "    SELECT v.chapter, ts.translation_key, ts.composite_deviation\n",
       "    FROM translation_scores ts\n",
       "    JOIN verses v ON ts.verse_id = v.verse_id\n",
       "    WHERE v.book_num = 19\n",
       "\"\"\")\n",
       "\n",
       "if score_rows:\n",
       "    import pandas as pd\n",
       "    df = pd.DataFrame(score_rows)\n",
       "    pivot = df.pivot_table(\n",
       "        index='chapter', columns='translation_key',\n",
       "        values='composite_deviation', aggfunc='mean'\n",
       "    )\n",
       "    chapters = sorted(pivot.index.tolist())\n",
       "    keys = pivot.columns.tolist()\n",
       "    matrix = [\n",
       "        [float(pivot.loc[ch, k]) for k in keys]\n",
       "        for ch in chapters\n",
       "    ]\n",
       "    fig_heatmap = deviation_heatmap(psalm_chapters=chapters,\n",
       "                                    translation_keys=keys,\n",
       "                                    scores=matrix)\n",
       "    fig_heatmap.show()\n",
       "\n",
       "    # Save static image for Typst report\n",
       "    import pathlib\n",
       "    fig_dir = pathlib.Path('/data/outputs/figures')\n",
       "    fig_dir.mkdir(parents=True, exist_ok=True)\n",
       "    fig_heatmap.write_image(str(fig_dir / 'deviation_heatmap.png'))\n",
       "    print('Deviation heatmap saved.')\n",
       "else:\n",
       "    print('No score rows found — run Stage 4 first.')\n",
       "\n",
       "# ── Breath curve for Psalm 23:1 ──────────────────────────────────\n",
       "verse_row = query(\"\"\"\n",
       "    SELECT v.verse_id, bp.breath_curve\n",
       "    FROM verses v\n",
       "    LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id\n",
       "    WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1\n",
       "\"\"\")\n",
       "\n",
       "if verse_row and verse_row[0].get('breath_curve'):\n",
       "    heb_curve = verse_row[0]['breath_curve']\n",
       "    fig_breath = breath_curve_figure(\n",
       "        verse_id=verse_row[0]['verse_id'],\n",
       "        hebrew_curve=heb_curve,\n",
       "        translation_curves={},\n",
       "        title='Psalm 23:1 — Hebrew Breath Curve',\n",
       "    )\n",
       "    fig_breath.show()\n",
       "    fig_breath.write_image(str(fig_dir / 'breath_sample.png'))\n",
       "    print('Breath curve saved.')\n",
       "else:\n",
       "    print('No breath profile for Psalm 23:1 — run Stage 3 first.')\n",
       "\n",
       "# ── Pipeline row count summary ───────────────────────────────────\n",
       "count_rows = query(\"\"\"\n",
       "    SELECT 'verses' AS tbl, COUNT(*) AS cnt FROM verses WHERE book_num = 19\n",
       "    UNION ALL SELECT 'word_tokens',        COUNT(*)\n",
       "      FROM word_tokens wt JOIN verses v ON wt.verse_id = v.verse_id\n",
       "      WHERE v.book_num = 19\n",
       "    UNION ALL SELECT 'breath_profiles',    COUNT(*)\n",
       "      FROM breath_profiles bp JOIN verses v ON bp.verse_id = v.verse_id\n",
       "      WHERE v.book_num = 19\n",
       "    UNION ALL SELECT 'translation_scores', COUNT(*)\n",
       "      FROM translation_scores ts JOIN verses v ON ts.verse_id = v.verse_id\n",
       "      WHERE v.book_num = 19\n",
       "\"\"\")\n",
       "counts = {r['tbl']: int(r['cnt']) for r in count_rows}\n",
       "fig_summary = pipeline_summary_chart(row_counts=counts, run_history=[])\n",
       "fig_summary.show()\n",
       "print('Summary chart done.')"
      ]
     }
    ]
   }
   ```

4. Validate the notebook file is well-formed JSON:

   ```bash
   python -c "
   import json
   nb = json.load(open('pipeline/docs/analysis.ipynb'))
   assert nb['nbformat'] == 4
   assert len(nb['cells']) == 3
   print('OK — 3 cells, nbformat 4')
   "
   # Expected: OK — 3 cells, nbformat 4
   ```

5. N/A (not Python source — no ruff or pyright).

6. Commit: `"feat: add analysis.ipynb notebook stub for Sphinx/nbconvert"`

---

### Task 6: Create `pipeline/docs/report.typ` — Typst PDF template

**Files:** `pipeline/docs/report.typ`

**Steps:**

1. No automated tests. Integration testing is done in Task 7 inside the container.

2. N/A.

3. Create the Typst template. The template uses the `charged-ieee` preset for a clean
   academic layout. Figure paths reference the static images saved by the notebook.

   ```typst
   // pipeline/docs/report.typ
   // Psalms NLP Analysis — Typst PDF template
   //
   // Compiled by: typst compile report.typ /data/outputs/report.pdf
   // Pre-requisite: analysis.ipynb must have been executed and figures saved to
   //   /data/outputs/figures/

   #import "@preview/charged-ieee:0.1.3": ieee

   #show: ieee.with(
     title: [Psalms NLP: Quantifying Translation Fidelity to Hebrew Phonetic Structure],
     authors: (
       (name: "Psalms NLP Pipeline", organization: "Self-hosted"),
     ),
     abstract: [
       This report presents a computational analysis of the Hebrew Psalms corpus
       using morphological and phonetic fingerprinting to quantify style deviation
       and breath alignment across configured English translations. Four dimensions
       are measured per verse: syllable density, morpheme ratio, sonority score,
       and clause compression. Deviation scores and breath alignment metrics are
       stored in PostgreSQL and visualised as heatmaps, radar charts, and arc
       diagrams.
     ],
   )

   = Introduction

   The analysis is conducted using the BHSA (Biblia Hebraica Stuttgartensia
   Amstelodamensis) morphological database @bhsa2021, which provides word-level
   annotation for the entire Hebrew Bible. Each verse in the Psalms is characterised
   by a four-dimensional fingerprint vector and a per-syllable breath weight curve.
   English translations are scored against these fingerprints to produce composite
   deviation and breath alignment metrics.

   = Methodology

   Syllable density is computed from the BHSA syllable segmentation. Morpheme ratio
   is derived from the ratio of roots to total word tokens. Sonority score aggregates
   vowel openness values from the syllable token table. Clause compression quantifies
   the number of syntactic clauses per colon.

   Breath weight curves map syllable-level phonetic weight to a normalised [0, 1]
   interval. Translation stress positions are estimated via English syllabification
   and compared to the Hebrew curve using Pearson correlation.

   = Results

   == Style Deviation Heatmap

   #figure(
     image("/data/outputs/figures/deviation_heatmap.png", width: 100%),
     caption: [
       Mean style deviation by Psalm chapter and translation. Red cells indicate
       high composite deviation; green cells indicate close alignment to the
       Hebrew fingerprint.
     ],
   )

   == Breath Curve — Psalm 23:1

   #figure(
     image("/data/outputs/figures/breath_sample.png", width: 80%),
     caption: [
       Per-syllable breath weight curve for Psalm 23:1 (Hebrew source).
       Relative position on the x-axis normalises verse length to [0, 1].
     ],
   )

   = Discussion

   Translations with lower composite deviation values more closely mirror the
   phonetic and structural density of the Hebrew source. Breath alignment scores
   above 0.7 indicate reasonable stress correspondence. Results should be interpreted
   alongside traditional scholarly commentary @alter2007 @watson1984 — computational
   metrics are descriptive, not prescriptive.

   = Conclusion

   The Psalms NLP pipeline provides reproducible, quantitative fingerprints for
   every verse in the Psalter and scores any configured English translation against
   them. The pipeline is config-driven and designed for expansion to other Hebrew
   Bible books.

   #bibliography("references.bib")
   ```

4. Verify the file is readable plain text:

   ```bash
   python -c "
   content = open('pipeline/docs/report.typ').read()
   assert '#import' in content
   assert 'deviation_heatmap' in content
   print(f'OK — {len(content)} chars')
   "
   # Expected: OK — <N> chars
   ```

5. N/A (Typst source, not Python).

6. Commit: `"feat: add Typst PDF template pipeline/docs/report.typ"`

---

### Task 7: Integration test inside the pipeline container

**Files:** none (verification only)

This task verifies that the full export pipeline runs end-to-end inside the Docker
container, where all dependencies (Sphinx, myst-nb, nbconvert, Typst) are installed.

**Steps:**

1. No new test code.

2. Start the pipeline container (without running the full pipeline):

   ```bash
   docker compose run --rm pipeline bash
   ```

3. Inside the container, run the following sequence:

   ```bash
   # Verify docs directory is mounted correctly
   ls /pipeline/docs/
   # Expected: analysis.ipynb  conf.py  index.md  references.bib  report.typ

   # Verify visualize package is importable
   python -c "from visualize import breath_curve_figure; print('OK')"
   # Expected: OK

   # Verify export module is importable and interface is correct
   python -c "
   import modules.export as e
   import inspect
   sig = inspect.signature(e.run)
   print('params:', list(sig.parameters.keys()))
   "
   # Expected: params: ['conn', 'config']

   # Run export with a minimal config (no live DB needed for path creation test)
   python -c "
   from unittest.mock import MagicMock
   import modules.export as e
   conn = MagicMock()
   config = {
       'export': {
           'output_dir': '/tmp/test_output',
           'report_dir': '/tmp/test_output/report',
           'pdf_path': '/tmp/test_output/report.pdf',
       }
   }
   result = e.run(conn, config)
   print('result:', result)
   assert 'rows_written' in result
   assert 'elapsed_s' in result
   print('PASS')
   "
   # Expected: result: {'rows_written': 0, 'elapsed_s': ..., 'notebook_execute': ...,
   #                     'sphinx': ..., 'typst': ...}
   # Expected: PASS

   # Check if Sphinx HTML was generated
   ls /tmp/test_output/report/index.html 2>/dev/null && echo "HTML OK" || echo "HTML not found"
   # Expected: HTML OK  (Sphinx step ran) or "HTML not found" if nbconvert failed first

   # Check if Typst is available
   which typst && echo "Typst present" || echo "Typst not installed"
   ```

4. Confirm outputs:

   - If Sphinx ran: `/tmp/test_output/report/index.html` exists
   - If Typst is installed: `/tmp/test_output/report.pdf` exists
   - If Typst is absent: `result["typst"] == "skipped"` (no exception raised)

5. Exit the container:

   ```bash
   exit
   ```

6. Commit: `"chore: Stage 06c integration verification complete"`

---

### Task 8: Run full test suite and final quality checks

**Files:** none (verification only)

**Steps:**

1. No new tests.

2. Run the complete test suite to confirm nothing was broken by new additions:

   ```bash
   uv run --frozen pytest tests/test_visualize.py tests/test_export.py -v
   # Expected: 12 passed (8 visualize + 4 export)
   ```

   If the streamlit test file also exists:

   ```bash
   uv run --frozen pytest tests/test_streamlit_queries.py -v
   # Expected: 8 passed
   ```

3. No implementation changes.

4. Confirm all tests pass.

5. Full lint, format, and typecheck:

   ```bash
   uv run --frozen ruff check pipeline/ streamlit/ tests/ --fix
   uv run --frozen ruff format pipeline/ streamlit/ tests/
   uv run --frozen pyright pipeline/modules/export.py
   ```

   Resolve any remaining type errors before closing the plan.

6. Commit: `"feat: Stage 06c complete — export module, docs, notebook, Typst template"`
