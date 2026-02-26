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
        logger.error(
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
        logger.error(
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
            logger.error(
                "Typst compilation failed (returncode %d)",
                typst_result.returncode,
            )
            results["typst"] = "failed"
        else:
            logger.info("PDF written to %s", pdf_path)
            results["typst"] = "ok"
    else:
        logger.info("Typst not available or report.typ not found — skipping PDF")
        results["typst"] = "skipped"

    elapsed = time.monotonic() - t0
    return {
        "rows_written": 0,
        "elapsed_s": round(elapsed, 3),
        **results,
    }
