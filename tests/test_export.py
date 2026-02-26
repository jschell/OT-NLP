# tests/test_export.py
"""Unit tests for pipeline/modules/export.py.

All subprocess calls and filesystem side-effects are mocked.
No live DB, Sphinx, nbconvert, or Typst installation required.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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
        patch("modules.export.shutil.copy"),  # executed nb copy to docs dir
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
        patch("modules.export.shutil.copy"),  # executed nb copy to docs dir
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
        patch("modules.export.shutil.copy"),  # executed nb copy to docs dir
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
        patch("modules.export.shutil.copy"),  # executed nb copy to docs dir
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
