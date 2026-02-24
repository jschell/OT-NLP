# tests/test_scaffold.py
"""Verify that the project scaffold is correctly configured."""

import sys
from pathlib import Path


def test_pipeline_on_sys_path() -> None:
    """conftest.py must add pipeline/ to sys.path so pipeline modules import."""
    pipeline_dir = str(Path(__file__).parent.parent / "pipeline")
    assert (
        pipeline_dir in sys.path
    ), f"pipeline/ not in sys.path.\nExpected: {pipeline_dir}\nGot: {sys.path}"


def test_pyproject_toml_exists() -> None:
    """pyproject.toml must exist at repo root."""
    assert (Path(__file__).parent.parent / "pyproject.toml").exists()


def test_pyproject_has_pytest() -> None:
    """pyproject.toml must declare pytest as a dev dependency."""
    content = (Path(__file__).parent.parent / "pyproject.toml").read_text()
    assert "pytest" in content


def test_pre_commit_config_exists() -> None:
    """.pre-commit-config.yaml must exist at repo root."""
    assert (Path(__file__).parent.parent / ".pre-commit-config.yaml").exists()


def test_pre_commit_uses_ruff() -> None:
    """.pre-commit-config.yaml must configure ruff hooks."""
    content = (Path(__file__).parent.parent / ".pre-commit-config.yaml").read_text()
    assert "ruff" in content
