# tests/test_dockerfile.py
"""Static verification of pipeline/Dockerfile.pipeline."""

from pathlib import Path

DOCKERFILE = Path(__file__).parent.parent / "pipeline" / "Dockerfile.pipeline"


def test_dockerfile_exists() -> None:
    assert DOCKERFILE.exists()


def test_dockerfile_uses_python311_slim() -> None:
    assert "FROM python:3.11-slim" in DOCKERFILE.read_text()


def test_dockerfile_installs_typst_pinned() -> None:
    content = DOCKERFILE.read_text()
    assert "typst" in content.lower()
    assert "0.12.0" in content


def test_dockerfile_workdir_is_pipeline() -> None:
    assert "WORKDIR /pipeline" in DOCKERFILE.read_text()


def test_dockerfile_copies_lockfile() -> None:
    """Dockerfile must copy pyproject.toml and uv.lock, not a requirements.txt."""
    content = DOCKERFILE.read_text()
    assert "pyproject.toml" in content
    assert "uv.lock" in content


def test_dockerfile_uses_uv_sync() -> None:
    """Dockerfile must install via uv sync — not pip or uv pip install."""
    content = DOCKERFILE.read_text()
    assert "uv sync" in content, "Dockerfile must use 'uv sync' to install deps"
    assert (
        "pip install" not in content
    ), "Dockerfile must not call pip install in any form"


def test_dockerfile_sets_venv_on_path() -> None:
    """The venv bin dir must be on PATH so 'python' resolves correctly."""
    assert "/venv/bin" in DOCKERFILE.read_text()


def test_dockerfile_cmd_is_run_py() -> None:
    assert "run.py" in DOCKERFILE.read_text()
