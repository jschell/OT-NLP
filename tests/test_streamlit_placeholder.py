# tests/test_streamlit_placeholder.py
"""Static verification of Streamlit service files."""

from pathlib import Path

STREAMLIT_DIR = Path(__file__).parent.parent / "streamlit"


def test_dockerfile_exists() -> None:
    assert (STREAMLIT_DIR / "Dockerfile.streamlit").exists()


def test_dockerfile_uses_python311_slim() -> None:
    content = (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()
    assert "FROM python:3.11-slim" in content


def test_dockerfile_exposes_8501() -> None:
    content = (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()
    assert "EXPOSE 8501" in content


def test_requirements_exists() -> None:
    assert (STREAMLIT_DIR / "requirements_streamlit.txt").exists()


def test_streamlit_pyproject_exists() -> None:
    """streamlit/pyproject.toml must exist (uv sync source for the container)."""
    assert (STREAMLIT_DIR / "pyproject.toml").exists()


def test_streamlit_pyproject_pins_streamlit() -> None:
    content = (STREAMLIT_DIR / "pyproject.toml").read_text()
    assert "streamlit==" in content


def test_dockerfile_uses_uv_sync() -> None:
    """Streamlit Dockerfile must use 'uv sync' — not pip or uv pip install."""
    content = (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()
    assert "uv sync" in content, "Dockerfile.streamlit must use 'uv sync'"
    assert "pip install" not in content, (
        "Dockerfile.streamlit must not call pip install in any form"
    )


def test_dockerfile_sets_venv_on_path() -> None:
    assert "/app/.venv/bin" in (STREAMLIT_DIR / "Dockerfile.streamlit").read_text()


def test_app_exists() -> None:
    assert (STREAMLIT_DIR / "app.py").exists()



def test_app_has_set_page_config() -> None:
    content = (STREAMLIT_DIR / "app.py").read_text()
    assert "st.set_page_config" in content
