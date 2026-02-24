# tests/test_package_structure.py
"""Verify that pipeline package directories exist and are importable."""
from pathlib import Path


def test_adapters_init_exists() -> None:
    """pipeline/adapters/__init__.py must exist."""
    assert (
        Path(__file__).parent.parent / "pipeline" / "adapters" / "__init__.py"
    ).exists()


def test_modules_init_exists() -> None:
    """pipeline/modules/__init__.py must exist."""
    assert (
        Path(__file__).parent.parent / "pipeline" / "modules" / "__init__.py"
    ).exists()


def test_adapters_importable() -> None:
    """pipeline/adapters must be importable as a package."""
    import adapters  # noqa: F401


def test_modules_importable() -> None:
    """pipeline/modules must be importable as a package."""
    import modules  # noqa: F401
