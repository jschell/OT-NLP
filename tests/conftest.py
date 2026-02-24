# tests/conftest.py
"""
Root conftest — adds pipeline/ to sys.path for all tests.

Every test module can import pipeline source directly, e.g.:
    from adapters.translation_adapter import SQLiteScrollmapperAdapter
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
