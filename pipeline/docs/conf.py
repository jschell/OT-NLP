# pipeline/docs/conf.py
"""Sphinx configuration for the Psalms NLP Analysis report site."""

from __future__ import annotations

import sys
from pathlib import Path

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
sys.path.insert(0, str(Path(__file__).parent.parent))
