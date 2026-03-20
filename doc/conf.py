# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "kafka-dae-diagnostics"
copyright = ""
author = "ISIS Experiment Controls"
release = ""

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

nitpicky = True
nitpick_ignore_regex = [
    ("py:class", r"^.*\.T$"),
    ("py:obj", r"^.*\.T$"),
    ("py:class", r"^.*\.T.*_co$"),
    ("py:obj", r"^.*\.T.*_co$"),
    ("py:class", r"^numpy.uint64$"),
    ("py:class", r"^numpy.int32"),
    ("py:class", r"^numpy.float64"),
    ("py:class", r"^cimpl.Consumer"),
    ("py:class", r"^annotated_types\..*"),
    ("py:class", r"^pydantic\..*"),
]

myst_enable_extensions = ["dollarmath", "strikethrough", "colon_fence", "attrs_block"]
suppress_warnings = ["myst.strikethrough"]

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    # and making summary tables at the top of API docs
    "sphinx.ext.autosummary",
    # This can parse google style docstrings
    "sphinx.ext.napoleon",
    # For linking to external sphinx documentation
    "sphinx.ext.intersphinx",
    # Add links to source code in API docs
    "sphinx.ext.viewcode",
    # Mermaid diagrams
    "sphinxcontrib.mermaid",
    # Documentation links in code blocks
    "sphinx_codeautolink",
]
mermaid_d3_zoom = True
napoleon_google_docstring = True
napoleon_numpy_docstring = False

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_context = {
    "display_github": True,  # Integrate GitHub
    "github_user": "ISISComputingGroup",  # Username
    "github_repo": "kafka_dae_diagnostics",  # Repo name
    "github_version": "main",  # Version
    "conf_py_path": "/doc/",  # Path in the checkout to the docs root
}

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "logo_only": False,
    "style_nav_header_background": "#343131",
}
html_static_path = ["_static"]
html_css_files = [
    "css/custom.css",
]

autoclass_content = "init"
myst_heading_anchors = 7
autodoc_preserve_defaults = True

spelling_lang = "en_GB"
spelling_filters = ["enchant.tokenize.MentionFilter"]
spelling_warning = True
spelling_show_suggestions = True
spelling_suggestion_limit = 3

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pcaspy": ("https://pcaspy.readthedocs.io/en/latest/", None),
    "p4p": ("https://epics-base.github.io/p4p/", None),
    "fastcs": ("https://diamondlightsource.github.io/fastcs/main/", None),
    "genie": ("https://isiscomputinggroup.github.io/genie", None),
    "ibex_bluesky_core": ("https://isiscomputinggroup.github.io/ibex_bluesky_core", None),
    "ibex_developers_manual": ("https://isiscomputinggroup.github.io/ibex_developers_manual", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "confluent_kafka": (
        "https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/",
        None,
    ),
    "pydantic": ("https://docs.pydantic.dev", None),
}
