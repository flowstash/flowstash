"""Sphinx configuration for the FlowStash documentation."""

# -- Project information -----------------------------------------------------

project = "FlowStash"
copyright = "2026, FlowStash"
author = "FlowStash"
release = "0.9.3"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- MyST (Markdown) ---------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "attrs_block",
    "attrs_inline",
    "substitution",
    "tasklist",
]
# Generate anchors for h1..h3 so in-page `#section` links resolve.
myst_heading_anchors = 3

# -- Autodoc / Napoleon ------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
}
autodoc_typehints = "description"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# Heavy third-party deps are mocked during autodoc import so building the API
# reference never depends on their (version-sensitive) import side effects.
autodoc_mock_imports = [
    "fastapi",
    "uvicorn",
    "dramatiq",
    "apscheduler",
    "redis",
    "google",
    "firebase_admin",
]

# Only wire up the stdlib inventory when it's actually reachable. CI runners
# occasionally see docs.python.org return 5xx/timeouts, and with -W that would
# fail the whole build over a transient network blip unrelated to our docs.
intersphinx_mapping = {}


def _python_docs_reachable() -> bool:
    import urllib.request

    try:
        urllib.request.urlopen("https://docs.python.org/3/objects.inv", timeout=3)
        return True
    except Exception:
        return False


if _python_docs_reachable():
    intersphinx_mapping["python"] = ("https://docs.python.org/3", None)

# -- HTML output (Furo) ------------------------------------------------------

html_theme = "furo"
html_title = "FlowStash"
html_static_path = ["_static"]
html_logo = "_static/logo.svg"
html_favicon = "_static/favicon.ico"

# Load webfonts (Inter + JetBrains Mono) and our stylesheet on top of Furo.
html_css_files = [
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;450;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap",
    "custom.css",
]

# Content-rich left sidebar, minimal top chrome — Furo's default layout.
html_theme_options = {
    "source_repository": "https://github.com/flowstash/flowstash",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {
        "color-brand-primary": "#0d9488",
        "color-brand-content": "#0d9488",
        "font-stack": '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        "font-stack--monospace": '"JetBrains Mono", ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace',
    },
    "dark_css_variables": {
        "color-brand-primary": "#2dd4bf",
        "color-brand-content": "#5eead4",
    },
}
