"""interloper-app — Nuxt SPA frontend, packaged as static assets."""

from __future__ import annotations

from pathlib import Path

_PACKAGE_DIR = Path(__file__).parent


def static_dir() -> Path:
    """Return the path to the built SPA static files.

    Raises:
        FileNotFoundError: If the app hasn't been built yet.
    """
    d = _PACKAGE_DIR / "static"
    if not d.exists() or not (d / "index.html").exists():
        msg = (
            "interloper-app static files not found. "
            "Run 'make build-app' to build the frontend first."
        )
        raise FileNotFoundError(msg)
    return d


def source_dir() -> Path:
    """Return the path to the Nuxt source directory (for dev mode).

    Raises:
        FileNotFoundError: If the Nuxt source directory is not available.
    """
    d = _PACKAGE_DIR.parent.parent / "app"
    if not d.exists():
        msg = "Nuxt source directory not found — dev mode requires a source checkout."
        raise FileNotFoundError(msg)
    return d
