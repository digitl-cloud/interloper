"""Tests for ``interloper_app.assets``.

Both lookups are resolved from the package location, so every test points
``_PACKAGE_DIR`` at a temporary tree rather than depending on whether this
checkout happens to have run ``make build-app``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from interloper_app import assets as assets_module
from interloper_app.assets import source_dir, static_dir


@pytest.fixture
def package_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Relocate the package root into a temporary tree.

    Args:
        tmp_path: The temporary directory standing in for the package location.
        monkeypatch: Fixture used to swap the module-level constant.

    Returns:
        The directory the lookups now resolve against.
    """
    installed = tmp_path / "site-packages" / "interloper_app"
    installed.mkdir(parents=True)
    monkeypatch.setattr(assets_module, "_PACKAGE_DIR", installed)
    return installed


class TestStaticDir:
    """Where the API serves the built SPA from."""

    def test_a_built_app_resolves(self, package_dir: Path) -> None:
        static = package_dir / "static"
        static.mkdir()
        (static / "index.html").write_text("<html>spa</html>")

        assert static_dir() == static

    def test_an_unbuilt_app_says_how_to_build_it(self, package_dir: Path) -> None:
        # API-only images ship without the SPA; the message has to be actionable.
        with pytest.raises(FileNotFoundError, match="Run 'make build-app'"):
            static_dir()

    def test_a_static_dir_without_an_index_is_not_a_build(self, package_dir: Path) -> None:
        # A stale or partial directory must not be served as if it were built.
        (package_dir / "static").mkdir()

        with pytest.raises(FileNotFoundError, match="Run 'make build-app'"):
            static_dir()


class TestSourceDir:
    """Where the dev server runs Nuxt from."""

    def test_the_nuxt_checkout_resolves(self, package_dir: Path) -> None:
        # Two levels up from the package, beside `src/`.
        source = package_dir.parent.parent / "app"
        source.mkdir()

        assert source_dir() == source

    def test_without_a_checkout_it_says_dev_mode_needs_one(self, package_dir: Path) -> None:
        # An installed wheel carries the built assets but no Nuxt source.
        with pytest.raises(FileNotFoundError, match="dev mode requires a source checkout"):
            source_dir()


class TestPackageSurface:
    """Both lookups are the package's public surface."""

    def test_they_are_re_exported(self) -> None:
        import interloper_app

        assert interloper_app.static_dir is static_dir
        assert interloper_app.source_dir is source_dir
        assert set(interloper_app.__all__) == {"source_dir", "static_dir"}
