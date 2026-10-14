"""Tests for ``interloper_app.assets``.

Both lookups are resolved from the package location, so every test points
``_PACKAGE_DIR`` at a temporary tree rather than depending on whether this
checkout happens to have run ``make build-app``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from interloper_app import assets as assets_module
from interloper_app.assets import SpaFallback, source_dir, static_dir


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
    """Both lookups and the SPA fallback are the package's public surface."""

    def test_they_are_re_exported(self) -> None:
        import interloper_app

        assert interloper_app.static_dir is static_dir
        assert interloper_app.source_dir is source_dir
        assert interloper_app.SpaFallback is SpaFallback
        assert set(interloper_app.__all__) == {"SpaFallback", "source_dir", "static_dir"}


class TestSpaFallback:
    """The SPA fallback only takes over requests the API does not own."""

    @pytest.fixture
    def client(self, tmp_path: Path) -> Any:
        """A FastAPI app with one API route, wrapped by the fallback over a built SPA.

        Args:
            tmp_path: The directory standing in for the built SPA.

        Returns:
            A test client for the wrapped app.
        """
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        (tmp_path / "index.html").write_text("<html>spa</html>")
        (tmp_path / "favicon.ico").write_bytes(b"icon-bytes")

        app = FastAPI()

        @app.get("/api/health")
        def health() -> dict[str, str]:
            return {"status": "ok"}

        app.add_middleware(SpaFallback, directory=tmp_path)
        return TestClient(app)

    def test_api_routes_are_served_by_the_api(self, client: Any) -> None:
        assert client.get("/api/health").json() == {"status": "ok"}

    def test_unmatched_api_routes_keep_their_404(self, client: Any) -> None:
        # Falling back to index.html here would turn a bad API call into a
        # 200 page and hide the error from the frontend.
        assert client.get("/api/does-not-exist").status_code == 404

    def test_existing_files_are_served_from_disk(self, client: Any) -> None:
        assert client.get("/favicon.ico").content == b"icon-bytes"

    def test_client_side_routes_fall_back_to_index(self, client: Any) -> None:
        response = client.get("/organisations/1/jobs")

        assert response.status_code == 200
        assert "spa" in response.text

    def test_the_root_serves_index(self, client: Any) -> None:
        assert "spa" in client.get("/").text

    async def test_the_api_prefix_matches_the_segment_only(self, tmp_path: Path) -> None:
        """``/api`` and ``/api/...`` reach the API; ``/apifoo`` is an SPA route served index.html."""
        (tmp_path / "index.html").write_text("<html>spa</html>")
        reached: list[str] = []
        sent: list[dict[str, Any]] = []

        async def downstream(scope: dict[str, Any], receive: Any, send: Any) -> None:
            reached.append(scope["path"])

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": b""}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        middleware = SpaFallback(downstream, directory=tmp_path)  # ty: ignore[invalid-argument-type]
        for path in ("/api", "/api/health", "/apifoo"):
            scope = {"type": "http", "path": path, "method": "GET", "headers": [], "query_string": b""}
            await middleware(scope, receive, send)  # ty: ignore[invalid-argument-type]

        assert reached == ["/api", "/api/health"]
        assert any(message.get("status") == 200 for message in sent)

    async def test_websocket_traffic_is_left_to_the_application(self, tmp_path: Path) -> None:
        (tmp_path / "index.html").write_text("<html>spa</html>")
        delegated: list[str] = []

        async def downstream(scope: dict[str, Any], receive: Any, send: Any) -> None:
            delegated.append(scope["type"])

        middleware = SpaFallback(downstream, directory=tmp_path)  # ty: ignore[invalid-argument-type]
        await middleware({"type": "websocket", "path": "/events"}, None, None)  # ty: ignore[invalid-argument-type]

        assert delegated == ["websocket"]
