"""Tests for ``interloper_api.routes.extensions``."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.catalog import Catalog
from interloper.extension import Extension, ExtensionFailure
from interloper_db import Store

from interloper_api.app import create_app
from interloper_api.dependencies import get_caller
from interloper_api.routes import extensions as extensions_module
from interloper_api.routes.extensions import ExtensionResponse, UIBundle
from interloper_api.settings import ApiSettings

ROUTES = """\
from fastapi import APIRouter

router = APIRouter()
NOT_A_ROUTER = "routes"
"""

UI = """\
from pathlib import Path

from interloper_api import UIBundle

UI = UIBundle.from_vite(Path(__file__).parent / "static")
"""

VITE_MANIFEST: dict[str, dict[str, Any]] = {
    "src/index.ts": {
        "file": "assets/index-3f9a1c.js",
        "isEntry": True,
        "imports": ["_shared-77aa01.js"],
        "css": ["assets/index-b72e04.css"],
    },
    "_shared-77aa01.js": {
        "file": "assets/shared-77aa01.js",
        "imports": ["src/index.ts"],
        "css": ["assets/shared-0c11d2.css", "assets/index-b72e04.css"],
        "assets": ["assets/logo-5e5e5e.svg"],
    },
}


def _manifest(key: str, routes: str) -> str:
    return f"manifest_version: 1\nkey: {key}\nname: {key.title()}\nroutes: {routes}\n"


def _build(directory: Path, manifest: dict[str, dict[str, Any]] | None = None) -> Path:
    """Write a Vite build: the manifest, the files it names, and an unhashed public file.

    Args:
        directory: The build's output directory, created.
        manifest: The chunks to write; the module's two-chunk build when ``None``.

    Returns:
        The directory.
    """
    chunks = VITE_MANIFEST if manifest is None else manifest
    (directory / ".vite").mkdir(parents=True)
    (directory / ".vite" / "manifest.json").write_text(json.dumps(chunks))
    for chunk in chunks.values():
        for path in (chunk["file"], *chunk.get("css", ()), *chunk.get("assets", ())):
            (directory / path).parent.mkdir(parents=True, exist_ok=True)
            (directory / path).write_text(f"/* {path} */")
    (directory / "favicon.svg").write_text("<svg/>")
    return directory


def _files(directory: Path, **files: str) -> Path:
    """Write a build of any bundler: files by path relative to the directory.

    Args:
        directory: The build's output directory, created.
        **files: Each file's content, by its path with ``__`` for ``/``.

    Returns:
        The directory.
    """
    for name, content in files.items():
        path = directory / name.replace("__", "/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return directory


class TestUIBundle:
    def test_a_build_is_declared_by_its_entry_and_stylesheets(self, tmp_path: Path):
        directory = _files(tmp_path / "dist", **{"main.js": "export {}", "styles__main.css": "a {}"})

        bundle = UIBundle(directory, entry="main.js", css=["styles/main.css"])

        assert (bundle.entry, bundle.css) == ("main.js", ("styles/main.css",))

    def test_the_entry_is_index_js_and_no_stylesheet_unless_named(self, tmp_path: Path):
        bundle = UIBundle(_files(tmp_path / "dist", **{"index.js": "export {}"}))

        assert (bundle.entry, bundle.css) == ("index.js", ())

    @pytest.mark.parametrize(("entry", "css"), [("main.js", ()), ("index.js", ("index.css",))])
    def test_a_named_file_the_directory_does_not_hold_is_refused(self, tmp_path: Path, entry: str, css: tuple):
        directory = _files(tmp_path / "dist", **{"index.js": "export {}"})

        with pytest.raises(FileNotFoundError, match="holds no"):
            UIBundle(directory, entry=entry, css=css)

    def test_the_digest_follows_every_file_its_path_and_content(self, tmp_path: Path):
        def digest(name: str, **files: str) -> str:
            return UIBundle(_files(tmp_path / name, **{"index.js": "export {}", **files})).digest

        base = digest("base", **{"chunk.js": "1"})

        assert digest("same", **{"chunk.js": "1"}) == base
        assert digest("edited", **{"chunk.js": "2"}) != base
        assert digest("renamed", **{"part.js": "1"}) != base
        assert digest("added", **{"chunk.js": "1", "logo.svg": "<svg/>"}) != base


class TestFromVite:
    def test_the_entry_and_its_stylesheets_come_from_the_vite_manifest(self, tmp_path: Path):
        bundle = UIBundle.from_vite(_build(tmp_path / "static"))

        assert bundle.entry == "assets/index-3f9a1c.js"
        assert bundle.css == ("assets/index-b72e04.css", "assets/shared-0c11d2.css")

    def test_a_directory_without_a_build_cannot_be_declared(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            UIBundle.from_vite(tmp_path)

    @pytest.mark.parametrize("entries", [0, 2])
    def test_a_build_names_exactly_one_entry(self, tmp_path: Path, entries: int):
        chunks = {f"src/page{i}.ts": {"file": f"assets/page{i}-0a0a0a.js", "isEntry": i < entries} for i in range(2)}

        with pytest.raises(ValueError, match=f"names {entries} entry chunks, expected one"):
            UIBundle.from_vite(_build(tmp_path / "static", chunks))


class TestUISection:
    def test_a_declared_bundle_is_named_by_the_extension_key(self, install, load_extensions, tmp_path: Path):
        install("acme_ext", "manifest_version: 1\nkey: acme\nui: acme_ext.ui:UI\n", ui=UI)
        _build(tmp_path / "acme_ext" / "static")
        manifests = load_extensions()

        assert extensions_module.UI_BUNDLES.keys() == ("acme",)
        assert manifests.failures == ()

    def test_a_package_installed_without_its_build_is_skipped_and_recorded(self, install, load_extensions):
        install("acme_ext", "manifest_version: 1\nkey: acme\nui: acme_ext.ui:UI\n", ui=UI)
        manifests = load_extensions()

        assert extensions_module.UI_BUNDLES.keys() == ()
        (failure,) = manifests.failures
        assert (failure.package, failure.section, failure.entry) == ("acme_ext", "ui", "acme")
        assert "FileNotFoundError" in failure.error

    def test_an_entry_naming_something_else_is_skipped_and_recorded(self, install, load_extensions):
        install("acme_ext", "manifest_version: 1\nkey: acme\nui: acme_ext.routes:router\n", routes=ROUTES)
        manifests = load_extensions()

        (failure,) = manifests.failures
        assert "'acme' names an instance of APIRouter, not a UIBundle" in failure.error

    def test_a_name_that_is_no_path_segment_is_skipped_and_recorded(self, install, load_extensions, tmp_path: Path):
        install("acme_ext", "manifest_version: 1\nkey: acme\nui:\n  Acme-UI: acme_ext.ui:UI\n", ui=UI)
        _build(tmp_path / "acme_ext" / "static")
        manifests = load_extensions()

        assert extensions_module.UI_BUNDLES.keys() == ()
        (failure,) = manifests.failures
        assert "'Acme-UI' is not a lowercase identifier" in failure.error


class TestServingUIBundles:
    @pytest.fixture
    def app(self, install, load_extensions, fake_settings: ApiSettings, oauth_store: Store, tmp_path: Path) -> FastAPI:
        install("acme_ext", "manifest_version: 1\nkey: acme\nname: Acme\nui: acme_ext.ui:UI\n", ui=UI)
        install("plain_ext", "manifest_version: 1\nkey: plain\nname: Plain\n")
        _build(tmp_path / "acme_ext" / "static")
        load_extensions()
        return create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

    @pytest.fixture
    def root(self, app: FastAPI) -> str:
        return f"/api/ext/acme/ui/{extensions_module.UI_BUNDLES['acme'].digest}"

    def test_the_entry_is_served_to_anyone_and_cached_for_good(self, app: FastAPI, root: str):
        response = TestClient(app).get(f"{root}/assets/index-3f9a1c.js")

        assert response.status_code == 200
        assert response.text == "/* assets/index-3f9a1c.js */"
        assert response.headers["content-type"].startswith("text/javascript")
        assert response.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_a_file_whose_name_carries_no_hash_is_cached_for_good_too(self, app: FastAPI, root: str):
        response = TestClient(app).get(f"{root}/favicon.svg")

        assert response.status_code == 200
        assert response.headers["cache-control"] == "public, max-age=31536000, immutable"

    @pytest.mark.parametrize("path", ["assets/missing-000000.js", "../plain_ext/__init__.py"])
    def test_a_file_outside_the_build_is_a_404(self, app: FastAPI, root: str, path: str):
        assert TestClient(app).get(f"{root}/{path}").status_code == 404

    @pytest.mark.parametrize("stale", ["/api/ext/acme/ui/0123456789abcdef", "/api/ext/acme/ui"])
    def test_a_file_under_another_digest_is_a_404(self, app: FastAPI, stale: str):
        assert TestClient(app).get(f"{stale}/assets/index-3f9a1c.js").status_code == 404

    def test_the_listing_addresses_the_entry_and_its_stylesheets(self, app: FastAPI, root: str):
        app.dependency_overrides[get_caller] = lambda: SimpleNamespace(id=uuid4(), is_super_admin=False)

        listing = {item["key"]: item["ui"] for item in TestClient(app).get("/api/extensions").json()}

        assert listing == {
            "acme": {
                "entry": f"{root}/assets/index-3f9a1c.js",
                "css": [f"{root}/assets/index-b72e04.css", f"{root}/assets/shared-0c11d2.css"],
            },
            "plain": None,
        }


class TestRoutesSection:
    def test_a_declared_router_is_named_by_the_extension_key(self, install, load_extensions):
        install("acme_ext", _manifest("acme", "acme_ext.routes:router"), routes=ROUTES)
        manifests = load_extensions()

        assert extensions_module.EXTENSION_ROUTERS.keys() == ("acme",)
        assert manifests.failures == ()

    def test_an_entry_naming_something_else_is_skipped_and_recorded(self, install, load_extensions):
        install("acme_ext", _manifest("acme", "acme_ext.routes:NOT_A_ROUTER"), routes=ROUTES)
        manifests = load_extensions()

        assert extensions_module.EXTENSION_ROUTERS.keys() == ()
        (failure,) = manifests.failures
        assert (failure.package, failure.section, failure.entry) == ("acme_ext", "routes", "acme")
        assert "'acme' names an instance of str, not an APIRouter" in failure.error

    def test_a_name_that_is_no_path_segment_is_skipped_and_recorded(self, install, load_extensions):
        install(
            "acme_ext",
            "manifest_version: 1\nkey: acme\nroutes:\n  Acme-Routes: acme_ext.routes:router\n",
            routes=ROUTES,
        )
        manifests = load_extensions()

        assert extensions_module.EXTENSION_ROUTERS.keys() == ()
        (failure,) = manifests.failures
        assert "'Acme-Routes' is not a lowercase identifier" in failure.error


class TestExtensionResponse:
    def test_a_package_with_nothing_skipped_is_ok(self):
        extension = Extension("acme_ext", "1.2.3", "acme", "Acme", "Acme things.", "i-lucide-box")
        other = ExtensionFailure("other_ext", "routes", "other", "failed to load: ImportError: boom")

        assert ExtensionResponse.from_extension(extension, [other]) == ExtensionResponse(
            package="acme_ext",
            key="acme",
            name="Acme",
            description="Acme things.",
            icon="i-lucide-box",
            version="1.2.3",
            status="ok",
        )

    def test_a_package_with_anything_skipped_failed(self):
        extension = Extension("broken_ext", None)
        failure = ExtensionFailure("broken_ext", None, None, "package 'broken_ext' ships no extension.yaml")

        assert ExtensionResponse.from_extension(extension, [failure]).status == "failed"


class TestListExtensions:
    @pytest.fixture
    def app(self, install, load_extensions, fake_settings: ApiSettings, oauth_store: Store) -> FastAPI:
        install("acme_ext", _manifest("acme", "acme_ext.routes:router"), routes=ROUTES)
        install("broken_ext", _manifest("broken", "broken_ext.routes:router"))
        load_extensions()
        return create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

    def test_every_signed_in_user_reads_the_installed_packages_and_their_status(self, app: FastAPI):
        app.dependency_overrides[get_caller] = lambda: SimpleNamespace(id=uuid4(), is_super_admin=False)

        response = TestClient(app).get("/api/extensions")

        assert response.status_code == 200
        assert [(item["key"], item["name"], item["status"]) for item in response.json()] == [
            ("acme", "Acme", "ok"),
            ("broken", "Broken", "failed"),
        ]
        assert "failures" not in response.json()[1]

    def test_an_anonymous_caller_is_refused(self, app: FastAPI):
        assert TestClient(app).get("/api/extensions").status_code == 401
