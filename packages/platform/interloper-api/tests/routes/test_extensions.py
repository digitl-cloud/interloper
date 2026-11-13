"""Tests for ``interloper_api.routes.extensions``."""

from __future__ import annotations

from types import SimpleNamespace
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
from interloper_api.routes.extensions import ExtensionResponse
from interloper_api.settings import ApiSettings

ROUTES = """\
from fastapi import APIRouter

router = APIRouter()
NOT_A_ROUTER = "routes"
"""


def _manifest(key: str, routes: str) -> str:
    return f"manifest_version: 1\nkey: {key}\nname: {key.title()}\nroutes: {routes}\n"


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
        assert "'acme' names a str, not an APIRouter" in failure.error

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
