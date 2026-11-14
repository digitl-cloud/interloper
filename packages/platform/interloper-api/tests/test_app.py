"""Tests for the app factory — error handlers, optional agent routes, feature flags."""

from __future__ import annotations

import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.catalog import Catalog
from interloper.errors import CatalogKeyError, ConfigError
from interloper_agent.settings import AgentSettings
from interloper_db import (
    ComponentDriftError,
    ConflictError,
    HydrationError,
    InUseError,
    NotFoundError,
    Organisation,
    Profile,
    QuotaExceededError,
    Store,
)

from interloper_api.app import create_app, install_error_handlers
from interloper_api.dependencies import get_caller
from interloper_api.routes import websocket as websocket_module
from interloper_api.settings import ApiSettings

_ORG = "00000000-0000-0000-0000-000000000000"


def test_a_plain_http_external_url_mounts_no_authorization_server(
    fake_settings: ApiSettings, oauth_store: Store, caplog: pytest.LogCaptureFixture
):
    AgentSettings.get().enabled = False
    fake_settings.server.external_url = "http://interloper.example.com"

    with caplog.at_level("WARNING", logger="interloper_api.app"):
        app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

    assert TestClient(app).get("/.well-known/oauth-authorization-server").status_code == 503
    assert "OAuth authorization server not mounted" in caplog.text


def test_agent_routes_absent_when_disabled(fake_settings: ApiSettings, oauth_store: Store):
    AgentSettings.get().enabled = False
    app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)
    client = TestClient(app)

    assert client.post(f"/api/organisations/{_ORG}/agent/conversations").status_code == 404
    assert app.state.interloper.features == {"agent": False}


def test_agent_routes_mounted_when_enabled(fake_settings: ApiSettings, oauth_store: Store):
    app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)
    client = TestClient(app)

    # 401 (not 404): the router is mounted and the auth guard answers first.
    assert client.post(f"/api/organisations/{_ORG}/agent/conversations").status_code == 401
    assert app.state.interloper.features == {"agent": True}


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", f"/api/organisations/{_ORG}/relations"),
        ("GET", f"/api/organisations/{_ORG}/executions"),
        ("GET", f"/api/organisations/{_ORG}/runs"),
        ("GET", f"/api/organisations/{_ORG}/components"),
        ("GET", f"/api/organisations/{_ORG}/backfills"),
        ("GET", f"/api/organisations/{_ORG}/overview"),
        ("GET", f"/api/organisations/{_ORG}/catalog"),
        ("POST", f"/api/organisations/{_ORG}/catalog/facebook_ads/check"),
        ("POST", f"/api/organisations/{_ORG}/catalog/facebook_ads/resolve"),
        ("GET", f"/api/organisations/{_ORG}/members"),
        ("PATCH", f"/api/organisations/{_ORG}"),
        ("GET", "/api/organisations"),
        ("GET", "/api/auth/me"),
        ("GET", "/api/auth/tokens"),
        ("POST", "/api/oauth/providers/amazon"),
    ],
)
def test_the_reorganised_routes_are_mounted(fake_settings: ApiSettings, method: str, path: str, oauth_store: Store):
    app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

    # 401 (not 404): the route is mounted and the auth guard answers first.
    assert TestClient(app).request(method, path, json={}).status_code == 401


@pytest.mark.parametrize(
    "path",
    [
        "/api/runs",
        "/api/components",
        "/api/catalog",
        "/api/overview",
        "/api/agent/conversations",
        "/api/tokens",
        "/api/oauth/amazon",
    ],
)
def test_organisation_routes_are_gone_from_the_top_level(fake_settings: ApiSettings, path: str, oauth_store: Store):
    app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

    assert TestClient(app).get(path).status_code == 404


ACME_ROUTES = """\
from fastapi import APIRouter

from interloper_api.dependencies import EditorDep, ViewerDep

router = APIRouter()


@router.get("/scope")
def scope(scope: ViewerDep) -> dict:
    return {"organisation": str(scope.organisation.id), "role": scope.role}


@router.get("/open")
def open_route() -> dict:
    return {}


@router.post("/write")
def write(scope: EditorDep) -> dict:
    return {}
"""


class TestExtensionRoutes:
    """A router an installed package declares answers in the organisations its caller belongs to."""

    @pytest.fixture
    def app(self, install, load_extensions, fake_settings: ApiSettings, oauth_store: Store) -> FastAPI:
        install("acme_ext", "manifest_version: 1\nkey: acme\nroutes: acme_ext.routes:router\n", routes=ACME_ROUTES)
        install("broken_ext", "manifest_version: 1\nkey: broken\nroutes: broken_ext.routes:router\n")
        load_extensions()
        return create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

    @pytest.fixture
    def viewer(self, oauth_store: Store) -> tuple[Profile, Organisation]:
        profile = oauth_store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-viewer", email="viewer@example.com", name="Viewer"
        )
        organisation = oauth_store.organisations.create("Acme")
        oauth_store.members.add(organisation.id, profile.id, "viewer")
        return profile, organisation

    def test_a_member_reaches_the_routes_under_the_organisation_and_extension_key(self, app: FastAPI, viewer):
        profile, organisation = viewer
        app.dependency_overrides[get_caller] = lambda: profile

        response = TestClient(app).get(f"/api/organisations/{organisation.id}/ext/acme/scope")

        assert response.status_code == 200
        assert response.json() == {"organisation": str(organisation.id), "role": "viewer"}

    def test_every_route_requires_membership_even_one_declaring_no_role(self, app: FastAPI, viewer, oauth_store):
        profile, _ = viewer
        other = oauth_store.organisations.create("Other")
        app.dependency_overrides[get_caller] = lambda: profile
        client = TestClient(app)

        assert client.get(f"/api/organisations/{other.id}/ext/acme/open").status_code == 404
        assert client.get(f"/api/organisations/{other.id}/ext/acme/scope").status_code == 404

    def test_a_route_raising_the_floor_refuses_a_viewer(self, app: FastAPI, viewer):
        profile, organisation = viewer
        app.dependency_overrides[get_caller] = lambda: profile

        assert TestClient(app).post(f"/api/organisations/{organisation.id}/ext/acme/write").status_code == 403

    def test_an_anonymous_caller_is_refused(self, app: FastAPI):
        assert TestClient(app).get(f"/api/organisations/{_ORG}/ext/acme/open").status_code == 401

    def test_a_router_that_fails_to_load_is_reported_while_the_others_serve(self, app: FastAPI, viewer):
        profile, organisation = viewer
        app.dependency_overrides[get_caller] = lambda: profile
        client = TestClient(app)

        assert client.get(f"/api/organisations/{organisation.id}/ext/acme/open").status_code == 200
        assert client.get(f"/api/organisations/{organisation.id}/ext/broken/open").status_code == 404
        assert {item["key"]: item["status"] for item in client.get("/api/extensions").json()} == {
            "acme": "ok",
            "broken": "failed",
        }
        broken = next(item for item in app.state.interloper.admin_config.extensions if item.key == "broken")
        (failure,) = broken.failures
        assert (failure.section, failure.entry) == ("routes", "broken")
        assert "No module named 'broken_ext.routes'" in failure.error


class TestErrorHandlers:
    """Domain errors become the status the UI can act on, never an opaque 500."""

    @staticmethod
    def _client(raise_error: Exception) -> TestClient:
        """Mount one route that raises, behind the app's error handlers.

        Args:
            raise_error: The exception the probe route raises.

        Returns:
            A client for the probe app.
        """
        app = FastAPI()
        install_error_handlers(app)

        @app.get("/probe")
        def probe() -> dict[str, str]:
            raise raise_error

        return TestClient(app, raise_server_exceptions=False)

    def test_a_missing_record_is_a_404(self) -> None:
        response = self._client(NotFoundError("Run 1 not found")).get("/probe")

        assert response.status_code == 404
        assert response.json() == {"detail": "Run 1 not found"}

    def test_catalog_drift_is_a_409(self) -> None:
        # A drifted component stays broken until the user resolves it, so it
        # is a conflict rather than a server fault.
        response = self._client(ComponentDriftError("source 'fb' drifted")).get("/probe")

        assert response.status_code == 409
        assert response.json() == {"detail": "source 'fb' drifted"}

    def test_an_unreadable_record_is_a_409(self) -> None:
        response = self._client(HydrationError("cannot decrypt payload")).get("/probe")

        assert response.status_code == 409
        assert response.json() == {"detail": "cannot decrypt payload"}

    @pytest.mark.parametrize("error", [ConfigError("bad sort"), CatalogKeyError("bad sort")])
    def test_a_rejected_value_is_a_400(self, error: Exception) -> None:
        response = self._client(error).get("/probe")

        assert response.status_code == 400
        assert response.json() == {"detail": "bad sort"}

    def test_a_state_conflict_is_a_409(self) -> None:
        response = self._client(ConflictError("Run 1 is already success")).get("/probe")

        assert response.status_code == 409
        assert response.json() == {"detail": "Run 1 is already success"}

    def test_a_refused_deletion_is_a_409_naming_its_referrers(self) -> None:
        referrers: list[dict[str, str | None]] = [{"id": "1", "kind": "job", "key": "daily", "name": "Daily"}]

        response = self._client(InUseError("in use by Daily", referrers=referrers)).get("/probe")

        assert response.status_code == 409
        assert response.json() == {"detail": {"message": "in use by Daily", "used_by": referrers}}

    def test_an_unmapped_value_error_stays_a_500(self) -> None:
        assert self._client(ValueError("a bug")).get("/probe").status_code == 500

    def test_a_quota_refusal_is_a_429_with_its_numbers(self) -> None:
        error = QuotaExceededError("Too many sources", quota="max_sources", limit=10, used=10)

        response = self._client(error).get("/probe")

        assert response.status_code == 429
        assert response.json() == {
            "detail": {"message": "Too many sources", "quota": "max_sources", "limit": 10, "used": 10}
        }


class TestCreateApp:
    """Wiring ``create_app`` does beyond mounting the routers."""

    def test_cors_origins_are_installed_when_given(self, fake_settings: ApiSettings, oauth_store: Store) -> None:
        app = create_app(
            store=oauth_store, catalog=Catalog(), settings=fake_settings, cors_origins=["http://localhost:3000"]
        )

        assert any("CORSMiddleware" in getattr(m.cls, "__name__", "") for m in app.user_middleware)

    def test_no_cors_middleware_without_origins(self, fake_settings: ApiSettings, oauth_store: Store) -> None:
        app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        assert not any("CORSMiddleware" in getattr(m.cls, "__name__", "") for m in app.user_middleware)

    def test_a_missing_agent_extra_is_reported_and_survived(
        self,
        fake_settings: ApiSettings,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        oauth_store: Store,
    ) -> None:
        # An API-only image is built without the extra on purpose. The
        # submodule is already imported, so the parent package's attribute has
        # to go too — `from X import Y` reads that before sys.modules.
        import interloper_api.routes as routes_package

        monkeypatch.delattr(routes_package, "agent", raising=False)
        monkeypatch.setitem(sys.modules, "interloper_api.routes.agent", None)

        with caplog.at_level("WARNING", logger="interloper_api.app"):
            app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        assert "the 'agent' extra is not installed" in caplog.text
        assert TestClient(app).post("/api/agent/conversations").status_code == 404

    def test_the_agent_feature_flag_follows_the_mount(self, fake_settings: ApiSettings, oauth_store: Store) -> None:
        app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        assert app.state.interloper.features == {"agent": True}

    def test_a_configured_login_keeps_its_session(self, fake_settings: ApiSettings, oauth_store: Store) -> None:
        app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        assert app.state.interloper.login is not None
        assert any("SessionMiddleware" in getattr(m.cls, "__name__", "") for m in app.user_middleware)

    def test_without_an_encryption_key_no_login_is_offered(
        self,
        fake_settings: ApiSettings,
        caplog: pytest.LogCaptureFixture,
        oauth_store: Store,
    ) -> None:
        fake_settings.encryption.key = ""

        with caplog.at_level("WARNING", logger="interloper_api.app"):
            app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        assert "Sign-in unavailable: INTERLOPER_ENCRYPTION_KEY is not configured" in caplog.text
        assert not any("SessionMiddleware" in getattr(m.cls, "__name__", "") for m in app.user_middleware)
        assert TestClient(app).get("/api/auth/google/login", follow_redirects=False).status_code == 404

    def test_the_store_catalog_and_settings_are_installed(self, fake_settings: ApiSettings, oauth_store: Store) -> None:
        catalog = Catalog(components={})

        app = create_app(store=oauth_store, catalog=catalog, settings=fake_settings)

        assert app.state.interloper.store is oauth_store
        assert app.state.interloper.catalog is catalog
        assert app.state.interloper.settings is fake_settings


class TestStartup:
    """What the application does once, when it starts serving."""

    @pytest.fixture
    def listener(self, monkeypatch: pytest.MonkeyPatch) -> list[FastAPI]:
        """Replace the realtime listener with one recording the applications it starts for.

        Args:
            monkeypatch: Fixture used to swap the listener.

        Returns:
            The applications the listener was started for, in order.
        """
        started: list[FastAPI] = []

        @asynccontextmanager
        async def realtime_lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
            started.append(app)
            yield

        monkeypatch.setattr(websocket_module, "realtime_lifespan", realtime_lifespan)
        return started

    async def test_the_configured_super_admins_are_promoted(
        self,
        fake_settings: ApiSettings,
        oauth_store: Store,
        listener: list[FastAPI],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        AgentSettings.get().enabled = False
        listed = oauth_store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-boss", email="boss@example.com", name="Boss"
        )
        unlisted = oauth_store.profiles.upsert(
            issuer="https://accounts.google.com", subject="g-other", email="other@example.com", name="Other"
        )
        app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        with caplog.at_level("INFO", logger="interloper_api.app"):
            async with app.router.lifespan_context(app):
                assert oauth_store.profiles.get(listed.id).is_super_admin is True

        assert oauth_store.profiles.get(unlisted.id).is_super_admin is False
        assert f"Promoted profile {listed.id} to super-admin." in caplog.text
        assert "boss@example.com" not in caplog.text
        assert listener == [app]

    async def test_the_listener_starts_with_the_app(
        self,
        fake_settings: ApiSettings,
        listener: list[FastAPI],
        oauth_store: Store,
    ) -> None:
        AgentSettings.get().enabled = False
        app = create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)

        async with app.router.lifespan_context(app):
            pass

        assert listener == [app]
