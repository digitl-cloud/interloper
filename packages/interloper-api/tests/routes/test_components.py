"""Tests for ``interloper_api.routes.components`` type-level operations (resolve + check)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import interloper as il
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from interloper.errors import (
    CatalogKeyError,
    ComponentDriftError,
    ConfigError,
    DataNotFoundError,
    HydrationError,
    InUseError,
    NotFoundError,
)
from interloper_assets.facebook_ads import connection as fb_connection
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.source import FacebookAds
from interloper_db import Component, ComponentStatus, Store

from interloper_api import app as app_module
from interloper_api.dependencies import (
    get_catalog,
    get_current_user,
    get_org_id,
    get_store,
    require_editor,
    require_viewer,
)
from interloper_api.routes import components as components_module

CONNECTION_CONFIG = {"access_token": "TOK", "app_id": "A", "app_secret": "S", "_id": "x"}


class UncheckableConnection(il.Connection):
    """A connection with no ``check()`` hook — module-level so its path imports."""

    api_key: str = il.SecretField()


def _client(catalog: il.Catalog) -> TestClient:
    app = FastAPI()
    app.include_router(components_module.router)
    app.dependency_overrides[require_viewer] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: catalog
    return TestClient(app)


@pytest.fixture
def source_catalog() -> il.Catalog:
    return il.Catalog.from_assets([FacebookAds])


@pytest.fixture
def connection_catalog() -> il.Catalog:
    return il.Catalog(components={FacebookAdsConnection.key: FacebookAdsConnection.definition()})


@pytest.fixture
def mock_graph(monkeypatch: pytest.MonkeyPatch):
    """Patch the Facebook connection's httpx client with a mock transport.

    Returns:
        The list the transport records each handled request into.
    """

    def install(handler) -> None:
        real_client = httpx.AsyncClient

        def factory(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return real_client(*args, **kwargs)

        monkeypatch.setattr(fb_connection.httpx, "AsyncClient", factory)

    return install


class TestResolve:
    def test_resolves_provider_options(self, source_catalog: il.Catalog, mock_graph):
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"account_id": "111", "name": "Acme", "account_status": 1},
                        {"account_id": "222", "name": "Paused", "account_status": 2},  # filtered out
                    ]
                },
            )

        mock_graph(handler)

        resp = _client(source_catalog).post(
            "/components/resolve",
            json={
                "component_key": "facebook_ads",
                "field": "account_id",
                # Credentials carry an internal _id marker that must be stripped.
                "deps": {"connection": CONNECTION_CONFIG},
            },
        )

        assert resp.status_code == 200
        assert resp.json() == [{"account_id": "111", "name": "Acme"}]
        # The connection's access token reached the Graph call; _id was not sent as a field.
        assert captured[0].url.params["access_token"] == "TOK"

    def test_unknown_component_404(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "nope", "field": "account_id", "deps": {}},
        )
        assert resp.status_code == 404

    def test_non_provider_field_400(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "facebook_ads", "field": "dataset", "deps": {}},
        )
        assert resp.status_code == 400


def _check(catalog: il.Catalog, config: dict) -> httpx.Response:
    return _client(catalog).post(
        "/components/check",
        json={"component_key": "facebook_ads_connection", "config": config},
    )


class TestCheck:
    def test_live_check_passes(self, connection_catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx.Response(200, json={"data": []}))

        resp = _check(connection_catalog, CONNECTION_CONFIG)

        assert resp.status_code == 200
        body = resp.json()
        assert (body["ok"], body["live"]) == (True, True)

    def test_rejected_credentials_reported_as_auth(self, connection_catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx.Response(401, json={"error": "bad token"}))

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "auth")

    def test_unreachable_provider_reported_as_network(self, connection_catalog: il.Catalog, mock_graph):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route to host")

        mock_graph(handler)

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "network")

    def test_invalid_config_reports_field_errors(self, connection_catalog: il.Catalog):
        # Static tier: a missing required field never reaches the provider.
        body = _check(connection_catalog, {"app_id": "A", "app_secret": "S"}).json()

        assert (body["ok"], body["live"], body["category"]) == (False, False, "config")
        assert [e["field"] for e in body["errors"]] == ["access_token"]

    def test_uncheckable_connection_is_static_only(self):
        catalog = il.Catalog(components={UncheckableConnection.key: UncheckableConnection.definition()})
        resp = _client(catalog).post(
            "/components/check",
            json={"component_key": "uncheckable_connection", "config": {"api_key": "k"}},
        )

        body = resp.json()
        assert (body["ok"], body["live"]) == (True, False)

    def test_unknown_component_404(self, connection_catalog: il.Catalog):
        resp = _client(connection_catalog).post("/components/check", json={"component_key": "nope", "config": {}})
        assert resp.status_code == 404

    def test_non_connection_component_404(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/components/check", json={"component_key": "facebook_ads", "config": {}}
        )
        assert resp.status_code == 404


class TestDelete:
    """DELETE /components/{id} maps store errors to HTTP statuses."""

    def test_in_use_maps_to_409_with_referrers(self):
        org_id = uuid4()
        referrers: list[dict[str, str | None]] = [
            {"id": str(uuid4()), "kind": "source", "key": "facebook_ads", "name": "FB"}
        ]

        def _delete(component_id):
            raise InUseError("Cannot delete connection 'C': in use by FB", referrers=referrers)

        class FakeStore:
            def __init__(self):
                self.organisations = SimpleNamespace(member_role=lambda user_id, org_id: "admin")
                self.components = SimpleNamespace(
                    get=lambda component_id: SimpleNamespace(id=component_id, org_id=org_id),
                    delete=_delete,
                )

        app = FastAPI()
        app.include_router(components_module.router)
        app.dependency_overrides[get_store] = lambda: FakeStore()
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4(), is_super_admin=False)

        resp = TestClient(app).delete(f"/components/{uuid4()}")

        assert resp.status_code == 409
        assert resp.json()["detail"]["used_by"] == referrers


class TestPublicConfigDisclosure:
    """Secret kinds disclose only the schema's x-public subset outside detail responses."""

    @staticmethod
    def _row(kind: str, config: dict | None = None) -> Component:
        return cast(Component, SimpleNamespace(
            id=uuid4(),
            org_id=uuid4(),
            kind=kind,
            key="k",
            name=None,
            config=config,
            state=None,
            encrypted=kind == "connection",
            parent_id=None,
            out_relations=[],
            children=[],
            created_at=None,
            updated_at=None,
        ))

    @staticmethod
    def _store(decoded: dict, public: dict) -> Store:
        components = SimpleNamespace(
            status=lambda row, parent_key=None: "ok",
            decode_config=lambda row: decoded,
            public_config=lambda row: public,
        )
        return cast(Store, SimpleNamespace(components=components))

    def test_list_response_carries_the_public_subset(self):
        store = self._store(decoded={"api_key": "SECRET", "auto_renew": False}, public={"auto_renew": False})
        response = components_module.ComponentResponse.from_row(
            self._row("connection"), store, include_config=False
        )
        assert response.config == {"auto_renew": False}

    def test_detail_response_carries_the_full_decode(self):
        store = self._store(decoded={"api_key": "SECRET", "auto_renew": False}, public={"auto_renew": False})
        response = components_module.ComponentResponse.from_row(
            self._row("connection"), store, include_config=True
        )
        assert response.config == {"api_key": "SECRET", "auto_renew": False}

    def test_non_secret_kinds_pass_their_config_through(self):
        response = components_module.ComponentResponse.from_row(
            self._row("job", config={"enabled": True}), self._store({}, {}), include_config=False
        )
        assert response.config == {"enabled": True}


class TestUnreadablePayload:
    """An unreadable payload is reported as a state, not raised at the caller."""

    @staticmethod
    def _row() -> Component:
        return cast(Component, SimpleNamespace(
            id=uuid4(),
            org_id=uuid4(),
            kind="connection",
            key="k",
            name=None,
            config=None,
            state=None,
            encrypted=True,
            parent_id=None,
            out_relations=[],
            children=[],
            created_at=None,
            updated_at=None,
        ))

    @staticmethod
    def _store() -> Store:
        """A store whose cipher rejects this row, the way `status` reports it.

        Returns:
            The store stand-in: ``unreadable`` status, and both decode paths
            raising the way the real ones do (nothing should call them).
        """
        def _raise(row: Component) -> dict:
            raise HydrationError(f"Failed to decrypt component {row.id}: InvalidToken")

        components = SimpleNamespace(
            status=lambda row, parent_key=None: ComponentStatus.UNREADABLE,
            decode_config=_raise,
            public_config=_raise,
        )
        return cast(Store, SimpleNamespace(components=components))

    def test_list_response_discloses_nothing(self):
        response = components_module.ComponentResponse.from_row(
            self._row(), self._store(), include_config=False
        )
        assert response.status is ComponentStatus.UNREADABLE
        assert response.config is None

    def test_detail_response_reports_the_state_instead_of_failing(self):
        response = components_module.ComponentResponse.from_row(
            self._row(), self._store(), include_config=True
        )
        assert response.status is ComponentStatus.UNREADABLE
        assert response.config is None

    def test_the_app_handler_renders_a_hydration_failure_as_a_conflict(self):
        """Paths that cannot degrade (hydration for a run) still surface the reason."""
        app = FastAPI()

        @app.get("/boom")
        async def _boom() -> None:
            raise HydrationError("Connection 'criteo' (abc) cannot be hydrated: its stored config does not decrypt")

        @app.exception_handler(HydrationError)  # mirrors create_app's handler
        async def _hydration_handler(_request, exc: HydrationError):
            return await app_module._hydration_failed(_request, exc)

        resp = TestClient(app, raise_server_exceptions=False).get("/boom")

        assert resp.status_code == 409
        assert "does not decrypt" in resp.json()["detail"]


# -- Component CRUD ------------------------------------------------------------


_ORG_ID = uuid4()
_USER_ID = uuid4()


def _row(
    component_id: UUID | None = None,
    *,
    kind: str = "job",
    key: str = "k",
    org_id: UUID = _ORG_ID,
    config: dict | None = None,
    relations: list[Any] | None = None,
    children: list[Any] | None = None,
) -> Any:
    return SimpleNamespace(
        id=component_id or uuid4(),
        org_id=org_id,
        kind=kind,
        key=key,
        name=None,
        config=config,
        state=None,
        encrypted=False,
        parent_id=None,
        out_relations=relations or [],
        children=children or [],
        created_at=None,
        updated_at=None,
    )


class CrudStore:
    """Fake store covering the component and relation facets the CRUD routes use."""

    def __init__(self) -> None:
        """Set up the recorders and the default happy-path behaviour."""
        self.created: list[dict[str, Any]] = []
        self.updated: list[dict[str, Any]] = []
        self.deleted: list[UUID] = []
        self.added_relations: list[dict[str, Any]] = []
        self.removed_relations: list[dict[str, Any]] = []
        self.listed: list[dict[str, Any]] = []
        #: Raised by whichever store call the test is exercising.
        self.error: Exception | None = None
        self.rows: list[Any] = []
        self.relation_rows: list[Any] = []
        self.role: str | None = "admin"
        self.get_org_id = _ORG_ID
        self.loaded: Any = None
        self.load_error: Exception | None = None

        self.organisations = SimpleNamespace(member_role=lambda user_id, org_id: self.role)
        self.components = SimpleNamespace(
            list_all=self._list_all,
            create=self._create,
            get=self._get,
            update=self._update,
            delete=self._delete,
            load=self._load,
            status=lambda row, parent_key=None: ComponentStatus.OK,
            decode_config=lambda row: row.config or {},
            public_config=lambda row: {},
        )
        self.relations = SimpleNamespace(
            list_all=self._list_relations,
            add=self._add_relation,
            remove=self._remove_relation,
        )

    def _list_all(self, org_id: UUID, kinds: list[str] | None = None) -> list[Any]:
        self.listed.append({"org_id": org_id, "kinds": kinds})
        return self.rows

    def _create(self, org_id: UUID, **kwargs: Any) -> Any:
        if self.error:
            raise self.error
        self.created.append({"org_id": org_id, **kwargs})
        return _row(kind=kwargs["kind"], key=kwargs["key"], config=kwargs.get("config"))

    def _get(self, component_id: UUID) -> Any:
        return _row(component_id, org_id=self.get_org_id)

    def _update(self, component_id: UUID, **kwargs: Any) -> Any:
        if self.error:
            raise self.error
        self.updated.append({"id": component_id, **kwargs})
        return _row(component_id)

    def _delete(self, component_id: UUID) -> None:
        if self.error:
            raise self.error
        self.deleted.append(component_id)

    def _load(self, component_id: UUID) -> Any:
        if self.load_error:
            raise self.load_error
        return self.loaded

    def _list_relations(self, org_id: UUID, type: str | None = None) -> list[Any]:
        return [r for r in self.relation_rows if type is None or r.type == type]

    def _add_relation(self, src_id: UUID, *, type: str, dst_id: UUID, slot: str) -> Any:
        if self.error:
            raise self.error
        self.added_relations.append({"src_id": src_id, "type": type, "dst_id": dst_id, "slot": slot})
        return SimpleNamespace(src_id=src_id, dst_id=dst_id, type=type, slot=slot, dst_kind="connection")

    def _remove_relation(self, src_id: UUID, *, type: str, dst_id: UUID) -> None:
        if self.error:
            raise self.error
        self.removed_relations.append({"src_id": src_id, "type": type, "dst_id": dst_id})


@pytest.fixture
def crud_store() -> CrudStore:
    """A fresh CRUD fake store.

    Returns:
        The fake store.
    """
    return CrudStore()


@pytest.fixture
def crud_client(crud_store: CrudStore) -> TestClient:
    """Mount the components router with every role gate satisfied.

    Args:
        crud_store: The fake store the routes resolve against.

    Returns:
        A client for the probe app.
    """
    user = SimpleNamespace(id=_USER_ID, email="ada@example.com", is_super_admin=False)
    app = FastAPI()
    app.include_router(components_module.router)
    app.dependency_overrides[get_store] = lambda: crud_store
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[require_viewer] = lambda: user
    app.dependency_overrides[require_editor] = lambda: user
    return TestClient(app)


class TestListComponents:
    """``GET /components/`` — org-scoped, secrets withheld."""

    def test_lists_every_kind_by_default(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.rows = [_row(kind="job", key="nightly"), _row(kind="source", key="fb")]

        response = crud_client.get("/components/")

        assert [row["key"] for row in response.json()] == ["nightly", "fb"]
        assert crud_store.listed == [{"org_id": _ORG_ID, "kinds": None}]

    def test_the_kind_filter_is_forwarded(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_client.get("/components/?kind=source&kind=job")

        assert crud_store.listed == [{"org_id": _ORG_ID, "kinds": ["source", "job"]}]

    def test_no_components_is_an_empty_list(self, crud_client: TestClient) -> None:
        assert crud_client.get("/components/").json() == []


class TestListRelations:
    """``GET /components/relations`` — optionally narrowed by type."""

    def test_lists_every_relation(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        source_id, destination_id = uuid4(), uuid4()
        crud_store.relation_rows = [
            SimpleNamespace(
                src_id=source_id, dst_id=destination_id, type="resource", slot="connection", dst_kind="connection"
            )
        ]

        response = crud_client.get("/components/relations")

        assert response.json() == [
            {
                "src_id": str(source_id),
                "dst_id": str(destination_id),
                "type": "resource",
                "slot": "connection",
                "dst_kind": "connection",
            }
        ]

    def test_the_type_filter_narrows_the_result(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.relation_rows = [
            SimpleNamespace(src_id=uuid4(), dst_id=uuid4(), type="resource", slot="a", dst_kind="connection"),
            SimpleNamespace(src_id=uuid4(), dst_id=uuid4(), type="destination", slot="b", dst_kind="destination"),
        ]

        response = crud_client.get("/components/relations?type=destination")

        assert [r["type"] for r in response.json()] == ["destination"]


class TestCreateComponent:
    """``POST /components/`` — store errors become the right status."""

    def test_creates_and_returns_the_component(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        body = {"kind": "job", "key": "nightly", "config": {"cron": "0 2 * * *"}}

        response = crud_client.post("/components/", json=body)

        assert response.status_code == 201
        assert response.json()["key"] == "nightly"
        assert crud_store.created[0]["kind"] == "job"
        assert crud_store.created[0]["config"] == {"cron": "0 2 * * *"}

    def test_relations_are_flattened_into_store_bindings(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        destination_id = uuid4()
        body = {
            "kind": "source",
            "key": "fb",
            "relations": {"resource": [{"dst_id": str(destination_id), "slot": "connection"}]},
        }

        crud_client.post("/components/", json=body)

        assert crud_store.created[0]["relations"] == {"resource": [(destination_id, "connection")]}

    def test_omitted_relations_stay_none(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        # None means "leave every relation type untouched", which is not the
        # same as an empty map.
        crud_client.post("/components/", json={"kind": "job", "key": "nightly"})

        assert crud_store.created[0]["relations"] is None

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (ConfigError("bad config"), 400),
            (CatalogKeyError("unknown key"), 400),
            (NotFoundError("relation target gone"), 404),
        ],
    )
    def test_store_errors_map_to_statuses(
        self, crud_client: TestClient, crud_store: CrudStore, error: Exception, expected: int
    ) -> None:
        crud_store.error = error

        response = crud_client.post("/components/", json={"kind": "job", "key": "nightly"})

        assert response.status_code == expected


class TestGetComponent:
    """``GET /components/{id}`` — detail responses decode the config."""

    def test_returns_the_component_with_its_config(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        component_id = uuid4()

        response = crud_client.get(f"/components/{component_id}")

        assert response.status_code == 200
        assert response.json()["id"] == str(component_id)

    def test_a_component_of_another_org_is_a_404(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        crud_store.role = None

        response = crud_client.get(f"/components/{uuid4()}")

        assert response.status_code == 404


class TestUpdateComponent:
    """``PUT /components/{id}`` — omitted facets are untouched."""

    def test_updates_the_named_facets(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        component_id = uuid4()

        response = crud_client.put(f"/components/{component_id}", json={"name": "Renamed"})

        assert response.status_code == 200
        assert crud_store.updated[0]["id"] == component_id
        assert crud_store.updated[0]["name"] == "Renamed"
        assert crud_store.updated[0]["config"] is None

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (ConfigError("bad config"), 400),
            (CatalogKeyError("unknown key"), 400),
            (NotFoundError("gone"), 404),
        ],
    )
    def test_store_errors_map_to_statuses(
        self, crud_client: TestClient, crud_store: CrudStore, error: Exception, expected: int
    ) -> None:
        crud_store.error = error

        response = crud_client.put(f"/components/{uuid4()}", json={"name": "Renamed"})

        assert response.status_code == expected

    def test_breaking_a_depended_on_binding_is_a_409_with_referrers(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        referrers: list[dict[str, str | None]] = [
            {"id": str(uuid4()), "kind": "source", "key": "fb", "name": "FB"}
        ]
        crud_store.error = InUseError("still bound", referrers=referrers)

        response = crud_client.put(f"/components/{uuid4()}", json={"config": {}})

        assert response.status_code == 409
        assert response.json()["detail"]["used_by"] == referrers

    def test_editing_requires_the_editor_role(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        crud_store.role = "viewer"

        response = crud_client.put(f"/components/{uuid4()}", json={"name": "Renamed"})

        assert response.status_code == 403


class TestDeleteComponentStatuses:
    """``DELETE /components/{id}`` — the rest of the error mapping."""

    def test_a_successful_delete_acknowledges(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        component_id = uuid4()

        response = crud_client.delete(f"/components/{component_id}")

        assert response.json() == {"status": "deleted"}
        assert crud_store.deleted == [component_id]

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (NotFoundError("already gone"), 404),
            (ValueError("store refused"), 400),
        ],
    )
    def test_store_errors_map_to_statuses(
        self, crud_client: TestClient, crud_store: CrudStore, error: Exception, expected: int
    ) -> None:
        crud_store.error = error

        assert crud_client.delete(f"/components/{uuid4()}").status_code == expected


class TestAddRelation:
    """``POST /components/{id}/relations`` — both ends must be in the caller's org."""

    def test_adds_the_relation(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        source_id, destination_id = uuid4(), uuid4()
        body = {"type": "resource", "dst_id": str(destination_id), "slot": "connection"}

        response = crud_client.post(f"/components/{source_id}/relations", json=body)

        assert response.status_code == 201
        assert response.json()["dst_kind"] == "connection"
        assert crud_store.added_relations == [
            {"src_id": source_id, "type": "resource", "dst_id": destination_id, "slot": "connection"}
        ]

    def test_a_target_in_another_org_is_a_404(
        self, crud_client: TestClient, crud_store: CrudStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Both loads authorize, but the two rows must belong to one org —
        # otherwise a relation could straddle organisations.
        source_id, destination_id = uuid4(), uuid4()
        other_org = uuid4()

        def get(component_id: UUID) -> Any:
            return _row(component_id, org_id=other_org if component_id == destination_id else _ORG_ID)

        crud_store.components.get = get

        response = crud_client.post(
            f"/components/{source_id}/relations",
            json={"type": "resource", "dst_id": str(destination_id), "slot": "connection"},
        )

        assert response.status_code == 404
        assert crud_store.added_relations == []

    @pytest.mark.parametrize(
        ("error", "expected"),
        [(ConfigError("not allowed on this kind"), 400), (NotFoundError("gone"), 404)],
    )
    def test_store_errors_map_to_statuses(
        self, crud_client: TestClient, crud_store: CrudStore, error: Exception, expected: int
    ) -> None:
        crud_store.error = error

        response = crud_client.post(
            f"/components/{uuid4()}/relations",
            json={"type": "resource", "dst_id": str(uuid4()), "slot": "connection"},
        )

        assert response.status_code == expected


class TestRemoveRelation:
    """``DELETE /components/{id}/relations/{type}/{dst_id}``."""

    def test_removes_the_relation(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        source_id, destination_id = uuid4(), uuid4()

        response = crud_client.delete(f"/components/{source_id}/relations/resource/{destination_id}")

        assert response.status_code == 204
        assert crud_store.removed_relations == [
            {"src_id": source_id, "type": "resource", "dst_id": destination_id}
        ]

    def test_a_required_slot_is_refused(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        # Required dependency slots are repointed, never emptied.
        crud_store.error = ConfigError("slot 'connection' is required")

        response = crud_client.delete(f"/components/{uuid4()}/relations/resource/{uuid4()}")

        assert response.status_code == 400


class TestPartitionRowCounts:
    """``GET /components/{id}/partition-row-counts`` — asset data, not catalog metadata."""

    def test_returns_counts_ordered_by_partition(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        class Daily(il.Asset):
            """Partitioned asset whose destination reports two partitions."""

            partitioning = il.TimePartitionConfig(column="date")

            def data(self) -> Any:
                return []

            def partition_row_counts(self) -> dict[str, int]:
                """Counts in deliberately unsorted order.

                Returns:
                    Two partitions, newest first.
                """
                return {"2026-06-02": 5, "2026-06-01": 3}

        crud_store.loaded = Daily()

        response = crud_client.get(f"/components/{uuid4()}/partition-row-counts")

        assert response.status_code == 200
        assert response.json() == {
            "asset_key": "daily",
            "partition_column": "date",
            "counts": [
                {"partition": "2026-06-01", "row_count": 3},
                {"partition": "2026-06-02", "row_count": 5},
            ],
        }

    @pytest.mark.parametrize("error", [NotFoundError("gone"), ComponentDriftError("drifted")])
    def test_an_unloadable_asset_is_a_404(
        self, crud_client: TestClient, crud_store: CrudStore, error: Exception
    ) -> None:
        crud_store.load_error = error

        response = crud_client.get(f"/components/{uuid4()}/partition-row-counts")

        assert response.status_code == 404

    def test_an_unpartitioned_asset_is_a_400(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        crud_store.loaded = SimpleNamespace(partitioning=None)

        response = crud_client.get(f"/components/{uuid4()}/partition-row-counts")

        assert response.status_code == 400
        assert response.json()["detail"] == "Component is not a partitioned asset"

    def test_a_destination_that_cannot_count_is_a_400(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        def unsupported() -> dict[str, int]:
            raise NotImplementedError

        crud_store.loaded = SimpleNamespace(
            partitioning=SimpleNamespace(column="date"), partition_row_counts=unsupported
        )

        response = crud_client.get(f"/components/{uuid4()}/partition-row-counts")

        assert response.status_code == 400
        assert response.json()["detail"] == "Destination does not support partition row counts"

    def test_an_empty_destination_is_a_404(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        def empty() -> dict[str, int]:
            raise DataNotFoundError("no data for asset 'daily'")

        crud_store.loaded = SimpleNamespace(
            partitioning=SimpleNamespace(column="date"), partition_row_counts=empty
        )

        response = crud_client.get(f"/components/{uuid4()}/partition-row-counts")

        assert response.status_code == 404

    def test_any_other_destination_failure_is_a_500_without_a_traceback(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        def broken() -> dict[str, int]:
            raise RuntimeError("warehouse unreachable")

        crud_store.loaded = SimpleNamespace(
            partitioning=SimpleNamespace(column="date"), partition_row_counts=broken
        )

        response = crud_client.get(f"/components/{uuid4()}/partition-row-counts")

        assert response.status_code == 500
        assert response.json()["detail"] == "warehouse unreachable"


class TestRelationGrouping:
    """``_relations_of`` and ``_bindings`` — the two shape converters."""

    def test_outgoing_relations_group_by_type(self) -> None:
        first, second = uuid4(), uuid4()
        row = _row(
            relations=[
                SimpleNamespace(type="resource", dst_id=first, slot="connection", dst_kind="connection"),
                SimpleNamespace(type="resource", dst_id=second, slot="other", dst_kind="connection"),
                SimpleNamespace(type="destination", dst_id=first, slot="", dst_kind="destination"),
            ]
        )

        grouped = components_module._relations_of(row)

        assert set(grouped) == {"resource", "destination"}
        assert [ref.dst_id for ref in grouped["resource"]] == [first, second]

    def test_no_relations_is_an_empty_map(self) -> None:
        assert components_module._relations_of(_row()) == {}

    def test_bindings_flatten_entries_to_tuples(self) -> None:
        destination_id = uuid4()
        entries = {"resource": [components_module.RelationEntry(dst_id=destination_id, slot="connection")]}

        assert components_module._bindings(entries) == {"resource": [(destination_id, "connection")]}

    def test_bindings_of_none_stay_none(self) -> None:
        # None means "leave every relation type untouched".
        assert components_module._bindings(None) is None


class TestHandleError:
    """``handle_error`` maps a provider failure to a status, never a traceback."""

    @staticmethod
    def _status_error(status: int) -> httpx.HTTPStatusError:
        request = httpx.Request("GET", "https://provider.example.com/x")
        return httpx.HTTPStatusError(
            "boom", request=request, response=httpx.Response(status, request=request)
        )

    @pytest.mark.parametrize("status", [401, 403])
    def test_an_auth_failure_keeps_its_status(self, status: int) -> None:
        with pytest.raises(HTTPException) as excinfo:
            components_module.handle_error(self._status_error(status), "resolving facebook.ads_stats")

        assert excinfo.value.status_code == status
        assert "Authorization failed while resolving facebook.ads_stats." == excinfo.value.detail

    def test_a_provider_404_stays_a_404(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            components_module.handle_error(self._status_error(404), "resolving x")

        assert excinfo.value.status_code == 404
        assert "Resource not found while resolving x." == excinfo.value.detail

    def test_another_provider_status_falls_through_to_500(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            components_module.handle_error(self._status_error(503), "resolving x")

        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Failed resolving x."

    def test_an_http_exception_is_re_raised_as_is(self) -> None:
        original = HTTPException(status_code=409, detail="conflict")

        with pytest.raises(HTTPException) as excinfo:
            components_module.handle_error(original, "resolving x")

        assert excinfo.value is original

    def test_anything_else_becomes_a_500(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            components_module.handle_error(RuntimeError("kaboom"), "resolving x")

        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Failed resolving x."


class TestCheckResponseFromFailure:
    """A failed connection check is a categorised result, never a raised error."""

    @staticmethod
    def _status_error(status: int) -> httpx.HTTPStatusError:
        request = httpx.Request("GET", "https://provider.example.com/x")
        return httpx.HTTPStatusError(
            "boom", request=request, response=httpx.Response(status, request=request)
        )

    def test_a_connection_check_error_carries_its_own_message(self) -> None:
        from interloper.errors import ConnectionCheckError

        response = components_module.CheckResponse.from_failure(
            ConnectionCheckError("missing httpx extra"), "facebook_ads"
        )

        assert (response.ok, response.live, response.category) == (False, True, "error")
        assert response.message == "missing httpx extra"

    @pytest.mark.parametrize("status", [401, 403])
    def test_a_rejected_credential_is_categorised_as_auth(self, status: int) -> None:
        response = components_module.CheckResponse.from_failure(self._status_error(status), "fb")

        assert response.category == "auth"
        assert response.message == "The provider rejected the credentials."

    def test_another_provider_status_is_reported_verbatim(self) -> None:
        response = components_module.CheckResponse.from_failure(self._status_error(503), "fb")

        assert response.category == "error"
        assert response.message == "The provider responded with HTTP 503."

    @pytest.mark.parametrize(
        "exception",
        [TimeoutError("slow"), httpx.TimeoutException("slow")],
    )
    def test_a_timeout_is_categorised_as_network(self, exception: Exception) -> None:
        response = components_module.CheckResponse.from_failure(exception, "fb")

        assert response.category == "network"
        assert response.message == "The provider did not respond in time."

    def test_an_unreachable_provider_is_categorised_as_network(self) -> None:
        response = components_module.CheckResponse.from_failure(httpx.ConnectError("no route"), "fb")

        assert response.category == "network"
        assert response.message == "The provider could not be reached."

    def test_anything_else_is_a_generic_error(self) -> None:
        # The raw exception text may carry credentials, so it is not echoed.
        response = components_module.CheckResponse.from_failure(RuntimeError("token=SECRET"), "fb")

        assert response.category == "error"
        assert response.message == "The connection check failed unexpectedly."
        assert "SECRET" not in response.message


class TestResolveEdgeCases:
    """``POST /components/resolve`` — the guards between the field and the provider."""

    def test_an_unknown_resource_slot_is_a_400(
        self, source_catalog: il.Catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The FetchField names a slot the component does not declare.
        from interloper_assets.facebook_ads.source import FacebookAds

        monkeypatch.setattr(FacebookAds, "resource_types", {})

        response = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "facebook_ads", "field": "account_id", "deps": {}},
        )

        assert response.status_code == 400
        assert "Resource slot" in response.json()["detail"]

    def test_a_provider_failure_is_mapped_not_raised(
        self, source_catalog: il.Catalog, mock_graph
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route to host")

        mock_graph(handler)

        response = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "facebook_ads", "field": "account_id", "deps": {"connection": CONNECTION_CONFIG}},
        )

        assert response.status_code == 500
        assert response.json()["detail"].startswith("Failed resolving facebook_ads.account_id")

    def test_a_slot_that_is_not_a_fetch_provider_is_a_403(
        self, source_catalog: il.Catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Validated at catalog build, so this is a defensive guard.
        monkeypatch.setattr(components_module, "is_fetch_field_provider", lambda fn: False)

        response = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "facebook_ads", "field": "account_id", "deps": {"connection": CONNECTION_CONFIG}},
        )

        assert response.status_code == 403
        assert "is not a fetch provider" in response.json()["detail"]


class TestCheckFalsyResult:
    """A check that returns falsy is a failure, not a pass."""

    def test_a_false_check_is_reported_as_an_error(
        self, connection_catalog: il.Catalog, mock_graph, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(FacebookAdsConnection, "check", lambda self: False)

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "error")
        assert body["message"] == "The connection check failed."
