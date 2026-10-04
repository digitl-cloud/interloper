"""Tests for ``interloper_api.routes.components``: component CRUD, relations and partition counts."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI
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
from interloper_db import Component, ComponentQuery, ComponentReading, ComponentStatus, DeleteImpact, Page, Store

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import (
    get_current_user,
    get_org_id,
    get_store,
    require_editor,
    require_viewer,
)
from interloper_api.routes import components as components_module


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
                self.members = SimpleNamespace(role=lambda org_id, user_id: "admin")
                self.components = SimpleNamespace(
                    get=lambda component_id: SimpleNamespace(id=component_id, org_id=org_id),
                    delete=_delete,
                )

        app = FastAPI()
        install_error_handlers(app)
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
            read=lambda row, parent_key=None: ComponentReading(
                status=ComponentStatus.OK,
                config=row.config if row.kind == "job" else decoded,
                public_config=public,
                discriminator=None,
            ),
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


class TestDiscriminatorDisclosure:
    """Every response names the instance by its discriminator, list and detail alike."""

    def test_response_carries_the_discriminator(self):
        row = TestPublicConfigDisclosure._row("source", config={"account_id": "act_1"})
        components = SimpleNamespace(
            read=lambda row, parent_key=None: ComponentReading(
                status=ComponentStatus.OK, config=row.config, public_config={}, discriminator="act_1"
            ),
        )
        store = cast(Store, SimpleNamespace(components=components))
        response = components_module.ComponentResponse.from_row(row, store, include_config=False)
        assert response.discriminator == "act_1"


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
        """A store whose cipher rejects this row, the way a reading reports it.

        Returns:
            The store stand-in: an ``unreadable`` reading disclosing no payload
            at all, which is what a row whose cipher fails yields.
        """
        components = SimpleNamespace(
            read=lambda row, parent_key=None: ComponentReading(
                status=ComponentStatus.UNREADABLE, config=None, public_config={}, discriminator=None
            ),
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
        install_error_handlers(app)

        @app.get("/boom")
        async def _boom() -> None:
            raise HydrationError("Connection 'criteo' (abc) cannot be hydrated: its stored config does not decrypt")

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


def _relation(
    name: str, dst_id: UUID, dst_kind: str, *, dst_key: str = "k", dst_name: str | None = None
) -> Any:
    return SimpleNamespace(
        name=name,
        dst_id=dst_id,
        dst_kind=dst_kind,
        dst=SimpleNamespace(id=dst_id, key=dst_key, name=dst_name),
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
        self.listed: list[tuple[UUID, ComponentQuery]] = []
        self.error: Exception | None = None
        self.rows: list[Any] = []
        self.role: str | None = "admin"
        self.get_org_id = _ORG_ID
        self.loaded: Any = None
        self.load_error: Exception | None = None
        self.impact_requested: list[list[UUID]] = []
        self.impact = DeleteImpact(blocking=[], detaching=[])

        self.members = SimpleNamespace(role=lambda org_id, user_id: self.role)
        self.components = SimpleNamespace(
            list=self._list,
            create=self._create,
            get=self._get,
            update=self._update,
            delete=self._delete,
            delete_impact=self._delete_impact,
            load=self._load,
            read=lambda row, parent_key=None: ComponentReading(
                status=ComponentStatus.OK, config=row.config or {}, public_config={}, discriminator=None
            ),
        )
        self.relations = SimpleNamespace(
            add=self._add_relation,
            delete=self._remove_relation,
        )

    def _list(self, org_id: UUID, query: ComponentQuery) -> Page[Any]:
        self.listed.append((org_id, query))
        return Page.window(self.rows, query)

    def _delete_impact(self, component_ids: list[UUID]) -> DeleteImpact:
        if self.error:
            raise self.error
        self.impact_requested.append(component_ids)
        return self.impact

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

    def _add_relation(self, src_id: UUID, *, name: str, dst_id: UUID) -> Any:
        if self.error:
            raise self.error
        self.added_relations.append({"src_id": src_id, "name": name, "dst_id": dst_id})
        return SimpleNamespace(src_id=src_id, name=name, dst_id=dst_id, src_kind="source", dst_kind="connection")

    def _remove_relation(self, src_id: UUID, *, name: str, dst_id: UUID) -> None:
        if self.error:
            raise self.error
        self.removed_relations.append({"src_id": src_id, "name": name, "dst_id": dst_id})


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
    install_error_handlers(app)
    app.include_router(components_module.router)
    app.dependency_overrides[get_store] = lambda: crud_store
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[require_viewer] = lambda: user
    app.dependency_overrides[require_editor] = lambda: user
    return TestClient(app)


class TestListComponents:
    """``GET /components``: org-scoped roots, secrets withheld."""

    def test_lists_every_kind_by_default(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.rows = [_row(kind="job", key="nightly"), _row(kind="source", key="fb")]

        response = crud_client.get("/components")

        assert response.status_code == 200
        body = response.json()
        assert [row["key"] for row in body["items"]] == ["nightly", "fb"]
        assert body["total"] == 2
        ((org_id, query),) = crud_store.listed
        assert org_id == _ORG_ID
        assert query == ComponentQuery()

    def test_the_kind_and_text_filters_are_forwarded(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_client.get("/components?kind=source&kind=job&q=face")

        ((_, query),) = crud_store.listed
        assert query.kind == ["source", "job"]
        assert query.q == "face"
        assert query.roots_only is True

    def test_the_page_window_is_forwarded(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.rows = [_row(key=f"k{i}") for i in range(5)]

        body = crud_client.get("/components", params={"limit": 2, "offset": 1}).json()

        ((_, query),) = crud_store.listed
        assert (query.limit, query.offset) == (2, 1)
        assert [row["key"] for row in body["items"]] == ["k1", "k2"]
        assert body["total"] == 5

    def test_a_page_larger_than_the_cap_is_a_422(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        assert crud_client.get("/components", params={"limit": 501}).status_code == 422
        assert crud_store.listed == []

    def test_no_components_is_an_empty_page(self, crud_client: TestClient) -> None:
        assert crud_client.get("/components").json() == {"items": [], "total": 0}

    def test_owned_components_ride_under_their_owner(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        asset = _row(kind="asset", key="ads")
        crud_store.rows = [_row(kind="source", key="fb", children=[asset])]

        [source] = crud_client.get("/components").json()["items"]

        assert [child["key"] for child in source["children"]] == ["ads"]

    def test_relation_refs_carry_their_target(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        connection_id = uuid4()
        crud_store.rows = [
            _row(
                kind="source",
                key="fb",
                relations=[
                    _relation("connection", connection_id, "connection", dst_key="facebook_ads", dst_name="FB")
                ],
            )
        ]

        [row] = crud_client.get("/components").json()["items"]

        assert row["relations"] == {
            "connection": [
                {
                    "dst_id": str(connection_id),
                    "dst_kind": "connection",
                    "dst_key": "facebook_ads",
                    "dst_name": "FB",
                }
            ]
        }


class TestDeleteImpact:
    """``GET /components/delete-impact`` — the preview behind the delete confirmation."""

    def test_returns_blocking_and_detaching_referrers(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        first, second = uuid4(), uuid4()
        referrer: dict[str, str | None] = {"id": str(uuid4()), "kind": "source", "key": "fb", "name": "FB"}
        crud_store.impact = DeleteImpact(blocking=[referrer], detaching=[])

        response = crud_client.get(f"/components/delete-impact?id={first}&id={second}")

        assert response.status_code == 200
        assert response.json() == {"blocking": [referrer], "detaching": []}
        assert crud_store.impact_requested == [[first, second]]

    def test_a_non_member_gets_404(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.role = None

        assert crud_client.get(f"/components/delete-impact?id={uuid4()}").status_code == 404

    def test_an_unknown_id_is_404(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        crud_store.error = NotFoundError("gone")

        assert crud_client.get(f"/components/delete-impact?id={uuid4()}").status_code == 404


class TestCreateComponent:
    """``POST /components``: store errors become the right status."""

    def test_creates_and_returns_the_component(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        body = {"kind": "job", "key": "nightly", "config": {"cron": "0 2 * * *"}}

        response = crud_client.post("/components", json=body)

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
            "relations": {"connection": [{"dst_id": str(destination_id)}]},
        }

        crud_client.post("/components", json=body)

        assert crud_store.created[0]["relations"] == {"connection": [destination_id]}

    def test_omitted_relations_stay_none(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        # None means "leave every relation type untouched", which is not the
        # same as an empty map.
        crud_client.post("/components", json={"kind": "job", "key": "nightly"})

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

        response = crud_client.post("/components", json={"kind": "job", "key": "nightly"})

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

    def test_a_successful_delete_is_an_empty_204(
        self, crud_client: TestClient, crud_store: CrudStore
    ) -> None:
        component_id = uuid4()

        response = crud_client.delete(f"/components/{component_id}")

        assert response.status_code == 204
        assert response.content == b""
        assert crud_store.deleted == [component_id]

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (NotFoundError("already gone"), 404),
            (ConfigError("Cannot delete a source-owned asset directly"), 400),
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
        body = {"name": "connection", "dst_id": str(destination_id)}

        response = crud_client.post(f"/components/{source_id}/relations", json=body)

        assert response.status_code == 201
        assert response.json()["dst_kind"] == "connection"
        assert crud_store.added_relations == [
            {"src_id": source_id, "name": "connection", "dst_id": destination_id}
        ]

    def test_add_relation_by_name(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        source_id, destination_id = uuid4(), uuid4()

        response = crud_client.post(
            f"/components/{source_id}/relations", json={"name": "destinations", "dst_id": str(destination_id)}
        )

        assert response.status_code == 201
        assert response.json()["name"] == "destinations"
        assert crud_store.added_relations == [
            {"src_id": source_id, "name": "destinations", "dst_id": destination_id}
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
            json={"name": "connection", "dst_id": str(destination_id)},
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
            json={"name": "connection", "dst_id": str(uuid4())},
        )

        assert response.status_code == expected


class TestRemoveRelation:
    """``DELETE /components/{id}/relations/{name}/{dst_id}``."""

    def test_removes_the_relation(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        source_id, destination_id = uuid4(), uuid4()

        response = crud_client.delete(f"/components/{source_id}/relations/connection/{destination_id}")

        assert response.status_code == 204
        assert crud_store.removed_relations == [
            {"src_id": source_id, "name": "connection", "dst_id": destination_id}
        ]

    def test_remove_relation_route_uses_name(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        source_id, destination_id = uuid4(), uuid4()

        response = crud_client.delete(f"/components/{source_id}/relations/destinations/{destination_id}")

        assert response.status_code == 204
        assert crud_store.removed_relations == [
            {"src_id": source_id, "name": "destinations", "dst_id": destination_id}
        ]

    def test_a_required_name_is_refused(self, crud_client: TestClient, crud_store: CrudStore) -> None:
        # Required dependency names are repointed, never emptied.
        crud_store.error = ConfigError("'connection' is required")

        response = crud_client.delete(f"/components/{uuid4()}/relations/connection/{uuid4()}")

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

    def test_outgoing_relations_group_by_name(self) -> None:
        first, second = uuid4(), uuid4()
        row = _row(
            relations=[
                _relation("connection", first, "connection"),
                _relation("connection", second, "connection"),
                _relation("destinations", first, "destination"),
            ]
        )

        grouped = components_module._relations_of(row)

        assert set(grouped) == {"connection", "destinations"}
        assert [ref.dst_id for ref in grouped["connection"]] == [first, second]

    def test_no_relations_is_an_empty_map(self) -> None:
        assert components_module._relations_of(_row()) == {}

    def test_bindings_flatten_entries_to_ids(self) -> None:
        destination_id = uuid4()
        entries = {"connection": [components_module.RelationEntry(dst_id=destination_id)]}

        assert components_module._bindings(entries) == {"connection": [destination_id]}

    def test_bindings_of_none_stay_none(self) -> None:
        # None means "leave every relation name untouched".
        assert components_module._bindings(None) is None


class TestComponentResponseRelations:
    """``ComponentResponse.relations``, keyed by name, dst_kind carried along."""

    def test_component_response_relations_keyed_by_name(self) -> None:
        connection_id, destination_id = uuid4(), uuid4()
        row = _row(
            relations=[
                _relation("connection", connection_id, "connection", dst_key="facebook_ads", dst_name="FB"),
                _relation("destinations", destination_id, "destination"),
            ]
        )
        store = cast(Store, SimpleNamespace(
            components=SimpleNamespace(
                read=lambda row, parent_key=None: ComponentReading(
                    status=ComponentStatus.OK, config=row.config, public_config={}, discriminator=None
                ),
            )
        ))

        response = components_module.ComponentResponse.from_row(row, store, include_config=False)

        assert set(response.relations) == {"connection", "destinations"}
        assert response.relations["connection"] == [
            components_module.RelationRef(
                dst_id=connection_id, dst_kind="connection", dst_key="facebook_ads", dst_name="FB"
            )
        ]
