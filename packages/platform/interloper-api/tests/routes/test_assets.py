"""Tests for the assets routes: an asset's rows, read and, for an editable asset, rewritten."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from interloper.errors import ConfigError
from interloper_db import ComponentDriftError
from interloper_db.models import Organisation, Profile, Token

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import CallerCredential, OrgScope, get_org_scope, get_store
from interloper_api.routes import assets as assets_module

_ORG_ID = uuid4()
_ROWS_URL = f"/assets/{uuid4()}/rows"


class _Unprintable:
    """A value the JSON conversion has no type for and cannot render as text either."""

    def __str__(self) -> str:
        """Refuse to render.

        Raises:
            ValueError: Always.
        """
        raise ValueError("no text for this value")


class Note(il.Schema):
    note_id: str
    text: str
    day: dt.date | None = None


@il.source
class Notebook(il.Source):
    @il.reference(editable=True, schema=Note)
    def notes(self) -> None:
        """Notes people write."""

    @il.asset
    def events(self) -> list:
        return []


class AssetStore:
    """Fake store: every component belongs to the test organisation and loads as ``loaded``."""

    def __init__(self) -> None:
        """Start as an editor, with nothing loaded."""
        self.role: str | None = "editor"
        self.loaded: Any = None
        self.load_error: Exception | None = None
        self.components = SimpleNamespace(get=self._get, load=self._load)

    def org_scope(self, org_id: UUID) -> OrgScope:
        """Stand in for ``get_org_scope``: the caller acts in the path's organisation with ``role``.

        Args:
            org_id: The organisation the path names.

        Returns:
            The scope.

        Raises:
            HTTPException: 404 for a non-member, as the real dependency answers.
        """
        if self.role is None:
            raise HTTPException(status_code=404, detail=f"Organisation {org_id} not found")
        caller = CallerCredential(user=cast(Profile, SimpleNamespace(id=uuid4())), token=cast(Token, SimpleNamespace()))
        return OrgScope(credential=caller, organisation=cast(Organisation, SimpleNamespace(id=org_id)), role=self.role)

    def _get(self, component_id: UUID) -> Any:
        return SimpleNamespace(id=component_id, org_id=_ORG_ID)

    def _load(self, component_id: UUID) -> Any:
        if self.load_error:
            raise self.load_error
        return self.loaded


@pytest.fixture(autouse=True)
def _clear_memory() -> Iterator[None]:
    il.MemoryDestination.clear()
    yield
    il.MemoryDestination.clear()


@pytest.fixture
def store() -> AssetStore:
    """A fresh fake store.

    Returns:
        The store.
    """
    return AssetStore()


@pytest.fixture
def client(store: AssetStore) -> TestClient:
    """Mount the assets router over the fake store.

    Args:
        store: The fake store the routes resolve against.

    Returns:
        A client for the probe app.
    """
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(assets_module.org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_org_scope] = store.org_scope
    return TestClient(app, base_url=f"http://testserver/organisations/{_ORG_ID}")


def _write(asset: il.Asset, rows: Any) -> None:
    il.MemoryDestination().write_partition(il.IOContext(asset=asset), None, rows)


def _notes(store: AssetStore, *destinations: il.Destination) -> il.Asset:
    asset = Notebook(destinations=list(destinations) or [il.MemoryDestination()]).notes
    store.loaded = asset
    return asset


def _events(store: AssetStore, rows: Any) -> il.Asset:
    asset = Notebook(destinations=[il.MemoryDestination()]).events
    _write(asset, rows)
    store.loaded = asset
    return asset


def _version(client: TestClient) -> str:
    return client.get(_ROWS_URL).json()["version"]


def _recording(write_partition: Any, order: list[str]) -> Any:
    def record(self: il.Destination, context: il.IOContext, partition: Any, data: Any) -> None:
        order.append(self.id)
        write_partition(self, context, partition, data)

    return record


class TestRead:
    def test_a_viewer_reads_the_first_page(self, client: TestClient, store: AssetStore) -> None:
        store.role = "viewer"
        _events(store, [{"name": "a", "day": dt.date(2026, 1, 1)}, {"name": "b", "day": None}])

        response = client.get(_ROWS_URL)

        assert response.status_code == 200
        assert response.json() == {
            "columns": ["name", "day"],
            "rows": [{"name": "a", "day": "2026-01-01"}, {"name": "b", "day": None}],
            "total": 2,
            "truncated": False,
            "version": None,
        }

    def test_values_are_json_safe(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"ratio": float("nan"), "at": dt.datetime(2026, 1, 1, 9, 30, tzinfo=dt.UTC)}])

        assert client.get(_ROWS_URL).json()["rows"] == [{"ratio": None, "at": "2026-01-01T09:30:00Z"}]

    def test_an_editable_asset_lists_its_schema_columns_in_order(self, client: TestClient, store: AssetStore) -> None:
        _write(_notes(store), [{"text": "hi", "note_id": "n1"}])

        body = client.get(_ROWS_URL).json()

        assert body["columns"] == ["note_id", "text", "day"]
        assert body["rows"] == [{"note_id": "n1", "text": "hi", "day": None}]

    def test_rows_take_the_schemas_shape_and_types(self, client: TestClient, store: AssetStore) -> None:
        notes = _notes(store)
        _write(notes, [{"note_id": "n1", "text": "hi", "day": dt.datetime(2026, 3, 1), "colour": "red"}])

        body = client.get(_ROWS_URL).json()

        assert body["rows"] == [{"note_id": "n1", "text": "hi", "day": "2026-03-01"}]
        saved = client.put(_ROWS_URL, json={"rows": body["rows"], "version": body["version"]})
        assert saved.status_code == 200
        assert il.MemoryDestination().read(il.IOContext(asset=notes)) == [
            {"note_id": "n1", "text": "hi", "day": dt.date(2026, 3, 1)}
        ]

    def test_bytes_are_base64_text(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"blob": b"\xff\xfe"}])

        assert client.get(_ROWS_URL).json()["rows"] == [{"blob": "__4="}]

    def test_numpy_values_become_python_ones(self, client: TestClient, store: AssetStore) -> None:
        numpy = pytest.importorskip("numpy")
        _events(store, [{"count": numpy.int64(3), "scores": numpy.array([1.5, 2.0])}])

        assert client.get(_ROWS_URL).json()["rows"] == [{"count": 3, "scores": [1.5, 2.0]}]

    def test_a_value_json_cannot_carry_is_a_502(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"value": _Unprintable()}])

        response = client.get(_ROWS_URL)

        assert response.status_code == 502
        assert response.json()["detail"] == "no text for this value"

    def test_pages_through_the_rows(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"n": n} for n in range(5)])

        middle = client.get(_ROWS_URL, params={"limit": 2, "offset": 2}).json()
        last = client.get(_ROWS_URL, params={"limit": 2, "offset": 4}).json()

        assert (middle["rows"], middle["total"], middle["truncated"]) == ([{"n": 2}, {"n": 3}], 4, True)
        assert (last["rows"], last["total"], last["truncated"]) == ([{"n": 4}], 5, False)

    @pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 10_001}, {"offset": -1}])
    def test_a_page_outside_the_bounds_is_a_422(self, client: TestClient, store: AssetStore, params: dict) -> None:
        _events(store, [{"n": 1}])

        assert client.get(_ROWS_URL, params=params).status_code == 422

    def test_a_search_keeps_the_rows_a_text_cell_of_which_contains_it(
        self, client: TestClient, store: AssetStore
    ) -> None:
        names = ["Black Friday", "Spring", "black week", None]
        _events(store, [{"name": name, "code": 7} for name in names])

        body = client.get(_ROWS_URL, params={"search": "BLACK", "limit": 1, "offset": 1}).json()

        assert body["rows"] == [{"name": "black week", "code": 7}]
        assert (body["total"], body["truncated"]) == (2, False)

    def test_a_search_never_matches_a_cell_that_is_not_text(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"name": "a", "code": 7}])

        body = client.get(_ROWS_URL, params={"search": "7"}).json()

        assert (body["rows"], body["total"]) == ([], 0)

    def test_an_empty_search_reads_every_row(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"name": "a"}, {"name": "b"}])

        body = client.get(_ROWS_URL, params={"search": ""}).json()

        assert body["total"] == 2

    def test_a_search_reads_only_up_to_the_cap(
        self, client: TestClient, store: AssetStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(assets_module, "ROW_CAP", 3)
        _events(store, [{"name": name} for name in ("a1", "b2", "a3", "a4", "a5")])

        body = client.get(_ROWS_URL, params={"search": "a"}).json()

        assert body["rows"] == [{"name": "a1"}, {"name": "a3"}]
        assert (body["total"], body["truncated"]) == (2, True)

    def test_a_table_never_written_has_no_rows(self, client: TestClient, store: AssetStore) -> None:
        _notes(store)

        body = client.get(_ROWS_URL).json()

        assert body == {
            "columns": ["note_id", "text", "day"],
            "rows": [],
            "total": 0,
            "truncated": False,
            "version": assets_module.AssetRows._version([]),
        }

    def test_a_component_that_is_no_asset_is_a_400(self, client: TestClient, store: AssetStore) -> None:
        store.loaded = il.MemoryDestination()

        response = client.get(_ROWS_URL)

        assert response.status_code == 400
        assert response.json()["detail"] == "Component is not an asset"

    def test_an_asset_without_a_destination_is_a_400(self, client: TestClient, store: AssetStore) -> None:
        store.loaded = Notebook().events

        response = client.get(_ROWS_URL)

        assert response.status_code == 400
        assert response.json()["detail"] == "Asset 'notebook.events' has no destination"

    def test_a_drifted_asset_is_a_404(self, client: TestClient, store: AssetStore) -> None:
        store.load_error = ComponentDriftError("drifted")

        assert client.get(_ROWS_URL).status_code == 404

    def test_a_non_member_is_a_404(self, client: TestClient, store: AssetStore) -> None:
        store.role = None

        assert client.get(_ROWS_URL).status_code == 404

    def test_a_destination_failure_is_a_502_without_a_traceback(
        self, client: TestClient, store: AssetStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _events(store, [{"n": 1}])

        def broken(self: il.Destination, context: il.IOContext, limit: int) -> Any:
            raise RuntimeError("warehouse unreachable")

        monkeypatch.setattr(il.MemoryDestination, "head", broken)

        response = client.get(_ROWS_URL)

        assert response.status_code == 502
        assert response.json()["detail"] == "warehouse unreachable"

    def test_a_destination_that_cannot_preview_is_a_400(
        self, client: TestClient, store: AssetStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _events(store, [{"n": 1}])

        def refuse(self: il.Destination, context: il.IOContext, limit: int) -> Any:
            raise ConfigError(f"Destination '{self.key}' cannot preview rows")

        monkeypatch.setattr(il.MemoryDestination, "head", refuse)

        response = client.get(_ROWS_URL)

        assert response.status_code == 400
        assert response.json()["detail"] == "Destination 'memory_destination' cannot preview rows"


class TestWrite:
    def test_a_viewer_cannot_write(self, client: TestClient, store: AssetStore) -> None:
        _notes(store)
        version = _version(client)
        store.role = "viewer"

        response = client.put(_ROWS_URL, json={"rows": [], "version": version})

        assert response.status_code == 403

    def test_an_asset_that_is_not_editable_is_a_400(self, client: TestClient, store: AssetStore) -> None:
        _events(store, [{"n": 1}])

        response = client.put(_ROWS_URL, json={"rows": [], "version": "x"})

        assert response.status_code == 400
        assert response.json()["detail"] == "Asset 'notebook.events' is not editable"

    @pytest.mark.parametrize(
        ("rows", "message"),
        [
            ([{"note_id": "n1", "text": "ok"}, {"note_id": "n2"}], "row 1: missing required fields: ['text']"),
            ([{"note_id": "n1", "text": "ok", "colour": "red"}], "row 0: extra fields not in schema: ['colour']"),
        ],
    )
    def test_a_row_failing_the_schema_is_a_422_naming_it(
        self, client: TestClient, store: AssetStore, rows: list[dict], message: str
    ) -> None:
        _notes(store)

        response = client.put(_ROWS_URL, json={"rows": rows, "version": _version(client)})

        assert response.status_code == 422
        assert message in response.json()["detail"]

    def test_a_failing_value_names_its_field(self, client: TestClient, store: AssetStore) -> None:
        _notes(store)
        rows = [{"note_id": "n1", "text": "ok", "day": "not a day"}]

        detail = client.put(_ROWS_URL, json={"rows": rows, "version": _version(client)}).json()["detail"]

        assert "row 0" in detail
        assert "day" in detail

    def test_more_rows_than_the_cap_is_a_422(
        self, client: TestClient, store: AssetStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(assets_module, "ROW_CAP", 1)
        _notes(store)
        rows = [{"note_id": "n1", "text": "a"}, {"note_id": "n2", "text": "b"}]

        response = client.put(_ROWS_URL, json={"rows": rows, "version": _version(client)})

        assert response.status_code == 422

    def test_a_table_changed_since_it_was_read_is_a_409(self, client: TestClient, store: AssetStore) -> None:
        notes = _notes(store)
        version = _version(client)
        _write(notes, [{"note_id": "n9", "text": "someone else's"}])

        response = client.put(_ROWS_URL, json={"rows": [{"note_id": "n1", "text": "mine"}], "version": version})

        assert response.status_code == 409
        assert response.json()["detail"] == "The table changed since it was read"
        assert il.MemoryDestination().read(il.IOContext(asset=notes)) == [{"note_id": "n9", "text": "someone else's"}]

    def test_the_version_ignores_row_order(self, client: TestClient, store: AssetStore) -> None:
        notes = _notes(store)
        rows = [{"note_id": "n1", "text": "a"}, {"note_id": "n2", "text": "b"}]
        _write(notes, rows)
        version = _version(client)
        _write(notes, rows[::-1])

        assert _version(client) == version

    def test_the_rows_replace_the_table_typed_and_the_version_changes(
        self, client: TestClient, store: AssetStore
    ) -> None:
        notes = _notes(store)
        _write(notes, [{"note_id": "old", "text": "gone", "day": None}])
        version = _version(client)

        response = client.put(
            _ROWS_URL, json={"rows": [{"note_id": "n1", "text": "new", "day": "2026-03-01"}], "version": version}
        )

        assert response.status_code == 200
        body = response.json()
        assert body["rows"] == [{"note_id": "n1", "text": "new", "day": "2026-03-01"}]
        assert body["version"] not in (None, version)
        assert body["version"] == _version(client)
        stored = il.MemoryDestination().read(il.IOContext(asset=notes))
        assert stored == [{"note_id": "n1", "text": "new", "day": dt.date(2026, 3, 1)}]

    def test_an_empty_list_empties_the_table(self, client: TestClient, store: AssetStore, tmp_path: Path) -> None:
        notes = _notes(store, il.CSVDestination(base_path=str(tmp_path)))
        client.put(_ROWS_URL, json={"rows": [{"note_id": "n1", "text": "a"}], "version": _version(client)})

        response = client.put(_ROWS_URL, json={"rows": [], "version": _version(client)})

        assert response.status_code == 200
        assert (response.json()["rows"], response.json()["total"]) == ([], 0)
        assert il.CSVDestination(base_path=str(tmp_path)).read(il.IOContext(asset=notes, schema=Note)) == []

    def test_the_rows_go_to_every_destination(self, client: TestClient, store: AssetStore, tmp_path: Path) -> None:
        csv = il.CSVDestination(base_path=str(tmp_path))
        notes = _notes(store, il.MemoryDestination(), csv)

        client.put(_ROWS_URL, json={"rows": [{"note_id": "n1", "text": "a"}], "version": _version(client)})

        expected = [{"note_id": "n1", "text": "a", "day": None}]
        assert il.MemoryDestination().read(il.IOContext(asset=notes)) == expected
        assert csv.read(il.IOContext(asset=notes, schema=Note)) == expected

    def test_a_table_holding_more_rows_than_the_cap_cannot_be_saved(
        self, client: TestClient, store: AssetStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notes = _notes(store)
        stored = [{"note_id": "n1", "text": "a"}, {"note_id": "n2", "text": "b"}]
        _write(notes, stored)
        version = _version(client)
        monkeypatch.setattr(assets_module, "ROW_CAP", 1)

        response = client.put(_ROWS_URL, json={"rows": [], "version": version})

        assert response.status_code == 409
        assert response.json()["detail"] == "The table holds more than 1 rows and cannot be edited in the app"
        assert il.MemoryDestination().read(il.IOContext(asset=notes)) == stored

    def test_the_default_destination_is_written_last(
        self, client: TestClient, store: AssetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        csv = il.CSVDestination(id="files", base_path=str(tmp_path))
        _notes(store, csv, il.MemoryDestination(id="cache"))
        order: list[str] = []
        for kind in (il.CSVDestination, il.MemoryDestination):
            monkeypatch.setattr(kind, "write_partition", _recording(kind.write_partition, order))

        client.put(_ROWS_URL, json={"rows": [{"note_id": "n1", "text": "a"}], "version": _version(client)})

        assert order == ["cache", "files"]

    def test_a_failed_write_names_the_destination_and_those_already_written(
        self, client: TestClient, store: AssetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        csv = il.CSVDestination(id="files", base_path=str(tmp_path))
        notes = _notes(store, csv, il.MemoryDestination(id="cache"))
        version = _version(client)

        def broken(self: il.Destination, context: il.IOContext, partition: Any, data: Any) -> None:
            raise RuntimeError("disk full")

        monkeypatch.setattr(il.CSVDestination, "write_partition", broken)

        response = client.put(_ROWS_URL, json={"rows": [{"note_id": "n1", "text": "a"}], "version": version})

        assert response.status_code == 502
        assert response.json()["detail"] == (
            "Writing to 'csv_destination' (files) failed: disk full. Already written: 'memory_destination' (cache)"
        )
        assert il.MemoryDestination().read(il.IOContext(asset=notes)) == [{"note_id": "n1", "text": "a", "day": None}]

    def test_a_failure_before_any_write_says_none_was_written(
        self, client: TestClient, store: AssetStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _notes(store, il.MemoryDestination(id="cache"))
        version = _version(client)

        def broken(self: il.Destination, context: il.IOContext, partition: Any, data: Any) -> None:
            raise RuntimeError("quota exceeded")

        monkeypatch.setattr(il.MemoryDestination, "write_partition", broken)

        response = client.put(_ROWS_URL, json={"rows": [], "version": version})

        assert response.status_code == 502
        assert response.json()["detail"] == (
            "Writing to 'memory_destination' (cache) failed: quota exceeded. Already written: none"
        )
