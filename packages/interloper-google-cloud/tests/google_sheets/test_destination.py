"""Tests for GoogleSheetsDestination."""

import datetime
import json
from decimal import Decimal
from functools import partial
from typing import Any, cast
from urllib.parse import unquote

import httpx2
import interloper as il
import pytest
from interloper.destination import IOContext
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning import Partition, PartitionConfig
from interloper.schema import Schema
from pydantic import Field

from interloper_google_cloud.connection import GoogleCloudConnection
from interloper_google_cloud.google_sheets import destination as sheets
from interloper_google_cloud.google_sheets.destination import GoogleSheetsDestination, _cell

_SA_KEY = json.dumps({"type": "service_account", "project_id": "test-proj"})
_PREFIX = "/v4/spreadsheets/sheet-id"


class _Credentials:
    token = "t"
    valid = True


class _FakeSheets:
    """An in-memory spreadsheet behind the Sheets REST API, recording every request."""

    def __init__(self, tabs: dict[str, list[list[Any]]] | None = None):
        self.tabs = tabs or {}
        self.requests: list[httpx2.Request] = []

    def calls(self) -> list[tuple[str, str]]:
        return [(r.method, unquote(r.url.path).removeprefix(_PREFIX)) for r in self.requests]

    def writes(self) -> list[httpx2.Request]:
        return [r for r in self.requests if r.method in {"POST", "PUT"}]

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        path = unquote(request.url.path).removeprefix(_PREFIX)
        body = json.loads(request.content) if request.content else {}
        if path == "":
            return httpx2.Response(200, json={"sheets": [{"properties": {"title": t}} for t in self.tabs]})
        if path == ":batchUpdate":
            self.tabs[body["requests"][0]["addSheet"]["properties"]["title"]] = []
            return httpx2.Response(200, json={})
        range_, _, action = path.removeprefix("/values/").partition(":")
        title = range_.strip("'").replace("''", "'")
        if action == "append":
            self.tabs[title].extend(body["values"])
        elif action == "clear":
            self.tabs[title] = []
        else:
            return httpx2.Response(200, json={"values": self.tabs[title]} if self.tabs[title] else {})
        return httpx2.Response(200, json={})


class _RowSchema(Schema):
    day: datetime.date | None = Field(...)
    clicks: int | None = Field(...)
    cost: Decimal | None = Field(...)
    meta: dict[str, Any] | None = Field(...)


@il.asset
def report() -> list:
    return []


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def daily() -> list:
    return []


@il.asset(partitioning=PartitionConfig(column="region"))
def regional() -> list:
    return []


def _ctx(asset: Any, scope: Any = None, schema: type[Schema] | None = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> _FakeSheets:
    server = _FakeSheets()
    monkeypatch.setattr(sheets, "_credentials", lambda connection: _Credentials())
    transport = httpx2.MockTransport(server.handler)
    monkeypatch.setattr(sheets, "_SheetsAPI", partial(sheets._SheetsAPI, transport=transport))
    return server


@pytest.fixture
def dest(fake: _FakeSheets) -> GoogleSheetsDestination:
    return GoogleSheetsDestination(
        id="sheets", spreadsheet_id="sheet-id", connection=GoogleCloudConnection(id="c", service_account_key=_SA_KEY)
    )


class TestTitle:
    """Tab title derivation."""

    def test_table_without_dataset(self, dest, fake):
        dest.write(_ctx(report()), [{"a": 1}])
        assert list(fake.tabs) == ["report"]

    def test_dataset_prefixes_table(self, dest, fake):
        dest.write(_ctx(report()(dataset="ds")), [{"a": 1}])
        assert list(fake.tabs) == ["ds.report"]

    def test_over_100_characters_raises(self):
        assert GoogleSheetsDestination._title("t" * 100, None) == "t" * 100
        with pytest.raises(ConfigError, match="101 characters"):
            GoogleSheetsDestination._title("t" * 98, "ds")


class TestCell:
    """Cell encoding per type."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, ""),
            (True, True),
            (3, 3),
            (1.5, 1.5),
            (float("nan"), ""),
            (float("inf"), ""),
            (Decimal("9.99"), "9.99"),
            (datetime.date(2024, 1, 2), "2024-01-02"),
            (datetime.datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05"),
            ({"k": datetime.date(2024, 1, 2)}, '{"k": "2024-01-02"}'),
            ([1, float("nan")], "[1, null]"),
            ("text", "text"),
        ],
    )
    def test_encoding(self, value, expected):
        assert _cell(value) == expected
        assert type(_cell(value)) is type(expected)


class TestWrite:
    """Writes across the three shapes."""

    def test_first_write_creates_tab_and_header(self, dest, fake):
        dest.write(_ctx(report()), [{"a": 1, "b": None}, {"a": 2, "b": "x"}])

        assert fake.calls() == [
            ("GET", ""),
            ("GET", ""),
            ("POST", ":batchUpdate"),
            ("POST", "/values/'report':append"),
        ]
        assert fake.tabs["report"] == [["a", "b"], [1, ""], [2, "x"]]

    def test_header_follows_schema(self, dest, fake):
        rows = [{"day": datetime.date(2024, 1, 1), "clicks": 3, "cost": Decimal("1.50"), "meta": {"k": 1}}]
        dest.write(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 1)), _RowSchema), rows)
        assert fake.tabs["daily"] == [["day", "clicks", "cost", "meta"], ["2024-01-01", 3, "1.50", '{"k": 1}']]

    def test_time_partition_rewrites_other_rows_then_appends(self, dest, fake):
        fake.tabs["daily"] = [
            ["day", "clicks"],
            ["2024-01-01", 1],
            ["2024-01-02T10:00:00", 2],
            ["2024-01-03", 3],
        ]
        dest.write(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 2))), [{"day": "2024-01-02", "clicks": 9}])

        assert [c for c in fake.calls() if c[0] == "POST"] == [
            ("POST", "/values/'daily':clear"),
            ("POST", "/values/'daily':append"),
            ("POST", "/values/'daily':append"),
        ]
        assert fake.tabs["daily"] == [["day", "clicks"], ["2024-01-01", 1], ["2024-01-03", 3], ["2024-01-02", 9]]

    def test_untouched_tab_is_not_rewritten(self, dest, fake):
        fake.tabs["daily"] = [["day", "clicks"], ["2024-01-01", 1]]
        dest.write(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 2))), [{"day": "2024-01-02", "clicks": 9}])
        assert [c for c in fake.calls() if c[0] == "POST"] == [("POST", "/values/'daily':append")]

    def test_non_time_partition_matches_by_equality(self, dest, fake):
        fake.tabs["regional"] = [["region", "n"], ["eu", 1], ["us", 2], ["eu-west", 3]]
        dest.write(_ctx(regional(), Partition("eu")), [{"region": "eu", "n": 7}])
        assert fake.tabs["regional"] == [["region", "n"], ["us", 2], ["eu-west", 3], ["eu", 7]]

    def test_whole_write_keeps_only_header(self, dest, fake):
        fake.tabs["report"] = [["a"], [1], [2]]
        dest.write(_ctx(report()), [{"a": 3}])
        assert fake.tabs["report"] == [["a"], [3]]

    def test_window_deletes_each_partition_then_appends_once(self, dest, fake):
        fake.tabs["daily"] = [["day", "clicks"], ["2024-01-01", 1], ["2024-01-02", 2], ["2024-01-03", 3]]
        window = il.TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
        rows = [{"day": "2024-01-01", "clicks": 10}, {"day": "2024-01-02", "clicks": 20}]
        dest.write(_ctx(daily(), window), rows)

        posts = [c for c in fake.calls() if c[0] == "POST"]
        assert posts.count(("POST", "/values/'daily':clear")) == 2
        assert posts[-1] == ("POST", "/values/'daily':append")
        assert fake.tabs["daily"] == [["day", "clicks"], ["2024-01-03", 3], ["2024-01-01", 10], ["2024-01-02", 20]]

    def test_rows_align_to_existing_header_and_extras_warn(self, dest, fake):
        fake.tabs["report"] = [["b", "a"]]
        with pytest.warns(UserWarning, match=r"Columns \['c'\] are not in the schema for 'report'"):
            dest.write(_ctx(report()), [{"a": 1, "c": 3}])
        assert fake.tabs["report"] == [["b", "a"], ["", 1]]

    def test_new_tab_drops_columns_outside_the_schema(self, dest, fake):
        rows = [{"day": datetime.date(2024, 1, 1), "clicks": 3, "cost": None, "meta": None, "extra": 1}]
        with pytest.warns(UserWarning, match=r"Columns \['extra'\]"):
            dest.write(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 1)), _RowSchema), rows)
        assert fake.tabs["daily"][0] == ["day", "clicks", "cost", "meta"]

    def test_appends_in_chunks(self, dest, fake, monkeypatch):
        monkeypatch.setattr(sheets, "_APPEND_CHUNK_ROWS", 2)
        dest.write(_ctx(report()), [{"a": i} for i in range(3)])
        assert [c for c in fake.calls() if c[0] == "POST"].count(("POST", "/values/'report':append")) == 2
        assert fake.tabs["report"] == [["a"], [0], [1], [2]]

    def test_every_request_is_authorized_and_writes_are_raw(self, dest, fake):
        fake.tabs["daily"] = [["day", "clicks"], ["2024-01-02", 1]]
        dest.write(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 2))), [{"day": "2024-01-02", "clicks": 9}])

        assert all(r.headers["Authorization"] == "Bearer t" for r in fake.requests)
        appends = [r for r in fake.writes() if r.url.path.endswith(":append")]
        assert appends
        assert all(r.url.params["valueInputOption"] == "RAW" for r in appends)


class TestRead:
    """Selects, reads and counts."""

    def test_select_filters_and_empty_cells_are_none(self, dest, fake):
        fake.tabs["daily"] = [["day", "clicks", "note"], ["2024-01-01", 1, ""], ["2024-01-02", 2]]
        rows = dest.read(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 2))))
        assert rows == [{"day": "2024-01-02", "clicks": 2, "note": None}]

    def test_select_whole(self, dest, fake):
        fake.tabs["report"] = [["a"], [1], [""]]
        assert dest.read(_ctx(report())) == [{"a": 1}, {"a": None}]

    def test_read_partition_reconciles_types(self, dest, fake):
        fake.tabs["daily"] = [["day", "clicks", "cost", "meta"], ["2024-01-02", 3, "1.50", '{"k": 1}']]
        rows = dest.read(_ctx(daily(), il.TimePartition(datetime.date(2024, 1, 2)), _RowSchema))
        assert rows == [{"day": datetime.date(2024, 1, 2), "clicks": 3, "cost": Decimal("1.50"), "meta": {"k": 1}}]

    def test_round_trip(self, dest, fake):
        rows = [{"day": datetime.date(2024, 1, 2), "clicks": None, "cost": Decimal(2), "meta": None}]
        context = _ctx(daily(), il.TimePartition(datetime.date(2024, 1, 2)), _RowSchema)
        dest.write(context, rows)
        assert dest.read(context) == rows

    def test_missing_tab_raises(self, dest):
        with pytest.raises(DataNotFoundError, match="Sheet 'report' does not exist in spreadsheet 'sheet-id'"):
            dest.read(_ctx(report()))

    def test_count_groups_by_value(self, dest, fake):
        fake.tabs["daily"] = [["day", "clicks"], ["2024-01-01", 1], ["2024-01-01", 2], ["2024-01-02", 3]]
        assert dest.partition_row_counts(_ctx(daily())) == {"2024-01-01": 2, "2024-01-02": 1}

    def test_count_missing_tab_raises(self, dest):
        with pytest.raises(DataNotFoundError):
            dest.partition_row_counts(_ctx(daily()))

    def test_delete_on_missing_tab_is_a_no_op(self, dest, fake):
        dest.delete("report", None, None)
        assert fake.calls() == [("GET", "")]


class TestCredentials:
    """Credential resolution and refresh."""

    def test_expired_token_is_refreshed(self, monkeypatch):
        refreshed = []

        class Expired:
            token = "fresh"
            valid = False

            def refresh(self, request):
                refreshed.append(request)

        server = _FakeSheets({"report": []})
        api = sheets._SheetsAPI("sheet-id", cast(Any, Expired()), transport=httpx2.MockTransport(server.handler))
        assert api.sheet_titles() == {"report"}
        assert len(refreshed) == 1
        assert server.requests[0].headers["Authorization"] == "Bearer fresh"

    def test_ambient_credentials_without_key(self, monkeypatch):
        seen = {}

        def default(scopes):
            seen["scopes"] = scopes
            return "ambient", "proj"

        monkeypatch.setattr(sheets.google.auth, "default", default)
        assert sheets._credentials(GoogleCloudConnection(id="c", service_account_key="")) == "ambient"
        assert seen["scopes"] == [sheets.SHEETS_SCOPE]
