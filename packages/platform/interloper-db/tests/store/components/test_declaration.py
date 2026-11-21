"""Tests for ``interloper_db.store.components.declaration``."""

from __future__ import annotations

from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import SpecError

from interloper_db.store.components.declaration import Declaration, Declarations

ORG = uuid4()
FILE = "dev/collection.yaml"


class Weather(il.Connection):
    """A connection with a token."""

    token: str = il.SecretField(default="")


class Forecasts(il.Source):
    """A source reading the weather connection."""

    city: str = il.InputField(default="Paris")
    connection: Weather

    class Daily(il.Asset):
        """Daily forecasts."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []

    class Hourly(il.Asset):
        """Hourly forecasts."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


class Report(il.Asset):
    """An asset standing on its own."""

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class Upstreams(il.Source):
    """A source another source's asset reads."""

    class Rows(il.Asset):
        """Rows."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


class Downstreams(il.Source):
    """A source whose asset reads another source's asset."""

    class Totals(il.Asset):
        """Totals."""

        rows = il.Relation("asset", "upstreams.rows", many=True, optional=True)

        def data(self, context: il.ExecutionContext, rows: list[il.Upstream]) -> list[dict]:
            return []


@pytest.fixture
def catalog() -> il.Catalog:
    return il.Catalog.from_assets([Forecasts, Weather])


def _spec(**init: object) -> il.Spec:
    return il.Spec(path=f"{__name__}.Forecasts", init=dict(init))


def _connection(**fields: object) -> dict[str, object]:
    return {"path": f"{__name__}.Weather", "init": dict(fields)}


class TestFromDocuments:
    def test_rows_come_targets_first_with_the_ids_of_their_place(self, catalog: il.Catalog):
        spec = _spec(city="Lyon", connection=_connection(token="secret"))

        first = Declarations.from_documents(ORG, FILE, [spec], catalog)
        again = Declarations.from_documents(ORG, FILE, [spec], catalog)

        connection, source = first.rows
        assert (connection.kind, connection.key, source.kind, source.key) == (
            "connection",
            "weather",
            "source",
            "forecasts",
        )
        assert source.relations == {"connection": [connection.id]}
        assert source.config["city"] == "Lyon"
        assert sorted((child.key, child.id) for child in source.children or []) == [
            ("daily", Declaration.child_id(source.id, "daily")),
            ("hourly", Declaration.child_id(source.id, "hourly")),
        ]
        assert [row.id for row in again.rows] == [row.id for row in first.rows]
        assert first.classes == [Weather, Forecasts]

    def test_an_edit_keeps_the_ids(self, catalog: il.Catalog):
        before = Declarations.from_documents(ORG, FILE, [_spec(city="Lyon")], catalog).rows[0]
        after = Declarations.from_documents(ORG, FILE, [_spec(city="Nice")], catalog).rows[0]

        assert before.id == after.id
        assert (before.config["city"], after.config["city"]) == ("Lyon", "Nice")

    def test_a_given_id_survives_a_reordering(self, catalog: il.Catalog):
        named = il.Spec(path=f"{__name__}.Forecasts", id="paris", init={"city": "Paris"})
        other = _spec(city="Lyon")

        first = Declarations.from_documents(ORG, FILE, [named, other], catalog)
        reordered = Declarations.from_documents(ORG, FILE, [other, named], catalog)

        assert first.rows[0].id == reordered.rows[1].id

    def test_a_component_referenced_twice_is_one_row(self, catalog: il.Catalog):
        shared = {"path": f"{__name__}.Weather", "id": "shared", "init": {"token": "secret"}}
        specs = [_spec(city="Lyon", connection=shared), _spec(city="Nice", connection={"ref": "shared"})]

        rows = Declarations.from_documents(ORG, FILE, specs, catalog).rows

        connections = [row for row in rows if row.kind == "connection"]
        assert len(connections) == 1
        assert all(row.relations == {"connection": [connections[0].id]} for row in rows if row.kind == "source")

    def test_a_relation_the_file_leaves_unbound_is_left_out(self, catalog: il.Catalog):
        (source,) = Declarations.from_documents(ORG, FILE, [_spec()], catalog).rows

        assert source.relations == {}

    def test_the_ids_belong_to_the_organisation_and_the_file(self, catalog: il.Catalog):
        here = Declarations.from_documents(ORG, FILE, [_spec()], catalog).rows[0].id

        assert Declarations.from_documents(uuid4(), FILE, [_spec()], catalog).rows[0].id != here
        assert Declarations.from_documents(ORG, "other.yaml", [_spec()], catalog).rows[0].id != here


class TestFromInstances:
    def test_a_module_attribute_names_its_component(self):
        connection = Weather(token="secret")
        paris = Forecasts(city="Paris", connection=connection)

        first = Declarations.from_instances(
            ORG, "weather.py", {"paris": paris}, {"paris": paris, "connection": connection}
        )
        rebuilt = Weather(token="secret")
        moved = Forecasts(city="Paris", connection=rebuilt)
        again = Declarations.from_instances(
            ORG, "weather.py", {"paris": moved}, {"connection": rebuilt, "paris": moved}
        )

        assert [row.id for row in first.rows] == [row.id for row in again.rows]
        assert [row.kind for row in first.rows] == ["connection", "source"]

    def test_only_the_targets_and_what_they_bind_are_declared(self):
        paris, lyon = Forecasts(city="Paris", connection=Weather()), Forecasts(city="Lyon", connection=Weather())

        rows = Declarations.from_instances(ORG, "weather.py", {"paris": paris}, {"paris": paris, "lyon": lyon}).rows

        assert [row.config["city"] for row in rows if row.kind == "source"] == ["Paris"]


class TestReach:
    def test_a_job_targeting_an_asset_declares_its_source_and_points_at_the_asset_row(self):
        paris = Forecasts(city="Paris", connection=Weather())
        job = il.CronJob(cron="0 6 * * *", targets=[next(asset for asset in paris.assets if asset.key == "daily")])

        rows = Declarations.from_instances(ORG, "jobs.py", {"job": job}, {"job": job, "paris": paris}).rows

        source = next(row for row in rows if row.kind == "source")
        declared_job = next(row for row in rows if row.kind == "job")
        assert declared_job.relations == {"targets": [Declaration.child_id(source.id, "daily")]}

    def test_a_component_two_modules_reach_is_one_row_of_the_first(self):
        connection = Weather(token="secret")
        paris = Forecasts(city="Paris", connection=connection)
        lyon = Forecasts(city="Lyon", connection=connection)
        seen: dict[int, UUID] = {}

        first = Declarations.from_instances(ORG, "paris.py", {"paris": paris}, {"paris": paris}, seen)
        second = Declarations.from_instances(ORG, "lyon.py", {"lyon": lyon}, {"lyon": lyon}, seen)

        assert [row.kind for row in first.rows] == ["connection", "source"]
        assert [row.kind for row in second.rows] == ["source"]
        assert second.rows[0].relations == first.rows[1].relations

    def test_a_component_that_changes_class_is_a_new_row(self, catalog: il.Catalog):
        forecast = Declarations.from_documents(ORG, FILE, [_spec()], catalog).rows[0]
        weather = Declarations.from_documents(ORG, FILE, [il.Spec(path=f"{__name__}.Weather")], catalog).rows[0]

        assert forecast.id != weather.id

    def test_a_job_keeps_its_enabled_flag(self, catalog: il.Catalog):
        job = il.Spec(key="cron_job", init={"cron": "0 6 * * *", "enabled": False})

        (row,) = Declarations.from_documents(ORG, FILE, [job], catalog).rows

        assert row.config["enabled"] is False

    def test_a_standalone_asset_keeps_its_enabled_flag(self, catalog: il.Catalog):
        (row,) = Declarations.from_documents(
            ORG, FILE, [il.Spec(path=f"{__name__}.Report", init={"enabled": False})], catalog
        ).rows

        assert row.config["enabled"] is False


class TestFromInstances:
    def test_an_asset_s_relation_to_another_source_s_asset_is_declared_with_it(self):
        up, down = Upstreams(), Downstreams()
        totals = next(asset for asset in down.assets if asset.key == "totals")
        totals.bind("rows", next(asset for asset in up.assets if asset.key == "rows"))

        rows = Declarations.from_instances(ORG, "pipelines.py", {"down": down}, {"down": down, "up": up}).rows

        upstream = next(row for row in rows if row.key == "upstreams")
        downstream = next(row for row in rows if row.key == "downstreams")
        assert [row.key for row in rows] == ["upstreams", "downstreams"]
        assert [(child.key, child.relations) for child in downstream.children or []] == [
            ("totals", {"rows": [Declaration.child_id(upstream.id, "rows")]})
        ]


class TestDocuments:
    def test_a_document_that_builds_no_component_is_refused(self, catalog: il.Catalog):
        with pytest.raises(SpecError, match="not a component"):
            Declarations.from_documents(ORG, FILE, [il.Spec(path=f"{__name__}.Settings")], catalog)

    def test_ids_given_inside_lists_count(self):
        documents = [{"key": "forecasts", "init": {"destinations": [{"path": "acme.Memory", "id": "memory"}]}}]

        assert Declarations._given_ids(documents) == {"memory"}


class Settings(il.Serializable):
    """A serializable that is not a component."""
