"""Tests for ``interloper_app.dev.targets``."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import interloper as il
import pytest

from interloper_app.dev import targets as targets_module
from interloper_app.dev.targets import Targets

MODULE = '''
import interloper as il


class Forecasts(il.Source):
    """Forecasts for a city."""

    city: str = il.InputField(default="Paris")

    class Daily(il.Asset):
        """Daily forecasts."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


paris = Forecasts(city="Paris")
lyon = Forecasts(city="Lyon")
NOT_A_COMPONENT = "a string"
'''


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A project root holding a ``forecasting`` module, importable for the test.

    Yields:
        The root.
    """
    (tmp_path / "forecasting.py").write_text(MODULE)
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path.resolve()
    sys.modules.pop("forecasting", None)


def test_classes_join_the_catalog_and_instances_are_declared_in_their_module(project: Path):
    targets = Targets(project, ["forecasting.Forecasts", "forecasting.paris"], [])

    (declared,) = targets.declarations(uuid4(), il.Catalog())

    assert [cls.__name__ for cls in targets.classes] == ["Forecasts"]
    assert declared.file == "forecasting.py"
    assert [row.config["city"] for row in declared.rows] == ["Paris"]


def test_a_spec_file_is_declared_in_itself(project: Path):
    spec = project / "dev" / "collection.yaml"
    spec.parent.mkdir()
    spec.write_text("path: forecasting.Forecasts\ninit:\n  city: Nice\n")

    (declared,) = Targets(project, [], [str(spec)]).declarations(uuid4(), il.Catalog())

    assert declared.file == "dev/collection.yaml"
    assert declared.rows[0].config["city"] == "Nice"


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("forecasting.NOT_A_COMPONENT", "neither a component class nor a component instance"),
        ("forecasting.missing", "cannot import"),
        ("no_such_module.Forecasts", "cannot import"),
    ],
)
def test_a_target_that_names_no_component_stops_dev(project: Path, target: str, message: str):
    with pytest.raises(SystemExit, match=message):
        Targets(project, [target], [])


@pytest.mark.parametrize(
    ("document", "message"),
    [("path: forecasting.Missing\n", "does not build"), ("init: {}\n", "Invalid spec file")],
)
def test_a_spec_file_that_does_not_build_stops_dev(project: Path, document: str, message: str):
    spec = project / "broken.yaml"
    spec.write_text(document)

    with pytest.raises(SystemExit, match=message):
        Targets(project, [], [str(spec)]).declarations(uuid4(), il.Catalog())


def test_a_file_outside_the_project_keeps_its_absolute_name(project: Path, tmp_path_factory: pytest.TempPathFactory):
    elsewhere = tmp_path_factory.mktemp("elsewhere") / "spec.yaml"
    elsewhere.write_text("path: forecasting.Forecasts\n")

    (declared,) = Targets(project, [], [str(elsewhere)]).declarations(uuid4(), il.Catalog())

    assert declared.file == str(elsewhere.resolve())


def test_a_module_whose_instances_do_not_declare_stops_dev(project: Path, monkeypatch: pytest.MonkeyPatch):
    def broken(*args: object, **kwargs: object) -> None:
        raise ValueError("cannot walk")

    monkeypatch.setattr(targets_module.Declarations, "from_instances", broken)

    with pytest.raises(SystemExit, match=r"forecasting\.py does not declare its components: cannot walk"):
        Targets(project, ["forecasting.paris"], []).declarations(uuid4(), il.Catalog())
