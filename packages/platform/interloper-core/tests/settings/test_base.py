"""Tests for ``interloper.settings.base``."""

from __future__ import annotations

import importlib

import pytest
from pydantic import ValidationError

from interloper.errors import ConfigError
from interloper.extension import Manifests
from interloper.settings import Settings
from interloper.settings import base as settings_base

WIDGETS = '''\
from typing import ClassVar

from pydantic import Field

from interloper.settings import Settings


class WidgetSettings(Settings):
    """Widget settings."""

    public: ClassVar[frozenset[str]] = frozenset({"size"})

    size: int = Field(default=1, description="Widget size.")
    colour: str = Field(default="red", description="Widget colour.")


class GadgetSettings(Settings):
    """Gadget settings."""

    count: int = Field(default=0, description="Gadgets.")
'''

ACME = """\
manifest_version: 1
key: acme
settings:
  acme_widgets: acme_ext.settings:WidgetSettings
  acme_gadgets: acme_ext.settings:GadgetSettings
"""


@pytest.fixture(autouse=True)
def manifests(tmp_path, monkeypatch: pytest.MonkeyPatch) -> Manifests:
    """Read only the throwaway packages a test installs, from a directory holding no other config file.

    Returns:
        The manifests the settings sections are declared in.
    """
    monkeypatch.chdir(tmp_path)
    manifests = Manifests()
    monkeypatch.setattr(settings_base, "MANIFESTS", manifests)
    return manifests


@pytest.fixture
def widgets(install) -> type[Settings]:
    """Install the acme extension, which declares two sections.

    Returns:
        The class of its ``acme_widgets`` section.
    """
    install("acme_ext", ACME, settings=WIDGETS)
    return importlib.import_module("acme_ext.settings").WidgetSettings


class TestDeclaration:
    def test_a_section_is_named_by_the_manifest_that_declares_it(self, widgets):
        assert widgets.section() == "acme_widgets"
        assert set(Settings.declared()) == {"acme_widgets", "acme_gadgets"}

    def test_a_section_declared_by_two_packages_fails_every_load_naming_both(self, install, widgets):
        install(
            "rival_ext",
            "manifest_version: 1\nkey: rival\nsettings:\n  acme_widgets: rival_ext.settings:WidgetSettings\n",
            settings=WIDGETS,
        )

        with pytest.raises(
            ConfigError, match="'acme_widgets' is declared by package 'acme_ext' and by package 'rival_"
        ):
            widgets.get()

    def test_a_class_is_declared_under_one_name(self, install):
        install(
            "acme_ext",
            "manifest_version: 1\nkey: acme\nsettings:\n"
            "  acme_widgets: acme_ext.settings:WidgetSettings\n  acme_more: acme_ext.settings:WidgetSettings\n",
            settings=WIDGETS,
        )

        with pytest.raises(ConfigError, match="declared as settings section 'acme_widgets' and as 'acme_more'"):
            Settings.declared()

    def test_a_section_name_is_an_identifier(self, install):
        install(
            "acme_ext",
            "manifest_version: 1\nkey: acme\nsettings:\n  Acme-Widgets: acme_ext.settings:WidgetSettings\n",
            settings=WIDGETS,
        )

        with pytest.raises(ConfigError, match="'Acme-Widgets' of package 'acme_ext' must be lowercase"):
            Settings.declared()

    def test_a_class_no_manifest_declares_fails_naming_its_import_path(self, widgets):
        class StraySettings(Settings):
            """Declared nowhere."""

        with pytest.raises(ConfigError, match=r"test_base:.*StraySettings is declared in no package manifest"):
            StraySettings.get()


class TestSources:
    def test_a_section_with_no_file_and_no_environment_takes_its_defaults(self, widgets):
        assert (widgets.get().size, widgets.get().colour) == (1, "red")

    def test_a_section_validates_from_its_block_of_the_config_file(self, widgets, tmp_path):
        (tmp_path / "interloper.yaml").write_text("acme_widgets:\n  size: 3\n")

        assert widgets.get().size == 3

    def test_a_section_validates_from_its_environment_variables(self, widgets, monkeypatch):
        monkeypatch.setenv("INTERLOPER_ACME_WIDGETS_SIZE", "4")

        assert widgets.get().size == 4

    def test_the_environment_beats_the_file_for_every_field(self, widgets, tmp_path, monkeypatch):
        (tmp_path / "interloper.yaml").write_text("acme_widgets:\n  size: 3\n  colour: blue\n")
        monkeypatch.setenv("INTERLOPER_ACME_WIDGETS_SIZE", "4")

        assert (widgets.get().size, widgets.get().colour) == (4, "blue")

    def test_a_block_no_package_declares_is_ignored(self, widgets, tmp_path):
        (tmp_path / "interloper.yaml").write_text("acme_widgets:\n  size: 3\nelsewhere:\n  anything: 1\n")

        assert widgets.get().size == 3

    def test_an_unknown_field_inside_a_declared_block_is_an_error(self, widgets, tmp_path):
        (tmp_path / "interloper.yaml").write_text("acme_widgets:\n  sise: 3\n")

        with pytest.raises(ValidationError, match="sise"):
            widgets.get()

    def test_an_invalid_value_from_the_environment_is_an_error(self, widgets, monkeypatch):
        monkeypatch.setenv("INTERLOPER_ACME_WIDGETS_SIZE", "large")

        with pytest.raises(ValidationError, match="size"):
            widgets.get()

    def test_an_empty_block_is_no_block(self, widgets, tmp_path):
        (tmp_path / "interloper.yaml").write_text("acme_widgets:\n")

        assert widgets.get().size == 1

    @pytest.mark.parametrize(
        ("document", "error"),
        [
            ("acme_widgets: 3\n", "Section 'acme_widgets' of interloper.yaml must be a mapping of fields"),
            ("- acme_widgets\n", "interloper.yaml must be a mapping of settings sections"),
        ],
    )
    def test_a_config_file_of_another_shape_is_refused(self, widgets, tmp_path, document, error):
        (tmp_path / "interloper.yaml").write_text(document)

        with pytest.raises(ConfigError, match=error):
            widgets.get()


class TestActiveInstance:
    def test_a_section_is_loaded_once_and_kept(self, widgets, monkeypatch):
        first = widgets.get()
        monkeypatch.setenv("INTERLOPER_ACME_WIDGETS_SIZE", "4")

        assert widgets.get() is first

    def test_an_activated_section_is_the_one_returned(self, widgets):
        widgets.activate(widgets(size=9))

        assert widgets.get().size == 9

    def test_forgetting_a_section_makes_the_next_read_load_it_again(self, widgets, monkeypatch):
        widgets.get()
        monkeypatch.setenv("INTERLOPER_ACME_WIDGETS_SIZE", "4")

        widgets.clear_active()

        assert widgets.get().size == 4

    def test_forgetting_one_section_keeps_the_others(self, widgets):
        gadgets = importlib.import_module("acme_ext.settings").GadgetSettings
        gadgets.activate(gadgets(count=2))
        widgets.activate(widgets(size=9))

        widgets.clear_active()

        assert (widgets.get().size, gadgets.get().count) == (1, 2)

    def test_forgetting_through_the_base_class_forgets_every_section(self, widgets):
        gadgets = importlib.import_module("acme_ext.settings").GadgetSettings
        gadgets.activate(gadgets(count=2))
        widgets.activate(widgets(size=9))

        Settings.clear_active()

        assert (widgets.get().size, gadgets.get().count) == (1, 0)
