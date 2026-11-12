"""Tests for ``interloper.settings.sections``."""

import pytest

from interloper.errors import ConfigError
from interloper.settings import (
    CatalogSettings,
    LogSettings,
    RunnerSettings,
    RunSettings,
    ServerSettings,
    TelemetrySettings,
)

SECTIONS = [CatalogSettings, LogSettings, RunnerSettings, RunSettings, ServerSettings, TelemetrySettings]


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered.

    A section reads its block of the config file in the working directory,
    so without this these tests assert against whatever the repo-root config
    happens to contain.
    """
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize(
    ("section", "name"),
    [
        (CatalogSettings, "catalog"),
        (LogSettings, "log"),
        (RunnerSettings, "runner"),
        (RunSettings, "run"),
        (ServerSettings, "server"),
        (TelemetrySettings, "otel"),
    ],
)
def test_core_declares_its_own_sections(section, name):
    """Each section is read from the block and the ``INTERLOPER_<SECTION>_`` variables of the name core declares."""
    assert section.section() == name
    assert section.declared()[name].extension == "core"


@pytest.mark.parametrize("section", SECTIONS)
def test_every_public_name_reaches_a_field(section):
    """A public name is a field, or a key below a mapping field."""
    assert {name.partition(".")[0] for name in section.public} <= set(section.model_fields)


def test_catalog_include_defaults_to_everything_installed():
    """No import paths unless configured: the catalog is then everything installed."""
    assert CatalogSettings().include == []


def test_catalog_include_env(monkeypatch: pytest.MonkeyPatch):
    """The env var takes a comma-separated list; entries are trimmed, their case kept."""
    monkeypatch.setenv("INTERLOPER_CATALOG_INCLUDE", " pkg.sources.Shop ,pkg.sources.Finance,")

    assert CatalogSettings.get().include == ["pkg.sources.Shop", "pkg.sources.Finance"]


def test_catalog_include_yaml(tmp_path):
    """The YAML ``catalog`` block lists the import paths under ``include``."""
    (tmp_path / "interloper.yaml").write_text("catalog:\n  include:\n    - pkg.sources.Shop\n")

    assert CatalogSettings.get().include == ["pkg.sources.Shop"]


def test_a_catalog_written_as_a_list_is_refused(tmp_path):
    """A catalog written as a bare list fails loudly rather than enabling everything installed."""
    (tmp_path / "interloper.yaml").write_text("catalog:\n  - pkg.sources.Shop\n")

    with pytest.raises(ConfigError, match=r"'catalog' of interloper\.yaml must be a mapping of fields"):
        CatalogSettings.get()


def test_telemetry_settings_defaults():
    """Telemetry is off by default; both signals default on once enabled."""
    settings = TelemetrySettings()

    assert settings.enabled is False
    assert settings.protocol == "grpc"
    assert settings.traces is True
    assert settings.metrics is True
    assert settings.sample_ratio == 1.0


def test_telemetry_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_OTEL_* env vars reach the otel section."""
    monkeypatch.setenv("INTERLOPER_OTEL_ENABLED", "true")
    monkeypatch.setenv("INTERLOPER_OTEL_ENDPOINT", "http://collector:4317")
    monkeypatch.setenv("INTERLOPER_OTEL_PROTOCOL", "http/protobuf")
    monkeypatch.setenv("INTERLOPER_OTEL_SAMPLE_RATIO", "0.25")

    settings = TelemetrySettings.get()

    assert settings.enabled is True
    assert settings.endpoint == "http://collector:4317"
    assert settings.protocol == "http/protobuf"
    assert settings.sample_ratio == 0.25


def test_telemetry_metric_export_interval_default():
    """Delta temporality makes this a freshness knob, so the SDK default stands."""
    assert TelemetrySettings().metric_export_interval == 60


def test_telemetry_metric_export_interval_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_OTEL_METRIC_EXPORT_INTERVAL reaches the otel section."""
    monkeypatch.setenv("INTERLOPER_OTEL_METRIC_EXPORT_INTERVAL", "5")

    assert TelemetrySettings.get().metric_export_interval == 5


def test_server_internal_url_defaults_to_the_local_api():
    """A run pod reaches the API on this host unless the deployment says where."""
    assert ServerSettings().internal_url == "http://localhost:3000"


def test_server_internal_url_env_override(monkeypatch: pytest.MonkeyPatch):
    """The chart points run pods at the cluster-internal API service."""
    monkeypatch.setenv("INTERLOPER_SERVER_INTERNAL_URL", "http://interloper-api:3000")
    assert ServerSettings().internal_url == "http://interloper-api:3000"


@pytest.mark.parametrize(
    ("external_url", "public_url"),
    [
        ("", "http://localhost:3100"),
        ("https://app.example.com", "https://app.example.com"),
        ("https://app.example.com/", "https://app.example.com"),
    ],
)
def test_server_public_url_is_the_external_url_else_the_local_server(external_url: str, public_url: str):
    """The OAuth issuer the API and the MCP server both name."""
    assert ServerSettings(external_url=external_url, port=3100).public_url == public_url
