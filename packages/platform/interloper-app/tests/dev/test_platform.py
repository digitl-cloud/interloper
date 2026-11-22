"""Tests for ``interloper_app.dev.platform``."""

from __future__ import annotations

import http.server
import io
import os
import runpy
import socket
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock
from uuid import uuid4

import pytest
from interloper.catalog import Catalog
from interloper.settings import ServerSettings
from interloper_api.settings import AuthSettings
from interloper_db.store.components import Applied

from interloper_app.dev import platform as platform_module
from interloper_app.dev.database import DevDatabase
from interloper_app.dev.platform import Platform, PlatformSpec
from interloper_app.dev.signin import SignIn

ORG = uuid4()


@pytest.fixture
def spec() -> PlatformSpec:
    return PlatformSpec(
        port=3100,
        parent=os.getpid(),
        root="/work/acme",
        public_url="http://127.0.0.1:3100",
        sign_in=SignIn.generate("acme", "acme"),
        ui_entries={"acme": "http://127.0.0.1:5173/src/index.ts", "beta": "http://127.0.0.1:5173/other.ts"},
    )


def test_the_spec_crosses_the_process_boundary_as_json(spec: PlatformSpec):
    assert PlatformSpec.model_validate_json(spec.model_dump_json()) == spec


def test_each_dev_server_origin_is_named_once(spec: PlatformSpec):
    assert spec.dev_origins == ["http://127.0.0.1:5173"]


def test_main_serves_the_spec_read_from_standard_input(spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch):
    served: list[PlatformSpec] = []
    monkeypatch.setattr(platform_module.sys, "stdin", io.StringIO(spec.model_dump_json()))
    monkeypatch.setattr(PlatformSpec, "serve", lambda self: served.append(self))

    platform_module.main()

    assert served == [spec]


class TestPlatform:
    def test_the_child_reads_its_spec_from_standard_input(self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch):
        started: list[tuple[list[str], bytes | None]] = []
        group = SimpleNamespace(start=lambda command, *, stdin=None: started.append((command, stdin)))
        platform = Platform(spec)
        monkeypatch.setattr(platform, "_group", group)

        platform.start()

        assert started == [
            ([platform_module.sys.executable, "-m", "interloper_app.dev"], spec.model_dump_json().encode())
        ]

    def test_a_platform_that_is_not_running_is_never_ready(self, spec: PlatformSpec):
        assert Platform(spec).wait_ready(timeout=5) is False

    def test_the_health_check_never_goes_through_a_proxy(self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch):
        class Health(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200 if self.path == "/api/health" else 404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Health)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        monkeypatch.setenv("http_proxy", "http://proxy.invalid:8080")
        spec.port = server.server_address[1]
        platform = Platform(spec)
        monkeypatch.setattr(platform, "_group", SimpleNamespace(running=True))
        try:
            assert platform.wait_ready(timeout=10) is True
        finally:
            server.shutdown()


class TestServe:
    @staticmethod
    def _serve(
        spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch, declared: list[Any] | None = None
    ) -> tuple[dict[str, Any], dict[str, Any], mock.Mock, SimpleNamespace]:
        """Serve the spec with every outside piece replaced, recording what it was handed.

        Returns:
            What the app was composed with, what the store was built with, the
            store, and the app.
        """
        monkeypatch.setattr(platform_module, "UI_BUNDLES", mock.Mock())
        database: dict[str, Any] = {}
        monkeypatch.setattr(DevDatabase, "bootstrap", lambda self, files=False: database.update(written=files))
        monkeypatch.setattr(DevDatabase, "connect", lambda self: database.update(checked=self.root))
        monkeypatch.setattr(platform_module.Catalog, "from_settings", classmethod(lambda cls: Catalog()))
        store = mock.Mock()
        store.components.apply.return_value = Applied()
        built: dict[str, Any] = {"database": database}

        def build(cls: object, **kwargs: object) -> mock.Mock:
            built.update(kwargs)
            return store

        monkeypatch.setattr(platform_module.Store, "from_settings", classmethod(build))
        monkeypatch.setattr(
            platform_module,
            "Targets",
            lambda root, paths, files: SimpleNamespace(
                classes=[], declares=declared is not None, declarations=lambda *a: declared or []
            ),
        )
        monkeypatch.setattr(platform_module, "configure_console_logging", lambda *args, **kwargs: None)
        monkeypatch.setattr(SignIn, "ensure", lambda self, store: (mock.Mock(), mock.Mock()))
        app = SimpleNamespace(services={}, run=mock.Mock())
        composed: dict[str, Any] = {}

        def compose(*args: object, **kwargs: object) -> SimpleNamespace:
            composed.update(kwargs)
            return app

        monkeypatch.setattr(platform_module.App, "from_settings", compose)
        monkeypatch.setattr(platform_module.sys, "path", list(platform_module.sys.path))
        spec.serve()
        return composed, built, store, app

    def test_it_serves_the_platform_on_the_loopback_with_dev_s_pieces(
        self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch
    ):
        composed, _, _, app = self._serve(spec, monkeypatch)

        assert platform_module.sys.path[0] == "/work/acme"
        assert (ServerSettings.get().host, ServerSettings.get().port) == ("127.0.0.1", 3100)
        assert AuthSettings.get().cookie_secure is False
        overrides = cast(mock.Mock, platform_module.UI_BUNDLES).override
        assert [call.args[0] for call in overrides.call_args_list] == ["acme", "beta"]
        assert (composed["api"], composed["spa"], composed["port"]) == (True, True, 3100)
        assert ServerSettings.get().external_url == "http://127.0.0.1:3100"
        assert composed["dev_origins"] == ["http://127.0.0.1:5173"]
        assert len(composed["routers"]) == 1
        assert "parent-watchdog" in app.services
        app.run.assert_called_once_with()

    def test_without_files_the_app_edits_the_database_dev_brought_up(
        self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch
    ):
        _, built, store, _ = self._serve(spec, monkeypatch)

        assert built["database"] == {"checked": Path("/work/acme")}
        assert built["served_from"] is None
        store.components.apply.assert_not_called()

    def test_files_serve_the_collection_from_a_database_of_their_own(
        self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch
    ):
        declared: list[Any] = [SimpleNamespace(file="collection.yaml", rows=["row"], classes=[])]

        _, built, store, _ = self._serve(spec, monkeypatch, declared)

        assert built["database"] == {"written": True}
        assert built["served_from"] == "collection.yaml"
        store.components.apply.assert_called_once_with(mock.ANY, ["row"])


class TestLifecycle:
    def test_a_restart_stops_the_process_before_starting_another(
        self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch
    ):
        calls: list[str] = []
        group = SimpleNamespace(
            start=lambda command, *, stdin=None: calls.append("start"), stop=lambda: calls.append("stop")
        )
        platform = Platform(spec)
        monkeypatch.setattr(platform, "_group", group)

        platform.restart()

        assert calls == ["stop", "start"]

    def test_a_platform_that_never_answers_is_not_ready(self, spec: PlatformSpec, monkeypatch: pytest.MonkeyPatch):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            spec.port = probe.getsockname()[1]
        platform = Platform(spec)
        monkeypatch.setattr(platform, "_group", SimpleNamespace(running=True))

        assert platform.wait_ready(timeout=0.6) is False

    def test_the_package_runs_the_platform_process(self, monkeypatch: pytest.MonkeyPatch):
        served = mock.Mock()
        monkeypatch.setattr(platform_module, "main", served)

        runpy.run_module("interloper_app.dev", run_name="__main__")

        served.assert_called_once_with()


class TestApply:
    def test_the_files_components_are_written_and_what_changed_is_logged(self, caplog: pytest.LogCaptureFixture):
        store = mock.Mock()
        store.components.apply.return_value = Applied(written=3, created=[uuid4()], deleted=[uuid4(), uuid4()])
        declared: list[Any] = [
            SimpleNamespace(file="collection.yaml", rows=["source", "job"]),
            SimpleNamespace(file="weather.py", rows=["paris"]),
        ]
        caplog.set_level("INFO")

        PlatformSpec._apply(store, ORG, declared, "collection.yaml, weather.py")

        store.components.apply.assert_called_once_with(ORG, ["source", "job", "paris"])
        assert "collection.yaml, weather.py: 3 components, 1 new, 2 removed" in caplog.text

    def test_declarations_the_store_refuses_stop_the_platform_naming_the_files(self):
        store = mock.Mock()
        store.components.apply.side_effect = ValueError("'demo' collides")

        with pytest.raises(SystemExit, match=r"collection\.yaml: 'demo' collides"):
            PlatformSpec._apply(store, ORG, [], "collection.yaml")
