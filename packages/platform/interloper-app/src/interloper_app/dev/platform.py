"""The platform ``interloper dev`` runs, in a process of its own that it restarts on every watched edit.

Python cannot unload what it imported, so a restart is a new process: the
parent keeps the sign-in code and the Vite servers, and hands each new
platform process its `PlatformSpec` as JSON on its standard input. That is
the only channel the dev-server URLs travel: no setting, environment variable
or request can carry one.

Run as ``python -m interloper_app.dev``, which only ``dev`` does.
"""

from __future__ import annotations

import logging
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.component import Component
from interloper.settings import LogSettings, ServerSettings
from interloper.utils.imports import import_from_path
from interloper_api import DevServerUI
from interloper_api.routes.extensions import UI_BUNDLES
from interloper_api.settings import ApiSettings, AuthSettings
from interloper_db import Store
from interloper_db.provision import bootstrap
from interloper_db.settings import PostgresSettings
from interloper_scheduler.settings import SchedulerSettings
from pydantic import BaseModel

from interloper_app.dev.process import ProcessGroup
from interloper_app.dev.signin import SignIn
from interloper_app.services import App, ParentWatchdog

HOST = "127.0.0.1"


class PlatformSpec(BaseModel):
    """What one platform process serves.

    Attributes:
        port: The port it binds on ``127.0.0.1``.
        parent: The ``dev`` process's pid: the platform stops when it is gone.
        python_path: Directories put first on the import path: the project's root.
        classes: Import paths of the component classes added to the catalog.
        sign_in: The sign-in link's route.
        ui_entries: Each held extension UI's entry on its Vite dev server, by
            extension key.
    """

    port: int
    parent: int
    python_path: list[str]
    classes: list[str] = []
    sign_in: SignIn
    ui_entries: dict[str, str] = {}

    @property
    def dev_origins(self) -> list[str]:
        """The Vite dev servers' origins, which the page's import map names the shared modules under.

        Returns:
            Each origin once, in the entries' order.
        """
        origins = (urlsplit(entry) for entry in self.ui_entries.values())
        return list(dict.fromkeys(f"{origin.scheme}://{origin.netloc}" for origin in origins))

    def serve(self) -> None:
        """Run the platform this spec describes, in this process, until a signal, a service's death or dev's.

        A class target that is not a component class exits the process with
        its error before anything is served.
        """
        watchdog = ParentWatchdog(parent=self.parent)
        sys.path[:0] = self.python_path
        configure_console_logging(
            logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=LogSettings.get().format
        )
        ServerSettings.activate(
            ServerSettings.get().model_copy(update={"host": HOST, "port": self.port, "enabled": True})
        )
        # Plain HTTP on the loopback: a cookie marked secure would never come back.
        AuthSettings.activate(AuthSettings.get().model_copy(update={"cookie_secure": False}))
        for key, entry in self.ui_entries.items():
            UI_BUNDLES.override(key, DevServerUI(entry))
        postgres = PostgresSettings.get()
        bootstrap(postgres.dsn, statement_timeout=postgres.statement_timeout, create_tables=False)
        api_settings = ApiSettings.get()
        scheduler_settings = SchedulerSettings.get()
        catalog = Catalog.from_settings().including(self._classes())
        store = Store.from_settings(
            catalog=catalog,
            session_expiry=api_settings.auth.session_expiry,
            access_token_expiry=api_settings.auth.access_token_expiry,
            refresh_token_expiry=api_settings.auth.refresh_token_expiry,
        )
        self.sign_in.ensure(store)
        app = App.from_settings(
            api_settings,
            scheduler_settings,
            store=store,
            catalog=catalog,
            api=True,
            cron=scheduler_settings.cron.enabled,
            worker=scheduler_settings.worker.enabled,
            reaper=scheduler_settings.reaper.enabled,
            dev=False,
            port=self.port,
            routers=[self.sign_in.router()],
            dev_origins=self.dev_origins,
        )
        app.services["parent-watchdog"] = watchdog
        app.run()

    # -- Internals -------------------------------------------------------------

    def _classes(self) -> list[type[Component]]:
        """Import the class targets.

        Returns:
            The component classes, in the targets' order.

        Raises:
            SystemExit: When a target fails to import or names no component class.
        """
        classes: list[type[Component]] = []
        for path in self.classes:
            try:
                loaded = import_from_path(path)
            except (ImportError, AttributeError) as error:
                raise SystemExit(f"Error: cannot import '{path}': {error}") from error
            if not (isinstance(loaded, type) and issubclass(loaded, Component)):
                raise SystemExit(f"Error: '{path}' is not a component class, which a target must be")
            classes.append(loaded)
        return classes


class Platform:
    """The platform process, as the ``dev`` process starts, watches and restarts it."""

    _LOOPBACK = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def __init__(self, spec: PlatformSpec) -> None:
        """Prepare the process; nothing starts until `start`.

        Args:
            spec: What it serves.
        """
        self.spec = spec
        self._group = ProcessGroup()

    @property
    def running(self) -> bool:
        """Whether the process is alive.

        Returns:
            True from `start` until it exits or is stopped.
        """
        return self._group.running

    def start(self) -> None:
        """Start a platform process and hand it its spec."""
        self._group.start([sys.executable, "-m", "interloper_app.dev"], stdin=self.spec.model_dump_json().encode())

    def stop(self) -> None:
        """Stop the process gracefully, killing it when its services do not stop in time."""
        self._group.stop()

    def restart(self) -> None:
        """Replace the process with a new one, which imports every held file afresh."""
        self.stop()
        self.start()

    def wait_ready(self, timeout: float = 120.0) -> bool:
        """Wait until the platform answers its health check, asked directly rather than through a proxy.

        Args:
            timeout: Seconds to wait.

        Returns:
            True once ``GET /api/health`` answers 200; False when the process
            exits first or the timeout passes.
        """
        url = f"http://{HOST}:{self.spec.port}/api/health"
        deadline = time.monotonic() + timeout
        while self.running and time.monotonic() < deadline:
            try:
                with self._LOOPBACK.open(url, timeout=1) as response:
                    if response.status == 200:
                        return True
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(0.25)
        return False


def main() -> None:
    """Serve the spec read from standard input: the platform process's entry point."""
    PlatformSpec.model_validate_json(sys.stdin.read()).serve()
