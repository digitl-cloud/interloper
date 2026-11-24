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
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.settings import LogSettings, ServerSettings
from interloper.utils import ReadyLine
from interloper_api import DevServerUI
from interloper_api.routes.extensions import UI_BUNDLES
from interloper_api.settings import ApiSettings, AuthSettings
from interloper_db import Store
from interloper_db.store.components.declaration import Declarations
from interloper_scheduler.settings import SchedulerSettings
from pydantic import BaseModel, Field

from interloper_app.dev.database import DevDatabase
from interloper_app.dev.port import Port
from interloper_app.dev.process import ProcessGroup
from interloper_app.dev.signin import SignIn
from interloper_app.dev.targets import Targets
from interloper_app.services import App, ParentWatchdog

logger = logging.getLogger(__name__)


class PlatformSpec(BaseModel):
    """What one platform process serves.

    Attributes:
        port: The port it binds on ``127.0.0.1``.
        parent: The ``dev`` process's pid: the platform stops when it is gone.
        root: The project root, which declaring files are named relative to;
            put first on the import path.
        targets: Import paths of the component classes added to the catalog
            and of the component instances applied to the dev organisation.
        files: The spec files applied to the dev organisation, absolute.
        sign_in: The sign-in link's route.
        ui_entries: Each held extension UI's entry on its Vite dev server, by
            extension key.
        spa: Whether the API serves the built SPA; off when the Nuxt dev
            server serves the app in front of it.
        public_url: Where the browser reaches the app, ``http://127.0.0.1:<port>``:
            the platform's external URL, which OAuth flows send the browser
            back to, so the session cookie of that host comes along.
        started: When ``dev`` started this process (``time.time()``), which
            the ready line counts from: a restart counts from the edit.
        verbose: Whether the console shows debug lines (``dev -v``).
    """

    port: int
    parent: int
    root: str
    targets: list[str] = []
    files: list[str] = []
    sign_in: SignIn
    ui_entries: dict[str, str] = {}
    spa: bool = True
    public_url: str
    started: float = Field(default_factory=time.time)
    verbose: bool = False

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

        In file mode (an instance target or a spec file), the platform uses a
        database of its own (`DevDatabase`), and before anything is served the
        files' components are written to its dev organisation, which they then
        fill alone, read-only in the app. A target that does not import or
        build exits the process with its error.
        """
        watchdog = ParentWatchdog(parent=self.parent)
        root = Path(self.root)
        sys.path.insert(0, self.root)
        configure_console_logging(logging.DEBUG if self.verbose else logging.INFO, log_format=LogSettings.get().format)
        ServerSettings.activate(
            ServerSettings.get().model_copy(
                update={"host": Port.HOST, "port": self.port, "enabled": True, "external_url": self.public_url}
            )
        )
        # Plain HTTP on the loopback: a cookie marked secure would never come back.
        AuthSettings.activate(AuthSettings.get().model_copy(update={"cookie_secure": False}))
        for key, entry in self.ui_entries.items():
            UI_BUNDLES.override(key, DevServerUI(entry))
        targets = Targets(root, self.targets, self.files)
        database = DevDatabase.from_settings(root)
        # dev brought the app's database up; file mode's own is brought up here.
        if targets.declares:
            database.bootstrap(files=True)
        else:
            database.connect()
        api_settings = ApiSettings.get()
        scheduler_settings = SchedulerSettings.get()
        _, organisation = self.sign_in.ensure(Store.from_settings())
        declared = targets.declarations(organisation.id, Catalog.from_settings().including(targets.classes))
        catalog = Catalog.from_settings().including(
            [*targets.classes, *(cls for declarations in declared for cls in declarations.classes)]
        )
        served_from = ", ".join(declarations.file for declarations in declared) if targets.declares else None
        store = Store.from_settings(
            catalog=catalog,
            session_expiry=api_settings.auth.session_expiry,
            access_token_expiry=api_settings.auth.access_token_expiry,
            refresh_token_expiry=api_settings.auth.refresh_token_expiry,
            served_from=served_from,
        )
        if served_from is not None:
            self._apply(store, organisation.id, declared, served_from)
        app = App.from_settings(
            api_settings,
            scheduler_settings,
            store=store,
            catalog=catalog,
            api=True,
            cron=scheduler_settings.cron.enabled,
            worker=scheduler_settings.worker.enabled,
            reaper=scheduler_settings.reaper.enabled,
            port=self.port,
            spa=self.spa,
            routers=[self.sign_in.router()],
            dev_origins=self.dev_origins,
        )
        link = self.sign_in.link(urlsplit(self.public_url).port or self.port)
        ready = ReadyLine(logger, self.started, list(app.services), len(catalog.components), link)
        app.services["parent-watchdog"] = watchdog
        app.run(ready)

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _apply(store: Store, org_id: UUID, declared: list[Declarations], served_from: str) -> None:
        """Write what the files declare to the dev organisation, logging what changed.

        Args:
            store: The store the rows are written through.
            org_id: The dev organisation.
            declared: What each file declares.
            served_from: The files, as the log names them.

        Raises:
            SystemExit: When the declarations cannot be written.
        """
        try:
            applied = store.components.apply(org_id, [row for declarations in declared for row in declarations.rows])
        except Exception as error:
            raise SystemExit(f"Error: {served_from}: {error}") from error
        changes = {"new": applied.created, "removed": applied.deleted}
        summary = "".join(f", {len(ids)} {verb}" for verb, ids in changes.items() if ids)
        logger.info("%s: %d components%s", served_from, applied.written, summary)


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
        """Start a platform process and hand it its spec, stamped with when it started."""
        self.spec.started = time.time()
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
        url = f"{Port(self.spec.port).origin}/api/health"
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
