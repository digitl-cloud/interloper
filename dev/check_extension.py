"""Create an extension the way its author would, then build it and check it.

A fresh ``uv init --lib`` project outside the repository runs the
``interloper extension`` commands of this repository's packages: ``init``,
``add`` of every part, ``build`` against ``@interloper/ui`` packed as npm
publishes it, and ``check``. The wheel it builds must carry a UI that loads
with nothing but the wheel and the platform installed.

``make check-extension`` runs it once the SDK is built. It needs the network
(PyPI and npm). The project is removed when every step passes, and kept, its
path printed, when one fails.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
PLATFORM = REPOSITORY / "packages" / "platform"
SOURCES = ("interloper-core", "interloper-db", "interloper-api", "interloper-scheduler", "interloper-app")
DEPLOYED = ("interloper-core", "interloper-db", "interloper-api")
PARTS = ("source", "connection", "settings", "command", "ui")
LOADS = "from acme_interloper.ui import UI; assert (UI.directory / UI.entry).is_file(), UI.directory"


@dataclass(frozen=True)
class ExtensionCheck:
    """One run of the author's path, in a directory of its own.

    Attributes:
        work: The directory holding the SDK's tarball, the project and the
            environment the wheel is installed into.
    """

    work: Path

    @property
    def project(self) -> Path:
        """The extension's project.

        Returns:
            Its directory.
        """
        return self.work / "acme-interloper"

    def run(self) -> None:
        """Create the extension, grow it with every part, build it and check it."""
        tarball = self._pack_sdk()
        self._create_project()
        self._interloper("init")
        for part in PARTS:
            self._interloper("add", part)
        self._use_sdk(tarball)
        self._interloper("build")
        self._interloper("check")
        self._load_from_wheel()

    def _pack_sdk(self) -> Path:
        """Pack ``@interloper/ui`` as npm publishes it.

        Returns:
            The tarball.
        """
        self._run(["pnpm", "pack", "--pack-destination", str(self.work)], PLATFORM / "interloper-app" / "sdk")
        (tarball,) = self.work.glob("*.tgz")
        return tarball

    def _create_project(self) -> None:
        """Make the library project, its platform packages this repository's, and install ``interloper-app``."""
        self._run(["uv", "init", "--lib", self.project.name, "--vcs", "none"], self.work)
        sources = "".join(f'{name} = {{ path = "{PLATFORM / name}", editable = true }}\n' for name in SOURCES)
        with (self.project / "pyproject.toml").open("a") as pyproject:
            pyproject.write(f"\n[tool.uv.sources]\n{sources}")
        self._run(["uv", "add", "--dev", "interloper-app"], self.project)

    def _interloper(self, *args: str) -> None:
        """Run an ``interloper extension`` command in the project.

        Args:
            *args: The command and its arguments.
        """
        self._run(["uv", "run", "interloper", "extension", *args], self.project)

    def _use_sdk(self, tarball: Path) -> None:
        """Make the UI depend on the packed SDK instead of the published one.

        Args:
            tarball: The packed SDK.
        """
        path = self.project / "ui" / "package.json"
        package = json.loads(path.read_text())
        package["devDependencies"]["@interloper/ui"] = f"file:{tarball}"
        path.write_text(f"{json.dumps(package, indent=2)}\n")

    def _load_from_wheel(self) -> None:
        """Install the wheel beside the platform, outside the project, and load its UI."""
        (wheel,) = (self.project / "dist").glob("*.whl")
        environment = self.work / "deployment"
        self._run(["uv", "venv", str(environment)], self.work)
        python = environment / "bin" / "python"
        self._run(
            ["uv", "pip", "install", "--python", str(python), str(wheel), *(str(PLATFORM / name) for name in DEPLOYED)],
            self.work,
        )
        self._run([str(python), "-c", LOADS], self.work)

    @staticmethod
    def _run(command: list[str], directory: Path) -> None:
        """Run a command, its output shown as it runs, outside any active environment.

        Args:
            command: The command and its arguments.
            directory: Where it runs.
        """
        environment = {name: value for name, value in os.environ.items() if name != "VIRTUAL_ENV"}
        print(f"\n$ {' '.join(command)}", flush=True)
        subprocess.run(command, cwd=directory, env=environment, check=True)


def main() -> None:
    """Run the check in a temporary directory, removed when it passes.

    Raises:
        SystemExit: With status 1 when a step fails.
    """
    work = Path(tempfile.mkdtemp(prefix="interloper-extension-"))
    try:
        ExtensionCheck(work).run()
    except subprocess.CalledProcessError as error:
        print(f"\n{' '.join(error.cmd)} failed; the project is kept in {work}", file=sys.stderr)
        raise SystemExit(1) from error
    shutil.rmtree(work)
    print("\nThe extension was created, built and checked: its wheel carries a UI that loads.")


if __name__ == "__main__":
    main()
