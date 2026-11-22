"""``interloper extension build``: the UI, then the sdist and the wheel that carry it."""

from __future__ import annotations

import argparse

from interloper_api.ui import VITE_MANIFEST

from interloper_app.extension.command import ProjectCommand
from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.project import Project


class BuildCommand(ProjectCommand):
    """Build the extension here: its UI, then its sdist and wheel in dist/.

    With a UI, it installs the UI's dependencies with npm (npm ci when ui/package-lock.json pins them), type-checks
    the UI and builds it into the package's static/, then checks that the wheel carries the build. Without one, it
    needs no Node. Either way uv build writes the sdist, and the wheel built from it, into dist/, replacing the
    files of the same name; a wheel that fails the check never reaches dist/.
    """

    def execute(self, project: Project, args: argparse.Namespace) -> None:
        """Build the UI when the manifest declares one, then the sdist and the wheel.

        Args:
            project: The project in the current directory.
            args: The parsed command line; the command takes no arguments.

        Raises:
            ExtensionError: If the project is not an extension yet, a step
                fails, or the wheel lacks the UI's Vite manifest.
        """
        if not project.declares_extension:
            raise ExtensionError(
                f"{project.distribution} is not an extension yet: run uv run interloper extension init first"
            )
        has_ui = "ui" in project.read_manifest().document
        if has_ui:
            project.build_ui()
        built = project.build(carrying=f"{project.package}/static/{VITE_MANIFEST}" if has_ui else None)
        print(f"Built {project.relative(built)}.")
