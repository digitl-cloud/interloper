"""``interloper extensions check``: load everything the installed packages' manifests declare.

Every entry of every section is imported, which also imports the module of
each registry the entries register into, so the sections no registry claimed
afterwards are the ones nothing in this installation reads: a misspelt
section name most often.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from typing import ClassVar

from interloper.cli.command import Command


class ExtensionsCheckCommand(Command):
    """Load every declared entry and name what fails or what nothing reads.

    Reads every installed package's extension.yaml and imports every entry it declares. Names each manifest,
    section or entry that cannot be read or imported, and each section no registry reads, which is usually a
    misspelt section name. The exit status is non-zero when anything is named.
    """

    def run(self, args: argparse.Namespace) -> None:
        """Import every declared entry and print what fails or what no registry reads.

        The manifests and sections skipped while reading come from
        `Manifests.failures`; every entry is imported here, whether or not a
        registry in this process has loaded it yet.

        Args:
            args: The parsed command line; the check takes no arguments.

        Raises:
            SystemExit: With status 1 when anything is named.
        """
        from interloper.extension import MANIFESTS

        problems: list[str] = []
        for failure in MANIFESTS.failures:
            if failure.entry is None:
                section = f", section '{failure.section}'" if failure.section else ""
                problems.append(f"package '{failure.package}'{section}: {failure.error}")
        entries = [entry for section in MANIFESTS.sections() for entry in MANIFESTS.entries(section)]
        for entry in entries:
            try:
                entry.load()
            except Exception as error:  # noqa: BLE001 - importing an entry may raise anything, and each is reported
                problems.append(
                    f"package '{entry.package}', section '{entry.section}', entry '{entry.name}' ({entry.path}): "
                    f"{type(error).__name__}: {error}"
                )
        problems.extend(
            f"package '{package}', section '{section}': no registry reads this section"
            for package, section in MANIFESTS.unclaimed()
        )
        for problem in problems:
            print(problem)
        verdict = f"{len(problems)} problem(s)" if problems else "no problems"
        print(f"Checked {len(entries)} entries in {len(MANIFESTS.sections())} sections: {verdict}.")
        if problems:
            raise SystemExit(1)


class ExtensionsCommand(Command):
    """Inspect the installed packages' extension manifests (extension.yaml)."""

    commands: ClassVar[Mapping[str, type[Command]]] = {"check": ExtensionsCheckCommand}
