# Extensions & registries

Every package says what it brings in one YAML manifest, `extension.yaml`, at the root of its
import package. Installing the package is all the registration there is: no import-order
dependence, no registration calls, and discovery works in every process where the package is
present.

## The manifest

A package names its import package under the `interloper.extensions` entry point, the one entry
point interloper reads, and ships the manifest inside that package so it lands in the wheel:

```toml
[project.entry-points."interloper.extensions"]
acme = "acme_interloper"
```

```yaml
# acme_interloper/extension.yaml
manifest_version: 1
key: acme
name: Acme
description: "Acme sources, a Polars representation and a Ray runner."
components: acme_interloper
representations:
  - acme_interloper.polars:POLARS_REPRESENTATION
oauth_providers:
  - acme_interloper.oauth:ACME
runners:
  ray: acme_interloper.runner:RayRunner
```

Five keys say what the package is:

| Key | Meaning |
|-----|---------|
| `manifest_version` | `1`, required. |
| `key` | The extension's identifier, required: lowercase letters, digits and underscores, starting with a letter, and unique among the installed packages. |
| `name` | Its display name. |
| `description` | One sentence on what it brings. |
| `icon` | Its icon. |

Its version is its distribution's. Every other top-level key is a **section**. A section is read
by the registry built for it, wherever that registry lives: core knows nothing of the sections
the platform reads, and a section no registry reads is ignored. Name a section an extension of
yours reads after your extension's key (`acme_widgets`), so it can never collide with another.

A section holds import strings, `module` or `module:attribute`, in one of three shapes:

- a **map** of names to import strings, for a registry that looks entries up by the name you
  declare (`runners`, `launchers`, `cli`, `settings`);
- a **list** of import strings, for a registry that keys an entry by the object itself
  (`kinds`, `representations`, `oauth_providers`, `components`);
- a **single** import string, named by the extension's key.

Nothing is imported when the manifest is read: an entry is imported when its registry first
needs it.

## The sections

| Section | Declares | Entry | Read by |
|---------|----------|-------|---------|
| `kinds` | a component kind | the anchor class | `il.KINDS`; catalog validation |
| `components` | component classes | a class, or a module whose component classes are all declared | `il.Catalog.discover()` |
| `representations` | a table representation | an instance or class | `TableData.from_data()`, `il.Upstream.rows` |
| `runners` | a runner | the class | `il.Runner.from_settings()`, `interloper.yaml` `runner.type` |
| `oauth_providers` | an OAuth provider | an instance or class | `interloper.oauth.PROVIDERS`, `OAuthConfig` |
| `cli` | an `interloper` command | the `Command` class | the `interloper` CLI |
| `launchers` | a launcher | the class | the scheduler's queue worker |
| `settings` | a settings section | the `Settings` class | its own `get()`; the admin configuration page; the settings reference |

Core declares its own kinds, jobs, hooks, runners, representation, providers, commands and
settings in its own manifest, the same way.

## Settings

A `settings` entry names a subclass of `interloper.settings.Settings`, and the entry's name is
the section's: its block of `interloper.yaml` and the prefix of its environment variables.

```yaml
settings:
  acme_widgets: acme_interloper.settings:WidgetSettings
```

```py
from typing import ClassVar

from interloper.settings import Settings
from pydantic import Field


class WidgetSettings(Settings):
    """Widget settings."""

    public: ClassVar[frozenset[str]] = frozenset({"size"})

    size: int = Field(default=1, description="Widget size.")
    token: str = Field(default="", description="Widget service token.")
```

`WidgetSettings.get()` is the section this process runs with, loaded on the first read from
`INTERLOPER_ACME_WIDGETS_SIZE` and the `acme_widgets` block, the environment winning. The class
finds its name by looking up its own import path, so declare it by the module that defines it.
Two packages declaring the same section, or one class declared under two names, fail every load
with an error naming both.

Each field's `description` is the settings reference's copy. `public` names the fields whose
values the admin configuration page shows; every other field shows only whether the deployment
set it, so a secret never leaves the process and a new field stays hidden until listed. A dotted
name (`config.image`) shows one key of a mapping field.

## Commands

A `cli` entry names a subclass of `interloper.cli.Command`, and the entry's name is the
command's:

```yaml
cli:
  backup: acme_interloper.cli:BackupCommand
```

```py
import argparse

from interloper.cli import Command


class BackupCommand(Command):
    """Back up the database.

    Writes a dump of every table to the given directory.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("directory", help="Where the dump is written")

    def run(self, args: argparse.Namespace) -> None:
        ...
```

The docstring is the command's copy: its first line is the help `interloper --help` lists, the
whole docstring the description `interloper backup --help` shows. `add_arguments` declares the
arguments on the command's `argparse` parser and `run` receives them parsed. Keep heavy imports
inside `run`: `interloper --help` builds every command.

A group lists its commands, each named the same way, and prints its help when named alone:

```py
from collections.abc import Mapping
from typing import ClassVar


class StorageCommand(Command):
    """Storage operations."""

    commands: ClassVar[Mapping[str, type[Command]]] = {"backup": BackupCommand, "restore": RestoreCommand}
```

## When something does not load

One package never takes the others down. A manifest that cannot be read, a section of another
shape and an entry that fails to import are each skipped with a warning naming the package,
section and entry, and recorded in `interloper.extension.MANIFESTS.failures`. When two packages
declare the same name in a section, the first one found wins and the other is skipped with a
warning.

`interloper extensions check` imports every entry of every installed manifest and names each
manifest, section or entry that fails, and each section no registry reads, which is usually a
misspelt section name. It exits non-zero when it names anything:

```sh
$ interloper extensions check
package 'acme_interloper', section 'runner': no registry reads this section
Checked 48 entries in 8 sections: 1 problem(s).
```

## The registry primitive

Every registry is an instance of `il.Registry`: a lazily-populated name-to-object mapping,
optionally fed by one manifest section:

```py
import interloper as il
from interloper.oauth import PROVIDERS
from interloper.representation import REPRESENTATIONS
from interloper.runner import RUNNERS

il.KINDS["connection"]          # the anchor class; KeyError with the available names on a miss
PROVIDERS.get("google")         # None on a miss
"dataframe" in REPRESENTATIONS
RUNNERS.keys(), RUNNERS.items(), len(RUNNERS)
```

A package that wants extensions of its own builds one, naming its section:

```py
from interloper.registry import Registry

WIDGETS: Registry[type[Widget]] = Registry("acme_widgets")
```

Registration is first-wins and idempotent. Loading happens once, on first lookup, serialized
across threads so concurrent first lookups never observe a half-populated registry, and a failed
read retries on the next lookup. An `adopt` transform lets a registry key entries by the object's
own `key` rather than the declared name, which is why providers and representations are found
under their declared keys.

Code can also register explicitly, for tests or dynamic composition:

```py
from interloper.runner import RUNNERS
RUNNERS.register("fake", FakeRunner)
```

## Kinds versus components

`kinds` says what kinds exist; `components` says what classes exist. Kind anchors are framework,
not content: they live in `KINDS` and never appear in a catalog. A component class whose kind has
no anchor makes catalog construction fail with `ConfigError`.

## Narrowing

Declared universe and enabled catalog are different things. Installation declares; the
`catalog.include` setting enables a subset, closed over dependencies. See
[Catalog](../guide/catalog.md#building-a-catalog).
