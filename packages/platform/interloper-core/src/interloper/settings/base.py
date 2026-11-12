"""Settings sections: blocks of the config file and the environment, each declared by the package it configures.

A package declares its sections in its manifest's ``settings`` section, a map
of section names to the import paths of their classes:

```yaml
settings:
  postgres: interloper_db.settings:PostgresSettings
```

The manifest names the section, and the class finds its name by looking its
own import path up there, so it is declared by the module that defines it. A
section is declared once: a second package declaring the same name, or one
class declared under two names, fails every load with a `ConfigError` naming
both.

A section loads from three sources, highest first: the environment
(``INTERLOPER_<SECTION>_<FIELD>``), the section's block of ``interloper.yaml``
in the current directory, and the field defaults, so the environment beats
the file for every field. One file serves every role, so a block no installed
package declares is ignored, while an unknown field inside a declared block
is an error.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, ClassVar, Self

import yaml
from pydantic_settings import BaseSettings, EnvSettingsSource, InitSettingsSource, PydanticBaseSettingsSource

from interloper.errors import ConfigError
from interloper.extension import MANIFESTS, ExtensionEntry
from interloper.registry import Registry

CONFIG_FILE = "interloper.yaml"
PREFIX = "INTERLOPER_"
_SECTION_NAME = re.compile(r"[a-z][a-z0-9_]*")


class Settings(BaseSettings):
    """One settings section: its fields, where they load from, and the instance this process runs with.

    A subclass is a section. Every field's ``description`` is the settings
    reference the documentation renders, so it is user-facing copy, and
    `public` names the fields whose values the admin configuration page
    shows; every other field shows only whether the deployment set it. A
    dotted name (``config.image``) reaches into a mapping field.
    """

    public: ClassVar[frozenset[str]] = frozenset()

    _active: ClassVar[dict[type[Settings], Settings]] = {}

    # -- Declaration -----------------------------------------------------------

    @classmethod
    def declared(cls) -> dict[str, ExtensionEntry]:
        """Every section the installed manifests declare, by name.

        Returns:
            The manifest entry of each section, keyed by section name.

        Raises:
            ConfigError: When a section name is not lowercase letters, digits
                and underscores, when two packages declare the same section,
                or when one class is declared under two names.
        """
        by_name: dict[str, ExtensionEntry] = {}
        by_path: dict[str, ExtensionEntry] = {}
        for entry in MANIFESTS.entries("settings"):
            if not _SECTION_NAME.fullmatch(entry.name):
                raise ConfigError(
                    f"Settings section '{entry.name}' of package '{entry.package}' must be lowercase letters, digits "
                    "and underscores, starting with a letter"
                )
            if (other := by_name.get(entry.name)) is not None:
                raise ConfigError(
                    f"Settings section '{entry.name}' is declared by package '{other.package}' and by package "
                    f"'{entry.package}'; a section is declared once"
                )
            if (other := by_path.get(entry.path)) is not None:
                raise ConfigError(
                    f"{entry.path} is declared as settings section '{other.name}' and as '{entry.name}'; a class "
                    "is declared under one name"
                )
            by_name[entry.name] = by_path[entry.path] = entry
        return by_name

    @classmethod
    def section(cls) -> str:
        """The name this class is declared under, which its config block and environment variables carry.

        Returns:
            The section name.

        Raises:
            ConfigError: When no manifest declares this class by the module
                that defines it.
        """
        path = f"{cls.__module__}:{cls.__qualname__}"
        for name, entry in cls.declared().items():
            if entry.path == path:
                return name
        raise ConfigError(
            f"{path} is declared in no package manifest's 'settings' section; declare it there by the module that "
            "defines it"
        )

    # -- Sources ---------------------------------------------------------------

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        **kwargs: Any,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Load the section from its init values, then its environment variables, then its config block.

        Args:
            settings_cls: The section being built.
            **kwargs: Pydantic's default sources, of which ``init_settings``
                is kept.

        Returns:
            The sources, highest priority first.
        """
        name = cls.section()
        return (
            kwargs["init_settings"],
            EnvSettingsSource(settings_cls, env_prefix=f"{PREFIX}{name.upper()}_"),
            InitSettingsSource(settings_cls, init_kwargs=cls._config_block(name)),
        )

    @classmethod
    def _config_block(cls, name: str) -> dict[str, Any]:
        """Read the section's block of the config file in the current directory.

        Args:
            name: The section name, the block's top-level key.

        Returns:
            The block's fields, empty when there is no file, no block, or an
            empty one.

        Raises:
            ConfigError: When the file is not a mapping of sections, or the
                block is not a mapping of fields.
        """
        path = Path(CONFIG_FILE)
        if not path.is_file():
            return {}
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(document, dict):
            raise ConfigError(f"{CONFIG_FILE} must be a mapping of settings sections")
        block = document.get(name)
        if block is None:
            return {}
        if not isinstance(block, dict):
            raise ConfigError(f"Section '{name}' of {CONFIG_FILE} must be a mapping of fields")
        return block

    # -- Active instance -------------------------------------------------------

    @classmethod
    def get(cls) -> Self:
        """The section this process runs with: the one activated, else loaded on this first read and activated.

        Returns:
            The section.
        """
        active = Settings._active.get(cls)
        if not isinstance(active, cls):
            active = cls()
            Settings._active[cls] = active
        return active

    @classmethod
    def activate(cls, settings: Self) -> None:
        """Make ``settings`` the section every later `get` returns, as a command applying its flags does.

        Args:
            settings: The section to run with.
        """
        Settings._active[cls] = settings

    @classmethod
    def clear_active(cls) -> None:
        """Forget the active section of this class and its subclasses, so the next `get` loads it again.

        Called on `Settings` itself, it forgets every section, as a command
        does when it ends.
        """
        for section in [section for section in Settings._active if issubclass(section, cls)]:
            del Settings._active[section]


SETTINGS: Registry[type[Settings]] = Registry("settings")
