"""The database ``interloper dev`` keeps its state in."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import ClassVar

from interloper_db.provision import bootstrap, connect
from interloper_db.settings import PostgresSettings
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError


class DevDatabase:
    """The database ``dev`` runs on: the configured Postgres, else SQLite files in the project's `FOLDER`.

    File mode keeps a database of its own, so the files' instances never touch
    the app's: ``files.db`` beside ``interloper.db``, or on Postgres one beside
    the configured database, one per checkout.
    """

    FOLDER: ClassVar[str] = ".interloper"

    def __init__(self, root: Path, postgres: PostgresSettings) -> None:
        """Hold the project and the Postgres settings the choice is made from.

        Args:
            root: The project root, which holds `FOLDER`.
            postgres: The Postgres settings this process runs with.
        """
        self.root = root
        self.postgres = postgres

    @classmethod
    def from_settings(cls, root: Path) -> DevDatabase:
        """The project's database, from the Postgres settings this process runs with.

        Args:
            root: The project root.

        Returns:
            The database.
        """
        return cls(root, PostgresSettings.get())

    def url(self, *, files: bool = False) -> str:
        """The URL of the collection made in the app, or of file mode's.

        Args:
            files: Whether it is file mode's.

        Returns:
            The configured Postgres's URL (file mode's database beside it), or a SQLite file's.
        """
        if not self.postgres.configured:
            return f"sqlite:///{self.root / self.FOLDER / ('files.db' if files else 'interloper.db')}"
        if not files:
            return self.postgres.dsn
        return self.postgres.model_copy(update={"database": self._files_database()}).dsn

    def bootstrap(self, *, files: bool = False) -> Engine:
        """Bring the database up and bind the process engine to it.

        Args:
            files: Whether it is file mode's.

        Returns:
            The bound engine.

        Raises:
            SystemExit: When the database cannot be reached or opened, naming where it was looked for.
        """
        if not self.postgres.configured:
            self._create_folder()
        try:
            return bootstrap(self.url(files=files), statement_timeout=self.postgres.statement_timeout)
        except OperationalError as error:
            if not self.postgres.configured:
                raise SystemExit(
                    f"Error: the SQLite database in {self.root / self.FOLDER} cannot be opened.\n{error.orig}"
                ) from error
            raise SystemExit(
                f"Error: Postgres at {self.postgres.host}:{self.postgres.port} (database '{self.postgres.database}') "
                "does not answer. Start it, point the postgres section of interloper.yaml at one you run, or remove "
                f"the section to keep dev's state in {self.FOLDER}/.\n{error.orig}"
            ) from error

    def connect(self) -> Engine:
        """Bind the process engine to the collection made in the app, which ``dev`` brought up.

        Returns:
            The bound engine.
        """
        return connect(self.url(), statement_timeout=self.postgres.statement_timeout)

    # -- Internals -------------------------------------------------------------

    def _create_folder(self) -> None:
        """Create `FOLDER` holding a ``.gitignore`` of ``*``, as uv does ``.venv``, so no checkout picks it up."""
        folder = self.root / self.FOLDER
        folder.mkdir(parents=True, exist_ok=True)
        ignore = folder / ".gitignore"
        if not ignore.exists():
            ignore.write_text("*\n", encoding="utf-8")

    def _files_database(self) -> str:
        """Name file mode's Postgres database: one per checkout, beside the configured one.

        Returns:
            The configured database's name, cut so the whole stays within
            Postgres's 63-byte identifiers, suffixed with a digest of the
            project root.
        """
        return f"{self.postgres.database[:48]}_files_{hashlib.sha256(str(self.root).encode()).hexdigest()[:8]}"
