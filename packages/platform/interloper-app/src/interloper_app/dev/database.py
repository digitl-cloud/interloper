"""The database ``interloper dev`` keeps its state in."""

from __future__ import annotations

import hashlib
import logging
import sys
from pathlib import Path
from typing import ClassVar

from interloper_db.engine import get_engine
from interloper_db.errors import SQLiteRevisionError
from interloper_db.provision import bootstrap, connect
from interloper_db.settings import PostgresSettings
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


class DevDatabase:
    """The database ``dev`` runs on: the configured Postgres, else SQLite files in the project's `FOLDER`.

    File mode keeps a database of its own, so the files' instances never touch
    the app's: ``files.db`` beside ``interloper.db``, or on Postgres one beside
    the configured database, one per checkout.

    A SQLite file is never migrated: one at another revision than this
    release's is deleted and made anew on the person's word, asked on the
    terminal ``dev`` runs in, and stops ``dev`` otherwise.
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

    def path(self, *, files: bool = False) -> Path:
        """The SQLite file of the collection made in the app, or of file mode's, which serves when no Postgres does.

        Args:
            files: Whether it is file mode's.

        Returns:
            ``interloper.db`` or ``files.db`` in the project's `FOLDER`.
        """
        return self.root / self.FOLDER / ("files.db" if files else "interloper.db")

    def url(self, *, files: bool = False) -> str:
        """The URL of the collection made in the app, or of file mode's.

        Args:
            files: Whether it is file mode's.

        Returns:
            The configured Postgres's URL (file mode's database beside it), or a SQLite file's.
        """
        if not self.postgres.configured:
            return f"sqlite:///{self.path(files=files)}"
        if not files:
            return self.postgres.dsn
        return self.postgres.model_copy(update={"database": self._files_database()}).dsn

    def prepare(self) -> None:
        """Bring up, before anything is served, every database ``dev`` keeps: the app's, and file mode's SQLite file.

        File mode's is brought up only when its file exists: the platform
        process brings it up again in file mode, but has no terminal to ask
        on, so a file at another revision is recreated here, where the person
        still can be asked. This process holds no connection afterwards.
        """
        self.bootstrap().dispose()
        if not self.postgres.configured and self.path(files=True).exists():
            self.bootstrap(files=True).dispose()

    def bootstrap(self, *, files: bool = False) -> Engine:
        """Bring the database up and bind the process engine to it.

        A SQLite file at another revision than this release's is deleted and
        brought up anew when the person, asked on the terminal, says so.

        Args:
            files: Whether it is file mode's.

        Returns:
            The bound engine.

        Raises:
            SystemExit: When the database cannot be reached or opened, naming where it was looked for, or is a SQLite
                file at another revision that is kept, saying how to start afresh.
        """
        if not self.postgres.configured:
            self._create_folder()
        try:
            return self._bring_up(files)
        except SQLiteRevisionError as error:
            if not self._delete_if_confirmed(error):
                raise SystemExit(f"Error: {error}") from error
        return self._bring_up(files)

    def connect(self) -> Engine:
        """Bind the process engine to the collection made in the app, which ``dev`` brought up.

        Returns:
            The bound engine.
        """
        return connect(self.url(), statement_timeout=self.postgres.statement_timeout)

    # -- Internals -------------------------------------------------------------

    def _bring_up(self, files: bool) -> Engine:
        """Bring the database up and bind the process engine to it, or say where it was looked for.

        Args:
            files: Whether it is file mode's.

        Returns:
            The bound engine.

        Raises:
            SystemExit: When the database cannot be reached or opened, naming where it was looked for.
        """
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

    @staticmethod
    def _delete_if_confirmed(error: SQLiteRevisionError) -> bool:
        """Ask on the terminal whether to delete a SQLite file at another revision, with its WAL, and delete it on yes.

        Args:
            error: The refusal, naming the file and both revisions.

        Returns:
            Whether the file was deleted: never without a terminal to ask on.
        """
        if not sys.stdin.isatty():
            return False
        try:
            answer = input(
                f"The SQLite database {error.path} is at revision {error.revision}, and this release needs "
                f"{error.head}. SQLite databases are never migrated: starting afresh deletes it, and with it the "
                "components made in the app and the run history. What a spec file or a target declares is applied "
                "again on start.\nDelete it and start afresh? [y/N] "
            )
        except (EOFError, KeyboardInterrupt):
            print()
            return False
        if answer.strip().lower() not in ("y", "yes"):
            return False
        get_engine().dispose()
        for suffix in ("", "-wal", "-shm"):
            error.path.with_name(error.path.name + suffix).unlink(missing_ok=True)
        logger.info("Deleted %s: starting afresh", error.path)
        return True

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
