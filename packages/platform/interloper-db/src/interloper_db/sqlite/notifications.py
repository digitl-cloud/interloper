"""The change notifications migration 0002 sends on Postgres, as SQLite triggers."""

from __future__ import annotations

from collections.abc import Iterator
from typing import NamedTuple

from sqlalchemy import JSON, Table
from sqlmodel import SQLModel


class Notification(NamedTuple):
    """How one table's changes are notified, as 0002's trigger on it notifies them.

    Attributes:
        operations: The operations notified (``INSERT``, ``UPDATE``, ``DELETE``).
        columns: The columns the record carries; every column when ``None``.
        target: Whether the record also names the row's component: its kind,
            key and name.
        update_when: The condition an update must meet to be notified;
            every update when ``None``.
    """

    operations: tuple[str, ...]
    columns: tuple[str, ...] | None = None
    target: bool = False
    update_when: str | None = None


class SQLiteNotifications:
    """0002's notifying triggers on SQLite: the same tables, conditions and payload, sent with ``notify()``.

    Each record is built from the table's columns, so it follows the models.
    """

    def statements(self) -> Iterator[str]:
        """The statements that (re)create the notifying triggers, each dropped first.

        Yields:
            One DDL statement at a time.
        """
        for name, definition in self.triggers().items():
            yield f"DROP TRIGGER IF EXISTS {name}"
            yield definition

    def notifications(self) -> dict[str, Notification]:
        """Each notifying table's notification, as 0002 sets them.

        Returns:
            The notification of each table, keyed by table name.
        """
        return {
            "backfills": Notification(("INSERT", "UPDATE"), target=True),
            "component_relations": Notification(("INSERT", "UPDATE", "DELETE")),
            "components": Notification(("INSERT", "UPDATE", "DELETE"), columns=("id", "kind", "parent_id")),
            "events": Notification(("INSERT",)),
            "executions": Notification(("INSERT", "UPDATE"), update_when=self._changed("executions")),
            "runs": Notification(
                ("INSERT", "UPDATE"),
                target=True,
                update_when="OLD.heartbeat_at IS NEW.heartbeat_at OR OLD.status IS NOT NEW.status",
            ),
        }

    def triggers(self) -> dict[str, str]:
        """Each notifying trigger's definition, one per table and operation, by name.

        Returns:
            The ``CREATE TRIGGER`` statement of each trigger, keyed by its name.
        """
        triggers = {}
        for table_name, notification in self.notifications().items():
            table = SQLModel.metadata.tables[table_name]
            for operation in notification.operations:
                row = "OLD" if operation == "DELETE" else "NEW"
                name = f"trg_{table_name}_notify_{operation.lower()}"
                condition = notification.update_when if operation == "UPDATE" else None
                when = f"\nWHEN {condition}" if condition else ""
                payload = (
                    f"json_object('table', '{table_name}', 'op', '{operation}', 'org_id', {row}.org_id, "
                    f"'record', {self._record(table, row, notification)})"
                )
                triggers[name] = (
                    f"CREATE TRIGGER {name}\nAFTER {operation} ON {table_name}{when}\nBEGIN\n"
                    f"    SELECT notify({payload});\nEND"
                )
        return triggers

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _record(table: Table, row: str, notification: Notification) -> str:
        """Render the record a change carries: its columns, JSON ones nested, and its component when targeted.

        Args:
            table: The table changed.
            row: ``NEW``, or ``OLD`` for a delete.
            notification: The table's notification.

        Returns:
            The ``json_object`` expression.
        """
        names = notification.columns or tuple(table.columns.keys())
        pairs = [
            f"'{name}', json({row}.{name})" if isinstance(table.c[name].type, JSON) else f"'{name}', {row}.{name}"
            for name in names
        ]
        if notification.target:
            pairs += [
                f"'component_{field}', (SELECT {field} FROM components WHERE id = {row}.component_id)"
                for field in ("kind", "key", "name")
            ]
        return f"json_object({', '.join(pairs)})"

    @staticmethod
    def _changed(table_name: str) -> str:
        """Render Postgres's ``OLD.* IS DISTINCT FROM NEW.*`` for a table: any of its columns changed.

        Args:
            table_name: The table updated.

        Returns:
            The condition.
        """
        columns = SQLModel.metadata.tables[table_name].columns.keys()
        return " OR ".join(f"OLD.{name} IS NOT NEW.{name}" for name in columns)
