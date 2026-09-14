"""Run and backfill notifications carry their target's identity.

The runs and backfills APIs resolve the target component on the record
(``component_kind``/``component_key``/``component_name``) while the realtime
payload for the same rows was the bare row, so a client had to refetch or
show a placeholder until it did. ``notify_target_change`` joins the target
into the record, giving both paths one shape. The identity fields null
together exactly when the target is gone: deletion nulls ``component_id``
(``ON DELETE SET NULL``), and that update fires this same trigger.

Revision ID: 019
Revises: 018
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "019"
down_revision: str | None = "018"
branch_labels: str | None = None
depends_on: str | None = None

_TRIGGERS = [("trg_runs_notify", "runs"), ("trg_backfills_notify", "backfills")]

_NOTIFY_TARGET_FN = """
CREATE OR REPLACE FUNCTION notify_target_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_kind text;
    target_key text;
    target_name text;
BEGIN
    SELECT kind, key, name INTO target_kind, target_key, target_name
    FROM components WHERE id = NEW.component_id;
    PERFORM pg_notify('table_changes', jsonb_build_object(
        'table', TG_TABLE_NAME,
        'op', TG_OP,
        'org_id', NEW.org_id,
        'record', to_jsonb(NEW) || jsonb_build_object(
            'component_kind', target_kind,
            'component_key', target_key,
            'component_name', target_name
        )
    )::text);
    RETURN NEW;
END;
$$
"""


def _bind_triggers(function: str) -> None:
    """Point the run and backfill notify triggers at a notify function.

    Args:
        function: The trigger function to execute on INSERT or UPDATE.
    """
    for trigger, table in _TRIGGERS:
        op.execute(
            f"CREATE OR REPLACE TRIGGER {trigger} "
            f"AFTER INSERT OR UPDATE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {function}()"
        )


def upgrade() -> None:
    op.execute(_NOTIFY_TARGET_FN)
    _bind_triggers("notify_target_change")


def downgrade() -> None:
    _bind_triggers("notify_table_change")
    op.execute("DROP FUNCTION IF EXISTS notify_target_change()")
