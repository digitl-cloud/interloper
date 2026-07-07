"""Realtime notifications for components and their relations.

Extends the ``table_changes`` fan-out (003) to the component domain so the
UI stops polling for component edits.

Component rows carry unbounded payloads (JSONB config, encrypted blobs)
while ``pg_notify`` caps payloads at ~8KB — so ``components`` notifies with
a slim record (``id``/``kind``/``parent_id``; subscribers refetch through
the API, which also keeps secret payloads off the notification channel).
Relation rows are small and bounded, so they reuse the generic full-record
function.

Revision ID: 005
Revises: 004
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "005"
down_revision: str | None = "004"
branch_labels: str | None = None
depends_on: str | None = None

_SLIM_FUNCTION = """
CREATE OR REPLACE FUNCTION notify_component_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    payload jsonb;
    rec RECORD;
BEGIN
    rec := COALESCE(NEW, OLD);
    payload := jsonb_build_object(
        'table', TG_TABLE_NAME,
        'op', TG_OP,
        'org_id', rec.org_id,
        'record', jsonb_build_object('id', rec.id, 'kind', rec.kind, 'parent_id', rec.parent_id)
    );
    PERFORM pg_notify('table_changes', payload::text);
    RETURN rec;
END;
$$
"""


def upgrade() -> None:
    """Create the slim notify function and the component-domain triggers."""
    op.execute(_SLIM_FUNCTION)
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_components_notify "
        "AFTER INSERT OR UPDATE OR DELETE ON components "
        "FOR EACH ROW EXECUTE FUNCTION notify_component_change()"
    )
    op.execute(
        "CREATE OR REPLACE TRIGGER trg_component_relations_notify "
        "AFTER INSERT OR UPDATE OR DELETE ON component_relations "
        "FOR EACH ROW EXECUTE FUNCTION notify_table_change()"
    )


def downgrade() -> None:
    """Drop the component-domain triggers and the slim function."""
    op.execute("DROP TRIGGER IF EXISTS trg_components_notify ON components")
    op.execute("DROP TRIGGER IF EXISTS trg_component_relations_notify ON component_relations")
    op.execute("DROP FUNCTION IF EXISTS notify_component_change()")
