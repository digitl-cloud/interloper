"""Demote backfill_id and partition_or_window on events to ``data``.

Neither column earned its place: nothing anywhere read ``backfill_id``
(backfill scoping goes through ``runs``), and ``partition_or_window`` was
never filtered or rendered and always equals the run's ``partition_date``
for persisted events. Both match the profile of the ``data`` JSONB column,
where the metadata spill now carries them; existing values are folded into
``data`` before the columns drop so history is preserved.

Idempotent: fresh databases get the columnless shape from ``create_all()``,
so every step no-ops when the columns are already gone.

Revision ID: 006
Revises: 005
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("events")}

    if "backfill_id" in columns:
        op.execute(
            """
            UPDATE events
            SET data = coalesce(data, '{}'::jsonb) || jsonb_build_object('backfill_id', backfill_id::text)
            WHERE backfill_id IS NOT NULL
            """
        )
        op.drop_column("events", "backfill_id")

    if "partition_or_window" in columns:
        op.execute(
            """
            UPDATE events
            SET data = coalesce(data, '{}'::jsonb) || jsonb_build_object('partition_or_window', partition_or_window)
            WHERE partition_or_window IS NOT NULL
            """
        )
        op.drop_column("events", "partition_or_window")


def downgrade() -> None:
    op.add_column("events", sa.Column("partition_or_window", sa.String(), nullable=True))
    op.add_column("events", sa.Column("backfill_id", sa.Uuid(), sa.ForeignKey("backfills.id"), nullable=True))
    op.execute(
        """
        UPDATE events
        SET backfill_id = (data ->> 'backfill_id')::uuid,
            partition_or_window = data ->> 'partition_or_window',
            data = nullif(data - 'backfill_id' - 'partition_or_window', '{}'::jsonb)
        WHERE data ?| array['backfill_id', 'partition_or_window']
        """
    )
