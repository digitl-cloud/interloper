"""Rename persisted job rows to the concrete cron_job catalog key.

``Job`` is now the abstract anchor of the ``job`` kind; the concrete,
catalog-listed class is ``CronJob`` (key ``cron_job``, contributed through
the ``interloper.jobs`` entry-point group). Existing job rows were created
before the split and all carry cron schedules, so they are re-keyed in
place — history is facts, rows keep their ids.

Revision ID: 007
Revises: 006
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "007"
down_revision: str | None = "006"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Re-key job rows to the concrete class."""
    op.execute("UPDATE components SET key = 'cron_job' WHERE kind = 'job' AND key = 'job'")


def downgrade() -> None:
    """Restore the pre-split key."""
    op.execute("UPDATE components SET key = 'job' WHERE kind = 'job' AND key = 'cron_job'")
