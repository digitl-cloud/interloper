"""Persist executions as a table folded from events, instead of a view over them.

The ``executions`` view ranked an organisation's whole operation-event
history on every read, so its cost grew with history: on production about
800 ms per read for half a million events, fifteen seconds at ten times that.
The table holds the same rows, one per ``(run, operation)``, kept current by
``fold_execution_event``, an ``AFTER INSERT`` trigger on ``events``.

The fold is the view's rule applied one event at a time. Every column is a
maximum, a minimum, or the status of the event that ranks first by
``(attempt DESC, severity)``, so folding the same events in any order, or
one of them twice, gives the row the view computed. A duplicate delivery
inserts no event row, so the trigger never even sees it. Like ``events``,
the table keeps a deleted component's rows: ``component_id`` has no foreign
key, so an event racing a component's deletion still saves.

The upgrade runs in one transaction. ``CREATE TRIGGER`` takes a lock on
``events`` that holds concurrent inserts until the commit, so the backfill
reads a closed history and every later event meets the trigger: none falls
between the two. The view's partial index on ``events`` served only the view
and goes with it. Downgrading restores the view of migration 008.

Revision ID: 012
Revises: 011
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "012"
down_revision: str | None = "011"
branch_labels: str | None = None
depends_on: str | None = None

_OPERATION_EVENT_TYPES = """(
    'operation_queued', 'operation_skipped', 'operation_started', 'operation_completed',
    'operation_failed', 'operation_canceled', 'operation_retried'
)"""

_TERMINAL_EVENT_TYPES = "('operation_completed', 'operation_failed', 'operation_canceled')"

# Lower ranks first. A status read back from the table stands for the event that set it:
# ``running`` for ``operation_started``, which outranks ``operation_retried`` in the same attempt.
_STATUS_RANK = """CREATE OR REPLACE FUNCTION execution_status_rank(status text) RETURNS int
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE status
        WHEN 'failed' THEN 1
        WHEN 'canceled' THEN 2
        WHEN 'success' THEN 3
        WHEN 'running' THEN 4
        WHEN 'skipped' THEN 5
        WHEN 'queued' THEN 7
    END
$$"""

_FOLD = f"""CREATE OR REPLACE FUNCTION fold_execution_event() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    event_attempt int := COALESCE((NEW.data->>'attempt')::int, 1);
    event_status text := CASE NEW.event_type
        WHEN 'operation_failed' THEN 'failed'
        WHEN 'operation_canceled' THEN 'canceled'
        WHEN 'operation_completed' THEN 'success'
        WHEN 'operation_started' THEN 'running'
        WHEN 'operation_skipped' THEN 'skipped'
        WHEN 'operation_retried' THEN 'running'
        WHEN 'operation_queued' THEN 'queued'
    END;
    event_rank int := CASE NEW.event_type WHEN 'operation_retried' THEN 6 ELSE execution_status_rank(event_status) END;
BEGIN
    INSERT INTO executions AS e
        (run_id, component_id, org_id, component_key, status, attempts, created_at, started_at, completed_at)
    VALUES (
        NEW.run_id, NEW.component_id, NEW.org_id, NEW.component_key, event_status, event_attempt,
        CASE WHEN NEW.event_type = 'operation_queued' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type = 'operation_started' THEN NEW.timestamp END,
        CASE WHEN NEW.event_type IN {_TERMINAL_EVENT_TYPES} THEN NEW.timestamp END
    )
    ON CONFLICT (run_id, component_id) DO UPDATE SET
        status = CASE
            WHEN event_attempt > e.attempts
                OR (event_attempt = e.attempts AND event_rank < execution_status_rank(e.status))
            THEN EXCLUDED.status ELSE e.status
        END,
        component_key = CASE
            WHEN event_attempt > e.attempts
                OR (event_attempt = e.attempts AND event_rank < execution_status_rank(e.status))
            THEN EXCLUDED.component_key ELSE e.component_key
        END,
        attempts = GREATEST(e.attempts, EXCLUDED.attempts),
        created_at = LEAST(e.created_at, EXCLUDED.created_at),
        started_at = LEAST(e.started_at, EXCLUDED.started_at),
        completed_at = GREATEST(e.completed_at, EXCLUDED.completed_at);
    RETURN NULL;
END
$$"""

_TABLE = """CREATE TABLE executions (
    run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    component_id uuid NOT NULL,
    org_id uuid NOT NULL,
    component_key varchar,
    status varchar NOT NULL,
    attempts integer NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    created_at timestamp with time zone,
    PRIMARY KEY (run_id, component_id)
)"""

_VIEW_COLUMNS = "run_id, component_id, org_id, component_key, status, attempts, started_at, completed_at, created_at"

_OPERATION_EVENTS = f"component_id IS NOT NULL AND event_type IN {_OPERATION_EVENT_TYPES}"

# Migration 008's view, restored on downgrade.
_VIEW = f"""CREATE VIEW executions AS
WITH ranked AS (
    SELECT
        e.run_id,
        e.org_id,
        e.component_id,
        e.component_key,
        e.event_type,
        e.timestamp,
        row_number() OVER (
            PARTITION BY e.org_id, e.run_id, e.component_id
            ORDER BY
                COALESCE((e.data->>'attempt')::int, 1) DESC,
                CASE e.event_type
                    WHEN 'operation_failed' THEN 1
                    WHEN 'operation_canceled' THEN 2
                    WHEN 'operation_completed' THEN 3
                    WHEN 'operation_started' THEN 4
                    WHEN 'operation_skipped' THEN 5
                    WHEN 'operation_retried' THEN 6
                    WHEN 'operation_queued' THEN 7
                END,
                e.timestamp DESC
        ) AS rn,
        max(COALESCE((e.data->>'attempt')::int, 1)) OVER (
            PARTITION BY e.org_id, e.run_id, e.component_id
        ) AS attempts,
        min(CASE WHEN e.event_type = 'operation_queued' THEN e.timestamp END) OVER (
            PARTITION BY e.org_id, e.run_id, e.component_id
        ) AS queued_at,
        min(CASE WHEN e.event_type = 'operation_started' THEN e.timestamp END) OVER (
            PARTITION BY e.org_id, e.run_id, e.component_id
        ) AS started_at,
        max(CASE WHEN e.event_type IN {_TERMINAL_EVENT_TYPES} THEN e.timestamp END) OVER (
            PARTITION BY e.org_id, e.run_id, e.component_id
        ) AS completed_at
    FROM events e
    WHERE {_OPERATION_EVENTS}
)
SELECT
    r.run_id,
    r.org_id,
    r.component_id,
    r.component_key,
    CASE r.event_type
        WHEN 'operation_failed' THEN 'failed'
        WHEN 'operation_canceled' THEN 'canceled'
        WHEN 'operation_completed' THEN 'success'
        WHEN 'operation_started' THEN 'running'
        WHEN 'operation_skipped' THEN 'skipped'
        WHEN 'operation_retried' THEN 'running'
        WHEN 'operation_queued' THEN 'queued'
    END AS status,
    r.started_at,
    r.completed_at,
    r.queued_at AS created_at,
    r.attempts
FROM ranked r
WHERE r.rn = 1"""


def upgrade() -> None:
    op.execute("ALTER VIEW executions RENAME TO executions_view")
    op.execute(_TABLE)
    op.execute("CREATE INDEX ix_executions_latest ON executions (org_id, component_id, created_at DESC)")
    op.execute(_STATUS_RANK)
    op.execute(_FOLD)
    op.execute(
        "CREATE TRIGGER trg_events_fold_execution AFTER INSERT ON events FOR EACH ROW WHEN ("
        f"NEW.run_id IS NOT NULL AND NEW.component_id IS NOT NULL AND NEW.event_type IN {_OPERATION_EVENT_TYPES}"
        ") EXECUTE FUNCTION fold_execution_event()"
    )
    op.execute(
        f"INSERT INTO executions ({_VIEW_COLUMNS}) SELECT {_VIEW_COLUMNS} FROM executions_view WHERE run_id IS NOT NULL"
    )
    op.execute("DROP VIEW executions_view")
    op.execute(
        "CREATE TRIGGER trg_executions_notify_insert AFTER INSERT ON executions "
        "FOR EACH ROW EXECUTE FUNCTION notify_table_change()"
    )
    op.execute(
        "CREATE TRIGGER trg_executions_notify_update AFTER UPDATE ON executions FOR EACH ROW "
        "WHEN (OLD.* IS DISTINCT FROM NEW.*) EXECUTE FUNCTION notify_table_change()"
    )
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_events_executions")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_events_executions ON events (org_id, run_id, component_id) "
            f"WHERE {_OPERATION_EVENTS}"
        )
    op.execute("DROP TRIGGER IF EXISTS trg_events_fold_execution ON events")
    op.execute("DROP TABLE IF EXISTS executions")
    op.execute("DROP FUNCTION IF EXISTS fold_execution_event()")
    op.execute("DROP FUNCTION IF EXISTS execution_status_rank(text)")
    op.execute(_VIEW)
