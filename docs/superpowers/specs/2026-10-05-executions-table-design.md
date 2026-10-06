# Executions as a table folded from events

The performance step of the interloper-db / interloper-api simplification, after #423 to #432.

## Measurements that drove it

- **Prod, 14 days of load-balancer logs (0.100.0):** `GET /runs/executions/latest` was 84% of all user wait time: 19,151 calls, p50 0.78 s, p95 2.24 s. Overview and coverage followed at about 2.7 s p50 each, far fewer calls.
- **One read shape caused all three:** the `executions` view ranked an organisation's whole operation-event history on every read. On prod that was 174k buffer blocks for 478k events, about 800 ms of CPU. On a 10M-event perf database the same routes took 15 s (latest), 9 s (overview) and 11 s (coverage); every other route, and every scheduler tick query, stayed under 150 ms.
- **Two amplifiers:** the app refetched the org-wide latest list one second after every burst of events, per open tab; and on `main` the `Page` total added a `count(*)` that re-ran the whole view, doubling the hottest route.

## Decisions

1. **`executions` is a table**, one row per `(run, operation)` with the view's columns. Primary key `(run_id, component_id)`, `run_id` references `runs` with `ON DELETE CASCADE`. `component_id` has no foreign key, like `events.component_id`: a deleted component's rows stay, and an event racing a deletion still saves. Index `(org_id, component_id, created_at DESC)` serves the latest-per-asset read.
2. **A trigger maintains it.** `fold_execution_event`, `AFTER INSERT ON events` for operation events with a run and a component, upserts the row in the same transaction as the event. It covers every writer, whatever its version, including old pods during a rolling deploy.
3. **The fold is the view's rule, applied per event.** `attempts` is a maximum, `created_at` and `started_at` minima over queued and started events, `completed_at` a maximum over terminal events, and the status is taken from an event that outranks the stored one by `(attempt DESC, severity)`. Every part commutes, so arrival order and repeated deliveries do not change the result; a repeated delivery inserts no event row and never reaches the trigger. The table keeps the status, not the deciding event, so `running` reads as `operation_started`: the one case this differs from the view, `operation_retried` and `operation_skipped` in the same attempt, cannot be produced by a runner.
4. **Migration 012 in one transaction:** rename the view aside, create table, functions and trigger, backfill from the view, drop it. `CREATE TRIGGER` holds concurrent event inserts until the commit, so nothing falls between backfill and trigger. The view's partial index on `events` goes with the view. Downgrade restores migration 008's view and index.
5. **`create_all` leaves the table to the migration** (`migration_owned` marker, renamed from `is_view`), since the table and its trigger must be created together.
6. **Latest per asset drives from components:** one index probe per component of the organisation for its newest execution, so the cost follows the number of components, not history. A deleted component no longer appears in the latest list.
7. **`Page.read` counts only when the page cannot tell:** a page that came back short ends the matching set, so its total is its offset plus its rows.
8. **The app applies pushed rows.** `executions` notifies on insert and on any update that changes the row; the executions store upserts pushed rows into its latest map and the open run's list instead of refetching on event bursts.
9. **CI runs the Postgres tests.** The checks job gets a Postgres service, so the migration tests, including the fold equivalence test, run on every push instead of skipping.

## Verification

- An equivalence test on Postgres: realistic lifecycles and arbitrary event mixes over several attempts, shuffled with repeated deliveries, must fold to exactly the rows migration 008's view computes over the same events. Five mutants of the fold (each merge rule and the ranking) are each caught.
- Live on a dev instance: the backfilled table matched the view on every row, new runs folded identically, the websocket carried 15 execution messages for a five-asset run, and the collection and run pages updated with no execution refetches.

## Measured effect (perf database, 10M events)

| Route | Big org before | after | Small org before | after |
|---|---|---|---|---|
| executions latest | 15,436 ms | 18 ms | 1,133 ms | 16 ms |
| overview | 9,190 ms | 505 ms | 1,225 ms | 196 ms |
| coverage 12 months | 11,074 ms | 2,568 ms | 1,480 ms | 724 ms |

## Not done here

- Coverage still joins every execution to its run for the partition key (about 1.2 s of SQL at the big org) and spends about as long building its arrays in Python. A windowed read, now that reads are cheap, is the next lever.
- The migration's backfill holds event inserts for its duration: seconds at prod's size, about two minutes at 10M events.
