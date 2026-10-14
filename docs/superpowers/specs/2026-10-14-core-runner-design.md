# Core phase 3: the runner family

Date: 2026-10-14. Status: approved in conversation, implementation pending.

Phase 3 of the interloper-core consolidation programme (five stacked phases: scope, contexts and
observation; Asset and Source; the runner family; Component, Serializable and decorators; the
surface sweep). Phases 1 and 2 settled the execution spine and how assets and sources relate. This
phase settles how a DAG is run: one walk, one attempt loop, one result value, one state machine,
and runners that are configuration a run is made from.

Scope: `interloper.runner` (`base`, `state`, `results`, `async_runner`, `serial`; `sync_runner` and
`multi_process` removed), `interloper.operation` (`OperationResult`), and the consumers those seams
reach: the Docker and Kubernetes runners, the scheduler's run executor, the CLI's `run` and
`launch`, `DAG.materialize`, the db store's event ids, the settings descriptions, the docs, the
plugin skills and the examples. Built on phase 2 (PR #459).

---

## 1. Problem

The walk is written twice. `AsyncRunner._run` and `SyncRunner._run_blocking` are the same loop
(initialize, submit ready operations up to capacity, wait for the first to finish, record it, break
on a failure under `fail_fast`, drain, finalize, re-raise), differing only in `asyncio.wait` against
`concurrent.futures.wait`.

The attempt loop is written twice: `AsyncRunner._execute_operation` and the multi-process `_worker`
each implement the retry policy, the backoff, `retryable`, the `failure` hook and the traceback
opt-out.

An execution's outcome has no type. The worker returns a 6-tuple; `_handle_completed` and
`_handle_flushed` are written twice in `SyncRunner` and twice again in `MultiProcessRunner`, and the
two base versions disagree (the flush path drops the exception).

The state machine is spread out: statuses on `ExecutionInfo`, attempts in a separate
`RunState.attempts` dict, transitions in `RunState.mark_*` with an `emit=` flag each caller sets by
hand (the container runners pass `False` to start and `True` to finish), and a `run_id` generated
in two places.

Run state lives on the runner instance (`runner._state`, read by the container runners' hooks as
`self.state`), so a runner cannot run twice at once; the scheduler copies it per run.

`MultiProcessRunner` loses every event its workers emit: under spawn a worker's `EventBus` has no
subscribers, and under fork the inherited subscribers have no dispatcher thread (on macOS the
forked worker crashes the pool: `BrokenProcessPool`). Reproduced on the branch. Nothing in the
repo, the deployments or the manifests uses it.

The container runners run each operation through two threads (a polling future and a log
streaming thread), joined with one and five second timeouts; a child event arriving after the join
is dropped.

---

## 2. Decisions

- **A runner is configuration; a `Run` is one run of it.** `Runner` keeps the serializable
  settings. Each runner names its kind of run (`Run = InProcessRun`), a small class whose attributes
  are what the run holds and whose `execute(operation)` is where an operation executes. A runner
  can serve several runs at once.
- **One walk, on `Run`.** `Run.walk()` is the only DAG walk; `Runner.run` scopes `on_event`, opens
  the span, opens the run and re-raises.
- **One result value.** `OperationResult` carries the verdict too (`error`, `traceback`,
  `exception`); there is no second outcome type.
- **One attempt loop**, `OperationResult.from_execution`, reporting each retry live.
- **One state machine.** `RunState` owns the operations' transitions and their events, one verb per
  transition, no `emit=` flags. Who reports progress is one fact on the `Run` class.
- **The operation lifecycle stays in `RunState`**, not on `Step`: it is six events with
  deterministic ids, not a started/completed/failed triplet. (Phase 1 had deferred the question to
  this phase.)
- **`MultiProcessRunner` and `SyncRunner` are removed.**
- Persisted contracts unchanged: event types and messages, the deterministic operation event ids
  (`RunState.operation_event_id`), `RunResult`'s fields, the CLI the containers run
  (`interloper run --format inline --run-id ...`), the `@EVENT:` line format, and the runner config
  keys in `interloper.yaml` and the chart (`max_workers`, `max_containers`, `max_jobs`, ...).
- Not folded in: how a container's events reach the store without a database connection (an
  ingest endpoint, an outcome line, a post-exit log read) is a separate, optional topic.

---

## 3. Runner and Run

```py
class Runner(Serializable):
    Run: ClassVar[type[Run]]

    fail_fast: bool
    reraise: bool
    on_event: Callable[[Event], None] | None

    @property
    @abstractmethod
    def capacity(self) -> int: ...

    async def run(self, dag, window=None, metadata=None) -> RunResult:
        window = PartitionWindow.of(window)
        metadata = {**(metadata or {})}            # with a run_id, generated here and only here
        # on_event scoped to this run's id, the runner span opened (as today)
        async with self.Run(self, dag, window, metadata) as run:
            result = await run.walk()
        # with reraise: the first failed operation's exception, or a RunnerError with its message
        return result


class Run:
    """One run of a runner: where its operations execute, and what it holds while the run lasts."""

    reports_progress: ClassVar[bool] = True

    def __init__(self, runner: Runner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        self.runner = runner
        self.state = RunState(dag, window, metadata, reports_progress=self.reports_progress)

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc_info: object) -> None: ...   # close()

    async def walk(self) -> RunResult: ...
    def start(self) -> None: ...
    def finish(self, error: str | None = None) -> RunResult: ...

    @abstractmethod
    async def execute(self, operation: Operation) -> OperationResult: ...

    async def close(self) -> None: ...                          # nothing by default
```

`Run.__init__` checks the window against every enabled partitioned operation before anything
emits, listing every failing operation (today's `_preflight`, unchanged); a `PartitionError` leaves
`Runner.run` as today.

### 3.1 The walk

`Run.walk()`:

1. `self.start()`: `RUN_STARTED`, and `OPERATION_QUEUED` for each queued or ready operation.
2. While the state is not done: for each ready operation not in flight, up to
   `self.runner.capacity`, `self.state.start(operation)` and an `asyncio` task over
   `self.execute(operation)`. No ready operation and nothing in flight is a `RunnerError`
   (deadlock).
3. Await the first task to finish and `self.state.finish(operation, result)`.
4. Under `fail_fast`, stop submitting once any operation failed.
5. Await every task still in flight and finish each; `self.state.cancel_pending()`.
6. `return self.finish(error)`: `RUN_COMPLETED` or `RUN_FAILED`, and the `RunResult`.

A task that raises, rather than returning a failed result, is a fault of the runner itself: the
walk drains, records the run's `error`, finishes, and `Runner.run` re-raises it under `reraise`, as
today.

### 3.2 The runs

| Runner | `Run` | `execute(operation)` | Holds |
|---|---|---|---|
| `AsyncRunner` (`max_workers`) | `InProcessRun` | `OperationResult.from_execution(operation, state.context(operation), on_retry=...)` | nothing |
| `SerialRunner` (`AsyncRunner`, `max_workers=1`) | `InProcessRun` | as above | nothing |
| `DockerRunner` (`max_containers`) | `ContainerRun` | runs `follow` on its polling pool | a thread pool sized to `capacity` |
| `KubernetesRunner` (`max_jobs`) | `JobRun` | runs `follow` on its polling pool | a thread pool sized to `capacity` |

`ContainerRun` and `JobRun` set `reports_progress = False`: their child reports its own start and
retries.

```py
class JobRun(Run):
    """One run of a KubernetesRunner: each operation in its own Job, its events read from the Job's log."""

    runner: KubernetesRunner
    reports_progress = False

    def __init__(self, runner, dag, window, metadata) -> None:
        super().__init__(runner, dag, window, metadata)
        self.polling = ThreadPoolExecutor(max_workers=runner.max_jobs)

    async def execute(self, operation: Operation) -> OperationResult:
        job = self.job(operation)
        return await asyncio.get_running_loop().run_in_executor(self.polling, self.follow, job)

    def job(self, operation: Operation) -> V1Job:
        """The Job running the operation's mini-DAG through `interloper run --format inline`."""

    def follow(self, job: V1Job) -> OperationResult:
        """Create the Job, forward its log's events until the pod exits, then read the verdict from its status."""

    async def close(self) -> None:
        self.polling.shutdown(wait=True)
```

`follow` creates the Job, waits for its pod, follows its log to the end (which comes when the
container exits), forwarding each `@EVENT:` line for the target operation to the host's `EventBus`
(the `RUN_*` lines and other operations' lines are dropped, as today), then reads the Job's status:
`OperationResult()` when it succeeded, `OperationResult.from_error("Job ... failed ...")` when it did
not. A stream that ends early falls back to polling the status until it is terminal. A Job that
cannot be created is `OperationResult.from_error(...)`. `ContainerRun` has the same shape over the
Docker API (`containers.run`, `logs(stream=True, follow=True)`, `wait()`, `remove()` when
`auto_remove`). One thread per container; every event the child streamed is forwarded before the
verdict is recorded.

The runners keep their configuration fields and builders (`image`, `namespace`, `env_vars`,
`volumes`, `resources`, `tolerations`, `ttl_seconds_after_finished`, ...); the command, environment
and name builders move to the run when they read the run (`run_id`, the mini-DAG).

Removed: `SyncRunner`, `MultiProcessRunner` (and its `multi_process` entry point, the
`il.MultiProcessRunner` export, `examples/runner/multi_process.py`), `Runner._run`, `_run_blocking`,
`_init_run`, `_finalize_run`, `_on_start`, `_on_end`, `_capacity`, `_submit_operation`,
`_handle_completed`, `_handle_flushed`, both `_flush`es, `AsyncRunner._execute_operation`, the
public `Runner.state`, and the container runners' `_poll_pool`, `_job_map`/`_container_map`,
`_log_threads`, `_stop_log_streaming`, `_start_log_streaming`, `_stop_*_log_streaming`.

`fail_fast` and `reraise` keep each runner's default (the container runners run with
`fail_fast=False` in production).

---

## 4. OperationResult: the one result, the one attempt loop

```py
@dataclass
class OperationResult:
    config: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    traceback: str | None = None
    exception: BaseException | None = field(default=None, compare=False, repr=False)

    @property
    def succeeded(self) -> bool: ...                      # error is None

    @classmethod
    async def from_execution(
        cls, operation: Operation, context: OperationContext, *, on_retry: Callable[[str], None]
    ) -> OperationResult: ...

    @classmethod
    def from_error(cls, error: str, traceback: str | None = None) -> OperationResult: ...
```

- Authoring is unchanged: `execute` returns `OperationResult(config=..., state=...)`, `failure(error)`
  returns one carrying `error`.
- `from_execution` is the one attempt loop: each attempt runs `operation.execute(context)` inside
  the `interloper.operation.execute` span; an error is retried when `operation.retry` allows another
  attempt and `operation.retryable(error)` agrees (`on_retry(message)`, then the policy's backoff);
  otherwise the result is `operation.failure(error)` with `error` filled when the hook left it empty,
  the traceback when `capture_traceback` allows, and the original exception.
- `exception` is in-process only and only for `reraise`.

---

## 5. RunState: the state machine of a run's operations

```py
@dataclass
class ExecutionInfo:
    component_id: str
    component_key: str
    status: ExecutionStatus
    attempt: int = 1
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    result: OperationResult | None = None

    @property
    def error(self) -> str | None: ...       # result.error
```

`ExecutionInfo.to_dict` keeps its keys (`error`, `traceback`, ...), read from `result`.

```py
class RunState:
    """A run's operations: their statuses, attempts and results, and the events their transitions emit."""

    def __init__(self, dag, window, metadata, *, reports_progress: bool = True) -> None: ...

    ready: list[Operation]          # dependencies met, not started
    failed: list[Operation]
    done: bool                      # every operation terminal
    def context(self, operation: Operation) -> OperationContext: ...

    def start(self, operation: Operation) -> None: ...                         # RUNNING; OPERATION_STARTED
    def retry(self, operation: Operation, error: str) -> None: ...             # OPERATION_RETRIED; attempt + 1
    def finish(self, operation: Operation, result: OperationResult) -> None: ...
        # COMPLETED, dependents promoted; or FAILED, OPERATION_FAILED, dependents canceled
    def cancel_pending(self) -> None: ...                                      # OPERATION_CANCELED for the rest

    @staticmethod
    def operation_event_id(run_id, component_id, event_type, attempt=1) -> str: ...   # unchanged
```

- With `reports_progress=False`, `start` and `retry` record the transition and emit nothing; every
  verdict is still recorded and its terminal event authored by the host, deduplicated against the
  child's by the deterministic id, as today.
- The run's own lifecycle is on `Run`: `run.start()` emits `RUN_STARTED` and the queued events,
  `run.finish(error)` emits `RUN_COMPLETED` or `RUN_FAILED` with the state's counts and returns the
  `RunResult`.
- Removed: `mark_running`, `mark_completed`, `mark_failed`, `mark_retried` and their `emit=` flags,
  `start_run`, `end_run`, the `attempts` dict, the second `run_id` generation. `mark_canceled` stays
  internal to `cancel_pending` and failure propagation.
- `RunResult` is unchanged.

---

## 6. Consumers

- **Scheduler executor**: `_apply_effects` reads `info.result`; the per-run `model_copy` stays (it
  scopes `on_event`).
- **CLI** (`run`, `launch`) and `DAG.materialize`: unchanged (`Runner.from_settings(...)`,
  `runner.run(...)`, `RunResult`).
- **db store**: `RunState.operation_event_id` keeps its signature.
- **Settings**: the runner key descriptions drop `multi_process`; an unknown key is a `ConfigError`
  naming the available runners, as today.
- **Docs**: `guide/execution.md` (the runner table), `extending/runners.md` (a runner as a short
  `Run`, shown in full), `guide/cli.md`, `guide/specs.md`, `guide/telemetry.md`, the README line,
  `plugins/interloper/skills/interloper-deploy`, the upgrade table row.

---

## 7. Errors

- A deadlock: `RunnerError`, the run's `error`.
- A task that raises: the run's `error`; re-raised under `reraise`.
- A failed operation under `reraise`: its original exception when the run was in-process, else
  `RunnerError` carrying its message.
- A window no partitioned operation can serve: `PartitionError` before anything emits.
- An unknown runner key: `ConfigError`.

---

## 8. Out of scope, follow-ups

- One name for capacity across runners (`max_workers` accepting `max_containers` and `max_jobs`):
  phase 5, with the old keys still accepted.
- Container event collection (ingest endpoint, outcome line, post-exit log read): separate,
  optional.
- The asset entry points (`run`, `run_async`, `materialize`, `materialize_async`, `execute`) are
  unchanged: the sync-first bridge is deliberate.

---

## 9. Testing

Mirror layout, red then green per unit.

- `tests/runner/test_base.py`: the walk (ordering, capacity, `fail_fast`, draining, the deadlock, a
  task that raises), `reraise`, `on_event` scoping, one runner running two DAGs concurrently, the
  preflight.
- `tests/runner/test_state.py`: each verb and its events, `reports_progress=False`, cancellation of
  dependents, `cancel_pending`, event ids unchanged (`tests/runner/test_state_event_ids.py` folds in
  here: the layout mirrors the modules).
- `tests/runner/test_results.py`: `ExecutionInfo` with its `result`, `to_dict` keys unchanged.
- `tests/operation/test_base.py`: `from_execution` (success, retry reported live, exhausted budget,
  declined retry, the `failure` hook, the traceback opt-out, the exception kept), `from_error`.
- `tests/runner/test_async_runner.py`: the in-process run; `tests/runner/test_serial.py` (new, for
  `serial.py`): one slot.
- interloper-docker and interloper-k8s `tests/test_runner.py`: `follow` against fake clients (events
  forwarded before the verdict, other operations' and `RUN_*` lines dropped, a failed exit, a launch
  failure, a stream ending early); each package's `tests/test_runner_terminal_events.py` folds in.
- interloper-scheduler: `_apply_effects` over `info.result`.
- `tests/runner/test_multi_process.py` and `sync_runner` tests: removed with their modules.
