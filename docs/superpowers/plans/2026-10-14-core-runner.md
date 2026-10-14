# Core Phase 3: The Runner Family Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One walk (`Run.walk`), one attempt loop (`OperationResult.from_execution`), one result value (`OperationResult`), one state machine (`RunState`), and runners that are reusable configuration a `Run` is made from.

**Architecture:** A `Runner` keeps serializable configuration and names its kind of run (`Run = InProcessRun`). A `Run` owns one run: its `RunState`, its resources (attributes, released by `close`), the walk, and the run's own lifecycle (`start`, `finish`). Kinds of run differ only in `execute(operation) -> OperationResult`. `MultiProcessRunner` and `SyncRunner` are removed; the container runners become `ContainerRun` and `JobRun`, one thread per container, reading the child's log through one shared `EventLogReader`.

**Tech Stack:** Python 3.12 (floor 3.10), asyncio, Pydantic 2.12, docker-py, kubernetes client, pytest, ruff, ty, uv workspace.

**Spec:** `docs/superpowers/specs/2026-10-14-core-runner-design.md`

## Global Constraints

- No em dash character in any text: code, comments, docstrings, docs, messages, tests.
- Google-style docstrings, complete: every module, class, function and method, private helpers, `__init__` and dunders included; `Args:` lists every parameter, `Returns:`/`Raises:` whenever they apply (`Raises:` only for an explicit `raise` in the body).
- Beautiful shapes: no nested functions or closures as structure, no context managers yielding callables, no `PrivateAttr`/`None`-guard choreography; something with a lifetime and behaviour is a small class.
- Names written in full (`context`, `operation`, `destination`, never `ctx`, `op`, `dest`).
- Comments only for a non-obvious why; no attribute-level comments; no comments narrating the change.
- Tests mirror the source layout one-to-one: `src/interloper/<pkg>/<module>.py` is tested in `tests/<pkg>/test_<module>.py`; no new `test_<feature>.py`.
- Run uv with `UV_FROZEN=1`; never change `uv.lock`.
- Never commit, amend or push during the tasks; the work rests uncommitted until the finish step.
- Persisted contracts unchanged: event types and messages, `RunState.operation_event_id(run_id, component_id, event_type, attempt=1)`, `RunResult`'s fields, `interloper run --format inline --run-id=<id> <spec> [--date ... | --start-date ... --end-date ...]`, the `@EVENT:` line format, the runner config keys (`max_workers`, `max_containers`, `max_jobs`, `image`, `namespace`, ...).
- Ruff line length 120.
- Gate per task: Tasks 1, 2 and 6 end with `UV_FROZEN=1 make check-python` green. Task 3 removes `SyncRunner`, which the Docker and Kubernetes runners import until Tasks 4 and 5 port them, so Task 3 gates on the suites it names; Task 4 adds Docker; Task 5 ends with the full check green again.

## Review Focus

1. One runner instance serving two concurrent runs must keep their states, events and results apart (`on_event` scoped to each run's id). Pinned in Task 3 (`test_one_runner_serves_two_concurrent_runs`).
2. Under `fail_fast`, an operation already in flight finishes and is recorded, queued ones are canceled once, and no event is emitted twice. Pinned in Task 3 (ported `test_in_flight_operations_finish_and_queued_ones_are_canceled`).
3. A `Run.close()` that raises must not mask the run's result. Pinned in Task 3 (`test_a_failing_close_does_not_mask_the_result`).
4. A container's log stream that breaks mid-run must still forward what arrived and return the exit-status verdict. Pinned in Task 4 (`test_a_broken_log_stream_still_returns_the_verdict`) and Task 5 (`test_a_broken_log_stream_still_returns_the_verdict`).
5. A Job that finishes without its pod ever starting (an admission or scheduling failure) must return its verdict, not wait for a pod forever. Pinned in Task 5 (`test_a_job_that_fails_before_its_pod_starts_returns_the_failure`).

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `packages/interloper-core/src/interloper/operation/base.py` | `OperationResult` with the verdict and `from_execution`, the one attempt loop | 1 |
| `packages/interloper-core/src/interloper/events/stderr.py` | `EventLogReader`, beside the `StderrEventHandler` that writes the lines | 2 |
| `packages/interloper-core/src/interloper/runner/subscription.py` (new) | `RunSubscription`: one run's events delivered to `on_event` | 2 |
| `packages/interloper-core/src/interloper/runner/results.py` | `ExecutionInfo` with `attempt` and `result` | 3 |
| `packages/interloper-core/src/interloper/runner/state.py` | `RunState`, the state machine of a run's operations | 3 |
| `packages/interloper-core/src/interloper/runner/run.py` (new) | `Run`: the walk, the run's lifecycle, `reraise` | 3 |
| `packages/interloper-core/src/interloper/runner/base.py` | `Runner`: configuration, `run`, `capacity`, `from_settings` | 3 |
| `packages/interloper-core/src/interloper/runner/async_runner.py` | `InProcessRun`, `AsyncRunner` | 3 |
| `packages/interloper-core/src/interloper/runner/serial.py` | `SerialRunner` (unchanged shape) | 3 |
| `packages/interloper-core/src/interloper/runner/sync_runner.py`, `multi_process.py` | removed | 3 |
| `packages/interloper-docker/src/interloper_docker/runner.py` | `ContainerRun`, `DockerRunner` | 4 |
| `packages/interloper-k8s/src/interloper_k8s/runner.py` | `JobRun`, `KubernetesRunner` | 5 |
| docs, skills, README, examples, settings descriptions | surface | 6 |

---

### Task 1: OperationResult carries the verdict; one attempt loop

**Files:**
- Modify: `packages/interloper-core/src/interloper/operation/base.py` (`OperationResult`)
- Test: `packages/interloper-core/tests/operation/test_base.py`

**Interfaces:**
- Produces: `OperationResult(error=None, config={}, state={}, traceback=None, exception=None)` (dataclass, `exception` excluded from comparison and repr); `OperationResult.succeeded -> bool`; `async OperationResult.from_execution(operation: Operation, context: OperationContext, *, on_retry: Callable[[str], None]) -> OperationResult`.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-core/tests/operation/test_base.py`:

```python
class _Flaky(Operation):
    """Fails a set number of times, then succeeds with an effect."""

    failures: int = 0
    retry: il.RetryPolicy | None = il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0)

    async def execute(self, context: OperationContext) -> OperationResult:
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("transient")
        return OperationResult(state={"done": True})


class _Curated(Operation):
    """Always fails, and curates its failure."""

    capture_traceback: ClassVar[bool] = False

    async def execute(self, context: OperationContext) -> OperationResult:
        raise ValueError("secret in here")

    def failure(self, error: Exception) -> OperationResult:
        return OperationResult(error="curated", state={"failed": True})


class TestFromExecution:
    async def test_success_returns_the_operations_effects(self):
        result = await OperationResult.from_execution(_Flaky(), OperationContext(), on_retry=_never)
        assert result.succeeded
        assert result.state == {"done": True}

    async def test_a_retryable_failure_is_reported_and_retried(self):
        reported: list[str] = []
        result = await OperationResult.from_execution(_Flaky(failures=2), OperationContext(), on_retry=reported.append)
        assert result.succeeded
        assert reported == ["ConnectionError: transient", "ConnectionError: transient"]

    async def test_an_exhausted_budget_returns_the_failure(self):
        reported: list[str] = []
        result = await OperationResult.from_execution(_Flaky(failures=5), OperationContext(), on_retry=reported.append)
        assert not result.succeeded
        assert result.error == "ConnectionError: transient"
        assert len(reported) == 2
        assert isinstance(result.exception, ConnectionError)
        assert result.traceback is not None and "ConnectionError" in result.traceback

    async def test_an_error_the_operation_declines_is_not_retried(self):
        class Declining(_Flaky):
            def retryable(self, error: Exception) -> bool:
                return False

        reported: list[str] = []
        result = await OperationResult.from_execution(Declining(failures=1), OperationContext(), on_retry=reported.append)
        assert not result.succeeded
        assert reported == []

    async def test_the_failure_hook_curates_and_the_traceback_can_be_withheld(self):
        result = await OperationResult.from_execution(_Curated(), OperationContext(), on_retry=_never)
        assert (result.error, result.state, result.traceback) == ("curated", {"failed": True}, None)
        assert isinstance(result.exception, ValueError)

    def test_the_exception_is_not_part_of_equality(self):
        assert OperationResult(error="x", exception=ValueError("a")) == OperationResult(error="x")


def _never(error: str) -> None:
    raise AssertionError(f"no retry expected, got {error}")
```

(`ClassVar` and `il` are already imported in this file.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/operation/test_base.py -q`
Expected: FAIL (`AttributeError: type object 'OperationResult' has no attribute 'from_execution'`, `succeeded` missing, unexpected keyword `exception`).

- [ ] **Step 3: Implement**

In `packages/interloper-core/src/interloper/operation/base.py` (imports: `asyncio`, `from collections.abc import Callable` under `TYPE_CHECKING` if only annotated, `from dataclasses import dataclass, field, replace`, `from traceback import format_exc`, `from interloper.telemetry import attributes as telemetry_attributes`, `from interloper.telemetry.tracer import tracer`), replace `OperationResult` with:

```python
@dataclass
class OperationResult:
    """What an operation came to: the effects the platform persists, and the verdict.

    ``config`` fields are merged into the target component's stored config
    (through its encryption path) and ``state`` fields onto its machine-owned
    state: how an operation's effects reach the row without the operation
    knowing the store. An operation's `execute` returns one carrying its
    effects, and its `failure` hook one carrying the persistence-safe
    ``error``. The attempt loop (`from_execution`) completes a failure with the
    ``traceback`` (when the operation's class allows one) and the original
    ``exception``, kept in memory only so a runner can re-raise it. An unset
    ``error`` means the operation succeeded.
    """

    error: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    traceback: str | None = None
    exception: BaseException | None = field(default=None, compare=False, repr=False)

    @property
    def succeeded(self) -> bool:
        """Whether the operation succeeded, which is whether no error is set."""
        return self.error is None

    @classmethod
    async def from_execution(
        cls,
        operation: Operation,
        context: OperationContext,
        *,
        on_retry: Callable[[str], None],
    ) -> OperationResult:
        """Execute an operation under its retry policy and return what it came to.

        Each attempt runs inside the ``interloper.operation.execute`` span. A
        failure is retried when the operation's `RetryPolicy` allows another
        attempt and `Operation.retryable` agrees: ``on_retry`` is told the
        error as it happens, the policy's backoff is slept, and the operation
        executes again. Any other failure becomes the operation's `failure`
        result, completed with the formatted error when the hook left it empty,
        the traceback when the class's ``capture_traceback`` allows, and the
        original exception.

        Args:
            operation: The operation to execute.
            context: The facts it executes with.
            on_retry: Told the formatted error of each attempt that will be
                retried, before its backoff.

        Returns:
            The result: the operation's own on success, its failure otherwise.
        """
        span_attributes = telemetry_attributes.from_metadata(operation.event_metadata(context.metadata, context.window))
        attempt = 1
        while True:
            try:
                with tracer().start_as_current_span("interloper.operation.execute", attributes=span_attributes):
                    return await operation.execute(context)
            except Exception as error:  # noqa: BLE001 - every failure becomes the operation's result
                policy = operation.retry
                if policy is not None and policy.allows(attempt + 1) and operation.retryable(error):
                    on_retry(format_exception(error))
                    await asyncio.sleep(policy.delay_before(attempt + 1))
                    attempt += 1
                    continue
                failed = operation.failure(error)
                return replace(
                    failed,
                    error=failed.error or format_exception(error),
                    traceback=format_exc() if type(operation).capture_traceback else None,
                    exception=error,
                )
```

`Operation` is defined below in the same module; with `from __future__ import annotations` the annotations resolve. If `ruff` flags the forward use at runtime, it does not: the names are only read when the method runs.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/operation -q`
Expected: PASS. Existing tests asserting `OperationResult(...) == OperationResult(...)` keep passing (`exception` is not compared).

- [ ] **Step 5: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green (nothing uses the new members yet).

- [ ] **Step 6: Checkpoint (no commit)**

---

### Task 2: EventLogReader and RunSubscription

**Files:**
- Modify: `packages/interloper-core/src/interloper/events/stderr.py`, `packages/interloper-core/src/interloper/events/__init__.py`
- Create: `packages/interloper-core/src/interloper/runner/subscription.py`
- Test: `packages/interloper-core/tests/events/test_stderr.py`, `packages/interloper-core/tests/runner/test_subscription.py` (new, mirrors the new module)

**Interfaces:**
- Produces: `EventLogReader(component_id: str, label: str)` with `feed(chunk: bytes) -> None` and `close() -> None` (exported from `interloper.events`); `RunSubscription(handler: Callable[[Event], None], run_id: str)`, an async context manager and a callable.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-core/tests/events/test_stderr.py`:

```python
class TestEventLogReader:
    def _capture(self) -> list[Event]:
        captured: list[Event] = []
        EventBus.flush(timeout=5.0)
        EventBus.subscribe(captured.append)
        self._handler = captured.append
        return captured

    def _release(self) -> None:
        EventBus.flush(timeout=5.0)
        EventBus.unsubscribe(self._handler)

    def _line(self, event_type: EventType, **metadata: Any) -> bytes:
        return (EVENT_LINE_PREFIX + Event(type=event_type, metadata=metadata).to_json() + "\n").encode()

    def test_forwards_the_operations_events_across_split_chunks(self):
        captured = self._capture()
        reader = EventLogReader("asset-1", label="container x")
        line = self._line(EventType.ASSET_DATA_STARTED, component_id="asset-1")
        reader.feed(line[:10])
        reader.feed(line[10:])
        reader.close()
        self._release()
        assert [event.type for event in captured] == [EventType.ASSET_DATA_STARTED]

    def test_drops_run_events_and_other_operations(self):
        captured = self._capture()
        reader = EventLogReader("asset-1", label="container x")
        reader.feed(self._line(EventType.RUN_STARTED, component_id="asset-1"))
        reader.feed(self._line(EventType.ASSET_DATA_STARTED, component_id="upstream"))
        reader.feed(b"plain log output\n")
        reader.close()
        self._release()
        assert captured == []

    def test_close_reads_a_trailing_line_without_newline(self):
        captured = self._capture()
        reader = EventLogReader("asset-1", label="container x")
        reader.feed(self._line(EventType.ASSET_DATA_COMPLETED, component_id="asset-1").rstrip(b"\n"))
        reader.close()
        self._release()
        assert [event.type for event in captured] == [EventType.ASSET_DATA_COMPLETED]
```

(Imports to add if missing: `from typing import Any`, `from interloper.events import Event, EventBus, EventLogReader, EventType`, `from interloper.events.stderr import EVENT_LINE_PREFIX`.)

`packages/interloper-core/tests/runner/test_subscription.py`:

```python
"""Tests for ``interloper.runner.subscription``."""

from interloper.events import Event, EventBus, EventType
from interloper.runner.subscription import RunSubscription


async def test_delivers_only_its_runs_events_and_flushes_on_exit():
    """A subscription delivers events carrying its run id, all of them by the time it exits."""
    delivered: list[Event] = []
    async with RunSubscription(delivered.append, "run-1"):
        EventBus.emit(EventType.RUN_STARTED, metadata={"run_id": "run-1"})
        EventBus.emit(EventType.RUN_STARTED, metadata={"run_id": "run-2"})
    assert [event.metadata["run_id"] for event in delivered] == ["run-1"]


async def test_unsubscribes_on_exit():
    """After the block, the handler receives nothing."""
    delivered: list[Event] = []
    async with RunSubscription(delivered.append, "run-1"):
        pass
    EventBus.emit(EventType.RUN_STARTED, metadata={"run_id": "run-1"})
    EventBus.flush(timeout=5.0)
    assert delivered == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/events/test_stderr.py packages/interloper-core/tests/runner/test_subscription.py -q`
Expected: import errors (`EventLogReader`, `interloper.runner.subscription`).

- [ ] **Step 3: Implement `EventLogReader`**

In `packages/interloper-core/src/interloper/events/stderr.py` (imports: `logging`; `EventType` from `interloper.events.types` or wherever this module imports `Event`/`EventBus` from; keep the module's existing import style):

```python
logger = logging.getLogger(__name__)

_RUN_EVENTS = frozenset({EventType.RUN_STARTED, EventType.RUN_COMPLETED, EventType.RUN_FAILED})


class EventLogReader:
    """Read one child's log and forward its operation's events to the host's EventBus.

    The other side of `StderrEventHandler`: a child process started for one
    operation writes each event as an ``@EVENT:`` line, and the host feeds the
    child's log through this reader as it arrives, in chunks of any size. The
    child's own ``RUN_*`` events and events about other operations (the
    read-only upstreams of its mini-DAG) are dropped; a line that is not an
    event is logged at debug level.
    """

    def __init__(self, component_id: str, label: str) -> None:
        """Start reading the log of one operation's child.

        Args:
            component_id: The operation whose events are forwarded.
            label: Names the child in debug log lines, e.g. ``"pod interloper-run-1a2b"``.
        """
        self.component_id = component_id
        self.label = label
        self.pending = ""

    def feed(self, chunk: bytes) -> None:
        """Read a chunk of the log, forwarding every complete line it ends.

        Args:
            chunk: The next bytes of the log, split anywhere.
        """
        self.pending += chunk.decode("utf-8", errors="ignore")
        *lines, self.pending = self.pending.split("\n")
        for line in lines:
            self._read(line)

    def close(self) -> None:
        """Read the last line, which the log may end without a newline."""
        self._read(self.pending)
        self.pending = ""

    def _read(self, line: str) -> None:
        """Forward one line when it is an event about this operation.

        Args:
            line: One line of the log.
        """
        line = line.rstrip()
        if not line:
            return
        event = Event.from_log_line(line)
        if event is None:
            logger.debug("[%s] %s", self.label, line)
            return
        if event.type in _RUN_EVENTS or event.metadata.get("component_id") not in (None, self.component_id):
            return
        EventBus.emit_event(event)
```

Export it from `interloper/events/__init__.py` (`__all__` too).

- [ ] **Step 4: Implement `RunSubscription`**

`packages/interloper-core/src/interloper/runner/subscription.py`:

```python
"""RunSubscription: one run's events delivered to a handler while the run lasts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from typing_extensions import Self

from interloper.events import Event, EventBus


class RunSubscription:
    """Deliver one run's events to a handler while the run lasts.

    The EventBus is process-wide, so several runs in one process (the
    in-process launcher) would otherwise reach every handler: only events
    carrying this run's id are delivered. Leaving the block flushes what is
    still queued, so every event the run emitted has reached the handler when
    the run returns.
    """

    def __init__(self, handler: Callable[[Event], None], run_id: str) -> None:
        """Bind the handler to one run.

        Args:
            handler: Receives each of the run's events.
            run_id: The run whose events are delivered.
        """
        self.handler = handler
        self.run_id = run_id

    def __call__(self, event: Event) -> None:
        """Deliver an event when it belongs to this run.

        Args:
            event: Any event the bus dispatches.
        """
        if event.metadata.get("run_id") == self.run_id:
            self.handler(event)

    async def __aenter__(self) -> Self:
        """Subscribe for the length of the run.

        Returns:
            This subscription.
        """
        EventBus.subscribe(self)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Deliver what is still queued, then unsubscribe.

        Args:
            *exc_info: The exception leaving the block, if any; not suppressed.
        """
        await asyncio.to_thread(EventBus.flush, 5.0)
        EventBus.unsubscribe(self)
```

- [ ] **Step 5: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/events packages/interloper-core/tests/runner/test_subscription.py -q`
Expected: PASS.

- [ ] **Step 6: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.

- [ ] **Step 7: Checkpoint (no commit)**

---

### Task 3: Run, the one walk; RunState, the one state machine; the in-process runners

**Files:**
- Modify: `packages/interloper-core/src/interloper/runner/results.py` (`ExecutionInfo`)
- Modify: `packages/interloper-core/src/interloper/runner/state.py` (`RunState`)
- Create: `packages/interloper-core/src/interloper/runner/run.py` (`Run`)
- Modify: `packages/interloper-core/src/interloper/runner/base.py` (`Runner`)
- Modify: `packages/interloper-core/src/interloper/runner/async_runner.py` (`InProcessRun`, `AsyncRunner`)
- Modify: `packages/interloper-core/src/interloper/runner/__init__.py`, `packages/interloper-core/src/interloper/__init__.py`, `packages/interloper-core/pyproject.toml` (entry points)
- Delete: `packages/interloper-core/src/interloper/runner/sync_runner.py`, `packages/interloper-core/src/interloper/runner/multi_process.py`, `packages/interloper-core/tests/runner/test_sync_runner.py`, `packages/interloper-core/tests/runner/test_multi_process.py`, `examples/runner/multi_process.py`
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/executor.py` (`_apply_effects`)
- Test: `packages/interloper-core/tests/runner/test_base.py`, `test_async_runner.py`, `test_state.py` (with `test_state_event_ids.py` folded in and deleted), `test_results.py`, `test_run.py` (new, mirrors `run.py`), `test_serial.py` (new, mirrors `serial.py`); `packages/interloper-core/tests/operation/test_base.py`; `packages/interloper-scheduler/tests/test_executor.py`

**Interfaces:**
- Consumes: `OperationResult.from_execution`, `OperationResult.succeeded` (Task 1); `RunSubscription` (Task 2).
- Produces:
  - `ExecutionInfo(component_id, component_key, status, attempt=1, start_time=None, end_time=None, result=None)` with `start()`, `finish(result)`, `cancel()`, `error`, `traceback`, `execution_time`, `is_terminal`, `to_dict()`.
  - `RunState(dag, window, metadata, *, reports_progress=True)` with `run_id`, `ready`, `completed`, `failed`, `done`, `context(operation)`, `queue()`, `start(operation)`, `retry(operation, error)`, `finish(operation, result)`, `cancel_pending()`, `operation_event_id(...)` (static, unchanged).
  - `Run(runner, dag, window, metadata)` with `reports_progress: ClassVar[bool]`, `state`, `fault`, `walk() -> RunResult`, `start()`, `finish(error=None) -> RunResult`, `reraise()`, abstract `execute(operation) -> OperationResult`, `close()`; exported from `interloper.runner`.
  - `Runner` with `Run: ClassVar[type[Run]]`, abstract `capacity -> int`, `run(dag, window=None, metadata=None) -> RunResult`, `from_settings` (unchanged).
  - `InProcessRun(Run)`; `AsyncRunner.Run = InProcessRun`, `AsyncRunner.capacity -> max_workers`.

- [ ] **Step 1: Write the failing tests**

`packages/interloper-core/tests/runner/test_run.py`:

```python
"""Tests for ``interloper.runner.run``."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import interloper as il
from interloper.events import Event, EventType
from interloper.errors import PartitionError, RunnerError
from interloper.operation import Operation, OperationResult
from interloper.runner import Run
from interloper.runner.results import ExecutionStatus


@il.asset
def first() -> list[dict[str, Any]]:
    """An upstream asset."""
    return [{"a": 1}]


@il.asset
def second(first: il.Upstream) -> list[dict[str, Any]]:
    """A downstream asset."""
    return first.records


class ScriptedRun(Run):
    """A run whose operations return scripted results, recording what executed."""

    results: dict[str, OperationResult] = {}
    executed: list[str] = []

    async def execute(self, operation: Operation) -> OperationResult:
        self.executed.append(operation.key)
        await asyncio.sleep(0)
        return self.results.get(operation.key, OperationResult())


class ScriptedRunner(il.AsyncRunner):
    """An in-process runner whose run is scripted."""

    Run = ScriptedRun


def _dag() -> il.DAG:
    destination = il.MemoryDestination()
    upstream = first(destinations=[destination])
    return il.DAG(upstream, second(destinations=[destination], first=upstream))


class TestWalk:
    async def test_operations_execute_in_dependency_order(self):
        ScriptedRun.executed = []
        result = await ScriptedRunner().run(_dag())
        assert result.status is ExecutionStatus.COMPLETED
        assert ScriptedRun.executed == ["first", "second"]

    async def test_a_failure_cancels_its_dependents(self):
        ScriptedRun.executed = []
        ScriptedRun.results = {"first": OperationResult(error="boom")}
        try:
            result = await ScriptedRunner(fail_fast=False).run(_dag())
        finally:
            ScriptedRun.results = {}
        statuses = {info.component_key: info.status for info in result.executions.values()}
        assert statuses == {"first": ExecutionStatus.FAILED, "second": ExecutionStatus.CANCELED}

    async def test_a_raising_execute_is_the_runs_error(self):
        class Faulty(ScriptedRun):
            async def execute(self, operation: Operation) -> OperationResult:
                raise RuntimeError("runner bug")

        class FaultyRunner(il.AsyncRunner):
            Run = Faulty

        result = await FaultyRunner().run(_dag())
        assert result.status is ExecutionStatus.FAILED
        with pytest.raises(RuntimeError, match="runner bug"):
            await FaultyRunner(reraise=True).run(_dag())

    async def test_a_failing_close_does_not_mask_the_result(self):
        class Leaky(ScriptedRun):
            async def close(self) -> None:
                raise OSError("could not release")

        class LeakyRunner(il.AsyncRunner):
            Run = Leaky

        result = await LeakyRunner().run(_dag())
        assert result.status is ExecutionStatus.COMPLETED

    async def test_one_runner_serves_two_concurrent_runs(self):
        events: list[Event] = []
        runner = ScriptedRunner(on_event=events.append)
        left, right = await asyncio.gather(
            runner.run(_dag(), metadata={"run_id": "left"}),
            runner.run(_dag(), metadata={"run_id": "right"}),
        )
        assert left.status is right.status is ExecutionStatus.COMPLETED
        started = [event.metadata["run_id"] for event in events if event.type is EventType.RUN_STARTED]
        assert sorted(started) == ["left", "right"]

    async def test_a_window_no_operation_can_serve_fails_before_anything_emits(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def daily() -> list[dict[str, Any]]:
            """A daily asset."""
            return []

        events: list[Event] = []
        with pytest.raises(PartitionError):
            await ScriptedRunner(on_event=events.append).run(il.DAG(daily()), window=None)
        assert events == []


class TestReraise:
    async def test_the_original_exception_when_in_process(self):
        @il.asset
        def broken() -> list[dict[str, Any]]:
            """An asset that raises."""
            raise KeyError("missing")

        with pytest.raises(KeyError):
            await il.AsyncRunner(reraise=True).run(il.DAG(broken()))

    async def test_a_runner_error_when_only_the_message_crossed(self):
        ScriptedRun.results = {"first": OperationResult(error="child failed")}
        try:
            with pytest.raises(RunnerError, match="child failed"):
                await ScriptedRunner(reraise=True, fail_fast=False).run(_dag())
        finally:
            ScriptedRun.results = {}
```

`packages/interloper-core/tests/runner/test_serial.py`:

```python
"""Tests for ``interloper.runner.serial``."""

import interloper as il


def test_a_serial_runner_runs_one_operation_at_a_time():
    """Its capacity is one slot, and its run is the in-process run."""
    runner = il.SerialRunner()
    assert runner.capacity == 1
    assert il.SerialRunner.Run is il.AsyncRunner.Run
```

In `packages/interloper-core/tests/runner/test_state.py`, add (porting is Step 7):

```python
class TestVerbs:
    def _state(self, reports_progress: bool = True) -> tuple[RunState, il.Asset]:
        asset = _Asset(id="asset-1")
        return RunState(il.DAG(asset), None, {"run_id": "run-1"}, reports_progress=reports_progress), asset

    def test_retry_reports_and_advances_the_attempt(self):
        state, asset = self._state()
        with _capture() as events:
            state.start(asset)
            state.retry(asset, "transient")
        retried = [event for event in events if event.type is EventType.OPERATION_RETRIED]
        assert retried[0].metadata["attempt"] == 1
        assert state.executions[asset.id].attempt == 2

    def test_a_run_whose_child_reports_progress_records_starts_and_retries_silently(self):
        state, asset = self._state(reports_progress=False)
        with _capture() as events:
            state.start(asset)
            state.retry(asset, "transient")
            state.finish(asset, OperationResult())
        assert [event.type for event in events] == [EventType.OPERATION_COMPLETED]
        assert state.executions[asset.id].status is ExecutionStatus.COMPLETED

    def test_finish_keeps_the_result(self):
        state, asset = self._state()
        result = OperationResult(error="boom", state={"slot": 1})
        state.start(asset)
        state.finish(asset, result)
        assert state.executions[asset.id].result is result
        assert state.failed == [asset]
```

(`_Asset`, `_capture` as in the folded `test_state_event_ids.py`; import `OperationResult` from `interloper.operation`.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/runner/test_run.py packages/interloper-core/tests/runner/test_serial.py packages/interloper-core/tests/runner/test_state.py -q`
Expected: import error for `interloper.runner.Run`; `capacity`, `RunState(..., reports_progress=...)`, `retry`, `finish` missing.

- [ ] **Step 3: `ExecutionInfo`**

In `packages/interloper-core/src/interloper/runner/results.py`, `ExecutionInfo` becomes:

```python
@dataclass
class ExecutionInfo:
    """One operation's record in a run: its status, its attempt, when it ran, and its result.

    ``result`` is set when the operation finished, either way; ``None`` for an
    operation skipped, canceled or still running.
    """

    component_id: str
    component_key: str
    status: ExecutionStatus
    attempt: int = 1
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    result: OperationResult | None = None

    @property
    def error(self) -> str | None:
        """The failure's message, ``None`` unless the operation failed."""
        return self.result.error if self.result is not None else None

    @property
    def traceback(self) -> str | None:
        """The failure's traceback, when one was captured."""
        return self.result.traceback if self.result is not None else None

    @property
    def execution_time(self) -> float | None:
        """Seconds between start and end, ``None`` until both are known."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def is_terminal(self) -> bool:
        """Whether the operation reached a final status: completed, failed, canceled or skipped."""
        return self.status in _TERMINAL_STATUSES

    def start(self) -> None:
        """Mark the operation running, from now."""
        self.status = ExecutionStatus.RUNNING
        self.start_time = dt.datetime.now(dt.timezone.utc)

    def finish(self, result: OperationResult) -> None:
        """Record what the operation came to: completed when it succeeded, failed otherwise.

        Args:
            result: The operation's result.
        """
        self.status = ExecutionStatus.COMPLETED if result.succeeded else ExecutionStatus.FAILED
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.result = result

    def cancel(self) -> None:
        """Mark the operation canceled, from now."""
        self.status = ExecutionStatus.CANCELED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record to a plain dict, with the keys it always had.

        Returns:
            The record as JSON-able values.
        """
        return {
            "component_id": self.component_id,
            "component_key": self.component_key,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_time": self.execution_time,
            "error": self.error,
            "traceback": self.traceback,
        }
```

`RunResult` is unchanged.

- [ ] **Step 4: `RunState`**

Rewrite `packages/interloper-core/src/interloper/runner/state.py` (keep the module comment on the event-id namespace, and `operation_event_id` exactly as it is):

```python
class RunState:
    """The operations of one run: their statuses, attempts and results, and the events their transitions emit.

    Every transition is one verb, and emits its operation-lifecycle event with
    a deterministic id. With ``reports_progress`` off, a start and a retry are
    recorded without an event: the operation executes in a child process that
    reports its own, while every verdict is still recorded, and its terminal
    event authored, here.
    """

    _OPERATION_EVENT_NS = uuid.UUID("a3f1c2d4-5b6e-4a7c-9d8f-0e1a2b3c4d5e")

    def __init__(
        self,
        dag: DAG,
        window: PartitionWindow | None,
        metadata: dict[str, Any],
        *,
        reports_progress: bool = True,
    ) -> None:
        """Track a DAG's operations for one run, roots ready.

        Args:
            dag: The DAG the run walks.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
            reports_progress: Whether starts and retries emit events here.
        """
        self.dag = dag
        self.window = window
        self.metadata = metadata
        self.reports_progress = reports_progress
        self.executions: dict[str, ExecutionInfo] = {
            operation.id: ExecutionInfo(
                component_id=operation.id,
                component_key=operation.key,
                status=ExecutionStatus.QUEUED if operation.enabled else ExecutionStatus.SKIPPED,
            )
            for operation in dag.operations
        }
        for operation in dag.operations:
            self._promote(operation.id)

    # properties: run_id, ready, completed, failed (lists in DAG order, as today's
    # *_operations), done (every execution terminal)

    def context(self, operation: Operation) -> OperationContext:
        """The facts an operation executes with: its own window, the DAG and the run's metadata.

        Args:
            operation: The operation about to execute.

        Returns:
            Its context.
        """
        return OperationContext(window=operation.effective_window(self.window), dag=self.dag, metadata=self.metadata)

    def queue(self) -> None:
        """Announce every operation that will execute (``OPERATION_QUEUED``)."""

    def start(self, operation: Operation) -> None:
        """Mark an operation running (``OPERATION_STARTED``)."""

    def retry(self, operation: Operation, error: str) -> None:
        """Record a failed attempt that will be retried (``OPERATION_RETRIED``), and open the next one."""

    def finish(self, operation: Operation, result: OperationResult) -> None:
        """Record what an operation came to.

        Completed: its dependents whose every predecessor is done become ready
        (``OPERATION_COMPLETED``). Failed: its dependents are canceled
        (``OPERATION_FAILED``, then ``OPERATION_CANCELED`` for each).
        """

    def cancel_pending(self) -> None:
        """Cancel every operation that never reached a final status (``OPERATION_CANCELED``)."""
```

Implement each verb from today's `mark_*` body (messages exactly as today: `"Operation '{key}' queued"`, `"... started"`, `"... failed on attempt {attempt}, retrying: {error}"`, `"... completed"`, `"... failed: {error}"` with `error` and, when present, `traceback` in the metadata, `"... canceled"`, `"... canceled (upstream failure)"`), through one private emitter:

```python
    def _emit(self, event_type: EventType, operation: Operation, message: str, **extra: Any) -> None:
        """Emit an operation-lifecycle event with its deterministic id.

        Args:
            event_type: The lifecycle event.
            operation: The operation it is about.
            message: The event's message.
            **extra: More metadata (``error``, ``traceback``).
        """
        attempt = self.executions[operation.id].attempt
        metadata = operation.event_metadata(self.metadata, self.window, attempt=attempt, message=message, **extra)
        event_id = self.operation_event_id(self.run_id, operation.id, event_type, attempt=attempt)
        EventBus.emit_event(Event(type=event_type, metadata=metadata, id=event_id))
```

- `start` and `retry` emit only when `self.reports_progress`; `retry` emits with the current attempt, then increments `ExecutionInfo.attempt`.
- `finish` calls `ExecutionInfo.finish(result)`; on success `_promote` each successor, on failure `_cancel_dependents` (today's `_propagate_failure`, calling `ExecutionInfo.cancel()`), then emits.
- `_promote(operation_id)` is today's root promotion and `_promote_dependents` merged: an operation still `QUEUED` whose every predecessor is completed or skipped becomes `READY`.
- Removed: `start_run`, `end_run`, `mark_*`, `attempts`, `elapsed_time`, `start_time`, `end_time`, `backfill_id`, `queued_operations`, `running_operations`, `_operation_event_metadata`, `_emit_operation_event`.

- [ ] **Step 5: `Run`**

`packages/interloper-core/src/interloper/runner/run.py`:

```python
"""Run: one run of a runner, walking its DAG and executing each operation where the runner says."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import Self

from interloper.errors import PartitionError, RunnerError, format_exception
from interloper.events import EventBus, EventType
from interloper.runner.results import ExecutionStatus, RunResult
from interloper.runner.state import RunState

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.operation.base import Operation, OperationResult
    from interloper.partitioning.base import PartitionWindow
    from interloper.runner.base import Runner

logger = logging.getLogger(__name__)


class Run(ABC):
    """One run of a runner: where its operations execute, and what it holds while the run lasts.

    A runner is configuration; a run is made from it for one DAG and one
    window, walks the DAG once and is closed. Each kind of run says where an
    operation executes (`execute`) and what it holds while the run lasts (its
    attributes, released by `close`); the walk, the run's own lifecycle events
    and its result are the same for every kind.

    ``reports_progress`` is off for a run whose operations execute in a child
    process that reports its own start and retries.
    """

    reports_progress: ClassVar[bool] = True

    def __init__(self, runner: Runner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Make a run of a runner, checking the window before anything is emitted.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers, ``None`` when unscoped.
            metadata: Run-level metadata, carrying the ``run_id``.
        """
        self._check_window(dag, window)
        self.runner = runner
        self.state = RunState(dag, window, metadata, reports_progress=self.reports_progress)
        self.fault: Exception | None = None
        self.started_at = dt.datetime.now(dt.timezone.utc)

    async def __aenter__(self) -> Self:
        """Enter the run.

        Returns:
            This run.
        """
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Close the run; a failure to release is logged, never masking the result.

        Args:
            *exc_info: The exception leaving the block, if any; not suppressed.
        """
        try:
            await self.close()
        except Exception:
            logger.warning("Closing run %s failed", self.state.run_id, exc_info=True)

    # -- The walk ---------------------------------------------------------------

    async def walk(self) -> RunResult:
        """Execute every operation in dependency order, at most ``capacity`` at a time.

        Operations whose dependencies are met start as slots free up; each
        result is recorded as it lands. Under ``fail_fast`` nothing new starts
        once an operation failed; what is in flight always finishes. An
        ``execute`` that raises, rather than returning a failed result, is a
        fault of the runner itself and becomes the run's error.

        Returns:
            The run's result.
        """
        self.start()
        inflight: dict[asyncio.Task[OperationResult], Operation] = {}
        try:
            await self._schedule(inflight)
        except Exception as fault:  # noqa: BLE001 - a fault of the runner itself becomes the run's error
            self.fault = fault
        finally:
            await self._drain(inflight)
        self.state.cancel_pending()
        return self.finish(format_exception(self.fault) if self.fault is not None else None)

    async def _schedule(self, inflight: dict[asyncio.Task[OperationResult], Operation]) -> None:
        """Start ready operations and record results until the run is done or stops early.

        Args:
            inflight: The tasks in flight, by operation; updated in place.

        Raises:
            RunnerError: If nothing is ready, nothing is in flight and the run is not done.
        """
        while not self.state.done:
            for operation in self.state.ready[: self.runner.capacity - len(inflight)]:
                self.state.start(operation)
                inflight[asyncio.create_task(self.execute(operation))] = operation
            if not inflight:
                raise RunnerError(
                    "No operations ready but execution not complete. "
                    "This indicates a circular dependency or invalid DAG state."
                )
            finished, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            for task in finished:
                self.state.finish(inflight.pop(task), task.result())
            if self.runner.fail_fast and self.state.failed:
                return

    async def _drain(self, inflight: dict[asyncio.Task[OperationResult], Operation]) -> None:
        """Let every task in flight finish, recording what each came to.

        Args:
            inflight: The tasks in flight, by operation; emptied.
        """
        if not inflight:
            return
        finished, _ = await asyncio.wait(inflight)
        for task in finished:
            operation = inflight.pop(task)
            if task.exception() is None:
                self.state.finish(operation, task.result())
            elif self.fault is None:
                self.fault = task.exception()

    # -- The run's lifecycle ------------------------------------------------------

    def start(self) -> None:
        """Start the run: ``RUN_STARTED``, then ``OPERATION_QUEUED`` for every operation that will execute."""
        EventBus.emit(
            EventType.RUN_STARTED,
            metadata={
                **self.state.metadata,
                "window": str(self.state.window) if self.state.window else None,
                "message": f"Run started ({len(self.state.dag.operations)} operations)",
            },
        )
        self.state.queue()

    def finish(self, error: str | None = None) -> RunResult:
        """Finish the run: ``RUN_COMPLETED`` or ``RUN_FAILED``, and its result.

        Args:
            error: The runner's own fault, ``None`` when there was none.

        Returns:
            The run's result.
        """
        # messages exactly as today's RunState.end_run
        ...
        return RunResult(
            window=self.state.window,
            status=status,
            executions=dict(self.state.executions),
            execution_time=(dt.datetime.now(dt.timezone.utc) - self.started_at).total_seconds(),
        )

    def reraise(self) -> None:
        """Raise what failed the run, for a caller that asked for exceptions.

        Raises:
            Exception: The runner's own fault; else the first failed operation's
                original exception when it executed in this process.
            RunnerError: When the failure's exception stayed in another process.
        """
        if self.fault is not None:
            raise self.fault
        if not self.state.failed:
            return
        operation = self.state.failed[0]
        info = self.state.executions[operation.id]
        if info.result is not None and isinstance(info.result.exception, Exception):
            raise info.result.exception
        raise RunnerError(f"Operation '{operation.key}' failed: {info.error}")

    # -- The kind of run ----------------------------------------------------------

    @abstractmethod
    async def execute(self, operation: Operation) -> OperationResult:
        """Execute one operation and return what it came to; never raise for the operation's own failure.

        Args:
            operation: The operation to execute.

        Returns:
            Its result.
        """

    async def close(self) -> None:
        """Release what the run holds; nothing by default."""

    @staticmethod
    def _check_window(dag: DAG, window: PartitionWindow | None) -> None:
        """Check the window against every enabled partitioned operation, listing every failure.

        Args:
            dag: The DAG whose operations are checked.
            window: The run's window.

        Raises:
            PartitionError: If the window is not valid for at least one enabled partitioned operation.
        """
        # today's Runner._preflight body, unchanged
```

Write `finish` out in full from today's `RunState.end_run` (status `FAILED` when any operation failed or `error` is set; the three messages; `"error"` and `"window"` in the metadata) and `_check_window` from today's `Runner._preflight`. In the `finish` stub above, the `...` and the comment are where that body goes; the shipped code has neither.

- [ ] **Step 6: `Runner`, `InProcessRun`, `AsyncRunner`**

`packages/interloper-core/src/interloper/runner/base.py`: keep `RUNNERS` and `from_settings`; replace the rest of the class with:

```python
class Runner(Serializable):
    """Configuration a run is made from: how many operations run at once, where, and what a failure does.

    A runner holds nothing per run, so one runner serves any number of runs,
    concurrently too. Each kind of runner names its kind of `Run`, which owns
    the walk, what the run holds, and where an operation executes. ``await``
    `run` from async code, or drive it from a sync entrypoint with
    `interloper.run`:

    ```py
    result = await runner.run(dag)     # async
    result = il.run(runner.run(dag))   # sync edge
    ```
    """

    Run: ClassVar[type[Run]]

    fail_fast: bool = False
    reraise: bool = True
    on_event: Callable[[Event], None] | None = Field(default=None, exclude=True, repr=False)

    @property
    @abstractmethod
    def capacity(self) -> int:
        """How many operations a run executes at once."""

    async def run(
        self,
        dag: DAG,
        window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """Run the DAG and return the result.

        Events carrying the run's id reach ``on_event`` (when set) before this
        returns. The run is traced as ``interloper.runner.run``, under the
        parent span the metadata or the environment carries. Operation failures
        are recorded on the result; with ``reraise`` set, the first one is
        raised once the run has finished.

        Args:
            dag: The DAG to run.
            window: The window the run covers; a partition is accepted as the window of one.
            metadata: Run-level metadata (``run_id``, ``backfill_id``, ...); a ``run_id`` is generated when absent.

        Returns:
            The run's result.
        """
        window = PartitionWindow.of(window)
        metadata = dict(metadata or {})
        metadata.setdefault("run_id", str(uuid.uuid4()))
        subscription = RunSubscription(self.on_event, metadata["run_id"]) if self.on_event else nullcontext()
        async with subscription:
            parent = extract_metadata(metadata) or context_from_env()
            with tracer().start_as_current_span(
                "interloper.runner.run", context=parent, attributes=self._span_attributes(metadata, window)
            ) as span:
                inject_metadata(metadata)
                async with self.Run(self, dag, window, metadata) as run:
                    result = await run.walk()
                if result.status is ExecutionStatus.FAILED:
                    span.set_status(StatusCode.ERROR, f"{len(result.failed_ids)} operation(s) failed")
        if self.reraise:
            run.reraise()
        return result

    def _span_attributes(self, metadata: dict[str, Any], window: PartitionWindow | None) -> dict[str, str]:
        """The run span's attributes: the metadata's, the runner's key and the window.

        Args:
            metadata: Run-level metadata.
            window: The run's window.

        Returns:
            The attributes.
        """
        span_attributes = attributes.from_metadata(metadata)
        span_attributes[attributes.RUNNER_TYPE] = self.key
        if window is not None:
            span_attributes[attributes.PARTITION] = str(window)
        return span_attributes
```

`packages/interloper-core/src/interloper/runner/async_runner.py`:

```python
"""AsyncRunner: every operation in this process, on one event loop."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from interloper.operation.base import OperationResult
from interloper.runner.base import Runner
from interloper.runner.run import Run

if TYPE_CHECKING:
    from interloper.operation.base import Operation


class InProcessRun(Run):
    """One run of an AsyncRunner: every operation executes in this process, on its event loop.

    A sync ``data()`` is offloaded to a thread by the asset itself, so one
    event loop serves the whole run.
    """

    async def execute(self, operation: Operation) -> OperationResult:
        """Execute the operation here, under its retry policy, reporting each retry as it happens.

        Args:
            operation: The operation to execute.

        Returns:
            Its result.
        """
        retry = partial(self.state.retry, operation)
        return await OperationResult.from_execution(operation, self.state.context(operation), on_retry=retry)


class AsyncRunner(Runner):
    """Run every operation in this process: ``max_workers`` at a time, sync ones offloaded to threads.

    ``AsyncRunner(max_workers=1)`` is serial and deterministic (see
    `SerialRunner`); the default runs four operations at once:

    ```py
    result = await AsyncRunner(max_workers=2, on_event=log_event).run(dag)
    result = il.run(AsyncRunner(on_event=log_event).run(dag))
    ```
    """

    Run = InProcessRun

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    @property
    def capacity(self) -> int:
        """How many operations a run executes at once: ``max_workers``."""
        return self.max_workers
```

`serial.py` keeps `SerialRunner(AsyncRunner)` with `max_workers: int = 1`.

`runner/__init__.py` exports `Run` and `RunState` beside the runners, and drops `SyncRunner` and `MultiProcessRunner`. `interloper/__init__.py` drops `MultiProcessRunner` (import and `__all__`). `pyproject.toml` drops the `multi_process` entry point. Delete `sync_runner.py`, `multi_process.py`, their tests and `examples/runner/multi_process.py`.

- [ ] **Step 7: Port the existing tests**

| Old | New |
|---|---|
| `RunState(dag, metadata={...})` then `state.start_run(window)` | `RunState(dag, window, {...})` then `state.queue()`; a `RUN_STARTED` assertion moves to a `Run` test |
| `state.mark_running(op)` / `mark_completed(op, effects=e)` / `mark_failed(op, msg, tb=..., effects=e, exception=x)` | `state.start(op)` / `state.finish(op, e)` / `state.finish(op, OperationResult(error=msg, traceback=..., exception=x))` |
| `mark_*(..., emit=False)` | `RunState(..., reports_progress=False)` for start and retry; a finish always emits |
| `state.mark_retried(op, error)` | `state.retry(op, error)` |
| `state.attempts[id]` | `state.executions[id].attempt` |
| `state.end_run(...)`, `elapsed_time` | `Run.finish(...)` through a run, or the `RunResult` |
| `*_operations` properties | `ready`, `completed`, `failed` |
| `info.mark_running()` / `mark_completed()` / `mark_failed(...)` / `mark_canceled()` | `info.start()` / `info.finish(OperationResult())` / `info.finish(OperationResult(error=...))` / `info.cancel()` |
| `info.effects` | `info.result` |
| `runner.state`, `runner._state` | the `RunResult`, or a scripted `Run` |

- `tests/runner/test_state_event_ids.py`: move its tests into `tests/runner/test_state.py` (event ids unchanged), then delete the file.
- `tests/runner/test_async_runner.py`: keep every test; `stalled_state` patches `RunState.ready` to return `[]` with operations still pending; `test_a_task_level_exception_aborts_the_walk` patches `InProcessRun.execute` to raise; `test_a_teardown_failure_does_not_mask_the_result` is covered by `test_run.py`'s `test_a_failing_close_does_not_mask_the_result` (delete the old one); `test_an_operation_still_listed_as_ready_is_not_scheduled_twice` keeps its assertion (a started operation leaves `ready`).
- `tests/operation/test_base.py` lines comparing `info.effects == OperationResult(error="curated", state={"failed": True})`: compare `(info.result.error, info.result.state)` instead (a failed result now carries its traceback and exception).
- `packages/interloper-scheduler/tests/test_executor.py`: `info.effects = effects` becomes `info.result = effects`.

- [ ] **Step 8: The scheduler reads the result**

In `packages/interloper-scheduler/src/interloper_scheduler/executor.py`, `_apply_effects` reads `info.result` (and its docstring says "result").

- [ ] **Step 9: Run the suites this task owns**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core packages/interloper-scheduler packages/interloper-db packages/interloper-toolkit -q -p no:cacheprovider`
Expected: PASS.
Run: `UV_FROZEN=1 uv run ruff check packages && UV_FROZEN=1 uv run ty check packages/interloper-core packages/interloper-scheduler`
Expected: clean (the Docker and Kubernetes packages still import `SyncRunner` until Tasks 4 and 5).

- [ ] **Step 10: Checkpoint (no commit)**

---

### Task 4: ContainerRun

**Files:**
- Modify: `packages/interloper-docker/src/interloper_docker/runner.py`
- Test: `packages/interloper-docker/tests/test_runner.py` (with `test_runner_terminal_events.py` folded in and deleted)

**Interfaces:**
- Consumes: `Run`, `Runner`, `EventLogReader`, `OperationResult`.
- Produces: `ContainerRun(Run)` with `reports_progress = False`, `execute`, `follow(operation) -> OperationResult`, `command(operation) -> list[str]`, `name(operation) -> str`, `close()`; `DockerRunner(Runner)` with `Run = ContainerRun`, `capacity -> max_containers`, `environment() -> dict[str, str]`, `mounts() -> dict[str, dict[str, str]]`.

- [ ] **Step 1: Write the failing tests**

Replace the scheduling and terminal-event tests in `packages/interloper-docker/tests/test_runner.py` (keep the command tests, now on `ContainerRun.command`) with tests over a fake client:

```python
class FakeContainer:
    """A container whose log and exit code are scripted."""

    def __init__(self, chunks: list[bytes], status_code: int = 0, broken: bool = False) -> None:
        self.chunks = chunks
        self.status_code = status_code
        self.broken = broken
        self.id = "c0ffee0000000000"
        self.short_id = "c0ffee000000"
        self.removed = False

    def logs(self, **kwargs: Any) -> Iterator[bytes]:
        yield from self.chunks
        if self.broken:
            raise requests.exceptions.ConnectionError("stream reset")

    def wait(self) -> dict[str, int]:
        return {"StatusCode": self.status_code}

    def remove(self) -> None:
        self.removed = True


class FakeDocker:
    """A Docker client whose containers are scripted, or whose launch fails."""

    def __init__(self, container: FakeContainer | None) -> None:
        self.container = container
        self.containers = self

    def run(self, **kwargs: Any) -> FakeContainer:
        if self.container is None:
            raise docker.errors.APIError("no such image")
        return self.container


def _event_line(component_id: str, event_type: il.EventType) -> bytes:
    event = il.Event(type=event_type, metadata={"component_id": component_id, "run_id": "run-1"})
    return (EVENT_LINE_PREFIX + event.to_json() + "\n").encode()


@pytest.fixture
def asset() -> il.Asset:
    @il.asset
    def contained() -> list[dict[str, Any]]:
        """An asset executed in a container."""
        return []

    return contained(id="asset-1")


def _run(monkeypatch, container: FakeContainer | None, asset: il.Asset) -> tuple[il.RunResult, list[il.Event]]:
    monkeypatch.setattr(docker, "from_env", lambda: FakeDocker(container))
    events: list[il.Event] = []
    runner = DockerRunner(image="img", on_event=events.append)
    result = il.run(runner.run(il.DAG(asset), metadata={"run_id": "run-1"}))
    return result, events


class TestContainerRun:
    def test_forwards_the_childs_events_before_recording_the_verdict(self, monkeypatch, asset):
        container = FakeContainer([_event_line("asset-1", il.EventType.ASSET_DATA_COMPLETED)])
        result, events = _run(monkeypatch, container, asset)
        types = [event.type for event in events]
        assert types.index(il.EventType.ASSET_DATA_COMPLETED) < types.index(il.EventType.OPERATION_COMPLETED)
        assert result.status is il.ExecutionStatus.COMPLETED
        assert container.removed

    def test_the_host_reports_no_start_of_its_own(self, monkeypatch, asset):
        _, events = _run(monkeypatch, FakeContainer([]), asset)
        assert il.EventType.OPERATION_STARTED not in [event.type for event in events]

    def test_a_failed_exit_is_a_failed_operation_with_the_hosts_terminal_event(self, monkeypatch, asset):
        result, events = _run(monkeypatch, FakeContainer([], status_code=3), asset)
        [failed] = [event for event in events if event.type is il.EventType.OPERATION_FAILED]
        assert failed.id == RunState.operation_event_id("run-1", "asset-1", il.EventType.OPERATION_FAILED)
        assert "exited with code 3" in (result.executions["asset-1"].error or "")

    def test_a_launch_failure_is_a_failed_operation(self, monkeypatch, asset):
        result, _ = _run(monkeypatch, None, asset)
        assert "no such image" in (result.executions["asset-1"].error or "")

    def test_a_broken_log_stream_still_returns_the_verdict(self, monkeypatch, asset):
        container = FakeContainer([_event_line("asset-1", il.EventType.ASSET_DATA_STARTED)], broken=True)
        result, events = _run(monkeypatch, container, asset)
        assert il.EventType.ASSET_DATA_STARTED in [event.type for event in events]
        assert result.status is il.ExecutionStatus.COMPLETED
```

(Imports: `from collections.abc import Iterator`, `from typing import Any`, `import docker`, `import pytest`, `import requests`, `import interloper as il`, `from interloper.events.stderr import EVENT_LINE_PREFIX`, `from interloper.runner import RunState`, `from interloper_docker.runner import ContainerRun, DockerRunner`.) Delete `tests/test_runner_terminal_events.py`: its cases are the failed-exit, completed and launch-failure tests above.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-docker -q`
Expected: FAIL (import of `SyncRunner` removed in Task 3; `ContainerRun` missing).

- [ ] **Step 3: Implement**

Rewrite `packages/interloper-docker/src/interloper_docker/runner.py` (module docstring as today, describing the mini-DAG, the `@EVENT:` stream and the host-authored terminal event):

```python
class ContainerRun(Run):
    """One run of a DockerRunner: each operation in its own container, its events read from the container's log."""

    runner: DockerRunner
    reports_progress = False

    def __init__(self, runner: DockerRunner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Open the Docker client and a polling pool sized to the runner's capacity.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers.
            metadata: Run-level metadata.
        """
        super().__init__(runner, dag, window, metadata)
        self.docker = docker.from_env()
        self.polling = ThreadPoolExecutor(max_workers=runner.max_containers)

    async def execute(self, operation: Operation) -> OperationResult:
        """Run the operation in its own container, waiting on the polling pool.

        Args:
            operation: The operation to execute.

        Returns:
            Its result.
        """
        return await asyncio.get_running_loop().run_in_executor(self.polling, self.follow, operation)

    def follow(self, operation: Operation) -> OperationResult:
        """Start the operation's container, forward its log's events until it exits, then read its exit code.

        Args:
            operation: The operation the container executes.

        Returns:
            An empty result for exit code 0, a failure naming the code otherwise,
            or a failure naming why the container could not start.
        """
        try:
            container = self.docker.containers.run(
                image=self.runner.image,
                name=self.name(operation),
                command=self.command(operation),
                environment=self.runner.environment(),
                volumes=self.runner.mounts() or None,
                labels={"interloper.component_id": operation.id},
                remove=False,
                detach=True,
                stdout=True,
                stderr=True,
            )
        except docker.errors.DockerException as error:
            return OperationResult(error=format_exception(error))
        reader = EventLogReader(operation.id, label=f"container {container.short_id}")
        try:
            for chunk in container.logs(stream=True, follow=True, stdout=False, stderr=True):
                reader.feed(chunk)
        except Exception:  # noqa: BLE001 - a broken stream never decides the verdict; the exit code does
            logger.debug("Log stream of container %s ended early", container.short_id, exc_info=True)
        reader.close()
        status_code = container.wait().get("StatusCode", 1)
        if self.runner.auto_remove:
            container.remove()
        if status_code == 0:
            return OperationResult()
        return OperationResult(error=f"Container {container.short_id} exited with code {status_code}")

    def command(self, operation: Operation) -> list[str]:
        """The command the container runs: its operation's mini-DAG through ``interloper run --format inline``.

        Args:
            operation: The operation the container executes.

        Returns:
            The argv.
        """
        # today's _build_command body, reading the mini-DAG spec from
        # self.state.dag.mini_dag(operation.id), the window from
        # operation.effective_window(self.state.window), the run id from self.state.run_id

    def name(self, operation: Operation) -> str:
        """A container name unique to this operation and run.

        Args:
            operation: The operation the container executes.

        Returns:
            ``interloper_run_<run>_<operation>``.
        """
        return f"interloper_run_{self.state.run_id[:8]}_{operation.id[:8]}"

    async def close(self) -> None:
        """Wait for every container's polling to finish, and release the pool."""
        self.polling.shutdown(wait=True)


class DockerRunner(Runner):
    """Execute each operation in its own Docker container (docstring as today, without the polling details)."""

    Run = ContainerRun

    image: str = "interloper:latest-worker"
    max_containers: int = 4
    env_vars: dict[str, str] = Field(default_factory=dict)
    volumes: dict[str, dict[str, str]] | list[str] = Field(default_factory=dict)
    fail_fast: bool = False
    reraise: bool = False
    auto_remove: bool = True

    @property
    def capacity(self) -> int:
        """How many containers a run executes at once: ``max_containers``."""
        return self.max_containers

    def environment(self) -> dict[str, str]:
        """The environment every container gets (today's ``_build_env``)."""

    def mounts(self) -> dict[str, dict[str, str]]:
        """The volumes every container mounts, in docker-py's shape (today's ``_build_volumes``)."""
```

Write `command`, `environment` and `mounts` out in full from today's `_build_command`, `_build_env` and `_build_volumes`; the comments above mark where those bodies go and are not shipped. Removed: `model_post_init`'s client, `_poll_pool`, `_log_threads`, `_stop_log_streaming`, `_container_map`, `_on_start`, `_on_end`, `_submit_operation`, `_handle_completed`, `_poll_container`, `_start_log_streaming`, `_stop_container_log_streaming`, `_RUN_EVENTS`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core packages/interloper-docker -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Checkpoint (no commit)**

---

### Task 5: JobRun

**Files:**
- Modify: `packages/interloper-k8s/src/interloper_k8s/runner.py`
- Test: `packages/interloper-k8s/tests/test_runner.py` (with `test_runner_terminal_events.py` folded in and deleted)

**Interfaces:**
- Consumes: `Run`, `Runner`, `EventLogReader`, `OperationResult`.
- Produces: `JobRun(Run)` with `reports_progress = False`, `execute`, `follow(operation) -> OperationResult`, `job(operation, name) -> V1Job`, `pod(name) -> str | None`, `forward_log(pod, operation) -> None`, `outcome(name) -> bool | None`, `command(operation)`, `name(operation)`, `close()`; `KubernetesRunner(Runner)` with `Run = JobRun`, `capacity -> max_jobs`, `environment() -> list[V1EnvVar]`, `requirements() -> V1ResourceRequirements | None`, `pod_tolerations() -> list[V1Toleration]`.

- [ ] **Step 1: Write the failing tests**

Replace the scheduling and terminal-event tests in `packages/interloper-k8s/tests/test_runner.py` (keep the command tests, now on `JobRun.command`) with tests over fake clients, patched in through `kubernetes.config.load_incluster_config`, `kubernetes.client.BatchV1Api` and `kubernetes.client.CoreV1Api`:

```python
class FakeCluster:
    """A Job, its pod and its log, scripted for one operation."""

    def __init__(self, chunks: list[bytes], succeeded: bool = True, pod_starts: bool = True, broken: bool = False, creatable: bool = True) -> None:
        self.chunks, self.succeeded, self.pod_starts, self.broken, self.creatable = chunks, succeeded, pod_starts, broken, creatable
        self.created: list[V1Job] = []

    # BatchV1Api
    def create_namespaced_job(self, namespace: str, body: V1Job) -> None:
        if not self.creatable:
            raise ApiException(status=422, reason="Invalid")
        self.created.append(body)

    def read_namespaced_job_status(self, name: str, namespace: str) -> V1Job:
        status = V1JobStatus(succeeded=1) if self.succeeded else V1JobStatus(failed=1)
        return V1Job(status=status)

    # CoreV1Api
    def list_namespaced_pod(self, namespace: str, label_selector: str) -> V1PodList:
        if not self.pod_starts:
            return V1PodList(items=[])
        pod = V1Pod(metadata=V1ObjectMeta(name="pod-1"), status=V1PodStatus(phase="Succeeded"))
        return V1PodList(items=[pod])

    def read_namespaced_pod_log(self, **kwargs: Any) -> Iterator[bytes]:
        yield from self.chunks
        if self.broken:
            raise urllib3.exceptions.ProtocolError("stream reset")


@pytest.fixture
def cluster(monkeypatch) -> Callable[..., FakeCluster]:
    def install(**kwargs: Any) -> FakeCluster:
        fake = FakeCluster(**kwargs)
        monkeypatch.setattr(config, "load_incluster_config", lambda: None)
        monkeypatch.setattr(client, "BatchV1Api", lambda: fake)
        monkeypatch.setattr(client, "CoreV1Api", lambda: fake)
        return fake

    return install
```

(Imports beyond Task 4's: `import urllib3`, `from kubernetes import client, config`, `from kubernetes.client import V1Job, V1JobStatus, V1ObjectMeta, V1Pod, V1PodList, V1PodStatus`, `from kubernetes.client.exceptions import ApiException`. The broken streams raise what the transports really raise mid-stream, `requests` and `urllib3` errors, not the clients' own exception types. A test fixture may nest an `install` factory: this is test scaffolding, not a production shape.)

Tests, mirroring Task 4's with `KubernetesRunner(image="img", poll_interval=0.0)`:
- `test_forwards_the_childs_events_before_recording_the_verdict`
- `test_the_host_reports_no_start_of_its_own`
- `test_a_failed_job_is_a_failed_operation_with_the_hosts_terminal_event` (`succeeded=False`; the `OPERATION_FAILED` id equals `RunState.operation_event_id("run-1", "asset-1", ...)`)
- `test_a_job_that_cannot_be_created_is_a_failed_operation` (`creatable=False`)
- `test_a_broken_log_stream_still_returns_the_verdict` (`broken=True`)
- `test_a_job_that_fails_before_its_pod_starts_returns_the_failure` (`pod_starts=False, succeeded=False`: the run completes with the operation failed, no hang)
- `test_the_job_carries_the_runs_labels_and_the_pods_settings` (the created `V1Job`'s labels carry `interloper.run_id` and `interloper.component_id`, `backoff_limit == 0`, `ttl_seconds_after_finished` from the runner)

Write each in full, in Task 4's style. Delete `tests/test_runner_terminal_events.py`.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-k8s -q`
Expected: FAIL (`SyncRunner` import; `JobRun` missing).

- [ ] **Step 3: Implement**

Rewrite `packages/interloper-k8s/src/interloper_k8s/runner.py`:

```python
_STARTED = frozenset({"Running", "Succeeded", "Failed"})


class JobRun(Run):
    """One run of a KubernetesRunner: each operation in its own Job, its events read from the Job's log."""

    runner: KubernetesRunner
    reports_progress = False

    def __init__(self, runner: KubernetesRunner, dag: DAG, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Connect to the cluster and open a polling pool sized to the runner's capacity.

        Args:
            runner: The runner this is a run of.
            dag: The DAG to walk.
            window: The window the run covers.
            metadata: Run-level metadata.
        """
        super().__init__(runner, dag, window, metadata)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.batch = client.BatchV1Api()
        self.core = client.CoreV1Api()
        self.polling = ThreadPoolExecutor(max_workers=runner.max_jobs)

    async def execute(self, operation: Operation) -> OperationResult:
        """Run the operation in its own Job, waiting on the polling pool.

        Args:
            operation: The operation to execute.

        Returns:
            Its result.
        """
        return await asyncio.get_running_loop().run_in_executor(self.polling, self.follow, operation)

    def follow(self, operation: Operation) -> OperationResult:
        """Create the operation's Job, forward its log's events until the pod exits, then read the verdict.

        Args:
            operation: The operation the Job executes.

        Returns:
            An empty result when the Job succeeded, a failure naming it otherwise,
            or a failure naming why it could not be created.
        """
        name = self.name(operation)
        try:
            self.batch.create_namespaced_job(namespace=self.runner.namespace, body=self.job(operation, name))
        except ApiException as error:
            return OperationResult(error=format_exception(error))
        pod = self.pod(name)
        if pod is not None:
            self.forward_log(pod, operation)
        while (succeeded := self.outcome(name)) is None:
            time.sleep(self.runner.poll_interval)
        return OperationResult() if succeeded else OperationResult(error=f"Job {name} failed")

    def pod(self, name: str) -> str | None:
        """Wait for the Job's pod to start.

        Args:
            name: The Job's name.

        Returns:
            The pod's name, or ``None`` when the Job finished without one starting.
        """
        while True:
            try:
                pods = self.core.list_namespaced_pod(namespace=self.runner.namespace, label_selector=f"job-name={name}").items
            except Exception:  # noqa: BLE001 - a failed listing is retried; the Job status ends the wait
                logger.debug("Listing the pods of Job %s failed", name, exc_info=True)
                pods = []
            started = [pod for pod in pods if pod.status and pod.status.phase in _STARTED]
            if started:
                return started[0].metadata.name
            if self.outcome(name) is not None:
                return None
            time.sleep(self.runner.poll_interval)

    def forward_log(self, pod: str, operation: Operation) -> None:
        """Forward the operation's events from the pod's log until the log ends.

        Args:
            pod: The pod's name.
            operation: The operation the pod executes.
        """
        reader = EventLogReader(operation.id, label=f"pod {pod}")
        try:
            stream = self.core.read_namespaced_pod_log(
                name=pod, namespace=self.runner.namespace, container="interloper", follow=True, _preload_content=False
            )
            for chunk in stream:
                reader.feed(chunk)
        except Exception:  # noqa: BLE001 - a broken stream never decides the verdict; the Job status does
            logger.debug("Log stream of pod %s ended early", pod, exc_info=True)
        reader.close()

    def outcome(self, name: str) -> bool | None:
        """Whether the Job succeeded: ``True`` or ``False`` once finished, ``None`` while it runs.

        Args:
            name: The Job's name.

        Returns:
            The Job's outcome so far.
        """
        status = self.batch.read_namespaced_job_status(name=name, namespace=self.runner.namespace).status
        if status.succeeded:
            return True
        if status.failed:
            return False
        return None

    def job(self, operation: Operation, name: str) -> V1Job:
        """The Job that runs the operation's mini-DAG (today's V1Job construction in ``_submit_operation``).

        Args:
            operation: The operation the Job executes.
            name: The Job's name.

        Returns:
            The Job to create.
        """

    def command(self, operation: Operation) -> list[str]:
        """The command the pod runs (today's ``_build_command``, reading the mini-DAG, window and run id from this run)."""

    def name(self, operation: Operation) -> str:
        """A Job name unique to this operation and run, within Kubernetes' 63 characters."""
        return f"interloper-run-{self.state.run_id[:8]}-{operation.id[:8]}"[:63]

    async def close(self) -> None:
        """Wait for every Job's polling to finish, and release the pool."""
        self.polling.shutdown(wait=True)
```

Write `job` and `command` out in full from today's `_submit_operation` and `_build_command` (labels with `self.state.run_id`, `backoff_limit=0`, `ttl_seconds_after_finished`, the runner's `environment()`, `requirements()`, `pod_tolerations()`, image pull settings); the docstring-only stubs above mark where those bodies go. The `ApiException` import is `from kubernetes.client.exceptions import ApiException`.

`KubernetesRunner(Runner)` keeps its fields; `Run = JobRun`; `capacity -> max_jobs`; `environment()`, `requirements()` and `pod_tolerations()` are today's `_build_env`, `_build_resources` and `_build_tolerations`. Removed: `_batch_v1`, `_core_v1`, `_poll_pool`, `_job_map`, `_log_threads`, `_stop_log_streaming`, `_on_start`, `_on_end`, `_submit_operation`, `_handle_completed`, `_poll_job`, `_build_job_name`, `_start_log_streaming`, `_stop_job_log_streaming`, `_RUN_EVENTS`, the commented-out `to_slug_case` name.

- [ ] **Step 4: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green again (ruff, ty, every suite).

- [ ] **Step 5: Checkpoint (no commit)**

---

### Task 6: Surface, docs and live verification

**Files:**
- Modify: `docs/guide/execution.md`, `docs/extending/runners.md`, `docs/guide/cli.md`, `docs/guide/specs.md`, `docs/guide/telemetry.md`, `docs/reference/execution.md`, `README.md`, `plugins/interloper/skills/interloper-deploy/SKILL.md`, `plugins/interloper/skills/interloper-upgrade/SKILL.md`, `packages/interloper-core/src/interloper/settings.py` (runner descriptions), `packages/interloper-scheduler/src/interloper_scheduler/launcher.py` (docstrings), `packages/interloper-core/src/interloper/telemetry/propagation.py` (module docstring), `examples/telemetry/README.md`

- [ ] **Step 1: Find every stale mention**

Run: `grep -rnE "multi_process|MultiProcessRunner|SyncRunner|_run_blocking|_handle_completed|_submit_operation|_on_start|_on_end|mark_running|mark_completed|mark_failed|mark_retried|start_run|end_run|runner\.state|info\.effects" docs plugins examples README.md packages/*/src --include='*.md' --include='*.py' | grep -v docs/superpowers`
Expected: only the files listed above.

- [ ] **Step 2: Rewrite**

- `guide/execution.md`: the runner table lists `AsyncRunner`, `SerialRunner`, `DockerRunner`, `KubernetesRunner`; a paragraph on a runner being reusable configuration and a `Run` being one run of it; the option table (`fail_fast`, `reraise`, `on_event`) per runner.
- `extending/runners.md`: writing a runner is a `Runner` subclass naming its `Run`, and a `Run` subclass with `execute` and, when it holds something, `close`; show a complete, short example (a `Run` executing each operation on a thread pool through `OperationResult.from_execution`), and `reports_progress` for runs whose child reports its own progress.
- `guide/cli.md`, the settings descriptions and the scheduler launcher docstrings: `async`, `serial`, plus registered keys (`docker`, `kubernetes`).
- `guide/specs.md`, `guide/telemetry.md`, `telemetry/propagation.py`, `examples/telemetry/README.md`, `docs/reference/execution.md`: drop the multi-process mentions.
- `README.md`: the runner comment lists `SerialRunner`, `DockerRunner`, `KubernetesRunner`.
- Deploy skill: runner types `async`, `serial`, plus registered keys such as `k8s`.

- [ ] **Step 3: Upgrade table**

Add one row to `plugins/interloper/skills/interloper-upgrade/SKILL.md`, above the phase 2 row:

| unreleased (core phase 3) | `MultiProcessRunner` and `runner: {type: multi_process}`; `SyncRunner`; `runner.state`; `Runner._run`, `_on_start`/`_on_end`, `_submit_operation`, `_handle_completed`; `RunState.mark_*(..., emit=...)`, `start_run`/`end_run`, `RunState.attempts`; `ExecutionInfo.effects`/`.exception`/`mark_*` | `AsyncRunner`, or `DockerRunner`/`KubernetesRunner` for isolation; a custom runner names its `Run` (`Run = MyRun`) and implements `Run.execute(operation) -> OperationResult` (and `close()`); the run's state on the `RunResult`; `RunState.start/retry/finish/cancel_pending`, `Run.start/finish`; `ExecutionInfo.result` (`.error`, `.traceback` read from it), `ExecutionInfo.attempt`; `OperationResult.from_execution` |

- [ ] **Step 4: Full check and docs build**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -5 .superpowers/check.log`
Expected: all green.
Run: `UV_FROZEN=1 uv run zensical build --strict > .superpowers/docs.log 2>&1; echo $?`
Expected: `0`.

- [ ] **Step 5: Live verification**

Use the project `verify` skill: `INTERLOPER_SERVER_PORT=3100 make dev-up`, mint a session, run the demo source for one day through the API (the scheduler drives an `AsyncRunner` through the executor), assert the run succeeds and its events include `operation_started`, `operation_completed` and the asset steps with the right `run_id`, and that `RunResult`-derived executions show every demo asset completed. Tear down, delete the session row.

- [ ] **Step 6: Checkpoint (no commit)**
