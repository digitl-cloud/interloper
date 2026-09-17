# Retry Phase 1: Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An operation that fails is re-executed in place, within a declared attempt budget, without the run ever seeing the intermediate failure.

**Architecture:** One flat `RetryPolicy` carries the numbers. It is declared on the component whose
unit it governs: `Operation.retry` for an operation's execution, `Source.retry` as the default for its
assets, `Job.retry` for that job's runs (consumed in phase 2). `Operation.retryable(error)` is the
code-level classifier. The runner wraps its node execution in an attempt loop, emits
`OPERATION_RETRIED` for an attempt that will be retried, and reserves `OPERATION_FAILED` for
exhaustion, so an intermediate attempt is recorded without ever becoming the verdict.

**Tech Stack:** Python 3.10+, pydantic v2, asyncio, pytest, ruff, ty, uv workspace.

Spec: `docs/superpowers/specs/2026-09-17-retry-design.md`, sections 3 and 4.
Prerequisite: `docs/superpowers/plans/2026-09-17-operation-is-a-component.md` must be complete.
`Operation` is a `Component` and can carry a pydantic field.

## Global Constraints

- Line length 120, ruff-formatted. Type-checked with `ty`.
- Google-style docstrings on every module, class, function and method, with every applicable section
  (`Args:`, `Returns:`, `Raises:`). See `.claude/rules/python-style.md`.
- Comment sparingly. Never attribute-level comments on pydantic fields.
- Every concept gets a package with a `base.py`; `__init__.py` re-exports with an explicit `__all__`
  and defines nothing.
- Tests mirror the package layout one to one.
- Run checks from the repo root: `uv run ruff check`, `uv run ty check`, `uv run pytest`.
- **Do not commit without Guillaume asking.** The commit step records the intended message.

---

### Task 1: The RetryPolicy type

**Files:**
- Create: `packages/interloper-core/src/interloper/retry/__init__.py`
- Create: `packages/interloper-core/src/interloper/retry/base.py`
- Create: `packages/interloper-core/tests/retry/__init__.py`
- Create: `packages/interloper-core/tests/retry/test_base.py`
- Modify: `packages/interloper-core/src/interloper/__init__.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `interloper.RetryPolicy` with fields `max_attempts: int`, `delay: float`,
  `backoff: float`, `max_delay: float`, `jitter: float`, and methods `allows(attempt: int) -> bool`
  and `delay_before(attempt: int) -> float`. Every later task imports it from `interloper.retry`.

- [ ] **Step 1: Write the failing test**

Create `packages/interloper-core/tests/retry/__init__.py` (empty) and
`packages/interloper-core/tests/retry/test_base.py`:

```python
import pytest

from interloper.retry import RetryPolicy


def test_allows_up_to_max_attempts():
    policy = RetryPolicy(max_attempts=3)
    assert policy.allows(1)
    assert policy.allows(3)
    assert not policy.allows(4)


def test_first_attempt_has_no_delay():
    assert RetryPolicy(delay=10.0).delay_before(1) == 0.0


def test_delay_grows_geometrically():
    policy = RetryPolicy(delay=10.0, backoff=2.0, jitter=0.0)
    assert policy.delay_before(2) == 10.0
    assert policy.delay_before(3) == 20.0
    assert policy.delay_before(4) == 40.0


def test_delay_is_capped():
    policy = RetryPolicy(delay=10.0, backoff=10.0, max_delay=50.0, jitter=0.0)
    assert policy.delay_before(4) == 50.0


def test_jitter_stays_within_its_fraction():
    policy = RetryPolicy(delay=10.0, backoff=1.0, jitter=0.5)
    delays = [policy.delay_before(2) for _ in range(200)]
    assert all(5.0 <= delay <= 15.0 for delay in delays)
    assert len(set(delays)) > 1


def test_max_attempts_is_at_least_one():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest packages/interloper-core/tests/retry/test_base.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'interloper.retry'`.

- [ ] **Step 3: Write the implementation**

Create `packages/interloper-core/src/interloper/retry/base.py`:

```python
"""Retry: an attempt budget for a unit of work."""

from __future__ import annotations

import random

from pydantic import BaseModel, Field


class RetryPolicy(BaseModel):
    """How many times a unit of work is attempted, and how long between attempts.

    Declared on the component whose unit it governs: an operation's policy
    retries that operation's execution, a source's is the default for its
    assets, a job's retries that job's runs. The policy carries numbers only;
    whether a given error is worth another attempt is behaviour, and lives on
    ``Operation.retryable``.
    """

    max_attempts: int = Field(
        default=3,
        ge=1,
        title="Max attempts",
        description="Total attempts, including the first",
    )
    delay: float = Field(
        default=5.0,
        ge=0,
        title="Delay",
        description="Seconds to wait before the second attempt",
    )
    backoff: float = Field(
        default=2.0,
        ge=1,
        title="Backoff",
        description="Multiplier applied to the delay for each further attempt",
    )
    max_delay: float = Field(
        default=3600.0,
        ge=0,
        title="Max delay",
        description="Upper bound on any single delay, in seconds",
    )
    jitter: float = Field(
        default=0.1,
        ge=0,
        le=1,
        title="Jitter",
        description="Fraction of the delay spread randomly around it, so attempts do not align",
    )

    def allows(self, attempt: int) -> bool:
        """Whether the budget covers an attempt.

        Args:
            attempt: The attempt number, counting the first execution as 1.

        Returns:
            ``True`` while the attempt is within the budget.
        """
        return attempt <= self.max_attempts

    def delay_before(self, attempt: int) -> float:
        """How long to wait before an attempt.

        The growth is geometric from the second attempt (the first retry),
        capped at ``max_delay``, then spread by ``jitter`` so that operations
        failing together do not retry in lockstep.

        Args:
            attempt: The attempt number, counting the first execution as 1.

        Returns:
            The delay in seconds; ``0.0`` for the first attempt.
        """
        if attempt <= 1:
            return 0.0
        delay = min(self.delay * self.backoff ** (attempt - 2), self.max_delay)
        if not self.jitter:
            return delay
        spread = delay * self.jitter
        return max(0.0, random.uniform(delay - spread, delay + spread))
```

Create `packages/interloper-core/src/interloper/retry/__init__.py`:

```python
"""Retry policies."""

from interloper.retry.base import RetryPolicy

__all__ = ["RetryPolicy"]
```

- [ ] **Step 4: Export it from the package root**

In `packages/interloper-core/src/interloper/__init__.py`, add the import alongside the other
alphabetically-grouped imports and the name to `__all__`:

```python
from interloper.retry import RetryPolicy
```

```python
    "RetryPolicy",
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/retry/test_base.py -v`
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/interloper-core/src/interloper/retry packages/interloper-core/tests/retry packages/interloper-core/src/interloper/__init__.py
git commit -m "feat(core): add a retry policy

By Digitl"
```

---

### Task 2: Declare the policy on the components

**Files:**
- Modify: `packages/interloper-core/src/interloper/operation/base.py`
- Modify: `packages/interloper-core/src/interloper/source/base.py` (class body, and `_resolve` at :547)
- Modify: `packages/interloper-core/src/interloper/job/base.py:32-56`
- Test: `packages/interloper-core/tests/operation/test_base.py`
- Test: `packages/interloper-core/tests/source/test_base.py`
- Test: `packages/interloper-core/tests/job/test_base.py`

**Interfaces:**
- Consumes: `interloper.retry.RetryPolicy` from Task 1.
- Produces: `Operation.retry: RetryPolicy | None` (inherited by `Asset` and `Connection`),
  `Operation.retryable(error: Exception) -> bool`, `Source.retry`, `Job.retry`, and the
  source-to-asset inheritance in `Source._resolve`. Task 4 reads `operation.retry` and
  `operation.retryable`.

- [ ] **Step 1: Write the failing tests**

Add to `packages/interloper-core/tests/operation/test_base.py`:

```python
def test_operation_has_no_retry_policy_by_default():
    class Thing(Operation):
        kind: ClassVar[str] = "thing"

        async def execute(self, context: OperationContext) -> OperationResult:
            return OperationResult()

    assert Thing().retry is None


def test_every_error_is_retryable_by_default():
    class Thing(Operation):
        kind: ClassVar[str] = "thing"

        async def execute(self, context: OperationContext) -> OperationResult:
            return OperationResult()

    assert Thing().retryable(ValueError("nope")) is True


def test_retryable_can_be_narrowed():
    class Picky(Operation):
        kind: ClassVar[str] = "picky"

        async def execute(self, context: OperationContext) -> OperationResult:
            return OperationResult()

        def retryable(self, error: Exception) -> bool:
            return not isinstance(error, ValueError)

    assert Picky().retryable(TypeError()) is True
    assert Picky().retryable(ValueError()) is False
```

Add to `packages/interloper-core/tests/source/test_base.py`:

```python
def test_source_retry_policy_fills_its_assets():
    policy = il.RetryPolicy(max_attempts=5)

    @il.asset
    def one(**kwargs): ...

    @il.source(retry=policy)
    class MySource(il.Source):
        assets = (one,)

    source = MySource()
    assert source.assets[0].retry == policy


def test_an_assets_own_retry_policy_wins():
    source_policy = il.RetryPolicy(max_attempts=5)
    asset_policy = il.RetryPolicy(max_attempts=2)

    @il.asset(retry=asset_policy)
    def one(**kwargs): ...

    @il.source(retry=source_policy)
    class MySource(il.Source):
        assets = (one,)

    assert MySource().assets[0].retry == asset_policy
```

Match the `@il.source` declaration style already used in that test module; copy the surrounding
tests' shape for how assets are attached rather than inventing one.

Add to `packages/interloper-core/tests/job/test_base.py`:

```python
def test_job_carries_a_retry_policy():
    policy = il.RetryPolicy(max_attempts=2)
    assert il.Job(name="nightly", retry=policy).retry == policy
    assert il.Job(name="nightly").retry is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/operation/test_base.py packages/interloper-core/tests/source/test_base.py packages/interloper-core/tests/job/test_base.py -v -k retry`
Expected: FAIL. `retry` is not a field and `retryable` does not exist.

- [ ] **Step 3: Add the field and the classifier to Operation**

In `packages/interloper-core/src/interloper/operation/base.py`, import `RetryPolicy`
(`from interloper.retry import RetryPolicy`) and add the field to the class body next to
`materializable`:

```python
    retry: RetryPolicy | None = Field(
        default=None,
        title="Retry",
        description="Attempt budget for this operation's execution",
    )
```

Add the classifier in the `# -- Execution` section, directly above `failure`:

```python
    def retryable(self, error: Exception) -> bool:
        """Whether another attempt at this operation is worth making.

        Consulted by the runner before it spends an attempt from the budget.
        Override to recognise a permanent error, such as a vendor rejecting a
        request that will be rejected identically every time. The default is
        permissive: the budget, not the classifier, is what bounds waste.

        Args:
            error: The exception :meth:`execute` raised.

        Returns:
            ``True`` when the failure may be transient.
        """
        return True
```

Extend the class docstring with one sentence naming `retry` and `retryable` as the execution
contract's retry half.

- [ ] **Step 4: Add the field to Source and Job**

In `packages/interloper-core/src/interloper/source/base.py`, add to the class body:

```python
    retry: RetryPolicy | None = Field(
        default=None,
        title="Retry",
        description="Default attempt budget for this source's assets",
    )
```

and one line to `_resolve`, in the per-asset loop alongside the other source-level defaults:

```python
            if asset.retry is None and self.retry is not None:
                asset.retry = self.retry
```

In `packages/interloper-core/src/interloper/job/base.py`, add to the class body:

```python
    retry: RetryPolicy | None = Field(
        default=None,
        title="Retry",
        description="Attempt budget for this job's runs",
    )
```

Import `RetryPolicy` in both modules.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/operation packages/interloper-core/tests/source packages/interloper-core/tests/job -v`
Expected: PASS.

- [ ] **Step 6: Verify the decorators take it for free**

Run: `uv run python -c "import interloper as il; print(il.asset(lambda **kw: None, retry=il.RetryPolicy(max_attempts=2)).model_fields['retry'])"`
Expected: prints the field. `@il.asset` and `@il.source` split `**overrides` against the class's
ClassVars and `model_fields`, so a new field needs no decorator change. Add `retry` to the
`**overrides` list in both decorators' docstrings.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-core/src/interloper packages/interloper-core/tests
git commit -m "feat(core): declare a retry policy on operations, sources and jobs

By Digitl"
```

---

### Task 3: Record attempts in the event stream

An operation retried in place must not emit `OPERATION_FAILED` for an intermediate attempt, and each
attempt's events need distinct ids: `RunState._operation_event_id` is a uuid5 of
`(run_id, component_id, event_type)`, so a second attempt would collide with the first and dedup away.

**Files:**
- Modify: `packages/interloper-core/src/interloper/events/types.py:25-29`
- Modify: `packages/interloper-core/src/interloper/events/console.py:24-28`
- Modify: `packages/interloper-core/src/interloper/runner/state.py` (`__init__`, `_operation_event_metadata` at :352, `_operation_event_id` at :373, `_emit_operation_event` at :391, plus the new `mark_retried`)
- Test: `packages/interloper-core/tests/runner/test_state.py`
- Test: `packages/interloper-core/tests/runner/test_state_event_ids.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `EventType.OPERATION_RETRIED`; `RunState.attempts: dict[str, int]` (operation id to
  current attempt, starting at 1); `RunState.mark_retried(operation, error, *, emit=True)` which
  emits the retried event and increments the counter; every operation event carries `attempt` in its
  metadata; `_operation_event_id(run_id, component_id, event_type, attempt=1)`. Task 4 calls
  `mark_retried`.

- [ ] **Step 1: Write the failing tests**

Add to `packages/interloper-core/tests/runner/test_state_event_ids.py`:

```python
def test_event_id_is_unchanged_for_a_first_attempt():
    first = RunState._operation_event_id("run", "component", EventType.OPERATION_FAILED)
    explicit = RunState._operation_event_id("run", "component", EventType.OPERATION_FAILED, attempt=1)
    assert first == explicit


def test_event_id_differs_per_attempt():
    first = RunState._operation_event_id("run", "component", EventType.OPERATION_STARTED, attempt=1)
    second = RunState._operation_event_id("run", "component", EventType.OPERATION_STARTED, attempt=2)
    assert first != second
```

Add to `packages/interloper-core/tests/runner/test_state.py`, following that module's existing way of
building a `RunState` and capturing emitted events:

```python
def test_mark_retried_emits_and_advances_the_attempt(state, events):
    operation = state.dag.operations[0]

    state.mark_retried(operation, "boom")

    assert state.attempts[operation.id] == 2
    retried = [event for event in events if event.type is EventType.OPERATION_RETRIED]
    assert len(retried) == 1
    assert retried[0].metadata["attempt"] == 1
    assert retried[0].metadata["error"] == "boom"


def test_events_after_a_retry_carry_the_new_attempt(state, events):
    operation = state.dag.operations[0]

    state.mark_retried(operation, "boom")
    state.mark_failed(operation, "boom again")

    failed = [event for event in events if event.type is EventType.OPERATION_FAILED]
    assert failed[0].metadata["attempt"] == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/runner/test_state.py packages/interloper-core/tests/runner/test_state_event_ids.py -v -k "retry or retried or attempt"`
Expected: FAIL. `OPERATION_RETRIED`, `mark_retried` and `attempts` do not exist, and
`_operation_event_id` takes three arguments.

- [ ] **Step 3: Add the event type**

In `packages/interloper-core/src/interloper/events/types.py`, in the operation-lifecycle block:

```python
    OPERATION_RETRIED = "operation_retried"
```

In `packages/interloper-core/src/interloper/events/console.py`, in the level map:

```python
    EventType.OPERATION_RETRIED: logging.WARNING,
```

- [ ] **Step 4: Track attempts in RunState**

In `packages/interloper-core/src/interloper/runner/state.py`, initialise the counter in `__init__`
next to the `executions` dict:

```python
        self.attempts: dict[str, int] = {operation.id: 1 for operation in dag.operations}
```

Add `attempt` to `_operation_event_metadata`, in the dict it builds:

```python
            "attempt": self.attempts[operation.id],
```

Thread the attempt through the id derivation:

```python
    @staticmethod
    def _operation_event_id(run_id: str, component_id: str, event_type: EventType, attempt: int = 1) -> str:
        key = f"{run_id}:{component_id}:{event_type.value}"
        if attempt > 1:
            key = f"{key}:{attempt}"
        return str(uuid.uuid5(RunState._OPERATION_EVENT_NS, key))
```

Extend its docstring with an `attempt` entry and the reason the first attempt is left out of the key:
ids written before retries existed stay identical, so nothing in history re-keys.

In `_emit_operation_event`, pass the attempt the metadata carries:

```python
        event_id = self._operation_event_id(
            run_id=str(self.metadata.get("run_id")),
            component_id=str(metadata["component_id"]),
            event_type=event_type,
            attempt=int(metadata.get("attempt", 1)),
        )
```

Match the existing call's argument style in that method rather than the keyword form above if it
differs.

- [ ] **Step 5: Add mark_retried**

In the same module, next to `mark_failed`:

```python
    def mark_retried(self, operation: Operation, error: str, *, emit: bool = True) -> None:
        """Record a failed attempt that will be retried, and open the next one.

        A retried attempt is not a verdict: the execution keeps its current
        status, nothing downstream is canceled, and ``OPERATION_FAILED`` stays
        reserved for an exhausted budget. The counter it advances is what makes
        the next attempt's events distinct from this one's.

        Args:
            operation: The operation whose attempt failed.
            error: Error message describing the failed attempt.
            emit: Emit ``OPERATION_RETRIED`` on the EventBus. Set to ``False``
                for cross-process runners where the child emits its own events.
        """
        attempt = self.attempts[operation.id]
        if emit:
            self._emit_operation_event(
                EventType.OPERATION_RETRIED,
                {
                    **self._operation_event_metadata(operation),
                    "error": error,
                    "message": f"Operation '{operation.key}' failed on attempt {attempt}, retrying: {error}",
                },
            )
        self.attempts[operation.id] = attempt + 1
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/runner -v`
Expected: PASS, including the existing state and event-id suites unchanged.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-core/src/interloper/events packages/interloper-core/src/interloper/runner/state.py packages/interloper-core/tests/runner
git commit -m "feat(core): record operation attempts in the event stream

By Digitl"
```

---

### Task 4: The attempt loop in AsyncRunner

**Files:**
- Modify: `packages/interloper-core/src/interloper/runner/async_runner.py:166-205`
- Test: `packages/interloper-core/tests/runner/test_async_runner.py`

**Interfaces:**
- Consumes: `RetryPolicy` (Task 1), `Operation.retry` and `Operation.retryable` (Task 2),
  `RunState.mark_retried` and `RunState.attempts` (Task 3).
- Produces: an `AsyncRunner` that re-executes a failed node in place within its declared budget. An
  operation that declares no policy is attempted once, as today. Task 5 mirrors the loop in
  `MultiProcessRunner`.

- [ ] **Step 1: Write the failing tests**

Add to `packages/interloper-core/tests/runner/test_async_runner.py`, following that module's existing
way of building a DAG of test assets and collecting events:

```python
async def test_an_operation_that_heals_on_the_second_attempt_succeeds():
    calls = []

    @il.asset(retry=il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0))
    def flaky(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return "ok"

    events = []
    result = await il.AsyncRunner(on_event=events.append).run(il.DAG(flaky()))

    assert result.status is ExecutionStatus.COMPLETED
    assert len(calls) == 2
    assert [e.type for e in events if e.type is EventType.OPERATION_RETRIED]
    assert not [e.type for e in events if e.type is EventType.OPERATION_FAILED]


async def test_an_exhausted_budget_fails_once():
    calls = []

    @il.asset(retry=il.RetryPolicy(max_attempts=2, delay=0.0, jitter=0.0))
    def broken(**kwargs):
        calls.append(1)
        raise RuntimeError("permanent")

    events = []
    result = await il.AsyncRunner(on_event=events.append, reraise=False).run(il.DAG(broken()))

    assert result.status is ExecutionStatus.FAILED
    assert len(calls) == 2
    assert len([e for e in events if e.type is EventType.OPERATION_RETRIED]) == 1
    assert len([e for e in events if e.type is EventType.OPERATION_FAILED]) == 1


async def test_an_unretryable_error_is_not_retried():
    calls = []

    class Picky(il.Asset):
        retry: il.RetryPolicy | None = il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0)

        def data(self, **kwargs):
            calls.append(1)
            raise ValueError("permanent")

        def retryable(self, error: Exception) -> bool:
            return not isinstance(error, ValueError)

    result = await il.AsyncRunner(reraise=False).run(il.DAG(Picky()))

    assert result.status is ExecutionStatus.FAILED
    assert len(calls) == 1


async def test_an_operation_without_a_policy_is_attempted_once():
    calls = []

    @il.asset
    def flaky(**kwargs):
        calls.append(1)
        raise RuntimeError("transient")

    result = await il.AsyncRunner(reraise=False).run(il.DAG(flaky()))

    assert result.status is ExecutionStatus.FAILED
    assert len(calls) == 1


async def test_a_retrying_node_does_not_trip_fail_fast():
    downstream_ran = []

    @il.asset(retry=il.RetryPolicy(max_attempts=2, delay=0.0, jitter=0.0))
    def flaky(**kwargs):
        if not downstream_ran:
            raise RuntimeError("transient")
        return "ok"

    @il.asset
    def after(flaky: il.Upstream, **kwargs):
        downstream_ran.append(1)
        return "ok"

    result = await il.AsyncRunner(fail_fast=True).run(il.DAG(flaky(), after()))

    assert result.status is ExecutionStatus.COMPLETED
```

Adapt the asset-construction and DAG-assembly style to whatever the surrounding tests already use.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/runner/test_async_runner.py -v -k "attempt or retry or heal or exhaust"`
Expected: FAIL. `AsyncRunner` has no `retry` field and executes each node exactly once.

- [ ] **Step 3: Wrap the execution in the attempt loop**

In `packages/interloper-core/src/interloper/runner/async_runner.py`, replace the body of
`_execute_operation` from `self.state.mark_running(operation)` to the end with:

```python
        effective_partition = operation.effective_partition(partition_or_window)
        span_attrs = attributes.from_metadata(operation._event_metadata(self.state.metadata, effective_partition))
        context = OperationContext(
            partition_or_window=effective_partition,
            dag=self.state.dag,
            metadata=self.state.metadata,
        )
        policy = operation.retry

        while True:
            self.state.mark_running(operation)
            try:
                with tracer().start_as_current_span("interloper.operation.execute", attributes=span_attrs):
                    result = await operation.execute(context)
            except Exception as e:  # noqa: BLE001 — every failure becomes the node's record
                attempt = self.state.attempts[operation.id]
                if policy is not None and policy.allows(attempt + 1) and operation.retryable(e):
                    self.state.mark_retried(operation, format_exception(e))
                    await asyncio.sleep(policy.delay_before(attempt + 1))
                    continue
                failed = operation.failure(e)
                tb = traceback.format_exc() if type(operation).capture_traceback else None
                self.state.mark_failed(
                    operation, failed.error or format_exception(e), tb=tb, effects=failed, exception=e
                )
                return None
            self.state.mark_completed(operation, effects=result)
            return result
```

Update the method's docstring to describe the loop: an operation carrying a policy has a failed
attempt recorded as retried and executed again after the backoff when the error is retryable and the
budget allows; an operation carrying none is attempted once; only an exhausted or declined failure
marks the node failed and propagates downstream.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/runner/test_async_runner.py -v`
Expected: PASS, including every existing test in the module.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper/runner packages/interloper-core/tests/runner
git commit -m "feat(core): retry a failed operation in place

By Digitl"
```

---

### Task 5: Parity in MultiProcessRunner

**Files:**
- Modify: `packages/interloper-core/src/interloper/runner/multi_process.py:40-80`
- Test: `packages/interloper-core/tests/runner/test_multi_process.py`

**Interfaces:**
- Consumes: everything from Tasks 1 to 4.
- Produces: the same retry behaviour in the cross-process runner. No new API.

- [ ] **Step 1: Confirm the execution paths**

Run: `uv run grep -rn "operation.execute(" packages/interloper-core/src/interloper/runner/`
Expected: two call sites, `async_runner.py` (done in Task 4) and `multi_process.py`. If a third
appears, it gets the same treatment in this task.

- [ ] **Step 2: Write the failing test**

Add to `packages/interloper-core/tests/runner/test_multi_process.py`, following that module's
existing way of declaring picklable module-level assets (a closure will not survive the process
boundary, so the counter has to live in a file or a module-level global that the worker mutates):

```python
def test_an_operation_that_heals_on_the_second_attempt_succeeds(tmp_path):
    marker = tmp_path / "attempts"

    @il.asset(retry=il.RetryPolicy(max_attempts=3, delay=0.0, jitter=0.0))
    def flaky(**kwargs):
        attempts = marker.read_text().count("x") if marker.exists() else 0
        marker.write_text("x" * (attempts + 1))
        if attempts == 0:
            raise RuntimeError("transient")
        return "ok"

    result = il.run(il.MultiProcessRunner().run(il.DAG(flaky())))

    assert result.status is ExecutionStatus.COMPLETED
    assert marker.read_text() == "xx"
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest packages/interloper-core/tests/runner/test_multi_process.py -v -k heal`
Expected: FAIL. The worker executes the operation once and the run fails.

- [ ] **Step 4: Apply the loop in the worker**

In `packages/interloper-core/src/interloper/runner/multi_process.py`, wrap the `operation.execute(...)`
call in the same loop, in its synchronous form. The worker process has no shared `RunState`, so it
counts its own attempts and reports the final count back with the outcome; the parent is what emits
the events:

```python
def _execute_in_worker(
    operation: Operation,
    context: OperationContext,
    policy: RetryPolicy | None,
) -> tuple[Any, int, Exception | None]:
    """Execute an operation in the worker process, retrying within its budget.

    The worker owns the attempt loop because only it sees the failures, but it
    owns none of the reporting: the parent holds the ``RunState`` and emits
    every event, so the attempt count travels back with the outcome.

    Args:
        operation: The operation to execute.
        context: The facts the execution is scoped to.
        policy: The attempt budget, or ``None`` for a single attempt.

    Returns:
        The result, the number of attempts made, and the exception that ended
        the loop (``None`` on success).
    """
    attempt = 1
    while True:
        try:
            return il.run(operation.execute(context)), attempt, None
        except Exception as error:  # noqa: BLE001 — every failure becomes the node's record
            if policy is None or not policy.allows(attempt + 1) or not operation.retryable(error):
                return None, attempt, error
            time.sleep(policy.delay_before(attempt + 1))
            attempt += 1
```

Resolve `policy = operation.retry` in the parent, before submitting, and pass it in.
On the way back, call `self.state.mark_retried(operation, format_exception(error))` once per attempt
beyond the first so the parent's counter and events match what the worker did, then
`mark_completed` or `mark_failed` as the outcome dictates. Match the module's existing submit and
result-handling shape (`_handle_completed` at :157, `_handle_flushed` at :186) rather than the sketch
above; the point is the loop, its budget checks and the returned attempt count.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/runner -v`
Expected: PASS.

- [ ] **Step 6: Run the full checks**

Run: `uv run ruff check && uv run ty check && uv run pytest`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-core
git commit -m "feat(core): retry a failed operation in the multi-process runner

By Digitl"
```
