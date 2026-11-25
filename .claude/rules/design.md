---
paths:
  - "**/*.py"
---

# Design

How a change is shaped in this repo: which types, names and mechanisms it may add. How the code itself is written is in [python-style.md](python-style.md). These rules are binding for all produced code, and they are what review pushes back on most.

## 1. Dependencies point from caller to callee

A sink, transport or recorder never drives the thing that feeds it: an event recorder does not run the runner. A seam that crosses a process or a package is one shared type with one serialization that the type checker sees, never two signatures that happen to match. Each fact has one author.

## 2. One type per concept

Before adding a class, dataclass, `NamedTuple` or `TypedDict`, find the type that already represents the entity. A type whose fields mirror another's is a duplicate: return the existing model and fix the query's shape instead of adding a carrier, even when the carrier would be faster. A PR lists the types it adds, each with the concept it names that had no type before.

## 3. One word per concept

Grep for how the code already says it before naming anything. `enabled` already means "this will run", so a new kind of thing that runs is `enabled`, not `executable`; a bound `il.Upstream` is an upstream, never a leg. A second implementation of a mechanism (another dialect, another backend) takes the first one's names, never a pattern's textbook name. Name mechanisms in plain operational terms: heartbeat and timeout, not lease.

## 4. Shapes a reader can follow

No closures as structure, no context manager that yields a callable, no private attribute plus `None` guards choreographing a lifetime. When something has a lifetime and behaviour, it is a small class with attributes and methods. A dependency bundle such as `ToolkitContext` stays a bundle: the behaviour goes on the type that owns the data (`store.components.get(..., org_id=...)`), never on the bundle.

## 5. No new concept for a one-off

Before adding a declaration, hook, `ClassVar`, `ContextVar`, setting or flag, ask who has to know it exists. If one kind uses it, it is that kind's override, not a framework-wide mechanism. A new run-scoped fact travels the way the window and metadata already do (`Runner.run` to `RunState` to `OperationContext`), never through a parallel channel. An author's model of a component fits in one sentence; a change that needs a second sentence is suspect. Core's contracts stay minimal: a hook's docstring is its caller contract, and two small callers written against it beat a shared helper hoisted into core.

## 6. Uniform primitives, not observed symptoms

Design from consistency, simplicity and robustness. Production data motivates or validates a design, it never shapes its structure: the primitive holds for every case, and today's numbers become settings it exposes. Each layer owns one behaviour and declares its policy where it applies, so a reader finds it there; a hidden module constant that changes behaviour is the smell.

## 7. A special case in a shared default is a named table

When a rule moves into a shared default, it becomes a module-level table that says what it is for and grows by adding an entry (`SYMBOL_WORDS`, `RECONCILERS`), even with one entry. Never a one-off conditional bolted onto a generic path.

## 8. The platform mirrors core

Platform behaviour (db, api, app) applies the semantics core already has, with the organisation as the bigger pool. A row means the same thing for every kind. Check how core does it before proposing a platform-only rule; core must stay usable on its own.

## 9. Fix the design, not the symptom

A fix that needs a filter, exclusion or dedupe to undo what another component produced means there are two paths for one thing: remove one. Each kind of data has one path. A refactor truly simplifies: delete rather than adapt, and when the new shape fights the code, raise it instead of wrapping it.

## 10. Store what cannot be derived

No table and no stored column for a value the existing row holds or a read can derive.

## 11. Safe by default

Production is safe without any flag. When a setting or CLI option is only safe once set, invert it: the safe behaviour is the default, and the convenience is an explicit command or mode.

## 12. No one-off scripts

Local and dev data is disposable: a change that breaks an existing dev database says "recreate it" and adds no migration script or make target. Production data changes are alembic migrations.

## 13. The idiom, not the letter of an autofix

After a bulk autofix or a linter upgrade, read the diff for what it multiplied and use the framework's own consolidating idiom (FastAPI's shared `Annotated` dependency aliases, not the same `Annotated[..., Depends(...)]` spelled out a hundred times). Call something the recommended way only with its documentation in hand.
