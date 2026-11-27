---
name: review-slop
description: Review the Python a branch changes for the slop ruff and ty cannot catch, such as narration comments, docstrings that only repeat the name, duplicate types, unrequested features, defensive checks on guaranteed values, silent fallbacks and tests that only test their mocks. Use before opening a PR, or when asked to review a diff for quality.
---

# Reviewing a branch for slop

Slop is code that costs its reader more than it cost its writer. Ruff and ty catch what a rule can see; this review covers what only a reader can.

## Scope

The Python the branch changes, committed or not: `git diff origin/main -- '*.py'` for tracked files and `git ls-files --others --exclude-standard -- '*.py'` for new ones. Read each hunk inside its whole function or class, and grep for what it names before judging it new. Skip what a linter reports. A line counts as touched when the branch changed how it reads; a line that only gained `from e` or lost a `noqa` keeps its old behaviour and is out of scope.

Back each finding before reporting it: run the touched package's tests, or probe ruff and ty on a scratch snippet, whenever the claim is about behaviour or about what a tool accepts.

The house rules are the yardstick: [python-style.md](../../rules/python-style.md) for how code is written, [design.md](../../rules/design.md) for what a change may add, and the package's own rules (`assets.md`, `star-schema.md`).

## What to look for

**Comments and docstrings**

- A comment that narrates the next line (`# Now loop over the rows`), addresses the reviewer (`# changed to fix the test`), or annotates a field. A comment that gives a non-obvious why, a vendor quirk or an ordering constraint is not slop.
- A docstring whose summary only turns the name into a sentence (`"""Gets the user."""`). Product-copy docstrings (components, agent and toolkit tools) are summary-only by rule and are not slop.

**More than the task asked for**

- A type that mirrors an existing one. List the types the branch adds (`git diff origin/main -- '*.py' | grep -E '^\+\s*class '`, plus the `class` lines of new files) and name, for each, the concept it represents that had no type before.
- A parameter, flag, option, mode or setting nobody asked for, or handling for a case the contract excludes.
- An abstraction with one user: a protocol or base class with one implementation, a class whose methods only delegate, a function whose body is one call with the same arguments, a helper called once that hides two lines. A named table hoisted into a shared default is not one of these (design rule 7).
- Caching, laziness or concurrency for a value that is cheap, always used, or a handful of items.
- Code that re-implements the standard library or a helper the codebase already has (look in `utils/` and on the owning class first).

**Errors that pass silently**

- A check on a value the types or the caller already guarantee: `is None` on a non-optional, `or default` on a required field, `isinstance` on a typed parameter.
- A fallback that hides which path succeeded, `except ...: return None` (or `[]`, `{}`, `False`), a retry that drops the failures it absorbed, a specific error replaced with a generic message.
- `suppress(Exception)` without a line saying why. Ruff's blind-except rules see `except Exception: pass` but not its `suppress` form, so review is the only guard.
- Parsing or coercion that accepts malformed input by guessing, truncating, clamping or silently creating what is missing.
- A boolean success flag or a sentinel value where raising would carry the reason.
- A raw string for a mode, kind or status where an enum or `Literal` exists.

**What an autofix did**

On a diff that enables a rule or applies a bulk fix, read what each fix did: a rewrite that reads worse than the original (an assertion turned around so the subject sits on the right), a `noqa` whose reason is wrong, a rewrite that moves code out of another rule's sight, the same verbose form multiplied where the framework has an idiom (design rule 13).

**Tests that pass without proving anything**

- Assertions that only check what the test itself set up or mocked.
- An assertion removed or loosened, a `skip` or `xfail` added, or `pytest.raises(Exception)` where the code raises something specific, so that a failing test passes.
- A new standalone test file where the module's test file exists (tests mirror the package layout one to one).

## Report

Findings, most consequential first, one per item: `file:line`, which item above, why it costs the reader, and the smallest fix. Report only what you would defend in review; if nothing qualifies, say so. Do not change code unless asked.

Several items adapt the LLM-judge rubric of [SlopCodeBench](https://github.com/SprocketLab/slop-code-bench) (MIT).
