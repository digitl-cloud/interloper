---
description: Run only the Python checks (ruff + pyright + pytest) and fix failures
---

Run the Python half of the check suite from the repo root:

```
make check-python
```

This runs `uv run ruff check packages`, `uv run pyright`, and `uv run pytest`
(the `functional` marker is excluded by default).

If everything passes, report a one-line summary and stop.

If anything fails, diagnose and fix the root cause, then re-run
`make check-python` to confirm. Honor repo conventions: ruff line length 120,
pyright `basic` mode.
