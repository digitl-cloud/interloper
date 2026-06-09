---
description: Run the full Python + frontend check suite and fix any failures
---

Run the complete check suite from the repo root:

```
make check
```

This runs both halves:

- `make check-python` — `uv run ruff check packages`, `uv run ty check`, `uv run pytest`
- `make check-typescript` — `pnpm run lint` + `pnpm exec nuxt typecheck` in `packages/interloper-app/app`

If everything passes, report a one-line summary and stop.

If anything fails, diagnose the root cause and fix it (don't just silence the
error), then re-run the relevant half — `make check-python` or
`make check-typescript` — to confirm. Stay within repo conventions: ruff line
length 120, type-checked with `ty`, Python ≥3.10.
