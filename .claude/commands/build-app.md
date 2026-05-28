---
description: Build the Nuxt SPA and stage it inside the interloper-app Python package
---

Build the frontend and stage the static output inside the Python package:

```
make build-app
```

This installs deps, runs `NUXT_PRESET=static pnpm build` in
`packages/interloper-app/app`, wipes
`packages/interloper-app/src/interloper_app/static/`, and copies the fresh
`.output/public/*` into it (keeping a `.gitkeep`).

If the build succeeds, report what changed under `static/` in one line. If it
fails, diagnose the root cause (it's usually a TypeScript/lint error — try
`make check-typescript` to surface it cleanly) and fix it before rebuilding.
