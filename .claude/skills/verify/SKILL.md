---
name: verify
description: Build, launch, and drive a seeded interloper dev instance headlessly to observe a change at its API/UI surface.
---

# Verifying interloper changes live

## Launch

`make dev-up DEV_ARGS="--port 3100 --no-browser"` (background), which runs `interloper dev -f collection.yaml` from `dev/`. Needs local Postgres at `localhost:5432` (`postgres/postgres/interloper`). A taken port is an error naming it, never a move to another: free it or pick another. Wait for the `Ready. Sign in: http://127.0.0.1:3100/api/dev/sign-in?code=…` line; a Python edit restarts the platform and prints it again.

## Authenticate headlessly (no OAuth)

Open the printed link in the browser that drives the app: it starts an ordinary session for the local developer (a super-admin no login provider reaches) and lands on the repository's dev organisation. For API calls from a script, follow the link with redirects off and keep the `session_token` cookie it sets. A session carries no organisation: organisation pages live under `/<key>/…` and API calls under `/api/organisations/{org_id}/…`. Read the link from the log in the script itself; never paste the code into a shell command.

## Drive

- **API**: httpx2 with `follow_redirects=True` (collection routes 307-redirect to the trailing-slash form; httpx2 doesn't follow by default) against `http://127.0.0.1:<port>/api`.
- **UI**: `npm install playwright-core` in the scratchpad, `chromium.launch({channel: 'chrome', headless: true})`, `page.goto(<printed link>)`, then click through the real pages and screenshot. Use `127.0.0.1`, never `localhost`: the session cookie belongs to the host it was set on. Toasts are readable via `[role="alert"]` innerTexts.

## Hygiene

The dev DB is shared — track every component id you create and delete exactly those (children/referrers first: job → source → connection; the in-use guard 409s out-of-order deletes). Never key-filtered bulk deletes. Components `dev/collection.yaml` declares are read-only: change them in the file. Tear down with TaskStop on the dev-up task; confirm the port is free.
