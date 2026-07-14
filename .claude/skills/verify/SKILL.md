---
name: verify
description: Build, launch, and drive a seeded interloper dev instance headlessly to observe a change at its API/UI surface.
---

# Verifying interloper changes live

## Launch

`INTERLOPER_SERVER_PORT=3100 make dev-up` (background). Needs local Postgres at `localhost:5432` (`postgres/postgres/interloper`). Watch the log for the Nuxt port: if 3100 is held by a stale process, **Nuxt silently falls back to another port (even 3000)** — read the "Local:" line, don't assume.

## Authenticate headlessly (no OAuth)

Sessions are plain DB rows (`sessions.token_hash` = sha256 hexdigest of a `secrets.token_urlsafe(48)` token). Mint one with a short psycopg2 script inserting `(user_id, organisation_id, token_hash, expires_at)` for the seeded profile, keep the raw token in a scratchpad file, and send it as the `session_token` cookie. Delete the session row when done.

Note: shell commands that interpolate the token (`TOK=$(cat …); curl -b …`) get denied — drive everything from python (httpx) / node scripts that read the token file themselves.

## Drive

- **API**: httpx with `follow_redirects=True` (collection routes 307-redirect to the trailing-slash form; httpx doesn't follow by default) against `http://localhost:<port>/api`.
- **UI**: `npm install playwright-core` in the scratchpad, `chromium.launch({channel: 'chrome', headless: true})`, add the `session_token` cookie via `context.addCookies`, then click through the real pages and screenshot. Toasts are readable via `[role="alert"]` innerTexts.

## Hygiene

The dev DB is shared — track every component id you create and delete exactly those (children/referrers first: job → source → connection; the in-use guard 409s out-of-order deletes). Never key-filtered bulk deletes. Tear down with TaskStop on the dev-up task; confirm the port is free.
