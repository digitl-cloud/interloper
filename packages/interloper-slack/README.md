# interloper-slack

Slack notifications for interloper runs: a `SlackHook` that posts a run
outcome to a channel, and the `SlackConnection` that holds the bot token.

The hook is the notification counterpart to core's `WebhookHook` — same
events, but the payload is a message a human reads rather than a document a
service parses.

## Setup

1. Create a Slack app (**From scratch**) at
   [api.slack.com/apps](https://api.slack.com/apps).
2. Under **OAuth & Permissions**, add these bot token scopes:
   - `chat:write` — post the notification
   - `channels:read`, `groups:read` — populate the channel picker
3. **Install to Workspace** and copy the bot user token (`xoxb-…`).
4. Invite the bot to each channel it posts to (`/invite @your-app`). Slack
   rejects `chat.postMessage` with `not_in_channel` otherwise.

One connection serves the whole workspace; each hook picks its own channel.

## Usage

```python
import interloper as il
from interloper_slack import SlackConnection, SlackHook

hook = SlackHook(
    connection=SlackConnection(bot_token="xoxb-..."),
    channel="C0123456789",
    watches=[my_source],
    events=["run_failed"],
)
```

The token also loads from the environment (`SLACK_BOT_TOKEN`), so
`SlackConnection()` works with no arguments.

`events` defaults to `["run_failed"]` like every hook — the alert most teams
want. Add `"run_completed"` for a full-chatter channel.

In a deployed instance you configure this through the UI instead: add a Slack
connection, then a Slack hook watching the source or job you care about. The
scheduler's hook evaluator fires it on terminal runs.

## Message shape

```
❌ *Facebook Ads* failed
Run `9f3c…` · partition `2026-07-30`
```
```
HTTPStatusError: 429 Too Many Requests
```

The headline also travels as the message's `text`, so notification previews
and screen readers get the outcome without parsing blocks.

## Notes

Every Slack call goes through the connection's `client` — a `cached_property`
`il.RESTClient` with `il.HTTPBearerAuth`, like every other connection in the
workspace. That client owns the base URL, the token, and the connection pool,
so callers name a path and nothing else; the channel picker paginates with
`il.JSONCursorPaginator` over Slack's `response_metadata.next_cursor`.

The client is **sync**, where most connections' are async. The two consumers
are a hook firing (sync by contract) and a form lookup — neither has
independent requests to overlap, and the API process runs sync providers in a
thread, so it never blocks the event loop.

The one thing the framework can't cover is that Slack answers a *rejected*
call with HTTP 200 and `{"ok": false, "error": "..."}`, so `raise_for_status()`
alone lets failures pass silently. Every response goes through `api.unwrap`,
which checks `ok` and raises `SlackAPIError` carrying Slack's own error code.
For the paginated picker that check lives in the `data_selector`, since
`paginate` only raises for HTTP status.

A failed firing is recorded on the hook's firing claim (as `hook_failed`) and
is not retried.
