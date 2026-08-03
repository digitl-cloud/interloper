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

Slack answers a rejected API call with HTTP 200 and `{"ok": false, "error":
"..."}`, so every call in this package goes through `api.post` / `api.apost`,
which check `ok` and raise `SlackAPIError` carrying Slack's own error code.
A failed firing is recorded on the hook's firing claim (as `hook_failed`) and
is not retried.

`post` and `apost` are the same function twice, once per colour: same argument
order, same keywords, same return. The client is always the caller's — it owns
the timeout and decides how many calls share a connection — and the verb is
always POST, which every Slack method accepts. Pass `json=` for endpoints that
document `application/json` (`chat.postMessage`) and `data=` for the
form-encoded ones (`conversations.list`); that split is Slack's, and the
keywords mirror httpx's own.
