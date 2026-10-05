# Claude Code plugin

The repository ships a [Claude Code](https://docs.anthropic.com/en/docs/claude-code) plugin with
skills for people building on the framework. The skills encode the recipes and the traps from
this documentation, so an agent working in your project writes sources and runs them the way the
framework expects.

```
/plugin marketplace add digitl-cloud/interloper
/plugin install interloper@interloper
```

| Skill | Use it when |
|-------|-------------|
| `interloper-source` | Adding or changing a source, asset, connection or schema in your project. |
| `interloper-run` | Running, materializing or backfilling from Python, a notebook, CI or the CLI, including the spec file a CLI run needs. |
| `interloper-connection` | Writing a connection: credentials, OAuth sign-in, a health check, an account picker, environment loading. |
| `interloper-schema` | Typing and reshaping asset rows: schemas, materialization strategies, normalizers, `SchemaError` fixes. |
| `interloper-destination` | Writing a custom destination over files, object storage or a database, with partition and window semantics. |
| `interloper-manifest` | Writing a job spec over several sources: shared destinations, secrets, asset selection, cross-source wiring. |
| `interloper-backfill` | Running ranges of partitions, windowed runs, scheduled windows, per-partition progress. |
| `interloper-triage` | Reading a failed run's events, finding the root cause and the fix, deciding whether to retry. |
| `interloper-deploy` | Configuring the platform: `interloper.yaml`, environment variables, sign-in, catalog, provider apps, telemetry, images and chart. |
| `interloper-upgrade` | Moving code, specs and deployments to a newer release using the changelog's breaking changes. |

Skills trigger on their own from the task description, or by name (`/interloper-source`). The
plugin is versioned with the framework: its version matches the `interloper-core` release it
describes.

The skills live under `plugins/interloper/skills/` in the repository and are reviewed with the
changes that affect them; a breaking release updates the skill in the same pull request as the
code and these pages.
