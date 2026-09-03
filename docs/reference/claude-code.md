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

Skills trigger on their own from the task description, or by name (`/interloper-source`). The
plugin is versioned with the framework: its version matches the `interloper-core` release it
describes.

The skills live under `plugins/interloper/skills/` in the repository and are reviewed with the
changes that affect them; a breaking release updates the skill in the same pull request as the
code and these pages.
