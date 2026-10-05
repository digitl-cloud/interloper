---
render_macros: true
---

# CLI

```
interloper <command> [options]
```

Without a command, the help is printed. A `.env` file in the working directory is loaded when
`python-dotenv` is installed. Telemetry is initialized from settings for every command.

{{ cli_reference() }}
