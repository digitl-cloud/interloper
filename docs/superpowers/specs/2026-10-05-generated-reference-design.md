# Reference pages rendered from the source

## Problem

The nine pages under `docs/reference` were tables transcribed from the source: settings fields and defaults, CLI flags, event types, the exception tree, the utilities. Each drifted as soon as the source moved (the settings page already lacked `postgres.statement_timeout`), and nothing documented the public framework classes at all.

## Decisions

1. **zensical's own extensions, no MkDocs.** zensical bundles `mkdocstrings` (over `mkdocstrings-python` and griffe, added to the dev group) and `macros`. Both are configured in `zensical.toml`; `--strict` builds in CI.
2. **Docstrings are the API reference.** `::: interloper.X` directives render the public classes, decorators and functions from their Google docstrings, grouped by concept under `reference/api/` (components, assets, sources, destinations, connections, resources & fields, jobs & hooks, partitioning, execution, schema, REST client, catalog). `errors.md` and `utils.md` are directives over `interloper.errors` and the `interloper.utils` modules. Docstrings therefore follow Markdown, not RST: Sphinx roles became code spans and `::` literal blocks became fenced code.
3. **Derived tables are macros.** `dev/docs_macros.py` registers `settings_reference()` and `cli_reference()`, which read `AppSettings` (every section's `model_fields`, prefix, type, default, description) and `interloper.cli.main.build_parser()` (every command's description, requirements, flags, choices, defaults). Macros render only on the pages that opt in (`render_macros: true`), so the Jinja braces in other pages stay literal. The module lives in `dev/` because zensical copies every file under `docs/` into the site.
4. **The source carries the copy.** Every settings field has a `Field(description=...)`; every CLI command a `description=`; every `EventType` member an attribute docstring saying when it is emitted. The class and module docstrings say so, so the next field or flag arrives with its text.
5. **Hand-written pages stay where the content is a design, not a signature:** decorator channels, spans and metrics (string literals without a registry), ecosystem, the Claude Code plugin, the YAML precedence example and the non-settings environment variables.
6. **The docs job syncs the workspace.** The reference imports `interloper`, so `docs.yaml` runs `uv sync` before `zensical build --strict`, and triggers on `packages/interloper-core/src/**` as well as `docs/**`.

## Out of scope

The REST API reference (OpenAPI) is deferred.
