# ================================================================
# Interloper docs — static documentation site (zensical)
# ================================================================
#
# Standalone image. The docs are independent of the Python workspace, so they
# get their own dockerfile rather than a target in the multi-target dockerfile.
#
#   docker build -f docs.dockerfile --target docs -t interloper-docs .
#
# Published as ghcr.io/digitl-cloud/interloper-docs by the `docs` entry in the
# Makefile image catalog and the publish workflow matrix.
# ================================================================

# ── build (zensical) ──────────────────────────────────────────
# Like the SPA build, the output is architecture-independent static HTML/CSS,
# so pin this stage to the *build* platform — there's no reason to run the
# zensical build under QEMU for the linux/arm64 target. Only the trivial nginx
# runtime stage below is built per-target-arch (a COPY).
#
# zensical is run from an ephemeral, isolated environment (`uv run --no-project
# --with`), so this stage needs only the docs sources and the site config. The
# version is pinned to match the dev dependency group in pyproject.toml.
FROM --platform=$BUILDPLATFORM ghcr.io/astral-sh/uv:python3.12-alpine AS build-docs

WORKDIR /docs
ENV UV_LINK_MODE=copy
# Mirror the repo layout: zensical.toml at the root, docs/ alongside it
# (docs_dir defaults to ./docs relative to the config). Output -> /docs/site.
COPY zensical.toml ./
COPY docs/ docs/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv run --no-project --with "zensical>=0.0.24" zensical build


# ── runtime (nginx serving the pre-built static site) ─────────
# Pure static site — no API proxy, so a plain conf.d drop-in (no envsubst).
FROM nginx:1.27-alpine-slim AS docs
COPY --from=build-docs /docs/site/ /usr/share/nginx/html/
COPY docker/docs.nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
