#!/bin/sh
# Helper for Dockerfile build stages.
# Usage: uv-sync.sh [--frozen] <packages...>
#
# Reads extras from env:
#   CORE_EXTRAS       → --package interloper-{name} flags
#   ASSETS_EXTRAS     → --extra {name} flags (scoped to interloper-assets)
#   SCHEDULER_EXTRAS  → --extra {name} flags (scoped to interloper-scheduler)
set -e

MODE="--locked --no-editable"
if [ "$1" = "--frozen" ]; then
    MODE="--frozen --no-install-workspace"
    shift
fi

# Build --package flags from positional args
PACKAGES=""
for p in "$@"; do
    PACKAGES="$PACKAGES --package $p"
done

# Expand CORE_EXTRAS → additional --package interloper-{name}
for e in $(echo "${CORE_EXTRAS:-}" | tr ',' ' '); do
    [ -n "$e" ] && PACKAGES="$PACKAGES --package interloper-$e"
done

# shellcheck disable=SC2086
uv sync $MODE $PACKAGES

# Install extras. Must include ALL packages to avoid uv sync removing
# previously installed deps (uv sync reconciles the full venv).
# Only runs on the install pass (not --frozen).
if [ "$MODE" = "--locked --no-editable" ]; then
    EXTRA_FLAGS=""

    # ASSETS_EXTRAS → --extra flags (only when interloper-assets is included)
    HAS_ASSETS=false
    for p in "$@"; do
        [ "$p" = "interloper-assets" ] && HAS_ASSETS=true
    done
    if $HAS_ASSETS && [ -n "${ASSETS_EXTRAS:-}" ]; then
        for e in $(echo "$ASSETS_EXTRAS" | tr ',' ' '); do
            EXTRA_FLAGS="$EXTRA_FLAGS --extra $e"
        done
    fi

    # SCHEDULER_EXTRAS → --extra flags (only when interloper-scheduler is included)
    HAS_SCHEDULER=false
    for p in "$@"; do
        [ "$p" = "interloper-scheduler" ] && HAS_SCHEDULER=true
    done
    if $HAS_SCHEDULER && [ -n "${SCHEDULER_EXTRAS:-}" ]; then
        for e in $(echo "$SCHEDULER_EXTRAS" | tr ',' ' '); do
            EXTRA_FLAGS="$EXTRA_FLAGS --extra $e"
        done
    fi

    if [ -n "$EXTRA_FLAGS" ]; then
        # shellcheck disable=SC2086
        uv sync --locked --no-editable $PACKAGES $EXTRA_FLAGS
    fi

    # Slim the venv: drop test dirs, type stubs, and bundled tests inside
    # installed packages. Bytecode (.pyc) is preserved.
    VENV="/interloper/.venv"
    if [ -d "$VENV" ]; then
        find "$VENV" -type d \( -name tests -o -name testing -o -name __pycache__ -path '*/tests/*' \) -prune -exec rm -rf {} + 2>/dev/null || true
        find "$VENV" -type f \( -name '*.pyi' -o -name '*.pyx' -o -name '*.c' -o -name '*.h' \) -delete 2>/dev/null || true
        find "$VENV" -type d -name '*.dist-info' -exec sh -c 'rm -f "$1/RECORD" "$1/INSTALLER" "$1/REQUESTED"' _ {} \; 2>/dev/null || true
    fi
fi
