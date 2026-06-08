.PHONY: build docker-build docker-build-linux docker-push docker-build-push \
        dev dev-db dev-seed dev-reset dev-up compose-up compose-down compose-seed


# ###############
# SETUP
# ###############

setup:
	pre-commit install
	uv sync --all-packages --all-extras


# ###############
# QUALITY
# ###############

check-python:
	uv run ruff check packages
	uv run pyright
	uv run pytest

check-typescript:
	cd packages/interloper-app/app && pnpm run lint && pnpm exec nuxt typecheck

check:
	make check-python
	make check-typescript


# ###############
# BUILD
# ###############

build-app:
	cd packages/interloper-app/app && pnpm install && NUXT_PRESET=static pnpm build
	rm -rf packages/interloper-app/src/interloper_app/static/*
	cp -r packages/interloper-app/app/.output/public/* packages/interloper-app/src/interloper_app/static/
	touch packages/interloper-app/src/interloper_app/static/.gitkeep


# ###############
# DOCKER
# ###############

REGISTRY      := europe-docker.pkg.dev/dc-int-connectors-prd/docker
VERSION       := $(shell python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
CORE_EXTRAS   ?= google-cloud
ASSETS_EXTRAS ?= bing,facebook,google

# Image catalog. Each component is its own repository: "interloper-<role>".
# NOTE: the `docker` job matrix in .github/workflows/release.yaml mirrors this
# catalog — update both together when adding or removing a role.
#   ROLES                  → image "interloper-<role>",         tag = "<version>"
#   ROLES_LAUNCHER_AWARE   → one image per (role, launcher) pair, all tagged
#                            "<version>", with the launcher in the image name:
#                              interloper-<role>             (no launcher extras)
#                              interloper-<role>-k8s
#                              interloper-<role>-docker
# The chart picks the suffix from config.launcher.type — no manual mapping.
ROLES                := api frontend worker
ROLES_LAUNCHER_AWARE := scheduler
LAUNCHERS            := k8s docker

# Concrete target stems built by `docker-build`.
TARGETS := $(ROLES) $(ROLES_LAUNCHER_AWARE) \
           $(foreach r,$(ROLES_LAUNCHER_AWARE), \
             $(foreach l,$(LAUNCHERS),$(r)-$(l)))

# Parse a target stem (e.g. "scheduler-k8s") into role + launcher.
# Launcher is empty for base targets ("scheduler", "api", …).
launcher_of = $(if $(filter %-k8s %-docker,$(1)),$(lastword $(subst -, ,$(1))))
role_of     = $(if $(call launcher_of,$(1)),$(patsubst %-$(call launcher_of,$(1)),%,$(1)),$(1))
# Image name derived from a stem. Tag carries the version only;
# launcher variants live in the image name.
image_of    = interloper-$(call role_of,$(1))$(if $(call launcher_of,$(1)),-$(call launcher_of,$(1)))

# Pattern rules. Order matters on macOS' GNU make 3.81 (no shortest-stem):
# the more specific docker-build-linux-% must come first.
docker-build-linux-%:
	docker build --target $(call role_of,$*) --platform linux/amd64 \
		--build-arg CORE_EXTRAS=$(CORE_EXTRAS) \
		--build-arg ASSETS_EXTRAS=$(ASSETS_EXTRAS) \
		--build-arg SCHEDULER_EXTRAS=$(call launcher_of,$*) \
		-t $(call image_of,$*):$(VERSION) \
		-t $(call image_of,$*):latest \
		-t $(REGISTRY)/$(call image_of,$*):$(VERSION) \
		-t $(REGISTRY)/$(call image_of,$*):latest .

docker-build-%:
	docker build --target $(call role_of,$*) \
		--build-arg CORE_EXTRAS=$(CORE_EXTRAS) \
		--build-arg ASSETS_EXTRAS=$(ASSETS_EXTRAS) \
		--build-arg SCHEDULER_EXTRAS=$(call launcher_of,$*) \
		-t $(call image_of,$*):$(VERSION) \
		-t $(call image_of,$*):latest \
		-t $(REGISTRY)/$(call image_of,$*):$(VERSION) \
		-t $(REGISTRY)/$(call image_of,$*):latest .

docker-push-%:
	docker push $(REGISTRY)/$(call image_of,$*):$(VERSION)
	docker push $(REGISTRY)/$(call image_of,$*):latest

docker-build:       $(addprefix docker-build-,$(TARGETS))
docker-build-linux: $(addprefix docker-build-linux-,$(TARGETS))
docker-push:        $(addprefix docker-push-,$(TARGETS))

docker-build-push:
	$(MAKE) docker-build-linux
	$(MAKE) docker-push


# ###############
# DEV INSTANCE
# ###############
#
# Stand up a running interloper instance with the DB migrated and seeded with a
# minimal dataset (one super-admin + org + the demo source). Host path:
#
#   make dev      reset + seed + run the app (full bootstrap, one command).
#   make dev-up   just run the app against the existing DB (no reset/seed).
#   make dev-reset  reset + seed, no server.
#
# docker-compose path (Postgres + api + scheduler + frontend in containers, no
# host Postgres/Python needed): make compose-up. See dev/seed.py.

# Run the CLI from dev/, whose interloper.yaml carries only the catalog so the
# INTERLOPER_* vars below take effect (the repo-root interloper.yaml pins
# postgres/launcher to prod/k8s, and YAML wins over env per submodel). The
# encryption key is a throwaway dev secret — never use it outside local dev.
DEV_DIR := dev

# Dev super-admin identity. INTERLOPER_DEV_USER_GOOGLE_ID (your Google subject
# id) makes the seed write the exact profile your login resolves to — so you're
# super-admin out of the box with no duplicate. Without it the seed falls back
# to matching/creating by email. Resolved highest-first from: the make command
# line, a repo-root .env (gitignored), the shell env, else the defaults below.
# Set them once in .env, e.g.:
#   INTERLOPER_DEV_USER_EMAIL=you@example.com
#   INTERLOPER_DEV_USER_GOOGLE_ID=1234567890
-include .env
# Export every .env-loaded var to recipe environments, so the app subprocess
# also sees vars the harness doesn't forward explicitly — connector OAuth creds
# (<PROVIDER>_CLIENT_ID, …), GEMINI_API_KEY, INTERLOPER_SMTP_PASSWORD, etc. The
# explicit INTERLOPER_* below still pin the harness-critical settings (they're
# set inline on each recipe, which wins over the exported value).
export
INTERLOPER_DEV_USER_EMAIL ?= admin@dev.local
INTERLOPER_DEV_USER_GOOGLE_ID ?=

# Google OAuth for the app login (the /auth flow). Put your client id + secret
# in .env (they're secrets). The redirect URI must be registered on the OAuth
# client in Google Cloud; cookie_secure=false lets the session cookie stick over
# local http. e.g. in .env:
#   INTERLOPER_AUTH_GOOGLE_CLIENT_ID=...apps.googleusercontent.com
#   INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET=...
INTERLOPER_AUTH_GOOGLE_CLIENT_ID ?=
INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET ?=
INTERLOPER_AUTH_GOOGLE_REDIRECT_URI ?= http://localhost:3000/api/auth/google/callback
INTERLOPER_AUTH_COOKIE_SECURE ?= false

DEV_ENV := INTERLOPER_POSTGRES_HOST=localhost \
           INTERLOPER_POSTGRES_PORT=5432 \
           INTERLOPER_POSTGRES_USER=postgres \
           INTERLOPER_POSTGRES_PASSWORD=postgres \
           INTERLOPER_POSTGRES_DATABASE=interloper \
           INTERLOPER_LAUNCHER_TYPE=in_process \
           INTERLOPER_RUNNER_TYPE=multi_thread \
           INTERLOPER_ENCRYPTION_KEY=dev-encryption-key-not-for-production \
           INTERLOPER_DEV_USER_EMAIL=$(INTERLOPER_DEV_USER_EMAIL) \
           INTERLOPER_DEV_USER_GOOGLE_ID=$(INTERLOPER_DEV_USER_GOOGLE_ID) \
           INTERLOPER_AUTH_GOOGLE_CLIENT_ID=$(INTERLOPER_AUTH_GOOGLE_CLIENT_ID) \
           INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET=$(INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET) \
           INTERLOPER_AUTH_GOOGLE_REDIRECT_URI=$(INTERLOPER_AUTH_GOOGLE_REDIRECT_URI) \
           INTERLOPER_AUTH_COOKIE_SECURE=$(INTERLOPER_AUTH_COOKIE_SECURE)

dev-db:
	cd $(DEV_DIR) && $(DEV_ENV) uv run interloper db reset --yes

dev-seed:
	cd $(DEV_DIR) && $(DEV_ENV) uv run python seed.py

dev-reset: dev-db dev-seed

# --dev runs the Nuxt dev server (hot reload) on :3000 and moves the API to a
# free port it proxies to — so the app is at http://localhost:3000 with no SPA
# build needed. It runs `pnpm dev`, so install the app deps first if missing.
APP_DEPS := packages/interloper-app/app/node_modules

$(APP_DEPS):
	cd packages/interloper-app/app && pnpm install

# Run the app only — non-destructive, so it keeps your data/session across
# restarts. Use `make dev` (or dev-reset) when you want a fresh seeded DB.
dev-up: $(APP_DEPS)
	cd $(DEV_DIR) && $(DEV_ENV) uv run interloper app --api --cron --worker --reaper --dev

# Full bootstrap: reset + seed + run.
dev: dev-reset dev-up

# Compose stack lives in dev/. Run from there and forward the dev-user identity
# (resolved from .env / shell env above) into compose's env interpolation.
COMPOSE_ENV := INTERLOPER_DEV_USER_EMAIL=$(INTERLOPER_DEV_USER_EMAIL) \
               INTERLOPER_DEV_USER_GOOGLE_ID=$(INTERLOPER_DEV_USER_GOOGLE_ID) \
               INTERLOPER_AUTH_GOOGLE_CLIENT_ID=$(INTERLOPER_AUTH_GOOGLE_CLIENT_ID) \
               INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET=$(INTERLOPER_AUTH_GOOGLE_CLIENT_SECRET) \
               INTERLOPER_AUTH_GOOGLE_REDIRECT_URI=$(INTERLOPER_AUTH_GOOGLE_REDIRECT_URI) \
               INTERLOPER_AUTH_COOKIE_SECURE=$(INTERLOPER_AUTH_COOKIE_SECURE)

compose-up:
	cd $(DEV_DIR) && $(COMPOSE_ENV) docker compose up --build

compose-down:
	cd $(DEV_DIR) && docker compose down -v

compose-seed:
	cd $(DEV_DIR) && $(COMPOSE_ENV) docker compose up --build seed
