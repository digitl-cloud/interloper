.PHONY: build


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

REGISTRY      := europe-docker.pkg.dev/dc-int-connectors-prd/docker/interloper
VERSION       := $(shell python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
CORE_EXTRAS   ?= google-cloud
ASSETS_EXTRAS ?= bing,facebook,google

# Image catalog. Two-tier:
#   ROLES                  → one image per role, tag = "<v>-<role>"
#   ROLES_LAUNCHER_AWARE   → one image per (role, launcher) pair:
#                              base   (no launcher extras) → tag "<v>-<role>"
#                              -k8s   → tag "<v>-<role>-k8s"
#                              -docker→ tag "<v>-<role>-docker"
# The chart picks the suffix from config.launcher.type — no manual mapping.
ROLES                := api frontend
ROLES_LAUNCHER_AWARE := scheduler worker
LAUNCHERS            := k8s docker

# Concrete target stems built by `docker-build`.
TARGETS := $(ROLES) $(ROLES_LAUNCHER_AWARE) \
           $(foreach r,$(ROLES_LAUNCHER_AWARE), \
             $(foreach l,$(LAUNCHERS),$(r)-$(l)))

# Parse a target stem (e.g. "scheduler-k8s") into role + launcher.
# Launcher is empty for base targets ("scheduler", "api", …).
launcher_of = $(if $(filter %-k8s %-docker,$(1)),$(lastword $(subst -, ,$(1))))
role_of     = $(if $(call launcher_of,$(1)),$(patsubst %-$(call launcher_of,$(1)),%,$(1)),$(1))

# Pattern rules. Order matters on macOS' GNU make 3.81 (no shortest-stem):
# the more specific docker-build-linux-% must come first.
docker-build-linux-%:
	docker build --target $(call role_of,$*) --platform linux/amd64 \
		--build-arg CORE_EXTRAS=$(CORE_EXTRAS) \
		--build-arg ASSETS_EXTRAS=$(ASSETS_EXTRAS) \
		--build-arg SCHEDULER_EXTRAS=$(call launcher_of,$*) \
		-t interloper:$(VERSION)-$* \
		-t interloper:latest-$* \
		-t $(REGISTRY):$(VERSION)-$* \
		-t $(REGISTRY):latest-$* .

docker-build-%:
	docker build --target $(call role_of,$*) \
		--build-arg CORE_EXTRAS=$(CORE_EXTRAS) \
		--build-arg ASSETS_EXTRAS=$(ASSETS_EXTRAS) \
		--build-arg SCHEDULER_EXTRAS=$(call launcher_of,$*) \
		-t interloper:$(VERSION)-$* \
		-t interloper:latest-$* \
		-t $(REGISTRY):$(VERSION)-$* \
		-t $(REGISTRY):latest-$* .

docker-push-%:
	docker push $(REGISTRY):$(VERSION)-$*
	docker push $(REGISTRY):latest-$*

docker-build:       $(addprefix docker-build-,$(TARGETS))
docker-build-linux: $(addprefix docker-build-linux-,$(TARGETS))
docker-push:        $(addprefix docker-push-,$(TARGETS))

docker-build-push:
	$(MAKE) docker-build-linux
	$(MAKE) docker-push


# ###############
# GIT
# ###############

claude-commit:
	claude --dangerously-skip-permissions --model haiku -p "Create a git commit for all staged changes. Use a single-line commit message following the Conventional Commits format (e.g. feat:, fix:, chore:, refactor, etc...). Keep it compact. Do not add co-author information. Do not push."

codex-commit:
	codex exec --dangerous-skip-permissions --model gpt-5-codex-mini "Create a git commit for all staged changes. Use a single-line commit message following the Conventional Commits format (e.g. feat:, fix:, chore:, refactor, etc...). Keep it compact. Do not add co-author information. Do not push."
