SHELL := /usr/bin/env bash

RUNTIME_ENV_FILE ?= /etc/institutional-signal-engine/runtime.env
COMPOSE_FILE := infra/vast/compose.yaml

.PHONY: setup lint typecheck test build

setup:
	sudo bash infra/vast/bootstrap.sh

lint:
	shellcheck infra/vast/bootstrap.sh infra/vast/verify.sh
	bash -n infra/vast/bootstrap.sh infra/vast/verify.sh

typecheck:
	POSTGRES_DB=check POSTGRES_USER=check POSTGRES_PASSWORD=not-a-secret REDIS_PASSWORD=not-a-secret docker compose -f $(COMPOSE_FILE) config --quiet
	systemd-analyze verify infra/vast/systemd/institutional-signal-dependencies.service

test:
	sudo bash infra/vast/verify.sh --persistence

build:
	docker compose --env-file $(RUNTIME_ENV_FILE) -f $(COMPOSE_FILE) pull
