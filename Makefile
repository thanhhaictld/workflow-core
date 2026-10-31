# Local development: Workflow Core distributed PostgreSQL runtime + Next.js monitor.
# GNU Make and a POSIX shell are required (macOS/Linux/WSL/Git Bash).
.DEFAULT_GOAL := help
SHELL := /bin/sh
.SHELLFLAGS := -eu -c

PG_PORT ?= 55432
PG_USER ?= workflow
PG_PASSWORD ?= workflow_dev_only
PG_DATABASE ?= workflow
API_PORT ?= 5000
UI_PORT ?= 3000

DOTNET ?= dotnet
NPM ?= npm
DOCKER ?= docker
COMPOSE = $(DOCKER) compose -f scaling/compose.monitor.yaml
API_PROJECT = src/samples/WorkflowCore.Scaling.Api/WorkflowCore.Scaling.Api.csproj
TEST_PROJECT = tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj

# Default credentials are ONLY for local loopback development.
WFC_PG ?= Host=127.0.0.1;Port=$(PG_PORT);Database=$(PG_DATABASE);Username=$(PG_USER);Password=$(PG_PASSWORD)
export WFC_PG PG_PORT PG_USER PG_PASSWORD PG_DATABASE

.PHONY: help check-tools bootstrap setup up dev api ui install db-up db-ef db-schema db-down db-logs db-reset seed test build check clean

help: ## Display available commands
	@printf '\nWorkflow Core Monitor (local development)\n\n'
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nQuick start: make bootstrap && make dev; open http://127.0.0.1:$(UI_PORT)/workflows\n\n'

check-tools: ## Verify Docker Compose v2, .NET SDK 10, Node 22+ and npm
	@for tool in $(DOCKER) $(DOTNET) node $(NPM); do \
		if ! command -v "$$tool" >/dev/null 2>&1; then \
			echo "Missing prerequisite: $$tool"; exit 1; \
		fi; \
	done
	@$(DOCKER) compose version >/dev/null || { echo "Docker Compose v2 is required"; exit 1; }
	@$(DOTNET) --list-sdks | grep -q '^10\.' || { echo ".NET 10 SDK is required"; exit 1; }
	@node -e 'if (+process.versions.node.split(".")[0] < 22) { console.error("Node.js 22+ is required"); process.exit(1) }'
	@printf 'All prerequisites found.\n'

db-up: ## Start local PostgreSQL 17 and wait for readiness
	@$(COMPOSE) up -d --wait postgres

db-ef: db-up ## Apply Workflow Core EF Core migrations (idempotent)
	@echo "Applying Workflow Core EF Core migrations..."
	@WFC_MIGRATE_ONLY=1 $(DOTNET) run --no-launch-profile --project $(API_PROJECT)

db-schema: db-ef ## Apply distributed runtime and execution journal migrations 002-005
	@echo "Applying PostgreSQL distributed schema migrations..."
	@for file in 002_distributed_runtime.sql 003_signal_delivery.sql 004_request_dedupe.sql 005_execution_history.sql; do \
		echo "  -> $$file"; \
		$(COMPOSE) exec -T postgres psql -U "$(PG_USER)" -d "$(PG_DATABASE)" -v ON_ERROR_STOP=1 -f "/migrations/$$file" || exit $$?; \
	done

install: ## Install Next.js dependencies and initialize .env.local
	@cd monitor && if [ -f package-lock.json ]; then $(NPM) ci --no-audit --no-fund; else $(NPM) install --no-audit --no-fund; fi
	@if [ ! -f monitor/.env.local ]; then cp monitor/.env.example monitor/.env.local; echo "Created monitor/.env.local"; fi

bootstrap: check-tools db-schema install ## One-time setup: DB, migrations, UI packages, env
	@printf '\nBootstrap complete. Start the stack with: make dev\n'

setup: bootstrap ## Alias for bootstrap

up: dev ## Bootstrap and launch API + monitor in foreground

dev: bootstrap ## Start API + Next.js together; Ctrl+C stops both
	@echo "API: http://127.0.0.1:$(API_PORT) | Dashboard: http://127.0.0.1:$(UI_PORT)/workflows"
	@set -eu; \
		$(DOTNET) run --no-launch-profile --project $(API_PROJECT) --urls "http://127.0.0.1:$(API_PORT)" & \
		api_pid=$$!; \
		(cd monitor && WORKFLOW_API_URL="http://127.0.0.1:$(API_PORT)" $(NPM) run dev -- --hostname 127.0.0.1 --port $(UI_PORT)) & \
		ui_pid=$$!; \
		trap 'kill $$api_pid $$ui_pid 2>/dev/null || true; wait $$api_pid $$ui_pid 2>/dev/null || true' EXIT; \
		trap 'exit 130' INT; \
		trap 'exit 143' TERM; \
		while kill -0 $$api_pid 2>/dev/null && kill -0 $$ui_pid 2>/dev/null; do sleep 1; done; \
		echo "A development process exited; stopping the other process."

api: db-schema ## Start only .NET API on loopback
	@$(DOTNET) run --no-launch-profile --project $(API_PROJECT) --urls "http://127.0.0.1:$(API_PORT)"

ui: install ## Start only Next.js monitor on loopback
	@cd monitor && WORKFLOW_API_URL="http://127.0.0.1:$(API_PORT)" $(NPM) run dev -- --hostname 127.0.0.1 --port $(UI_PORT)

seed: ## Generate five demo workflows; API must already be running
	@command -v curl >/dev/null 2>&1 || { echo "curl is required for seeding"; exit 1; }
	@for i in 1 2 3 4 5; do \
		printf 'Demo workflow %s: ' "$$i"; \
		curl --fail-with-body --silent --show-error -X POST "http://127.0.0.1:$(API_PORT)/api/workflows" \
			-H 'Content-Type: application/json' \
			-d '{"tenantId":"bootstrap-demo-'"$$i"'"}' || exit $$?; \
		printf '\n'; \
	done

test: db-schema ## Run PostgreSQL integration tests with local connection
	@WFC_TEST_PG="$(WFC_PG)" $(DOTNET) test $(TEST_PROJECT) -c Release

check: install ## Type-check Next.js monitor
	@cd monitor && $(NPM) run check-types

build: install ## Build .NET API and Next.js monitor
	@$(DOTNET) build $(API_PROJECT) -c Release
	@cd monitor && $(NPM) run build

db-logs: ## Follow PostgreSQL logs
	@$(COMPOSE) logs -f postgres

db-down: ## Stop PostgreSQL without deleting local data
	@$(COMPOSE) down

db-reset: ## DESTRUCTIVE: delete local PostgreSQL volume (CONFIRM=reset)
	@if [ "$(CONFIRM)" != "reset" ]; then \
		echo "Refusing to remove the DB volume. Use: make db-reset CONFIRM=reset"; exit 1; \
	fi
	@$(COMPOSE) down -v
	@echo "Database deleted; run make bootstrap to recreate it."

clean: ## Clear Next.js build output (retains DB and dependencies)
	@rm -rf monitor/.next monitor/tsconfig.tsbuildinfo
