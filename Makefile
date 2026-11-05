# GNU Make aliases for the cross-platform PowerShell 7 developer script.
# PowerShell implements the development tasks; observability uses Docker Compose.
# Windows users without GNU Make can call: pwsh -File scripts/dev.ps1 up
.DEFAULT_GOAL := help

PWSH ?= pwsh
PG_PORT ?= 55432
PG_USER ?= workflow
PG_PASSWORD ?= workflow_dev_only
PG_DATABASE ?= workflow
API_PORT ?= 5000
UI_PORT ?= 3000
CONFIRM ?=


TASKS := help check-tools bootstrap setup up dev api worker ui install db-up db-ef db-schema db-down db-logs db-reset seed test build check clean
.PHONY: $(TASKS)

# Development tasks are implemented by scripts/dev.ps1.
$(TASKS) approve: export PG_PORT := $(PG_PORT)
$(TASKS) approve: export PG_USER := $(PG_USER)
$(TASKS) approve: export PG_PASSWORD := $(PG_PASSWORD)
$(TASKS) approve: export PG_DATABASE := $(PG_DATABASE)
$(TASKS) approve: export API_PORT := $(API_PORT)
$(TASKS) approve: export UI_PORT := $(UI_PORT)
$(TASKS) approve: export CONFIRM := $(CONFIRM)
$(TASKS):
	@$(PWSH) -NoProfile -File scripts/dev.ps1 $@

# Approval uses an explicit workflow id: make approve WORKFLOW_ID=<uuid>
.PHONY: approve
WORKFLOW_ID ?=
approve:
	@$(PWSH) -NoProfile -File scripts/dev.ps1 approve -WorkflowId "$(WORKFLOW_ID)"

# Observability Compose stack. The env file supplies Compose settings; make's
# development-stack defaults above must not override its PostgreSQL credentials.
COMPOSE ?= docker compose
OBS_COMPOSE_FILE ?= scaling/observability/compose.yaml
OBS_ENV_FILE ?= scaling/observability/.env
PROVISIONERS ?= 4
OBS_COMPOSE = $(COMPOSE) -f "$(OBS_COMPOSE_FILE)" --env-file "$(OBS_ENV_FILE)"

.PHONY: obs-setup obs-up obs-scale obs-stress-test obs-down obs-clean obs-ps
obs-setup:
	@$(PWSH) -NoProfile -Command "if (Test-Path -LiteralPath '$(OBS_ENV_FILE)') { Write-Host 'Keeping existing $(OBS_ENV_FILE)' } else { Copy-Item -LiteralPath 'scaling/observability/.env.example' -Destination '$(OBS_ENV_FILE)'; Write-Host 'Created $(OBS_ENV_FILE); edit its token and password before starting.' }"
obs-up:
	$(OBS_COMPOSE) up -d --build --scale provisioner=$(PROVISIONERS)

obs-scale:
	$(OBS_COMPOSE) up -d --scale provisioner=$(PROVISIONERS)

obs-stress-test:
	$(OBS_COMPOSE) --profile load run --rm k6

obs-down:
	$(OBS_COMPOSE) down

# Destructive: deletes named volumes belonging to this Compose project.
obs-clean:
	$(OBS_COMPOSE) down --volumes

obs-ps:
	$(OBS_COMPOSE) ps
