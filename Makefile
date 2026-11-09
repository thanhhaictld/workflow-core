# GNU Make aliases for the cross-platform PowerShell 7 developer script.
# PowerShell implements the development tasks; observability uses Docker Compose.
# Windows users without GNU Make can call: pwsh -File scripts/dev.ps1 up
.DEFAULT_GOAL := help

PWSH ?= pwsh
# Use PowerShell 7 for Make recipes on Windows, Linux and macOS.
# Avoid Bash-only syntax in targets; .SHELLFLAGS controls pwsh's command mode.
SHELL := $(PWSH)
.SHELLFLAGS := -NoLogo -NoProfile -NonInteractive -Command
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
COMPOSE ?= $(PWSH) -NoProfile -File scaling/observability/compose.ps1
OBS_COMPOSE_FILE ?= scaling/observability/compose.yaml
OBS_ENV_FILE ?= scaling/observability/.env
PROVISIONERS ?= 4
OFFBOARD_PROCESSORS ?= 16
# Reuse previously built service images without contacting container registries.
OBS_LOCAL_IMAGES ?= 1
ifeq ($(OBS_LOCAL_IMAGES),1)
OBS_UP_IMAGE_FLAGS = --pull never
else
OBS_UP_IMAGE_FLAGS = --build
endif
OBS_BUILD_DNS ?= 0
ifeq ($(OBS_BUILD_DNS),1)
OBS_BUILD_DNS_COMPOSE = -f "scaling/observability/compose.build-dns.yaml"
OBS_BUILD_DNS_HA_COMPOSE = -f "scaling/observability/compose.build-dns-ha.yaml"
endif
OBS_COMPOSE = $(COMPOSE) -f "$(OBS_COMPOSE_FILE)" $(OBS_BUILD_DNS_COMPOSE) --env-file "$(OBS_ENV_FILE)"
# Preserve the base file and layer matching HA and four-shard ownership.
OBS_FANOUT_COMPOSE = $(COMPOSE) -f "$(OBS_COMPOSE_FILE)" -f "scaling/observability/compose.matching-ha.yaml" -f "scaling/observability/compose.matching-shards.yaml" $(OBS_BUILD_DNS_COMPOSE) $(OBS_BUILD_DNS_HA_COMPOSE) --env-file "$(OBS_ENV_FILE)"
FANOUT_MATCHING_SHARDS ?= 4
FANOUT_MAX_SHARD_POLLS ?= 4
K6_OFFBOARD_VUS ?= 200
K6_OFFBOARD_COUNT ?= 1000


.PHONY: obs-setup obs-up obs-scale obs-stress-test obs-down obs-clean obs-ps obs-offboarding-up obs-offboarding-test obs-offboarding-ps
.PHONY: obs-offboarding-fanout-up obs-offboarding-fanout-test obs-offboarding-fanout-ps obs-offboarding-fanout-logs obs-offboarding-fanout-down

# These target-specific exports reach Docker Compose on every platform,
# including when GNU Make runs with pwsh. They do not affect normal obs-*.
FANOUT_TARGETS := obs-offboarding-fanout-up obs-offboarding-fanout-test obs-offboarding-fanout-ps obs-offboarding-fanout-logs obs-offboarding-fanout-down
$(FANOUT_TARGETS): export WFC_HUB_WORKER_CAPACITY_FANOUT := true
$(FANOUT_TARGETS): export WFC_HUB_WORKER_MAX_SHARD_POLLS := $(FANOUT_MAX_SHARD_POLLS)
$(FANOUT_TARGETS): export WFC_HUB_MATCHING_PARTITIONS := $(FANOUT_MATCHING_SHARDS)
$(FANOUT_TARGETS): export WFC_HUB_MATCHING_EVENT_GATED := true
$(FANOUT_TARGETS): export WFC_HUB_LONG_POLL_ENABLED := true

# Server-initiated task leases over persistent SSE. Credit replenishment uses
# authenticated REST; existing long-poll flags remain OFF for this target.
SSE_PUSH_TARGETS := obs-offboarding-sse-up obs-offboarding-sse-test obs-offboarding-sse-ps obs-offboarding-sse-logs obs-offboarding-sse-down
.PHONY: $(SSE_PUSH_TARGETS)
$(SSE_PUSH_TARGETS): export WFC_HUB_TASK_SSE_PUSH := true
$(SSE_PUSH_TARGETS): export WFC_HUB_WORKER_CAPACITY_FANOUT := false
$(SSE_PUSH_TARGETS): export WFC_HUB_MATCHING_PARTITIONS := $(FANOUT_MATCHING_SHARDS)
$(SSE_PUSH_TARGETS): export WFC_HUB_MATCHING_EVENT_GATED := true
$(SSE_PUSH_TARGETS): export WFC_HUB_LONG_POLL_ENABLED := true
obs-setup:
	@$(PWSH) -NoProfile -Command "if (Test-Path -LiteralPath '$(OBS_ENV_FILE)') { Write-Host 'Keeping existing $(OBS_ENV_FILE)' } else { Copy-Item -LiteralPath 'scaling/observability/.env.example' -Destination '$(OBS_ENV_FILE)'; Write-Host 'Created $(OBS_ENV_FILE); edit its token and password before starting.' }"
obs-up:
	$(OBS_COMPOSE) up -d $(OBS_UP_IMAGE_FLAGS)

	
# Push variant of the same two-Hub + four-matching-shard laboratory.
obs-onboarding-sse-up:
	$(OBS_FANOUT_COMPOSE) --profile onboarding-load up -d $(OBS_UP_IMAGE_FLAGS) --scale processor=$(OFFBOARD_PROCESSORS)

obs-onboarding-sse-test:
	$(OBS_FANOUT_COMPOSE) --profile onboarding-k6 run --rm -e K6_OFFBOARD_VUS=$(K6_OFFBOARD_VUS) -e K6_OFFBOARD_COUNT=$(K6_OFFBOARD_COUNT) k6-onboarding

obs-scale:
	$(OBS_COMPOSE) up -d

obs-stress-test:
	$(OBS_COMPOSE) --profile load run --rm k6

# Run exactly eight native-only offboarding processor containers; disable the
# normal onboarding Provisioner replicas to make the test easy to interpret.
obs-offboarding-up:
	$(OBS_COMPOSE) --profile offboarding-load up -d $(OBS_UP_IMAGE_FLAGS) --scale processor=$(OFFBOARD_PROCESSORS)

# Run exactly eight native-only offboarding processor containers; disable the
# normal onboarding Provisioner replicas to make the test easy to interpret.
obs-offboarding-stats:
	$(OBS_COMPOSE) --profile offboarding-load stats


obs-offboarding-test-light:
	$(OBS_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=10 -e K6_OFFBOARD_COUNT=100 k6-offboarding

obs-offboarding-test:
	$(OBS_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=$(K6_OFFBOARD_VUS) -e K6_OFFBOARD_COUNT=$(K6_OFFBOARD_COUNT) k6-offboarding

obs-offboarding-test-4k:
	$(OBS_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=100 -e K6_OFFBOARD_COUNT=4000 k6-offboarding

obs-offboarding-test-100b:
	$(OBS_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=$(K6_OFFBOARD_VUS) k6-offboarding

obs-offboarding-ps:
	$(OBS_COMPOSE) --profile offboarding-load ps

# One command for the full fanout experiment: PostgreSQL + both matching Hubs
# + HA load balancer + four matching shards + native-only processor replicas.
# Requires obs-setup (configured scaling/observability/.env) before first run.
obs-offboarding-fanout-up:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load up -d $(OBS_UP_IMAGE_FLAGS) --scale processor=$(OFFBOARD_PROCESSORS)

# Use the identical three-file topology for k6, inspection and cleanup.
obs-offboarding-fanout-test:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=$(K6_OFFBOARD_VUS) -e K6_OFFBOARD_COUNT=$(K6_OFFBOARD_COUNT) k6-offboarding

# Use the identical three-file topology for k6, inspection and cleanup.
obs-offboarding-fanout-test-4k:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=4000 -e K6_OFFBOARD_COUNT=10000 k6-offboarding

obs-offboarding-fanout-ps:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load ps

obs-offboarding-fanout-logs:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load logs --tail=100 processor hub hub-2 hub-lb

# This only stops the stack. It does not delete PostgreSQL or other volumes.
obs-offboarding-fanout-down:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load down

# Push variant of the same two-Hub + four-matching-shard laboratory.
obs-offboarding-sse-up:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load up -d $(OBS_UP_IMAGE_FLAGS) --scale processor=$(OFFBOARD_PROCESSORS)

obs-offboarding-sse-test:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=$(K6_OFFBOARD_VUS) -e K6_OFFBOARD_COUNT=$(K6_OFFBOARD_COUNT) k6-offboarding

obs-offboarding-sse-test-4k:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-k6 run --rm -e K6_OFFBOARD_VUS=400 -e K6_OFFBOARD_COUNT=10000 k6-offboarding

obs-offboarding-sse-ps:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load ps

obs-offboarding-sse-logs:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load logs --tail=100 processor hub hub-2 hub-lb

obs-offboarding-sse-down:
	$(OBS_FANOUT_COMPOSE) --profile offboarding-load down

obs-down:
	$(OBS_FANOUT_COMPOSE) --profile * down

# Destructive: deletes named volumes belonging to this Compose project.
obs-clean:
	$(OBS_COMPOSE) --profile * down --volumes

obs-ps:
	$(OBS_COMPOSE) --profile * ps -a

# Standalone two-Hub/four-shard ownership recovery suite. Its worktree-derived
# project, ports and retained PostgreSQL volumes are independent of OBS_COMPOSE.
OWNER_PROJECT ?=
OWNER_PORT_BASE ?= 26200
OWNER_RESULTS ?= artifacts/owner-recovery
OWNER_PYTHON ?= python
OWNER_TEST_FILTER ?=
OWNER_FAULT_VICTIM ?= both
OWNER_FAULT_MODE ?= both
OWNER_FAULT_REPEAT ?= 1
OWNER_TARGETS := help config build up ps logs down db-test smoke-test test fault-test
.PHONY: $(addprefix obs-owner-recovery-,$(OWNER_TARGETS))
$(addprefix obs-owner-recovery-,$(OWNER_TARGETS)):
	@$(PWSH) -NoProfile -File scripts/owner-recovery.ps1 $(patsubst obs-owner-recovery-%,%,$@) -Project "$(OWNER_PROJECT)" -PortBase $(OWNER_PORT_BASE) -Results "$(OWNER_RESULTS)" -Python "$(OWNER_PYTHON)" -Filter "$(OWNER_TEST_FILTER)" -Victim "$(OWNER_FAULT_VICTIM)" -Mode "$(OWNER_FAULT_MODE)" -Repeat $(OWNER_FAULT_REPEAT)
