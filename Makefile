# Observability and owner recovery shortcuts. The cross-platform development
# stack remains available through scripts/dev.ps1 directly.
.DEFAULT_GOAL := help

PWSH ?= pwsh
OBS_COMPOSE_FILE ?= scaling/observability/compose.yaml
OBS_ENV_FILE ?= scaling/observability/.env
OWNER_PROJECT ?=
OWNER_PORT_BASE ?= 0
OWNER_RESULTS ?= artifacts/owner-recovery
OWNER_PYTHON ?= python
OWNER_TEST_FILTER ?=
OWNER_FAULT_VICTIM ?= both
OWNER_FAULT_MODE ?= both
OWNER_FAULT_REPEAT ?= 1
K6_POLICY_MODE ?= unordered
K6_POLICY_COUNT ?= 300
K6_POLICY_VUS ?= 60
K6_POLICY_TIMEOUT_SECONDS ?= 600

GOLDEN_TARGETS := ups-4 ups-8 ups-16 obs-config obs-ps obs-logs obs-down obs-clean obs-test-4k obs-test obs-test-light
.PHONY: help ups-4 ups-8 ups-16 obs-setup obs-config obs-ps obs-logs obs-down obs-clean obs-test-4k obs-test obs-test-light obs-matcher-failover

$(GOLDEN_TARGETS): export WFC_HUB_REUSE_SNAPSHOT_CONTRACTS := true
$(GOLDEN_TARGETS): export WFC_HUB_METRICS_SAMPLER_ENABLED := false
$(GOLDEN_TARGETS): export WFC_HUB_DB_POOL := 12
$(GOLDEN_TARGETS): export WFC_HUB_CLAIM_ADMISSION_ENABLED := true
$(GOLDEN_TARGETS): export WFC_HUB_CLAIM_ADMISSION_MODE := demand
$(GOLDEN_TARGETS): export WFC_HUB_CLAIM_ADMISSION_LIMIT := 3
$(GOLDEN_TARGETS): export WFC_HUB_CLAIM_ADMISSION_MAX_WAITERS := 1
$(GOLDEN_TARGETS): export WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS := false
$(GOLDEN_TARGETS): export WFC_HUB_SPLIT_BATCH_CLAIM_CANDIDATES := true
$(GOLDEN_TARGETS): export WFC_HUB_MATCHING_INDEXED_BUCKET_CLAIMS := true
$(GOLDEN_TARGETS): export WFC_HUB_COMPLETION_FAST_PATH_ENABLED := true
$(GOLDEN_TARGETS): export WFC_HUB_BATCH_HISTORY_ENABLED := true
$(GOLDEN_TARGETS): export WFC_HUB_NATIVE_EVENT_LANE_MODE := subscriptions
$(GOLDEN_TARGETS): export HUB_SCHEDULER_ENABLED := true
$(GOLDEN_TARGETS): export WFC_HUB_MATCHING_PARTITIONS := 4
$(GOLDEN_TARGETS): export WORKFLOW_WORKERS := 8
$(GOLDEN_TARGETS): export ACTIVITY_WORKERS := 8
$(GOLDEN_TARGETS): export WFC_HUB_WORKER_MAX_INFLIGHT_PER_LANE := 0
$(GOLDEN_TARGETS): export WFC_HUB_PUSH_ENABLED := true
$(GOLDEN_TARGETS): export WFC_HUB_PUSH_TRUSTED_OWNERS := http://hub:5100,http://hub-2:5100
# Existing partition-owned candidate matching is enabled in the standard stack.
$(GOLDEN_TARGETS): export WFC_HUB_MATCHING_CANDIDATES_ENABLED := true
# Exact task-ID hydration is an experimental enhancement and remains off by default.
$(GOLDEN_TARGETS): export WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED := false
# Candidate admission concurrency is configured only by the canonical setting.
$(GOLDEN_TARGETS): export WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY := 2
$(GOLDEN_TARGETS): export WFC_HUB_CANDIDATE_BACKPRESSURE := true
$(GOLDEN_TARGETS): export WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH := true
$(GOLDEN_TARGETS): export WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION := true
$(GOLDEN_TARGETS): export WFC_HUB_CANDIDATE_BATCH_SIZE := 32
# Lightweight, transaction-coupled NOTIFY signals make the outbox an
# unnecessary per-task write/poll cost in this MVP benchmark.
$(GOLDEN_TARGETS): export WFC_HUB_OUTBOX_ENABLED := false
$(GOLDEN_TARGETS): export WFC_HUB_OUTBOX_BATCH_SIZE := 128
$(GOLDEN_TARGETS): export WFC_HUB_OUTBOX_MIN_DELAY_MS := 10
$(GOLDEN_TARGETS): export WFC_HUB_OUTBOX_MAX_DELAY_MS := 1000

# Preserve the established large durable audit contract for this target.
obs-test: K6_POLICY_COUNT := 16000
obs-test: K6_POLICY_VUS := 200
obs-test: K6_POLICY_TIMEOUT_SECONDS := 1800

obs-test-light: K6_POLICY_COUNT := 100
obs-test-light: K6_POLICY_VUS := 10

help:
	@$(PWSH) -NoProfile -Command "Write-Output 'Targets: help ups-4 ups-8 ups-16 obs-setup obs-config obs-ps obs-logs obs-down obs-clean obs-test-4k obs-test obs-test-light obs-matcher-failover'; Write-Output 'obs-test-light runs 100 workflows with 10 VUs.'; Write-Output 'obs-clean removes the ups-16 Compose stack and its volumes.'; Write-Output 'Use pwsh -NoProfile -File scripts/dev.ps1 help for the development stack.'"

obs-setup:
	@$(PWSH) -NoProfile -Command "if (Test-Path -LiteralPath '$(OBS_ENV_FILE)') { Write-Host 'Keeping existing $(OBS_ENV_FILE)' } else { Copy-Item -LiteralPath 'scaling/observability/.env.example' -Destination '$(OBS_ENV_FILE)'; Write-Host 'Created $(OBS_ENV_FILE); edit its token and password before starting.' }"

ups-4:
	@$(PWSH) -NoProfile -File scripts/owner-recovery.ps1 up -ComposeFile "$(OBS_COMPOSE_FILE)" -EnvFile "$(OBS_ENV_FILE)" -Project "$(OWNER_PROJECT)" -PortBase $(OWNER_PORT_BASE) -Processors 4 -Results "$(OWNER_RESULTS)" -Python "$(OWNER_PYTHON)" -Filter "$(OWNER_TEST_FILTER)" -Victim "$(OWNER_FAULT_VICTIM)" -Mode "$(OWNER_FAULT_MODE)" -Repeat $(OWNER_FAULT_REPEAT) -StressMode "$(K6_POLICY_MODE)" -Count $(K6_POLICY_COUNT) -Vus $(K6_POLICY_VUS) -TimeoutSeconds $(K6_POLICY_TIMEOUT_SECONDS)

ups-8:
	@$(PWSH) -NoProfile -File scripts/owner-recovery.ps1 up -ComposeFile "$(OBS_COMPOSE_FILE)" -EnvFile "$(OBS_ENV_FILE)" -Project "$(OWNER_PROJECT)" -PortBase $(OWNER_PORT_BASE) -Processors 8 -Results "$(OWNER_RESULTS)" -Python "$(OWNER_PYTHON)" -Filter "$(OWNER_TEST_FILTER)" -Victim "$(OWNER_FAULT_VICTIM)" -Mode "$(OWNER_FAULT_MODE)" -Repeat $(OWNER_FAULT_REPEAT) -StressMode "$(K6_POLICY_MODE)" -Count $(K6_POLICY_COUNT) -Vus $(K6_POLICY_VUS) -TimeoutSeconds $(K6_POLICY_TIMEOUT_SECONDS)

ups-16:
	@$(PWSH) -NoProfile -File scripts/owner-recovery.ps1 up -ComposeFile "$(OBS_COMPOSE_FILE)" -EnvFile "$(OBS_ENV_FILE)" -Project "$(OWNER_PROJECT)" -PortBase $(OWNER_PORT_BASE) -Processors 16 -Results "$(OWNER_RESULTS)" -Python "$(OWNER_PYTHON)" -Filter "$(OWNER_TEST_FILTER)" -Victim "$(OWNER_FAULT_VICTIM)" -Mode "$(OWNER_FAULT_MODE)" -Repeat $(OWNER_FAULT_REPEAT) -StressMode "$(K6_POLICY_MODE)" -Count $(K6_POLICY_COUNT) -Vus $(K6_POLICY_VUS) -TimeoutSeconds $(K6_POLICY_TIMEOUT_SECONDS)

obs-config obs-ps obs-logs obs-down obs-clean obs-test-4k:
	@$(PWSH) -NoProfile -File scripts/owner-recovery.ps1 $(patsubst obs-%,%,$@) -ComposeFile "$(OBS_COMPOSE_FILE)" -EnvFile "$(OBS_ENV_FILE)" -Project "$(OWNER_PROJECT)" -PortBase $(OWNER_PORT_BASE) -Processors 16 -Results "$(OWNER_RESULTS)" -Python "$(OWNER_PYTHON)" -Filter "$(OWNER_TEST_FILTER)" -Victim "$(OWNER_FAULT_VICTIM)" -Mode "$(OWNER_FAULT_MODE)" -Repeat $(OWNER_FAULT_REPEAT) -StressMode "$(K6_POLICY_MODE)" -Count $(K6_POLICY_COUNT) -Vus $(K6_POLICY_VUS) -TimeoutSeconds $(K6_POLICY_TIMEOUT_SECONDS)

obs-matcher-failover:
	@dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
	@$(PWSH) -NoProfile -File scaling/benchmarks/hub-matching-failover-suite.ps1

obs-test obs-test-light:
	@$(PWSH) -NoProfile -File scripts/owner-recovery.ps1 stress-test -ComposeFile "$(OBS_COMPOSE_FILE)" -EnvFile "$(OBS_ENV_FILE)" -Project "$(OWNER_PROJECT)" -PortBase $(OWNER_PORT_BASE) -Processors 16 -Results "$(OWNER_RESULTS)" -Python "$(OWNER_PYTHON)" -Filter "$(OWNER_TEST_FILTER)" -Victim "$(OWNER_FAULT_VICTIM)" -Mode "$(OWNER_FAULT_MODE)" -Repeat $(OWNER_FAULT_REPEAT) -StressMode "$(K6_POLICY_MODE)" -Count $(K6_POLICY_COUNT) -Vus $(K6_POLICY_VUS) -TimeoutSeconds $(K6_POLICY_TIMEOUT_SECONDS)
