# GNU Make aliases for the cross-platform PowerShell 7 developer script.
# PowerShell is the single implementation for Windows, macOS and Linux.
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

export PG_PORT PG_USER PG_PASSWORD PG_DATABASE API_PORT UI_PORT CONFIRM

TASKS := help check-tools bootstrap setup up dev api worker ui install db-up db-ef db-schema db-down db-logs db-reset seed approve test build check clean
.PHONY: $(TASKS)

# No shell-specific logic here. All task orchestration belongs to scripts/dev.ps1.
$(TASKS):
	@$(PWSH) -NoProfile -File scripts/dev.ps1 $@
