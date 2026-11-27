# Deploy and observe the local Hub lab

Use this page after [Day 1](../tutorials/day-1-first-workflow.md). It describes a **local Docker Compose development environment**, not a production Kubernetes manifest.

## Start the supported development preset

From the repository root (GNU Make, PowerShell 7, Docker Compose and .NET 10):

```bash
make obs-setup
# Change PG_PASSWORD and WFC_HUB_AUTH_TOKEN in scaling/observability/.env
make ups-16
make obs-ps
make obs-logs
```

This preset uses two Hub instances, four matching partitions, one PostgreSQL 17 primary, 16 processor containers, an embedded scheduler, load balancing and the OpenTelemetry / Grafana stack. The Make target exports matching/admission flags, so inspect them with `make obs-config` rather than assuming the `.env` file is the complete effective config.

To run a smaller two-processor manual lab, follow [Day 2: multi-Hub setup](../tutorials/day-2-multi-hub.md).

## Observe the system

| Local service | URL / signal |
| --- | --- |
| Hub A REST/health | `http://127.0.0.1:5100/health/ready` |
| Two-Hub load balancer | `http://127.0.0.1:5110/health/ready` |
| Grafana | `http://127.0.0.1:3001` |
| Prometheus | `http://127.0.0.1:9090` |
| Next.js workflow monitor | `http://127.0.0.1:3002` |

Ports may be overridden by the lab's environment file or owner-recovery launcher. Inspect `make obs-config` and `make obs-ps` for the actual host mapping.

Useful dashboards: durable workflow backlog, ready-work age, matching and admission rates, worker credits, PostgreSQL CPU/WAL and locks, p95/p99 schedule-to-start and completion. Use **per-cohort before/after counters** to attribute database cost.

## Security and cleanup

Only Hub infrastructure receives `WFC_PG`; processors get the Hub REST URL and token. The local `.env.example` passwords are **not production credentials**, and the sample's HTTP transport is for loopback/isolated networks only. Use a secret manager, authenticated TLS and tenant isolation in a real deployment.

```bash
make obs-down    # stop; retain named PostgreSQL/telemetry volumes
# make obs-clean # destructive: deletes the lab's persistent volumes
```

For current failure semantics and operational guardrails, consult the [engineering runbook](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/current-runtime.md) and [production-readiness checklist](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/production-readiness.md).
