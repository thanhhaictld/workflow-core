> [Distributed Hub overview](../index.md) · [Learning path](../learning-path.md)

# Troubleshooting, cleanup and further reading

## Troubleshooting and cleanup

| Symptom | Check first |
| --- | --- |
| No workflow definition | Is the Provisioner/processor started and did it register the requested **version**? |
| `401` from Hub | Bearer token differs between caller and Hub |
| `hub-lb` unavailable | Both Hub readiness checks, nginx startup, host port collision (`HUB_LB_PORT`) |
| Workers connect but no tasks run | Trusted direct owner URLs, partition count, candidate matching, available credits, DB lease state |
| More workers, less throughput | PostgreSQL pool/CPU, matching admission waits, task-claim contention, backlog p95 |
| Retry duplicates external effect | Use the stable dispatch key for idempotency; leases provide **at-least-once**, not exactly-once side effects |
| Two PostgreSQL nodes do not increase writes | Standby is read-only; all Hub claims and commits still hit the primary |

```powershell
# Stop Day 1 Hub and Provisioner via Ctrl+C in their terminals.
pwsh -NoProfile -File scripts/dev.ps1 db-down      # retains Day 1 PostgreSQL volume

# Stop Day 2 stack; retain PostgreSQL and telemetry volumes.
pwsh -NoProfile -File scaling/observability/compose.ps1 @compose down

# DANGEROUS: 'down -v' deletes named database/telemetry volumes.
# Only use after backing up and explicitly deciding to discard lab data.
```

## Source map and next reading

- [Current Hub runtime, matching ownership, SSE and failure semantics](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/current-runtime.md)
- [Native fluent workflow authoring contract](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/hub-native-fluent-runtime.md)
- [Current Compose stack and k6 guide](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/observability/README.md)
- [Hub matching/claim SQL flow](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/hub-matching-claim-flow.md)
- [Demand-aware claim admission and shared DB-pool protection](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/demand-aware-claim-admission.md)
- [Native commit A–D optimization and benchmark report](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/native-commit-phased-optimization.md)
- [Single vs batch completion transaction experiment](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/native-task-completion-batch-commit-experiment.md)
- [Official PostgreSQL streaming replication documentation](https://www.postgresql.org/docs/17/warm-standby.html)

> **GitHub rendering:** Mermaid diagrams render on GitHub repository Markdown pages. If publishing with GitHub Pages/Jekyll, configure Mermaid rendering in the Pages theme or substitute static diagrams.

> **Finish line:** By the end of Day 1 you can deploy and inspect a real, durable native C# workflow. By the end of Day 2 you can scale Hub owners and processors, run a correct A/B benchmark, explain when batching trades latency for throughput, and distinguish database **high availability** from database **write scalability**.