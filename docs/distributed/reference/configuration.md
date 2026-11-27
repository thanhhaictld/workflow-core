# Configuration reference (Hub-native)

Settings below are relevant to the **Hub-native REST/SSE runtime**, not the original embedded `IWorkflowHost`. Values represent the current repository development presets, **not necessarily library defaults**.

## Hub and PostgreSQL

| Environment variable | Local behavior |
| --- | --- |
| `WFC_PG` | PostgreSQL connection string on Hub only; never on processors |
| `WFC_HUB_AUTH_TOKEN` | Bearer token for the local Hub REST APIs |
| `WFC_HUB_URL` | Processor/Provisioner REST base address |
| `WFC_HUB_DB_POOL` | Npgsql pool cap per Hub; typical Compose preset `12` |
| `HUB_SCHEDULER_ENABLED` | Enables embedded Hub timer scheduler |
| `WFC_HUB_PUSH_ENABLED` | Enables fenced SSE assignment delivery |

## Matching and claim tuning

| Variable | Golden preset | Meaning |
| --- | --- | --- |
| `WFC_HUB_MATCHING_CANDIDATES_ENABLED` | `true` | Partition-owned candidate matching |
| `WFC_HUB_MATCHING_PARTITIONS` | `4` | Matching owner shards (consistent on all peers) |
| `WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED` | `false` | Separate experimental exact-ID hints |
| `WFC_HUB_CANDIDATE_BACKPRESSURE` | `true` | Bounded demand-aware matching |
| `WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH` | `true` | Group compatible credits across processors |
| `WFC_HUB_CANDIDATE_BATCH_SIZE` | `32` | Upper bound on candidate admission groups |
| `WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION` | `true` | Opt-in concurrent admission groups |
| `WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY` | `2` | Admission groups per Hub |
| `WFC_HUB_CLAIM_ADMISSION_MODE` | `demand` | Defer extra speculative requests before DB |
| `WFC_HUB_CLAIM_ADMISSION_LIMIT` | `3` | Local SQL-claim gate, protecting completion pool access |
| `WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS` | `false` | Independent PostgreSQL set-based claim path |

## Optional native workflow commit improvements

| Flag | What it changes |
| --- | --- |
| `WFC_HUB_NATIVE_SKIP_EMPTY_SUBS_SQL` | Skips no-op subscription work |
| `WFC_HUB_NATIVE_BULK_EVENT_SQL` | Bulk inbox consumption and subscription inserts |
| `WFC_HUB_NATIVE_BULK_ACTIVITIES` | Bulk scheduled activity/task history inserts |
| `WFC_HUB_NATIVE_COMPACT_RECEIPT` | Compact v2 commit receipt rather than full duplicated JSON |

For the lab, `scaling/observability/compose.native-commit-abcd.yaml` opts in to A–D on **both** Hub replicas. Ordinary application defaults outside that overlay remain unchanged. Before compact v2 writes can be enabled on a rolling deployment, **all Hub instances must support reading both v1 and v2 receipts**.

Inspect effective Compose settings with `make obs-config` (golden lab) or `pwsh -NoProfile -File scaling/observability/compose.ps1 @compose config` (manual lab). The Make preset exports flags and may override local `.env` values.

Full authority: [Current runtime settings](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/current-runtime.md) and [actual Makefile](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/Makefile).
