# Native commit A–D integration on Phase D

Branch/PR: `opt/native-commit-d-compact-receipt-merged` / #56, stacked on A #51, B #52 and C #53.

## Enabling all improvements

The optional Compose overlay `scaling/observability/compose.native-commit-abcd.yaml` enables each native optimization on both Hub replicas:

- `WFC_HUB_NATIVE_SKIP_EMPTY_SUBS_SQL=true` — skip empty subscription work (A)
- `WFC_HUB_NATIVE_BULK_EVENT_SQL=true` — bulk inbox/subscriptions (B)
- `WFC_HUB_NATIVE_BULK_ACTIVITIES=true` — bulk scheduled task/history inserts (C)
- `WFC_HUB_NATIVE_COMPACT_RECEIPT=true` — hashed v2 replay receipt (D)

Append the overlay to `compose.yaml`, `compose.matching-ha.yaml`, and `compose.matching-shards.yaml` when launching the two Hub replicas. Every option remains independently reversible by setting its variable to `false`.

**Safety:** a pre-upgrade Hub cannot verify compact v2 commit receipts. Deploy the dual-reader application everywhere with D **OFF**, then enable D once no old Hub replicas remain. The overlay is opt-in and must not be applied to a rolling mixed-version cluster. Native journal/snapshot, epoch fencing and atomic transactions are preserved.

## Full-workflow CI

`.github/workflows/native-commit-abcd-e2e-1000.yml` runs OFF→ON→ON→OFF, 1,000 `tenant-offboarding-fast` completions per stage, 2 Hubs, 16 processors and 200 k6 VUs. Every stage recreates PostgreSQL and verifies exactly 1,000 completed workflows plus persisted history. It records end-to-end workflow throughput, p95 and p99, PostgreSQL/Hub cgroup CPU, WAL and physical I/O. Full evidence appears in `native-commit-abcd-e2e-1000` CI artifacts and `comparison.json`.

The fast offboarding workflow has no external event waits or routed activity fan-out. This E2E test primarily measures A/D behavior; B/C have separate real-PostgreSQL persistence benchmarks and integration tests. Repeat end-to-end parallel onboarding when a fully configured routed-activity environment is available.
