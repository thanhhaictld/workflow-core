# Workflow p95 A/B experiment

PR #18 compares baseline and optimized modes for 100 workflows per ordering policy, at 8 and 16 processors.

Baseline disables `WFC_HUB_ADAPTIVE_CLAIMS`, `WFC_HUB_CLAIM_ADMISSION_ENABLED`, and `WFC_HUB_COMPLETION_FAST_PATH_ENABLED`. Optimized mode enables all three.

The GitHub Actions workflow `.github/workflows/workflow-hub-p99-diagnostic.yml` uploads traces, K6 summaries, PostgreSQL waits and durable workflow timelines. Compare completed workflows/second, workflow p95/p99, activity queue p95/p99, completion HTTP latency and empty claims. Require zero failures, retries and FIFO overlaps. The CI runner has shared CPUs; this does not establish independent-host scaling.
