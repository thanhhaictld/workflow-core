# Isolated Hub audit-export SQL experiment

This is a diagnostic only. It creates a uniquely named, disposable PostgreSQL
17 container with no network, no host port, no explicit/external mounts, and 1
CPU / 1 GiB memory limits. The official image supplies an anonymous ephemeral
data volume that `--rm` removes with the container. It generates synthetic
`hub_workflow` and `hub_workflow_event` history,
then compares the exact full-row JSON aggregate shape used by
`scaling/observability/benchmark/owner_recovery_lab.py::export_run` with one
workflow-ID-indexed LATERAL prototype.

The large fixture matches the observed local history estimate (66,472 workflows
and 898,501 event rows) and its 4,000-workflow audit cohort (56,000 events,
6.2% of event history). A smaller fixture has 7,000 workflows and 98,000 events,
with the same 4,000-workflow / 56,000-event cohort (57.1% of history). The
cohort event payloads are synthetic and sized approximately from the observed
export; no shared database rows or credentials are copied.

Run from the repository root:

```powershell
python scaling/benchmarks/hub-audit-export-experiment/run_experiment.py `
  --artifact-dir C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/sequential-event-reads-luna/isolated
```

The harness refuses to use an existing container name, records its identity and
raw JSON `EXPLAIN ANALYZE BUFFERS SERIALIZE TEXT` plans, executes both complete
JSON exports, and verifies their output row multisets by event ID. It retains
both measured query orders and both output files. The generated container is
stopped and removed by its `--rm` policy after success or failure. The script
does not inspect or modify the shared Compose project.

The LATERAL `OFFSET 0` subquery is only a candidate shape, not a plan guarantee;
the saved actual plans are the evidence. Results are specific to this synthetic
fixture, PostgreSQL 17 image and resource limits, and do not predict end-to-end
CPU or workflow throughput.
