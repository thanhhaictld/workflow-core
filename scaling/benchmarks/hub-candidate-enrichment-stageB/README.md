# Incomplete comparison attempt (excluded)

`raw-paired/` contains an interrupted 4-scenario run. It was stopped after
repetition 3 because the first telemetry collector scrape reused identical
service-instance labels across restarted Hub processes, yielding negative
counter deltas. The PostgreSQL/workload raw records are retained unchanged but
this directory is not used for performance conclusions. `raw-controlled/`
contains the first 5-run selected-scenario comparison. Although each process
had a unique instance label, the scrape aggregated stale series from earlier
runs and some deltas were negative; its raw DB/workload data is retained but
its telemetry/performance conclusions are excluded. The harness now filters
per-run metric snapshots to the current instance IDs. `raw-paired-smoke/`
verifies successful startup and positive cache/hydration counters; it is not a
five-repetition result.
