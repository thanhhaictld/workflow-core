# Incomplete comparison attempt (excluded)

`raw-paired/` contains an interrupted 4-scenario run. It was stopped after
repetition 3 because the first telemetry collector scrape reused identical
service-instance labels across restarted Hub processes, yielding negative
counter deltas. The PostgreSQL/workload raw records are retained unchanged but
this directory is not used for performance conclusions. `raw-paired-smoke/`
verifies unique per-run service-instance labels and positive cache/hydration
counter deltas; it is only a harness smoke check, not a five-repetition result.
