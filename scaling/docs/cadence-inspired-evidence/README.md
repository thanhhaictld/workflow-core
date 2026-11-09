# Ownership lab evidence

This is development evidence for the [validation report](../cadence-inspired-scaling-validation.md).
The runtime fix is `e194c25edd76328b7edb93409e194141e3ab553e`; final test-only
changes are `0f3e87d5`. Prerequisite integration is `dcdab7bd`; diagnostics are
`d6f041f5`. No Terra code is imported. `delivery-provenance.json` pins sources,
test/tool hashes and the read-only Terra inspection. The untracked user brief
is hashed for provenance and remains outside the commits and archives.

## Contents and integrity

`historical-evidence.zip` preserves the two source-checkout reports and historical
evidence with original relative paths, including decomposition-20261109.
`historical-manifest.json` gives each file's byte count and SHA-256. The source
checkout was read only. Historical image/source correspondence is not assumed.

`runtime-evidence.zip` preserves expanded raw records in this directory, including:

- All fault timelines, per-shard zero-credit observer frames, Hub/worker/LB logs,
  persisted workflow/task/attempt/history snapshots and failed/excluded runs.
- Exact-source build archive hashes, image IDs, resolved container environments,
  earlier worktree-image identities/diffs, builds, configured test output and TRX.
- Warmups, eight normal trials, telemetry-off trials, missed-notification trial,
  raw metrics, DB waits/statements and cgroup CPU snapshots.
- Clock samples, superseded analysis output, final analysis, final process logs
  and before/after owned-resource inventory and stop receipts.

`runtime-manifest.json` hashes every runtime archive member. `archive-integrity.json`
records archive/manifest hashes and counts. Expanded originals remain on the
implementation machine; Git stores the compressed records. Archive integrity
does not prove causal conclusions: the report distinguishes measurements from
inferences and explicitly excluded runs.

From repository root, verify every archive member without extracting:

```powershell
python scaling/observability/benchmark/package_owner_evidence.py --verify
```

For independent reanalysis, expand the runtime ZIP into a **fresh scratch directory**
and run:

```powershell
Expand-Archive scaling/docs/cadence-inspired-evidence/runtime-evidence.zip -DestinationPath artifacts/owner-evidence-review
python scaling/observability/benchmark/analyze_owner_recovery.py --evidence-dir artifacts/owner-evidence-review
```

The analyzer rejects duplicate logical completions/results, missing completed
business snapshots, keyed FIFO inversions, capacity overlaps, failed modified
takeover and incomplete expected cohorts. Baseline fault runs are expected to
remain unfinished in their observation windows. Raw results are not changed;
the generated `analysis.json` in the scratch directory can be compared with the
committed copy. Do not regenerate either retained evidence archive to append a
new trial; use a separate evidence package.

## Reproduction commands

Requirements: Docker, Python standard library, PowerShell 7, .NET SDK 10. Run
from repository root. Commands below are examples for a **new** isolated lab,
with new image tags and unique run labels; they do not resume/reset the retained
lab. Pick unused loopback ports (base + 0/1/10/89/240). Build logs refuse overwrite.
The build helper archives the named commit; it never changes a checkout.

```powershell
$evidence = 'artifacts/owner-repro-evidence'
$lab = @('--project', 'cadence-owner-repro', '--port-base', '26200', '--evidence-dir', $evidence)
python scaling/observability/benchmark/owner_recovery_build.py --commit dcdab7bd --tag repro-baseline --evidence-dir $evidence
python scaling/observability/benchmark/owner_recovery_build.py --commit e194c25e --tag repro-modified --evidence-dir $evidence
python scaling/observability/benchmark/owner_recovery_lab.py setup @lab --tag repro-baseline
python scaling/observability/benchmark/owner_recovery_lab.py experiment @lab --label repro-baseline-idle-a --victim hub --seconds 85
python scaling/observability/benchmark/owner_recovery_lab.py upgrade @lab --tag repro-modified
python scaling/observability/benchmark/owner_recovery_lab.py experiment @lab --label repro-modified-busy-b --victim hub-2 --busy --seconds 85
```

Repeat both victims, idle and `--busy`, twice per case. Use `--fault pause` for
old-owner resume and `--fault database` for database outage; these pause for
16 seconds before resuming. Hard-kill victims stay stopped until the window
ends. The harness records a live-victim-ownership precondition before injection.
The original valid modified hard kills are idle-A `3/4`, idle-B `1/2`, busy-A
`1/2`, busy-B `1/2`; earlier idle-A `1/2` remain explicitly excluded. Separate
observer sessions issue zero credits and cannot perform business work.

Normal-path collection disables owner diagnostic logs; flags affect newly
created processes, so apply them **before** setup/upgrade:

```powershell
$env:OWNER_LAB_DIAGNOSTICS = 'false'
python scaling/observability/benchmark/owner_recovery_lab.py upgrade @lab --tag repro-modified
python scaling/observability/benchmark/owner_recovery_lab.py benchmark @lab --label repro-warmup-keyed --count 40 --mode keyed
python scaling/observability/benchmark/owner_recovery_lab.py benchmark @lab --label repro-measure-keyed --count 200 --mode keyed
```

Run unordered and eight-key modes in ABBA image order, reverse workload order
between stages, and warm up before each measured cohort. The retained matrix
uses `bench-{baseline,modified}-{a,b}-{unordered,keyed}` with corresponding
`warmup2-*` cohorts. `OWNER_LAB_METRICS=off` disables OTEL SDK setup for the
overhead comparison; clear it before normal collection. `OWNER_LAB_NOTIFY=off`
disables both Hub listeners for missed-hint recovery; clear it afterward.
Use `Remove-Item Env:OWNER_LAB_METRICS` / `Env:OWNER_LAB_NOTIFY` to clear these
task-local settings. Avoid interpreting collector cache as live off-run metrics.

For configured PostgreSQL tests, create a new database only on the new lab's
PostgreSQL container, apply ordered `scaling/sql/*.sql` with `ON_ERROR_STOP=1`,
and set `WFC_TEST_PG`. Never use an existing shared database as disposable.

```powershell
docker exec cadence-owner-repro-pg createdb -U workflow owner_repro_tests
Get-ChildItem scaling/sql/*.sql | Sort-Object Name | ForEach-Object {
    Get-Content -Raw $_.FullName | docker exec -i cadence-owner-repro-pg psql -X -U workflow -d owner_repro_tests -v ON_ERROR_STOP=1
    if ($LASTEXITCODE) { throw "Migration failed: $($_.Name)" }
}
$env:WFC_TEST_PG = 'Host=127.0.0.1;Port=26440;Database=owner_repro_tests;Username=workflow;Password=owner_lab_only;Maximum Pool Size=40'
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build
```

Windows full baseline had four immediate-claim failures; they are retained rather
than called passing. The final 146/146 pass uses a Linux SDK 10 image, configured
separate `owner_linux_final` database, exact runtime source `e194c25e` and the
read-only final routing test from `0f3e87d5`, rebuilt before testing. Its image,
environment, mounts and exact test command are in `lab-before-stop.json`;
`linux-final-tests.txt` and `test-results/linux-final.trx` prove zero skips.
`OwnerTests.Dockerfile` is the reusable Linux build/test recipe. Supply an
archive context of the final source and that Dockerfile; do not include local
evidence/artifacts as a Docker context. The final TRX records the tested cases.

## Retained resource state

All nine running containers labeled `cadence.owner-lab=cadence-owner-20261109`
were stopped after collection. Two Linux test containers had already exited 0.
Containers, network, images and the `cadence-owner-20261109-pg` volume remain;
the four lab databases were not dropped. No other stack was stopped or reset.
`lab-before-stop.json`, `lab-after-stop.json`, `stopped-container-names.txt` and
`lab-stop-output.txt` identify exact targets. The stop list was checked against
both the owned label and the expected names. Future cleanup must inspect those
targets again; the reproduction harness deliberately has no broad cleanup action.

No deployment or publishing occurred. Terra's committed patch and combined
validation remain an external integration gate; sustained capacity/overhead and
queue/deployment changes remain conditional, not claimed capabilities.
