import argparse
import csv
import json
import os
from pathlib import Path

root = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument("--raw-root", type=Path, default=Path(os.environ.get("WFC_BATCH_RAW_DIR", root / "raw")))
raw_root = parser.parse_args().raw_root.resolve()

with (root / "cell-summary.csv").open(encoding="utf-8", newline="") as f:
    summaries = list(csv.DictReader(f))
assert len(summaries) == 96, f"expected 96 arm cells, found {len(summaries)}"
assert all(int(r["samples"]) == 5 for r in summaries), "each arm cell must have five measured samples"
baseline = {(r["stats"], r["plan"], r["batch"], r["position"]): r for r in summaries if r["arm"] == "baseline"}
prototype = {(r["stats"], r["plan"], r["batch"], r["position"]): r for r in summaries if r["arm"] == "prototype"}
assert baseline.keys() == prototype.keys() and len(baseline) == 48, "paired cell coverage incomplete"
for key in baseline:
    batch = int(baseline[key]["batch"])
    assert int(float(baseline[key]["leased_median"])) == batch
    assert int(float(prototype[key]["leased_median"])) == batch

sample_plans = {}
read_samples = 0
nonwins = 0
for stats, dirname in (("analyzed", "paired-analyzed"), ("stale-active", "paired-stale-active")):
    for arm in ("baseline", "prototype"):
        folder = raw_root / dirname / arm
        plans = list(folder.glob("*.json"))
        assert len(plans) == 168, f"{folder}: expected 168 raw plans including warmups, found {len(plans)}"
        for path in plans:
            item = json.loads(path.read_text(encoding="utf-8"))
            assert item.get("arm") == arm and item["cell"]["statsMode"] == stats
            cell = item["cell"]
            sample_key = (stats, cell["planMode"], cell["batchSize"], cell["position"], item["sample"], arm)
            assert sample_key not in sample_plans, f"duplicate plan sample: {sample_key}"
            sample_plans[sample_key] = item
            if item["warmup"]:
                assert item["sample"] in (1, 2)
                continue
            assert item["sample"] in (3, 4, 5, 6, 7)
            batch = cell["batchSize"]
            plan = item["plan"]["Plan"]
            assert plan.get("Actual Rows", 0) == batch, f"EXPLAIN leased count mismatch in {path}"
            assert item.get("returned_rows") == batch, f"actual RETURNING count mismatch in {path}"
            ids = item.get("returned_ids", [])
            owners = item.get("returned_owners", {})
            assert len(ids) == batch and len(set(ids)) == batch, f"RETURNING IDs invalid in {path}"
            assert set(owners) == set(ids), f"RETURNING owner mapping incomplete in {path}"
            assert item.get("returned_hash"), f"missing exact-row hash in {path}"
            if plan.get("Shared Read Blocks", 0) > 0:
                read_samples += 1

for key in baseline:
    stats, mode, batch, position = key
    for sample in range(3, 8):
        b = sample_plans[(stats, mode, int(batch), position, sample, "baseline")]
        p = sample_plans[(stats, mode, int(batch), position, sample, "prototype")]
        assert b["returned_hash"] == p["returned_hash"], f"returned row values differ: {key}, sample={sample}"
        assert b["returned_ids"] == p["returned_ids"], f"returned IDs differ: {key}, sample={sample}"
        assert b["returned_owners"] == p["returned_owners"], f"returned owners differ: {key}, sample={sample}"
    if float(prototype[key]["execution_ms_median"]) >= float(baseline[key]["execution_ms_median"]):
        nonwins += 1

print(f"PASS integrity: 48 complete paired cells, 5 samples plus 2 warmups per arm/cell, exact returned rows match, requested batch counts returned; median nonwins={nonwins}; physical-read measured samples={read_samples} (reported, not rejected).")
