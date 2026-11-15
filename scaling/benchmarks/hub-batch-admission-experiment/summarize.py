from __future__ import annotations

import csv
import argparse
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).parent
parser = argparse.ArgumentParser()
parser.add_argument("--raw-root", type=Path, default=Path(__import__("os").environ.get("WFC_BATCH_RAW_DIR", ROOT / "raw")))
RAW_ROOT = parser.parse_args().raw_root.resolve()
CASES = {
    ("baseline", "analyzed"): RAW_ROOT / "paired-analyzed/baseline",
    ("prototype", "analyzed"): RAW_ROOT / "paired-analyzed/prototype",
    ("baseline", "stale-active"): RAW_ROOT / "paired-stale-active/baseline",
    ("prototype", "stale-active"): RAW_ROOT / "paired-stale-active/prototype",
}


def nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from nodes(child)


def summarize(directory):
    groups = {}
    for path in directory.glob("*.json"):
        item = json.loads(path.read_text(encoding="utf-8"))
        if item.get("warmup"):
            continue
        p = item["plan"]
        root = p["Plan"]
        scans = [n for n in nodes(root) if n.get("Relation Name") == "hub_task" and "Scan" in n.get("Node Type", "")]
        actual_rows = next((n.get("Actual Rows", 0) for n in nodes(root) if n.get("Operation") == "Update"), 0)
        row = {
            "execution_ms": p["Execution Time"],
            "planning_ms": p["Planning Time"],
            "client_ms": item["client_elapsed_ms"],
            "leased": actual_rows,
            "hits": root.get("Shared Hit Blocks", 0),
            "reads": root.get("Shared Read Blocks", 0),
            "wal_bytes": root.get("WAL Bytes", 0),
            "task_nodes": scans,
        }
        key = (item["cell"]["statsMode"], item["cell"]["planMode"], item["cell"]["batchSize"], item["cell"]["position"])
        groups.setdefault(key, []).append(row)
    return groups


all_groups = {(arm, *key): rows for (arm, _), folder in CASES.items() for key, rows in summarize(folder).items()}
fields = ("execution_ms", "planning_ms", "client_ms", "leased", "hits", "reads", "wal_bytes")
output = ROOT / "cell-summary.csv"
columns = ["stats", "plan", "batch", "position", "arm", "samples"]
for field in fields:
    columns += [field + "_median", field + "_min", field + "_max"]
columns += ["hits_per_lease_median", "task_scan_nodes"]
columns += ["read_samples"]
rows_out = []
for key in sorted(all_groups, key=lambda k: (k[1], k[2], k[3], k[4], k[0])):
    arm, stats, mode, batch, position = key
    values = all_groups[key]
    row = {"arm": arm, "stats": stats, "plan": mode, "batch": batch, "position": position, "samples": len(values)}
    for field in fields:
        vector = [v[field] for v in values]
        row[field + "_median"] = statistics.median(vector)
        row[field + "_min"] = min(vector)
        row[field + "_max"] = max(vector)
    row["hits_per_lease_median"] = statistics.median(v["hits"] / v["leased"] for v in values if v["leased"])
    row["task_scan_nodes"] = json.dumps(values[0]["task_nodes"], separators=(",", ":"))
    row["read_samples"] = sum(1 for v in values if v["reads"] > 0)
    rows_out.append(row)
with output.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows_out)

md = ["# Paired batch-admission result summary", "", "No-028 deployment-matching index set. All times are milliseconds. Each cell has 5 measured samples after 2 warmups. Ranges show min–max; SQL execution is from PostgreSQL EXPLAIN, while client elapsed includes Npgsql execution/roundtrip. `read_samples` reports measured samples with physical reads; reads and slower prototype cells are valid measurements, not verifier failures. WAL is retained, no CPU was measured.", "", "| Stats | Plan | Batch | IDs | Arm | SQL median (min–max) | Planning median | Leased | Buffer hits | Hits / lease | Read samples | Scan node(s) |", "|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|"]
for r in rows_out:
    nodes_text = r["task_scan_nodes"]
    scans = json.loads(nodes_text)
    scan = "; ".join(f"{n.get('Node Type')} est={n.get('Plan Rows')} actual={n.get('Actual Rows')} removed={n.get('Rows Removed by Filter', 0)} hit={n.get('Shared Hit Blocks', 0)} read={n.get('Shared Read Blocks', 0)} loops={n.get('Actual Loops', 0)}" for n in scans)
    md.append(f"| {r['stats']} | {r['plan']} | {r['batch']} | {r['position']} | {r['arm']} | {r['execution_ms_median']:.3f} ({r['execution_ms_min']:.3f}–{r['execution_ms_max']:.3f}) | {r['planning_ms_median']:.3f} | {r['leased_median']:.0f} | {r['hits_median']:.0f} | {r['hits_per_lease_median']:.1f} | {r['read_samples']} | {scan} |")
md += ["", "## Paired SQL execution deltas", "", "Percent change is prototype vs baseline on the same row set and statistics condition; negative is faster, positive is slower. Results are descriptive; a nonwin or a physical read is a measurement, not an integrity failure.", "", "| Stats | Plan | Batch | IDs | Baseline median ms | Prototype median ms | Change | Baseline hits/lease | Prototype hits/lease |", "|---|---|---:|---|---:|---:|---:|---:|---:|"]
for stats in ("stale-active", "analyzed"):
    for mode in ("npgsql-unprepared", "custom", "generic"):
        for batch in (1, 8, 16, 32):
            for position in ("early", "late"):
                b = next(r for r in rows_out if (r["arm"], r["stats"], r["plan"], r["batch"], r["position"]) == ("baseline", stats, mode, batch, position))
                p = next(r for r in rows_out if (r["arm"], r["stats"], r["plan"], r["batch"], r["position"]) == ("prototype", stats, mode, batch, position))
                delta = (p["execution_ms_median"] / b["execution_ms_median"] - 1) * 100 if b["execution_ms_median"] else float("nan")
                change = f"{delta:+.1f}%" if delta == delta else "n/a"
                md.append(f"| {stats} | {mode} | {batch} | {position} | {b['execution_ms_median']:.3f} | {p['execution_ms_median']:.3f} | {change} | {b['hits_per_lease_median']:.1f} | {p['hits_per_lease_median']:.1f} |")
nonwins = sum(1 for key in {(r["stats"], r["plan"], r["batch"], r["position"]) for r in rows_out if r["arm"] == "baseline"}
              if next(r for r in rows_out if (r["arm"], r["stats"], r["plan"], r["batch"], r["position"]) == ("prototype", *key))["execution_ms_median"] >=
                 next(r for r in rows_out if (r["arm"], r["stats"], r["plan"], r["batch"], r["position"]) == ("baseline", *key))["execution_ms_median"])
read_groups = sum(int(r["read_samples"]) for r in rows_out)
md.append(f"\nIntegrity/outcome summary: {len(rows_out)} arm cells; median nonwins/equalities={nonwins}; samples with physical reads={read_groups}. Neither outcome invalidates a cell.")
(ROOT / "cell-summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
print(f"wrote {output} and {ROOT / 'cell-summary.md'} with {len(rows_out)} arm cells; median nonwins={nonwins}; read-bearing arm-samples={read_groups}")
