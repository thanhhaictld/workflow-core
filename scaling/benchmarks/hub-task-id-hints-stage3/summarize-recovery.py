"""Summarize durable-claim recovery samples, preserving small-N ranges."""
import argparse
import json
import pathlib
import statistics
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("recovery", nargs="+", type=pathlib.Path)
args = parser.parse_args()
groups = {}
failed = []
for path in args.recovery:
    artifact = json.loads(path.read_text(encoding="utf-8"))
    for row in artifact["observations"]:
        if not row["Claimed"]:
            failed.append({"artifact": path.name, **row})
            continue
        key = (row["Arm"], row["RequireDemand"], row["Scenario"])
        demand_to_claim = (datetime.fromisoformat(row["ClaimedAtUtc"]) -
                           datetime.fromisoformat(row["DemandAtUtc"])).total_seconds() * 1000
        groups.setdefault(key, []).append({
            "repetition": row["Repetition"],
            "arrivalToClaimMs": row["ArrivalToClaimMs"],
            "demandToClaimMs": demand_to_claim,
            "eligibleToClaimMs": row["EligibleToClaimMs"],
            "pendingOutboxRows": row["PendingOutboxRows"],
        })

summary = []
for (arm, require_demand, scenario), samples in sorted(groups.items()):
    entry = {"arm": arm, "requireDemand": require_demand, "scenario": scenario,
             "sampleCount": len(samples), "samples": samples}
    for metric in ("arrivalToClaimMs", "demandToClaimMs", "eligibleToClaimMs"):
        values = [sample[metric] for sample in samples if sample[metric] is not None]
        entry[metric] = ({"min": min(values), "median": statistics.median(values),
                          "max": max(values), "n": len(values)} if values else None)
    summary.append(entry)

output = args.recovery[0].with_name("recovery-summary.json")
output.write_text(json.dumps({"cells": summary, "failedSamples": failed}, indent=2),
                  encoding="utf-8")
print(output)
