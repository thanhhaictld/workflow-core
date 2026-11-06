#!/usr/bin/env python3
"""P0 snapshot: per-workflow commit performance, by Hub lock policy.

Call immediately after k6 completes and BEFORE restarting the Hub. The
Prometheus exporter can drop the old process's series on restart.
"""
import argparse
import json
import math
import time
import urllib.parse
import urllib.request


def query(expression, host):
    url = host.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expression})
    with urllib.request.urlopen(url, timeout=15) as response:
        data = json.load(response)
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {data}")
    series = data.get("data", {}).get("result", [])
    if not series:
        return None
    try:
        result = float(series[0]["value"][1])
        return round(result, 6) if math.isfinite(result) else None
    except (TypeError, ValueError, KeyError, IndexError):
        return None


def metrics(mode, prometheus):
    filters = {
        "nativeCommit": f'workflow_hub_native_commit_duration_seconds_bucket{{event_lane_mode="{mode}"}}',
        "workflowRowLock": f'workflow_hub_native_workflow_row_lock_duration_seconds_bucket{{event_lane_mode="{mode}"}}',
        "claim": 'workflow_hub_claim_duration_seconds_bucket{history_mode="batch"}',
    }
    # With the new fast path there should be NO native-commit advisory waits
    # for this event-free workload. Do not falsely label missing series as zero.
    if mode == "hub":
        filters["advisoryWait"] = (
            'workflow_hub_native_event_lane_lock_wait_seconds_bucket'
            '{event_lane_mode="hub",operation="native_commit"}'
        )
    result = {}
    for metric, selector in filters.items():
        for p in (95, 99):
            expression = (
                f"histogram_quantile({p / 100}, "
                f"sum by(le) (rate({selector}[5m])))"
            )
            value = query(expression, prometheus)
            if value is not None:
                result[f"{metric}P{p}Seconds"] = value
    if mode == "subscriptions":
        bypass = query(
            'sum(increase(workflow_hub_native_event_lane_bypass_total'
            '{event_lane_mode="subscriptions",operation="native_commit"}[5m]))',
            prometheus
        )
        if bypass is not None:
            result["lockBypassCount"] = bypass
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("hub", "subscriptions"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    args = parser.parse_args()
    report = {}
    for _ in range(8):
        report = metrics(args.mode, args.prometheus)
        required = ["nativeCommitP95Seconds", "nativeCommitP99Seconds",
                    "workflowRowLockP95Seconds", "workflowRowLockP99Seconds",
                    "claimP95Seconds", "claimP99Seconds"]
        if args.mode == "hub":
            required += ["advisoryWaitP95Seconds", "advisoryWaitP99Seconds"]
        else:
            required += ["lockBypassCount"]
        if all(key in report for key in required):
            break
        time.sleep(5)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps({"mode": args.mode, "p0": report}, indent=2))
    missing = [key for key in required if key not in report]
    if missing:
        raise SystemExit(f"Missing lock benchmark metrics: {missing}")


if __name__ == "__main__":
    main()
