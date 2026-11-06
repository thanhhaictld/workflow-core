#!/usr/bin/env python3
"""Capture P0 histogram p95/p99 before an A/B Hub restart loses old series.

No extra dependencies. Requires a running Prometheus on 127.0.0.1:9090.
Metrics are low-cardinality and intentionally omit workflow/tenant identifiers.
"""
import argparse
import json
import math
import time
import urllib.parse
import urllib.request

METRICS = {
    "eventLaneLock": 'workflow_hub_native_event_lane_lock_wait_seconds_bucket{operation="native_commit",history_mode="%s"}',
    "historyWrite": 'workflow_hub_native_history_write_duration_seconds_bucket{history_mode="%s"}',
    "commitDuration": 'workflow_hub_native_commit_duration_seconds_bucket{history_mode="%s"}',
    "claimLatency": "workflow_hub_claim_duration_seconds_bucket",
}

def query(prometheus: str, expression: str) -> float | None:
    url = prometheus.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expression})
    with urllib.request.urlopen(url, timeout=20) as response:
        result = json.load(response)
    if result.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {result.get('error')}")
    values = result.get("data", {}).get("result", [])
    if not values:
        return None
    try:
        value = float(values[0]["value"][1])
        return round(value, 6) if math.isfinite(value) else None
    except (ValueError, TypeError, IndexError, KeyError):
        return None

def capture(mode: str, prometheus: str) -> dict[str, float]:
    report: dict[str, float] = {}
    for key, template in METRICS.items():
        series = template % mode if "%s" in template else template
        for percentile in (95, 99):
            expression = (
                f"histogram_quantile({percentile / 100}, "
                f"sum by (le) (rate({series}[5m])))"
            )
            result = query(prometheus, expression)
            if result is not None:
                report[f"{key}P{percentile}Seconds"] = result
    return report

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("legacy", "batch"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    args = parser.parse_args()
    report = {}
    # Allow one additional collector/export/scrape interval after k6 exits.
    for _ in range(6):
        report = capture(args.mode, args.prometheus)
        if all(f"{name}P95Seconds" in report for name in METRICS):
            break
        time.sleep(5)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps({"mode": args.mode, "p0": report}, indent=2))
    for name in METRICS:
        if f"{name}P95Seconds" not in report or f"{name}P99Seconds" not in report:
            raise SystemExit(f"P0 histogram not available: {name} ({args.mode}); check OTLP/Prometheus export")

if __name__ == "__main__":
    main()
