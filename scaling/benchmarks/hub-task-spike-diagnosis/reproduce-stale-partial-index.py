#!/usr/bin/env python3
"""Compare the selected-ID claim SQL on one isolated, matched PG17 fixture.

The fixture starts fully completed, is ANALYZEd with an empty claim partial
index, then receives 44k ready rows without ANALYZE.  Baseline and checkout SQL
run against that exact state, plus fresh-statistics and post-analyze churn arms.
Every EXPLAIN ANALYZE is inside a transaction that is rolled back.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import time
import uuid


HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
IMAGE = "postgres:17"
LABEL = "io.orca.task=hub-task-stale-statistics-fix-luna"

SEED_SQL = r"""
CREATE SCHEMA wfc_scale;
CREATE TABLE wfc_scale.hub_task(
 id uuid PRIMARY KEY, sequence bigint NOT NULL UNIQUE, hub_id text NOT NULL,
 partition_id integer NOT NULL, task_type text NOT NULL, task_version integer NOT NULL,
 workflow_id uuid NOT NULL, epoch bigint NOT NULL, worker text, owner text, last_owner text,
 attempts integer NOT NULL, max_attempts integer NOT NULL, state integer NOT NULL,
 due_at timestamptz NOT NULL, lease_until timestamptz, updated_at timestamptz NOT NULL,
 execution_pointer_id uuid, dispatch_key text, native_step_id integer, workflow_step integer,
 payload jsonb NOT NULL DEFAULT '{}'::jsonb);
CREATE TABLE wfc_scale.hub_task_constraint(
 task_id uuid NOT NULL, resource_key text NOT NULL, PRIMARY KEY(task_id,resource_key));
CREATE TABLE wfc_scale.hub_workflow(
 id uuid PRIMARY KEY, execution_mode text, native_status text, hub_id text,
 definition_id text, definition_version integer);
INSERT INTO wfc_scale.hub_task(
 id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,
 attempts,max_attempts,state,due_at,updated_at)
SELECT ('00000000-0000-0000-0000-'||lpad(g::text,12,'0'))::uuid,g,
 'policy-benchmark',0,
 'policy-unordered-step',1,
 ('10000000-0000-0000-0000-'||lpad(((g-1)/4+1)::text,12,'0'))::uuid,
 1,1,3,2,clock_timestamp()-interval '1 hour',clock_timestamp()
FROM generate_series(1,144000) g;
CREATE INDEX ix_hub_task_claim_sequence ON wfc_scale.hub_task(sequence)
 WHERE state IN (0,1) AND attempts<max_attempts;
-- This table and this setting belong only to this disposable container.
ALTER TABLE wfc_scale.hub_task SET (autovacuum_enabled=false);
ANALYZE wfc_scale.hub_task;
UPDATE wfc_scale.hub_task SET state=0 WHERE sequence>100000;
"""


def run(command: list[str], *, input_text: str | None = None, timeout: int = 120) -> str:
    result = subprocess.run(command, input=input_text, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def normalized_sql(text: str) -> str:
    for name, position in (("selected", 1), ("hub", 2), ("count", 3), ("partition", 4), ("lease", 5)):
        text = text.replace("@" + name, f"${position}")
    return text.strip().rstrip(";")


def extract_candidate_sql() -> str:
    source = (ROOT / "scaling/src/WorkflowCore.Hub/HubTaskStore.cs").read_text(encoding="utf-8")
    match = re.search(r'CandidateBatchAdmissionSql\s*=\s*"""\s*\n(.*?)\n\s*""";', source, re.S)
    if not match:
        raise RuntimeError("Could not extract CandidateBatchAdmissionSql from HubTaskStore.cs")
    return normalized_sql(match.group(1))


def selected_json(ids: list[int] | None, *, epoch: int = 1) -> str:
    return json.dumps([
        {
            "id": f"00000000-0000-0000-0000-{number:012d}",
            "epoch": epoch,
            "worker": "diagnostic-worker",
            "capabilities": [{"hub": "policy-benchmark", "task_type": "policy-unordered-step", "version": 1}],
            "pins": None,
        }
        for number in (ids or [])
    ], separators=(",", ":"))


def plan_once(container: str, query: str, values: str, plan_mode: str) -> dict:
    json_literal = values.replace("'", "''")
    sql = f"""BEGIN;
SET LOCAL plan_cache_mode={plan_mode};
PREPARE batchdiag(jsonb,text,integer,integer,interval) AS {query};
EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON)
 EXECUTE batchdiag('{json_literal}'::jsonb,'policy-benchmark',64,0,'30 seconds'::interval);
ROLLBACK;
"""
    output = run([
        "docker", "exec", "-i", container, "psql", "-U", "diag", "-d", "diag",
        "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
    ], input_text=sql)
    return json.loads(output)[0]


def plan_summary(result: dict) -> dict:
    root = result["Plan"]
    nodes: list[dict] = []

    def visit(node: dict) -> None:
        if node.get("Relation Name") == "hub_task" and node.get("Node Type") in {
            "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan",
        }:
            loops = int(node.get("Actual Loops", 0))
            actual_rows = float(node.get("Actual Rows", 0))
            removed = float(node.get("Rows Removed by Filter", 0))
            nodes.append({
                "node": node.get("Node Type"),
                "index": node.get("Index Name"),
                "loops": loops,
                "actual_rows_per_loop": actual_rows,
                "rows_removed_per_loop": removed,
                "observed_rows_across_loops": (actual_rows + removed) * loops,
                "shared_hit_blocks": node.get("Shared Hit Blocks", 0),
            })
        for child in node.get("Plans", []):
            visit(child)

    visit(root)
    return {
        "returned_rows": root.get("Actual Rows", 0),
        "execution_ms": result.get("Execution Time"),
        "root_shared_hit_blocks": root.get("Shared Hit Blocks", 0),
        "root_shared_read_blocks": root.get("Shared Read Blocks", 0),
        "hub_task_scan_work": sum(n["observed_rows_across_loops"] for n in nodes),
        "hub_task_scans": nodes,
    }


def index_estimate(container: str) -> tuple[float, int]:
    value = run([
        "docker", "exec", container, "psql", "-U", "diag", "-d", "diag", "-X", "-qAt",
        "-v", "ON_ERROR_STOP=1", "-c",
        "SELECT reltuples::bigint||'|'||relpages FROM pg_class "
        "WHERE oid='wfc_scale.ix_hub_task_claim_sequence'::regclass",
    ]).strip()
    rows, pages = value.split("|")
    return float(rows), int(pages)


def analyze(container: str) -> None:
    run(["docker", "exec", container, "psql", "-U", "diag", "-d", "diag", "-X", "-v", "ON_ERROR_STOP=1",
         "-c", "ANALYZE wfc_scale.hub_task"])


def update_churn(container: str) -> None:
    run(["docker", "exec", container, "psql", "-U", "diag", "-d", "diag", "-X", "-v", "ON_ERROR_STOP=1",
         "-c", "UPDATE wfc_scale.hub_task SET state=2 WHERE sequence BETWEEN 122001 AND 144000; "
               "UPDATE wfc_scale.hub_task SET state=0 WHERE sequence BETWEEN 50001 AND 72000; "
               "UPDATE wfc_scale.hub_task SET state=0 WHERE sequence BETWEEN 121969 AND 122000"])
    check = run(["docker", "exec", container, "psql", "-U", "diag", "-d", "diag", "-X", "-qAt",
                 "-v", "ON_ERROR_STOP=1", "-c",
                 "SELECT count(*) FROM wfc_scale.hub_task WHERE sequence BETWEEN 121969 AND 122000 "
                 "AND state=0 AND partition_id=0 AND attempts<max_attempts AND due_at<now()"])
    if check.strip() != "32":
        raise RuntimeError(f"churn fixture selected range is not fully eligible; rows={check.strip()}")


def run_arms(container: str, out: pathlib.Path, before_sql: str, current_sql: str,
             scenario: str, sizes: list[int], modes: list[str]) -> list[dict]:
    est, pages = index_estimate(container)
    results = []
    for variant, query in (("before", before_sql), ("current", current_sql)):
        for size in sizes:
            ids = list(range(144001 - size, 144001)) if not scenario.startswith("churn") else list(range(122001 - size, 122001))
            values = selected_json(ids)
            for mode in modes:
                raw = plan_once(container, query, values, mode)
                item = {
                    "scenario": scenario,
                    "index_estimate_rows": est,
                    "index_pages": pages,
                    "variant": variant,
                    "batch_size": size,
                    "plan_mode": mode,
                    **plan_summary(raw),
                }
                results.append({"summary": item, "plan": raw})
                print(f"{scenario:23} {variant:7} b={size:2} {mode:18} "
                      f"rows={item['returned_rows']:2} work={item['hub_task_scan_work']:.0f} "
                      f"hits={item['root_shared_hit_blocks']} exec={item['execution_ms']:.3f}ms")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=pathlib.Path,
                        help="Orca scratch directory for raw plan JSON and the summary")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    container = "pg-hubtask-fix-diag-" + time.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    before_sql = normalized_sql((HERE / "before-selected-id.sql").read_text(encoding="utf-8"))
    current_sql = extract_candidate_sql()
    try:
        image_id = run(["docker", "image", "inspect", "--format", "{{.Id}}", IMAGE]).strip()
        run(["docker", "run", "--rm", "-d", "--name", container, "--network", "none",
             "--cpus=2", "--memory=2g", "--memory-swap=2g", "--label", LABEL,
             "-e", "POSTGRES_USER=diag", "-e", "POSTGRES_PASSWORD=localdiagonly", "-e", "POSTGRES_DB=diag", IMAGE])
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "diag", "-d", "diag"],
                                   capture_output=True, text=True)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("isolated PostgreSQL 17 container did not become ready")
        run(["docker", "exec", "-i", container, "psql", "-U", "diag", "-d", "diag", "-X", "-v", "ON_ERROR_STOP=1"],
            input_text=SEED_SQL)
        pg_version = run(["docker", "exec", container, "psql", "-U", "diag", "-d", "diag", "-X", "-qAt", "-c", "SELECT version()"])
        all_results: list[dict] = []
        modes = ["force_custom_plan", "force_generic_plan"]

        # The same 44k active rows are compared before and after with stale
        # empty-index stats, then both SQL versions are re-run after ANALYZE.
        all_results += run_arms(container, args.output, before_sql, current_sql,
                                "stale-empty-estimate", [1, 8, 16, 32], modes)
        stale_estimates = {row["index_estimate_rows"] for row in (entry["summary"] for entry in all_results)}
        if stale_estimates != {0.0}:
            raise RuntimeError(f"stale fixture did not preserve a zero estimate: {stale_estimates}")
        analyze(container)
        all_results += run_arms(container, args.output, before_sql, current_sql,
                                "fresh-44k-estimate", [1, 8, 16, 32], modes)
        fresh_estimates = {row["index_estimate_rows"] for row in (entry["summary"] for entry in all_results
                           if entry["summary"]["scenario"] == "fresh-44k-estimate")}
        if not fresh_estimates or min(fresh_estimates) < 43_000:
            raise RuntimeError(f"fresh fixture estimate did not reflect its 44k active entries: {fresh_estimates}")

        # Keep the estimate at 44k but move half of the indexed rows; this tests
        # ordered traversal under post-ANALYZE queue churn, not just cardinality.
        update_churn(container)
        all_results += run_arms(container, args.output, before_sql, current_sql,
                                "churn-44k-estimate", [1, 8, 16, 32], modes)

        # Missing ID, wrong observed epoch, and empty selection must short-circuit
        # without walking the broad queue index, even with stale estimates.
        no_match: list[dict] = []
        for label, ids, epoch in (("missing-id", [999999], 1), ("stale-epoch", [144000], 999), ("empty-selection", [], 1)):
            for variant, query in (("before", before_sql), ("current", current_sql)):
                for mode in modes:
                    raw = plan_once(container, query, selected_json(ids, epoch=epoch), mode)
                    item = {"scenario": label, "variant": variant, "batch_size": len(ids), "plan_mode": mode,
                            "index_estimate_rows": index_estimate(container)[0], **plan_summary(raw)}
                    no_match.append({"summary": item, "plan": raw})

        summary_rows = [entry["summary"] for entry in all_results + no_match]
        # Fail closed if the old SQL stopped reproducing the defect, or if any
        # production-size arm still scans more than a bounded number of rows.
        stale_before_32 = [row for row in summary_rows if row["scenario"] == "stale-empty-estimate"
                           and row["variant"] == "before" and row["batch_size"] == 32]
        if not stale_before_32 or max(r["hub_task_scan_work"] for r in stale_before_32) < 1_000_000:
            raise RuntimeError("matched stale fixture did not reproduce the baseline broad scan")
        unbounded_current = [row for row in summary_rows if row["variant"] == "current"
                             and row["scenario"] in {"stale-empty-estimate", "fresh-44k-estimate", "churn-44k-estimate"}
                             and row["hub_task_scan_work"] > row["batch_size"] * 4]
        if unbounded_current:
            raise RuntimeError("current SQL performed non-bounded hub_task work: " + repr(unbounded_current[:2]))
        invalid_no_match = [entry["summary"] for entry in no_match
                            if entry["summary"]["returned_rows"] != 0 or
                            (entry["summary"]["variant"] == "current" and entry["summary"]["hub_task_scan_work"] > 4)]
        if invalid_no_match:
            raise RuntimeError("empty/missing/stale-ID case returned rows or scanned the queue: " + repr(invalid_no_match[:2]))

        payload = {
            "container": container,
            "image": IMAGE,
            "image_id": image_id,
            "postgres_version": pg_version.strip(),
            "container_limits": {"cpus": 2, "memory_bytes": 2147483648, "network": "none"},
            "fixture": {"total_rows": 144000, "stale_active_rows": 44000,
                        "stale_index_estimate_rows": 0, "autovacuum_disabled_on_fixture_table": True},
            "sql_sha256": {
                "baseline_fixture": hashlib.sha256((HERE / "before-selected-id.sql").read_bytes()).hexdigest(),
                "current_candidate_batch_admission": hashlib.sha256(current_sql.encode()).hexdigest(),
            },
            "summary": summary_rows,
            "raw_plans": all_results + no_match,
        }
        raw_path = args.output / "matched-before-after-plans.json"
        raw_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        csv_path = args.output / "matched-before-after-summary.csv"
        columns = ["scenario", "variant", "batch_size", "plan_mode", "index_estimate_rows",
                   "returned_rows", "hub_task_scan_work", "root_shared_hit_blocks", "root_shared_read_blocks", "execution_ms"]
        csv_path.write_text(",".join(columns) + "\n" + "\n".join(
            ",".join(str(row.get(column, "")) for column in columns) for row in summary_rows
        ) + "\n", encoding="utf-8")
        print(f"Wrote {raw_path}")
        print(f"Wrote {csv_path}")
        return 0
    finally:
        # The name and label are unique to this invocation; --rm deletes only
        # this isolated container and its anonymous data volume.
        subprocess.run(["docker", "stop", container], capture_output=True, text=True, timeout=30)


if __name__ == "__main__":
    raise SystemExit(main())
