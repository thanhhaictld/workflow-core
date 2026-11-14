"""Submit and observe real onboarding workflows with a fixed number of VUs.

The caller starts the Hub and worker host separately and sets WFC_HUB_URL,
WFC_HUB_AUTH_TOKEN, WFC_BENCH_RUN_ID and WFC_BENCH_OUTPUT. Every VU handles
at most one active workflow; the JSON artifact includes every iteration.
"""
import concurrent.futures
import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request

url = os.environ["WFC_HUB_URL"].rstrip("/")
token = os.environ["WFC_HUB_AUTH_TOKEN"]
run_id = os.environ["WFC_BENCH_RUN_ID"]
output = os.environ["WFC_BENCH_OUTPUT"]
vus = int(os.getenv("WFC_BENCH_VUS", "100"))
count = int(os.getenv("WFC_BENCH_WORKFLOWS", "100"))
timeout = int(os.getenv("WFC_BENCH_TIMEOUT", "300"))
container = os.getenv("WFC_BENCH_POSTGRES_CONTAINER", "workflow-core-monitor-postgres-1")
headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
samples = []
stop = threading.Event()


def request(path, data=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url + path, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


def sample():
    while not stop.is_set():
        entry = {"time": time.time()}
        try:
            entry["transport"] = request("/api/hub/workers/transport-diagnostics")
            result = subprocess.run(["docker", "stats", "--no-stream", "--format",
                                     "{{.CPUPerc}}", container], capture_output=True,
                                    text=True, timeout=10)
            entry["postgres_cpu_percent"] = float(result.stdout.strip().rstrip("%"))
            entry["docker_exit"] = result.returncode
        except Exception as exc:
            entry["error"] = repr(exc)
        samples.append(entry)
        stop.wait(2)


def iteration(index):
    tenant = f"{run_id}-tenant-{index}"
    start = time.monotonic()
    item = {"index": index, "start_unix": time.time(), "poll_errors": 0}
    try:
        response = request("/api/hub/workflows", {
            "hubId": "onboarding", "definitionId": "tenant-onboarding", "version": 3,
            "runId": run_id, "idempotencyKey": f"{run_id}:{index}",
            "input": {"tenantId": tenant, "organizationName": f"Load tenant {index}"}})
        item["workflow_id"] = response["workflowId"]
        while time.monotonic() - start < timeout:
            time.sleep(.5)
            try:
                status = request("/api/hub/workflows/" + item["workflow_id"])
            except Exception:
                item["poll_errors"] += 1
                continue
            if status["state"] in ("Complete", "Failed"):
                item["state"] = status["state"]
                break
        else:
            item["state"] = "Timeout"
    except Exception as exc:
        item["state"] = "Error"
        item["error"] = repr(exc)
    item["elapsed_ms"] = (time.monotonic() - start) * 1000
    return item


sampler = threading.Thread(target=sample, daemon=True)
sampler.start()
started = time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=vus) as pool:
    items = list(pool.map(iteration, range(count)))
duration = time.monotonic() - started
stop.set()
sampler.join(timeout=15)
result = {"run_id": run_id, "vus": vus, "requested": count, "duration_seconds": duration,
          "items": items, "samples": samples}
os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
with open(output, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)
completed = sum(i["state"] == "Complete" for i in items)
print(json.dumps({"run_id": run_id, "completed": completed, "requested": count,
                  "duration_seconds": duration, "output": output}))
if completed != count:
    raise SystemExit(1)
