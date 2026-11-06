import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

// A submission-first stress benchmark: each iteration only starts a workflow.
// A single teardown poller waits for all completions. This avoids 1000 VUs
// continuously polling individual workflows, which would load the Hub and
// PostgreSQL and distort the workflow processing throughput.
const BASE = (__ENV.HUB_URL || "http://hub:5100").replace(/\/$/, "");
const TOKEN = __ENV.HUB_TOKEN;
const TOTAL = Math.max(1, Number(__ENV.K6_OFFBOARD_COUNT || 1000));
const VUS = Math.max(1, Number(__ENV.K6_OFFBOARD_VUS || 100));
const TIMEOUT = Math.max(15, Number(__ENV.K6_OFFBOARD_TIMEOUT_SECONDS || 360));
const RUN_ID = __ENV.K6_OFFBOARD_RUN_ID || "";
const HUB = "offboarding";
const DEFINITION = "tenant-offboarding-fast";
const VERSION = 1;

const started = new Counter("hub_offboarding_started");
const failed = new Counter("hub_offboarding_failed");
const accepted = new Rate("hub_offboarding_start_success");
const submitLatency = new Trend("hub_offboarding_submit_latency_seconds", true);
const overallThroughput = new Trend("hub_offboarding_overall_throughput_workflows_per_second", true);

export const options = {
  // k6 defaults teardown() to 60 seconds regardless of TIMEOUT. The
  // offboarding benchmark intentionally waits for all queued workflows.
  teardownTimeout: String(TIMEOUT + 30) + "s",
  summaryTrendStats: ["avg", "min", "med", "max", "p(95)", "p(99)"],
  scenarios: {
    offboarding: {
      executor: "shared-iterations",
      vus: VUS,
      iterations: TOTAL,
      maxDuration: __ENV.K6_OFFBOARD_MAX_DURATION || "10m",
      gracefulStop: "10s",
    },
  },
  systemTags: ["method", "status", "name", "scenario", "expected_response"],
  thresholds: {
    http_req_failed: ["rate<0.01"],
    hub_offboarding_start_success: ["rate==1"],
    hub_offboarding_failed: ["count==0"],
  },
};

const headers = () => ({
  Authorization: "Bearer " + TOKEN,
  "Content-Type": "application/json",
});

function get(path, name) {
  return http.get(BASE + path, {
    headers: headers(),
    tags: { name },
    timeout: "20s",
  });
}

export function setup() {
  if (!TOKEN) throw new Error("HUB_TOKEN is required");
  const id = RUN_ID || "k6-offboard-" + Date.now();
  if (id.length > 100) throw new Error("K6_OFFBOARD_RUN_ID exceeds 100 characters");

  const ready = http.get(BASE + "/health/ready", {
    tags: { name: "hub_health" }, timeout: "10s",
  });
  if (ready.status !== 200) throw new Error("Hub not healthy: " + ready.status);

  const definitions = get("/api/hub/definitions", "hub_definitions");
  if (definitions.status !== 200 ||
      !definitions.json().some(d => d.hubId === HUB &&
        d.definitionId === DEFINITION && d.version === VERSION)) {
    throw new Error("Offboarding workflow is not registered. Start eight processor containers first.");
  }
  return { runId: id, startedAt: Date.now() };
}

export default function (data) {
  const index = exec.scenario.iterationInTest;
  const tenantId = data.runId + "-tenant-" + index;
  const input = JSON.stringify({
    hubId: HUB,
    definitionId: DEFINITION,
    version: VERSION,
    runId: data.runId,
    idempotencyKey: data.runId + ":" + index,
    input: { tenantId },
  });

  const before = Date.now();
  const response = http.post(BASE + "/api/hub/workflows", input, {
    headers: headers(),
    tags: { name: "offboarding_start" },
    timeout: "40s",
  });
  submitLatency.add((Date.now() - before) / 1000);

  const ok = check(response, { "offboarding accepted (202)": r => r.status === 202 });
  accepted.add(ok);
  if (ok) started.add(1);
  else {
    failed.add(1);
    console.error("Start failed: iteration=" + index +
      " status=" + response.status + " body=" + response.body.slice(0, 400));
  }
}

export function teardown(data) {
  const deadline = Date.now() + TIMEOUT * 1000;
  const path = "/api/hub/runs/" + encodeURIComponent(data.runId) + "/summary";
  let summary = null;
  let lastError = "";

  while (Date.now() < deadline) {
    const response = get(path, "offboarding_run_summary");
    if (response.status === 200) {
      try {
        summary = response.json();
        if (summary.total === TOTAL && summary.completed === TOTAL && summary.failed === 0)
          break;
        if (summary.failed > 0) break;
      } catch (e) { lastError = String(e); }
    } else {
      lastError = "run summary HTTP " + response.status;
    }
    sleep(1);
  }

  if (!summary || summary.total !== TOTAL || summary.completed !== TOTAL ||
      summary.failed !== 0) {
    failed.add(1);
    throw new Error("Offboarding incomplete after " + TIMEOUT + "s: " +
      JSON.stringify(summary) + " " + lastError);
  }

  // Verify actual native state (not merely the workflow completion counter).
  const list = get("/api/hub/workflows?hub=" + HUB +
      "&search=" + encodeURIComponent(data.runId) + "&take=1",
    "offboarding_sample_list");
  if (list.status !== 200 || !list.json("items.0.id")) {
    failed.add(1);
    throw new Error("Cannot find a completed sample for run " + data.runId);
  }
  const sampleId = list.json("items.0.id");
  const sample = get("/api/hub/workflows/" + sampleId + "/native-runtime",
    "offboarding_sample_state");
  if (sample.status !== 200) {
    failed.add(1);
    throw new Error("Cannot read native state for " + sampleId);
  }
  const snapshot = sample.json("snapshot.Data.value");
  if (!snapshot || snapshot.Validated !== true ||
      snapshot.AccessRevoked !== true || snapshot.ResourcesRemoved !== true) {
    failed.add(1);
    throw new Error("Invalid offboarding state: " + JSON.stringify(snapshot));
  }

  const elapsedSeconds = (Date.now() - data.startedAt) / 1000;
  const throughput = TOTAL / Math.max(0.001, elapsedSeconds);
  overallThroughput.add(throughput);
  const serverSeconds = summary.firstCreatedAt && summary.lastCompletedAt
    ? (Date.parse(summary.lastCompletedAt) - Date.parse(summary.firstCreatedAt)) / 1000
    : null;
  const serverRate = serverSeconds > 0 ? TOTAL / serverSeconds : null;
  console.log(JSON.stringify({
    workload: DEFINITION,
    constraints: "No activity registrations or FIFO/resource policies; hub-native workflow tasks still use internal fencing",
    runId: data.runId,
    submitted: summary.total,
    completed: summary.completed,
    failed: summary.failed,
    overallElapsedSeconds: Math.round(elapsedSeconds * 100) / 100,
    overallWorkflowsPerSecond: Math.round(throughput * 100) / 100,
    serverWindowWorkflowsPerSecond: serverRate === null ? null : Math.round(serverRate * 100) / 100,
    serverP95CompletionMs: summary.p95CompletionMs,
    serverP99CompletionMs: summary.p99CompletionMs,
    verifiedSampleWorkflowId: sampleId,
  }));
}

export function handleSummary(result) {
  return {
    "/results/k6-offboarding-summary.json": JSON.stringify(result, null, 2),
    stdout: "k6 unconstrained offboarding completed. Summary: /results/k6-offboarding-summary.json\n",
  };
}
