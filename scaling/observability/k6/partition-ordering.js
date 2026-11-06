import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE = (__ENV.HUB_URL || "http://hub:5100").replace(/\/$/, "");
const TOKEN = __ENV.HUB_TOKEN;
const MODE = __ENV.K6_POLICY_MODE || "unordered";
const DEFINITIONS = {
  unordered: "policy-unordered",
  partition: "policy-partition-fifo",
  keyed: "policy-keyed-fifo",
};
const DEFINITION = DEFINITIONS[MODE];
const TOTAL = Math.max(1, Number(__ENV.K6_POLICY_COUNT || 300));
const VUS = Math.max(1, Number(__ENV.K6_POLICY_VUS || 60));
const TIMEOUT = Math.max(15, Number(__ENV.K6_POLICY_TIMEOUT_SECONDS || 600));
const HUB = "policy-benchmark";
// Only the deliberate SIGKILL workload permits transient HTTP transport
// failures. Real workflow completion and activity-FIFO checks remain strict.
const FAULT_INJECTION = __ENV.K6_POLICY_FAULT_INJECTION === "true";
const MAX_START_RETRIES = FAULT_INJECTION ? 6 : 0;
const started = new Counter("hub_policy_started");
const failures = new Counter("hub_policy_failed");
const accepted = new Rate("hub_policy_accepted");
const submission = new Trend("hub_policy_submit_seconds", true);

export const options = {
  teardownTimeout: String(TIMEOUT + 30) + "s",
  summaryTrendStats: ["avg", "min", "med", "max", "p(95)", "p(99)"],
  scenarios: { policy: { executor: "shared-iterations", vus: VUS,
    iterations: TOTAL, maxDuration: "10m", gracefulStop: "10s" } },
  thresholds: {
    ...(FAULT_INJECTION ? {} : { http_req_failed: ["rate<0.01"] }),
    hub_policy_accepted: ["rate==1"], hub_policy_failed: ["count==0"]
  },
};
const headers = () => ({
  Authorization: "Bearer " + TOKEN, "Content-Type": "application/json",
});
function get(path, name) {
  return http.get(BASE + path, { headers: headers(), tags: { name }, timeout: "30s" });
}
export function setup() {
  if (!TOKEN || !DEFINITION) throw new Error("HUB_TOKEN and valid K6_POLICY_MODE required");
  const runId = __ENV.K6_POLICY_RUN_ID || "k6-policy-" + MODE + "-" + Date.now();
  const definitions = get("/api/hub/definitions", "definitions");
  if (definitions.status !== 200 ||
      !definitions.json().some(d => d.hubId === HUB && d.definitionId === DEFINITION && d.version === 1))
    throw new Error("Missing policy benchmark workflow registration: " + MODE);
  if (runId.length > 100) throw new Error("Run ID too long");
  return { runId, startedAt: Date.now() };
}
export default function (data) {
  const i = exec.scenario.iterationInTest;
  // 8 stable resource keys used by the keyed-FIFO mode. Workflow partitions
  // are distributed by workflow ID independently of these keys.
  const resourceId = "account-" + (i % 8);
  const body = {
    hubId: HUB, definitionId: DEFINITION, version: 1,
    runId: data.runId, idempotencyKey: data.runId + ":" + i,
    input: { resourceId },
  };
  const before = Date.now();
  const payload = JSON.stringify(body);
  let response;
  for (let attempt = 0; attempt <= MAX_START_RETRIES; attempt++) {
    // Keep the same idempotency key on every retry. The server may have
    // committed a workflow even when its gateway dies before the HTTP ACK.
    response = http.post(BASE + "/api/hub/workflows", payload,
      { headers: headers(), tags: { name: "policy_workflow_start" }, timeout: "40s" });
    if (response.status === 202) break;
    const recoverable = response.status === 0 || response.status === 408 ||
      response.status === 429 || response.status >= 500;
    if (!recoverable || attempt === MAX_START_RETRIES) break;
    sleep(Math.min(2, 0.2 * Math.pow(2, attempt)) * (0.8 + 0.4 * Math.random()));
  }
  submission.add((Date.now() - before) / 1000);
  const ok = check(response, { "policy start accepted": r => r.status === 202 });
  accepted.add(ok);
  if (ok) started.add(1);
  else {
    failures.add(1);
    console.error("Policy start failed (" + MODE + ") HTTP " + response.status +
      " body " + String(response.body).slice(0, 300));
  }
}
export function teardown(data) {
  const until = Date.now() + TIMEOUT * 1000;
  const path = "/api/hub/runs/" + encodeURIComponent(data.runId) + "/summary";
  let summary = null;
  while (Date.now() < until) {
    const response = get(path, "policy_summary");
    if (response.status === 200) {
      summary = response.json();
      if (summary.total === TOTAL && summary.completed === TOTAL && summary.failed === 0)
        break;
      if (summary.failed > 0) break;
    }
    sleep(1);
  }
  if (!summary || summary.total !== TOTAL || summary.completed !== TOTAL || summary.failed !== 0) {
    failures.add(1);
    throw new Error("Incomplete policy E2E " + MODE + ": " + JSON.stringify(summary));
  }
  const sampleResponse = get("/api/hub/workflows?hub=" + HUB +
    "&search=" + encodeURIComponent(data.runId) + "&take=1", "policy_sample");
  const id = sampleResponse.json("items.0.id");
  if (!id) throw new Error("No persisted sample workflow for " + MODE);
  const runtime = get("/api/hub/workflows/" + id + "/native-runtime", "policy_runtime");
  if (runtime.status !== 200 ||
      runtime.json("snapshot.Data.value.Completed") !== true)
    throw new Error("Workflow did not persist business result: " + id);
  const totalSeconds = (Date.now() - data.startedAt) / 1000;
  const serverSeconds = (Date.parse(summary.lastCompletedAt) -
    Date.parse(summary.firstCreatedAt)) / 1000;
  console.log(JSON.stringify({
    mode: MODE, runId: data.runId,
    count: summary.total, completed: summary.completed, failed: summary.failed,
    totalSeconds, serverSeconds,
    serverWorkflowsPerSecond: TOTAL / serverSeconds,
    workflowP95Ms: summary.p95CompletionMs,
    workflowP99Ms: summary.p99CompletionMs,
    verifiedSampleWorkflowId: id,
  }));
}
export function handleSummary(data) {
  return {
    "/results/k6-policy-summary.json": JSON.stringify(data, null, 2),
    stdout: "Policy E2E k6 finished; see /results/k6-policy-summary.json\n",
  };
}
