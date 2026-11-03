import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

// One iteration creates and observes one real, three-step Hub workflow.
// Default: 1000 workflow instances, 100 concurrent k6 VUs.
const BASE = (__ENV.HUB_URL || "http://hub:5100").replace(/\/$/, "");
const TOKEN = __ENV.HUB_TOKEN;
const TOTAL = Math.max(1, Number(__ENV.K6_WORKFLOWS || 1000));
const VUS = Math.max(1, Number(__ENV.K6_VUS || 100));
const TIMEOUT = Math.max(15, Number(__ENV.K6_MAX_WAIT_SECONDS || 180));
const MODE = __ENV.K6_MODE || "e2e";
const DEFINITION = __ENV.K6_DEFINITION || "tenant-onboarding";

const starts = new Counter("hub_workflows_started");
const completed = new Counter("hub_workflows_completed");
const failed = new Counter("hub_workflows_failed");
const workflowDuration = new Trend("hub_workflow_duration_seconds", true);
const successRate = new Rate("hub_workflow_success");

export const options = {
  scenarios: {
    workflows: {
      executor: "shared-iterations",
      vus: VUS,
      iterations: TOTAL,
      maxDuration: String(Math.max(TIMEOUT * 3, 300)) + "s",
      gracefulStop: "10s",
    },
  },
  systemTags: ["method", "status", "name", "scenario", "expected_response"],
  thresholds: {
    http_req_failed: ["rate<0.01"],
    hub_workflow_success: ["rate>0.99"],
    hub_workflows_failed: ["count==0"],
  },
};

function headers() {
  return { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" };
}

export function setup() {
  if (!TOKEN) throw new Error("HUB_TOKEN must be configured");
  if (MODE !== "e2e" && MODE !== "submit")
    throw new Error("K6_MODE must be e2e or submit");

  const runId = (__ENV.K6_RUN_ID && __ENV.K6_RUN_ID.length > 0)
    ? __ENV.K6_RUN_ID : "k6-" + Date.now();
  const response = http.get(BASE + "/health/ready", {
    tags: { name: "hub_health" }, timeout: "10s",
  });
  if (response.status !== 200)
    throw new Error("Hub not ready: " + response.status);
  return { runId };
}

export default function (data) {
  const index = exec.scenario.iterationInTest;
  const tenant = data.runId + "-tenant-" + index;
  const body = JSON.stringify({
    hubId: "onboarding",
    definitionId: DEFINITION,
    version: 1,
    runId: data.runId,
    idempotencyKey: data.runId + ":" + index,
    input: { tenantId: tenant, organizationName: "Load tenant " + index },
  });

  const startedAt = Date.now();
  const response = http.post(BASE + "/api/hub/workflows", body, {
    headers: headers(),
    tags: { name: "hub_workflow_start", endpoint: "start" },
    timeout: "30s",
  });

  if (!check(response, { "workflow start accepted": r => r.status === 202 })) {
    failed.add(1); successRate.add(false); return;
  }
  starts.add(1);

  if (MODE === "submit") {
    successRate.add(true); // Submission-only mode counts accepted requests.
    return;
  }

  let workflowId;
  try { workflowId = response.json("workflowId"); } catch (_) {
    failed.add(1); successRate.add(false); return;
  }
  if (!workflowId) {
    failed.add(1); successRate.add(false); return;
  }

  const deadline = Date.now() + TIMEOUT * 1000;
  while (Date.now() < deadline) {
    sleep(0.5);
    const status = http.get(BASE + "/api/hub/workflows/" + workflowId, {
      headers: headers(),
      tags: { name: "hub_workflow_status", endpoint: "status" },
      timeout: "15s",
    });
    if (status.status !== 200) continue;
    let result;
    try { result = status.json(); } catch (_) { continue; }

    if (result.state === "Complete") {
      const outcome = check(result, {
        "all workflow steps completed": r =>
          DEFINITION === "tenant-onboarding-timer"
            ? r.nextStep === 4 && r.stepCount === 4 &&
              r.results["0"].validated === true &&
              r.results["1"].timerFired === true &&
              r.results["2"].provisioned === true &&
              r.results["3"].welcomeSent === true
            : r.nextStep === 3 && r.stepCount === 3 &&
              r.results["0"].validated === true &&
              r.results["1"].provisioned === true &&
              r.results["2"].welcomeSent === true,
      });
      if (outcome) {
        completed.add(1);
        workflowDuration.add((Date.now() - startedAt) / 1000);
        successRate.add(true);
      } else {
        failed.add(1); successRate.add(false);
      }
      return;
    }
    if (result.state === "Failed") {
      failed.add(1); successRate.add(false); return;
    }
  }
  failed.add(1); successRate.add(false);
}

export function teardown(data) {
  const summary = http.get(
    BASE + "/api/hub/runs/" + encodeURIComponent(data.runId) + "/summary",
    { headers: headers(), tags: { name: "hub_run_summary" }, timeout: "20s" },
  );
  if (summary.status !== 200) {
    console.error("Run summary failed: HTTP " + summary.status);
    return;
  }
  const state = summary.json();
  console.log(JSON.stringify({
    runId: data.runId, total: state.total, completed: state.completed,
    failed: state.failed, ready: state.ready, running: state.running,
    serverP95Ms: state.p95CompletionMs,
  }));
  check(state, {
    "expected workflows persisted": s => s.total === TOTAL,
    "no failed workflow instances": s => s.failed === 0,
    "all workflows reached completion": s =>
      MODE === "submit" || s.completed === TOTAL,
  });
}

export function handleSummary(data) {
  return {
    "/results/k6-summary.json": JSON.stringify(data, null, 2),
    stdout: "k6 Hub load test complete. Results: /results/k6-summary.json\n",
  };
}
