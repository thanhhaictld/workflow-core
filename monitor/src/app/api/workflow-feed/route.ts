import { NextRequest, NextResponse } from "next/server";
import type { WorkflowItem, WorkflowOverview, WorkflowPage } from "@/lib/types";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

// The list is a merged, correctly paginated view across independent
// persistence backends. Source API credentials never leave this route.
type HubItem = {
  id: string; hubId: string; definitionId: string; version: number;
  runId: string | null; state: string; nextStep: number; stepCount: number;
  error: string | null; createdAt: string; completedAt: string | null;
};
type HubPage = { items: HubItem[]; total: number; page: number; take: number };
type HubOverview = {
  total: number; ready: number; running: number; completed: number; failed: number;
  created24h: number; completed24h: number;
};
type FeedPage = WorkflowPage & { overview: WorkflowOverview; hubs: string[]; warnings: string[] };

const PAGE_SIZE_LIMIT = 100;
const MAX_PAGE = 100;
const PAGE_CHUNK = 100;

function validHub(input: string) {
  return input === "all" || input === "legacy" || input === "hub" ||
    /^[a-zA-Z0-9_.:-]{1,80}$/.test(input);
}

async function fetchJson<T>(url: string, token?: string): Promise<T> {
  const result = await fetch(url, {
    cache: "no-store",
    headers: { Accept: "application/json", ...(token ? { Authorization: "Bearer " + token } : {}) },
    signal: AbortSignal.timeout(12_000),
  });
  if (!result.ok) throw new Error("HTTP " + result.status);
  return result.json() as Promise<T>;
}

function newOverview(): WorkflowOverview {
  return { total: 0, running: 0, completed: 0, suspended: 0, terminated: 0,
    withErrors: 0, created24h: 0, completed24h: 0, asOf: new Date().toISOString() };
}

function compareRows(a: WorkflowItem, b: WorkflowItem) {
  const date = Date.parse(b.createdAt) - Date.parse(a.createdAt);
  return (Number.isFinite(date) ? date : 0) ||
    (a.source ?? "legacy").localeCompare(b.source ?? "legacy") ||
    a.id.localeCompare(b.id);
}

function hubStatus(status: string): string | null | "skip" {
  switch (status) {
    case "All": return null;
    case "Runnable": return "Runnable"; // Ready OR Running in Hub store.
    case "Ready": case "Running": case "Complete": case "Failed": return status;
    default: return "skip";
  }
}

function legacyStatus(status: string): string | "skip" {
  if (status === "All" || status === "Runnable" || status === "Complete" ||
      status === "Suspended" || status === "Terminated") return status;
  if (status === "Running") return "Runnable";
  return "skip";
}

function toItem(item: HubItem): WorkflowItem {
  return {
    id: item.id,
    definitionId: item.definitionId,
    version: item.version,
    reference: item.runId,
    description: null,
    status: item.state,
    createdAt: item.createdAt,
    completedAt: item.completedAt,
    errorCount: item.state === "Failed" ? 1 : 0,
    source: "hub",
    hubId: item.hubId,
  };
}

export async function GET(request: NextRequest) {
  const query = request.nextUrl.searchParams;
  const hub = query.get("hub") ?? "all";
  const status = query.get("status") ?? "All";
  const search = (query.get("search") ?? "").trim();
  const page = Number(query.get("page") ?? 1);
  const take = Number(query.get("take") ?? 20);

  if (!validHub(hub) || !Number.isInteger(page) || page < 1 || page > MAX_PAGE ||
      !Number.isInteger(take) || take < 1 || take > PAGE_SIZE_LIMIT ||
      search.length > 200 || ![
        "All", "Runnable", "Ready", "Running", "Complete", "Suspended", "Failed", "Terminated",
      ].includes(status)) {
    return NextResponse.json({ error: "Invalid workflow list filters" }, { status: 400 });
  }

  const includeLegacy = hub === "all" || hub === "legacy";
  const includeHub = hub !== "legacy";
  const hubId = hub === "hub" || hub === "all" ? null : hub;
  const hubBase = process.env.WORKFLOW_HUB_URL?.replace(/\/$/, "");
  const hubToken = process.env.WORKFLOW_HUB_TOKEN;
  const legacyBase = process.env.WORKFLOW_API_URL?.replace(/\/$/, "") ?? "http://localhost:5000";
  const warnings: string[] = [];
  const rows: WorkflowItem[] = [];
  const overview = newOverview();
  const wanted = page * take;
  let total = 0;
  let succeeded = 0;
  let hubs: string[] = [];

  // The two sources can fail independently: show partial data explicitly,
  // rather than hiding valid Hub runs when the legacy monitor is down.
  async function readLegacy() {
    if (!includeLegacy) return;
    const selected = legacyStatus(status);
    try {
      const stats = await fetchJson<WorkflowOverview>(legacyBase + "/api/monitor/overview");
      overview.total += stats.total;
      overview.running += stats.running;
      overview.completed += stats.completed;
      overview.suspended += stats.suspended;
      overview.terminated += stats.terminated;
      overview.withErrors += stats.withErrors;
      overview.created24h += stats.created24h;
      overview.completed24h += stats.completed24h;

      if (selected !== "skip") {
        const params = new URLSearchParams({
          page: "1", take: "1", status: selected, search,
        });
        const initial = await fetchJson<WorkflowPage>(
          legacyBase + "/api/monitor/workflows?" + params,
        );
        total += initial.total;
        const pages = Math.ceil(Math.min(wanted, initial.total) / PAGE_CHUNK);
        for (let p = 1; p <= pages; p++) {
          params.set("page", String(p));
          params.set("take", String(PAGE_CHUNK));
          const result = await fetchJson<WorkflowPage>(
            legacyBase + "/api/monitor/workflows?" + params,
          );
          rows.push(...result.items.map(row => ({ ...row, source: "legacy" as const, hubId: null })));
        }
      }
      succeeded++;
    } catch {
      warnings.push("Legacy Workflow Core monitor is unavailable; legacy results are omitted.");
    }
  }

  async function readHub() {
    if (!hubBase || !hubToken) {
      if (includeHub) warnings.push("Workflow Hub is not configured; Hub executions are omitted.");
      return;
    }
    try {
      const [available, stats] = await Promise.all([
        fetchJson<string[]>(hubBase + "/api/hub/hubs", hubToken),
        includeHub
          ? fetchJson<HubOverview>(
              hubBase + "/api/hub/workflow-overview" +
                (hubId ? "?hub=" + encodeURIComponent(hubId) : ""), hubToken,
            )
          : Promise.resolve(null),
      ]);
      hubs = available;
      if (!includeHub) return;
      if (stats) {
        overview.total += stats.total;
        overview.running += stats.running + stats.ready;
        overview.completed += stats.completed;
        overview.withErrors += stats.failed;
        overview.created24h += stats.created24h;
        overview.completed24h += stats.completed24h;
      }

      const selected = hubStatus(status);
      if (selected !== "skip") {
        const params = new URLSearchParams({
          page: "1", take: "1", search,
          ...(hubId ? { hub: hubId } : {}),
          ...(selected ? { status: selected } : {}),
        });
        const initial = await fetchJson<HubPage>(
          hubBase + "/api/hub/workflows?" + params, hubToken,
        );
        total += initial.total;
        const pages = Math.ceil(Math.min(wanted, initial.total) / PAGE_CHUNK);
        for (let p = 1; p <= pages; p++) {
          params.set("page", String(p));
          params.set("take", String(PAGE_CHUNK));
          const result = await fetchJson<HubPage>(
            hubBase + "/api/hub/workflows?" + params, hubToken,
          );
          rows.push(...result.items.map(toItem));
        }
      }
      succeeded++;
    } catch {
      if (includeHub) warnings.push("Workflow Hub is unavailable; Hub executions are omitted.");
    }
  }

  await Promise.all([readLegacy(), readHub()]);
  if (succeeded === 0) {
    return NextResponse.json(
      { error: warnings.join(" ") || "No workflow sources are available" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }

  const result: FeedPage = {
    items: rows.sort(compareRows).slice((page - 1) * take, wanted),
    total, page, take, overview, hubs, warnings,
  };
  return NextResponse.json(result, { headers: { "Cache-Control": "no-store" } });
}
