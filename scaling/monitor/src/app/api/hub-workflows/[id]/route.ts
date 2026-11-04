import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id))
    return NextResponse.json({ error: "Invalid workflow ID" }, { status: 400 });
  const host = process.env.WORKFLOW_HUB_URL?.replace(/\/$/, "");
  const token = process.env.WORKFLOW_HUB_TOKEN;
  if (!host || !token)
    return NextResponse.json({ error: "Workflow Hub is not configured" }, { status: 503 });

  async function get(path: string) {
    const response = await fetch(host + "/api/hub/" + path, {
      cache: "no-store",
      headers: { Accept: "application/json", Authorization: "Bearer " + token },
      signal: AbortSignal.timeout(12_000),
    });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error("Hub returned HTTP " + response.status);
    return response.json();
  }

  try {
    const workflow = await get("workflows/" + id);
    if (!workflow)
      return NextResponse.json({ error: "Workflow not found" }, { status: 404 });
    const [history, trace, definitions, nativeRuntime] = await Promise.all([
      get("workflows/" + id + "/history"),
      get("workflows/" + id + "/trace"),
      get("definitions"),
      workflow.executionMode === "native" ? get("workflows/" + id + "/native-runtime") : Promise.resolve(null),
    ]);
    const definition = Array.isArray(definitions) ? definitions.find(
      value => value.hubId === workflow.hubId &&
        value.definitionId === workflow.definitionId &&
        value.version === workflow.version,
    ) : null;
    return NextResponse.json(
      { workflow, history: history ?? [], trace: trace ?? [], definition, nativeRuntime },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch {
    return NextResponse.json(
      { error: "Could not retrieve Hub workflow and its history" },
      { status: 502, headers: { "Cache-Control": "no-store" } },
    );
  }
}
