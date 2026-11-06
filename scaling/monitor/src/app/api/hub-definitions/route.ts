import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// Read-only server-side proxy: the Hub bearer token must never reach browsers.
// Unlike /backend/definitions, this is the live PostgreSQL Hub definition catalog.
export async function GET() {
  const base = process.env.WORKFLOW_HUB_URL?.replace(/\/$/, "");
  const token = process.env.WORKFLOW_HUB_TOKEN;
  const headers = { "Cache-Control": "no-store" };

  if (!base || !token)
    return NextResponse.json({ error: "Workflow Hub connection is not configured" },
      { status: 503, headers });

  try {
    const upstream = await fetch(base + "/api/hub/definitions", {
      headers: { Authorization: "Bearer " + token, Accept: "application/json" },
      cache: "no-store",
      signal: AbortSignal.timeout(12_000),
    });
    if (!upstream.ok)
      return NextResponse.json({ error: "Workflow Hub definitions unavailable" },
        { status: 502, headers });

    const definitions: unknown = await upstream.json();
    if (!Array.isArray(definitions))
      return NextResponse.json({ error: "Invalid Hub definition catalog" },
        { status: 502, headers });

    return NextResponse.json(definitions, { headers });
  } catch {
    return NextResponse.json({ error: "Could not retrieve Hub workflow definitions" },
      { status: 502, headers });
  }
}
