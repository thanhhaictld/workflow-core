import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

// The browser never receives the Hub bearer token. This development-only
// server endpoint exposes aggregate counts, not workflow/tenant payloads.
export async function GET() {
  const base = process.env.WORKFLOW_HUB_URL;
  const token = process.env.WORKFLOW_HUB_TOKEN;
  if (!base || !token) {
    return NextResponse.json(
      { error: "Hub connection is not configured in the monitor server" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }

  try {
    const get = async (path: string) => {
      const response = await fetch(base.replace(/\/$/, "") + "/api/hub/" + path, {
        headers: { Authorization: "Bearer " + token, Accept: "application/json" },
        signal: AbortSignal.timeout(8000),
        cache: "no-store",
      });
      if (!response.ok) throw new Error("Hub gateway returned HTTP " + response.status);
      return response.json();
    };
    const [diagnostics, runs] = await Promise.all([
      get("diagnostics"),
      get("runs?take=20"),
    ]);
    return NextResponse.json(
      { diagnostics, runs, observedAt: new Date().toISOString() },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch {
    return NextResponse.json(
      { error: "Hub gateway unavailable. Check the Hub and monitor container logs." },
      { status: 502, headers: { "Cache-Control": "no-store" } },
    );
  }
}
