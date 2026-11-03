import { NextRequest, NextResponse } from "next/server";
export const dynamic = "force-dynamic";
export const runtime = "nodejs";

// Server-only token; browser receives aggregates, not the Hub bearer secret.
// Same authentication boundary as /api/hub-overview; do not publish this
// development monitor without gateway auth, scoped access and TLS.
export async function GET(request: NextRequest) {
  const base = process.env.WORKFLOW_HUB_URL;
  const token = process.env.WORKFLOW_HUB_TOKEN;
  if (!base || !token) return NextResponse.json(
    { error: "Hub connection is not configured on the monitor server" },
    { status: 503, headers: { "Cache-Control": "no-store" } }
  );
  const hub = request.nextUrl.searchParams.get("hub") || "";
  const windowMinutes = request.nextUrl.searchParams.get("windowMinutes") || "60";
  if (hub.length > 80 || !["60", "360", "1440"].includes(windowMinutes))
    return NextResponse.json({ error: "Invalid monitor filter" }, { status: 400 });
  const query = new URLSearchParams({ windowMinutes });
  if (hub) query.set("hub", hub);
  try {
    const upstream = await fetch(base.replace(/\/$/, "") + "/api/hub/operations?" + query.toString(), {
      headers: { Authorization: "Bearer " + token, Accept: "application/json" },
      cache: "no-store",
      signal: AbortSignal.timeout(15000)
    });
    if (!upstream.ok) return NextResponse.json(
      { error: "Hub monitoring gateway returned HTTP " + upstream.status },
      { status: upstream.status >= 500 ? 502 : upstream.status,
        headers: { "Cache-Control": "no-store" } }
    );
    return NextResponse.json(await upstream.json(), { headers: { "Cache-Control": "no-store" } });
  } catch {
    return NextResponse.json(
      { error: "Hub monitoring gateway is unavailable" },
      { status: 502, headers: { "Cache-Control": "no-store" } }
    );
  }
}