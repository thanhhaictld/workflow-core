export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const value = new Date(iso);
  return Number.isNaN(value.getTime()) ? "—" : value.toLocaleString();
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const value = new Date(iso);
  return Number.isNaN(value.getTime()) ? "—" : value.toLocaleTimeString();
}

export function durationMs(start: string | null | undefined, end: string | null | undefined): number | null {
  if (!start || !end) return null;
  const ms = new Date(end).getTime() - new Date(start).getTime();
  return Number.isFinite(ms) && ms >= 0 ? ms : null;
}

export function formatDuration(milliseconds: number | null): string {
  if (milliseconds === null) return "—";
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1000).toFixed(2)} s`;
  if (milliseconds < 3_600_000) return `${(milliseconds / 60_000).toFixed(1)} min`;
  return `${(milliseconds / 3_600_000).toFixed(1)} hr`;
}

export function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return id.length <= 16 ? id : `${id.slice(0, 8)}…${id.slice(-5)}`;
}

export function displayEvent(name: string): string {
  return name.replace(/([a-z])([A-Z])/g, "$1 $2");
}

export function detailsObject(input: string | null): Record<string, unknown> {
  if (!input) return {};
  try {
    const value: unknown = JSON.parse(input);
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? value as Record<string, unknown> : { value };
  } catch {
    return { value: input };
  }
}
