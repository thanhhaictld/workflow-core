export class ApiError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

/**
 * /backend is a same-origin Next rewrite to the read-only .NET monitoring API.
 * No sample or synthetic data is returned when the API is unavailable.
 */
export async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`/backend${path}`, {
    method: "GET",
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal
  });
  if (!response.ok) {
    if (response.status === 404) throw new ApiError(404, "Workflow not found");
    throw new ApiError(response.status, `Monitoring API returned HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}
