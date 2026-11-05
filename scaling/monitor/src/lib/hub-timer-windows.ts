export type HubTimerEvent = {
  stepIndex: number;
  activityType?: string;
  eventType: string;
  occurredAt: string;
  payload?: unknown;
};

function timerId(event: HubTimerEvent): string | null {
  const value = event.payload;
  return typeof value === "object" && value !== null && "timerId" in value &&
    typeof value.timerId === "string" ? value.timerId : null;
}

export function hubTimerWindows(history: readonly HubTimerEvent[]) {
  return history.flatMap((scheduled, index) => {
    if (scheduled.eventType !== "TimerScheduled") return [];
    const id = timerId(scheduled);
    const final = history.slice(index + 1).find(event =>
      (event.eventType === "TimerFired" || event.eventType === "TimerCancelled") &&
      (id ? timerId(event) === id : event.stepIndex === scheduled.stepIndex));
    return [{
      key: id ?? `${scheduled.stepIndex}:${scheduled.occurredAt}:${index}`,
      stepIndex: scheduled.stepIndex,
      label: scheduled.activityType || "Timer",
      start: scheduled.occurredAt,
      finish: final?.occurredAt ?? null,
      outcome: final?.eventType ?? "Waiting",
    }];
  });
}
