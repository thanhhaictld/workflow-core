import { strict as assert } from "node:assert";
import { test } from "node:test";
import { hubTimerWindows } from "./hub-timer-windows.ts";

test("repeated and concurrent timers at one definition step retain independent windows", () => {
  const event = (eventType, timerId, occurredAt) => ({
    stepIndex: 1, activityType: "Delay", eventType, occurredAt, payload: { timerId },
  });
  const windows = hubTimerWindows([
    event("TimerScheduled", "a", "1"), event("TimerScheduled", "b", "2"),
    event("TimerCancelled", "a", "3"), event("TimerScheduled", "c", "4"),
    event("TimerFired", "b", "5"),
  ]);
  assert.deepEqual(windows.map(x => [x.key, x.finish, x.outcome]), [
    ["a", "3", "TimerCancelled"], ["b", "5", "TimerFired"], ["c", null, "Waiting"],
  ]);
});
