import test from "node:test";
import assert from "node:assert/strict";
import { countScale } from "./count-scale.ts";

test("count scales retain actual peaks and use distinct integer ticks", () => {
  for (const [values, peak] of [[[0, 0], 0], [[1], 1], [[65, null, 0], 65], [[1000], 1000], [[1234567890], 1234567890]]) {
    const scale = countScale(values);
    assert.equal(scale.peak, peak);
    assert.ok(scale.upper >= peak && scale.upper > 0);
    assert.equal(scale.ticks[0], 0);
    assert.equal(scale.ticks.at(-1), scale.upper);
    assert.equal(new Set(scale.ticks).size, scale.ticks.length);
    assert.ok(scale.ticks.every(Number.isInteger));
    assert.equal(scale.position(0), 0);
    assert.equal(scale.position(peak), peak / scale.upper * 100);
  }
});

test("missing values are not treated as zero samples", () => {
  assert.equal(countScale([]), null);
  assert.equal(countScale([null, undefined, NaN]), null);
  assert.equal(countScale([null, 0]).peak, 0);
});
