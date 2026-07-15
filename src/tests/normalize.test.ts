import test from "node:test";
import assert from "node:assert/strict";

import { normalizeUnitId } from "../utils/normalize";

test("normalizeUnitId collapses a doubled numeric prefix", () => {
  assert.equal(normalizeUnitId("120-120-a"), "120-a");
  assert.equal(normalizeUnitId(" 120-120-A "), "120-a");
});

test("normalizeUnitId leaves canonical and unrelated unit IDs unchanged", () => {
  assert.equal(normalizeUnitId("120-a"), "120-a");
  assert.equal(normalizeUnitId("121-120-a"), "121-120-a");
  assert.equal(normalizeUnitId("120-120-a-b"), "120-120-a-b");
});
