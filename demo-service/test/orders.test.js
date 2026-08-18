import test from "node:test";
import assert from "node:assert/strict";
import { userOrderSummary, listOrders } from "../src/orders.js";

test("summary works for a real user", () => {
  const s = userOrderSummary(1);
  assert.equal(s.user, "Ada");
  assert.equal(s.orderCount, 3);
});

test("summary handles an unknown user without throwing", () => {
  assert.doesNotThrow(() => userOrderSummary(999));
});

test("page 1 returns the first records", () => {
  const p = listOrders(1, 3);
  assert.equal(p.items[0].id, 101);
  assert.equal(p.items.length, 3);
});

test("total reports every order", () => {
  assert.equal(listOrders(1, 3).total, 7);
});
