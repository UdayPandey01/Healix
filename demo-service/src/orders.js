import { orders, findUser } from "./data.js";

export function userOrderSummary(userId) {
  const user = findUser(userId);
  const mine = orders.filter((o) => o.userId === userId);

  return {
    user: user.name,
    orderCount: mine.length,
    total: mine.reduce((sum, o) => sum + o.total, 0),
  };
}

export function listOrders(page, limit) {
  const start = page * limit;
  const end = start + limit;

  return {
    page,
    limit,
    total: orders.length,
    items: orders.slice(start, end),
  };
}
