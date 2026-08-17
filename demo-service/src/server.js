import express from "express";
import { userOrderSummary, listOrders } from "./orders.js";

const app = express();

app.get("/health", (req, res) => res.json({ status: "ok" }));

app.get("/users/:id/summary", (req, res) => {
  const summary = userOrderSummary(Number(req.params.id));
  res.json(summary);
});

app.get("/orders", (req, res) => {
  const page = Number(req.query.page ?? 1);
  const limit = Number(req.query.limit ?? 3);
  res.json(listOrders(page, limit));
});

const port = Number(process.env.PORT ?? 3001);
app.listen(port, () => console.log(`demo-service listening on ${port}`));
