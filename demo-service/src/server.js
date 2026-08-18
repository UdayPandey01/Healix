import express from "express";
import { userOrderSummary, listOrders } from "./orders.js";
import { registry, httpRequests } from "./metrics.js";

const app = express();

app.use((req, res, next) => {
  res.on("finish", () => {
    const route = req.route?.path ?? req.path;
    httpRequests.inc({ method: req.method, route, status: res.statusCode });
  });
  next();
});

app.get("/metrics", async (req, res) => {
  res.set("Content-Type", registry.contentType);
  res.send(await registry.metrics());
});

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
