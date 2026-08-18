import client from "prom-client";

export const registry = new client.Registry();

client.collectDefaultMetrics({ register: registry });

export const httpRequests = new client.Counter({
  name: "http_requests_total",
  help: "Total HTTP requests, labelled by method, route and status code.",
  labelNames: ["method", "route", "status"],
  registers: [registry],
});
