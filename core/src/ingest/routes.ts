import { randomUUID } from "node:crypto";
import type { FastifyPluginAsync } from "fastify";

import { alertSchema } from "@/ingest/schema";

// Ported from alert_ingestion_api/main.py. Logic preserved as-is, including
// the two quirks noted inline — both were the original endpoint's behaviour.
export const ingestRoutes: FastifyPluginAsync = async (app) => {
  app.post("/v1/alerts/ingest", async (request, reply) => {
    // FastAPI validates the body before the handler body runs, so a bad
    // payload short-circuits with 422 even when the auth header is missing.
    const parsed = alertSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(422).send({ detail: parsed.error.issues });
    }

    const authorization = request.headers.authorization;

    // Quirk preserved: the original returned 200 with an error body here,
    // not 401.
    if (authorization === undefined) {
      return { error: "Authorization header missing" };
    }

    // Quirk preserved: Python's str.replace() replaces every occurrence, so
    // replaceAll (not replace) is the faithful port.
    const token = authorization.replaceAll("Bearer ", "");

    const traceId = "hlx_trc_" + randomUUID().slice(0, 8);

    return {
      status: "queued",
      message: "Healix is investigating the incident.",
      healix_trace_id: traceId,
      token_received: token,
    };
  });
};
