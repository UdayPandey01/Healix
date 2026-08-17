import type { FastifyPluginAsync } from "fastify";

import { alertSchema, type Alert } from "@/ingest/schema";
import { createRun } from "@/db/client";
import { runAgent } from "@/agent/run";

function taskFromAlert(alert: Alert): string {
    return (
        `Incident ${alert.incident_id} on service "${alert.service}": ${alert.title}. ` +
        `${alert.description}\n\n` +
        `Investigate the codebase and identify the root cause. Name the exact file ` +
        `and line, and explain why it fails.`
    );
}

export const ingestRoutes: FastifyPluginAsync = async (app) => {
    app.post("/v1/alerts/ingest", async (request, reply) => {
        const parsed = alertSchema.safeParse(request.body);
        if (!parsed.success) {
            return reply.code(422).send({ detail: parsed.error.issues });
        }

        const authorization = request.headers.authorization;
        if (authorization === undefined) {
            return reply.code(401).send({ error: "Authorization header missing" });
        }

        const task = taskFromAlert(parsed.data);
        const runId = await createRun(task);

        void runAgent(task, { resumeId: runId }).catch((err) => {
            app.log.error({ err, runId }, "agent run failed");
        });

        return reply.code(202).send({
            status: "accepted",
            message: "Healix is investigating the incident.",
            run_id: runId,
        });
    });
};
