import type { FastifyPluginAsync } from "fastify";

import { alertSchema, type Alert } from "@/ingest/schema";
import { alertmanagerSchema, normalize } from "@/ingest/alertmanager";
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

async function startInvestigation(
    alert: Alert,
    onError: (err: unknown, runId: string) => void,
): Promise<string> {
    const task = taskFromAlert(alert);
    const runId = await createRun(task);

    void runAgent(task, { resumeId: runId }).catch((err) => onError(err, runId));

    return runId;
}

export const ingestRoutes: FastifyPluginAsync = async (app) => {
    const logFailure = (err: unknown, runId: string) =>
        app.log.error({ err, runId }, "agent run failed");

    app.post("/v1/alerts/ingest", async (request, reply) => {
        const parsed = alertSchema.safeParse(request.body);
        if (!parsed.success) {
            return reply.code(422).send({ detail: parsed.error.issues });
        }

        if (request.headers.authorization === undefined) {
            return reply.code(401).send({ error: "Authorization header missing" });
        }

        const runId = await startInvestigation(parsed.data, logFailure);

        return reply.code(202).send({
            status: "accepted",
            message: "Healix is investigating the incident.",
            run_id: runId,
        });
    });

    app.post("/v1/alerts/alertmanager", async (request, reply) => {
        const parsed = alertmanagerSchema.safeParse(request.body);
        if (!parsed.success) {
            return reply.code(422).send({ detail: parsed.error.issues });
        }

        if (request.headers.authorization === undefined) {
            return reply.code(401).send({ error: "Authorization header missing" });
        }

        const alerts = normalize(parsed.data);

        if (alerts.length === 0) {
            return reply.code(200).send({ status: "ignored", reason: "no firing alerts" });
        }

        const runIds: string[] = [];
        for (const alert of alerts) {
            runIds.push(await startInvestigation(alert, logFailure));
        }

        return reply.code(202).send({
            status: "accepted",
            message: "Healix is investigating.",
            run_ids: runIds,
        });
    });
};
