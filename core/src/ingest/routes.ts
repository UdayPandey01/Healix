import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";

import { alertSchema, type Alert } from "@/ingest/schema";
import { alertmanagerSchema, normalize } from "@/ingest/alertmanager";
import { createRun, findActiveRun, upsertIncident } from "@/db/client";
import { runAgent } from "@/agent/run";

const INGEST_TOKEN = process.env.INGEST_TOKEN;

function taskFromAlert(alert: Alert): string {
    return (
        `Incident ${alert.incident_id} on service "${alert.service}": ${alert.title}. ` +
        `${alert.description}\n\n` +
        `Investigate the codebase and identify the root cause. Name the exact file ` +
        `and line, and explain why it fails.`
    );
}

function unauthorized(request: FastifyRequest, reply: FastifyReply): boolean {
    if (!INGEST_TOKEN) {
        request.log.error("INGEST_TOKEN is not set — refusing all ingest requests");
        reply.code(503).send({ error: "Ingestion is not configured" });
        return true;
    }

    const header = request.headers.authorization;
    if (header !== `Bearer ${INGEST_TOKEN}`) {
        reply.code(401).send({ error: "Missing or invalid Authorization header" });
        return true;
    }

    return false;
}

type Started = { run_id: string; incident_id: string; deduplicated: boolean };

async function startInvestigation(
    alert: Alert,
    raw: unknown,
    onError: (err: unknown, runId: string) => void,
): Promise<Started> {
    const incidentId = await upsertIncident({
        fingerprint: alert.incident_id,
        service: alert.service,
        title: alert.title,
        description: alert.description,
        payload: raw,
    });

    const active = await findActiveRun(incidentId);
    if (active) {
        return { run_id: active, incident_id: incidentId, deduplicated: true };
    }

    const task = taskFromAlert(alert);
    const runId = await createRun(task, incidentId);

    void runAgent(task, { resumeId: runId }).catch((err) => onError(err, runId));

    return { run_id: runId, incident_id: incidentId, deduplicated: false };
}

export const ingestRoutes: FastifyPluginAsync = async (app) => {
    const logFailure = (err: unknown, runId: string) =>
        app.log.error({ err, runId }, "agent run failed");

    app.post("/v1/alerts/ingest", async (request, reply) => {
        if (unauthorized(request, reply)) return reply;

        const parsed = alertSchema.safeParse(request.body);
        if (!parsed.success) {
            return reply.code(422).send({ detail: parsed.error.issues });
        }

        const started = await startInvestigation(parsed.data, request.body, logFailure);

        return reply.code(202).send({
            status: started.deduplicated ? "already_investigating" : "accepted",
            message: started.deduplicated
                ? "An investigation into this incident is already running."
                : "Healix is investigating the incident.",
            ...started,
        });
    });

    app.post("/v1/alerts/alertmanager", async (request, reply) => {
        if (unauthorized(request, reply)) return reply;

        const parsed = alertmanagerSchema.safeParse(request.body);
        if (!parsed.success) {
            return reply.code(422).send({ detail: parsed.error.issues });
        }

        const firing = normalize(parsed.data);

        if (firing.length === 0) {
            return reply.code(200).send({ status: "ignored", reason: "no firing alerts" });
        }

        const started: Started[] = [];
        for (const { alert, raw } of firing) {
            started.push(await startInvestigation(alert, raw, logFailure));
        }

        return reply.code(202).send({
            status: "accepted",
            message: "Healix is investigating.",
            runs: started,
        });
    });
};
