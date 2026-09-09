import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";

import {
    countRuns,
    finishRun,
    getPatch,
    getRun,
    listRuns,
    markPatch,
} from "@/db/client";
import { normaliseFiles, openPullRequest } from "@/github/client";
import { WORKSPACE_ROOT } from "@/tools/workspace";

const DEFAULT_LIMIT = 25;
const MAX_LIMIT = 100;

const API_TOKEN = process.env.API_TOKEN ?? process.env.INGEST_TOKEN;

function unauthorized(request: FastifyRequest, reply: FastifyReply): boolean {
    if (!API_TOKEN) {
        request.log.error("API_TOKEN/INGEST_TOKEN unset — refusing all API requests");
        reply.code(503).send({ error: "API is not configured" });
        return true;
    }

    if (request.headers.authorization !== `Bearer ${API_TOKEN}`) {
        reply.code(401).send({ error: "Missing or invalid Authorization header" });
        return true;
    }

    return false;
}

export const apiRoutes: FastifyPluginAsync = async (app) => {
    app.get("/v1/runs", async (request, reply) => {
        if (unauthorized(request, reply)) return reply;

        const query = request.query as Record<string, string | undefined>;
        const limit = Math.min(Number(query["limit"]) || DEFAULT_LIMIT, MAX_LIMIT);
        const offset = Math.max(Number(query["offset"]) || 0, 0);

        const [runs, total] = await Promise.all([listRuns(limit, offset), countRuns()]);

        return reply.send({ runs, total, limit, offset });
    });

    app.get("/v1/runs/:id", async (request, reply) => {
        if (unauthorized(request, reply)) return reply;

        const { id } = request.params as { id: string };

        const run = await getRun(id);
        if (!run) return reply.code(404).send({ error: `No run with id ${id}` });

        const { messages: _messages, ...rest } = run;

        return reply.send(rest);
    });

    app.post("/v1/runs/:id/approve", async (request, reply) => {
        if (unauthorized(request, reply)) return reply;

        const { id } = request.params as { id: string };
        const patch = await getPatch(id);

        if (!patch) {
            return reply.code(404).send({ error: `No proposed patch for run ${id}` });
        }

        if (patch.status === "opened" && patch.prUrl) {
            return reply.send({
                status: "already_opened",
                pr_url: patch.prUrl,
                pr_number: patch.prNumber,
            });
        }

        if (patch.status === "rejected") {
            return reply.code(409).send({
                error: "This patch was rejected. The agent must propose a new one.",
            });
        }

        const result = await openPullRequest(
            id,
            { title: patch.title, body: patch.body, files: normaliseFiles(patch.files) },
            WORKSPACE_ROOT,
        );

        if (!result.ok) {
            request.log.error({ runId: id, error: result.error }, "opening PR failed");
            return reply.code(502).send({ error: `Could not open PR: ${result.error}` });
        }

        await markPatch(id, {
            status: "opened",
            reviewNote: (request.body as { note?: string } | null)?.note,
            prUrl: result.url,
            prNumber: result.number,
        });
        await finishRun(id, "completed", `Approved. Pull request opened: ${result.url}`);

        return reply.send({
            status: result.alreadyExisted ? "already_opened" : "opened",
            pr_url: result.url,
            pr_number: result.number,
            branch: result.branch,
        });
    });

    app.post("/v1/runs/:id/reject", async (request, reply) => {
        if (unauthorized(request, reply)) return reply;

        const { id } = request.params as { id: string };
        const patch = await getPatch(id);

        if (!patch) {
            return reply.code(404).send({ error: `No proposed patch for run ${id}` });
        }
        if (patch.status === "opened") {
            return reply.code(409).send({
                error: "This patch already has an open PR. Close it on GitHub instead.",
            });
        }

        const note = (request.body as { note?: string } | null)?.note;
        await markPatch(id, { status: "rejected", reviewNote: note });
        await finishRun(id, "rejected", note ?? "Patch rejected by a human reviewer.");

        return reply.send({ status: "rejected", note: note ?? null });
    });
};
