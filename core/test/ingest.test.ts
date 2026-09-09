import test, { after } from "node:test";
import assert from "node:assert/strict";

import { buildApp } from "../src/app";
import { prisma } from "../src/lib/prisma";
import {
    createRun,
    finishRun,
    findActiveRun,
    upsertIncident,
} from "../src/db/client";

const TOKEN = process.env.INGEST_TOKEN;
const FINGERPRINT = `test-fp-${process.pid}-${Date.now()}`;

const app = await buildApp({ logger: false });

after(async () => {
    const incident = await prisma.incident.findUnique({
        where: { fingerprint: FINGERPRINT },
        select: { id: true },
    });
    if (incident) {
        await prisma.run.deleteMany({ where: { incidentId: incident.id } });
        await prisma.incident.delete({ where: { id: incident.id } });
    }
    await app.close();
    await prisma.$disconnect();
});

test("unauthenticated request with a malformed body gets 401, not 422", async () => {
    const res = await app.inject({
        method: "POST",
        url: "/v1/alerts/ingest",
        payload: {},
    });

    assert.equal(res.statusCode, 401);
});

test("a wrong bearer token is rejected", async () => {
    const res = await app.inject({
        method: "POST",
        url: "/v1/alerts/ingest",
        headers: { authorization: "Bearer definitely-not-the-token" },
        payload: {},
    });

    assert.equal(res.statusCode, 401);
});

test("an authenticated request with a malformed body gets 422", async () => {
    const res = await app.inject({
        method: "POST",
        url: "/v1/alerts/ingest",
        headers: { authorization: `Bearer ${TOKEN}` },
        payload: {},
    });

    assert.equal(res.statusCode, 422);
});

test("the same fingerprint upserts to one incident", async () => {
    const first = await upsertIncident({
        fingerprint: FINGERPRINT,
        service: "demo-service",
        title: "Elevated 5xx rate",
        description: "first delivery",
        payload: { delivery: 1 },
    });

    const second = await upsertIncident({
        fingerprint: FINGERPRINT,
        service: "demo-service",
        title: "Elevated 5xx rate",
        description: "second delivery",
        payload: { delivery: 2 },
    });

    assert.equal(first, second);

    const stored = await prisma.incident.findUnique({ where: { fingerprint: FINGERPRINT } });
    assert.equal(stored?.description, "second delivery");
    assert.deepEqual(stored?.payload, { delivery: 2 });
});

test("findActiveRun sees a running run and stops seeing it once finished", async () => {
    const incidentId = await upsertIncident({
        fingerprint: FINGERPRINT,
        service: "demo-service",
        title: "Elevated 5xx rate",
        description: "for the dedupe check",
        payload: {},
    });

    assert.equal(await findActiveRun(incidentId), null);

    const runId = await createRun("investigate the thing", incidentId);
    assert.equal(await findActiveRun(incidentId), runId);

    await finishRun(runId, "completed", "it was the null user");
    assert.equal(
        await findActiveRun(incidentId),
        null,
        "a finished run must not block a genuine re-fire",
    );
});

test("GET /v1/runs/:id returns steps but never the messages blob", async () => {
    const incidentId = await upsertIncident({
        fingerprint: FINGERPRINT,
        service: "demo-service",
        title: "Elevated 5xx rate",
        description: "for the read api check",
        payload: {},
    });
    const runId = await createRun("investigate the thing", incidentId);

    const res = await app.inject({
        method: "GET",
        url: `/v1/runs/${runId}`,
        headers: { authorization: `Bearer ${TOKEN}` },
    });
    assert.equal(res.statusCode, 200);

    const body = res.json();
    assert.equal(body.id, runId);
    assert.equal(body.messages, undefined, "messages must not be exposed by the read API");
    assert.ok(Array.isArray(body.steps));
    assert.equal(body.incident.fingerprint, FINGERPRINT);
});

test("GET /v1/runs/:id 404s on an unknown id", async () => {
    const res = await app.inject({
        method: "GET",
        url: "/v1/runs/00000000-0000-0000-0000-000000000000",
        headers: { authorization: `Bearer ${TOKEN}` },
    });

    assert.equal(res.statusCode, 404);
});

test("GET /v1/runs lists newest first and caps the limit", async () => {
    const res = await app.inject({
        method: "GET",
        url: "/v1/runs?limit=500",
        headers: { authorization: `Bearer ${TOKEN}` },
    });
    assert.equal(res.statusCode, 200);

    const body = res.json();
    assert.equal(body.limit, 100, "limit must be capped at MAX_LIMIT");
    assert.ok(body.total >= 1);
    assert.ok(body.runs.length <= 100);
    assert.equal(body.runs[0].messages, undefined);
});
