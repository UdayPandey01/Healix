import test, { after } from "node:test";
import assert from "node:assert/strict";

import { buildApp } from "../src/app";
import { prisma } from "../src/lib/prisma";
import { createRun, getPatch, recordStep } from "../src/db/client";
import { openPrTool } from "../src/tools/openPr";
import { branchForRun, normaliseFiles } from "../src/github/client";

const TOKEN = process.env.API_TOKEN ?? process.env.INGEST_TOKEN;
const app = await buildApp({ logger: false });
const createdRuns: string[] = [];

async function newRun(task = "investigate the thing") {
    const id = await createRun(task);
    createdRuns.push(id);
    return id;
}

after(async () => {
    if (createdRuns.length > 0) {
        await prisma.patch.deleteMany({ where: { runId: { in: createdRuns } } });
        await prisma.step.deleteMany({ where: { runId: { in: createdRuns } } });
        await prisma.run.deleteMany({ where: { id: { in: createdRuns } } });
    }
    await app.close();
    await prisma.$disconnect();
});

test("the read API rejects an unauthenticated caller", async () => {
    const res = await app.inject({ method: "GET", url: "/v1/runs" });
    assert.equal(res.statusCode, 401);
});

test("approval rejects an unauthenticated caller", async () => {
    const res = await app.inject({ method: "POST", url: "/v1/runs/whatever/approve" });
    assert.equal(res.statusCode, 401);
});

test("open_pr proposes a patch and does not open anything", async () => {
    const runId = await newRun();

    const result = await openPrTool(
        {
            title: "Handle unknown users in the order summary",
            body: "findUser returns undefined for missing ids.",
            files: [{ path: "src/orders.js", content: "// fixed\n" }],
        },
        { runId },
    );

    assert.equal(result.ok, true);
    if (result.ok === true) {
        assert.match(result.content, /awaiting human approval/i);
        assert.match(result.content, /no pull request has been opened/i);
    }

    const patch = await getPatch(runId);
    assert.equal(patch?.status, "pending");
    assert.equal(patch?.prUrl, null);
    assert.equal(patch?.prNumber, null);
});

test("open_pr refuses a patch with no usable files", async () => {
    const runId = await newRun();

    const result = await openPrTool(
        { title: "t", body: "b", files: [{ path: "../../etc/passwd", content: "x" }] },
        { runId },
    );

    assert.equal(result.ok, false);
    assert.equal(await getPatch(runId), null);
});

test("a rejected patch is recorded and blocks approval", async () => {
    const runId = await newRun();
    await openPrTool(
        { title: "t", body: "b", files: [{ path: "src/a.js", content: "x" }] },
        { runId },
    );

    const rejected = await app.inject({
        method: "POST",
        url: `/v1/runs/${runId}/reject`,
        headers: { authorization: `Bearer ${TOKEN}` },
        payload: { note: "wrong root cause" },
    });
    assert.equal(rejected.statusCode, 200);
    assert.equal(rejected.json().status, "rejected");

    const patch = await getPatch(runId);
    assert.equal(patch?.status, "rejected");
    assert.equal(patch?.reviewNote, "wrong root cause");

    const approved = await app.inject({
        method: "POST",
        url: `/v1/runs/${runId}/approve`,
        headers: { authorization: `Bearer ${TOKEN}` },
    });
    assert.equal(approved.statusCode, 409);
});

test("approving a run with no proposal 404s", async () => {
    const runId = await newRun();
    const res = await app.inject({
        method: "POST",
        url: `/v1/runs/${runId}/approve`,
        headers: { authorization: `Bearer ${TOKEN}` },
    });
    assert.equal(res.statusCode, 404);
});

test("the branch name is stable, which is what makes approval idempotent", () => {
    const runId = "1656a5e9-3d28-496f-81a4-56f545891db8";
    assert.equal(branchForRun(runId), `healix/run-${runId}`);
    assert.equal(branchForRun(runId), branchForRun(runId));
});

test("path traversal and absolute paths never reach GitHub", () => {
    const files = normaliseFiles([
        { path: "src/ok.js", content: "ok" },
        { path: "../../etc/passwd", content: "bad" },
        { path: "/etc/passwd", content: "bad" },
        { path: "a/../../b.js", content: "bad" },
        { path: "src/no-content.js" },
    ]);

    assert.deepEqual(
        files.map((f) => f.path),
        ["src/ok.js"],
    );
});

test("re-recording a step number is stored as a new attempt, not a collision", async () => {
    const runId = await newRun();

    await recordStep({ runId, stepNumber: 1, type: "model_call" });
    await recordStep({ runId, stepNumber: 1, type: "model_call" });

    const steps = await prisma.step.findMany({
        where: { runId, stepNumber: 1 },
        orderBy: { attempt: "asc" },
    });

    assert.equal(steps.length, 2);
    assert.deepEqual(
        steps.map((s) => s.attempt),
        [1, 2],
    );
});

test("the proposed patch is visible on the run detail endpoint", async () => {
    const runId = await newRun();
    await openPrTool(
        { title: "Visible patch", body: "b", files: [{ path: "src/a.js", content: "x" }] },
        { runId },
    );

    const res = await app.inject({
        method: "GET",
        url: `/v1/runs/${runId}`,
        headers: { authorization: `Bearer ${TOKEN}` },
    });

    assert.equal(res.statusCode, 200);
    const body = res.json();
    assert.equal(body.patch.title, "Visible patch");
    assert.equal(body.patch.status, "pending");
    assert.equal(body.messages, undefined);
});

test("the PR body always says a human must merge", async () => {
    const { prBody } = await import("../src/github/client");
    const body = prBody("Fixed the null deref.", "run-1");

    assert.ok(body.includes("Fixed the null deref."));
    assert.ok(body.toLowerCase().includes("never merges"));
    assert.ok(body.includes("run-1"));
});
