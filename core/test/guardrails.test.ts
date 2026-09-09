import test from "node:test";
import assert from "node:assert/strict";
import type { Content } from "@google/genai";

import {
    callSignature,
    checkBudget,
    detectLoop,
    MAX_STEPS,
    REPEAT_LIMIT,
    TOKEN_BUDGET,
} from "../src/agent/guardrails";

const modelTurn = (name: string, args: unknown): Content => ({
    role: "model",
    parts: [{ functionCall: { name, args: args as Record<string, unknown> } }],
});

test("a run inside its budget is not halted", () => {
    assert.equal(checkBudget({ pass: 1, stepNumber: 0, tokensUsed: 0 }).halted, false);
});

test("the step cap halts the run", () => {
    const result = checkBudget({ pass: 2, stepNumber: MAX_STEPS, tokensUsed: 0 });
    assert.equal(result.halted, true);
    if (result.halted) {
        assert.equal(result.status, "exhausted");
        assert.match(result.reason, /step cap/);
    }
});

test("the token budget halts the run", () => {
    const result = checkBudget({ pass: 2, stepNumber: 1, tokensUsed: TOKEN_BUDGET });
    assert.equal(result.halted, true);
    if (result.halted) {
        assert.equal(result.status, "budget_exhausted");
        assert.match(result.reason, /token budget/);
    }
});

test("the budget is checked before the call, so the breaching call never happens", () => {
    assert.equal(
        checkBudget({ pass: 1, stepNumber: 1, tokensUsed: TOKEN_BUDGET - 1 }).halted,
        false,
    );
    assert.equal(checkBudget({ pass: 1, stepNumber: 1, tokensUsed: TOKEN_BUDGET }).halted, true);
});

test("argument key order does not change a call signature", () => {
    assert.equal(
        callSignature("read_file", { path: "a.js", encoding: "utf8" }),
        callSignature("read_file", { encoding: "utf8", path: "a.js" }),
    );
});

test("different arguments are different calls", () => {
    assert.notEqual(
        callSignature("read_file", { path: "a.js" }),
        callSignature("read_file", { path: "b.js" }),
    );
});

test("a varied investigation is not flagged as looping", () => {
    const contents: Content[] = [
        modelTurn("search_code", { query: "pagination" }),
        modelTurn("read_file", { path: "src/orders.js" }),
        modelTurn("run_tests", {}),
        modelTurn("read_file", { path: "src/cart.js" }),
    ];

    assert.equal(detectLoop(contents).halted, false);
});

test("run_tests twice is a legitimate retry, not a loop", () => {
    const contents: Content[] = [
        modelTurn("run_tests", {}),
        modelTurn("read_file", { path: "src/orders.js" }),
        modelTurn("run_tests", {}),
    ];

    assert.equal(detectLoop(contents).halted, false);
});

test("the same call repeated REPEAT_LIMIT times halts the run", () => {
    const contents: Content[] = Array.from({ length: REPEAT_LIMIT }, () =>
        modelTurn("read_file", { path: "src/orders.js" }),
    );

    const result = detectLoop(contents);
    assert.equal(result.halted, true);
    if (result.halted) {
        assert.equal(result.status, "looping");
        assert.match(result.reason, /same call was made/);
    }
});

test("a tight loop at the end is caught even in a long varied run", () => {
    const contents: Content[] = [
        modelTurn("search_code", { query: "a" }),
        modelTurn("list_files", { path: "." }),
        modelTurn("read_file", { path: "src/a.js" }),
        modelTurn("read_file", { path: "src/b.js" }),
        ...Array.from({ length: REPEAT_LIMIT }, () => modelTurn("run_tests", { suite: "x" })),
    ];

    const result = detectLoop(contents);
    assert.equal(result.halted, true);
    if (result.halted) assert.equal(result.status, "looping");
});

test("loop detection reads the conversation, so it survives a resume", () => {
    const beforeCrash: Content[] = Array.from({ length: REPEAT_LIMIT - 1 }, () =>
        modelTurn("grep_code", { query: "x" }),
    );
    assert.equal(detectLoop(beforeCrash).halted, false);

    const afterResume = [...beforeCrash, modelTurn("grep_code", { query: "x" })];
    assert.equal(detectLoop(afterResume).halted, true);
});

test("an empty conversation is not a loop", () => {
    assert.equal(detectLoop([]).halted, false);
});

test("runAgent halts a resumed run whose conversation is already looping", async () => {
    const { prisma } = await import("../src/lib/prisma");
    const { createRun, saveState } = await import("../src/db/client");
    const { runAgent } = await import("../src/agent/run");

    const call = {
        role: "model",
        parts: [{ functionCall: { name: "read_file", args: { path: "src/orders.js" } } }],
    };
    const result = {
        role: "user",
        parts: [
            {
                functionResponse: {
                    name: "read_file",
                    response: { output: "// contents" },
                },
            },
        ],
    };

    const looping = [
        { role: "user", parts: [{ text: "investigate" }] },
        call,
        result,
        call,
        result,
        call,
        result,
    ];

    const runId = await createRun("investigate the looping thing");
    try {
        await saveState(runId, looping, 6);

        const answer = await runAgent("", { resumeId: runId });

        const run = await prisma.run.findUnique({
            where: { id: runId },
            select: { status: true, haltReason: true },
        });

        assert.equal(run?.status, "looping");
        assert.match(run?.haltReason ?? "", /same call was made 3 times/);
        assert.match(answer, /^Halted:/);
    } finally {
        await prisma.step.deleteMany({ where: { runId } });
        await prisma.run.delete({ where: { id: runId } });
        await prisma.$disconnect();
    }
});

test("a permanently-failing run is marked failed and stops being swept", async () => {
    const { prisma } = await import("../src/lib/prisma");
    const { createRun, findStaleRuns, recordResumeFailure } = await import("../src/db/client");

    const runId = await createRun("a run whose stored conversation is malformed");
    try {
        await prisma.run.update({
            where: { id: runId },
            data: { updatedAt: new Date(Date.now() - 60 * 60 * 1000) },
        });

        const before = await findStaleRuns(60_000, 50);
        assert.ok(before.some((r) => r.id === runId), "should be sweepable before failing");

        const givenUp = await recordResumeFailure(runId, "400 INVALID_ARGUMENT", true);
        assert.equal(givenUp, true);

        const run = await prisma.run.findUnique({
            where: { id: runId },
            select: { status: true, haltReason: true },
        });
        assert.equal(run?.status, "failed");
        assert.match(run?.haltReason ?? "", /permanently/);

        const after = await findStaleRuns(60_000, 50);
        assert.ok(!after.some((r) => r.id === runId), "must not be swept again");
    } finally {
        await prisma.run.delete({ where: { id: runId } });
        await prisma.$disconnect();
    }
});

test("a transient failure is retried, then given up on after the attempt budget", async () => {
    const { prisma } = await import("../src/lib/prisma");
    const { createRun, findStaleRuns, recordResumeFailure } = await import("../src/db/client");

    const runId = await createRun("a run that keeps hitting 429");
    try {
        await prisma.run.update({
            where: { id: runId },
            data: { updatedAt: new Date(Date.now() - 60 * 60 * 1000) },
        });

        assert.equal(await recordResumeFailure(runId, "429", false, 3), false);
        assert.equal(await recordResumeFailure(runId, "429", false, 3), false);
        assert.equal(await recordResumeFailure(runId, "429", false, 3), true);

        const after = await findStaleRuns(60_000, 50, 3);
        assert.ok(!after.some((r) => r.id === runId), "budget spent — must not be swept again");
    } finally {
        await prisma.run.delete({ where: { id: runId } });
        await prisma.$disconnect();
    }
});
