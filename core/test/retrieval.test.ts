import test, { after } from "node:test";
import assert from "node:assert/strict";

import { rrfFuse, searchCode, type Hit } from "../src/retrieval/search";
import { searchCodeTool } from "../src/tools/searchCode";
import { prisma } from "../src/lib/prisma";

after(async () => {
    await prisma.$disconnect();
});

const row = (id: string): Omit<Hit, "score"> & { score: number } => ({
    id,
    path: `src/${id}.js`,
    symbol: id,
    kind: "function",
    startLine: 1,
    endLine: 2,
    content: id,
    score: 0,
});

test("RRF ranks by reciprocal rank, not by the incoming scores", () => {
    const a = [row("x"), row("y")];
    const b = [row("y"), row("z")];

    const fused = rrfFuse([a, b]);

    assert.equal(fused[0]!.id, "y");
    assert.deepEqual(
        fused.map((f) => f.id).sort(),
        ["x", "y", "z"],
    );
});

test("RRF demotes a chunk only one ranker found — the documented failure mode", () => {
    const strong = [row("a"), row("target"), row("b")];
    const weak = [row("b"), row("c")];

    const fused = rrfFuse([strong, weak]);
    const ids = fused.map((f) => f.id);

    assert.ok(
        ids.indexOf("b") < ids.indexOf("target"),
        `expected b to outrank target after fusion, got ${ids.join(" > ")}`,
    );
});

test("search_code rejects an empty query", async () => {
    const result = await searchCodeTool({ query: "   " });
    assert.equal(result.ok, false);
});

test("search finds the paginated-orders bug from a plain-words symptom", async () => {
    const hits = await searchCode("demo-service", "pagination skips the first page of orders", {
        limit: 5,
    });

    assert.ok(hits.length > 0, "no hits returned");
    assert.ok(
        hits.some((h) => h.symbol === "listOrders" && h.path.endsWith("orders.js")),
        `listOrders not in top 5: ${hits.map((h) => h.symbol).join(", ")}`,
    );
});

test("search returns whole chunks with real line numbers", async () => {
    const result = await searchCodeTool({ query: "session expires far too early" });

    assert.equal(result.ok, true);
    if (result.ok !== true) return;

    assert.match(result.content, /src\/auth\.js:\d+-\d+/);
    assert.ok(result.content.includes("verifyToken"));
});
