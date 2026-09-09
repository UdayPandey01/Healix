import "dotenv/config";
import { writeFile } from "node:fs/promises";

import { prisma } from "@/lib/prisma";
import { measureRecall } from "@/retrieval/recall";

const goldenPath = process.argv[2] ?? "../evals/golden-bugs.json";
const repo = process.argv[3] ?? process.env.INDEXED_REPO ?? "demo-service";
const k = Number(process.argv[4] ?? 5);

console.log(`measuring recall@${k} for "${repo}" against ${goldenPath}…\n`);

const report = await measureRecall(goldenPath, repo, k);

const rows = Object.entries(report.byStrategy).sort((a, b) => b[1].recall - a[1].recall);

for (const [name, s] of rows) {
    const pct = (s.recall * 100).toFixed(1).padStart(5);
    console.log(`  ${name.padEnd(14)} ${String(s.hits).padStart(2)}/${report.total}  ${pct}%`);
}

if (report.rerankFailures > 0) {
    console.log(
        `\n  WARNING: the reranker failed ${report.rerankFailures} times ` +
            `(${report.rerankError}).\n  The "+rerank" rows above are NOT measurements — ` +
            `they are the un-reranked order.`,
    );
}

console.log("\nmisses:");
for (const [name, s] of rows) {
    console.log(`  ${name.padEnd(14)} ${s.misses.join(", ") || "none"}`);
}

const out = "../evals/recall-at-5.json";
await writeFile(
    out,
    JSON.stringify({ measuredAt: new Date().toISOString(), k, repo, ...report }, null, 2) + "\n",
);
console.log(`\nwritten to ${out}`);

await prisma.$disconnect();
