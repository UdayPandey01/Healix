import { readFile } from "node:fs/promises";

import {
    denseSearch,
    lexicalSearch,
    rerank,
    rerankHealth,
    resetRerankHealth,
    rrfFuse,
    type Hit,
} from "./search";

type GoldenBug = {
    id: string;
    class: string;
    query: string;
    expect: { file: string; symbol: string; line: number };
};

type Golden = { target: string; bugs: GoldenBug[] };

function isHit(hit: Hit, bug: GoldenBug): boolean {
    const samePath = hit.path === bug.expect.file || hit.path.endsWith(bug.expect.file);
    return samePath && hit.symbol === bug.expect.symbol;
}

function recallAt(hits: Hit[], bug: GoldenBug, k: number): boolean {
    return hits.slice(0, k).some((hit) => isHit(hit, bug));
}

export type StrategyName = "dense" | "lexical" | "rrf" | "rrf+rerank" | "dense+rerank";

export type RecallReport = {
    total: number;

    rerankFailures: number;
    rerankError: string | null;
    byStrategy: Record<StrategyName, { hits: number; recall: number; misses: string[] }>;
};

export async function measureRecall(
    goldenPath: string,
    repo: string,
    k = 5,
): Promise<RecallReport> {
    const golden = JSON.parse(await readFile(goldenPath, "utf-8")) as Golden;
    resetRerankHealth();

    const strategies: StrategyName[] = [
        "dense",
        "lexical",
        "rrf",
        "rrf+rerank",
        "dense+rerank",
    ];
    const report: RecallReport = {
        total: golden.bugs.length,
        rerankFailures: 0,
        rerankError: null,
        byStrategy: Object.fromEntries(
            strategies.map((s) => [s, { hits: 0, recall: 0, misses: [] as string[] }]),
        ) as RecallReport["byStrategy"],
    };

    for (const bug of golden.bugs) {
        const [dense, lexical] = await Promise.all([
            denseSearch(repo, bug.query),
            lexicalSearch(repo, bug.query),
        ]);

        const fused = rrfFuse([dense, lexical]);
        const denseHits: Hit[] = dense.map((r) => ({ ...r }));

        const [reranked, denseReranked] = await Promise.all([
            rerank(bug.query, fused.slice(0, 20), k),
            rerank(bug.query, denseHits.slice(0, 20), k),
        ]);

        const results: Record<StrategyName, Hit[]> = {
            dense: denseHits,
            lexical: lexical.map((r) => ({ ...r })),
            rrf: fused,
            "rrf+rerank": reranked,
            "dense+rerank": denseReranked,
        };

        for (const strategy of strategies) {
            if (recallAt(results[strategy], bug, k)) {
                report.byStrategy[strategy].hits++;
            } else {
                report.byStrategy[strategy].misses.push(bug.id);
            }
        }
    }

    for (const strategy of strategies) {
        const entry = report.byStrategy[strategy];
        entry.recall = report.total === 0 ? 0 : entry.hits / report.total;
    }

    const health = rerankHealth();
    report.rerankFailures = health.failures;
    report.rerankError = health.lastError;

    return report;
}
