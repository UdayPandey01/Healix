import "dotenv/config";
import { GoogleGenAI } from "@google/genai";

import { prisma } from "@/lib/prisma";
import { embedQuery, toVectorLiteral } from "./embed";

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
const RERANK_MODEL = process.env.GEMINI_RERANK_MODEL ?? process.env.GEMINI_MODEL ?? "gemini-3.8-flash";

export type Hit = {
    id: string;
    path: string;
    symbol: string | null;
    kind: string;
    startLine: number;
    endLine: number;
    content: string;
    score: number;
};

type Row = Omit<Hit, "score"> & { score: number };

const CANDIDATES = 20;
const FINAL = 5;

const RRF_K = 60;

export async function denseSearch(repo: string, query: string, limit = CANDIDATES): Promise<Row[]> {
    const vector = toVectorLiteral(await embedQuery(query));

    return prisma.$queryRaw<Row[]>`
        SELECT id, path, symbol, kind, start_line AS "startLine", end_line AS "endLine",
               content, 1 - (embedding <=> ${vector}::vector) AS score
        FROM code_chunks
        WHERE repo = ${repo} AND embedding IS NOT NULL
        ORDER BY embedding <=> ${vector}::vector
        LIMIT ${limit}
    `;
}

export async function lexicalSearch(
    repo: string,
    query: string,
    limit = CANDIDATES,
): Promise<Row[]> {
    const tsquery = toOrQuery(query);
    if (tsquery === "") return [];

    return prisma.$queryRaw<Row[]>`
        SELECT id, path, symbol, kind, start_line AS "startLine", end_line AS "endLine",
               content,
               ts_rank_cd(
                   to_tsvector('english', coalesce(symbol, '') || ' ' || path || ' ' || content),
                   to_tsquery('english', ${tsquery})
               ) AS score
        FROM code_chunks
        WHERE repo = ${repo}
          AND to_tsvector('english', coalesce(symbol, '') || ' ' || path || ' ' || content)
              @@ to_tsquery('english', ${tsquery})
        ORDER BY score DESC
        LIMIT ${limit}
    `;
}

function toOrQuery(query: string): string {
    const words = query.match(/[A-Za-z][A-Za-z0-9_]*/g) ?? [];
    const terms = new Set<string>();

    for (const word of words) {
        if (word.length < 3) continue;
        terms.add(word.toLowerCase());

        for (const part of word.split(/(?=[A-Z])|_/)) {
            if (part.length >= 3) terms.add(part.toLowerCase());
        }
    }

    return [...terms].join(" | ");
}

export function rrfFuse(rankings: Row[][], k = RRF_K): Hit[] {
    const scores = new Map<string, number>();
    const byId = new Map<string, Row>();

    for (const ranking of rankings) {
        for (const [index, row] of ranking.entries()) {
            const previous = scores.get(row.id) ?? 0;
            scores.set(row.id, previous + 1 / (k + index + 1));
            byId.set(row.id, row);
        }
    }

    return [...scores.entries()]
        .map(([id, score]) => ({ ...byId.get(id)!, score }))
        .sort((a, b) => b.score - a.score);
}

export async function rerank(query: string, candidates: Hit[], limit = FINAL): Promise<Hit[]> {
    if (candidates.length <= limit) return candidates;

    const listing = candidates
        .map((c, i) => `[${i}] ${c.path}:${c.startLine}-${c.endLine} ${c.symbol ?? "(module)"}\n${c.content}`)
        .join("\n\n---\n\n");

    try {
        const res = await ai.models.generateContent({
            model: RERANK_MODEL,
            contents: [
                {
                    role: "user",
                    parts: [
                        {
                            text:
                                `An incident was reported:\n\n${query}\n\n` +
                                `Below are ${candidates.length} candidate code chunks. Identify the ` +
                                `${limit} most likely to contain the root cause, best first.\n\n` +
                                `${listing}\n\n` +
                                `Reply with ONLY a JSON array of ${limit} indices, e.g. [3,0,7,1,9].`,
                        },
                    ],
                },
            ],
        });

        const text = res.text ?? "";
        const match = text.match(/\[[\d,\s]*\]/);
        if (!match) return candidates.slice(0, limit);

        const order = (JSON.parse(match[0]) as number[])
            .filter((i) => Number.isInteger(i) && i >= 0 && i < candidates.length)
            .slice(0, limit);

        const picked = order.map((i) => candidates[i]!);
        const seen = new Set(picked.map((c) => c.id));

        for (const candidate of candidates) {
            if (picked.length >= limit) break;
            if (!seen.has(candidate.id)) picked.push(candidate);
        }

        return picked;
    } catch (err) {
        rerankFailures++;
        lastRerankError = err instanceof Error ? (err.message.split("\n")[0] ?? err.message) : String(err);
        console.warn(`rerank failed, falling back to fused order: ${lastRerankError}`);
        return candidates.slice(0, limit);
    }
}

let rerankFailures = 0;
let lastRerankError: string | null = null;

export function rerankHealth(): { failures: number; lastError: string | null } {
    return { failures: rerankFailures, lastError: lastRerankError };
}

export function resetRerankHealth(): void {
    rerankFailures = 0;
    lastRerankError = null;
}

export type Strategy = "dense" | "lexical" | "rrf" | "rrf+rerank" | "dense+rerank";
export type SearchOptions = { strategy?: Strategy; limit?: number };

export async function searchCode(
    repo: string,
    query: string,
    options: SearchOptions = {},
): Promise<Hit[]> {
    const limit = options.limit ?? FINAL;
    const strategy = options.strategy ?? "dense";

    if (strategy === "dense") {
        return (await denseSearch(repo, query, limit)).map((r) => ({ ...r }));
    }

    if (strategy === "lexical") {
        return (await lexicalSearch(repo, query, limit)).map((r) => ({ ...r }));
    }

    if (strategy === "dense+rerank") {
        const dense = (await denseSearch(repo, query, CANDIDATES)).map((r) => ({ ...r }));
        return rerank(query, dense, limit);
    }

    const [dense, lexical] = await Promise.all([
        denseSearch(repo, query, CANDIDATES),
        lexicalSearch(repo, query, CANDIDATES),
    ]);
    const fused = rrfFuse([dense, lexical]);

    if (strategy === "rrf") return fused.slice(0, limit);

    return rerank(query, fused.slice(0, CANDIDATES), limit);
}
