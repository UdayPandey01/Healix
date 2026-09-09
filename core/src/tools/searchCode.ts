import { Type, type FunctionDeclaration } from "@google/genai";

import { searchCode } from "@/retrieval/search";
import type { ToolResult } from "./workspace";

const REPO = process.env.INDEXED_REPO ?? "demo-service";
const LIMIT = 5;

export const searchCodeDeclaration: FunctionDeclaration = {
    name: "search_code",
    description:
        "Search the codebase by meaning and get back the most relevant functions with " +
        "their file paths and line numbers. Use this when you know what is going wrong " +
        "but not where — describe the symptom, the error, or the behaviour in plain " +
        "words. For an exact identifier or error string you already know, grep_code is " +
        "the better tool.",
    parameters: {
        type: Type.OBJECT,
        properties: {
            query: {
                type: Type.STRING,
                description:
                    "What you are looking for, in plain words. e.g. 'pagination skips " +
                    "the first page of results' or 'session expires far too early'.",
            },
        },
        required: ["query"],
    },
};

export async function searchCodeTool(args: Record<string, unknown>): Promise<ToolResult> {
    const query = String(args["query"] ?? "").trim();
    if (query === "") return { ok: false, error: "query cannot be empty." };

    let hits;
    try {
        hits = await searchCode(REPO, query, { limit: LIMIT });
    } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { ok: false, error: `Search failed: ${message}` };
    }

    if (hits.length === 0) {
        return {
            ok: true,
            content:
                `No indexed matches for "${query}". The index may be empty — ` +
                `list_files and grep_code still work.`,
        };
    }

    const body = hits
        .map(
            (hit, i) =>
                `[${i + 1}] ${hit.path}:${hit.startLine}-${hit.endLine}` +
                `${hit.symbol ? ` — ${hit.symbol}` : ""}\n${hit.content}`,
        )
        .join("\n\n");

    return { ok: true, content: body };
}
