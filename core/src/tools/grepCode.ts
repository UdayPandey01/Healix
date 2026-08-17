import { readdir, readFile } from "node:fs/promises";
import { Type, type FunctionDeclaration } from "@google/genai";
import { join, relative } from "node:path";
import { safeResolve, WORKSPACE_ROOT, IGNORED, type ToolResult } from "./workspace";

const MAX_MATCHES = 50;

export const grepCodeDeclaration: FunctionDeclaration = {
    name: "grep_code",
    description:
        "Search the workspace for an exact piece of text and return matching " +
        "file paths with line numbers. Use this to find where a function, error " +
        "message, or identifier appears. This is literal text matching, not a " +
        "semantic search — search for exact identifiers, not descriptions.",
    parameters: {
        type: Type.OBJECT,
        properties: {
            query: {
                type: Type.STRING,
                description: "Exact text to search for, e.g. userOrderSummary",
            },
        },
        required: ["query"],
    },
};

async function search(dir: string, query: string, hits: string[]): Promise<void> {
    if (hits.length >= MAX_MATCHES) return;

    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
        if (hits.length >= MAX_MATCHES) return;
        if (IGNORED.has(entry.name)) continue;

        const full = join(dir, entry.name);
        if (entry.isDirectory()) {
            await search(full, query, hits);
            continue;
        }

        let text: string;
        try {
            text = await readFile(full, { encoding: "utf-8" });
        } catch {
            continue; 
        }

        const lines = text.split("\n");
        for (let i = 0; i < lines.length; i++) {
            if (lines[i]?.includes(query)) {
                hits.push(`${relative(WORKSPACE_ROOT, full)}:${i + 1}: ${lines[i]?.trim()}`);
                if (hits.length >= MAX_MATCHES) return;
            }
        }
    }
}

export async function grepCodeTool(args: Record<string, unknown>): Promise<ToolResult> {
    const query = String(args["query"] ?? "");
    if (query === "") return { ok: false, error: "query cannot be empty." };

    const resolved = safeResolve(".");
    if (!resolved.ok) return resolved;

    const hits: string[] = [];
    await search(resolved.target, query, hits);

    if (hits.length === 0) {
        return { ok: true, content: `No matches for "${query}".` };
    }
    return { ok: true, content: hits.join("\n") };
}
