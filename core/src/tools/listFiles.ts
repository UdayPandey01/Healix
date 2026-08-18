import { readdir } from "node:fs/promises";
import { join, relative } from "node:path";
import { Type, type FunctionDeclaration } from "@google/genai";
import { safeResolve, WORKSPACE_ROOT, IGNORED, type ToolResult } from "./workspace";

export const listFilesDeclaration: FunctionDeclaration = {
    name: "list_files",
    description:
        "List every source file under a directory, recursively. Use this FIRST " +
        "when you do not yet know what files exist or what a path is called.",
    parameters: {
        type: Type.OBJECT,
        properties: {
            path: {
                type: Type.STRING,
                description: 'Directory relative to the workspace root. Use "." for everything.',
            },
        },
        required: ["path"],
    },
};

async function walk(dir: string): Promise<string[]> {
    const entries = await readdir(dir, { withFileTypes: true });
    const found: string[] = [];

    for (const entry of entries) {
        if (IGNORED.has(entry.name)) continue;
        const full = join(dir, entry.name);

        if (entry.isDirectory()) {
            found.push(...(await walk(full)));
        } else {
            found.push(relative(WORKSPACE_ROOT, full));
        }
    }
    return found;
}

export async function listFilesTool(args: Record<string, unknown>): Promise<ToolResult> {
    const path = String(args["path"] ?? ".");
    const resolved = safeResolve(path);
    if (!resolved.ok) return resolved;

    try {
        const found = await walk(resolved.target);
        return { ok: true, content: found.sort().join("\n") || "(no files found)" };
    } catch {
        return { ok: false, error: `Cannot list "${path}" — no such directory.` };
    }
}
