import { readdir } from "node:fs/promises";
import { Type, type FunctionDeclaration } from "@google/genai";
import { join, relative } from "node:path";
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
                description:
                    "Directory relative to the workspace root. Use \".\" for the whole workspace.",
            },
        },
        required: ["path"],
    },
};

async function walk(dir: string, found: string[]): Promise<void> {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
        if (IGNORED.has(entry.name)) continue;
        const full = join(dir, entry.name);
        if (entry.isDirectory()) {
            await walk(full, found);
        } else {
            found.push(relative(WORKSPACE_ROOT, full));
        }
    }
}

export async function listFilesTool(args: Record<string, unknown>): Promise<ToolResult> {
    const path = String(args["path"] ?? ".");
    const resolved = safeResolve(path);
    if (!resolved.ok) return resolved;

    try {
        const found: string[] = [];
        await walk(resolved.target, found);
        if (found.length === 0) return { ok: true, content: "(no files found)" };
        return { ok: true, content: found.sort().join("\n") };
    } catch {
        return { ok: false, error: `Cannot list "${path}" — no such directory.` };
    }
}
