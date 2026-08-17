import { readFile } from "node:fs/promises";
import { Type, type FunctionDeclaration } from "@google/genai";
import { safeResolve, type ToolResult } from "./workspace";

export const readFileDeclaration: FunctionDeclaration = {
    name: "read_file",
    description:
        "Read the full contents of one source file. Use this whenever you need to " +
        "see actual code rather than guessing at it. Paths are relative to the " +
        "workspace root.",
    parameters: {
        type: Type.OBJECT,
        properties: {
            path: {
                type: Type.STRING,
                description: "Path relative to the workspace root, e.g. src/orders.js",
            },
        },
        required: ["path"],
    },
};

export async function readFileTool(args: Record<string, unknown>): Promise<ToolResult> {
    const path = String(args["path"] ?? "");
    const resolved = safeResolve(path);
    if (!resolved.ok) return resolved;

    try {
        const content = await readFile(resolved.target, { encoding: "utf-8" });
        return { ok: true, content };
    } catch {
        return { ok: false, error: `No such file: "${path}". Try list_files first.` };
    }
}
