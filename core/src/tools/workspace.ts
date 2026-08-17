import { resolve, relative, isAbsolute } from "node:path";

export const WORKSPACE_ROOT = resolve(
    process.env.TARGET_REPO ?? resolve(process.cwd(), "..", "demo-service"),
);

export type ToolResult =
    | { ok: true; content: string }
    | { ok: false; error: string };

export function safeResolve(
    path: string,
): { ok: true; target: string } | { ok: false; error: string } {
    const target = resolve(WORKSPACE_ROOT, path);
    const rel = relative(WORKSPACE_ROOT, target);

    if (rel.startsWith("..") || isAbsolute(rel)) {
        return { ok: false, error: `Refused: "${path}" is outside the workspace.` };
    }
    return { ok: true, target };
}

export const IGNORED = new Set(["node_modules", ".git", "dist", ".next", "coverage"]);
