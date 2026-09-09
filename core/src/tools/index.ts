import type { FunctionDeclaration } from "@google/genai";
import { readFileDeclaration, readFileTool } from "./readFile";
import { listFilesDeclaration, listFilesTool } from "./listFiles";
import { grepCodeDeclaration, grepCodeTool } from "./grepCode";
import { runTestsDeclaration, runTestsTool } from "./runTests";
import { searchCodeDeclaration, searchCodeTool } from "./searchCode";
import { openPrDeclaration, openPrTool } from "./openPr";
import type { ToolContext, ToolResult } from "./workspace";

export type { ToolContext, ToolResult } from "./workspace";

export type Tool = {
    declaration: FunctionDeclaration;
    run: (args: Record<string, unknown>, ctx: ToolContext) => Promise<ToolResult>;
};

const TOOLS: Tool[] = [
    { declaration: readFileDeclaration, run: readFileTool },
    { declaration: listFilesDeclaration, run: listFilesTool },
    { declaration: grepCodeDeclaration, run: grepCodeTool },
    { declaration: runTestsDeclaration, run: runTestsTool },
    { declaration: searchCodeDeclaration, run: searchCodeTool },
    { declaration: openPrDeclaration, run: openPrTool },
];

const BY_NAME = new Map(TOOLS.map((t) => [t.declaration.name!, t]));

export const toolDeclarations = TOOLS.map((t) => t.declaration);

export async function dispatch(
    name: string | undefined,
    args: Record<string, unknown>,
    ctx: ToolContext = {},
): Promise<ToolResult> {
    const tool = name ? BY_NAME.get(name) : undefined;

    if (!tool) {
        return {
            ok: false,
            error: `Unknown tool "${name}". Available: ${[...BY_NAME.keys()].join(", ")}.`,
        };
    }

    try {
        return await tool.run(args, ctx);
    } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { ok: false, error: `Tool "${name}" failed: ${message}` };
    }
}
