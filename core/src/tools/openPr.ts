import { Type, type FunctionDeclaration } from "@google/genai";

import { savePatch } from "@/db/client";
import { isConfigured, normaliseFiles } from "@/github/client";
import type { ToolContext, ToolResult } from "./workspace";

export const openPrDeclaration: FunctionDeclaration = {
    name: "open_pr",
    description:
        "Propose a fix for human review. Call this only after run_tests passes with your " +
        "change applied. Supply the COMPLETE new contents of each file you are changing — " +
        "not a diff, not a fragment. This does NOT open a pull request by itself: a human " +
        "reviews the proposal and approves it, and only then is a PR opened. Calling this " +
        "ends your investigation.",
    parameters: {
        type: Type.OBJECT,
        properties: {
            title: {
                type: Type.STRING,
                description: "One line describing the fix, not the symptom.",
            },
            body: {
                type: Type.STRING,
                description:
                    "The root cause, why this fix addresses it, and what the tests showed " +
                    "before and after.",
            },
            files: {
                type: Type.ARRAY,
                description: "Every file being changed, with its full new contents.",
                items: {
                    type: Type.OBJECT,
                    properties: {
                        path: {
                            type: Type.STRING,
                            description: "Repo-relative path, e.g. src/orders.js",
                        },
                        content: {
                            type: Type.STRING,
                            description: "The complete new contents of the file.",
                        },
                    },
                    required: ["path", "content"],
                },
            },
        },
        required: ["title", "body", "files"],
    },
};

export async function openPrTool(
    args: Record<string, unknown>,
    ctx: ToolContext = {},
): Promise<ToolResult> {
    const runId = ctx.runId;
    if (!runId) {
        return { ok: false, error: "open_pr needs a run context and none was supplied." };
    }

    const title = String(args["title"] ?? "").trim();
    const body = String(args["body"] ?? "").trim();
    const files = normaliseFiles(args["files"]);

    if (title === "") return { ok: false, error: "title is required." };
    if (files.length === 0) {
        return {
            ok: false,
            error:
                "files must contain at least one { path, content } with a repo-relative " +
                "path and the complete new file contents.",
        };
    }

    await savePatch({ runId, title, body, files });

    const warning = isConfigured()
        ? ""
        : " (note: GITHUB_TOKEN/GITHUB_REPO are not configured, so approval will not be " +
          "able to open a real PR yet)";

    return {
        ok: true,
        content:
            `Patch proposed for run ${runId}: "${title}" touching ` +
            `${files.map((f) => f.path).join(", ")}. ` +
            `It is now awaiting human approval — no pull request has been opened and none ` +
            `will be until a human approves it${warning}.`,
    };
}
