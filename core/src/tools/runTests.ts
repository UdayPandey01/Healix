import { Type, type FunctionDeclaration } from "@google/genai";
import { WORKSPACE_ROOT, type ToolResult } from "./workspace";

const SANDBOX_URL = process.env.SANDBOX_URL ?? "http://localhost:4000";

const SANDBOX_REPO_PATH = process.env.SANDBOX_REPO_PATH ?? WORKSPACE_ROOT;
const TEST_COMMAND = process.env.TEST_COMMAND ?? "npm test";
const TIMEOUT_SECS = 60;

export const runTestsDeclaration: FunctionDeclaration = {
    name: "run_tests",
    description:
        "Run the project's test suite in an isolated sandbox and return the exit " +
        "code and output. Use this to see which tests currently fail, and to " +
        "confirm a diagnosis. Takes no arguments.",
    parameters: {
        type: Type.OBJECT,
        properties: {},
    },
};

type SandboxResponse = {
    exit_code: number | null;
    stdout: string;
    stderr: string;
    timed_out: boolean;
    duration_ms: number;
};

export async function runTestsTool(): Promise<ToolResult> {
    let res: Response;

    try {
        res = await fetch(`${SANDBOX_URL}/run`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                repo_path: SANDBOX_REPO_PATH,
                command: TEST_COMMAND,
                timeout_secs: TIMEOUT_SECS,
            }),
        });
    } catch {
        return { ok: false, error: `Sandbox unreachable at ${SANDBOX_URL}.` };
    }

    if (!res.ok) {
        return { ok: false, error: `Sandbox returned HTTP ${res.status}.` };
    }

    const result = (await res.json()) as SandboxResponse;

    if (result.timed_out) {
        return { ok: false, error: `Test suite exceeded ${TIMEOUT_SECS}s and was killed.` };
    }

    const passed = result.exit_code === 0;
    const body = [
        `exit_code: ${result.exit_code} (${passed ? "PASSED" : "FAILED"})`,
        `duration_ms: ${result.duration_ms}`,
        "",
        result.stdout.trim(),
        result.stderr.trim(),
    ]
        .filter(Boolean)
        .join("\n");

    return { ok: true, content: body };
}
