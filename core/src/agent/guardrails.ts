import type { Content } from "@google/genai";

export const MAX_PASSES = Number(process.env.AGENT_MAX_PASSES ?? 12);
export const MAX_STEPS = Number(process.env.AGENT_MAX_STEPS ?? 40);
export const TOKEN_BUDGET = Number(process.env.AGENT_TOKEN_BUDGET ?? 200_000);

export const REPEAT_LIMIT = Number(process.env.AGENT_REPEAT_LIMIT ?? 3);

export type Halt = { halted: true; status: string; reason: string };
export type Ok = { halted: false };
export type GuardResult = Halt | Ok;

const ok: Ok = { halted: false };

export function checkBudget(state: {
    pass: number;
    stepNumber: number;
    tokensUsed: number;
}): GuardResult {
    if (state.pass > MAX_PASSES) {
        return {
            halted: true,
            status: "exhausted",
            reason: `pass cap reached (${MAX_PASSES} passes)`,
        };
    }

    if (state.stepNumber >= MAX_STEPS) {
        return {
            halted: true,
            status: "exhausted",
            reason: `step cap reached (${MAX_STEPS} steps)`,
        };
    }

    if (state.tokensUsed >= TOKEN_BUDGET) {
        return {
            halted: true,
            status: "budget_exhausted",
            reason: `token budget spent (${state.tokensUsed} of ${TOKEN_BUDGET})`,
        };
    }

    return ok;
}

export function callSignature(name: string, args: unknown): string {
    return `${name}:${stableStringify(args)}`;
}

function stableStringify(value: unknown): string {
    if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
    if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;

    const entries = Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`);

    return `{${entries.join(",")}}`;
}

export function detectLoop(contents: Content[]): GuardResult {
    const signatures: string[] = [];

    for (const turn of contents) {
        if (turn.role !== "model") continue;
        for (const part of turn.parts ?? []) {
            const call = part.functionCall;
            if (call?.name) signatures.push(callSignature(call.name, call.args ?? {}));
        }
    }

    if (signatures.length === 0) return ok;

    const counts = new Map<string, number>();
    for (const signature of signatures) {
        counts.set(signature, (counts.get(signature) ?? 0) + 1);
    }

    for (const [signature, count] of counts) {
        if (count >= REPEAT_LIMIT) {
            return {
                halted: true,
                status: "looping",
                reason: `the same call was made ${count} times: ${truncate(signature)}`,
            };
        }
    }

    const tail = signatures.slice(-REPEAT_LIMIT);
    if (tail.length === REPEAT_LIMIT && new Set(tail).size === 1) {
        return {
            halted: true,
            status: "looping",
            reason: `the last ${REPEAT_LIMIT} calls were identical: ${truncate(tail[0]!)}`,
        };
    }

    return ok;
}

function truncate(text: string, limit = 160): string {
    return text.length <= limit ? text : `${text.slice(0, limit)}…`;
}
