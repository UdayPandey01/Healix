import "dotenv/config";
import { GoogleGenAI, type Content, type Part } from "@google/genai";
import { dispatch, toolDeclarations } from "../tools";
import {
    addTokens,
    createRun,
    finishRun,
    getRun,
    haltRun,
    recordStep,
    saveState,
} from "@/db/client";
import { checkBudget, detectLoop, MAX_PASSES } from "./guardrails";

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

const MODEL = process.env.GEMINI_MODEL ?? "gemini-3.8-flash";

const MAX_RETRIES = Number(process.env.AGENT_MAX_RETRIES ?? 6);
const MAX_BACKOFF_MS = 60_000;
const RETRYABLE = new Set([429, 500, 503]);

const tools = [{ functionDeclarations: toolDeclarations }];

const SYSTEM_INSTRUCTION =
    "You are an SRE investigating a production incident in an unfamiliar codebase. " +
    "Start with search_code, describing the symptom in plain words — it returns the " +
    "most relevant functions with their file paths and line numbers, and is usually " +
    "the fastest route to the right file. Use grep_code when you already know an exact " +
    "identifier or error string. Use list_files if you need to see what exists. Use " +
    "read_file to see a whole file. Use run_tests to observe the real failure and to " +
    "confirm a fix. " +
    "You may only describe or quote a file after reading it with read_file or receiving " +
    "it from search_code in this conversation. Never reconstruct code from memory or " +
    "infer it from a filename. If a tool returns an error, say so plainly and try a " +
    "different approach rather than guessing at the contents. " +
    "When you have a fix and run_tests passes with it, call open_pr with the complete " +
    "new contents of each changed file. Never claim to have opened a pull request " +
    "unless open_pr returned a URL.";

export type RunAgentOptions = {
    resumeId?: string;
};

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function callModel(contents: Content[]) {
    for (let attempt = 1; ; attempt++) {
        try {
            return await ai.models.generateContent({
                model: MODEL,
                contents,
                config: { tools, systemInstruction: SYSTEM_INSTRUCTION },
            });
        } catch (err) {
            const status = (err as { status?: number }).status;

            if (!status || !RETRYABLE.has(status) || attempt >= MAX_RETRIES) throw err;

            const waitMs = Math.min(2000 * 2 ** (attempt - 1), MAX_BACKOFF_MS);
            console.log(`model returned ${status}, retrying in ${waitMs}ms (${attempt}/${MAX_RETRIES})`);
            await sleep(waitMs);
        }
    }
}

export async function runAgent(
    task: string,
    options: RunAgentOptions = {},
): Promise<string> {
    const { resumeId } = options;

    let answer = "(the loop ended without producing an answer)";
    let reachedAnswer = false;
    let awaitingApproval = false;
    const contents: Content[] = [];
    let stepNumber = 0;
    let tokensUsed = 0;
    let runId: string;

    if (resumeId) {
        const prior = await getRun(resumeId);
        if (!prior) throw new Error(`No run found with id ${resumeId}`);

        if (prior.status !== "running") {
            console.log(`run ${resumeId} is already "${prior.status}" — nothing to resume.`);
            return prior.diagnosis ?? "(no diagnosis was saved)";
        }

        runId = prior.id;
        stepNumber = prior.stepCount;
        tokensUsed = prior.tokensUsed;
        contents.push(...(prior.messages as unknown as Content[]));

        const lastTurn = contents.at(-1);
        if (lastTurn?.role === "model") {
            const recovered = (lastTurn.parts ?? [])
                .map((p) => p.text ?? "")
                .join("")
                .trim();
            console.log(`run ${resumeId} already reached an answer — closing it out.`);
            await finishRun(runId, "completed", recovered);
            return recovered || "(the final turn carried no text)";
        }

        console.log(`resuming run ${runId} — ${contents.length} turns, ${stepNumber} steps done`);
    } else {
        runId = await createRun(task);
        contents.push({ role: "user", parts: [{ text: task }] });
        console.log(`run ${runId}`);
    }

    for (let pass = 1; pass <= MAX_PASSES; pass++) {
        const budget = checkBudget({ pass, stepNumber, tokensUsed });
        if (budget.halted) {
            console.log(`halting run ${runId}: ${budget.reason}`);
            await saveState(runId, contents, stepNumber);
            await haltRun(runId, budget.status, budget.reason, answer);
            return `Halted: ${budget.reason}`;
        }

        const loop = detectLoop(contents);
        if (loop.halted) {
            console.log(`halting run ${runId}: ${loop.reason}`);
            await saveState(runId, contents, stepNumber);
            await haltRun(runId, loop.status, loop.reason, answer);
            return `Halted: ${loop.reason}`;
        }

        const modelStartedAt = Date.now();
        const res = await callModel(contents);
        const modelDurationMs = Date.now() - modelStartedAt;

        const modelTurn = res.candidates?.[0]?.content;
        if (!modelTurn) throw new Error("No content came back from the model.");

        const usage = res.usageMetadata;
        const tokensIn = usage?.promptTokenCount;
        const tokensOut =
            (usage?.candidatesTokenCount ?? 0) + (usage?.thoughtsTokenCount ?? 0);

        tokensUsed = await addTokens(runId, (tokensIn ?? 0) + tokensOut);

        stepNumber++;
        await recordStep({
            runId,
            stepNumber,
            type: "model_call",
            input: { pass },
            output: modelTurn,
            tokensIn,
            tokensOut,
            durationMs: modelDurationMs,
        });

        contents.push(modelTurn);

        const calls = [];
        for (const part of modelTurn.parts ?? []) {
            if (part.functionCall) calls.push(part.functionCall);
        }

        if (calls.length === 0) {
            answer = res.text ?? "(model returned no text)";
            reachedAnswer = true;
            await saveState(runId, contents, stepNumber);
            break;
        }

        const resultParts: Part[] = [];
        for (const call of calls) {
            const args = (call.args ?? {}) as Record<string, unknown>;

            const toolStartedAt = Date.now();
            const result = await dispatch(call.name, args, { runId });
            const toolDurationMs = Date.now() - toolStartedAt;

            if (call.name === "open_pr" && result.ok) awaitingApproval = true;

            console.log(
                `pass ${pass}: ${call.name}(${JSON.stringify(args)}) -> ` +
                (result.ok ? `${result.content.length} chars` : result.error),
            );

            stepNumber++;
            await recordStep({
                runId,
                stepNumber,
                type: "tool_call",
                toolName: call.name,
                input: args,
                output: result,
                durationMs: toolDurationMs,
            });

            let response;
            if (result.ok) {
                response = { output: result.content };
            } else {
                response = { error: result.error };
            }

            resultParts.push({
                functionResponse: { id: call.id, name: call.name, response },
            });
        }

        contents.push({ role: "user", parts: resultParts });
        await saveState(runId, contents, stepNumber);

        if (awaitingApproval) {
            await haltRun(
                runId,
                "awaiting_approval",
                "patch proposed, waiting for human review",
                answer,
            );
            return "A patch has been proposed and is awaiting human approval.";
        }
    }

    await finishRun(runId, reachedAnswer ? "completed" : "exhausted", answer);
    return answer;
}
