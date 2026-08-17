import "dotenv/config";
import { GoogleGenAI, type Content, type Part } from "@google/genai";
import { dispatch, toolDeclarations } from "../tools";
import { createRun, finishRun, getRun, recordStep, saveState } from "@/db/client";

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
const MODEL = "gemini-3.6-flash";
const MAX_PASSES = 10;

const tools = [{ functionDeclarations: toolDeclarations }];

const SYSTEM_INSTRUCTION =
    "You are a code investigator working on an unfamiliar codebase. " +
    "Start with list_files to see what exists. Use grep_code to locate an " +
    "identifier or error message. Use read_file to see actual code. " +
    "You may only describe or quote a file after reading it with read_file in " +
    "this conversation. Never reconstruct code from memory or infer it from a " +
    "filename. If a tool returns an error, say so plainly and try a different " +
    "approach rather than guessing at the contents.";

export type RunAgentOptions = {
    resumeId?: string;
};

export async function runAgent(
    task: string,
    options: RunAgentOptions = {},
): Promise<string> {
    const { resumeId } = options;

    let answer = "(the loop ended without producing an answer)";
    const contents: Content[] = [];
    let stepNumber = 0;
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

        console.log(
            `resuming run ${runId} — ${contents.length} turns, ${stepNumber} steps done`,
        );
    } else {
        runId = await createRun(task);
        contents.push({ role: "user", parts: [{ text: task }] });
        console.log(`run ${runId}`);
    }

    for (let pass = 1; pass <= MAX_PASSES; pass++) {
        const modelStartedAt = Date.now();
        const res = await ai.models.generateContent({
            model: MODEL,
            contents,
            config: { tools, systemInstruction: SYSTEM_INSTRUCTION },
        });
        const modelDurationMs = Date.now() - modelStartedAt;

        const modelTurn = res.candidates?.[0]?.content;
        if (!modelTurn) throw new Error("No content came back from the model.");

        const usage = res.usageMetadata;
        const tokensIn = usage?.promptTokenCount;
        const tokensOut =
            (usage?.candidatesTokenCount ?? 0) + (usage?.thoughtsTokenCount ?? 0);

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
            if (part.functionCall) {
                calls.push(part.functionCall);
            }
        }

        if (calls.length === 0) {
            answer = res.text ?? "(model returned no text)";
            await saveState(runId, contents, stepNumber);
            break;
        }

        const resultParts: Part[] = [];
        for (const call of calls) {
            const args = (call.args ?? {}) as Record<string, unknown>;

            const toolStartedAt = Date.now();
            const result = await dispatch(call.name, args);
            const toolDurationMs = Date.now() - toolStartedAt;

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
                functionResponse: {
                    id: call.id,
                    name: call.name,
                    response: response,
                },
            });
        }

        contents.push({ role: "user", parts: resultParts });
        await saveState(runId, contents, stepNumber);
    }

    await finishRun(runId, "completed", answer);
    return answer;
}
