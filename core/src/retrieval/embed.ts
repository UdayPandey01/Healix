import "dotenv/config";
import { GoogleGenAI } from "@google/genai";

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

export const EMBEDDING_MODEL = "gemini-embedding-001";

export const EMBEDDING_DIMENSIONS = 768;

type TaskType = "RETRIEVAL_DOCUMENT" | "RETRIEVAL_QUERY";

const BATCH_SIZE = 32;

function normalise(vector: number[]): number[] {
    let sumOfSquares = 0;
    for (const value of vector) sumOfSquares += value * value;

    const magnitude = Math.sqrt(sumOfSquares);
    if (magnitude === 0) return vector;

    return vector.map((value) => value / magnitude);
}

async function embedBatch(texts: string[], taskType: TaskType): Promise<number[][]> {
    const res = await ai.models.embedContent({
        model: EMBEDDING_MODEL,
        contents: texts,
        config: { taskType, outputDimensionality: EMBEDDING_DIMENSIONS },
    });

    const embeddings = res.embeddings ?? [];
    if (embeddings.length !== texts.length) {
        throw new Error(
            `embedding count mismatch: asked for ${texts.length}, got ${embeddings.length}`,
        );
    }

    return embeddings.map((e) => normalise(e.values ?? []));
}

export async function embedDocuments(texts: string[]): Promise<number[][]> {
    const out: number[][] = [];

    for (let i = 0; i < texts.length; i += BATCH_SIZE) {
        out.push(...(await embedBatch(texts.slice(i, i + BATCH_SIZE), "RETRIEVAL_DOCUMENT")));
    }

    return out;
}

export async function embedQuery(text: string): Promise<number[]> {
    const [vector] = await embedBatch([text], "RETRIEVAL_QUERY");
    if (!vector) throw new Error("no embedding returned for query");
    return vector;
}

export function toVectorLiteral(vector: number[]): string {
    return `[${vector.join(",")}]`;
}
