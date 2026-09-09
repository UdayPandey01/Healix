import { prisma } from "@/lib/prisma";
import { chunkRepo, type Chunk } from "./chunk";
import { embedDocuments, toVectorLiteral } from "./embed";

export type IndexResult = { repo: string; chunks: number; embedded: number };

export async function indexRepo(root: string, repo: string): Promise<IndexResult> {
    const chunks = await chunkRepo(root);

    if (chunks.length === 0) {
        return { repo, chunks: 0, embedded: 0 };
    }

    const vectors = await embedDocuments(chunks.map(embeddableText));

    await prisma.$executeRaw`DELETE FROM code_chunks WHERE repo = ${repo}`;

    let embedded = 0;
    for (const [i, chunk] of chunks.entries()) {
        const vector = vectors[i];
        if (!vector) continue;

        await prisma.$executeRaw`
            INSERT INTO code_chunks
                (id, repo, path, symbol, kind, start_line, end_line, content, tokens, embedding)
            VALUES (
                gen_random_uuid(), ${repo}, ${chunk.path}, ${chunk.symbol}, ${chunk.kind},
                ${chunk.startLine}, ${chunk.endLine}, ${chunk.content}, ${chunk.tokens},
                ${toVectorLiteral(vector)}::vector
            )
            ON CONFLICT (repo, path, start_line, end_line) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                kind = EXCLUDED.kind,
                content = EXCLUDED.content,
                tokens = EXCLUDED.tokens,
                embedding = EXCLUDED.embedding
        `;
        embedded++;
    }

    return { repo, chunks: chunks.length, embedded };
}

function embeddableText(chunk: Chunk): string {
    const header = [chunk.path, chunk.symbol ?? "(module scope)"].join(" · ");
    return `${header}\n\n${chunk.content}`;
}
