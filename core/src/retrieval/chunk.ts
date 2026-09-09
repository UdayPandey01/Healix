import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";

export type Chunk = {
    path: string;
    symbol: string | null;
    kind: "function" | "class" | "module";
    startLine: number;
    endLine: number;
    content: string;
    tokens: number;
};

const IGNORED = new Set(["node_modules", ".git", "dist", ".next", "coverage", "test"]);
const SOURCE_EXTENSIONS = [".js", ".mjs", ".ts", ".jsx", ".tsx"];

const DECLARATION =
    /^(?:export\s+)?(?:default\s+)?(?:async\s+)?(?:function\s*\*?\s+(\w+)|class\s+(\w+)|(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s*)?(?:function|\([^)]*\)\s*=>|\w+\s*=>))/;

function estimateTokens(text: string): number {
    return Math.ceil(text.length / 4);
}

export function chunkSource(path: string, source: string): Chunk[] {
    const lines = source.split("\n");
    const starts: { line: number; symbol: string | null; kind: Chunk["kind"] }[] = [];

    for (let i = 0; i < lines.length; i++) {
        const match = DECLARATION.exec(lines[i] ?? "");
        if (!match) continue;

        const symbol = match[1] ?? match[2] ?? match[3] ?? null;
        const kind: Chunk["kind"] = match[2] ? "class" : "function";

        starts.push({ line: claimLeadingComments(lines, i), symbol, kind });
    }

    if (starts.length === 0) {
        const content = source.trim();
        if (content === "") return [];
        return [
            {
                path,
                symbol: null,
                kind: "module",
                startLine: 1,
                endLine: lines.length,
                content: source,
                tokens: estimateTokens(source),
            },
        ];
    }

    const chunks: Chunk[] = [];

    const preamble = lines.slice(0, starts[0]!.line).join("\n");
    if (preamble.trim() !== "") {
        chunks.push({
            path,
            symbol: null,
            kind: "module",
            startLine: 1,
            endLine: starts[0]!.line,
            content: preamble,
            tokens: estimateTokens(preamble),
        });
    }

    for (let s = 0; s < starts.length; s++) {
        const start = starts[s]!;
        const end = s + 1 < starts.length ? starts[s + 1]!.line : lines.length;
        const content = lines.slice(start.line, end).join("\n");

        if (content.trim() === "") continue;

        chunks.push({
            path,
            symbol: start.symbol,
            kind: start.kind,
            startLine: start.line + 1,
            endLine: end,
            content,
            tokens: estimateTokens(content),
        });
    }

    return chunks;
}

function claimLeadingComments(lines: string[], declarationLine: number): number {
    let start = declarationLine;

    while (start > 0) {
        const previous = (lines[start - 1] ?? "").trim();
        const isComment =
            previous.startsWith("//") ||
            previous.startsWith("*") ||
            previous.startsWith("/*") ||
            previous.endsWith("*/");

        if (!isComment) break;
        start--;
    }

    return start;
}

export async function chunkRepo(root: string): Promise<Chunk[]> {
    const files = await walk(root, root);
    const chunks: Chunk[] = [];

    for (const file of files) {
        const source = await readFile(join(root, file), { encoding: "utf-8" });
        chunks.push(...chunkSource(file, source));
    }

    return chunks;
}

async function walk(dir: string, root: string): Promise<string[]> {
    const entries = await readdir(dir, { withFileTypes: true });
    const found: string[] = [];

    for (const entry of entries) {
        if (IGNORED.has(entry.name)) continue;
        const full = join(dir, entry.name);

        if (entry.isDirectory()) {
            found.push(...(await walk(full, root)));
        } else if (SOURCE_EXTENSIONS.some((ext) => entry.name.endsWith(ext))) {
            found.push(relative(root, full));
        }
    }

    return found;
}
