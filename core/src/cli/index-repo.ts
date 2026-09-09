import "dotenv/config";

import { prisma } from "@/lib/prisma";
import { indexRepo } from "@/retrieval/index";
import { WORKSPACE_ROOT } from "@/tools/workspace";

const root = process.argv[2] ?? WORKSPACE_ROOT;
const name = process.argv[3] ?? process.env.INDEXED_REPO ?? "demo-service";

console.log(`indexing ${root} as "${name}"…`);

const started = Date.now();
const result = await indexRepo(root, name);
const seconds = ((Date.now() - started) / 1000).toFixed(1);

console.log(
    `done in ${seconds}s — ${result.chunks} chunks, ${result.embedded} embedded.\n` +
        `search_code will now search "${name}". Re-run this whenever the repo changes.`,
);

await prisma.$disconnect();
