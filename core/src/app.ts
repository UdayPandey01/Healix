import "dotenv/config";
import Fastify from "fastify";

import { ingestRoutes } from "@/ingest/routes";
import { apiRoutes } from "@/api/routes";

export async function buildApp(opts: { logger?: boolean } = {}) {
    const app = Fastify({ logger: opts.logger ?? true });

    app.get("/health", async () => ({ status: "ok" }));

    await app.register(ingestRoutes);
    await app.register(apiRoutes);

    return app;
}
