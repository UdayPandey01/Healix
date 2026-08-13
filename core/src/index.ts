import "dotenv/config";
import Fastify from "fastify";

import { ingestRoutes } from "@/ingest/routes";

const app = Fastify({ logger: true });

app.get("/health", async () => ({ status: "ok" }));

await app.register(ingestRoutes);

const port = Number(process.env.PORT ?? 8000);
const host = process.env.HOST ?? "0.0.0.0";

try {
  await app.listen({ port, host });
} catch (err) {
  app.log.error(err);
  process.exit(1);
}
