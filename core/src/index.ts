import "dotenv/config";

import { buildApp } from "@/app";
import { startSupervisor } from "@/agent/supervisor";

const app = await buildApp();

startSupervisor();

const port = Number(process.env.PORT ?? 8000);
const host = process.env.HOST ?? "0.0.0.0";

try {
    await app.listen({ port, host });
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
