import "dotenv/config";
import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "src/db/schema.prisma",
  migrations: {
    path: "src/db/migrations",
  },
  datasource: {
    // Migrations must run over a DIRECT (unpooled) connection. Neon's pooled
    // `-pooler` endpoint runs PgBouncer in transaction mode, which breaks the
    // advisory locks and session state Prisma Migrate relies on.
    // Runtime queries in src/db/client.ts still use DATABASE_URL (pooled).
    url: process.env["DIRECT_URL"] ?? process.env["DATABASE_URL"],
  },
});
