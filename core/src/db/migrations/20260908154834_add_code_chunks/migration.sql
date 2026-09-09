-- pgvector must exist before the vector column below.
CREATE EXTENSION IF NOT EXISTS vector;

-- CreateTable
CREATE TABLE "code_chunks" (
    "id" UUID NOT NULL,
    "repo" TEXT NOT NULL,
    "path" TEXT NOT NULL,
    "symbol" TEXT,
    "kind" TEXT NOT NULL,
    "start_line" INTEGER NOT NULL,
    "end_line" INTEGER NOT NULL,
    "content" TEXT NOT NULL,
    "tokens" INTEGER NOT NULL,
    "embedding" vector(768),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "code_chunks_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "code_chunks_repo_path_idx" ON "code_chunks"("repo", "path");

-- CreateIndex
CREATE UNIQUE INDEX "code_chunks_repo_path_start_line_end_line_key" ON "code_chunks"("repo", "path", "start_line", "end_line");

-- No HNSW index and no GIN full-text index, deliberately. Both are approximate or
-- maintenance-heavy structures that only pay off past roughly 10k rows; demo-service
-- indexes to a couple of hundred chunks, where an exact sequential scan is faster AND
-- more accurate than an approximate nearest-neighbour probe. Add
--   CREATE INDEX ON code_chunks USING hnsw (embedding vector_cosine_ops);
--   CREATE INDEX ON code_chunks USING gin (to_tsvector('english', content));
-- when the corpus grows enough for scan time to show up in the retrieval latency.
