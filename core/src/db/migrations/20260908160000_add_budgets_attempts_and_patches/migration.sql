-- AlterTable
ALTER TABLE "runs" ADD COLUMN     "halt_reason" TEXT,
ADD COLUMN     "tokens_used" INTEGER NOT NULL DEFAULT 0;

-- AlterTable
ALTER TABLE "steps" ADD COLUMN     "attempt" INTEGER NOT NULL DEFAULT 1;

-- CreateTable
CREATE TABLE "patches" (
    "id" UUID NOT NULL,
    "run_id" UUID NOT NULL,
    "title" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    "files" JSONB NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "pr_url" TEXT,
    "pr_number" INTEGER,
    "reviewed_at" TIMESTAMP(3),
    "review_note" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "patches_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "patches_run_id_key" ON "patches"("run_id");

-- CreateIndex
CREATE INDEX "patches_status_idx" ON "patches"("status");

-- CreateIndex
CREATE INDEX "runs_status_updated_at_idx" ON "runs"("status", "updated_at");


-- Backfill before the unique index below can be created.
--
-- Failure mode #4: a crash between recordStep and saveState leaves step rows the saved
-- conversation has no record of, so a resumed run restarts its numbering and writes a
-- second row with the same step_number. Those duplicates already exist in this table.
-- Numbering them by insertion order turns each collision into attempt 1, 2, ... rather
-- than deleting history — for a project whose claim is durability, evidence that crash
-- recovery happened is worth keeping.
WITH numbered AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY run_id, step_number
               ORDER BY created_at, id
           ) AS n
    FROM steps
)
UPDATE steps
SET attempt = numbered.n
FROM numbered
WHERE steps.id = numbered.id;

-- CreateIndex
CREATE UNIQUE INDEX "steps_run_id_step_number_attempt_key" ON "steps"("run_id", "step_number", "attempt");

-- AddForeignKey
ALTER TABLE "patches" ADD CONSTRAINT "patches_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "runs"("id") ON DELETE CASCADE ON UPDATE CASCADE;

