-- CreateTable
CREATE TABLE "runs" (
    "id" UUID NOT NULL,
    "task" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'running',
    "messages" JSONB NOT NULL DEFAULT '[]',
    "step_count" INTEGER NOT NULL DEFAULT 0,
    "diagnosis" TEXT,
    "error" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "runs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "steps" (
    "id" UUID NOT NULL,
    "run_id" UUID NOT NULL,
    "step_number" INTEGER NOT NULL,
    "type" TEXT NOT NULL,
    "tool_name" TEXT,
    "input" JSONB,
    "output" JSONB,
    "tokens_in" INTEGER,
    "tokens_out" INTEGER,
    "duration_ms" INTEGER,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "steps_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "steps_run_id_step_number_idx" ON "steps"("run_id", "step_number");

-- AddForeignKey
ALTER TABLE "steps" ADD CONSTRAINT "steps_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "runs"("id") ON DELETE CASCADE ON UPDATE CASCADE;
