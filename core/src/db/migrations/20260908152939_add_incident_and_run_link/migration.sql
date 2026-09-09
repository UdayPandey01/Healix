-- AlterTable
ALTER TABLE "runs" ADD COLUMN     "incident_id" UUID;

-- CreateTable
CREATE TABLE "incidents" (
    "id" UUID NOT NULL,
    "fingerprint" TEXT NOT NULL,
    "service" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "incidents_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "incidents_fingerprint_key" ON "incidents"("fingerprint");

-- CreateIndex
CREATE INDEX "runs_incident_id_status_idx" ON "runs"("incident_id", "status");

-- AddForeignKey
ALTER TABLE "runs" ADD CONSTRAINT "runs_incident_id_fkey" FOREIGN KEY ("incident_id") REFERENCES "incidents"("id") ON DELETE SET NULL ON UPDATE CASCADE;
