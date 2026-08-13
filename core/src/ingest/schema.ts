import { z } from "zod";

// Ported from alert_ingestion_api/schemas.py (pydantic BaseModel).
// All four fields were required `str` with no defaults.
export const alertSchema = z.object({
  incident_id: z.string(),
  title: z.string(),
  service: z.string(),
  description: z.string(),
});

export type Alert = z.infer<typeof alertSchema>;
