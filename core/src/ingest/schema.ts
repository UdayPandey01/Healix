import { z } from "zod";

export const alertSchema = z.object({
  incident_id: z.string(),
  title: z.string(),
  service: z.string(),
  description: z.string(),
});

export type Alert = z.infer<typeof alertSchema>;
