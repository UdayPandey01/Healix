import { z } from "zod";
import type { Alert } from "@/ingest/schema";

export const alertmanagerSchema = z.object({
    status: z.string(),
    alerts: z.array(
        z.object({
            status: z.string(),
            fingerprint: z.string().optional(),
            labels: z.record(z.string(), z.string()).default({}),
            annotations: z.record(z.string(), z.string()).default({}),
            startsAt: z.string().optional(),
        }),
    ),
});

export type AlertmanagerPayload = z.infer<typeof alertmanagerSchema>;

export function normalize(payload: AlertmanagerPayload): Alert[] {
    return payload.alerts
        .filter((a) => a.status === "firing")
        .map((a) => ({
            incident_id: a.fingerprint ?? `${a.labels["alertname"] ?? "alert"}-${a.startsAt ?? ""}`,
            title: a.annotations["summary"] ?? a.labels["alertname"] ?? "Unnamed alert",
            service: a.labels["service"] ?? a.labels["job"] ?? "unknown",
            description: a.annotations["description"] ?? "No description supplied.",
        }));
}
