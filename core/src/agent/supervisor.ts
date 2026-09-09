import { findStaleRuns, recordResumeFailure } from "@/db/client";
import { runAgent } from "./run";

const STALE_AFTER_MS = Number(process.env.SUPERVISOR_STALE_MS ?? 10 * 60 * 1000);
const SWEEP_INTERVAL_MS = Number(process.env.SUPERVISOR_INTERVAL_MS ?? 5 * 60 * 1000);
const MAX_PER_SWEEP = 5;
const MAX_RESUME_ATTEMPTS = Number(process.env.SUPERVISOR_MAX_ATTEMPTS ?? 3);

const PERMANENT = new Set([400, 401, 403, 404, 422]);

function isPermanent(err: unknown): boolean {
    const status = (err as { status?: number }).status;
    return status !== undefined && PERMANENT.has(status);
}

export async function sweepOrphanedRuns(): Promise<{ found: number; resumed: string[] }> {
    const stale = await findStaleRuns(STALE_AFTER_MS, MAX_PER_SWEEP, MAX_RESUME_ATTEMPTS);
    const resumed: string[] = [];

    for (const run of stale) {
        try {
            console.log(
                `supervisor: resuming orphaned run ${run.id} ` +
                    `(idle since ${run.updatedAt.toISOString()}, ${run.stepCount} steps done)`,
            );
            await runAgent("", { resumeId: run.id });
            resumed.push(run.id);
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            const permanent = isPermanent(err);
            const givenUp = await recordResumeFailure(
                run.id,
                message,
                permanent,
                MAX_RESUME_ATTEMPTS,
            );

            console.error(
                `supervisor: run ${run.id} failed to resume` +
                    `${permanent ? " (permanent)" : ""}` +
                    `${givenUp ? " — marked failed, will not retry" : ""}: ` +
                    message.split("\n")[0],
            );
        }
    }

    return { found: stale.length, resumed };
}

export function startSupervisor(): NodeJS.Timeout {
    const timer = setInterval(() => {
        void sweepOrphanedRuns().catch((err) => console.error("supervisor sweep failed", err));
    }, SWEEP_INTERVAL_MS);

    timer.unref();
    return timer;
}
