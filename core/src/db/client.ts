import {prisma} from '../lib/prisma';
import { Prisma } from '../generated/prisma/client';

export async function createRun(task : string, incidentId? : string) {
    const run = await prisma.run.create({
        data: {
            task,
            incidentId : incidentId ?? null,
            messages : [{
                role : 'user',
                parts : [{ text : task }]
            }] as Prisma.InputJsonValue[],
        }
    })
    return run.id;
}

export async function upsertIncident(i : {
    fingerprint : string;
    service : string;
    title : string;
    description : string;
    payload : unknown;
}) {
    const incident = await prisma.incident.upsert({
        where : { fingerprint : i.fingerprint },
        create : {
            fingerprint : i.fingerprint,
            service : i.service,
            title : i.title,
            description : i.description,
            payload : i.payload as Prisma.InputJsonValue
        },
        update : {
            service : i.service,
            title : i.title,
            description : i.description,
            payload : i.payload as Prisma.InputJsonValue
        }
    })
    return incident.id;
}

export async function findActiveRun(incidentId : string) {
    const run = await prisma.run.findFirst({
        where : { incidentId, status : 'running' },
        orderBy : { createdAt : 'desc' },
        select : { id : true }
    })
    return run?.id ?? null;
}

export async function listRuns(limit : number, offset : number) {
    return await prisma.run.findMany({
        orderBy : { createdAt : 'desc' },
        take : limit,
        skip : offset,
        select : {
            id : true,
            task : true,
            status : true,
            stepCount : true,
            diagnosis : true,
            error : true,
            createdAt : true,
            updatedAt : true,
            tokensUsed : true,
            haltReason : true,
            incident : {
                select : { id : true, fingerprint : true, service : true, title : true }
            },
            patch : {
                select : { id : true, title : true, status : true, prUrl : true, prNumber : true }
            }
        }
    })
}

export async function countRuns() {
    return await prisma.run.count();
}

export async function saveState(runId : string, message : unknown[], stepCount : number) {
    await prisma.run.update({
        where : {
            id : runId
        },
        data : {
            messages : message as Prisma.InputJsonValue[],
            stepCount
        }
    })
}

export async function finishRun(runId : string, status : string, diagnosis? : string, error? : string) {
    await prisma.run.update({
        where : {
            id : runId
        },
        data : {
            status,
            diagnosis : diagnosis ?? null,
            error : error ?? null
        }
    })
}

export async function recordStep(s : {
    runId : string;
    stepNumber : number;
    type : string;
    toolName? : string;
    input? : unknown;
    output? : unknown;
    tokensIn? : number;
    tokensOut? : number;
    durationMs? : number;
}) {
    const priorAttempts = await prisma.step.count({
        where : { runId : s.runId, stepNumber : s.stepNumber }
    })

    await prisma.step.create({
        data : {
            runId : s.runId,
            stepNumber : s.stepNumber,
            attempt : priorAttempts + 1,
            type : s.type,
            toolName : s.toolName ?? null,
            input : s.input as Prisma.InputJsonValue ?? null,
            output : s.output as Prisma.InputJsonValue ?? null,
            tokensIn : s.tokensIn ?? null,
            tokensOut : s.tokensOut ?? null,
            durationMs : s.durationMs ?? null
        }
    })
}

export async function getRun(runId : string) {
    return await prisma.run.findUnique({
        where : {
            id : runId
        },
        include : {
            incident : true,
            patch : true,
            steps : {
                orderBy : {
                    stepNumber : 'asc'
                }
            }
        }
    })
}

export async function addTokens(runId : string, tokens : number) {
    const run = await prisma.run.update({
        where : { id : runId },
        data : { tokensUsed : { increment : tokens } },
        select : { tokensUsed : true }
    })
    return run.tokensUsed;
}

export async function haltRun(runId : string, status : string, haltReason : string, diagnosis? : string) {
    await prisma.run.update({
        where : { id : runId },
        data : { status, haltReason, diagnosis : diagnosis ?? null }
    })
}

export async function savePatch(p : {
    runId : string;
    title : string;
    body : string;
    files : unknown;
}) {
    const patch = await prisma.patch.upsert({
        where : { runId : p.runId },
        create : {
            runId : p.runId,
            title : p.title,
            body : p.body,
            files : p.files as Prisma.InputJsonValue
        },
        update : {
            title : p.title,
            body : p.body,
            files : p.files as Prisma.InputJsonValue,
            status : 'pending',
            reviewedAt : null,
            reviewNote : null
        }
    })
    return patch.id;
}

export async function getPatch(runId : string) {
    return await prisma.patch.findUnique({ where : { runId } });
}

export async function markPatch(runId : string, data : {
    status : string;
    reviewNote? : string;
    prUrl? : string;
    prNumber? : number;
}) {
    return await prisma.patch.update({
        where : { runId },
        data : {
            status : data.status,
            reviewNote : data.reviewNote ?? null,
            prUrl : data.prUrl ?? null,
            prNumber : data.prNumber ?? null,
            reviewedAt : new Date()
        }
    })
}

export async function findStaleRuns(olderThanMs : number, limit = 20, maxAttempts = 3) {
    return await prisma.run.findMany({
        where : {
            status : 'running',
            updatedAt : { lt : new Date(Date.now() - olderThanMs) },

            resumeAttempts : { lt : maxAttempts }
        },
        orderBy : { updatedAt : 'asc' },
        take : limit,
        select : { id : true, updatedAt : true, stepCount : true, resumeAttempts : true }
    })
}

export async function recordResumeFailure(
    runId : string,
    error : string,
    permanent : boolean,
    maxAttempts = 3,
) {
    const run = await prisma.run.update({
        where : { id : runId },
        data : { resumeAttempts : { increment : 1 } },
        select : { resumeAttempts : true }
    })

    if (permanent || run.resumeAttempts >= maxAttempts) {
        await prisma.run.update({
            where : { id : runId },
            data : {
                status : 'failed',
                error,
                haltReason : permanent
                    ? 'resume failed permanently'
                    : `resume failed ${run.resumeAttempts} times`
            }
        })
        return true;
    }

    return false;
}
