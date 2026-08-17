import {prisma} from '../lib/prisma';
import { Prisma } from '../generated/prisma/client';

export async function createRun(task : string) {
    const run = await prisma.run.create({
        data: {
            task,
            messages : [{
                role : 'user',
                parts : [{ text : task }]
            }] as Prisma.InputJsonValue[],
        }
    })
    return run.id;
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
    await prisma.step.create({
        data : {
            runId : s.runId,
            stepNumber : s.stepNumber,
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
            steps : {
                orderBy : {
                    stepNumber : 'asc'
                }
            }
        }
    })
}