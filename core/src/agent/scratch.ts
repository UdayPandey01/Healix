import "dotenv/config";
import { runAgent } from "./run";

const task =
    "GET /users/999/summary returns a 500 instead of a 404. Find the root cause " +
    "in the code and explain exactly which line is wrong and why.";

const resumeId = process.argv[2];

const answer = await runAgent(task, { resumeId });

console.log("\n--- ANSWER ---");
console.log(answer);
