import "dotenv/config";
import { runAgent } from "./run";

const task =
    "The test suite is failing. Run the tests, read the failures, then find the " +
    "root cause in the source and name the exact file and line for each failure.";

const resumeId = process.argv[2];

const answer = await runAgent(task, { resumeId });

console.log("\n--- ANSWER ---");
console.log(answer);
