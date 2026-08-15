import "dotenv/config";
import { GoogleGenAI, Type, type Content } from "@google/genai";
import { readFileTool } from "../tools/readFile";

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
const MODEL = "gemini-3.6-flash";

const readFileDeclaration = {
  name: "read_file",
  description:
    "Read the content of a source file from the repository. Use this whenever " +
    "you need to see actual code rather than guessing at it.",
  parameters: {
    type: Type.OBJECT,
    properties: {
      path: {
        type: Type.STRING,
        description: "Path relative to the repo root, e.g. src/ingest/routes.ts",
      },
    },
    required: ["path"],
  },
};

const tools = [{ functionDeclarations: [readFileDeclaration] }];

const question =
  "In src/ingest/routes.ts, what HTTP status code is returned when the " +
  "Authorization header is missing?";

const contents: Content[] = [{ role: "user", parts: [{ text: question }] }];

const first = await ai.models.generateContent({
  model: MODEL,
  contents,
  config: { tools },
});

const modelTurn = first.candidates?.[0]?.content;
if (!modelTurn) throw new Error("No content came back from the model.");

const call = modelTurn.parts?.find((p) => p.functionCall)?.functionCall;

if (!call) {
  console.log("Model answered directly, no tool call:\n", first.text);
  process.exit(0);
}

console.log(`Model asked for: ${call.name}(${JSON.stringify(call.args)})`);

const path = String(call.args?.["path"] ?? "");
const fileContents = await readFileTool(path);
console.log(`Read ${fileContents.length} characters from ${path}`);

contents.push(modelTurn);

contents.push({
  role: "user",
  parts: [
    {
      functionResponse: {
        id: call.id,
        name: call.name,
        response: { output: fileContents },
      },
    },
  ],
});

const second = await ai.models.generateContent({
  model: MODEL,
  contents,
  config: { tools },
});

console.log("\n--- ANSWER ---");
console.log(second.text);
