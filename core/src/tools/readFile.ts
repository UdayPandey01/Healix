import {readFile} from "node:fs/promises";

export async function readFileTool(path: string) : Promise<string> {
    try {
        const content = await readFile(path, { encoding: "utf-8" });
        return content;
    }catch (error) {
        console.error(`Error reading file at ${path}:`, error);
        throw new Error(`Failed to read file at ${path}`);
    }
}