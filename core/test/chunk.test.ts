import test from "node:test";
import assert from "node:assert/strict";

import { chunkSource } from "../src/retrieval/chunk";

test("splits on top-level declarations, one chunk each", () => {
    const source = [
        'import { a } from "./a.js";',
        "",
        "export function first(x) {",
        "  return x + 1;",
        "}",
        "",
        "export function second(y) {",
        "  return y * 2;",
        "}",
    ].join("\n");

    const chunks = chunkSource("src/x.js", source);

    assert.deepEqual(
        chunks.map((c) => c.symbol),
        [null, "first", "second"],
    );
    assert.equal(chunks[0]!.kind, "module");
    assert.ok(chunks[1]!.content.includes("return x + 1"));
    assert.ok(!chunks[1]!.content.includes("return y * 2"), "chunks must not overlap");
});

test("a nested function does not start a new chunk", () => {
    const source = [
        "export function outer() {",
        "  function inner() {",
        "    return 1;",
        "  }",
        "  return inner();",
        "}",
    ].join("\n");

    const chunks = chunkSource("src/x.js", source);

    assert.equal(chunks.length, 1);
    assert.equal(chunks[0]!.symbol, "outer");
    assert.ok(chunks[0]!.content.includes("inner"));
});

test("leading comments attach to the declaration below them", () => {
    const source = [
        "const CONFIG = 1;",
        "",
        "// This explains what the function does.",
        "export function documented() {",
        "  return CONFIG;",
        "}",
    ].join("\n");

    const chunks = chunkSource("src/x.js", source);
    const documented = chunks.find((c) => c.symbol === "documented");

    assert.ok(documented, "documented chunk missing");
    assert.ok(
        documented.content.includes("This explains what the function does"),
        "the comment was orphaned into the previous chunk",
    );
});

test("line ranges point at the real location in the file", () => {
    const source = ["const A = 1;", "", "export function target() {", "  return A;", "}"].join(
        "\n",
    );

    const chunks = chunkSource("src/x.js", source);
    const target = chunks.find((c) => c.symbol === "target")!;
    const lines = source.split("\n");

    assert.equal(lines[target.startLine - 1], "export function target() {");
    assert.equal(target.endLine, 5);
});

test("arrow-function consts and classes are recognised", () => {
    const source = [
        "export const handler = async (req) => {",
        "  return 1;",
        "};",
        "",
        "class Widget {",
        "  render() {}",
        "}",
    ].join("\n");

    const chunks = chunkSource("src/x.js", source);

    assert.deepEqual(
        chunks.map((c) => c.symbol),
        ["handler", "Widget"],
    );
    assert.equal(chunks.find((c) => c.symbol === "Widget")!.kind, "class");
});

test("a file with no declarations yields one module chunk", () => {
    const chunks = chunkSource("src/config.js", "export default { a: 1 };\n");
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0]!.kind, "module");
});

test("an empty file yields nothing", () => {
    assert.deepEqual(chunkSource("src/empty.js", "\n\n"), []);
});
