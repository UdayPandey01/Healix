# Healix — Design & Working Notes

> Living document. Part 1 is the stable design of the system. Part 2 onward is a
> running log: every explanation gets appended here as we go, newest section last.
> The purpose is to be able to defend every decision in this repo four levels deep.

---

# PART 1 — THE DESIGN

## 1. The problem

Observability solved **monitoring**. Prometheus, Datadog, and Sentry are very good at
telling a human that something broke. They stop there. The expensive part — a human
paging in at 2am, sifting logs, locating the broken code, writing a fix under time
pressure — is untouched.

Healix targets **remediation**, not monitoring. It catches the alert, investigates root
cause at the source-code level, writes a patch, verifies it in an isolated sandbox, and
opens a Pull Request. Resolution goes from a multi-hour human scramble to a minutes-long
automated pipeline with a human approving at the end.

Healix **never auto-merges.** A human is always the last gate. This is a product decision
and a safety decision: the value is in collapsing investigation time, not in removing
review.

## 2. Scope: what v1 is, and what it deliberately is not

v1 is **single-tenant, against my own demo repo.** That is a deliberate constraint, not a
limitation I ran out of time to fix.

**In scope**

- Durable agent runtime (the technical core)
- Alert webhook ingestion → Postgres
- Code retrieval over the repo via pgvector
- Sandboxed test execution in Rust
- Patch generation, verified against the real test suite
- Real PRs opened via GitHub PAT
- Eval harness with measured, published results

**Deliberately out of scope** — these live in the README roadmap with written
justification, not as unfinished code:

| Excluded | Why |
|---|---|
| Multi-tenancy | 4–6 weeks of work, adds zero agent capability |
| Kafka ingestion buffer | Built for thousands of external tenants that don't exist. A webhook and a Postgres row is the correct design at this scale |
| Qdrant | pgvector handles ~5k chunks fine and Postgres is already running |
| GitHub App + installation flow | A PAT is sufficient for one repo |
| Developer portal, API keys, org linking | Multi-tenant concerns |
| Slack Block Kit | Presentation layer, not capability |
| Language-agnostic test running | Node only in v1 |

The general principle: **infrastructure that exists to serve scale I don't have is not
evidence of engineering judgment — it's evidence of the opposite.** Being able to explain
why Kafka is *wrong* here is a stronger interview answer than having built it.

## 3. The technical core

**This is a durable agent runtime, not a chatbot.**

The defining property: *an agent run is a state machine persisted to Postgres after every
single step.* Kill the process mid-run, restart it, and it resumes from the last completed
step. Everything else in the system is arranged around that guarantee.

**No LangChain, no LangGraph.** If a framework owns the state machine, I did not write the
state machine — and the state machine is the entire technical claim of this project. This
extends to the Anthropic SDK's own `client.beta.messages.tool_runner()`, which drives the
tool-call loop for you. It is deliberately unused here for the same reason.

## 4. Architecture

**A modular monolith plus one isolated Rust sandbox.**

```
healix/
├── core/                  TypeScript, Fastify, Prisma, Postgres
│   └── src/
│       ├── agent/         loop + state machine
│       ├── tools/         schemas + dispatch
│       ├── ingest/        alert webhook, normalization
│       ├── retrieval/     code indexing, pgvector
│       ├── telemetry/     OpenTelemetry, Prometheus
│       ├── db/            Prisma schema and queries
│       └── api/           Fastify routes
├── sandbox/               Rust, isolated
├── demo-service/          deliberately breakable Express app
├── evals/                 golden bug set + harness
└── observability/         Prometheus/Grafana config
```

This started as 8 microservices and was consolidated. Coordination cost — network hops,
independent deploys, distributed failure modes, 8 Dockerfiles — was not justified for a
single-node system. Consolidating *down* is the defensible direction; it demonstrates
that the split was evaluated rather than cargo-culted.

**The sandbox is the one exception, and it is not a scaling decision.** It is a separate
process because it executes model-generated code. That is a security boundary. Rust is
chosen for the resource ceilings and timeout control that boundary requires.

### Dependency direction

```
api / ingest  →  agent  →  tools  →  retrieval / sandbox

db  ←  called from anywhere, calls nothing
```

One direction, no cycles. `db/` is a leaf: everything may call it, it calls nothing. This
is what keeps the monolith modular rather than merely co-located — the module boundaries
are enforced by the dependency graph, so the thing can be split later if it ever needs to
be.

## 5. The flow

1. **TRIGGER** — `demo-service` throws 500s; Prometheus fires a webhook.
2. **PERSIST** — `ingest` normalizes the payload, writes an incident row, creates a run.
   *Durable before any work starts.* Nothing is attempted until it survives a crash.
3. **INVESTIGATE** — the agent loop runs: query logs, search code via pgvector, read
   candidate files, form a hypothesis.
4. **QUARANTINE** — the model writes a patch; the Rust sandbox runs the test suite in
   isolation with timeouts and resource caps. Tests fail → the agent retries with the
   failure output as context.
5. **RESOLVE** — tests pass, a PR opens. A human merges.

**Agent tools:** `query_logs`, `search_code`, `read_file`, `run_tests`, `open_pr`.

Note step 2. Persisting *before* any work begins is what makes the durability claim real
rather than aspirational — there is no window where work is in flight but unrecorded.

## 6. Retrieval design (Week 4)

Chunk on **function and class boundaries**, not fixed size — a function split in half is
useless as context. Embed into pgvector with metadata: file path, line range, symbol name.

The query is constructed from **stack trace symbols, error message, and recent commit
diffs** — not the raw alert text. The raw alert is written for humans; the retrieval query
should be written for the index.

**Hybrid search:** BM25 over identifiers + dense vectors over semantics, fused with RRF.
Pure dense retrieval is weak on code because identifiers matter *literally* —
`getUserOrders` and `fetchCustomerPurchases` are semantically close and functionally
unrelated. Rerank top-20 → top-5.

Measured with **recall@5** against the seeded bug set.

## 7. Evals are the point

20 seeded bugs in `demo-service`: unhandled null, unbounded cache leak, missing timeout,
connection pool exhaustion, off-by-one in pagination.

**Measured:** diagnosis accuracy, patch-passes-tests rate, retrieval recall@5, cost per
incident, steps per incident — plus a written catalogue of every observed failure mode.

Most agent projects have no evals. Having them is the difference between a demo and a
project. The failure catalogue in particular is what makes it credible: it proves the
system was actually run enough times to characterize how it breaks.

## 8. Stack

TypeScript · Fastify · Prisma · Postgres + pgvector · Google Gemini (`@google/genai`) ·
Rust · OpenTelemetry · Docker Compose.

Provider note: originally specced against the Anthropic SDK; switched to Gemini in W1 for
cost reasons. See the W1 decision entry in Part 2. The runtime is hand-written either way —
no framework owns the loop.

## 9. Current state

| Area | Status |
|---|---|
| Repo structure | Consolidated from 8 services to `core/` + `sandbox/` |
| `core/` | Fastify 5, Prisma 7.9.1, TypeScript strict, `@/*` path aliases |
| `/health` | Returns 200 |
| `ingest/` | Alert webhook ported from the old FastAPI service |
| `db/` | `Run` / `Step` models, one `init` migration applied locally |
| `agent/`, `tools/` | Empty — Week 1 |
| Database | Local Postgres via Docker Compose; Neon not yet wired |

### Open design decisions

Both block the `db/` schema being finalized, because the schema *is* the state machine's
persistence layer:

1. **What is a "step"?** One model request plus its tool execution — or is the model call
   one step and each tool execution another? This determines what resume means when the
   process dies *between* the API returning `tool_use` and the tool finishing.

2. **Is the message array stored or derived?** Persist the full serialized message history
   per step (simple, exact; rows grow and the same prefix is stored repeatedly), or persist
   structured step records and rebuild the array on resume (compact; reconstruction must be
   byte-identical or thinking-block replay breaks and the prompt cache is lost).

The current `Run`/`Step` schema is provisional — it was reverse-engineered from early code,
not derived from the state machine. Nothing is deployed, so it is still free to change.

---

# PART 2 — WORKING LOG

## W1 · Milestone 0 — The Anthropic SDK tool-calling shape

**Goal:** agent loop with one tool (`read_file`), state persisted to Postgres after every
step, run resumable after a process kill.

Everything below is the API contract that the loop is built on.

### 2.1 The wire shape

Tools are declared on the request:

```json
{
  "model": "claude-opus-5",
  "max_tokens": 16000,
  "tools": [{
    "name": "read_file",
    "description": "Read a file from the repo. Returns full contents...",
    "input_schema": {
      "type": "object",
      "properties": { "path": { "type": "string", "description": "Repo-relative path" } },
      "required": ["path"]
    }
  }],
  "messages": [{ "role": "user", "content": "Investigate incident i-42" }]
}
```

`input_schema` is JSON Schema. The **description is the highest-leverage field for
tool-call quality** — be prescriptive about *when* to call it, not just what it does.

The model responds with `stop_reason: "tool_use"`:

```json
{
  "stop_reason": "tool_use",
  "content": [
    { "type": "text", "text": "Let me look at the handler." },
    { "type": "tool_use", "id": "toolu_01abc", "name": "read_file",
      "input": { "path": "src/routes/orders.ts" } }
  ]
}
```

The result goes back as a **user** turn:

```json
{
  "role": "user",
  "content": [{
    "type": "tool_result",
    "tool_use_id": "toolu_01abc",
    "content": "export async function getOrders(...) { ... }"
  }]
}
```

Four rules that bite:

- `tool_use_id` must match exactly. Every `tool_use` needs a corresponding `tool_result`,
  or the next request 400s.
- Results go in a **user** turn. Counter-intuitive; it's the protocol.
- Tool failures are **not** exceptions — return `tool_result` with `"is_error": true` and
  the error text. The model reads it and adapts. Dropping the result is the bug.
- Parse `input` as structured data. Never regex the serialized form.

`stop_reason` values to branch on: `end_turn`, `tool_use`, `max_tokens`, `refusal`,
`pause_turn`.

### 2.2 Loop mechanics and parallel calls

**Append the entire `response.content` array to history — not the text extracted from it.**
This is the most common bug. Rebuilding an assistant turn from just text drops the
`tool_use` blocks, and the next request is rejected because a `tool_result` references an
id no longer present in the conversation.

One assistant turn may contain **several** `tool_use` blocks. Execute them (concurrently
where safe) and return **all** `tool_result` blocks in a **single** user message. Splitting
them across multiple user messages is accepted by the API but silently trains the model out
of parallel calls — steps-per-incident degrades with no visible cause.

Consequence for the "what is a step?" question: one assistant turn yielding three tool
calls is one model request and three tool executions, but only **two** message-array
entries either way.

### 2.3 Thinking blocks across turns

On `claude-opus-5`, **thinking is on by default** — omitting the `thinking` param still
runs adaptive thinking. Responses carry `thinking` blocks alongside `text` and `tool_use`.

Two properties that constrain persistence:

- **Blocks must be replayed unchanged.** They carry a `signature`; the API rejects modified
  blocks. Read them, display them, never rewrite them.
- **`display` defaults to `"omitted"`** — blocks arrive with empty text. Set
  `thinking: { type: "adaptive", display: "summarized" }` for readable traces. Affects
  visibility only, not cost or behavior.

Also: `max_tokens` caps thinking **plus** output together. A tight `max_tokens` truncates
mid-answer once thinking is on — relevant to W5 token budgets.

This is the concrete reason the storage decision is load-bearing: reconstructing the
message array from structured rows requires round-tripping thinking blocks byte-exact,
signature included.

### 2.4 History as the resumable unit

**The API is stateless.** There is no server-side conversation; every request resends the
entire history. "Resuming a run" means *reconstructing the exact message array* and calling
again. The model has no memory of the prior process.

A crash therefore loses only what wasn't written to Postgres. `kill -9` lands in one of
three places:

| Crash point | On resume |
|---|---|
| Before the API call | Re-send — nothing was consumed |
| After response, before tool ran | Re-execute the tool (it never ran) |
| After tool ran, before persisting | Re-execute, or the result is lost |

That third row is why **tool idempotency** matters. `read_file` re-executes harmlessly.
`open_pr` does not — re-running it opens a second PR. W4's `open_pr` needs an idempotency
key or a pre-write of intent.

**Cost note tied to the eval metrics:** resending full history each turn is exactly what
prompt caching exists for; cache reads cost ~10% of input rate. But caching is a **prefix
match** — a timestamp or run ID interpolated near the front of the system prompt
invalidates everything after it, surfacing as unexplained cost-per-incident. Keep volatile
content at the end.

### 2.5 Suggested first increment

Smaller than "the agent loop": get **one turn** round-tripping — user message → `tool_use`
→ execute `read_file` → `tool_result` → `end_turn` — with **no persistence at all**. Prove
the wire shape is understood. Then add the state machine underneath, knowing exactly what
is being persisted.

---

## W1 · Decision — model provider switched to Gemini

**Decision:** build against Google Gemini (`@google/genai`, model `gemini-3.6-flash`) instead
of the Anthropic SDK.

**Reason:** no Anthropic API credits available. This is a funding constraint, not an
architectural judgement — the Anthropic key authenticated fine; the account had no balance.

**What this does NOT change:** the runtime is still a hand-written durable state machine.
No framework owns the loop. `db/`, the schema, and the query layer are provider-agnostic
and unaffected.

**What it defers:** whether the loop speaks an internal message type with a provider adapter
at the edge (option B), or is written directly against Gemini (option A). Deferred
deliberately — the abstraction has nothing to abstract until the loop exists at rung 5.
Revisit then. `@anthropic-ai/sdk` is left installed for a possible Claude eval run later.

**Known cost of the choice:** `messages Json` stores raw provider format, so runs recorded
under Gemini are not replayable under Claude.

## W1 · Gemini wire shape — as observed, not as documented

Confirmed against a real rung-1 response.

### Structural differences from Anthropic

| Concept | Anthropic | Gemini |
|---|---|---|
| Turn container | `messages[]` of content blocks | `contents[]` of `parts[]` |
| Assistant role | `"assistant"` | `"model"` |
| Tool call | `tool_use` block: `id`, `name`, `input` | `functionCall` part: `name`, `args` |
| Tool result | `tool_result`, matched by `tool_use_id` | `functionResponse`, matched by `functionCall.id` |
| Loop condition | `stop_reason === "tool_use"` | **inspect `parts[]` for a `functionCall`** |
| Tool schema | `input_schema` | `config.tools[].functionDeclarations[]` |

**The loop condition is the important one.** Gemini returns `finishReason: "STOP"` even when
emitting a function call, so the finish reason cannot drive the loop. The condition is
structural: *does `parts[]` contain a `functionCall`?*

**Per-call ID — CORRECTED.** An earlier note here claimed Gemini has no per-call ID. It does:
observed `functionCall.id` (e.g. `"call_822215"`) on a real response. Results can therefore be
matched to calls exactly, and `Step` rows can key on it. Parallel calls to the same tool in one
turn are unambiguous.

### `thoughtSignature` — constrains the persistence layer

Parts can carry a `thoughtSignature`: an opaque blob that must be replayed **byte-exact** in
later turns to preserve reasoning continuity. `messages Json` must round-trip it unchanged.
Any normalization, key reordering, or stripping of unknown fields on persist breaks it
silently — no error, just degraded reasoning.

### Thinking is on by default and dominates cost

Observed on a trivial prompt:

```
promptTokenCount:     7
candidatesTokenCount: 1     <- the visible output ("OK")
thoughtsTokenCount:  58     <- thinking
totalTokenCount:     66
```

58 thinking tokens to produce one word. Use `thinkingConfig` to minimize thinking during
plumbing work (rungs 2-4); restore it when reasoning quality matters.

**Schema consequence:** `Step.tokensOut` should be `candidatesTokenCount + thoughtsTokenCount`.
Thinking tokens bill as output — counting only `candidatesTokenCount` understates
cost-per-incident by a large factor, and that number is a published eval metric.

Ignore `sdkHttpResponse.headers` — transport metadata, not model output. Do not persist it.
