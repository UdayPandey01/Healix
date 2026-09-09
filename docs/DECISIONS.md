# Healix — Decisions Log

> The running "why" log. One entry per file or function that does real work. **Append
> only** — never rewrite history. Before moving to the next task in a phase, the entry for
> the thing just written goes in.
>
> **Backfill notice:** entries marked `[backfilled Phase 0]` were written after the fact,
> reconstructed from the code as it stands plus the working log in the old `DESIGN.md`.
> They record the reasoning the code embodies. Where that reasoning was inferred rather
> than stated, the entry says so. Everything from Phase 4 onward is written live.

---

## [Phase 0 · backfilled] core/prisma.config.ts — defineConfig

**What it does:** points Prisma at `src/db/schema.prisma` and `src/db/migrations`, and
routes migrations over `DIRECT_URL` rather than `DATABASE_URL`.

**Why it exists / why this approach:** Neon serves two endpoints for the same database. The
pooled one (`-pooler` in the host) runs PgBouncer in transaction mode; the direct one does
not. Prisma Migrate takes a Postgres advisory lock for the duration of a migration and
depends on session-level state, and transaction-mode pooling drops both — migrations hang
or fail with a lock error. Runtime queries want the opposite: pooling, because serverless
Postgres charges for connection setup and Fastify will open many. So the two paths are
deliberately split.

**Tradeoffs considered:** running everything over the direct endpoint would remove the
second URL and the whole class of confusion, at the cost of unpooled connections at
runtime — the wrong trade for a serverless database. Running everything pooled is what the
fallback (`?? DATABASE_URL`) silently does today, and it is a latent bug, not a choice.

**Depends on / feeds into:** `core/.env` supplies both URLs; `src/lib/prisma.ts` consumes
the pooled one. **Known gap:** `DIRECT_URL` is currently unset in `core/.env`, so the
fallback is active and the next `prisma migrate dev` will use the pooler.

---

## [Phase 0 · backfilled] core/src/lib/prisma.ts — prisma

**What it does:** constructs one `PrismaClient` over the `PrismaPg` driver adapter and
exports it as a module singleton.

**Why it exists / why this approach:** every module that touches the database imports this
one instance. A `PrismaClient` per call site opens a connection pool per call site, which
against Neon means burning the connection budget on nothing. The `PrismaPg` adapter is
required rather than optional here — Prisma 7 with a driver adapter is what lets the
client speak to Neon over `pg` with the pooled connection string.

**Tradeoffs considered:** no real choice. The alternative — the default Rust query engine
without an adapter — is being removed in Prisma's own direction of travel.

**Depends on / feeds into:** reads `DATABASE_URL`; every function in `db/client.ts` uses it.

---

## [Phase 1 · backfilled] core/src/db/schema.prisma — Run, Step

**What it does:** defines the two tables the state machine persists into. `Run` holds the
task, status, the full serialized conversation (`messages Json`), a step counter, and the
final diagnosis. `Step` holds one row per event with type, tool name, input, output, token
counts, and duration.

**Why it exists / why this approach:** this schema *is* the state machine's persistence
layer — the durability claim is only as good as what these two tables record. Two
decisions are baked in:

*`messages` is stored, not derived.* The whole provider-format conversation is written back
each pass. Rebuilding it from structured `Step` rows on resume would be more compact, but
reconstruction must be **byte-exact** — Gemini parts carry a `thoughtSignature` that must
replay unchanged to preserve reasoning continuity, and any key reordering or dropping of
unknown fields breaks it silently, with no error, just degraded reasoning. Storing the
array verbatim makes that failure impossible.

*`Step` is one row per event, not per loop pass.* A pass with two tool calls produces three
rows (one model call, two tool calls). Per-pass rows would make per-tool timing and
per-call token accounting impossible, and both are published eval metrics.

`tokensOut` is written as `candidatesTokenCount + thoughtsTokenCount`, because thinking
tokens bill as output — counting only the visible output understated cost by roughly 6x on
an observed trivial request (58 of 66 tokens were thinking).

**Tradeoffs considered:** storing the array repeatedly means the same prefix is written
over and over and rows grow with conversation length. Accepted: correctness of replay beats
storage efficiency at this scale, and nothing here is at a size where it matters.

**Depends on / feeds into:** written exclusively through `db/client.ts`; read by
`agent/run.ts` on resume. **Known gap:** no `@@unique([runId, stepNumber])`, which is why
duplicate step numbers survive a resume — see `FAILURE_MODES.md` #4.

---

## [Phase 1 · backfilled] core/src/db/client.ts — createRun, saveState, finishRun, recordStep, getRun

**What it does:** the entire database surface for a run. `createRun` opens a run with the
first user turn already written; `recordStep` appends one event row; `saveState` overwrites
the conversation and step count; `finishRun` sets terminal status and diagnosis; `getRun`
loads a run with its steps ordered.

**Why it exists / why this approach:** it is the leaf of the dependency graph — everything
may call it, it calls nothing. Keeping every write behind these five functions is what
makes "persisted after every step" auditable: there is exactly one place to look to verify
the claim, rather than Prisma calls scattered through the loop.

`createRun` writes the opening message in Gemini's `{ role, parts: [{ text }] }` shape
rather than `{ role, content }`. This is load-bearing and not cosmetic: a resume loads
`messages` straight into `contents` and hands it to the API, so a mismatched shape breaks
any run that crashes before the first `saveState`.

**Tradeoffs considered:** `saveState` rewrites the whole `messages` array rather than
appending — Postgres `jsonb` has no cheap append, and an append-only step table plus a
rebuilt array was rejected for the byte-exactness reason in the schema entry above.

**Depends on / feeds into:** `lib/prisma.ts` below it; `agent/run.ts` and `ingest/routes.ts`
above it.

---

## [Phase 1 · backfilled] core/src/agent/run.ts — runAgent

**What it does:** the durable agent loop. Starts or resumes a run, then repeats: call the
model, record the step, execute any requested tools, record those steps, save state, until
the model answers with no tool call or `MAX_PASSES` is hit.

**Why it exists / why this approach:** this is the technical claim of the project. Three
decisions carry it:

*The loop condition is structural, not a status flag.* Gemini returns
`finishReason: "STOP"` even when it emits a function call, so the finish reason cannot
drive the loop. The condition is: does `parts[]` contain a `functionCall`? Branching on
finish reason — the obvious port from the Anthropic shape — silently terminates the loop
one turn early.

*The entire model turn is appended to history, never text extracted from it.* Rebuilding an
assistant turn from its text drops the `functionCall` parts, and the next request is
rejected because a `functionResponse` references a call id no longer in the conversation.

*`saveState` runs after tool execution, not immediately after the model responds.* A crash
therefore costs a repeated model call — money — but never a lost tool result. The reverse
placement is cheaper on crash but risks re-executing a tool, which is harmless for
`read_file` and unacceptable for `open_pr` in Phase 4.

The resume path guards on **data, not status**. A run can be persisted with
`status="running"` and a conversation that already ends on a model turn, because
`saveState` and `finishRun` are two writes with a crash window between them — and Gemini
rejects any request whose last turn is `role: "model"`. So resume checks
`contents.at(-1)?.role === "model"` and recovers the answer from that turn rather than
calling the API. See `FAILURE_MODES.md` #2 and #3.

**Tradeoffs considered:** the loop is written directly against Gemini rather than against
an internal message type with a provider adapter at the edge. Deferred deliberately — an
abstraction has nothing to abstract until there is a second provider, and there is not one
yet. The recorded cost of the choice: `messages` holds raw Gemini format, so runs recorded
under Gemini are not replayable under Claude.

**Depends on / feeds into:** `db/client.ts` for persistence, `tools/index.ts` for dispatch;
called by `ingest/routes.ts` and by `agent/scratch.ts` from the CLI.

---

## [Phase 1 · backfilled] core/src/agent/run.ts — callModel

**What it does:** wraps the model call in a retry with exponential backoff, retrying only
`429`, `500`, and `503`, up to `MAX_RETRIES` attempts.

**Why it exists / why this approach:** without it a single rate-limit response kills a run
that is otherwise perfectly resumable, and the run lands in `error` for a reason that had
nothing to do with the agent. The status allowlist is the important part — retrying a `400`
or `403` just burns attempts on a request that will never succeed, and retrying a `400`
caused by an unresumable conversation would mask failure #2 rather than surface it.

**Tradeoffs considered:** retrying inside the process rather than letting the run die and
be resumed externally. Both work, given resume exists; in-process is fewer moving parts for
a transient 429.

**Depends on / feeds into:** called only by `runAgent`.

---

## [Phase 2 · backfilled] core/src/tools/workspace.ts — safeResolve, WORKSPACE_ROOT

**What it does:** resolves a model-supplied path against the workspace root and refuses
anything that escapes it. Also exports the root and the ignore set.

**Why it exists / why this approach:** every path in this system arrives from a language
model, which means it is untrusted input at a trust boundary. `../../../etc/passwd` and an
absolute `/etc/passwd` both have to be refused, and they have to be refused in **one**
place — a check duplicated into each tool is a check that will eventually be forgotten in
the next tool. Resolving first and then testing the relative path (`..` prefix or still
absolute) catches both cases with the same test, and catches them after symlink-free
normalization rather than by string-matching the input, which is the way this check is
usually written wrong.

**Tradeoffs considered:** a string blocklist on `..` is the naive version and is bypassable.
`realpath` would additionally resolve symlinks out of the workspace — not done, and worth
revisiting once the agent can write files rather than only read them.

**Depends on / feeds into:** used by `read_file`, `list_files`, and `grep_code`;
`WORKSPACE_ROOT` is also handed to the sandbox by `run_tests`.

---

## [Phase 2 · backfilled] core/src/tools/index.ts — dispatch, toolDeclarations

**What it does:** holds the tool registry, derives the declaration list sent to the model
from it, and routes a call by name to its implementation.

**Why it exists / why this approach:** one array is the single source of truth, so the
declarations the model sees and the functions that can actually run cannot drift apart —
registering a tool the model can't see, or advertising one that isn't implemented, are both
made structurally impossible.

The error handling is the real content. An unknown tool name returns
`{ ok: false, error: ... }` **listing the available tools**, and a thrown exception inside a
tool is caught and returned the same way. Neither throws. This is deliberate: a tool failure
is information the model should read and adapt to, not an exception that kills the run.
Dropping the result instead — or letting it propagate — is the classic bug, because the API
then rejects the next request for a `functionResponse` that never came.

**Tradeoffs considered:** the registry is a plain array with a `Map` index rather than a
plugin/discovery mechanism. There are four tools; a registry that scans the directory would
be more machinery than the thing it manages.

**Note:** the schema in each `FunctionDeclaration` is sent to the model but **not** enforced
on the way back in — args are cast and coerced inside each tool (`String(args["path"] ?? "")`).
Argument validation and repair is still open work for a later phase.

**Depends on / feeds into:** the four tool modules below it; `agent/run.ts` above it.

---

## [Phase 2 · backfilled] core/src/tools/readFile.ts — readFileTool

**What it does:** reads one file inside the workspace and returns its full contents.

**Why it exists / why this approach:** the model cannot diagnose code it has not seen, and
the highest-severity failure observed on this project is the model describing a file it
never read (`FAILURE_MODES.md` #1). The failure message — `No such file: "x". Try list_files
first.` — is written to route the model to the recovery action rather than just reporting
the error, because a bare "not found" tends to produce a guess at another path.

**Tradeoffs considered:** no line-range parameter and no size cap. Fine for a small demo
repo; a large file will eventually blow out the context window, and a range parameter is the
obvious first fix when it does.

**Depends on / feeds into:** `workspace.ts` for containment; registered in `tools/index.ts`.

---

## [Phase 2 · backfilled] core/src/tools/listFiles.ts — listFilesTool, walk

**What it does:** recursively lists every file under a directory, relative to the workspace
root, skipping the ignore set.

**Why it exists / why this approach:** the agent starts with no idea what exists. Without
this it guesses paths, `read_file` fails, and passes are burned on recovery. The description
says "use this FIRST" for exactly that reason — the description field is the highest-leverage
lever on tool-call quality, so it is prescriptive about *when* to call, not just what it does.

The ignore set (`node_modules`, `.git`, `dist`, `.next`, `coverage`) is not cosmetic: walking
`node_modules` returns tens of thousands of paths and destroys the context window.

**Tradeoffs considered:** no depth limit and no result cap — the workspace is one small
service. Both become necessary the moment retrieval points at a real repo.

**Depends on / feeds into:** `workspace.ts`; registered in `tools/index.ts`.

---

## [Phase 2 · backfilled] core/src/tools/grepCode.ts — grepCodeTool, search

**What it does:** literal substring search across the workspace, returning
`path:line: text` hits, capped at 50 matches.

**Why it exists / why this approach:** it is the stand-in for `search_code` until Phase 4
builds real retrieval, and it covers the case dense retrieval is worst at anyway. Identifiers
in code match *literally* — `getUserOrders` and `fetchCustomerPurchases` are semantically
close and functionally unrelated, so a semantic index alone would rank the wrong one. The
description says so explicitly ("search for exact identifiers, not descriptions") to stop the
model passing natural language.

The 50-match cap is checked at three levels of the walk — before the directory loop, inside
it, and inside the line scan — so a query like `const` stops early rather than reading the
whole tree and truncating at the end.

**Tradeoffs considered:** shelling out to `ripgrep` would be faster and support regex, at the
cost of a binary dependency and an exec path taking model-influenced input. Reading files in
Node keeps it dependency-free and keeps the untrusted string as a `String.includes` argument
rather than anything a shell parses. Correct trade at this size; revisit if the workspace
grows.

**Depends on / feeds into:** `workspace.ts`; registered in `tools/index.ts`. Superseded in
part by Phase 4 retrieval, which should keep it as the BM25/lexical half of hybrid search
rather than replace it.

---

## [Phase 3 · backfilled] core/src/tools/runTests.ts — runTestsTool

**What it does:** asks the sandbox over HTTP to run the test command in the workspace, and
formats exit code, duration, stdout, and stderr back to the model.

**Why it exists / why this approach:** this is the tool that makes a patch *verified* rather
than *plausible* — the difference between the project's claim and a demo. It takes **no
arguments** on purpose: the command comes from `TEST_COMMAND` in the environment, not from
the model. A model-supplied command string is arbitrary code execution chosen by the model,
and no amount of sandboxing makes handing it the command a good default.

Both failure paths return `ok: false` with text rather than throwing: an unreachable sandbox
and a timeout are both things the model should be told about in its own transcript.

The result deliberately includes the exit code, an explicit `PASSED`/`FAILED`, and both
streams — the failure output *is* the retry context for the next pass.

**Tradeoffs considered:** parameterising the test command per call would let the agent run a
single failing test instead of the whole suite, which is a real speed win on a large suite.
Deferred: it reopens the arbitrary-command question and the demo suite runs in under a
second.

**Depends on / feeds into:** `WORKSPACE_ROOT` from `workspace.ts`; the Rust sandbox at
`SANDBOX_URL`; registered in `tools/index.ts`.

---

## [Phase 3 · backfilled] sandbox/src/main.rs — run, read_capped, kill_group

**What it does:** a single-endpoint axum service. `POST /run` executes a command in a
directory and returns exit code, captured output, timeout flag, and duration.

**Why it exists / why this approach:** it is a **security boundary**, not a scaling decision.
It executes model-generated code, so it is a separate process with its own limits, in its own
container, on an `internal: true` Docker network with no route out. Rust is chosen for the
resource and timeout control that boundary needs.

Three specific defences, each against a way a naive implementation leaks:

*`kill_group` sends `SIGKILL` to the negated pid — the whole process group — and the child is
spawned with `process_group(0)`.* Killing only the direct child on timeout orphans everything
it spawned; a test runner that forks workers would leave them running forever, and the
container slowly fills with them.

*`read_capped` truncates each stream at 64KB.* A test that logs in an infinite loop otherwise
exhausts memory in the reader, not in the sandbox — the attack lands on the wrong side of the
boundary.

*The timeout is `min`'d against `MAX_TIMEOUT_SECS`.* The caller supplies a timeout, so the
caller must not be able to disable it.

Output reading and `child.wait()` are joined concurrently rather than sequenced. Waiting on
exit before draining the pipes deadlocks the moment a child writes more than the pipe buffer
and blocks — a classic, and it only shows up under load.

**Tradeoffs considered:** the container-level limits (`cap_drop: ALL`, `no-new-privileges`,
`mem_limit`, `pids_limit`) live in `docker-compose.yml` rather than being enforced in-process
via `setrlimit`. Deliberate layering: the process enforces what it can observe — time, output
size, process tree — and Docker enforces what it cannot. Neither layer is sufficient alone,
and the compose file is the honest place for the ones the kernel has to apply at spawn.

`sh -c` is used rather than exec'ing an argv directly, which means shell metacharacters in
`command` are interpreted. Acceptable only because the command comes from server-side
configuration, never from the model — if that ever changes, this line is the first thing to
fix.

**Depends on / feeds into:** called only by `tools/runTests.ts`; the container's limits come
from `docker-compose.yml`.

---

## [Phase 2 · backfilled] core/src/ingest/schema.ts + alertmanager.ts — alertSchema, normalize

**What it does:** `schema.ts` defines Healix's internal `Alert` shape. `alertmanager.ts`
parses Alertmanager's webhook envelope and maps it down to that shape, dropping anything not
`firing`.

**Why it exists / why this approach:** it keeps Alertmanager's payload format out of the rest
of the system. One internal `Alert` type means a second alert source later is a second
normalizer, not a change to the agent. Alertmanager delivers resolved alerts through the same
endpoint as firing ones, so the `status === "firing"` filter is what stops Healix opening an
investigation into a problem that just fixed itself.

Every field falls back rather than failing: `fingerprint` → synthesized from alertname and
`startsAt`; `service` → `labels.service`, then `labels.job`, then `"unknown"`. A monitoring
integration that rejects a real alert because an optional annotation was missing is worse
than one that investigates with a slightly worse task string.

**Tradeoffs considered:** the fallback `incident_id` is not stable across restarts the way a
real fingerprint is, which weakens future deduplication — acceptable because Alertmanager
sends `fingerprint` in practice, and the fallback exists for hand-rolled test payloads.

**Depends on / feeds into:** consumed by `ingest/routes.ts`.

---

## [Phase 2 · backfilled] core/src/ingest/routes.ts — ingestRoutes, startInvestigation, taskFromAlert

**What it does:** exposes `POST /v1/alerts/ingest` (internal shape) and
`POST /v1/alerts/alertmanager` (Alertmanager webhook). Both validate, create a run, kick off
the agent, and return `202` with the run id.

**Why it exists / why this approach:** the run row is created **before** the agent starts and
the response is `202 Accepted`, not `200`. Both follow from the durability claim: the run must
survive a crash from the instant the alert is accepted, and the work outlives the request, so
the status code has to say "accepted, not finished". Alertmanager retries non-2xx deliveries —
holding the connection open for a multi-minute investigation would guarantee a timeout and a
duplicate.

`runAgent` is deliberately not awaited (`void ... .catch(...)`), with the catch handler
attached at the call site so a rejected promise is logged against its run id rather than
becoming an unhandled rejection.

`taskFromAlert` renders the alert into a task string that asks for a specific file and line.
An open-ended "investigate this" produces a summary of the alert; asking for the exact
location produces something checkable against the seeded bug set.

**Tradeoffs considered:** fire-and-forget in-process is the simplest thing that works and is
honest about what it is — if the process dies, the run is durable but nothing restarts it.
There is no supervisor scanning for `status="running"` orphans on boot. That is the gap
between "resumable" and "self-healing", and it is worth closing before the Phase 6 eval run,
because an eval harness will produce orphans.

**Known gaps:** (1) the Zod parse runs *before* the `Authorization` check, so an
unauthenticated caller with a bad body gets `422` and a schema error rather than `401` — the
checks should be inverted. (2) The header is only checked for presence; its value is never
verified. (3) No `Incident` row is written, so the raw alert payload is discarded and repeat
deliveries of the same fingerprint start duplicate runs.

**Depends on / feeds into:** `ingest/schema.ts` and `ingest/alertmanager.ts` for validation,
`db/client.ts` for the run row, `agent/run.ts` for the work; registered in `src/index.ts`.

---

## [Phase 2 · backfilled] demo-service/ — orders.js, server.js, metrics.js

**What it does:** a small Express service with two seeded bugs, a Prometheus `/metrics`
endpoint, and a test suite that fails on both bugs.

**Why it exists / why this approach:** it is the target Healix investigates, and it has to
fail in ways that are *realistic and diagnosable from source*, not artificial. The two seeded
so far:

- `userOrderSummary` — `findUser` returns `undefined` for an unknown id and `user.name`
  throws. Unhandled null: the single most common production 500.
- `listOrders` — `start = page * limit` treats a 1-based page as 0-based, so page 1 skips the
  first records. Off-by-one: silently wrong data rather than a crash, which is the harder and
  more interesting diagnosis.

Bug markers were deliberately removed from the source. A `// BUG:` comment turns retrieval
into a `grep` for the word "bug" and invalidates every recall number the eval harness will
produce.

The metrics middleware counts on `res.on("finish")` rather than inline, so responses that
error out are still counted — otherwise the 5xx rate that drives the Prometheus alert would
undercount exactly the requests the alert exists to catch.

**Tradeoffs considered:** in-memory arrays instead of a database. The bugs under test are
application-logic bugs; a database would add setup cost and teach the agent nothing.

**Depends on / feeds into:** scraped by Prometheus, whose `HighErrorRate` rule fires the
Alertmanager webhook into `ingest/`. Read by the agent's tools through `WORKSPACE_ROOT`, and
its test suite is what the sandbox executes.

---

## [Phase 0 debt] core/src/db/schema.prisma — Incident

**What it does:** one row per distinct alert, keyed on a unique `fingerprint`, holding the
normalized fields plus `payload` — the raw alert exactly as received. `Run` gains a nullable
`incidentId`.

**Why it exists / why this approach:** two problems, one row. Normalization threw away
everything the model didn't need — labels, annotations, `startsAt`, severity — which is
precisely the context a human needs when reviewing an incident later, and precisely what an
eval harness needs to correlate a run back to the alert that caused it. And with no stable
key per alert, every repeat delivery from Alertmanager was a new investigation.

`fingerprint` is `@unique` because that is what makes the upsert atomic — the database
rejects the second insert rather than the application racing to check-then-insert.

The relation is **nullable** with `onDelete: SetNull`. Nullable because runs started from the
CLI (`agent/scratch.ts`) legitimately have no incident, and because 17 runs already existed
when this landed — a required column would have meant a destructive migration or a fake
backfill incident. `SetNull` rather than `Cascade` because deleting an alert record should
not delete the evidence of the investigation it triggered.

**Tradeoffs considered:** storing only the normalized fields and dropping `payload` — smaller,
but it re-creates the exact problem this closes the moment a new label matters. Making
`incidentId` required and backfilling — cleaner schema, destructive migration, no benefit.

**Depends on / feeds into:** written by `upsertIncident` in `db/client.ts`; read by the API's
run list and detail endpoints.

---

## [Phase 0 debt] core/src/db/client.ts — upsertIncident, findActiveRun, listRuns, countRuns

**What it does:** `upsertIncident` creates or refreshes the incident for a fingerprint.
`findActiveRun` returns the id of a still-running investigation into that incident, if any.
`listRuns` / `countRuns` back the read API.

**Why it exists / why this approach:** the dedupe policy lives in `findActiveRun`, and the
policy is the interesting part. It matches on `status: "running"` only. Alertmanager
re-delivers a firing alert every `repeat_interval` (1h in `alertmanager.yml`), and without
this each delivery started a duplicate investigation into a problem already being
investigated. But deduplicating on fingerprint *forever* is the opposite bug: the same alert
firing again next week is a genuine new incident and must get a fresh run. Scoping the check
to in-flight runs distinguishes the two with one query and no extra state.

`upsertIncident` refreshes the descriptive fields on every delivery rather than only on
create — the newest delivery is the most current description of the alert.

`listRuns` uses an explicit `select` that omits `messages`. That column holds the entire
provider-format conversation; a 25-row list page that included it would be megabytes of
transcript to render a status table.

**Tradeoffs considered:** dedupe by holding a lock or an in-memory set of active
fingerprints — faster, but lost on restart, which is precisely the failure mode this project
claims to survive. The database already knows which runs are running; asking it is correct.

**Depends on / feeds into:** `lib/prisma.ts`; called by `ingest/routes.ts` and `api/routes.ts`.

---

## [Phase 0 debt] core/src/api/routes.ts — apiRoutes

**What it does:** `GET /v1/runs` (paginated list, newest first) and `GET /v1/runs/:id` (one
run with its ordered steps and its incident).

**Why it exists / why this approach:** Phase 8's dashboard is read-plus-one-approve over an
API that does not otherwise exist — this is that API, built now because it is the cheap half
and because it makes every run inspectable without opening a Postgres client.

`limit` is clamped to `MAX_LIMIT` rather than trusted. An unbounded `limit` is a
one-parameter denial of service against the process rendering the response, and the clamp is
one `Math.min`.

The detail route strips `messages` before responding. The agent needs that blob to resume; a
reader never does, and the step rows carry the same information in a form a trace view can
actually render.

**Tradeoffs considered:** stripping `messages` in the route rather than adding a second
query with an explicit `select`. The blob still crosses the wire from Neon, which is waste —
accepted for now because it reuses `getRun` exactly as the agent uses it, and one run detail
per page view is not a hot path. If it shows up in latency, the fix is a dedicated select.

No authentication on these routes: they are read-only, local, and the dashboard that consumes
them does not exist yet. **This must not ship to anything public as-is** — the run list
exposes task text and diagnoses.

**Depends on / feeds into:** `db/client.ts`; registered in `src/app.ts`.

---

## [Phase 0 debt] core/src/ingest/routes.ts — unauthorized, startInvestigation

**What it does:** `unauthorized` verifies the bearer token before anything else touches the
request. `startInvestigation` persists the incident, checks for an in-flight run, and only
then creates a run and starts the agent.

**Why it exists / why this approach:** the auth check moved *above* schema validation. With
the old order an unauthenticated caller with a malformed body received `422` and the full Zod
issue list — free schema disclosure to anyone who can reach the port. Ordering is the whole
fix; there is a regression test pinning it because the correct order is not visually obvious
and is easy to undo while editing the handler.

The token is now compared, not merely checked for presence. Presence-only means any string
authenticates, which is indistinguishable from no auth at all.

It **fails closed**: unset `INGEST_TOKEN` returns `503`, not open access. An ingestion
endpoint that silently accepts everything because an environment variable is missing is the
failure that gets discovered by an attacker rather than by a test.

`startInvestigation` returns `deduplicated` so the response tells the caller which happened.
Silently returning an existing run id looks identical to having started a new one, which
would make the dedupe impossible to observe from outside.

**Tradeoffs considered:** a Fastify `preHandler` hook or a plugin-wide guard instead of a
call at the top of each handler. Cleaner with more routes; with two, an explicit line at the
top of each is more obvious to a reader and harder to accidentally scope wrong. Revisit when
a third protected route appears.

Timing-safe comparison (`crypto.timingSafeEqual`) was considered and skipped — the shared
secret is a fixed local dev token, and a timing oracle over a network is not the threat here.
Worth revisiting if this ever faces the internet.

**Depends on / feeds into:** `db/client.ts` for the incident and dedupe; `agent/run.ts` for
the work.

---

## [Phase 0 debt] core/src/app.ts — buildApp

**What it does:** builds and returns the configured Fastify instance. `index.ts` is now only
the entry point that listens.

**Why it exists / why this approach:** tests need the routes without a bound port.
`app.inject()` gives full request/response semantics in-process, but only if something hands
back the app rather than starting a server at import time. Splitting construction from
listening is the standard shape and is what makes `test/ingest.test.ts` possible.

**Tradeoffs considered:** testing over a real port with a spawned server — slower, flakier,
and needs teardown that leaks processes when a test fails.

**Depends on / feeds into:** registers `ingest/routes.ts` and `api/routes.ts`; used by
`src/index.ts` and `test/ingest.test.ts`.

---

## [Phase 0 debt] Correction — DIRECT_URL

The `prisma.config.ts` entry above records a known gap: `DIRECT_URL` unset in `core/.env`,
leaving migrations on the pooled endpoint. Closed while landing the `Incident` migration — it
was blocking. `DIRECT_URL` is now set to the pooled host minus `-pooler`, Neon's documented
convention, and `prisma migrate dev` applied `20260908152939_add_incident_and_run_link` over
it successfully, which is the proof the derived host is correct.

Noted here as a correction rather than by editing the entry above, because this log is
append-only.

---

## [Phase 4] demo-service/ + evals/golden-bugs.json — the seeded bug set

**What it does:** grows `demo-service` from 5 files to 8 modules carrying 22 deliberate
bugs, each with a test that fails until it is fixed, plus a golden file mapping every bug
to the file and symbol that must be retrieved.

**Why it exists / why this approach:** Phase 4's exit criterion is a measured recall@5, and
recall@5 is meaningless on a corpus where the top 5 results are most of the repository. With
5 files and ~90 lines, every retrieval strategy scores 1.0 and the number proves nothing.
The corpus had to grow before the measurement could say anything.

The bugs are chosen to span *classes* rather than to be numerous: unhandled null, off-by-one,
race condition, memory leak, auth bypass, unit mismatch, float-precision money, input
mutation, missing validation, missing timeout. A bug set that is twenty variations of null
dereference measures one thing twenty times.

Two properties are deliberate. **No `BUG:` comments** — a marker turns retrieval into a grep
for the word "bug" and invalidates every recall number. And each bug is **diagnosable from
source alone**, since that is what the agent gets.

`golden-bugs.json` keys ground truth on **symbol**, not line number. Line numbers drift
every time `demo-service` is edited and would silently rot the measurement; file-only
matching would count any of the eight chunks in a file as a hit and flatter the result.

**Tradeoffs considered:** all 22 bugs are live simultaneously, so the suite has 23 failing
tests at rest. For Phase 6's patch-passes-tests metric each bug will need isolating, since a
patch cannot be judged against a suite that fails for 21 other reasons. Deferred to Phase 6
rather than solved now with per-bug fixtures nothing yet consumes.

**Depends on / feeds into:** the corpus indexed by `retrieval/index.ts`; the golden file read
by `retrieval/recall.ts`; the failing suite is what `run_tests` executes.

---

## [Phase 4] core/src/retrieval/chunk.ts — chunkSource, chunkRepo

**What it does:** splits source files into chunks on top-level function and class
boundaries, attaching leading comments to the declaration below them and keeping module-level
code as its own chunk.

**Why it exists / why this approach:** fixed-size chunking is the default everywhere and is
wrong for code. A function cut at 512 tokens puts its signature in one chunk and the line
with the bug in another, so the chunk that matches the query is not the chunk that contains
the answer. Declaration boundaries make every chunk a complete, independently readable unit.

Three specific decisions:

*Only column-0 declarations start a chunk.* A nested closure is not independently useful
context — it is meaningless without its parent's scope — so it stays inside the parent chunk.

*Leading comments attach downward.* A JSDoc block is frequently the most retrievable text
about a function, since it describes behaviour in the same natural language a query uses.
Orphaning it into the previous chunk gives that text to the wrong neighbour.

*Module-level code is its own chunk.* Imports and module state would otherwise be glued onto
whichever function happens to be first. Module-level mutable state is itself a bug class —
two of the seeded bugs live there — so it has to be independently retrievable.

**Tradeoffs considered:** a real parser (`@babel/parser`, `tree-sitter`) would handle
generators, decorators, and TypeScript overloads correctly where this regex will not. Not
taken: it is a dependency and a build step to correctly chunk one small JavaScript service,
and the regex handles every construct in the corpus. The upgrade path is one function.
Verified against the real corpus — 45 chunks, every one of the 22 bug symbols isolated with
correct line ranges.

**Depends on / feeds into:** consumed by `retrieval/index.ts`; pinned by `test/chunk.test.ts`.

---

## [Phase 4] core/src/retrieval/embed.ts — embedDocuments, embedQuery

**What it does:** turns text into 768-dimension vectors via `gemini-embedding-001`, batching
documents and re-normalising every vector.

**Why it exists / why this approach:** three decisions worth defending.

*768 dimensions rather than the model's native 3072.* Storage and scan cost scale linearly
with dimension, and pgvector's HNSW index refuses anything above 2000 — picking 3072 would
foreclose ever adding that index. Quality difference on this corpus is not measurable.

*Vectors are re-normalised after truncation.* The model guarantees unit length only at its
native dimension; a truncated prefix is not unit length. Cosine distance in pgvector
tolerates that, but anything using a dot-product shortcut silently does not. Normalising once
at the boundary means nothing downstream has to know.

*Documents and queries use different task types.* `RETRIEVAL_DOCUMENT` and `RETRIEVAL_QUERY`
embed into the same space but account for the asymmetry between a code chunk and a question
about it. Embedding both identically is the single most common retrieval mistake and costs
real recall.

**Verified:** 768 dims, magnitude 1.000000, cosine 0.69 for a related query against 0.48 for
an unrelated one.

**Tradeoffs considered:** a local embedding model would remove the API dependency and the
per-index cost. Rejected — it means shipping model weights and an inference runtime to save a
few cents on a corpus that embeds in seconds.

**Depends on / feeds into:** `retrieval/index.ts` for writes, `retrieval/search.ts` for
queries.

---

## [Phase 4] core/src/retrieval/index.ts — indexRepo

**What it does:** chunks a repo, embeds every chunk, and replaces that repo's rows in
`code_chunks`.

**Why it exists / why this approach:** the whole repo is re-indexed rather than diffed.
Incremental indexing needs content hashing plus a deletion pass for chunks whose symbol moved
or vanished; `demo-service` re-indexes in seconds, so the complexity buys nothing. The delete
before insert is the important half — without it, a chunk whose function was renamed stays in
the index forever and remains retrievable, which is a silent correctness bug rather than a
performance one.

Path and symbol are prepended to the embedded text. A query mentioning "shipping" or
"verifyToken" should match a chunk on those words even when they never appear in its body,
and the body alone throws away the two strongest signals available.

**Tradeoffs considered:** row-by-row inserts instead of one multi-row statement. 45 rows;
the loop is legible and the batch is not worth writing yet.

**Depends on / feeds into:** `chunk.ts` and `embed.ts`; writes the table `search.ts` reads.

---

## [Phase 4] core/src/retrieval/search.ts — denseSearch, lexicalSearch, rrfFuse, rerank, searchCode

**What it does:** the retrieval pipeline — dense vector search, lexical full-text search,
Reciprocal Rank Fusion, and an LLM reranker, behind one `searchCode` entry point.

**Why it exists / why this approach — and the result that changed the design:**

The phase specified hybrid BM25 + dense fused with RRF, reranked top-20 → top-5. That was
built, and then measured per stage, because reporting only a final number makes the design
unfalsifiable — if dense alone matches the full pipeline, the extra machinery is not earning
its place. recall@5 over 22 seeded bugs:

| strategy | hits | recall@5 |
|---|---|---|
| dense | 21/22 | **95.5%** |
| dense+rerank | 21/22 | **95.5%** |
| rrf | 20/22 | 90.9% |
| rrf+rerank | 20/22 | 90.9% |
| lexical | 15/22 | 68.2% |

**Hybrid fusion made retrieval worse, and the mechanism is specific and reproducible.** RRF
scores a chunk by summing `1/(k+rank)` over the rankers that returned it, so a chunk returned
by *both* rankers beats a chunk returned by only one — even when the good ranker ranked it
higher. `cache-key-collision` sits at dense rank 2 and lands at rank 7 after fusion, purely
because the lexical ranker had no opinion on it. RRF assumes rankers of comparable quality;
lexical here is 68% against dense's 95%, and fusing a weak ranker into a strong one costs
more than it adds. `test/retrieval.test.ts` pins this behaviour with a synthetic case so it
stays visible.

The reranker changed nothing in either direction while costing a model call per search.

So the **default strategy is dense only** — measured, not assumed. Hybrid and reranking stay
available behind `strategy`, because they are the evidence and because the conclusion is
corpus-specific.

**Stated honestly:** 21 vs 20 is a one-bug difference on n=22 and is inside the noise. The
*mechanism* is the finding; the ranking of strategies is not yet statistically meaningful.
Lexical matching should start paying off on a corpus where identifiers repeat across many
files — 45 chunks with almost no identifier collisions is close to the worst case for it.
Re-measure before pointing this at a real repository.

Two smaller decisions: the lexical half is Postgres FTS with `ts_rank_cd`, **not** BM25 —
Postgres has no BM25 without an extension, and it is named accurately here rather than
claimed. Its terms are OR-ed, because `plainto_tsquery` and `websearch_to_tsquery` both AND
them, and an alert sentence AND-ed against a code chunk matches nothing at all.

**Depends on / feeds into:** `embed.ts` and the `code_chunks` table; consumed by
`tools/searchCode.ts` and measured by `retrieval/recall.ts`.

---

## [Phase 4] core/src/retrieval/recall.ts — measureRecall

**What it does:** runs every golden-file query through each strategy and reports recall@k per
strategy, with the id of every miss.

**Why it exists / why this approach:** measuring only the final pipeline would have hidden
the result above entirely. Per-stage measurement is what makes "we built hybrid search"
falsifiable, and it is what produced the decision to default to dense.

Reporting miss *ids* rather than a bare percentage is deliberate — `cache-unbounded-growth`
missing on every strategy is a finding about the query, not the retriever, and a percentage
alone would never have surfaced it.

**Tradeoffs considered:** recall@5 only, no MRR or nDCG. Recall@5 is the metric that matches
how the agent actually consumes retrieval — it gets five chunks and reads them — so rank
order within the five does not change its behaviour.

**Depends on / feeds into:** reads `evals/golden-bugs.json`, writes `evals/recall-at-5.json`.

---

## [Phase 4] core/src/tools/searchCode.ts — searchCodeTool

**What it does:** exposes retrieval to the agent as `search_code`, returning the top 5 chunks
with paths, line ranges, and full bodies.

**Why it exists / why this approach:** it returns whole chunk bodies rather than locations.
Returning paths alone means five `read_file` calls before the agent can reason about
anything — five more model turns per incident, which is a published eval metric. The
declaration explicitly steers the model to `grep_code` for exact identifiers and `search_code`
for symptoms, because the two tools fail in opposite directions and the model needs to know
which is which.

**Depends on / feeds into:** `retrieval/search.ts`; registered in `tools/index.ts`.

---

## [Phase 4] core/src/tools/openPr.ts — openPrTool, branchForRun, normaliseFiles

**What it does:** commits the model's file changes to a branch via the GitHub Git Data API
and opens a pull request.

**Why it exists / why this approach:** `open_pr` is the first tool in this system with a side
effect outside the process, and that makes it the first one where the durability design has
teeth. The loop persists a step *after* a tool runs, so a crash in that window re-executes
the tool on resume — harmless for `read_file`, a duplicate pull request here.

**The branch name is derived from the run id, and that is the whole idempotency mechanism.**
A re-execution computes the same branch, finds its own open PR, and returns it instead of
opening a second. No idempotency table, no lock, no key to persist — the run id was already
durable. A 422 on branch creation is treated the same way: the branch exists because we
created it, so the ref is force-updated rather than failing.

*It takes complete file contents, not a diff.* Models produce broken unified diffs constantly
— wrong line numbers, wrong context, hunks that will not apply. Full contents cannot fail to
apply, and GitHub computes the diff itself. The cost is that the model must reproduce the
whole file correctly, which is a bigger output but a far more reliable one.

*Paths are validated before anything reaches GitHub.* `..` segments and absolute paths are
rejected: these paths come from a language model and this writes to a real repository.

*It fails closed when unconfigured*, telling the model to report its diagnosis in the answer
instead — an unconfigured PR tool should degrade to a useful answer, not to a crash.

**Tradeoffs considered:** the Git Data API rather than a local clone plus `git push`. No
working copy to keep in sync, no credentials on disk, and the whole operation is one
sequence of HTTP calls. The cost is more round trips and a `base_tree` that must be threaded
correctly, or the commit silently drops every file the model did not mention.

**Not yet done:** the human approval gate is Phase 5. Today the pull request itself is the
gate — Healix opens it and never merges.

**Depends on / feeds into:** `ToolContext.runId` threaded through `dispatch` from
`agent/run.ts`; registered in `tools/index.ts`.

---

## [Phase 4] core/src/tools/index.ts — ToolContext threading

**What it does:** `dispatch` now takes a context object carrying the run id and passes it to
every tool.

**Why it exists / why this approach:** `open_pr` needs the run id to derive its branch, and
the alternative was having the model pass its own run id as a tool argument — which asks the
model to know something it has no reliable way to know, and makes idempotency depend on the
model getting a UUID right. Context that the runtime owns should be supplied by the runtime.

**Tradeoffs considered:** a module-level "current run" variable would have avoided changing
the signature. Rejected: it is shared mutable state that breaks the moment two runs execute
concurrently, which they already do — `ingest/routes.ts` starts runs fire-and-forget.

**Depends on / feeds into:** set in `agent/run.ts`, consumed by `tools/openPr.ts`.

---

## [Phase 5] core/src/agent/guardrails.ts — checkBudget, detectLoop, callSignature

**What it does:** enforces the pass cap, step cap, and token budget, and detects when a run
is repeating itself.

**Why it exists / why this approach:** an agent without caps is an unbounded bill and an
unbounded outage. Three decisions carry this file:

*Guardrails run before the model call, never after.* Checking afterwards means the call that
breaches the budget is one that already cost money — the cap would report an overspend rather
than prevent it. There is a test pinning the boundary (one token under is allowed, exactly at
budget halts).

*Token spend lives on the run row, not in a variable.* This is the decision that matters, and
it follows directly from the project's own claim. A budget held in process memory is a budget
that any crash resets to zero — so in a system explicitly built to survive `kill -9`, an
in-memory cap is enforceable only by processes that never crash. `addTokens` increments the
row and returns the new total.

*Loop detection reads the conversation, not a counter.* Same reasoning, same payoff: the
conversation is restored on resume, so counting repeated calls from it is crash-safe for free.
A process-local tally would reset on restart, and an agent stuck in a loop that also crashes
would loop forever across restarts without ever tripping the detector. There is a test for
exactly that sequence.

`callSignature` sorts object keys before serialising. Key order is not meaningful but
`JSON.stringify` preserves it, so two identical calls serialised in different orders would
compare unequal and a loop would slip through.

**Tradeoffs considered:** `REPEAT_LIMIT` is 3, not 2. Two is too strict — the agent
legitimately calls `run_tests` with the same (empty) arguments before and after an edit, and
that is progress, not a loop. Three identical calls with nothing intervening is not a retry.

A semantic "no progress" detector — same *set* of files read, no new information — would catch
more. Not built: it needs a definition of progress that is easy to get wrong, and the cheap
signature check catches the observed failure.

**Depends on / feeds into:** consumed by `agent/run.ts` at the top of every pass; the halt
status and reason land in `Run.status` / `Run.haltReason`.

---

## [Phase 5] core/src/tools/openPr.ts — openPrTool (rewritten)

**What it does:** records a proposed patch in the `patches` table and stops. It no longer
touches GitHub at all.

**Why it exists / why this approach:** the vision says "a human approves at the end", and that
is only true if the agent is *structurally incapable* of skipping the human. In Phase 4 this
tool opened a pull request directly, which made approval a policy the model could decide to
ignore — a prompt instruction, not a guarantee. Splitting proposal from execution means the
only path from patch to PR runs through an endpoint the agent cannot call. The gate is now a
property of the system's shape rather than of the model's cooperation.

The run halts after a successful proposal (`awaitingApproval` in `agent/run.ts`). Continuing
to loop would let the agent keep spending on a run whose next move belongs to a person.

*Only a successful proposal halts.* A rejected `open_pr` — bad path, missing file contents —
flows back to the model as an error it can correct, rather than parking the run awaiting review
of a patch that was never saved. This was a real bug in the first cut of the wiring: it keyed
the halt off the call rather than the result.

**Tradeoffs considered:** keeping the GitHub call in the tool and gating it on an
`approved` flag read from the database. Rejected — it leaves the credential and the network
call on the agent's side of the boundary, so a prompt injection that convinces the model to
"just check the flag again" is one bug away from a PR. Moving the capability out entirely is
the stronger boundary.

**Depends on / feeds into:** `db/client.ts#savePatch`; the GitHub call now lives in
`github/client.ts`, invoked only from `api/routes.ts`.

---

## [Phase 5] core/src/github/client.ts — openPullRequest, branchForRun, normaliseFiles

**What it does:** the entire GitHub surface — commit files to a run-specific branch via the
Git Data API and open a pull request.

**Why it exists / why this approach:** extracted from the tool so that the capability to write
to GitHub sits behind the approval endpoint and nowhere else. Everything else in this entry
carried over from the Phase 4 design and still holds: the deterministic `healix/run-<id>`
branch is the idempotency mechanism, full file contents beat model-generated diffs that will
not apply, `base_tree` is threaded so the commit is a change on top of the branch rather than
a repo containing only the mentioned files, and paths are validated because they came from a
language model.

What changed is *who can call it*: previously the agent, now only a human-triggered endpoint.
The idempotency argument shifted with it — the risk is no longer only crash-resume but a
double-clicked approve button or a retried request, which the same deterministic branch
handles.

**Depends on / feeds into:** called only by `api/routes.ts`'s approve handler.

---

## [Phase 5] core/src/api/routes.ts — approve, reject, and API authentication

**What it does:** adds `POST /v1/runs/:id/approve` and `/reject`, and puts a bearer token in
front of every API route.

**Why it exists / why this approach:** the approve endpoint is the only path from a proposed
patch to a real pull request, which makes it the most sensitive route in the system — and it
is exactly why the read API's missing authentication, previously logged as an acceptable gap
for a read-only local service, stopped being acceptable the moment a write action appeared
next to it.

`API_TOKEN` is separate from `INGEST_TOKEN`, falling back to it. The alerting system and the
operator dashboard are different callers with different lifetimes; one leaking should not hand
over the other. Falling back keeps a single-token dev setup working.

*A failed PR attempt leaves the patch `pending`, not `approved`.* Marking it approved-but-failed
would strand it in a state nothing acts on — approval must stay retryable once the cause is
fixed.

*Approving an already-opened patch returns the existing PR rather than erroring*, and
*approving a rejected patch is a 409*. A human who already said no should not be silently
overridden by a stray retry.

**Tradeoffs considered:** a single shared token rather than per-user auth with an audit trail
of who approved what. For a single-tenant v1 whose operator is one person, a token is
proportionate; real review attribution belongs with the multi-tenant work that is explicitly
out of scope.

**Depends on / feeds into:** `db/client.ts` for patch state, `github/client.ts` for the PR.

---

## [Phase 5] core/src/agent/supervisor.ts — sweepOrphanedRuns

**What it does:** periodically finds runs stuck in `running` whose row has not been touched
for a while, and resumes them.

**Why it exists / why this approach:** the durability claim was only half-delivered. State
survived a crash and *could* be resumed, but resuming meant a human typing a run id — so in
practice a crashed run simply stopped. Everything the loop needs was already persisted; the
missing piece was something that notices. Without it, a Phase 6 eval batch that crashes leaves
orphans that never finish and silently corrupt every metric computed over those runs.

Deliberately conservative: a bounded number per sweep, oldest first, and only runs untouched
for `STALE_AFTER_MS`. **A supervisor that is too eager is worse than none** — it would resume
runs that are merely waiting on a slow model call, and two processes would then drive the same
conversation, interleaving writes to the same `messages` array. The staleness threshold must
comfortably exceed the slowest single step including retry backoff, which is why it defaults
to ten minutes against a step measured in seconds.

The timer is `unref`'d so it never holds the process open, and it starts in `index.ts` rather
than `buildApp()` — otherwise every test run would start resuming real runs against the live
database.

**Tradeoffs considered:** a proper job queue with leases and heartbeats would be the correct
answer at scale and would remove the staleness guess entirely. Out of scope for a single-node
v1, and it is the kind of infrastructure this project's scope section explicitly argues
against building before it is needed. The honest limitation: with two Healix processes running,
both could sweep the same orphan. A lease column is the upgrade path.

**Depends on / feeds into:** `db/client.ts#findStaleRuns`; calls `agent/run.ts#runAgent`.

---

## [Phase 5] core/src/db/client.ts — recordStep attempt derivation (failure #4 fixed)

**What it does:** derives each step's `attempt` from the rows already in the table rather than
from a counter, backed by a unique constraint on `(runId, stepNumber, attempt)`.

**Why it exists / why this approach:** failure mode #4 — a crash between `recordStep` and
`saveState` leaves step rows the saved conversation has no record of, so a resumed run restarts
its numbering and writes a second row with the same `stepNumber`. "Steps per incident" then
counted retries as real work, and that number is a published eval metric.

Of the three options recorded when the failure was catalogued, this is option 3: **make crash
recovery visible in the data rather than concealing it.** For a project whose central claim is
durability, a duplicate step row is evidence the resume worked — provided the schema says which
attempt it belongs to. Deleting or overwriting the duplicate would destroy exactly the evidence
that demonstrates the feature.

Counting existing rows costs one extra query per step. Cheap relative to a model call, and it
removes the dependency on a counter that a crash can leave stale.

The migration backfills historical duplicates by insertion order rather than deleting them —
the two pre-existing collisions in this database are now attempt 2, preserved.

**Tradeoffs considered:** the count-then-insert is not atomic; two processes recording the same
step number concurrently would both read the same count and one would hit the unique constraint.
That is the correct failure — loud, not silent — and it cannot currently happen because a run is
driven by one process at a time. The supervisor's staleness threshold is what keeps that true.

**Depends on / feeds into:** `Step.attempt` and the unique constraint in `schema.prisma`.


---

## [Phase 5] Correction — the Phase 4 reranker measurement was invalid

The `search.ts` entry above reports "The reranker changed nothing in either direction while
costing a model call per search" and uses that to justify leaving reranking off by default.

**That claim was wrong.** The reranker sent `thinkingConfig: { thinkingBudget: 0 }`, which
`gemini-3.6-flash` rejects with `400 INVALID_ARGUMENT`, and the function's `catch` returned
the un-reranked candidates. It threw on every call. The `dense+rerank` and `rrf+rerank` rows
were identical to `dense` and `rrf` because reranking never executed — not because it made
no difference. It also cost nothing, since no generation ever happened.

What survives unchanged: the **dense vs rrf vs lexical** comparison, and therefore the
decision to default to dense. None of those paths touch the model, and the RRF demotion
mechanism (`FAILURE_MODES.md` #5) is arithmetic, reproduced in a unit test.

What is withdrawn: any claim about whether reranking helps. It is unmeasured.

Fixed by removing `thinkingConfig` and making the fallback observable — it now logs and
increments a counter that `measureRecall` reports, so a broken reranker can never again be
mistaken for a measured null result.

Recorded here as a correction rather than by editing the entry above, because this log is
append-only. Full write-up in `FAILURE_MODES.md` #11.
