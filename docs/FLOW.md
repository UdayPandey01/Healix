# Healix — Request Flow

> The path a single incident takes through the system, end to end. Updated each phase.
>
> **Backfill notice:** the build protocol schedules this file for Phase 2. It was created in Phase 0
> because Phases 1–3 were already built — see `PHASE_LOG.md`.

---

## The flow as designed

1. **TRIGGER** — `demo-service` throws 500s; Prometheus fires a webhook.
2. **PERSIST** — `ingest` normalizes the payload, writes an incident row, creates a run.
   *Durable before any work starts.* Nothing is attempted until it survives a crash.
3. **INVESTIGATE** — the agent loop runs: query logs, search code via pgvector, read
   candidate files, form a hypothesis.
4. **QUARANTINE** — the model writes a patch; the Rust sandbox runs the test suite in
   isolation with timeouts and resource caps. Tests fail → the agent retries with the failure
   output as context.
5. **RESOLVE** — tests pass, a PR opens. A human merges.

Step 2 is the load-bearing one. Persisting *before* any work begins is what makes the
durability claim real rather than aspirational — there is no window where work is in flight
but unrecorded.

---

## The flow as actually built

Steps 1–3 are live. Step 4 runs tests but nothing writes patches yet. Step 5 does not exist.

```
 ┌─ 1. TRIGGER ────────────────────────────────────────────────────────────┐
 │  GET /users/999/summary  →  userOrderSummary throws  →  500             │
 │  metrics middleware counts it on res.on("finish")                       │
 │  Prometheus scrapes /metrics                                            │
 │  rule HighErrorRate: 5xx rate > 5% for 30s  →  fires                    │
 │  Alertmanager  →  POST /v1/alerts/alertmanager                          │
 └────────────────────────────────┬────────────────────────────────────────┘
                                  │
 ┌─ 2. PERSIST ────────────────────▼───────────────────────────────────────┐
 │  Authorization: Bearer === INGEST_TOKEN ?                               │
 │      no  → 401    unset → 503 (fails closed)   ← BEFORE any parsing     │
 │  alertmanagerSchema.safeParse()      → 422 on bad shape                 │
 │  normalize()  → drop non-firing → [{ alert, raw }]                      │
 │                                                                          │
 │  upsertIncident(fingerprint, ..., payload: raw)                          │
 │      → incidents row; raw labels/annotations/startsAt preserved          │
 │  findActiveRun(incidentId)                                               │
 │      → run already running?  yes → return it, deduplicated: true         │
 │                              no  → continue                              │
 │                                                                          │
 │  taskFromAlert()  → task string naming file+line as the deliverable     │
 │  createRun(task, incidentId) → runs row, status="running"               │
 │  reply 202 { run_id, incident_id, deduplicated }  ← request ends here   │
 │  void runAgent(task, { resumeId })   fire-and-forget                    │
 └────────────────────────────────┬────────────────────────────────────────┘
                                  │
 ┌─ 3. INVESTIGATE ────────────────▼───────────────────────────────────────┐
 │  loop, up to MAX_PASSES:                                                 │
 │                                                                          │
 │    GUARDRAILS FIRST — before the model call, so a breach costs nothing:   │
 │      checkBudget(pass, stepNumber, tokensUsed)  ← tokensUsed from the DB  │
 │      detectLoop(contents)                       ← counted from the convo  │
 │      breach → saveState, haltRun(status, reason), return                 │
 │      both read durable state, so a crash cannot reset either             │
 │                                                                          │
 │    callModel(contents)         retry 429/500/503, exp backoff            │
 │        │                                                                 │
 │    recordStep(type="model_call", tokensIn, tokensOut, durationMs)  ──► DB│
 │    contents.push(modelTurn)    whole turn, never extracted text          │
 │        │                                                                 │
 │    does parts[] contain a functionCall?                                  │
 │        ├─ no  → answer reached, saveState, break                         │
 │        └─ yes → for each call:                                           │
 │                   dispatch(name, args)                                   │
 │                     ├─ list_files  ─┐                                    │
 │                     ├─ grep_code   ─┼─ safeResolve() containment         │
 │                     ├─ read_file   ─┘                                    │
 │                     └─ run_tests   ──────────────────► step 4            │
 │                   recordStep(type="tool_call", ...)             ──► DB   │
 │                   push functionResponse (ok → output, else → error)      │
 │                 saveState(contents, stepCount)                  ──► DB   │
 │                                                                          │
 │  finishRun(status = answered ? "completed" : "exhausted", diagnosis) ──►DB│
 └────────────────────────────────┬────────────────────────────────────────┘
                                  │
 ┌─ 4. QUARANTINE ─────────────────▼───────────────────────────────────────┐
 │  POST http://sandbox:4000/run                                            │
 │    { repo_path: WORKSPACE_ROOT, command: TEST_COMMAND, timeout_secs: 60 }│
 │                                                                          │
 │  ── security boundary ───────────────────────────────────────────────    │
 │  spawn sh -c in its own process group, stdin closed                      │
 │  join(read stdout, read stderr, wait)   both streams capped at 64KB      │
 │  timeout min(requested, 120s)  →  SIGKILL the whole process group        │
 │  container: cap_drop ALL, no-new-privileges, mem 512m, pids 256,         │
 │             internal network — no egress                                 │
 │  ──────────────────────────────────────────────────────────────────────  │
 │                                                                          │
 │  { exit_code, stdout, stderr, timed_out, duration_ms }                   │
 │  → formatted PASSED/FAILED + both streams → back into the transcript     │
 │    as the retry context for the next pass                                │
 │                                                                          │
 │  [GAP] nothing writes a patch yet — the agent reads and runs, not edits  │
 └────────────────────────────────┬────────────────────────────────────────┘
                                  │
 ┌─ 5. RESOLVE ────────────────────▼───────────────────────────────────────┐
 │  open_pr  →  savePatch()  →  patches row, status "pending"               │
 │           →  run halts, status "awaiting_approval"                       │
 │           →  NOTHING has reached GitHub                                  │
 │                                                                          │
 │  ── the agent cannot cross this line ──────────────────────────────────  │
 │                                                                          │
 │  a human:  POST /v1/runs/:id/approve      (bearer auth)                  │
 │              → openPullRequest() → branch healix/run-<id>                │
 │              → commit → PR → patch "opened", run "completed"             │
 │            POST /v1/runs/:id/reject                                      │
 │              → patch "rejected", run "rejected"                          │
 │                                                                          │
 │  [NOT BUILT] dashboard to approve in — Phase 8                           │
 └─────────────────────────────────────────────────────────────────────────┘

 ── read path, alongside all of the above ────────────────────────────────
   GET /v1/runs        → paginated list, newest first, no messages blob
   GET /v1/runs/:id    → run + ordered steps + incident, messages stripped
   POST /v1/runs/:id/approve  → opens the PR (the only path to one)
   POST /v1/runs/:id/reject   → records the rejection
   All bearer-authenticated. This is what Phase 8 renders.
```

---

## Where a crash lands

The API is stateless — there is no server-side conversation, every request resends the whole
history. "Resuming a run" means reconstructing the exact message array and calling again; the
model cannot tell the process died. A crash therefore loses only what was not written to
Postgres.

| `kill -9` lands... | On resume |
|---|---|
| Before the model call | Re-send — nothing was consumed |
| After the response, before the tool ran | Re-execute the tool (it never ran) |
| After the tool ran, before `saveState` | Re-execute the tool, or the result is lost |
| Between `saveState` and `finishRun` | Conversation ends on a model turn and the API would reject it — recovered from the last turn's parts instead |

The third row is why **tool idempotency** matters. `read_file` re-executes harmlessly.
`open_pr` does not — re-running it opens a second PR. Phase 4's `open_pr` needs an
idempotency key or a pre-write of intent before it goes anywhere near this loop.

The fourth row is failure #3 in `FAILURE_MODES.md`, and the reason the resume guard tests the
data rather than the status column.

**Resume entry point today:** `npx tsx src/agent/scratch.ts <runId>`. No argument starts a new
run. Nothing automatically restarts orphaned runs on boot — that gap is called out in
`DECISIONS.md` under `ingest/routes.ts`.

---

## Cost note

Resending the full history each turn is exactly what prompt caching exists for; cache reads
cost roughly 10% of the input rate. Caching is a **prefix match**, so a timestamp or run id
interpolated near the front of the system prompt invalidates everything after it — surfacing
as unexplained cost-per-incident, which is a published eval metric. Keep volatile content at
the end. The system instruction in `agent/run.ts` is currently static, which is correct.

---

## The approval boundary

The single most important line in this diagram is the one the agent cannot cross.

`open_pr` writes a row and stops. The GitHub credential and the code that uses it live in
`github/client.ts`, reachable only from the approve endpoint. There is no code path from
the agent loop to a pull request.

This is deliberate and it replaced a weaker design. In Phase 4 the tool called GitHub
directly and "a human approves at the end" was a sentence in the system prompt — a rule the
model was asked to follow. Any prompt injection in a file the agent read was one step from
an unreviewed PR. **When a rule must not be broken, move the capability rather than adding
an instruction.** See `FAILURE_MODES.md` #7.

## The supervisor

A run left `running` by a dead process used to sit there forever: durable and resumable, but
only if a human typed the run id. `agent/supervisor.ts` sweeps for runs untouched longer
than `SUPERVISOR_STALE_MS` and calls `runAgent` with the run id — which is all resumption
ever needed, since the conversation was already persisted.

It is conservative on purpose. The staleness threshold must exceed the slowest single step
including retry backoff, because a supervisor that resumes a run which is merely waiting on
a slow model call puts two processes on the same conversation, interleaving writes to the
same `messages` array. Too eager is worse than absent.
