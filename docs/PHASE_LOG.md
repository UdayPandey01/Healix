# Healix — Phase Log

> One entry per phase, written at the phase gate. What shipped, what the exit criteria
> proved, what's next. The paper trail for the writeup and for answering "walk me through how
> you built this" without reconstructing it from memory.
>
> **Backfill notice:** Phases 1–3 were built before this protocol existed and are recorded
> here retrospectively during Phase 0, reconstructed from the code, the git history, and the
> working log in the old `DESIGN.md`. They are marked `[backfilled]` and are deliberately
> honest about what is *not* done — a phase is logged as complete only where its exit
> criteria were actually met.

---

## Phase 0 — Protocol setup

**Dates:** 2026-09-08 · **Status:** complete

### What shipped

- `docs/` created with the three mandated files plus two backfilled early:
  - `DECISIONS.md` — 15 backfilled entries covering every file doing real work across
    `agent/`, `tools/`, `sandbox/`, `ingest/`, `db/`, and `demo-service/`.
  - `ARCHITECTURE.md` — migrated from `DESIGN.md` Part 1 and corrected against the repo,
    with a runtime topology diagram of what is actually deployed and an explicit
    known-gaps list.
  - `PHASE_LOG.md` — this file, backfilled for Phases 0–3.
  - `FLOW.md` — scheduled for Phase 2 by the build protocol, created now because the flow it
    documents already exists.
  - `FAILURE_MODES.md` — scheduled for Phase 7, created now to carry forward the four
    failure entries that already existed rather than lose them in the migration.
- `DESIGN.md` deleted. Its Part 1 became `ARCHITECTURE.md`, its Part 2 working log became
  `DECISIONS.md` and `FAILURE_MODES.md`. Content was migrated, not duplicated — two
  overlapping sources of truth would have drifted within a week. Full text remains in git
  history at `5f39df4`.
- Local Postgres removed from `docker-compose.yml`. The database is Neon, and the unused
  local service plus its volume was a second, divergent source of truth for schema state.
- `EXPLAIN.md` left untouched — it is a line-by-line teaching document, a different job from
  the decisions log, and nothing in the protocol replaces it.

### What the exit criteria proved

| Criterion | Result |
|---|---|
| `docs/ARCHITECTURE.md` has a real diagram | Yes — runtime topology plus the sandbox boundary, drawn from the code, not the plan |
| `PHASE_LOG.md` has a Phase 0 entry | This entry |
| Prisma schema matches the plan | Partially — `Run`/`Step` are sound; **no `Incident` model**, which Phase 2 specifies |
| Fastify routes match the plan | Partially — `/health` and both ingest routes live; **`api/` is empty**, no read API |
| `/health` returns 200 | Verified: `HTTP 200`, `{"status":"ok"}` |
| Database reachable | Verified: Neon, 17 runs / 72 steps already persisted |

### Findings raised

1. **No `Incident` model.** The alert is rendered to a task string and thrown away; labels,
   annotations, fingerprint, and `startsAt` are never persisted. Consequences: an incident
   cannot be correlated back to its alert, and a repeat Alertmanager delivery of the same
   fingerprint starts a duplicate run instead of deduplicating.
2. **No read API.** `api/` is empty. Phase 8 cannot start without `GET /v1/runs` and
   `GET /v1/runs/:id`.
3. **Auth checked after schema validation** in both ingest routes — an unauthenticated caller
   with a malformed body gets `422` and a schema error rather than `401`. The header is also
   only checked for presence, never verified.
4. **`DIRECT_URL` unset in `core/.env`.** `prisma.config.ts` falls back to the pooled URL,
   which will fail or hang on the next `prisma migrate dev`.
5. **the build protocol still specifies the Anthropic SDK** in Phase 1; the code runs Gemini. The
   switch was deliberate and documented — the build protocol is what is now stale.
6. **Duplicate step numbers after resume** remains open (`FAILURE_MODES.md` #4) and corrupts
   a published eval metric.

### Debt closure — findings 1, 2, 3 (and 4)

Closed immediately after the Phase 0 gate, before starting Phase 4, on the reasoning that
retrieval work would build on top of them and make them more expensive to fix.

**Finding 1 — `Incident` model.** New `incidents` table keyed on a unique `fingerprint`,
storing the raw alert payload; `runs.incident_id` links them, nullable so CLI runs and the
17 pre-existing rows stay valid. Migration `20260908152939_add_incident_and_run_link`
applied to Neon. `upsertIncident` + `findActiveRun` implement dedupe: a repeat delivery of a
firing alert returns the in-flight run instead of starting a second investigation, while a
*finished* run leaves a genuine re-fire free to start a new one.

**Finding 2 — read API.** `api/routes.ts` with `GET /v1/runs` (paginated, newest first,
`limit` clamped to 100) and `GET /v1/runs/:id` (ordered steps + incident). Both strip the
`messages` blob. Phase 8 is now unblocked.

**Finding 3 — auth.** The bearer check moved above schema validation in both ingest routes,
the token is compared against `INGEST_TOKEN` rather than merely present, and a missing token
fails closed with `503`. Pinned by a regression test, since the correct ordering is easy to
undo by accident.

**Finding 4 — `DIRECT_URL`.** Closed as a prerequisite; it was blocking the migration.

**Supporting changes:** `src/app.ts` extracted from `index.ts` so tests can use
`app.inject()` without binding a port; `normalize()` now returns `{ alert, raw }` so the raw
payload survives; `npm test` wired to `node --import tsx --test`.

### Verification

- `npm test` — 8/8 pass, covering auth-before-parse (401 not 422), wrong-token rejection,
  fingerprint upsert idempotence, `findActiveRun` lifecycle, `messages` never exposed, 404
  on unknown id, and the limit clamp.
- Live end-to-end against a running server, with a deliberately invalid model key so rows
  were written without burning tokens: unauthenticated `401`; first delivery
  `deduplicated: false`; identical repeat delivery returned **the same `run_id`** with
  `deduplicated: true`; stored payload verified to contain labels, annotations, `startsAt`
  and fingerprint. Check rows deleted afterwards.
- `npm run typecheck` clean.

### Still open after this

Findings 5 (the build protocol still specifies the Anthropic SDK) and 6 (duplicate step numbers)
remain, along with the missing `query_logs` / `search_code` / `open_pr` tools. Two new gaps
recorded in `ARCHITECTURE.md`: the read API is unauthenticated, and nothing restarts orphaned
`running` runs after a process death.

### Next

Phase 4 — retrieval pipeline + `open_pr`.

---

## Phase 5 — Budgets, loop detection, approval gate

**Dates:** 2026-09-08 · **Status:** complete

### What shipped

**Guardrails** (`agent/guardrails.ts`) — pass cap, step cap, token budget, and loop
detection, all checked *before* the model call so a breach costs nothing. Both signals read
durable state: token spend accumulates on the run row via `addTokens`, and loop detection
counts repeated call signatures out of the restored conversation. A budget held in process
memory is a budget any crash resets to zero — unenforceable in a system built to survive
`kill -9`.

**Approval gate** — restructured rather than added. `open_pr` no longer touches GitHub: it
writes a `patches` row and halts the run at `awaiting_approval`. The GitHub capability moved
to `github/client.ts`, reachable only from `POST /v1/runs/:id/approve`. The agent has no
code path to a pull request.

**API authentication** — every route now requires a bearer token, with `API_TOKEN` separate
from `INGEST_TOKEN` so the dashboard and the alerting system rotate independently.

**Supervisor** (`agent/supervisor.ts`) — sweeps runs orphaned by a dead process and resumes
them, closing the half of the durability claim that was previously "a human types the run
id".

**Failure #4 fixed** — `Step.attempt` with a unique constraint on
`(runId, stepNumber, attempt)`, derived by counting existing rows rather than trusting a
counter a crash can leave stale. The migration backfills historical duplicates instead of
deleting them; the two pre-existing collisions are now attempt 2, preserved as evidence that
crash recovery happened.

### What the exit criteria proved

> *A deliberately looping bug scenario gets caught and halted, not exhausted infinitely.*

Met, with a caveat worth recording. The first attempt was to provoke a natural loop with a
task insisting the agent re-read a nonexistent file. **The model refused to loop** — it tried
once, tried a different tool, listed the directory, and answered correctly. That is the
Phase 1 anti-fabrication work paying off, but it meant the route could not test the wiring.

So the end-to-end test drives the more interesting path instead: it resumes a run whose
persisted conversation *already contains* the loop. That is deterministic, and it proves the
stronger property — a looping run cannot escape by crashing, because the repeats are restored
from Postgres along with the conversation. Verified: `status="looping"`,
`haltReason="the same call was made 3 times: read_file:{...}"`.

### Verification

- `npm test` in `core/` — **44/44 pass**, including the budget boundary (one token under is
  allowed, exactly at budget halts), signature stability under key reordering, `run_tests`
  twice not being flagged as a loop, the resumed-loop halt through `runAgent`, the approval
  gate's proposal-without-side-effects, rejection blocking approval, and step attempt
  numbering.
- `npm run typecheck` clean.
- Migration applied to Neon with the duplicate backfill verified in place.

### Not done / open

- **`open_pr` still has never opened a real pull request.** Unchanged from Phase 4 — it now
  needs both a target repository *and* a human approval to fire.
- **Nothing writes patches to disk**, so "broken patch → retry" remains unproven.
- **`query_logs` does not exist.**
- **`FAILURE_MODES.md` #6 — retrieval blind to bugs of absence — is untouched.** It is
  Phase 7 work and was not in this phase's scope.
- **The supervisor assumes a single Healix process.** Two would sweep the same orphan; a
  lease column is the upgrade path.
- One transient `SocketTimeout` from Neon's pooler was seen during a long-running script and
  did not reproduce. Noted, not diagnosed.

### Next

Phase 6 — telemetry, first eval run, ship v1. Two things now block a clean eval run: patches
are never applied (so patch-passes-tests cannot be measured), and all 22 bugs are live at
once (so a patch cannot be judged against a suite failing for 21 other reasons).

---

## Phase 4 — Retrieval pipeline + open_pr

**Dates:** 2026-09-08 · **Status:** complete

### What shipped

**Corpus first.** The phase exit criterion was unmeasurable as stated — recall@5 means
nothing on 5 files and 90 lines, where the top 5 results are most of the repository. So
`demo-service` grew to 8 modules with **22 seeded bugs** spanning ten defect classes, each
with a test that fails until fixed, plus `evals/golden-bugs.json` mapping every bug to the
symbol that must be retrieved.

**Retrieval.** `retrieval/chunk.ts` splits on top-level declaration boundaries with leading
comments attached (45 chunks, every bug symbol isolated). `retrieval/embed.ts` embeds via
`gemini-embedding-001` at 768 dimensions with document/query task types and re-normalisation.
`retrieval/index.ts` writes to a pgvector column on Neon. `retrieval/search.ts` implements
dense search, Postgres-FTS lexical search, RRF fusion, and an LLM reranker.
`retrieval/recall.ts` measures each stage separately.

**Tools.** `search_code` returns whole chunks with line numbers, not just locations.
`open_pr` commits via the GitHub Git Data API and opens a PR. `dispatch` now threads a
`ToolContext` carrying the run id.

### What the exit criteria proved

> *A first (even rough) recall@5 number measured against the seeded bug set.*

Met, and the measurement changed the design:

| strategy | hits | recall@5 |
|---|---|---|
| dense | 21/22 | **95.5%** |
| dense+rerank | 21/22 | **95.5%** |
| rrf | 20/22 | 90.9% |
| rrf+rerank | 20/22 | 90.9% |
| lexical | 15/22 | 68.2% |

**The specified design lost to the simpler one.** Hybrid RRF scored below dense alone. The
mechanism is reproducible, not noise: RRF sums `1/(k+rank)` across rankers, so a chunk found
by both beats a chunk found only by the strong one — `cache-key-collision` goes from dense
rank 2 to fused rank 7. RRF assumes rankers of comparable quality; lexical (68%) is far
weaker than dense (95%). Default is now dense-only, with hybrid kept selectable as the
evidence. Recorded as `FAILURE_MODES.md` #5 and pinned by a synthetic test.

Stated plainly: 21 vs 20 on n=22 is one bug, inside the noise. The mechanism is the finding;
the strategy ranking is not yet statistically meaningful.

### Verification

- `npm test` in `core/` — **24/24 pass**, including live pgvector searches against Neon,
  chunker boundary cases, the RRF demotion mechanism, and `open_pr` path-traversal rejection.
- `demo-service` — 39 tests, 23 failing by design, one per seeded bug.
- Recall harness run end to end; results committed to `evals/recall-at-5.json`.
- `npm run typecheck` clean.

### Not done / open

- **`open_pr` has never opened a real pull request.** The logic, idempotency, and validation
  are tested, but `GITHUB_TOKEN` and `GITHUB_REPO` are unset and no live PR has been fired —
  that needs a deliberate decision about which repository it targets.
- **Nothing writes patches yet.** `open_pr` accepts file contents, but the agent has no
  edit-and-verify loop, so Phase 3's "broken patch produces a retry" is still unproven.
- **`query_logs` still does not exist** — the agent reads code and runs tests but cannot
  query runtime logs.
- **`FAILURE_MODES.md` #6, new and open:** retrieval cannot find a bug that is an *absence*.
  `cache-unbounded-growth` is missed by every strategy, because "memory", "heap", and "leak"
  appear nowhere in a function whose defect is having no eviction. This is a whole class —
  leaks, missing timeouts, absent validation — and disproportionately the incidents that
  actually page someone.
- Carried forward: duplicate step numbers after resume (#4), unauthenticated read API, no
  supervisor for orphaned runs.

### Next

Phase 5 — budgets, loop detection, approval gate. Note that the approval gate and `open_pr`
interact directly: the gate must sit *before* the PR is opened, not after.

---

## Phase 1 — Agent loop, durable state, one tool · [backfilled]

**Dates:** 2026-08-13 → 2026-08-17 · **Status:** complete

### What shipped

- `agent/run.ts` — the durable loop. Model call → record step → dispatch tools → record each
  → save state, up to `MAX_PASSES = 10`.
- `db/schema.prisma` — `Run` and `Step`, applied to Neon as migration `20260816073147_init`.
- `db/client.ts` — the five-function database surface: `createRun`, `recordStep`,
  `saveState`, `finishRun`, `getRun`.
- `read_file` wired end to end with a `FunctionDeclaration` schema.
- Repo consolidated from 8 polyglot microservices to `core/` + `sandbox/` (`5f39df4`'s
  ancestor `refactor!`), and `runAgent` extracted from the CLI script so a route could call
  it.

### What the exit criteria proved

> *Kill the process mid-run, restart, watch it resume — verified manually, not just
> unit-tested.*

Met, manually:

```
kill -9 mid-run  →  status=running, messages=1 turn, steps=2
resume           →  exit=0, status=completed, answer produced
```

The mechanism is that the conversation *is* the agent's entire memory — the API is stateless,
so resume is `getRun(id)` → load `messages` into `contents` → keep looping. The model cannot
tell the process died.

### Decisions that came out of it

Recorded in full in `DECISIONS.md`; the load-bearing ones:

- **Provider switched from Anthropic to Gemini** for cost reasons (no Anthropic credits). A
  funding constraint, not an architectural judgement. The runtime is hand-written either way.
- **`saveState` runs after tool execution**, not after the model responds — a crash costs a
  repeated model call, never a lost tool result.
- **One `Step` row per event**, not per pass, so per-tool timing and per-call token accounting
  are possible.
- **`tokensOut = candidatesTokenCount + thoughtsTokenCount`** — thinking bills as output, and
  counting only visible output understated cost ~6x on an observed request.
- **The loop condition is structural** — inspect `parts[]` for a `functionCall`. Gemini
  returns `finishReason: "STOP"` even when calling a tool, so the finish reason cannot drive
  the loop.

Three failure modes were found and two fixed during this phase — `FAILURE_MODES.md` #1, #2,
#3. #4 was found and remains open.

---

## Phase 2 — Full tool suite, alert webhook, demo-service · [backfilled]

**Dates:** 2026-08-17 → 2026-08-18 · **Status:** partial

### What shipped

- `tools/index.ts` — registry + `dispatch`, with unknown-tool and thrown-exception paths both
  returning `{ ok: false, error }` rather than throwing, so a tool failure stays in the
  transcript as something the model can adapt to.
- `list_files` and `grep_code` added alongside `read_file`.
- `tools/workspace.ts` — `safeResolve` path containment, in one place rather than per tool.
- `ingest/routes.ts` — `POST /v1/alerts/ingest`, creating the run *before* starting work and
  returning `202` with the run id.
- `ingest/alertmanager.ts` — Alertmanager envelope parsed and normalized to the internal
  `Alert`, non-firing alerts dropped.
- `demo-service/` — Express app, `/metrics` via `prom-client`, two seeded bugs (unhandled
  null in `userOrderSummary`, off-by-one in `listOrders`), both failing their tests. Bug
  marker comments deliberately removed so retrieval cannot cheat by grepping for "BUG".
- `observability/` — Prometheus scrape config and a `HighErrorRate` rule, Alertmanager
  routing the webhook to Healix.

### What the exit criteria proved

> *A fake alert webhook produces a persisted run end-to-end, even with stubbed tools.*

Met. The path from `POST /v1/alerts/alertmanager` to a persisted `runs` row with `steps` is
live and has 17 runs / 72 steps of history on Neon to show for it.

### What did NOT ship against the phase spec

- **`query_logs`, `search_code`, `open_pr` were never created** — not even as stubs.
  `grep_code` partly covers `search_code`.
- **No incident row.** The spec says "webhook ingest → incident row → run created". The
  incident row does not exist; the alert is rendered to a task string and discarded.
- **2 of the 3–5 seeded bugs** were written.

This phase is logged as partial for those three reasons.

---

## Phase 3 — Rust sandbox · [backfilled]

**Dates:** 2026-08-18 → uncommitted at time of writing · **Status:** partial

### What shipped

- `sandbox/` — Rust + axum service, `GET /health` and `POST /run`.
- Isolation and caps: command spawned in its own process group with stdin closed;
  `SIGKILL` to the whole group on timeout so orphaned children die with the parent; both
  output streams capped at 64KB; caller-supplied timeout `min`'d against a 120s hard ceiling
  so the caller cannot disable it; stdout/stderr drained concurrently with `wait()` to avoid
  the pipe-buffer deadlock.
- Container-level limits in `docker-compose.yml`: `cap_drop: ALL`, `no-new-privileges`,
  `mem_limit: 512m`, `pids_limit: 256`, and an `internal: true` network with no egress.
- `tools/runTests.ts` — `run_tests` wired for real, taking **no arguments** so the command
  comes from server-side config rather than from the model.

### What the exit criteria proved

> *The sandbox runs a real test suite in isolation, enforces its caps, and a
> deliberately-broken patch produces a retry with the failure as context.*

Partially met:

- Runs a real suite in isolation — yes. `demo-service`'s failing tests are the fixture.
- Enforces caps — implemented at both layers and readable in the code; **not yet demonstrated
  by a test that actually trips a limit** (no recorded run of an infinite loop hitting the
  timeout, or a memory hog hitting `mem_limit`).
- Broken patch → retry with failure as context — **not met.** Failure output does flow back
  into the transcript as retry context, but nothing in the system writes a patch yet, so the
  loop the criterion describes has never actually run.

### Open before this phase can be called complete

1. A demonstration that trips the timeout and the memory cap, recorded.
2. Patch writing — which is really Phase 4 work — before the retry loop is provable.
3. `sandbox/` is still uncommitted on `feat/patch-and-pr`.

### Next

Phase 4 — retrieval pipeline + `open_pr`. Note that `open_pr` is the first
**non-idempotent** tool in the system: re-executing it after a crash opens a second PR. It
needs an idempotency key or a pre-write of intent before it goes anywhere near the loop. See
`FLOW.md`, "Where a crash lands".
