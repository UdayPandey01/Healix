# Healix — Architecture

> The current system picture. This file describes what is **actually built**, not what is
> planned. Updated whenever a phase changes the shape of the system — a new component, a
> new boundary, a new data flow.
>
> Migrated from `DESIGN.md` Part 1 in Phase 0 and corrected against the repo as it stands.

---

## 1. The problem

Observability solved **monitoring**. Prometheus, Datadog, and Sentry are very good at
telling a human that something broke. They stop there. The expensive part — a human paging
in at 2am, sifting logs, locating the broken code, writing a fix under time pressure — is
untouched.

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
- A thin frontend to demo and review runs

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
extends to provider-supplied loop drivers (the Anthropic SDK's
`client.beta.messages.tool_runner()`, and Gemini's equivalent automatic function calling),
which are deliberately unused for the same reason.

## 4. Architecture

**A modular monolith plus one isolated Rust sandbox.**

```
healix/
├── core/                  TypeScript, Fastify, Prisma, Neon Postgres
│   └── src/
│       ├── agent/         loop + guardrails + supervisor [BUILT]
│       ├── tools/         declarations + dispatch     [BUILT — 6 tools]
│       ├── ingest/        alert webhook, normalization[BUILT]
│       ├── retrieval/     chunk/embed/search/recall   [BUILT]
│       ├── telemetry/     OpenTelemetry, Prometheus   [EMPTY — Phase 6]
│       ├── github/        PR creation, behind approval [BUILT]
│       ├── db/            Prisma schema and queries   [BUILT]
│       ├── lib/           Prisma client singleton     [BUILT]
│       └── api/           read routes + approval gate [BUILT]
├── sandbox/               Rust, axum, isolated        [BUILT]
├── demo-service/          SEPARATE REPO — the target system, not part of Healix
│                          UdayPandey01/healix-demo-service, 22 bugs seeded
├── evals/                 golden bug set + recall@5   [BUILT — 22 bugs]
├── web/                   frontend dashboard          [NOT CREATED — Phase 8]
├── docs/                  living documentation        [THIS]
└── observability/         Prometheus/Alertmanager/Grafana config [BUILT]
```

This started as 8 microservices and was consolidated. Coordination cost — network hops,
independent deploys, distributed failure modes, 8 Dockerfiles — was not justified for a
single-node system. Consolidating *down* is the defensible direction; it demonstrates that
the split was evaluated rather than cargo-culted.

**The sandbox is the one exception, and it is not a scaling decision.** It is a separate
process because it executes model-generated code. That is a security boundary. Rust is
chosen for the resource ceilings and timeout control that boundary requires.

### Runtime topology — as deployed today

```
                    ┌──────────────────┐
                    │  demo-service    │  Express, :3000
                    │  /metrics        │  deliberately breakable
                    └────────┬─────────┘
                             │ scrape
                    ┌────────▼─────────┐
                    │   Prometheus     │  :9090, alerts.yml
                    └────────┬─────────┘
                             │ fires
                    ┌────────▼─────────┐
                    │  Alertmanager    │  :9093
                    └────────┬─────────┘
                             │ POST /v1/alerts/alertmanager
     ┌───────────────────────▼───────────────────────────────┐
     │  core — Fastify :8000                                 │
     │                                                       │
     │   ingest/routes.ts   verify bearer ──► 401/503        │
     │         │            upsertIncident ──► dedupe on     │
     │         │            fingerprint, skip if run active  │
     │         │                                             │
     │         └──► agent/run.ts  (fire-and-forget)          │
     │                                                       │
     │   api/routes.ts  (bearer-authenticated)               │
     │     GET  /v1/runs, /v1/runs/:id                       │
     │     POST /v1/runs/:id/approve ──► github/client.ts ───│──► GitHub API
     │     POST /v1/runs/:id/reject        THE ONLY PATH     │    (PAT, never merges)
     │                                     TO A REAL PR      │
     │                   │                                   │
     │                   ├─ callModel() ──────────────────►  │──► Gemini API
     │                   │                                   │
     │                   ├─ tools/index.ts dispatch({ runId })│
     │                   │     ├─ read_file  ┐               │
     │                   │     ├─ list_files ├─ workspace/   │──► demo-service files
     │                   │     ├─ grep_code  ┘  safeResolve  │
     │                   │     ├─ search_code ─ retrieval/ ──│──┐
     │                   │     ├─ run_tests ─── HTTP ────────│──┼─┐
     │                   │     └─ open_pr ──► patches table   │  │ │
     │                   │           proposes only; run halts │  │ │
     │                   ├─ guardrails: budget + loop detect │  │ │
     │                   └─ recordStep / saveState / finishRun  │ │
     └───────────────────────────┬───────────────────────────┘  │ │
                                 │ every step                   │ │
                        ┌────────▼──────────────┐    ┌──────────▼─│──────────┐
                        │  Neon Postgres        │◄───┘          ┌─▼──────────┴──┐
                        │  runs / steps         │               │ sandbox :4000 │
                        │  incidents / patches  │               │ Rust + axum   │
                        │  code_chunks (pgvector)│              │ cap_drop ALL  │
                        └───────────────────────┘               │ mem 512m      │
                                 ▲                              │ pids 256      │
                                 │ index-time                   │ internal net  │
                        ┌────────┴──────────┐                   └───────────────┘
                        │ retrieval/        │
                        │ chunk → embed →   │──► Gemini embeddings
                        │ search (dense)    │    768-d, task-typed
                        └───────────────────┘
```

### Why demo-service is a separate repository

Healix patches demo-service; demo-service is not part of Healix. Keeping them in one
repository would mean the agent proposing a path like `src/orders.js` that only makes sense
after a `demo-service/` prefix is bolted on somewhere — a translation step existing purely
because two unrelated systems shared a directory.

Split, the agent's paths land verbatim in the target repository, and the boundary in the
code matches the boundary in reality: one system observes and repairs another.

The working copy still lives inside the Healix tree so `TARGET_REPO` resolves without
configuration, but it is gitignored here and tracked by its own `.git`.

### The sandbox boundary

The one hard boundary in the system. Everything crossing it is explicit:

- **In:** a JSON `{ repo_path, command, timeout_secs }` over HTTP. Nothing else.
- **Out:** `{ exit_code, stdout, stderr, timed_out, duration_ms }`, output capped at 64KB.
- **Enforced by the sandbox process:** wall-clock timeout (30s default, 120s hard cap),
  process-group `SIGKILL` on timeout so orphaned children die with the parent, output
  truncation, `stdin` closed.
- **Enforced by Docker:** `cap_drop: ALL`, `no-new-privileges`, `mem_limit: 512m`,
  `pids_limit: 256`, and an `internal: true` network with no route out.

The caps are layered deliberately: the Rust process enforces what it can see (time,
output size, process tree), Docker enforces what it cannot (memory, capabilities, network
egress). Neither layer alone is sufficient.

### Dependency direction

```
api / ingest  →  agent  →  tools  →  retrieval / sandbox

db  ←  called from anywhere, calls nothing
```

One direction, no cycles. `db/` is a leaf: everything may call it, it calls nothing. This
is what keeps the monolith modular rather than merely co-located — the module boundaries
are enforced by the dependency graph, so the thing can be split later if it ever needs to
be.

## 5. Retrieval design (Phase 4 — not yet built)

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

**Built, and the measurement changed the design.** pgvector is enabled on Neon and the
pipeline exists end to end. Measured per stage over 22 seeded bugs, dense retrieval alone
(95.5% recall@5) beat hybrid RRF (90.9%), because RRF demotes chunks that only the stronger
ranker returned. The default is now dense-only; hybrid and reranking remain selectable.
See `FAILURE_MODES.md` #5 and the `search.ts` entry in `DECISIONS.md`.

The lexical half is Postgres FTS with `ts_rank_cd`, not true BM25 — Postgres has no BM25
without an extension, and it is named accurately rather than claimed.

## 6. Evals are the point (Phase 6 — not yet built)

20 seeded bugs in `demo-service`: unhandled null, unbounded cache leak, missing timeout,
connection pool exhaustion, off-by-one in pagination.

**Measured:** diagnosis accuracy, patch-passes-tests rate, retrieval recall@5, cost per
incident, steps per incident — plus a written catalogue of every observed failure mode
(`docs/FAILURE_MODES.md`).

Most agent projects have no evals. Having them is the difference between a demo and a
project. The failure catalogue in particular is what makes it credible: it proves the
system was actually run enough times to characterize how it breaks.

## 7. Stack

TypeScript · Fastify 5 · Prisma 7.9 · **Neon Postgres** (serverless, pooled) · Google
Gemini (`@google/genai`, `gemini-3.6-flash`) · Rust + axum · Docker Compose · Prometheus /
Alertmanager / Grafana.

**Provider note:** originally specced against the Anthropic SDK; switched to Gemini for
cost reasons. See the provider-switch entry in `DECISIONS.md`. The runtime is hand-written
either way — no framework owns the loop.

**Database note:** Postgres is **Neon**, not local Docker. `DATABASE_URL` points at the
pooled (`-pooler`) endpoint for runtime queries; `DIRECT_URL` points at the unpooled
endpoint for migrations, because Neon's pooler runs PgBouncer in transaction mode, which
drops the advisory locks and session state Prisma Migrate needs. The local Postgres
service was removed from `docker-compose.yml` in Phase 0.

## 8. Current state

| Area | Status |
|---|---|
| Repo structure | Consolidated from 8 services to `core/` + `sandbox/` |
| `core/` | Fastify 5, Prisma 7.9.1, TypeScript strict, `@/*` path aliases |
| `/health` | Returns 200 — verified Phase 0 |
| Database | **Neon Postgres**, reachable, 1 migration applied, `Run` + `Step` populated |
| `ingest/` | Two routes, both live, bearer-authenticated, deduplicating on fingerprint |
| `Incident` | Keyed on fingerprint, stores the raw alert payload, linked to runs |
| `agent/` | Durable loop built; resume across `kill -9` verified manually |
| `tools/` | `read_file`, `list_files`, `grep_code`, `run_tests` wired with dispatch |
| `sandbox/` | Rust axum service, timeouts + caps enforced, wired to `run_tests` |
| `observability/` | Prometheus scrape + alert rule, Alertmanager → Healix webhook |
| `demo-service/` | **Its own repository** — `UdayPandey01/healix-demo-service`, public. 22 bugs across 8 modules, 23 failing tests. Kept on disk inside the Healix working tree so `TARGET_REPO` resolves, but gitignored and pushed from its own `.git`. |
| `retrieval/` | Chunker, Gemini embeddings, pgvector search, RRF, reranker, recall harness |
| `evals/` | `golden-bugs.json` ground truth + `recall-at-5.json` measured results |
| `api/` | Runs list/detail + approve/reject, all bearer-authenticated |
| Guardrails | Pass cap, step cap, token budget, loop detection — all from durable state |
| Approval gate | `open_pr` only proposes; a human approves before any PR exists |
| Supervisor | Sweeps runs orphaned by a dead process and resumes them |
| `telemetry/` | Empty |
| `web/` | Not created |

### Known gaps against the plan

1. ~~No `Incident` model.~~ **Closed.** `Incident` is keyed on the Alertmanager fingerprint
   and stores the raw payload; `Run.incidentId` links the two. Repeat deliveries of a firing
   alert now return the in-flight run rather than starting a second investigation.
2. ~~No read API.~~ **Closed.** `api/routes.ts` serves `GET /v1/runs` (paginated) and
   `GET /v1/runs/:id` (steps + incident, `messages` stripped).
3. **Missing tools.** `search_code` and `open_pr` now exist. `query_logs` still does not —
   the agent reads code and runs tests but cannot query runtime logs.
4. ~~Auth is checked after schema validation.~~ **Closed.** Both ingest routes verify the
   bearer token against `INGEST_TOKEN` before parsing, and fail closed with `503` if the
   token is unconfigured. Pinned by a regression test.
5. ~~`DIRECT_URL` is unset in `core/.env`.~~ **Closed** while landing the `Incident`
   migration, which it was blocking.
6. **Duplicate step numbers after resume.** Open failure — see `FAILURE_MODES.md` #4.
7. ~~The read API is unauthenticated.~~ **Closed.** Every API route requires
   `Bearer $API_TOKEN` (falling back to `INGEST_TOKEN`) and fails closed with `503` when
   unconfigured. Closed because approval — which opens a real PR — sits on the same router.
8. ~~No supervisor for orphaned runs.~~ **Closed.** `agent/supervisor.ts` sweeps runs left
   `running` by a dead process and resumes them. Conservative by design: bounded batch,
   oldest first, and only after a staleness threshold that comfortably exceeds the slowest
   step — an eager supervisor would double-drive live runs.
9. **GitHub is now wired.** `GITHUB_REPO=UdayPandey01/healix-demo-service` with a
   fine-grained PAT scoped to that one repository and limited to Contents + Pull requests —
   least privilege, because this credential is handed to code acting on model output.
10. **Nothing writes patches to disk.** The agent proposes complete file contents, but there
    is no edit-then-verify loop, so Phase 3's "a broken patch produces a retry" is still
    unproven end to end.
11. **The supervisor assumes one Healix process.** Two would sweep the same orphan. A lease
    column is the upgrade path.

### Open design decisions

1. **What is a "step"?** Settled in practice — one `Step` row per *event* (one model call,
   or one tool call), not per loop pass. Recorded in `DECISIONS.md`.
2. **Is the message array stored or derived?** Settled in practice — stored. `Run.messages`
   holds the full serialized provider-format conversation, rewritten each pass. Simple and
   exact; rows grow and the same prefix is stored repeatedly. Revisit if row size bites.
3. **Provider abstraction.** Deferred. The loop is written directly against Gemini rather
   than against an internal message type with an adapter at the edge. Revisit if a Claude
   eval run is actually attempted.
