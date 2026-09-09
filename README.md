# Healix — an autonomous Tier-1 SRE agent

Observability tells a human that something broke, then leaves them to find the fix at
2am. Healix targets the expensive half. It catches the alert, investigates root cause in
the source, verifies a patch against the real test suite in an isolated sandbox, and
opens a pull request.

**A human approves at the end. Healix never merges.**

Example of a pull request it opens:
[healix-demo-service#1](https://github.com/UdayPandey01/healix-demo-service/pull/1).

---

## The technical claim

An agent run is a **state machine persisted to Postgres after every single step**. Kill
the process mid-run, restart it, and it resumes from the last completed step. Everything
else is arranged around that guarantee — budgets, loop detection, and the supervisor all
read durable state, because a guardrail held in memory is one any crash resets.

No LangChain, no LangGraph, and no provider-supplied loop driver. If a framework owns the
state machine, the state machine isn't the project.

---

## Before you start

You need five things. Four are free.

| | Why | Cost |
|---|---|---|
| **Node 22+** | runs `core/` and the demo service | — |
| **Rust + cargo** | the sandbox that executes model-generated code | — |
| **A Postgres with `pgvector`** | durable run state + the code index. [Neon](https://neon.tech) free tier is what this was built against | free tier |
| **A Gemini API key** | [aistudio.google.com](https://aistudio.google.com/apikey) | free tier is **20 requests/day** — see the warning below |
| **A GitHub PAT** | only if you want real pull requests | free |

> **The free Gemini tier will not carry a real run.** The quota is
> `GenerateRequestsPerDayPerProjectPerModel-FreeTier`, **20 requests per day, per model**.
> One investigation costs roughly 6–12 requests, so you get one or two runs a day before
> everything returns `429`. Enable billing, or switch `MODEL` in `core/src/agent/run.ts` —
> the quota is per model, so a different one has its own budget.

---

## Quickstart — the demo

```sh
git clone https://github.com/UdayPandey01/Healix.git
cd Healix

# 1. the target system Healix will investigate (its own repo, deliberately broken)
git clone https://github.com/UdayPandey01/healix-demo-service.git demo-service
(cd demo-service && npm install)

# 2. core
cd core && npm install
cp .env.example .env      # then fill in DATABASE_URL, DIRECT_URL, GEMINI_API_KEY
npm run db:deploy         # creates the schema, enables pgvector
npm run index             # MANDATORY — embeds the target repo. ~20s
cd ..

# 3. the sandbox, in its own terminal
cd sandbox && cargo run --release      # :4000

# 4. Healix, in another
cd core && npm start                   # :8000
```

Then fire an incident at it:

```sh
curl -X POST http://localhost:8000/v1/alerts/ingest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $INGEST_TOKEN" \
  -d '{
    "incident_id": "demo-001",
    "title": "500s on /users/:id/summary",
    "service": "demo-service",
    "description": "GET /users/999/summary returns 500 with TypeError: Cannot read properties of undefined (reading '\''name'\''). Find the root cause and fix it."
  }'
```

You get a `run_id` back immediately (`202`, because the work outlives the request). Watch it:

```sh
curl -H "Authorization: Bearer $API_TOKEN" http://localhost:8000/v1/runs/<run_id>
```

**`npm run index` is not optional.** With an empty index `search_code` returns nothing,
and the agent falls back to listing and grepping — much slower, much worse. Re-run it
whenever the target repo changes.

---

## Approving a patch

The agent never opens a pull request. `open_pr` writes a proposal to the `patches` table
and stops the run at `awaiting_approval`. The GitHub credential lives behind an endpoint
the agent has no way to call:

```sh
# see what it wants to change
curl -H "Authorization: Bearer $API_TOKEN" http://localhost:8000/v1/runs/<run_id> | jq .patch

# open the PR
curl -X POST http://localhost:8000/v1/runs/<run_id>/approve \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"note":"Reviewed — correct root cause."}'

# or don't
curl -X POST http://localhost:8000/v1/runs/<run_id>/reject \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"note":"Wrong root cause."}'
```

Approving twice is safe — the branch name is derived from the run id, so a second call
finds the first call's PR instead of opening another.

This is deliberate design, not ceremony. In an earlier version the tool called GitHub
directly and "a human approves" was a *sentence in the system prompt* — a rule the model
was asked to follow. Any prompt injection in a file the agent read was one step from an
unreviewed PR. **When a rule must not be broken, move the capability rather than adding an
instruction.**

---

## Pointing it at your own repository

Everything is environment-driven; nothing about the demo service is hardcoded.

```sh
TARGET_REPO="/abs/path/to/your-repo"   # what the agent reads and patches
INDEXED_REPO="your-repo"               # index name; must match `npm run index`
TEST_COMMAND="npm test"                # what run_tests executes in the sandbox
GITHUB_REPO="you/your-repo"            # where PRs are opened
GITHUB_BASE_BRANCH="main"
```

Then `npm run index -- /abs/path/to/your-repo your-repo`.

Two honest caveats before you point this at anything you care about:

1. **The chunker is JavaScript/TypeScript only.** It splits on top-level declarations with
   a regex — no parser. Python, Go, or Rust will chunk badly. `core/src/retrieval/chunk.ts`
   is one function to replace.
2. **`TEST_COMMAND` runs in a sandbox that is only as isolated as you deploy it.** Run it
   via `docker compose` (`cap_drop: ALL`, `mem_limit`, `pids_limit`, no network egress),
   not `cargo run`, for anything real. If you run it natively it executes with your
   privileges. When containerised, set `SANDBOX_REPO_PATH=/workspace/<your-repo>` — the
   container's view of the path is not yours.

---

## Guardrails

Defaults, all overridable:

| Variable | Default | What it stops |
|---|---|---|
| `AGENT_MAX_PASSES` | 12 | runaway loops |
| `AGENT_MAX_STEPS` | 40 | runaway loops |
| `AGENT_TOKEN_BUDGET` | 200000 | runaway cost |
| `AGENT_REPEAT_LIMIT` | 3 | the same call repeated with no progress |
| `SUPERVISOR_STALE_MS` | 600000 | how long before a run counts as orphaned |
| `SUPERVISOR_MAX_ATTEMPTS` | 3 | retrying a permanently broken run forever |

Budgets are checked *before* the model call, so a breach costs nothing, and token spend is
accumulated on the run row — a budget in memory is unenforceable in a system whose whole
premise is surviving a crash.

---

## Measured results

`npm run eval:recall` — recall@5 over 22 seeded bugs in the demo service:

| strategy | hits | recall@5 |
|---|---|---|
| dense | 21/22 | **95.5%** |
| rrf | 20/22 | 90.9% |
| lexical | 15/22 | 68.2% |
| dense+rerank | — | *not yet measured* |
| rrf+rerank | — | *not yet measured* |

> The two `+rerank` rows previously showed numbers identical to their inputs. That was not
> a result: the reranker was sending a config `gemini-3.6-flash` rejects, and a silent
> `catch` returned the un-reranked order every time. It never ran. Fixed, and pending
> re-measurement. See [`FAILURE_MODES.md`](docs/FAILURE_MODES.md) #11 — the most instructive
> failure in this repo.

**Hybrid retrieval lost to dense alone**, which is the opposite of the design that was
planned. RRF sums `1/(k+rank)` across rankers, so a chunk found by *both* beats one found
only by the strong ranker — even when the strong ranker ranked it higher. Lexical (68%) is
far weaker than dense (95%), and fusing it in cost more than it added. The default is now
dense; hybrid stays selectable as the evidence.

The difference is one bug on n=22 and is inside the noise — the *mechanism* is the finding,
not the ranking. Details in [`docs/FAILURE_MODES.md`](docs/FAILURE_MODES.md) #5.

---

## Documentation

Real documentation, written as the thing was built rather than reconstructed after:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — what is actually built, with diagrams
- [`docs/DECISIONS.md`](docs/DECISIONS.md) — one entry per file that does real work: why it
  exists, what was traded away, what would break without it
- [`docs/FLOW.md`](docs/FLOW.md) — alert → investigation → sandbox → approval → PR
- [`docs/FAILURE_MODES.md`](docs/FAILURE_MODES.md) — every failure class actually hit, with
  cause and fix. The most useful file here
- [`docs/PHASE_LOG.md`](docs/PHASE_LOG.md) — what shipped in each phase and what didn't

---

## What this is not

v1 is deliberately single-tenant, against one demo repository.

Out of scope, with reasons rather than apologies: multi-tenancy (weeks of work, zero agent
capability), a Kafka ingestion buffer (built for thousands of tenants that don't exist — a
webhook and a Postgres row is correct at this scale), Qdrant (pgvector handles a few
thousand chunks and Postgres is already running), a GitHub App install flow (a PAT is
enough for one repo).

Infrastructure serving scale you don't have is not evidence of engineering judgment.

**Known gaps, stated plainly:**

- The agent proposes complete file contents but nothing applies them locally, so
  "broken patch → retry against the tests" is not yet proven end to end.
- `query_logs` doesn't exist — it reads code and runs tests, it can't query runtime logs.
- Retrieval cannot find bugs that are an *absence* — a missing timeout, an unbounded
  cache. Nothing matches text that isn't there (`FAILURE_MODES.md` #6).
- The supervisor assumes a single Healix process; two would sweep the same orphan.
- Reranking is **unmeasured** — it was silently broken through Phase 4 and the fix is not
  yet re-measured (`FAILURE_MODES.md` #11).
- The test suite occasionally flakes under parallel load against Neon: 46/46 is the typical
  result, seen three times consecutively, not a determinism guarantee (`#12`).
- No telemetry yet (Phase 6) and no UI (Phase 8).

---

## Development

```sh
cd core
npm test              # 46 tests: guardrails, approval gate, chunker, retrieval, ingest
npm run typecheck
npm run index         # re-embed the target repo
npm run eval:recall   # recall@5 per strategy, writes evals/recall-at-5.json
npm run agent         # drive one run from the CLI, no webhook
```

Tests hit the real Neon database and the live embedding API — there are no mocks, because
the things most worth testing here are the ones that only fail against a real database and
a real model.
