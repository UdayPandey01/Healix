# Healix — Failure Modes

> Every failure class actually hit, what caused it, and what changed. This is the artefact
> that separates a demo from a project: it proves the system was run enough times to
> characterize how it breaks.
>
> **Backfill notice:** the build protocol schedules this file for Phase 7. It was created in Phase 0
> to carry forward the catalogue that already existed in `DESIGN.md` rather than lose it in
> the migration. Phase 7 extends it with what the eval run surfaces; it does not start it.

---

## #1 — Fabricated file contents · FIXED

Asked to compare two files, the model read one and confidently described the other: it
claimed `res.status(400).json(...)` (Express, 400) where the file actually contained
`reply.code(422).send(...)` (Fastify, 422). Wrong framework, wrong status code, written as
if quoted.

**Cause:** nothing in the prompt forbade describing a file that had not been read, and the
filename alone was enough for the model to pattern-match a plausible implementation.

**Impact:** the highest-severity failure class for this project. Healix diagnoses real bugs
in real code; an agent that invents code writes confident, wrong patches — and a wrong patch
that passes review is worse than no patch.

**Fix:** a system instruction forbidding any description or quotation of a file not read via
`read_file` in the current conversation. Not a code change. Verified fixed — the same
question now reads both files and quotes them accurately.

**Open residue:** this is a prompt-level mitigation, not a guarantee. A cheap structural
check would be to compare quoted spans in the final answer against the contents actually
returned by `read_file` in that run's steps — the data to do it is already persisted.

---

## #2 — Cannot resume a conversation ending on a model turn · FIXED

Gemini rejects any request whose final turn is `role: "model"`:

```
400 INVALID_ARGUMENT: "Requests ending with a model turn are not supported."
```

A finished run's conversation ends with the model's answer, so replaying it is invalid.

**Cause:** resume was written as "load the messages and keep looping" without asking whether
every stored conversation is a *valid request*. It is not — a completed one never is.

**Fix:** guard on `status !== "running"` and return the stored `diagnosis` instead of calling
the API.

---

## #3 — The `saveState` / `finishRun` gap · FIXED

The interesting one, and #2's guard does not cover it.

On the final pass the loop performs three ordered writes:

```
1. contents.push(modelTurn)   → conversation now ends on a model turn
2. await saveState(...)       → that state is persisted
3. await finishRun(...)       → only now does status become "completed"
```

A crash between (2) and (3) persists `status="running"` alongside a conversation that is
structurally unresumable. The status-based guard from #2 passes, and the API call then 400s.

**Cause:** two writes that must agree, with a crash window between them, and a reader that
trusted the flag rather than the data.

**Fix:** guard on the **data, not the status flag**. If `contents.at(-1)?.role === "model"`,
the answer already exists — recover it from that turn's parts and close the run out without
calling the API.

**General lesson, and the one worth carrying into every later phase:** any two writes that
must agree can be interrupted between them. Either make them one atomic write, or make every
reader able to cope with the half-state. This project chose the second, consistently.

---

## #4 — Duplicate step numbers after resume · FIXED

`recordStep` writes immediately; `saveState` writes only at the end of a pass. A crash
between them leaves `Step` rows the conversation has no record of. On resume, `stepCount` is
stale, numbering restarts, and rows collide.

Observed on a four-event run that was killed and resumed:

```
stepNumbers in insert order: #1 #2 #1 #2 #3 #4
duplicates? YES        (6 rows for 4 events)
```

**Impact:** "steps per incident" counts retries as real work. This matters because it is a
published eval metric — the number would be wrong in the direction that flatters nothing and
confuses everything.

**Not yet decided.** Three options:

1. `@@unique([runId, stepNumber])` — let the database reject collisions. Requires deciding
   skip-vs-overwrite on conflict.
2. Derive `stepNumber` by counting existing rows on resume rather than trusting `stepCount`.
   Fixes numbering, leaves orphan rows.
3. Add an `attempt` column so retries are recorded rather than hidden.

**Fixed in Phase 5, taking option 3.** For a project whose central claim is durability,
making crash recovery visible in the data is more defensible than concealing it — a
duplicate step row is *evidence the resume worked*, provided the schema says which attempt
it belongs to. Deleting or overwriting it would destroy exactly the evidence that
demonstrates the feature.

- `Step.attempt` added, with a unique constraint on `(runId, stepNumber, attempt)`.
- `recordStep` derives the attempt by counting the rows already present rather than
  trusting a counter a crash can leave stale.
- The migration backfills historical duplicates by insertion order instead of deleting
  them. The two pre-existing collisions in this database are now attempt 2, preserved.

Verified: re-recording the same step number twice produces attempts 1 and 2 rather than a
constraint violation.

**Residual:** the count-then-insert is not atomic. Two processes recording the same step
number concurrently would both read the same count and one would hit the unique
constraint — which is the correct failure, loud rather than silent. It cannot currently
happen because one run is driven by one process, and the supervisor's staleness threshold
is what keeps that true.

---

## #5 — RRF demotes chunks that only the strong ranker found · DESIGN CHANGED

Hybrid retrieval scored *worse* than dense retrieval alone on the seeded bug set:
dense 21/22 (95.5%), RRF 20/22 (90.9%).

**Cause:** Reciprocal Rank Fusion scores a chunk as the sum of `1/(k+rank)` over the
rankers that returned it. A chunk returned by both rankers therefore beats a chunk
returned by only one, *even when the better ranker ranked it higher*. `cache-key-collision`
sits at dense rank 2, is absent from the lexical results entirely, and lands at rank 7
after fusion — out of the top 5.

RRF's assumption is rankers of comparable quality. Here lexical scores 68.2% against
dense's 95.5%, and fusing a much weaker ranker into a much stronger one costs more than
it adds.

**Change:** default strategy is now dense-only. Hybrid and reranking remain available
behind a `strategy` option — they are the evidence for the decision, and the conclusion is
corpus-specific rather than universal. Pinned by a synthetic case in
`test/retrieval.test.ts` so the behaviour stays visible.

**Caveat, stated because the sample is small:** 21 vs 20 on n=22 is a one-bug difference
and is inside the noise. The *mechanism* is the finding; the ranking of strategies is not
yet statistically meaningful. Lexical matching should start earning its place on a corpus
where identifiers repeat across many files — 45 chunks with almost no identifier
collisions is close to its worst case. Re-measure against a real repository before
concluding anything general.

---

## #6 — Retrieval cannot find a bug that is an absence · OPEN

`cache-unbounded-growth` is missed by **every** strategy — dense, lexical, RRF, and
reranked. It is the only bug no configuration retrieves.

The alert describes a symptom: *"process memory grows without bound under sustained
traffic, heap climbs until OOM."* The culprit is `memoize` in `src/cache.js`, whose defect
is that it contains **no eviction** — an unbounded `Map` that is never trimmed.

**Cause:** every retrieval method here matches on what text is *present*. The words
"memory", "heap", "leak", and "OOM" appear nowhere in the function, because the bug is the
absence of code rather than the presence of wrong code. Dense embeddings do not encode
"this function is missing a bound"; lexical search has nothing to match at all.

**Impact:** this is a whole class, not one bug — resource leaks, missing timeouts, absent
validation, and unreleased locks are all defects of omission, and they are
disproportionately the ones that page a human at 2am. A retrieval layer blind to them is
blind to the incidents that matter most.

**Not yet fixed.** Candidate directions:

1. Index a derived description per chunk — have a model write "what could go wrong here"
   at index time and embed that alongside the code. Turns absence into present text, at
   the cost of a model call per chunk and a second thing to keep in sync.
2. Give the agent a fallback path: when retrieval returns nothing convincing, fall back to
   `list_files` and read whole modules. Cheap, and matches what a human does.
3. Accept it and record it as a known limitation with a measured number attached.

Leaning toward (1) for the specific classes worth the cost, plus (2) as the general safety
net. Both are Phase 7 work.

---

## #7 — The agent could open a pull request on its own authority · FIXED

Phase 4's `open_pr` called GitHub directly. "A human approves at the end" was therefore a
*prompt instruction* — a sentence in the system prompt asking the model to behave — rather
than a property of the system. Any prompt injection in a source file the agent read, or any
model misjudgement, was one step from a pull request nobody reviewed.

**Cause:** the capability to write to GitHub sat on the agent's side of the boundary. A
guardrail the model can decide to ignore is not a guardrail.

**Fix:** the tool was split from the capability. `open_pr` now writes a row to `patches`
and halts the run; the GitHub call moved to `github/client.ts` and is reachable only from
`POST /v1/runs/:id/approve`, which the agent cannot call. Approval is now the *only* path
from a proposed patch to a pull request, enforced structurally.

**Lesson, and it generalises past this project:** when a rule must not be broken, move the
capability rather than adding an instruction. The model cannot skip a step it has no way
to reach.

---

## #8 — A crash used to reset the budget · FIXED (design)

Noted while building Phase 5's caps rather than observed in production, but it is the same
class as #3 and worth recording.

A token budget or loop counter held in process memory is reset by any crash. In a system
whose central claim is that it survives `kill -9` and resumes, that makes the cap
enforceable only by processes that never crash — an agent stuck in an expensive loop that
also crashes would restart with a full budget every time, forever.

**Fix:** both guardrails read durable state. Token spend accumulates on the run row via
`addTokens`; loop detection counts repeated calls out of the restored conversation rather
than a local tally. A test pins the resume case: repeats made before a crash still count
after it.

**General rule this project keeps re-learning:** any invariant held only in memory is an
invariant a crash can violate. The durable state is the only thing a guardrail may trust.

---

## #9 — The supervisor retried permanently-broken runs forever · FIXED

Found the first time Healix was pointed at a real GitHub repository, which is exactly the
kind of bug that only appears when something real is at stake.

The Phase 5 supervisor swept runs stuck in `running` and resumed them. Nine such runs
existed from earlier development — some with malformed stored conversations that the
Gemini API rejects with `400 INVALID_ARGUMENT` every single time. The supervisor retried
all nine on every five-minute sweep, forever.

**Impact, and it was not the obvious one.** The wasted work was not the problem; the
*quota* was. Each doomed resume consumed a request against a free-tier limit of 20, so by
the time a genuine incident arrived there was no rate limit left for it. The real run sat
at `status="running"`, zero steps, while the supervisor burned the budget on runs from
three weeks earlier. **A background retry loop starved the foreground work it exists to
protect.**

Observed in the log as the same five run ids cycling with `429` between them:

```
supervisor: resuming orphaned run 59256410… (idle since 2026-08-16, 0 steps done)
supervisor: run 59256410… failed to resume ApiError: 400 INVALID_ARGUMENT
supervisor: resuming orphaned run 14fb9ce2… (idle since 2026-08-17, 0 steps done)
model returned 429, retrying in 2000ms (1/4)
…
```

**Cause:** a retry policy with no give-up condition. The Phase 5 DECISIONS entry worried
about the supervisor being *too eager in time* — resuming runs that were merely thinking —
and set a staleness threshold for it. It did not consider being too eager in *repetition*.
Retrying is only safe when failures are transient, and nothing checked whether they were.

**Fix:**

- `Run.resumeAttempts`, incremented on every failed resume.
- `findStaleRuns` skips runs that have spent their attempt budget, so give-up is enforced
  in the query rather than depending on the caller.
- Errors are classified: `400/401/403/404/422` are permanent and give up after one
  attempt, because a conversation the API calls malformed will never become valid.
  **`429` is deliberately not in that set** — it is the transient case retries exist for.
- The nine pre-existing dev runs were retired to `abandoned` rather than deleted.

Two tests pin it: a permanent failure disappears from the sweep after one attempt, and a
transient one after the budget is spent.

**Lesson:** "retry until it works" and "retry forever" are the same code. The difference is
a give-up condition, and a shared quota means the cost of missing one lands on unrelated
work rather than on the thing that is broken.

---

## #10 — The sandbox was sent a path it could not see · FIXED

`run_tests` sent `repo_path: WORKSPACE_ROOT` — an absolute path on the *host*, like
`/Users/uday/Developer/Projects/Healix/demo-service`. In `docker-compose.yml` the sandbox
mounts that directory at `/workspace/demo-service`, so inside the container the host path
does not exist and every run would have failed with "could not start command".

**Cause:** two processes with different filesystem views, and one of them assuming its own
view was universal. It went unnoticed because the sandbox had only ever been exercised with
the container down, where the tool's error is the indistinguishable "Sandbox unreachable".

**Fix:** `SANDBOX_REPO_PATH`, defaulting to `WORKSPACE_ROOT` (correct when the sandbox runs
natively, where the two views coincide) and set to `/workspace/demo-service` in
`docker-compose.yml`.

**Verified** by running the real suite through the sandbox: 39 tests, 23 failing, exit
code 1, 2.3s.

**Worth noting for later:** the same class of bug is waiting in any future feature that
hands a path across the sandbox boundary. The boundary translates paths, and only one side
knows it.

---

## #11 — A silent catch hid a reranker that never ran · FIXED (and it invalidated a published result)

The worst kind of bug on this project so far, because it did not look like a bug — it
looked like a measurement.

`rerank()` called Gemini with `config: { thinkingConfig: { thinkingBudget: 0 } }`, set to
keep reranking cheap. **`gemini-3.6-flash` rejects that with `400 INVALID_ARGUMENT`.** The
function's `catch` — added deliberately, so a failing reranker degrades to slightly worse
results rather than to none — swallowed the error and returned the un-reranked order.

So the reranker threw on every single call and silently did nothing, for the whole of
Phase 4.

**How it was found:** a probe of the daily quota returned `400` with `thinkingBudget: 0`
and `429` without it — same key, same model, same moment. `400` is returned *before* the
quota check, so the request was structurally invalid, not rate-limited.

**What it invalidated.** The Phase 4 recall table reported:

```
dense          21/22   95.5%
dense+rerank   21/22   95.5%     <- identical
rrf            20/22   90.9%
rrf+rerank     20/22   90.9%     <- identical
```

Those pairs are identical to the *bug*, not to the decimal. `+rerank` returned exactly its
input. The conclusion drawn at the time — "the reranker changed nothing in either direction
while costing a model call per search" — was wrong twice over: it changed nothing because
it never ran, and it cost nothing because the request failed before generating anything.

The **dense vs rrf vs lexical** comparison is unaffected; none of those touch the model, and
the RRF demotion mechanism in #5 stands. Only the two `+rerank` rows are void.

**Fix:**

- `thinkingConfig` removed. Reranking now runs with the model's default thinking.
- The fallback stays — degrading gracefully is still correct — but it is no longer silent:
  it logs, and `rerankHealth()` counts failures.
- `measureRecall` reports `rerankFailures`, and the CLI prints a warning that the
  `+rerank` rows are not measurements when it is non-zero.

**Lesson, and it is the sharp one:** a catch-all that produces a *plausible* fallback value
converts a hard failure into a silent wrong answer. The graceful degradation was right;
making it unobservable was not. Any fallback that can mask a component being entirely
broken needs a counter attached, especially when its output feeds a number that will be
published.

**Still owed:** the `+rerank` rows must be re-measured with a working reranker. Blocked on
the Gemini free-tier daily quota (20 requests; a full sweep needs ~44).

---

## #12 — Test suite flakes under parallel load against Neon · OPEN (low severity)

One full `npm test` run failed a single test in `test/ingest.test.ts`; the same file then
passed four times in isolation and the full suite passed three consecutive times
afterwards. No assertion detail was captured before the process exited.

**Suspected cause:** `node --test` runs the five test files in parallel, each with its own
Prisma client, against one Neon instance over the pooled endpoint. A transient
`SocketTimeout` from that pooler was observed earlier during a long-running script. This is
consistent with connection pressure rather than a logic error.

**Not fixed.** Recorded rather than chased because it has not reproduced in seven
subsequent runs. If it recurs, the first move is to capture the assertion, then either
serialise the DB-touching files (`--test-concurrency=1`) or give each file its own client
with a smaller pool.

**Honesty note:** the "46/46 pass" figure quoted elsewhere is the *typical* result, observed
three times consecutively. It is not a guarantee of determinism.
