# Healix — demo runbook

A 6–8 minute live demo, in the order that makes the point. Rehearsed end to end on
2026-09-10; every command here has been run against the real system.

---

## The one risk that will ruin the demo

**Model quota.** The Gemini free tier allows **20 requests per day, per model**. One
investigation costs 6–12. So you get roughly **two live runs per model per day**, and a
rehearsal spends the same budget as the real thing.

Mitigations, in order:

1. **Rehearse on one model, demo on the other.** Quota is per model:
   `GEMINI_MODEL=gemini-3.6-flash` for the rehearsal, `gemini-3.8-flash` for the demo (or
   the reverse — check which is healthy first, see pre-flight).
2. **Always have a completed run to fall back on.** If the live run 429s, open a finished
   run's trace instead. The trace is the interesting part; the audience cannot tell whether
   it finished thirty seconds or thirty hours ago.
3. `gemini-3.8-flash` intermittently returns `503 "high demand"` — that is Google-side
   capacity, not your quota. The retry logic rides it out, but it makes runs slow.

---

## Pre-flight (do this 10 minutes before, not live)

```sh
cd core

# 1. Which model is actually healthy right now? Pick the demo model from this.
npx tsx -e 'import "dotenv/config";
import { GoogleGenAI } from "@google/genai";
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
for (const m of ["gemini-3.8-flash","gemini-3.6-flash"]) {
  try { await ai.models.generateContent({ model: m, contents:[{role:"user",parts:[{text:"ok"}]}] }); console.log(m, "usable"); }
  catch (e) { console.log(m, e.status); }
}'

# 2. The target repo must match its remote, or the stale-base guard will refuse the PR.
cd ../demo-service && git pull --ff-only origin main && cd ../core

# 3. Re-index if the target repo changed (~30s). Skip if nothing changed.
npm run index

# 4. Close any open Healix PR for the bug you plan to demo, so the fix is available again.
```

**Pick a bug that is still broken.** `evals/golden-bugs.json` lists all 22. Good ones:

| Bug | Why it demos well |
|---|---|
| `orders-pagination-offset` | one-line fix, instantly understandable |
| `auth-role-substring` | `nonadmin` matching `admin` — an audience feels this one |
| `pricing-additive-discounts` | two discounts produce a negative price |
| `inventory-toctou-oversell` | a genuine race condition |

Avoid `cache-unbounded-growth` — retrieval cannot find it (a known limitation, worth
*mentioning* but not demonstrating live).

---

## Terminal layout

Three panes, all visible at once. The point is that the audience sees the agent working.

```
┌─ pane 1: sandbox ────────────┬─ pane 2: Healix ──────────────┐
│ cd sandbox                   │ cd core                       │
│ cargo run --release          │ GEMINI_MODEL=gemini-3.8-flash \│
│                              │   npm start                   │
├─ pane 3: you ────────────────┴───────────────────────────────┤
│ where you curl from                                          │
└──────────────────────────────────────────────────────────────┘
```

Health check both before you start talking:

```sh
curl -s localhost:4000/health   # sandbox -> ok
curl -s localhost:8000/health   # Healix  -> {"status":"ok"}
```

---

## The demo

### 1. Show the broken system (30s)

```sh
cd demo-service && npm test
```

> "22 deliberate bugs. 22 failing tests. This is the service Healix is responsible for."

### 2. Fire the incident (30s)

```sh
export ING=$(grep '^INGEST_TOKEN' core/.env | cut -d'"' -f2)

curl -X POST http://localhost:8000/v1/alerts/ingest \
  -H "Content-Type: application/json" -H "Authorization: Bearer $ING" \
  -d '{
    "incident_id": "live-demo-001",
    "title": "First page of /orders skips the earliest records",
    "service": "demo-service",
    "description": "GET /orders?page=1&limit=3 returns orders starting at id 104 instead of 101, so the first page skips the earliest records. The failing test is \"page 1 returns the first records\" in test/orders.test.js. Find the root cause in the source, fix that one function, and open a PR. Change only what this bug requires."
  }'
```

Returns `202` with a `run_id` **immediately**.

> "202, not 200. The investigation outlives the request. The run row is already in
> Postgres before any work starts — if this process dies right now, nothing is lost."

In real use this comes from Alertmanager, not curl. Say that; don't spend time on it.

### 3. Watch it work (2–3 min)

Point at **pane 2**. Real output from the rehearsal:

```
pass 1: run_tests({})                      -> 21894 chars
pass 2: read_file({"path":"test/orders.test.js"})
pass 3: read_file({"path":"src/orders.js"})
pass 4: grep_code({"query":"listOrders"})
pass 5: read_file({"path":"src/server.js"})
pass 6: open_pr({...})
```

> "It ran the tests in an isolated Rust sandbox first — it looks at the real failure, not
> just the alert text. Then it read the test to learn the expected behaviour, then the
> source. Every one of those steps is a row in Postgres before the next one starts."

### 4. The durability claim — the headline (1 min)

**This is the most technically interesting thing in the project. Do not skip it.**

While a run is in flight, kill Healix in pane 2:

```
Ctrl-C          # or: pkill -f "tsx src/index.ts"
```

Then restart and resume:

```sh
cd core && npm start
```

The supervisor picks up the orphaned run within `SUPERVISOR_STALE_MS`, or resume it now:

```sh
npm run agent -- <run_id>
```

> "It resumed from the last completed step. The conversation is the agent's whole memory
> and it lives in Postgres, so the model cannot tell the process died. No framework owns
> this loop — that's the point of the project."

This was verified for real: a run died on a 503 at step 10, resumed, and continued to
step 12.

### 5. The approval gate (1 min)

```sh
export API=$(grep '^API_TOKEN' core/.env | cut -d'"' -f2)
export RUN=<run_id>

curl -s -H "Authorization: Bearer $API" localhost:8000/v1/runs/$RUN | jq '.status, .patch.title'
```

Status is `awaiting_approval`.

> "The agent has stopped. It wrote the patch to the database and it cannot go further —
> the GitHub credential lives behind an endpoint the agent has no way to call. Earlier
> this was a sentence in the prompt asking the model to wait for a human. That's not a
> guarantee, it's a request. Now the model can't skip the step because it can't reach it."

Show the proposed diff:

```sh
curl -s -H "Authorization: Bearer $API" localhost:8000/v1/runs/$RUN | jq -r '.patch.body'
```

### 6. Approve — real PR (1 min)

```sh
curl -X POST localhost:8000/v1/runs/$RUN/approve \
  -H "Content-Type: application/json" -H "Authorization: Bearer $API" \
  -d '{"note":"Reviewed - correct root cause."}'
```

Open the returned `pr_url` in a browser. Real PR, real one-line diff.

> "A human approved. Healix opened it. Healix will never merge it."

Approve twice to show idempotency — same PR, no duplicate:

```sh
curl -X POST localhost:8000/v1/runs/$RUN/approve -H "Authorization: Bearer $API"
# -> {"status":"already_opened", ...}
```

### 7. Numbers (30s)

```sh
cat evals/recall-at-5.json | jq '.byStrategy | map_values(.recall)'
```

> "recall@5 is 95.5% over 22 seeded bugs. And the interesting part: I built hybrid
> BM25+dense with RRF fusion because that's the textbook answer, measured it, and it was
> *worse* than dense alone. RRF promotes chunks both rankers found, so a weak lexical
> ranker drags down a strong dense one. I default to dense now — measured, not assumed."

---

## If something breaks live

| Symptom | Say this, then do this |
|---|---|
| `429` quota | "That's the free tier — 20 requests a day." Switch to a finished run's trace. |
| `503` on 3.8 | "Model's overloaded server-side; the retry logic handles it." Restart with `GEMINI_MODEL=gemini-3.6-flash`. |
| Sandbox unreachable | `cd sandbox && cargo run --release`. `run_tests` fails cleanly — the agent keeps going without it. |
| Approve returns `stale base` | The target repo moved. `cd demo-service && git pull` — this guard exists because a stale checkout once made a PR silently revert a merged fix. **That story is worth telling.** |
| Agent picks a different bug | Fine. Follow it. An agent that investigates rather than pattern-matches is the better story. |

---

## Questions you will get

**"How is this different from Copilot / Cursor?"**
Those start from a human at a keyboard who already knows something is wrong. This starts
from a Prometheus alert at 2am and ends at a reviewed PR, with no human in the middle.

**"What stops it doing something destructive?"**
It cannot write to disk. It cannot merge. Test execution is in a separate Rust process with
`cap_drop: ALL`, memory and pid limits, and no network egress. The GitHub credential is
fine-grained and scoped to one repository. And the approval gate is structural, not a
prompt instruction.

**"What doesn't work?"**
Answer honestly — `docs/FAILURE_MODES.md` has twelve entries and it is the strongest
artifact in the repo. The best ones to volunteer:

- Retrieval cannot find bugs that are an *absence* — a missing timeout, an unbounded cache.
  Nothing matches text that isn't there.
- The reranker was silently broken for a whole phase because a defensive `catch` turned a
  hard failure into a plausible-looking fallback, and I reported its non-effect as a
  measurement.
- Nothing applies patches locally yet, so the agent proposes fixes it hasn't verified.

Volunteering these is stronger than being caught by them.
