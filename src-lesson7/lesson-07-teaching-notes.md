# Lesson 7 — Stream Processing I (Windows & Watermarks): Teaching Notes

> Live-teaching companion for `slides/lesson-07.html` (36 slides) and `src-lesson7/`.
> Built *with* an AI pair while walking the deck and running every demo for real on
> PySpark 4.0.1 / Python 3.14 / JDK 17. Each movement has: **the one thing to land**,
> **predict-first beats**, **demo commands**, **likely questions**, **timing**, and
> **the transition line**. Numbers in here are *measured on this repo's seeded topic*,
> not copied off the slide — where the live result differs from the deck, this file
> says so out loud (that honesty is the lesson).

---

## 0. The spine of the whole lesson (say this out loud, more than once)

Three threads carry everything. If students leave with only these, you won.

1. **"Kafka is a pipe, not a calculator."** L6 gave them an ordered, replayable log
   and a hand-written poll loop. A loop that *prints* is not analytics. The moment
   you ask "revenue in the last 5 minutes" you need something that consumes,
   transforms, and **aggregates continuously** — a stream processor. That's today.

2. **"The micro-batch loop IS last week's poll loop, honored."** Don't let Spark feel
   like magic. `poll → process → write → commit offsets` is exactly what they wrote
   by hand in L6; Spark just runs it on a trigger and stores offsets+state in the
   **checkpoint** (which plays the role of `__consumer_offsets`, plus state). Every
   feature today — windows, watermarks, output modes — is bookkeeping bolted onto
   that loop.

3. **"Three independent dials, and confusing them is THE streaming bug."**
   - **window** = the *question* ("per 5 minutes"). The grain.
   - **trigger** = how *often* you look. Latency only — never correctness.
   - **watermark** = how long you *wait* for stragglers (and how much state you keep).

   People who confuse these ship 1-second triggers "for accuracy" (waste) or 5-minute
   windows "for speed" (wrong answers). By the end they can read an interview spec
   straight into the three dials.

Recurring callbacks to name explicitly when they appear:

| Concept today | Callback to a past lesson |
|---|---|
| Poll → process → commit, on a trigger | L6 poll loop + offset commit |
| Checkpoint = offsets + state, atomic | L6 `__consumer_offsets`; L8 will own this |
| Out-of-order arrival is the adversary | L6 `produce_out_of_order.py` — today's lab rat |
| Defaults fail **silently**; you add the alarm | L5 polling drift, L5 swallowed column |
| Unbounded retention, nobody watching → blowup | L5 abandoned replication slot |
| Event time vs wall clock | L5/L6 CDC lag, consumer lag |
| Stream for speed, **batch for truth** | L4 idempotent batch becomes the nightly audit |
| One operational number per block | L1 TPS, L5 slot lag, L6 consumer lag → **`numRowsDroppedByWatermark`** |

The single sentence to repeat: **"The watermark is a promise — and the promise cuts
both ways: it lets Spark *forget* (bound state) and it lets Spark *drop* (lose late
data, silently)."**

---

## 1. Pre-class setup checklist

> **Copy-paste contract for every command in this file:** run from inside
> `src-lesson7/`, and `JAVA_HOME` is auto-detected (`config.py` finds Homebrew's
> openjdk@17), so no `export` is needed. Each command is meant to be pasted and run
> as-is. All flags shown match the scripts' actual `argparse` — no guesswork.
> Every script **narrates itself**: a `banner()` up front (what's happening + what to
> watch) and a `lesson()` block at the end (the one idea it just demonstrated) — so a
> student running it solo gets the same framing the room does. Streaming scripts print
> their `lesson()` when you Ctrl-C them.

Run these BEFORE class. The first Spark run downloads the Kafka connector JAR from
Maven (slow once, cached after) — never do that cold in front of the room.

```bash
cd src-lesson7

# 1. JDK 17. Spark 4 dropped Java 8/11. config.py AUTO-DETECTS Homebrew's openjdk@17,
#    so no export is needed if you installed it with brew. Only export if yours is
#    elsewhere (run `brew install openjdk@17` first if you don't have it at all):
# export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# 2. One single-broker Kafka (KRaft, no ZooKeeper) on localhost:19092.
docker compose up -d                     # container: kafka-l7 ; healthcheck → wait "healthy"

# 3. Warm the JAR cache + sanity-check Spark sees Java (prints a version = done).
uv run python -c "from pyspark.sql import SparkSession; \
  print(SparkSession.builder.master('local[*]').getOrCreate().version)"   # → 4.0.1

# 4. Seed the topic: 10k orders, 5% late with a real exponential tail past 10 min.
uv run python src/seed_events.py
```

- **Single broker on purpose.** L6 ran three for durability (RF=3, ISR). L7 is about
  *compute*; replication is irrelevant and a second JVM just steals RAM from Spark.
  The broker still advertises `localhost:19092`, identical to L6's `kafka-1`, so every
  slide command works unchanged. **You cannot run L6's cluster and this at once** —
  same port. (If L6's `kafka-1` is stopped, that's why; `docker start kafka-1` to
  restore it later, after `docker compose down` here.)
- **Spark runs on the HOST**, not in Docker — `uv run python …`, `master local[*]`,
  in-process JVM. Only Kafka is containerized. The visible micro-batch loop *is* the
  lesson, which is why we use the full `pyspark` (bundled JARs), not `pyspark-client`.
- **Spoiler reveals:** press **`r`** (or click) on a `.spoiler` block. Predict FIRST.
- Have **3 terminals** pre-opened and labeled: `pipeline`, `progress`, `inject/seed`.
- **Reset between runs:** delete `ckpt/` to restart from `earliest`; re-run
  `seed_events.py` (it appends — delete the topic to fully reset event times).

### Pre-flight: run every demo once, the morning of

Spark JVM cold-start is ~10–20s each; the numbers below were captured live so you
know what "right" looks like before the room sees it (and so a bad broker is obvious):

```bash
uv run python src/stream_revenue.py --watermark 10        # windows fill, wm advances
uv run python src/experiment_sliding.py --seconds 15       # tumbling vs sliding ratio
uv run python src/experiment_no_watermark.py --seconds 12  # Part A refusal, Part B climb
uv run python src/stream_to_kafka.py --reset               # then --readback in T2
```

---

## 2. Timing budget (matches the deck's structure)

The deck is one concept hour + two live "Parts" + synthesis. Roughly a 3-hour class:

| Block | Slides | Minutes | Notes |
|---|---|---|---|
| M1 · Mental model | 1–7 | ~20 | Move fast — DataFrames they know. Land the micro-batch loop. |
| M2 · Time (the hard idea) | 8–15 | ~35 | The heart of the lesson. Event time, the question, watermark, the dial. |
| — break — | | | |
| M3 · Build it (live) | 16–21 | ~35 | Read → window → output modes → trigger independence. |
| M4 · Break it (live) | 22–28 | ~40 | The silent drop, the metric, the sweep, no-watermark, sliding, sink. |
| M5 · Synthesis & close | 29–36 | ~20 | Three dials, stream+batch, L8 tease, take-home, annexes. |

If you're short on time, the two cuts that hurt least: session windows (slide 11,
"name it and move on") and `stream_to_kafka.py` (slide 28 — describe it, run the
`--readback` only if a terminal is already warm). **Never cut:** event time (8–9),
the watermark mechanism (13–14), the silent drop + the metric (23–24).

---

<!-- ════════════════════════════════════════════════════════════════════════ -->
# MOVEMENT 1 — The mental model (slides 1–7)

**The one thing to land:** *"Streaming = batch you already know, but the input never
ends — and the engine is just your L6 poll loop on a trigger."* Strip the mystique.

### Slide 1 — Cover: "Compute in flight."
The arc of the course in one line: CDC *captured* changes (L5), Kafka *transports*
them (L6), but a pipe answers no questions. "Revenue in the last five minutes" means
aggregating a table that never ends. Today: windows, event time, the watermark.

### Slide 2 — Recap L6: "The data flows. Nobody's *computing.*"
The three stats (∞ retained, 0 aggregations, "?" revenue) are the gap. Say the
promissory note out loud: *"Last week I told you you'd never hand-write a poll loop
again. Today Spark writes it for you."*

### Slide 3 — "Source → operators → sink."
The universal topology. **Stateless** (map/filter/flatMap): output depends only on the
current event — trivially parallel, nothing to checkpoint. **Stateful** (aggregations,
joins, sessions): output depends on history — state must survive crashes, which is L8.
Today we touch the *simplest* stateful case: the windowed aggregate. Land: **this DAG
is identical in Spark, Flink, Kafka Streams — the mental model transfers, the APIs differ.**

### Slide 4 — "Spark, on purpose."
Three reasons, in priority order: (1) **they'll meet it at work** — most-deployed
stream processor; (2) **they already think in DataFrames** (pandas, DuckDB from L3) —
the leap is one sentence: *"batch, but the input never ends"*; (3) **micro-batch makes
time visible** — each batch is a discrete step you can watch windows close in. Flag
honestly: micro-batch is also a latency floor — we *measure* whether that floor matters
in L9 (Spark vs Flink, CDFs). Today it's the teaching instrument.

### Slide 5 — "One word turns the table infinite."
`spark.read` (finite snapshot) vs `spark.readStream` (infinite, keeps ingesting). Same
six Kafka columns they saw in L6: key · value (binary!) · topic · partition · offset ·
timestamp. **`value` is raw bytes you must parse — exactly the `json.loads` from their
poll loop.** Almost every DataFrame op works unchanged on the streaming one.

### Slide 6 — "An infinite loop of batch jobs." (the keeper of M1)
Walk the pseudocode line by line and **map each line to L6 by name**:
`poll(since=offsets)` = their poll loop; `run_batch_query` = plan→optimize→execute;
`save(offsets+state, checkpoint)` = their offset commit, durably, plus state. The
**checkpoint dir = `__consumer_offsets` + state.** Then plant the trigger as a *latency
knob* — "shorter = fresher, longer = bigger batches that amortize planning; hold that
thought, we'll prove it doesn't change correctness." (Pays off slide 21.)

### Slide 7 — "Stateless: boring on purpose."
`select`/`from_json` = map, `filter` = filter, `explode` = flatMap. **Identical to batch
code — that's the point.** No memory between events = nothing to checkpoint, nothing to
lose. The hard part begins when the answer depends on **more than one event**:
aggregation. And aggregation over an endless table forces the question of **time**.

### Predict-first beats in M1
- None heavy yet. One rhetorical: "What's different between `read` and `readStream`?"
  (Answer: four letters and the fact that it never finishes.)

### Likely student questions (M1)
- *"Is micro-batch 'real' streaming or a hack?"* — It's a real, deliberate design
  point: bounded latency in exchange for high throughput + simple recovery. Flink does
  record-at-a-time; we contrast them with measurements in L9, not adjectives.
- *"Why not just use a database / materialized view?"* — Those poll or recompute; a
  stream processor maintains the aggregate incrementally and bounds state with the
  watermark. (And dashboards over MVs hit the same event-time problem — slide 9.)
- *"Spark Connect / `pyspark-client`?"* — Needs a separate server and has gaps in the
  streaming progress/listener APIs we rely on for `numRowsDroppedByWatermark`. We use
  the full engine so `local[*]` runs an in-process JVM. (See `pyproject.toml` comment.)

### Timing M1: ~20 min. Don't linger — they know DataFrames. Spend the saved minutes on M2.

### Transition line → M2
*"Stateless was the easy 20 minutes. The moment you aggregate over a stream that never
ends, you have to answer one question Spark cannot answer for you: **which clock?**"*

---

<!-- ════════════════════════════════════════════════════════════════════════ -->
# MOVEMENT 2 — Time, the hard idea (slides 8–15)

**The one thing to land:** *"Event time is the business truth; processing time is an
accident of your pipeline's mood. Window by event time, and the watermark is the
mechanism that decides when a window is 'done' — trading completeness against latency."*
This is the intellectual core of the whole lesson. Slow down here.

### Slide 8 — "When it *happened* vs when you *saw it.*"
Two columns: **event time** = `created_at`, stamped by the source, the business truth.
**Processing time** = wall clock when Spark sees it — after network, consumer lag, CDC
lag. Land the danger: *window by processing time and your numbers describe when your
pipeline felt like working, not when business happened.* Call back to L6: their shuffled
`produce_out_of_order` topic arrived in the wrong order **on purpose** — "that topic was
a curiosity last week; today it's the adversary."

### Slide 9 — "Flash sale, 12:00–12:05. Pipeline lags 30s." (PREDICT-FIRST)
**Beat:** before reveal, ask the room: *"Marketing wants sale-window revenue. You window
by processing time, pipeline lags 30s. What do the two windows report?"* Take two
guesses, then reveal:
- `[12:00,12:05)` — **misses the last 30s of the sale** → undercounted.
- `[12:05,12:10)` — **+30s of sale revenue it never earned** → post-sale inflated.

Land hard: *not slightly wrong — **structurally** wrong, and it gets worse exactly when
traffic spikes (lag grows when you care most).* This is why **every** windowing
expression today groups by `created_at`.

### Slide 10 — "Tumbling: every event, one window."
The window is **just a derived group-by key** — `window(col("created_at"), "5 minutes")`
yields a `{start, end}` struct. The aggregation machinery is the same `GROUP BY` they've
used since L3. *What's new: the groups stay **open** until time says otherwise.*

### Slide 11 — "Sliding overlaps. Session listens."
**Sliding:** `window(t, "10 minutes", "2 minutes")` → each event lands in size/slide = 5
windows → 5× the state (we'll measure this, slide 27). **Session:** `session_window`,
boundary defined by a gap of inactivity — *can't even say where it ends without waiting.*
Every window type eventually asks the same question… (segue to 12). If short on time:
name session windows, don't dwell.

### Slide 12 — "It's 12:06. Is [12:00, 12:05) *done?*" (the pivot slide)
The whole lesson hinges here. Maybe it's done — unless a 12:04 event is still in transit.
**Emit now = fast but maybe wrong. Wait = right but late. Wait how long? Forever?**
Completeness vs latency — *you cannot have both*, so you need a mechanism that picks a
point on the line. Let the question sit before you name the answer.

### Slide 13 — "The watermark is a *promise.*"
*"No event older than T will arrive anymore."* Spark computes it from the data:
`max(event time seen) − allowed lateness`. One line: `.withWatermark("created_at",
"10 minutes")`. Two consequences, both must be said:
1. It lets Spark **emit & forget** — finalize a window, drop its state. Without it,
   every window stays open and state grows until OOM.
2. It lets Spark **drop** — a late event whose window already closed is discarded
   **silently. No error, no log, no DLQ by default.** *The promise cuts both ways.*

### Slide 14 — "Watch it advance, batch by batch." (walk the table slowly)
Read the table row by row. The keeper insight is the last line: **what drives the clock
is the data's own timestamps, not the wall.** A 5-minute *consumer-lag* spike drops
*nothing* — the backlog still carries its event times; the watermark waits with them and
several windows close at once when it catches up. Lag ≠ lateness. (This distinction trips
everyone; say it twice.)

### Slide 15 — "Lateness is a number *you* choose." (the dial table)
1 min → fast/aggressive drops/small state. 10 min → moderate. 30 min → slow/nearly
complete/large state. none + append → never emits, unbounded → OOM. Land: **there is no
correct value — there's a correct value for a business question.** Dashboard tolerates
drops; regulatory reporting doesn't. *"You'll fill this table with your OWN measurements
in an hour — and the take-home makes lateness a CLI flag for exactly this reason."*

### Predict-first beats in M2
- **Slide 9** (flash sale): the marquee prediction. Don't skip it.
- Slide 12 is a *rhetorical* prediction — let them feel the tradeoff before 13 answers it.

### Likely student questions (M2)
- *"Where does Spark get the watermark if events are out of order?"* — `max(event time
  seen so far)`, monotonic, **global across all partitions** — it only ever moves
  forward. Out-of-order is fine; the max is still the max.
- *"What if one partition stalls?"* — The watermark follows the global max, so a stalled
  partition won't hold it back — and **that partition's events may be dropped when it
  finally catches up.** Rare in the lab, real in production (slide 35 gotcha).
- *"Can I recover dropped events?"* — Not from the stream's answer. They still exist in
  Kafka / Postgres — the **batch pipeline reconciles them nightly** (slide 30). "Dropped"
  means dropped *from the stream's answer*, not destroyed.
- *"Why not just set watermark huge and never drop?"* — State grows with it, and append
  latency grows with it: 30-min watermark = results wait ~30+ min. The dial has a cost on
  both ends.

### Common misconceptions to preempt (M2)
- "Watermark = trigger." No — watermark is event-time patience; trigger is wall-clock
  cadence. Orthogonal (proven slide 21).
- "Late = error." No — late = silently dropped. The *whole* danger is that it's silent.
- "Lag causes drops." No — lag delays; only *event-time* lateness beyond the watermark
  causes drops.

### Timing M2: ~35 min — the most important block. Spend it on 9, 12, 13, 14.

### Transition line → M3
*"Enough theory. Same orders, same broker, the L6 disorder included — let's build the
pipeline and watch the watermark advance with our own eyes."*

---

<!-- ════════════════════════════════════════════════════════════════════════ -->
# MOVEMENT 3 — Build the pipeline, live (slides 16–21)

**The one thing to land:** *"Three lines carry the lesson — watermark (how long to wait),
window (what to ask), trigger (how often to look) — and they are three independent dials."*

### Slide 16 — Divider: "Build the pipeline."
Set the frame: Kafka in, windowed revenue out, the L6 disorder is now the adversary.

### Slide 17 — Setup: "One pip install. One JVM. No cluster."
`docker compose up -d` (the broker), `uv add pyspark` (needs Java 17 → `JAVA_HOME`),
version check, `seed_events.py`. **The seeder bakes in 5% late events** — the test
population for everything with watermarks. If the version check fails it's `JAVA_HOME`,
99% of the time → annex 35, don't burn 30 min.

> **Demo — the seed, with its lateness histogram (run live or show pre-run):**
> ```bash
> uv run python src/seed_events.py
> ```
> It prints a `minutes-late → count` histogram. The point to show: the late population
> has a **real exponential tail past 10 minutes** (mean ~7 min, clamped 1–45). That tail
> is *why* a 10-minute watermark still drops a few stragglers and a 30-minute one drops
> ~zero. A naive uniform 1–10 would drop *exactly nothing* at a 10-min watermark and the
> whole sweep would be boring — this is a deliberate data-design choice worth naming.

### Slide 18 — Phase 1: "Read, parse, prove it flows."
`readStream.format("kafka")` → `from_json(value.cast("string"), schema)` → console sink,
5s trigger. **Land: `startingOffsets="earliest"`** — Kafka's default is `latest` =
you see NOTHING on a pre-seeded topic and burn a confused half hour. **Debug at the parse
stage, not inside a windowed aggregate** — a broken parse there is misery to diagnose.

> **Demo:**
> ```bash
> uv run python src/stream_revenue.py --watermark 10
> ```
> (`stream_revenue.py` does read+parse+window in one file; for slide 18 you're pointing
> at the parsed rows printing every few seconds before the aggregate. In `config.py`,
> `read_orders()` is exactly the slide-18 ingestion, reused everywhere.)

### Slide 19 — Phase 2: "Revenue per 5-minute window." (the core)
The three-line core: `.withWatermark("created_at","10 minutes")` ·
`.groupBy(window(col("created_at"),"5 minutes"))` · `.agg(sum/count/avg)`. Then the
output: `outputMode("update")`. **Three independent dials; mixing them up is the classic
streaming bug.** Watch the console: windows fill, the watermark advances batch by batch.

> **Live note — the `--max-per-trigger` knob (a genuine insight, not on the slide):**
> The script throttles to 300 rows/batch by default. *Why it matters:* the watermark only
> updates **between** batches, so a wide batch that swallows the whole topic at once
> advances the watermark in one jump and **drops nothing** — the dial looks dead. Small
> batches keep the watermark advancing visibly and make late drops actually happen. If a
> student says "I set watermark 1 minute and nothing dropped," the first question is
> *"how big are your batches?"* Set `--max-per-trigger` huge to demonstrate drops
> vanishing; small to sharpen the dial.

### Slide 20 — Live: output modes. "Switch `update` to `append`." (PREDICT-FIRST)
**Beat:** *"`update` emits partial results every batch — you just watched windows fill.
Restart in `append`, which only emits FINAL results. When does the first row print?"*
Predict, then run `--mode append`. Reveal: **silence** until the watermark passes a
window's END — first output ≈ window(5m) + lateness(10m) of *event time* later. That
silence isn't a bug — it's the lateness tolerance **working**. `append` = immutable rows
for sinks that can't update; `update` = upserts for dashboards/dev; `complete` = re-emit
the whole table each batch (tiny demos only). **Dev in `update`** — append's silence looks
exactly like a broken pipeline while you iterate.

> **Demo:**
> ```bash
> uv run python src/stream_revenue.py --watermark 10 --mode append   # silence, then finals
> uv run python src/stream_revenue.py --watermark 10 --mode update    # partials every batch
> ```

### Slide 21 — Live: trigger vs window. "Trigger 1s → 30s. Do the totals change?" (PREDICT-FIRST)
**Beat:** three runs, `--trigger 1`, `5`, `30`. *"Predict the effect on the window
totals."* Reveal: **identical totals, always.** Trigger 1s → many tiny batches, results
trickle; 30s → few big batches, same numbers. **The trigger controls latency. The window
controls the question. They never trade.** This is the receipt that the two dials are
orthogonal — the payoff of the slide-6 plant.

> **Demo:**
> ```bash
> uv run python src/stream_revenue.py --watermark 10 --trigger 1
> uv run python src/stream_revenue.py --watermark 10 --trigger 30
> ```
> (Totals match to the cent; only batch count and freshness differ.)

### Predict-first beats in M3
- **Slide 20** (append silence) and **Slide 21** (trigger independence) — both reveal
  via spoiler. These are the two that change how students *configure* pipelines forever.

### Likely student questions (M3)
- *"First run hangs for a minute?"* — Not a hang: `spark.jars.packages` is pulling the
  Kafka connector from Maven. Once. Cached after.
- *"Console shows nothing."* — `startingOffsets` defaulted to `latest`. Use `earliest`.
- *"Why a separate checkpoint discussion later?"* — `stream_revenue.py` deliberately has
  **no fixed checkpoint** so each run reprocesses from earliest (apples-to-apples sweep).
  Checkpoints + crash-resume come in `stream_to_kafka.py` (slide 28).

### Timing M3: ~35 min. The two predict-first reveals (20, 21) are the spend.

### Transition line → M4
*"We built it and it works. Now let's break it — on purpose — and watch it lose money
without saying a word."*

---

<!-- ════════════════════════════════════════════════════════════════════════ -->
# MOVEMENT 4 — Break it: lose data (slides 22–28)

**The one thing to land:** *"The watermark drops late data silently — and the one number
that tells on it is `numRowsDroppedByWatermark`. Zero means safe; nonzero means you're
shipping wrong totals knowingly, which is a business decision the moment you can see it."*

### Slide 22 — Divider: "Now lose some data."
Frame: the promise gets broken. Late events, the silent drop, the metric that catches it,
and the failure mode where forgetting one line means the pipeline never speaks again.

### Slide 23 — Live: the silent drop. "50 orders arrive. Where do they land?" (PREDICT-FIRST)
**Beat:** pipeline's running, watermark far past 12:10. Inject 50 orders of $999.99 each,
stamped `created_at=12:05` — a window emitted long ago. *"Predict what the pipeline does."*
Reveal: **nothing.** No console output, the `[12:05,12:10)` window is unchanged (state
gone), no error, no log. **$49,999.50 — vanished, silently.** This is the **third silent
lie of the course**: polling drift (L5), the swallowed column (L5), now the watermark eats
revenue. The pattern: **defaults fail silently; you add the alarm.**

> **⚠️ READ THIS — the deck's headline number is wrong, and the truth is a better lesson
> (verified live, see Appendix A #4).** The live-inject path works exactly as the slide
> depicts — start the pipeline, let it drain to idle (watermark past 12:10), inject into
> the **running** query, and it *does* detect the new events. (An earlier worry that a
> drained Kafka source won't re-detect did **not** reproduce; ignore the "stamp the past +
> whale partition" workaround — it was an unnecessary detour.) **Reliable recipe:**
> ```bash
> # terminal 1 — start it, let it print a few batches and go quiet (idle, caught up)
> uv run python src/stream_revenue.py --watermark 10 --mode update
> # terminal 2 — the metric
> uv run python src/watch_progress.py
> # terminal 3 — inject into the RUNNING query
> uv run python src/inject_late.py --at "12:05" --count 50
> ```
> **What actually happens (measured):** the running query reads the 50 events, the
> `[12:05,12:10)` window total **does not move** (its state was evicted long ago), and
> `numRowsDroppedByWatermark` rises by **1 — not 50.** That is not a bug; it is the real
> semantic of the counter (slide 24). Teach the reveal as: *"We just dropped \$49,999.50 of
> real paid orders. How much did our one alarm move? **One.**"* The silent drop is real —
> the **proof is the dollars, not the counter** (batch ground truth for that window was
> **\$116,777.82**; the stream reports **\$66,505.71** — the \$49,999.50 is gone). The
> batch audit (slide 30) is what reconciles it.

### Slide 24 — "The counter that tells on the watermark." (the metric — never cut)
`numRowsDroppedByWatermark` is **L7's operational number**, the lineage continuing L1 TPS
/ L5 slot lag / L6 consumer lag. Run `watch_progress.py` in terminal 2.

> **⚠️ THE ONE CORRECTION THAT MATTERS — what the counter actually counts (measured).**
> The slide implies `numRowsDroppedByWatermark` ≈ the number of dropped *events*
> ("…50 ← the \$49,999.50, found"). **It does not.** It counts dropped **post-aggregation
> rows** — distinct late *(group, window)* keys, *after* Spark's partial aggregation, not
> raw input events. `stream_revenue` groups by `window` only, so all 50 injected events
> (same `[12:05,12:10)` window) **collapse into one partial row**; dropping it bumps the
> counter by **1**. Inject \$49,999.50 into one window → the alarm reads **+1**.
> - **This is the lesson, sharpened, not a footnote:** a single **hot key** can hide a
>   fortune from your drop counter. An alarm like `dropped > 10` would never fire on a
>   whale that dumps everything into one window. *Defaults fail silently — and even the
>   alarm you added can under-fire.* (Ties straight to L6's hot-partition thread.)
> - It also explains the earlier sweep: ~46 "drops" for ~492 late events = ~46 distinct
>   late **windows**, not 492 events. The counter has always meant *groups*.
> - **So how do you prove the \$49,999.50 vanished?** Not the counter — the **dollars**:
>   the stream's window total never includes it, while a plain batch aggregate over the
>   same topic is ~\$50k richer. That reconciliation IS the slide-30 "batch is the audit"
>   point, arriving early. (Quick batch check: read the topic with `spark.read`, group by
>   `window(created_at,"5 minutes")`, and compare the `[12:05,12:10)` total to the stream's.)
> - **Operational takeaway to state plainly:** treat `numRowsDroppedByWatermark` as a
>   **binary alarm** — *zero vs nonzero* — not as a precise loss tally. Nonzero = you are
>   dropping; quantify the loss with the batch reconcile, not the counter.

> **Honest plumbing detail (say it — it teaches something real):** the slide says
> "reads `query.lastProgress`." `lastProgress` is a handle **inside the pipeline's own
> driver process** — a *separate* process can't read it. So `stream_revenue.py` runs a
> `ProgressPump` thread that polls `lastProgress` and appends one JSON line per batch to
> `data/progress.jsonl`; `watch_progress.py` (a different process) tails that file. Same
> numbers, one process hop. (This is the realistic shape: metrics get *exported*, not read
> across process boundaries.)
>
> **Second hard-won detail:** the pump scans `query.recentProgress` (the last ~100
> batches), not just `lastProgress`. A tiny batch — like the 50 injected events — can
> complete **between** two polls; a single-snapshot poll would miss it *and miss its
> drops*. If your drop counter ever seems to "skip," this is why.

> **Demo (terminal 2, alongside slide 23's pipeline):**
> ```bash
> uv run python src/watch_progress.py
> ```
> Land: **zero = lateness allowance generous enough; nonzero = knowingly shipping wrong
> numbers — a business decision, not a bug, the moment the counter is visible.**

### Slide 25 — Live: the dial table. "Same data. Three watermarks." (PREDICT-FIRST + DELIVERABLE)
**Beat:** re-run `--watermark 1`, `10`, `30`; predict each row before the reveal. This
table **IS the take-home deliverable** — students fill it with *their own* measured drops,
delays, and state rows.

> **Demo (three runs; let each drain, Ctrl-C, read the cumulative drop line it prints):**
> ```bash
> uv run python src/stream_revenue.py --watermark 1  --mode append
> uv run python src/stream_revenue.py --watermark 10 --mode append
> uv run python src/stream_revenue.py --watermark 30 --mode append
> ```
> **Measured anchor (this repo, one live run — 10k rows, `--max-per-trigger 300 --trigger 1`):**
>
> | `--watermark` | dropped events | peak state rows | direction |
> |---|---|---|---|
> | 1 min  | **92** | 5 | aggressive drops, tiny state |
> | 10 min | **43** | 5 | catches more, still small |
> | 30 min | **4**  | 8 | ~nothing dropped, larger state |
>
> The **direction is the lesson**: drops fall monotonically as the dial widens
> (92 → 43 → 4), state creeps up. **Read the absolute numbers honestly to the room** —
> they're *much* smaller than the deck's notional "~480 / ~30 / 0", and the gap is itself
> the deepest insight of the lesson (bench note #3): with 300-row batches (~1.8 event-min
> each) the watermark sits a batch behind, so even a 1-minute watermark still *catches*
> most short-late events — it only drops the ones beyond `batch_width + 1 min`. Crank
> `--max-per-trigger` up and drops fall toward zero (the watermark jumps the whole topic
> in one batch); crank it down and `--watermark 1` approaches the full 5%. Exact counts
> also depend on the seed's random draw. **That's the point** — the grader checks the
> student's README *story matches their own counter*, not a fixed number.

### Slide 26 — Live: no watermark + append. "What prints?" (PREDICT-FIRST)
**Beat:** delete `withWatermark`, keep `append`, run. Predict the console. The slide's
story: "runs green forever, emits nothing, state → OOM."

> **⚠️ HONEST CORRECTION — the slide simplifies; the script tells the truth, and the truth
> is the better lesson.** On Spark 4, `append` + a streaming aggregation **with no
> watermark is REFUSED at analysis time**, not run silently:
> ```
> [STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append.
> This output mode is not supported for streaming aggregations without watermark…
> ```
> That fail-fast guard is itself a **feature** — append can only emit *final* windows, and
> with no watermark no window is provably final, so Spark refuses rather than guarantee
> eternal silence. `experiment_no_watermark.py` shows **both halves**:
> - **Part A** — `append` + no watermark → Spark refuses to start. (Run live; read the
>   exception out loud.)
> - **Part B** — `update` + no watermark → *this* one runs, looks healthy, and
>   **`numRowsTotal` climbs every batch and never falls** because no window is ever
>   finalized/dropped. Measured live here: state `6 → 8 → 9 → 10 → 12 → 14 …`, watermark
>   `None`. That's the real unbounded-state / OOM path — the **L5 abandoned slot
>   reincarnated**: unbounded retention, nobody watching, dies weeks later at 3 AM.
>
> ```bash
> uv run python src/experiment_no_watermark.py --seconds 12
> ```
> Teaching framing: *"The slide's 'eternal silence' is the intuition; the real engine is
> stricter and smarter — it refuses append, and it's `update`-mode growth that actually
> kills you. Either way: **a watermark isn't an optimization. For a windowed aggregate in
> production it's a requirement.**"* (On our bounded 10k toy topic Part B plateaus near
> the ~12 total windows — say that the *mechanism* is the danger; with unbounded production
> input it climbs without limit.)

### Slide 27 — Experiment: sliding window cost. "Slide every minute. Pay ten times."
Swap tumbling 5m for `window(t, "10 minutes", "1 minute")` → each event in size/slide = 10
windows → ~10× state and output. Watch `numRowsTotal`.

> **Demo + the honest number:**
> ```bash
> uv run python src/experiment_sliding.py --seconds 15
> ```
> Measured live on this topic: **tumbling 5m peak ≈ 6 windows, sliding 10m/1m peak ≈ 34 →
> ~5.7×.** Note out loud why it's **below** the theoretical 10×: the watermark caps how
> many windows are *open at once*, and on a fast-draining 10k topic only a handful sit
> inside the watermark horizon — so the absolute peaks are small and the ratio is noisy.
> **The transferable invariant is the ratio ≈ size/slide, not the absolute count.** The
> slide's "~1,200 vs ~12,000" are *notional at production scale*. Pop quiz: 1-hour window
> sliding every 10s → **360 windows/event.** State and compute scale with size/slide — a
> capacity-planning number, like partition count in L6. **Smooth dashboards are bought
> with RAM.**

### Slide 28 — Experiment: two queries + Kafka sink. "Fork the stream. Land in Kafka."
One source, two queries, two topics, **two checkpoint dirs**. The sink wants a string
`value` column → `to_json(struct(...))`, the mirror image of the `from_json` they read
with. **ONE checkpoint dir per query, never shared — two queries on one checkpoint corrupt
each other.** Then the victory lap: the **L5 crash demo, inherited for free** — Ctrl-C it,
rerun without `--reset`, it resumes from the checkpoint (same offsets, same state), no
reprocessing.

> **Demo:**
> ```bash
> uv run python src/stream_to_kafka.py --reset       # start both queries (append → Kafka)
> #   … let it run, Ctrl-C …
> uv run python src/stream_to_kafka.py               # rerun: resumes from checkpoint, no reprocess
> uv run python src/stream_to_kafka.py --readback    # plain L6-style consumer dumps revenue-per-window
> ```
> Verify downstream with `--readback` (a plain confluent-kafka consumer — the L6 skill,
> reused). The checkpoint = `__consumer_offsets` + window state, written atomically per
> batch. **This is the protagonist of L8** — plant it.

### Predict-first beats in M4
- **Slide 23** (the $49,999.50 drop), **25** (the dial sweep), **26** (no-watermark). The
  three that make the silent-failure lesson visceral.

### Likely student questions (M4)
- *"If drops are silent, how does anyone catch them in prod?"* — Exactly the metric: alarm
  on `numRowsDroppedByWatermark > 0` (or a threshold). Plus the nightly batch reconcile.
- *"Is dropping ever OK?"* — Yes, *if you chose the watermark for the business question
  and you can see the counter.* A dashboard tolerating 0.1% drift is fine; finance isn't.
- *"Why two checkpoint dirs, really?"* — Each checkpoint pins one query's offsets + state +
  *query plan*. Share it and the two queries clobber each other's commit log. Also: change
  the aggregation logic and you must delete the checkpoint — it pins the old plan.

### Timing M4: ~40 min. The drop (23) + the metric (24) are the non-negotiable core.

### Transition line → M5
*"You've now built it, broken it, and measured the break. Let's compress the whole lesson
into three dials and one production pattern you'll use for the rest of your career."*

---

<!-- ════════════════════════════════════════════════════════════════════════ -->
# MOVEMENT 5 — Synthesis & close (slides 29–36)

**The one thing to land:** *"Read any windowing spec straight into three dials — and pair
the fast stream with the nightly batch audit. Streaming didn't replace L4; it made L4 the
reconciler."*

### Slide 29 — "Three dials, one sentence each." (the synthesis table)
window = grain of the question (never freshness); trigger = refresh cadence (never
correctness); watermark = patience with stragglers + state size (never the question).
**The interview question:** *"revenue per minute, refreshed every 10 seconds, tolerating
2 minutes of late data"* → 1-min tumbling window · 10-sec trigger · 2-min watermark. In
update mode partials every 10s; in append, finals ~2 min after each window closes. Have a
student read the spec into parameters out loud — that's the exit competency.

### Slide 30 — "Streaming for speed. Batch for truth."
The watermark drops, say, 0.1%. Acceptable for a dashboard, not for finance. But "dropped"
means dropped *from the stream's answer* — **the events still exist in Kafka and Postgres.**
The production pattern: the **stream** serves the fast, ~complete answer; the **L4 batch
pipeline** — idempotent, re-runnable, the one you already trust at 3 AM — reconciles truth
nightly. *Lesson 4 wasn't replaced by streaming; it became the audit.* Reappears in L10–11.

### Slide 31 — "The processor grows a memory." (→ L8)
Today's state was small and disposable. Next: stream-stream joins, dedup, sessionization —
state that must survive `kill -9` and come back **exactly** right. Plus the two-generals
problem and why end-to-end exactly-once is theoretically impossible yet practically
achieved. **Fair warning, by design: L8 is the hardest class of the course.** Today's
checkpoint dir is its protagonist; the L6 exactly-once promissory note comes due.

### Slide 32 — Take-home: "Ship a window you can defend."
The deliverable: configurable watermark (CLI arg, never hardcoded); late-event injector +
recorded observations; the **completeness/latency table** (1/10/30 min, *their* numbers);
README tradeoff analysis (which watermark for this data, and why). **Grading standard:**
the grader runs the pipeline, injects late events, and checks `numRowsDroppedByWatermark`
matches the README's story. Repo + README + CLAUDE.md/AGENTS.md via PR. **AI-assisted is
fine — the grade is on the semantics being right** (event-time column, watermark value,
output mode — the things that change the answer).

### Slides 33–36 — Annexes (reference; pull up on demand)
- **33 · Run it:** the end-to-end command list. Reset = delete `ckpt/`, reseed for fresh
  event times, verify downstream with the L6 consumer on `revenue-per-window`.
- **34 · opencode prompts:** let AI draft boilerplate; **you own the semantics** (event-time
  column, watermark value, output mode). Three ready prompts: pipeline, monitor, break-it.
- **35 · Gotchas (the one to actually read aloud if demos misbehave):**
  - `JAVA_HOME` — Java 17, `brew install openjdk@17`. Nearly every "Spark won't start."
  - First run slow — Maven pulls the Kafka connector once, caches.
  - Seeing nothing — `startingOffsets` defaults to `latest`; use `earliest`.
  - One checkpoint dir per query; changed the aggregation? delete it (pins old plan).
  - Watermark is global — one stalled partition won't hold it back; its events may drop
    when it catches up.
  - Dev in `update` — append's silence-until-watermark looks like a broken pipeline.
- **36 · Scripts table:** every script, what it does, which slide. All under
  `src-lesson7/src/`, same layout as L4–L6: `docker compose up -d`, then
  `uv run python src/<script>.py`.

### Timing M5: ~20 min. Slide 29 (read-the-spec) + slide 30 (stream+batch) are the keepers.

### Closing line
*"You can now turn 'revenue, last five minutes' into a running pipeline, name exactly which
events it drops and why, and see the counter that proves it. Next week the pipeline grows a
memory that survives a crash — and that's the hardest, best idea in the course."*

---

## Appendix A — Author's bench notes (bugs found, insights, the honest deltas)

Captured while building and running every demo for real. Surface these when they're
pedagogically useful; they're the difference between "the slide said" and "I ran it."

1. **`from pyspark.sql.functions import sum` shadows the builtin `sum`.** A real bug we
   hit and fixed. If a student's `sum("amount")` throws a bizarre type error, this is the
   first suspect. (Good teachable: namespace hygiene with Spark functions.)
2. **The progress pump must scan `recentProgress`, not just `lastProgress`.** Fast/tiny
   batches (the 50 injected events) complete between polls and a single snapshot misses
   their drops. (See slide 24 note.)
3. **Micro-batch width inflates effective lateness.** The watermark only updates between
   batches; a wide batch advances it in one jump and drops nothing. The `--max-per-trigger`
   flag is the dial that makes the watermark demo *work*. This is the single most useful
   non-obvious operational insight in the lesson — it explains nearly every "my watermark
   isn't dropping anything" confusion. **Concretely:** at `--max-per-trigger 300`, a
   `--watermark 1` run dropped only **92** of the ~500 late events (not all of them),
   because a 300-row batch spans ~1.8 event-minutes and the watermark trails it — so
   events less than ~`batch_width + 1 min` late are still caught. (Slides 19 & 25.)
4. **`numRowsDroppedByWatermark` counts dropped *groups*, not events — the deck's biggest
   factual slip.** Verified end to end: inject 50 late orders (\$49,999.50) all stamped
   `12:05`, live, into a running idle query. The query *does* detect them (the feared
   idle-source non-detection did **not** reproduce with `--trigger 2`); the window total
   stays put; and the counter rises by **1**, not 50, because all 50 share window
   `[12:05,12:10)` and pre-aggregate to one dropped partial row. Ground truth: batch
   `[12:05,12:10)` = \$116,777.82 vs stream \$66,505.71 — the \$49,999.50 is provably gone,
   but the **dollars** show it, not the counter. Teach the counter as a *binary alarm* and
   quantify loss with a batch reconcile. The old "stamp-the-past + whale partition"
   workaround was chasing the non-reproducing idle quirk — dropped. (Slides 23–24 ⚠️.)
5. **Append + no-watermark is *refused*, not silent, on Spark 4.** The deck's "eternal
   silence" is intuition; the engine fail-fasts. The truer OOM path is `update` + no
   watermark. `experiment_no_watermark.py` shows both. (Slide 26 ⚠️.)
6. **Sliding ratio measured ~5.7×, not 10×**, because the watermark caps simultaneously-open
   windows on a fast-draining toy topic. Teach the *ratio* (size/slide), not the absolute.
   (Slide 27.)
7. **Stack verified on Python 3.14 / PySpark 4.0.1 / JDK 17.** PySpark officially targets
   Python 3.10–3.12, but a full windowed+watermarked streaming query runs on 3.14 with
   `numRowsDroppedByWatermark` intact — we never touch the Python worker (no Python UDFs;
   `from_json`/`window`/`sum` are all JVM-side). (See `pyproject.toml`.)

## Appendix B — Reset / troubleshooting quick reference

```bash
# Full reset (fresh event times + clean state):
docker compose down                                  # or: docker exec kafka-l7 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orders-cdc
rm -rf ckpt/ data/progress.jsonl
docker compose up -d
uv run python src/seed_events.py

# Just restart state, keep the topic:
rm -rf ckpt/

# Confirm the topic is seeded (should sum to ~10000):
docker exec kafka-l7 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic orders-cdc

# Restore L6's cluster afterward (it shares port 19092 — bring this down first):
docker compose down && docker start kafka-1 kafka-2 kafka-3
```
