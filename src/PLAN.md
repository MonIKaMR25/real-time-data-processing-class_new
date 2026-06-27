# Sales Demo Plan — 1 Hour Live

> Audience: prospective students for the Real-Time Data Processing course (LATAM, Spanish-speaking).
> Goal: at the end of 1 hour, attendees want to sign up.
> Constraint: the syllabus alone doesn't sell it. We need to show, not tell.

## The demo: live audience heatmap

The audience IS the data source. They scan a QR code (or, for local rehearsal, just spam a button), open a page on their phones with a big colorful button. Every tap streams to a live dashboard projected at the front:

- A counter incrementing in real time (sub-second freshness)
- Taps-per-second sparkline (last 60s, 1-second buckets) — the line moves as the room buzzes
- A "leaderboard" of top tappers (anonymized session IDs)
- A side-by-side "what if you polled every 30s" panel — visibly stale, to make the streaming win viscerally clear

Why this works: the audience sees their own behavior on the screen. They can't dismiss it as canned. When 30 people tap simultaneously and the counter jumps from 200 to 230 in a second, that's the sale.

Optional flourish: ask everyone to vote 1–5 on something, show the histogram populating live, then have one person spam-click and watch them appear as an outlier. Sets up "fraud detection in 500ms" as a teaser for L9.

## Stack — slim version for the live demo

Cut the full L11 stack (Postgres + Debezium + Spark + Grafana). Brittle on stage and overkill for one hour. Build:

```
Phone (HTML/JS) ──HTTP POST──▶ FastAPI ──▶ Redpanda ──▶ ClickHouse ──▶ FastAPI ──▶ Dashboard (SSE)
                                          (Kafka API)   (Kafka engine    (poll
                                                         + matview)       ClickHouse
                                                                          every 500ms)
```

Specifics:
- **Redpanda, not Kafka.** Single binary, starts in 2 seconds, Kafka API compatible. No "ZooKeeper electing a leader" awkwardness.
- **ClickHouse Kafka engine + materialized view** for ingestion. This is exactly L10 material — point at it on stage and say "this is lesson 10."
- **No Spark, no Debezium, no Postgres** in the live path. Show them in the architecture slide; say "we go deeper in lessons 5–9."
- **SSE, not WebSockets.** One line on each side, survives flaky wifi.
- **Local for prep**, deploy to a small VM (Hetzner CPX21 or DigitalOcean droplet) only if you want a remote URL for the actual session.

Total live components: 4 (FastAPI, Redpanda, ClickHouse, dashboard HTML). All running and warmed up before the session starts.

## 1-hour timeline

| Time | Segment | What happens |
|---|---|---|
| 0:00–0:03 | **Hook** | QR on screen, audience taps, counter moves. No talking. Then: "What just happened?" |
| 0:03–0:08 | **Frame** | One motivating question on a slide. The promise: "By the end of this hour, you'll have seen what it takes to build this, and you'll know whether you want to learn it." |
| 0:08–0:20 | **Drive the demo** | Show dashboard reacting live. Show the leaderboard. Run the "polled vs. streamed" comparison side-by-side. Spam-click yourself to show an outlier. **Engagement, not architecture, in this segment.** |
| 0:20–0:35 | **Pull back the curtain** | Architecture diagram. Walk each component in ~90s, naming the lesson. Show the actual Kafka topic (`rpk topic consume taps`) scrolling in a terminal. Show the ClickHouse query running. Show the materialized view definition. Syllabus comes to life. |
| 0:35–0:48 | **AI vibe-coding moment** | Open Claude Code (or chosen agent). Out loud: "add p95 latency tracking — time between phone tap and dashboard render." Watch it edit two files. Save. Hit dashboard, new metric appears. Frame: "This is the multiplier we teach. Not 'AI builds your app.' It's 'you direct AI to build production systems, because you understand the system.'" |
| 0:48–0:57 | **Course pitch** | 12 lessons, what they build, what's in their portfolio at the end. Tie 3–4 specific demo moments to specific lessons ("the Kafka topic scrolling? L6. Materialized view? L10. p95 latency? Entire premise of L9."). Pricing/logistics. |
| 0:57–1:00 | **CTA + Q&A** | Sign-up link, deadline, one slide. 1–2 questions if appetite. |

## Motivating questions (pick ONE for the opening)

Don't list all. Drive one home:

- "How does Uber update your driver's location to thousands of riders, every second, without falling over?"
- "How does a bank decide in 200 milliseconds whether to approve your card?"
- "Why is your company's dashboard always 'last updated 4 hours ago'?"
- "If you wanted to detect fraud the moment it happens, not the next morning — what would you build?"

For LATAM career-outcomes framing: **"The companies hiring data engineers in 2026 don't want batch ETL. They want this. And there's almost no one who actually knows how to build it end-to-end."**

## Build order (4 hours of prep)

Priority order. If time runs out, later items get cut.

1. **(60 min) Local pipeline working end-to-end.** Tap → Redpanda → ClickHouse → FastAPI → dashboard. One tap flows through. Static, hardcoded. Foundation; nothing else matters if this doesn't work.
2. **(45 min) Polish the dashboard.** Big numbers, smooth animations, side-by-side "polled vs. streamed" panel. This is what people remember. Don't skimp.
3. **(30 min) Pre-write and rehearse the AI moment.** Decide the exact prompt. Run it once in advance to confirm it works in ~90 seconds. Have the diff ready to apply manually if Claude misbehaves.
4. **(30 min) Architecture slide.** One diagram, lesson numbers overlaid. Transition from demo → syllabus.
5. **(30 min) Rehearse the full hour twice.** Out loud. With a timer. The first run reveals everything broken or awkward.
6. **(15 min) Failure fallbacks.** Pre-recorded screencap of the demo working, in case wifi or VM dies on stage. Switch to it without apologizing.

## Things to push back on

**Don't actually live-code from scratch.** "Vibe coding live" sells well as a pitch, demos badly as a reality. Goes wrong, you flail, audience loses faith. Do the AI moment as ONE small, rehearsed edit (the p95 metric) on top of a fully-built system. Sells the AI workflow without the catastrophic failure mode.

**Don't show all 7 components live.** The full L11 stack is too brittle and audience can't follow it in 1 hour. Run 4 components live, show all 7 on a slide, reference the lessons. The slim version is honest — it's what most production "fast path" pipelines actually look like.

## What's already in this directory

- `docker-compose.yml` — Redpanda + ClickHouse, single command
- `clickhouse/init.sql` — `taps_kafka` (Kafka engine), `taps` (MergeTree), materialized view
- `app/main.py` — FastAPI: serves tap page, dashboard page, `/api/tap` (POST → Kafka), `/api/stream` (SSE → ClickHouse)
- `app/static/tap.html` — big colorful button, generates session ID, POSTs taps
- `app/static/dashboard.html` — total counter, 60s sparkline, leaderboard, all SSE
- `pyproject.toml` — fastapi, uvicorn, aiokafka, clickhouse-connect (managed with uv)
- `README.md` — run instructions

## What still needs doing (for the next agent or for you)

- [ ] **Polish dashboard.** Add the "polled every 30s" comparison panel (a second card that updates every 30 seconds with the current count, so audience sees how stale that feels).
- [ ] **Pick the AI vibe-coding target.** Best candidate: "add a histogram of inter-tap intervals per session" or "add p95 round-trip latency." Pre-rehearse the exact Claude prompt.
- [ ] **Architecture slide.** One image showing all 7 components with lesson numbers; the live demo only uses 4 of them, the others are labeled "L5", "L7-8", "L11".
- [ ] **Rehearse twice end-to-end.** Time each segment.
- [ ] **Record a fallback screencap** of the working demo. Have it ready to play if wifi/VM/anything dies.
- [ ] **Decide on QR vs. local-only.** For local rehearsal, just spam the button yourself. For the actual session, generate a QR pointing at the deployed URL.

## Deployment notes (only if going remote)

If you want a public URL for the actual session (so phones can hit it):

- **Hetzner Cloud CPX21** (€7/mo) or **DigitalOcean basic droplet** ($6/mo). 4GB RAM is plenty.
- Provision Ubuntu, install Docker + Docker Compose, `git clone` this repo, `docker compose up -d`, run `uvicorn` behind Caddy/nginx with HTTPS (Let's Encrypt).
- **Vercel does NOT work for this** — Redpanda and ClickHouse are stateful long-running processes. Vercel is serverless-only.
- For the demo, local is fine. Tether your laptop to your phone hotspot if conference wifi is sketchy.
