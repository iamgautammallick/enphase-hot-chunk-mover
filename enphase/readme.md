# auto_oplog_hot_chunk_mover.py

**Standalone, zero-config hot-chunk mover for Enphase rollups-3 sharded MongoDB.**

Copy **one file** (`auto_oplog_hot_chunk_mover.py`) to your ops host. No companion scripts required.

---

## What it does

When one shard receives far more write traffic than its peers, this tool:

1. Reads each shard's oplog and measures **real write bytes per shard**
2. Auto-detects **hot** and **cold** shards (nothing hardcoded)
3. Ranks **whole chunks** by aggregated heat
4. Moves the hottest chunks off hot shards with `moveChunk`
5. Re-measures between rounds until balanced or a safety guard stops the run
6. Writes a full audit trail: before/after snapshots, plan, results, Markdown report,
   and one document in `hotmover.runs`

**Dry-run by default.** Add `--execute` only after reviewing the plan.

Details: [How it works (technical reference)](#how-it-works-technical-reference)

---

## Requirements

| Item | Detail |
|------|--------|
| Python | 3.6.8+ |
| Package | `pip install "pymongo[srv]"` |
| URI | Must point at a **mongos** (cluster router) |
| Privileges | `clusterAdmin` (moveChunk/split), read on each shard's `local.oplog.rs` |
| Balancer | Native MongoDB balancer must be **OFF** (script refuses if ON) |

---

## Fixed profile (production)

These values are baked in. You do **not** pass them on the command line.

| Setting | Value | Meaning |
|---------|-------|---------|
| `hot_factor` | **1.0** | Shard is HOT when rate ≥ 1.0× cluster mean |
| `target_imbalance` | **1.0** | Stops when hottest/mean &lt; 1.0× |
| `window_minutes` | 15 | Oplog sampling window |
| `max_scan` | 800000 | Max oplog entries per shard sample |
| `limit` | 100 | Max chunk moves **per round** |
| `max_moves_total` | 100 | Stop after 100 successful moves per run |
| `max_runtime_min` | 240 | Stop starting new rounds after 4 hours |
| `sleep_ms` | 3000 | Pause between consecutive moves |
| `round_cooldown_sec` | 90 | Pause after each execute round |
| `fair-sources` | on | Split move budget across source shards |
| `auto` | on | Loop measure → move → re-measure |
| `out_dir` | `runs` | Artifact directory |
| `history_ns` | `hotmover.runs` | Run history collection |

---

## Install

```bash
mkdir -p ~/hot-chunk-mover/enphase && cd ~/hot-chunk-mover/enphase
pip install --user "pymongo[srv]"
mkdir -p runs
```

---

## Set connection string (required before every run)

You **must** export `MONGODB_URI` in the same shell session before running the script
(unless you pass `--uri` explicitly on every command):

```bash
export MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"
```

Verify it is set:

```bash
echo "$MONGODB_URI"
```

Production example:

```bash
export MONGODB_URI="mongodb+srv://USER:PASSWORD@rollups-3-xnttl.mongodb.net/"
```

QA example:

```bash
export MONGODB_URI="mongodb+srv://USER:PASSWORD@your-qa-cluster.mongodb.net/"
```

---

## Commands

Run `export MONGODB_URI=...` first (see above).

### Dry-run (always first)

```bash
export MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"

python3 auto_oplog_hot_chunk_mover.py \
  --uri "$MONGODB_URI" \
  --ns enlighten_production.rollup.site_daily_time_series
```

QA:

```bash
export MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"

python3 auto_oplog_hot_chunk_mover.py \
  --uri "$MONGODB_URI" \
  --ns enlighten_qa2.rollup.site_daily_time_series
```

### Execute (supervised)

```bash
export MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"

python3 auto_oplog_hot_chunk_mover.py \
  --uri "$MONGODB_URI" \
  --ns enlighten_production.rollup.site_daily_time_series \
  --execute
```

### Execute overnight (nohup)

```bash
export MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"

nohup python3 auto_oplog_hot_chunk_mover.py \
  --uri "$MONGODB_URI" \
  --ns enlighten_production.rollup.site_daily_time_series \
  --execute \
  --out-dir runs \
  > runs/nohup_$(date -u +%Y%m%dT%H%M%SZ).log 2>&1 &
```

### Continue after a guard stop

If the run stops with a move-count or runtime guard, re-run the **same command**.

---

## When to run

Monitor Atlas metrics through the month (per-shard CPU, oplog write rate, WiredTiger
cache dirty %).

**Mid-month:** run a **dry-run** while normal rollups traffic is active. In Step 3,
check whether write load is concentrated on one or two shards (imbalance score above
1.0×, or a shard marked `<< HOT`).

- **Balanced / no hot shard** — nothing to do until the next check.
- **Imbalance detected** — review the plan in `runs/hotmover_*_report.md`, then run
  again with `--execute` to move hot chunks and even out the shards.

Repeat execute runs (same command, up to 100 moves per night) until the dry-run shows
the cluster is balanced, or until Atlas metrics improve.

**Suggested customer email (copy/paste):**

> Please monitor rollups-3 metrics through the month (per-shard CPU, oplog rate, cache
> dirty %). Around mid-month, run a **dry-run** of `auto_oplog_hot_chunk_mover.py` during
> normal write traffic. If the dry-run shows shard imbalance (Step 3 — one shard doing
> more than its fair share of writes), run the tool again with **`--execute`** to move
> hot chunks and even out the shards. If the dry-run reports balanced, no execute is
> needed until the next check.

---

## Before you run

1. Stop the native balancer: `sh.stopBalancer()` then `sh.getBalancerState()` must show stopped.
2. Dry-run during active write traffic.
3. Confirm hot shards in Step 3 match Atlas / CPU expectations.
4. Review the drain estimate in Step 4.
5. Execute in batches (100 moves/night by design).

---

## How it works (technical reference)

Read this section if you need the exact math, ranking rules, move counts, and split
behavior. No tuning knobs in the customer script — values below are fixed in the
production profile.

### Scope

| Item | Value |
|------|-------|
| Collection | `enlighten_production.rollup.site_daily_time_series` (override with `--ns`) |
| Document `_id` | `{siteId}:{YYYYMM}` (e.g. `12345:202608`) |
| Shard key | `_id` (hashed) |
| Chunk bounds | Loaded from `config.chunks` on the mongos |
| Write signal | `local.oplog.rs` on each **shard primary** (`op: "u"`, matching `--ns`) |

### End-to-end flow

![Flow diagram](docs/flow_diagram.png)

```mermaid
flowchart TD
  A[Start] --> B{Balancer OFF?}
  B -->|no| X[Exit]
  B -->|yes| C[Sample oplog on each shard primary]
  C --> D[Compute bytes/sec per shard]
  D --> E[Mark HOT / cold shards]
  E --> F[Aggregate heat per chunk]
  F --> G[Rank chunks by oplog bytes]
  G --> H{--execute?}
  H -->|no| I[Dry-run: print plan + artifacts]
  H -->|yes| J[moveChunk hottest chunks]
  J --> K{Stop?}
  K -->|no| L[90s cooldown + re-sample]
  L --> E
  K -->|yes| M[Write after.json + report]
```

### Step 1 — Preconditions

- Connect via mongos; refuse if native balancer is ON.
- Resolve shard primaries from `config.shards`.
- Current calendar month suffix `:YYYYMM` is used to filter documents for **ranking**
  only (shard totals count all namespace updates in the window).

### Step 2 — Measure write heat (per shard)

On each shard primary, aggregation on `local.oplog.rs`:

```
match: { ns: <--ns>, op: "u", ts: { $gte: now - 15 minutes } }
limit: 800000
project: bytes = $bsonSize(o)
```

Definitions:

```
total_bytes     = sum(bytes) over all matched oplog entries on this shard
first_ts        = ts of earliest entry in the sample (after limit)
last_ts         = ts of latest entry in the sample
sampled_seconds = (last_ts - first_ts) in seconds  (minimum 1)

bytes_per_sec   = total_bytes / sampled_seconds
```

Notes:

- If the sample hits the 800000 cap, `sampled_seconds` is **shorter than 15 minutes**.
  Rates reflect the capped window, not the full `--window-minutes` setting.
- Shard-level `bytes_per_sec` uses **all** update ops for `--ns` in the sample.
- Per-document heat for ranking uses only `_id` values ending in `:{current_month}`.

### Step 3 — Hot and cold shards

```
mean = (sum of bytes_per_sec over all shards) / (number of shards)
```

| Label | Rule |
|-------|------|
| **HOT (source)** | `bytes_per_sec >= hot_factor × mean` → `hot_factor = 1.0` |
| **Cold (destination)** | Shards **not** marked HOT, sorted by `bytes_per_sec` ascending |

Source selection:

- All HOT shards are sources, capped at **3** (highest rate first if more than 3).

Destination selection:

- Prefer shards with `bytes_per_sec <= mean`.
- If fewer than 3 qualify, fill from remaining non-hot shards (coldest first).
- Minimum **3** destination shards when the cluster has enough shards.

Imbalance score (printed in Step 3):

```
imbalance = max(bytes_per_sec among source shards) / mean
```

Auto mode stops when `imbalance < target_imbalance` (`target_imbalance = 1.0`). In
practice, measurement noise and chunk granularity often mean the run stops on move
guards or “no movable chunks” before this threshold is hit cleanly.

### Step 4 — Chunk heat and ranking

For each oplog document on shard **S**:

1. Parse `_id` from the oplog entry.
2. Map to a chunk with `locate_on_shard(_id, S)` — chunk bounds from `config.chunks`,
   attributed to the shard where the oplog was **sampled** (not config owner alone).
3. Add that entry's `bytes` to the chunk's running total.

Per chunk record:

| Field | Meaning |
|-------|---------|
| `oplog_bytes` | Sum of oplog entry sizes for docs in this chunk (current month filter) |
| `top_doc` | `_id` with the largest single oplog entry in that chunk |
| `sampled_on` | Shard whose oplog contributed the heat |
| `shard` (config) | Owner shard from `config.chunks` |

Ranking:

```
hot_chunks = all chunks with oplog_bytes > 0 on HOT source shards
sort hot_chunks by oplog_bytes descending
store top 3000 in before.json (full list used for planning)
```

**Drain estimate** (printed in Step 4, diagnostic only):

```
need = bytes_per_sec(hottest source) - mean
Walk hot_chunks from hottest downward:
  cum += chunk.oplog_bytes / sampled_seconds
  count chunks until cum >= need
→ prints "~N chunks" to approximate how many moves would drain excess rate
```

This does **not** cap how many chunks are moved; the execute limits below do.

### Step 5 — How many chunks move

**Per round (plan):**

```
limit = 100   (max moves planned this round)
```

With `fair-sources` enabled (production default):

```
per_source = floor(100 / number_of_hot_source_shards)
```

For each hot source shard, take that shard's **hottest** chunks (by `oplog_bytes`),
up to `per_source` each. Merge lists, sort globally by `oplog_bytes` descending,
truncate to **100**.

Examples:

| Hot sources | Per source | Max this round |
|-------------|------------|----------------|
| 1 | 100 | 100 |
| 2 | 50 | 100 |
| 3 | 33 | 99 (33×3) |

**Destination assignment:** round-robin across the cold destination list (coldest shards
first in the list).

**Execute pacing:**

| Guard | Value |
|-------|-------|
| Pause between moves | 3000 ms |
| Cooldown after each execute round | 90 s |
| Max successful moves per **run** | 100 (`max_moves_total`) |
| Max wall time for new rounds | 240 min |

After each execute round: reload `config.chunks`, re-sample oplog, re-detect hot/cold,
build a new plan. Failed moves (e.g. orphan cleanup) are skipped for the rest of that
run; retry on the next invocation.

**Dry-run:** one planning pass — Steps 2–4 and artifact write. **No** `moveChunk`, **no**
round loop, **no** `after.json`.

### Step 6 — moveChunk and verification

Each planned move:

```javascript
sh.moveChunk("<ns>", { find: { _id: "<top_doc>" } }, "<dest_shard>")
```

- `top_doc` is the busiest document in the chunk (not a synthetic bound key).
- Post-move check: `db.collection.find({ _id: top_doc }).explain("executionStats")`
  — `shards[0].shardName` must equal the destination.

### Chunk splitting (ChunkTooBig)

rollups-3 has autosplit disabled; some chunks are ~900 MB. If `moveChunk` returns
`ChunkTooBig`:

1. Parse max documents per chunk and current document count from the error text.
2. Compute split depth:

```
need = ceil( log2( found_docs / (max_docs × 0.7) ) )
need = min(need, 6)
```

3. Run up to `need` median splits:

```javascript
sh.splitAt("<ns>", { find: { _id: "<top_doc>" } })
```

4. Retry `moveChunk` once splits complete.

The script does **not** call `forceJumbo`.

### Stop conditions (execute + auto)

Run ends when **any** of:

| Condition | Meaning |
|-----------|---------|
| `imbalance < 1.0` | Hottest source rate below target vs mean |
| No HOT shards | All shards at or below mean |
| No movable chunks | No ranked chunks on hot sources (often after a partial round) |
| `max_moves_total` | 100 successful moves in this invocation |
| `max_runtime_min` | 240 minutes elapsed |
| User interrupt | Ctrl+C / kill |

After a guard stop, re-run the same command; history in `hotmover.runs` is append-only.

### Artifact fields (quick map)

| JSON path | Content |
|-----------|---------|
| `before.json` → `shard_heat[]` | Per-shard `bytes_per_sec`, HOT flag |
| `before.json` → `hot_chunks[]` | Ranked chunks with `oplog_bytes`, `top_doc`, bounds |
| `plan.json` → `moves[]` | Ordered move list with source, dest, chunk index |
| `results.json` → `moves[]` | `ok` / error per move, timing |
| `after.json` | Same shape as `before.json` after execute rounds |

---


Written under `runs/` (default):

| File | Contents |
|------|----------|
| `hotmover_<stamp>_run.log` | Full console log |
| `hotmover_<stamp>_before.json` | Pre-move snapshot |
| `hotmover_<stamp>_plan.json` | Planned moves |
| `hotmover_<stamp>_results.json` | Per-move outcomes |
| `hotmover_<stamp>_after.json` | Post-move snapshot (execute) |
| `hotmover_<stamp>_report.md` | Human-readable summary |

Run history on cluster:

```javascript
db.getSiblingDB("hotmover").runs.find().sort({started_at: -1}).limit(10)
```

---

## Troubleshooting

| Symptom | Action |
|---------|--------|
| Balancer is ON | `sh.stopBalancer()`, re-run |
| ZERO updates in window | Re-run during active writes; verify `--ns` |
| Orphan cleanup error | Wait 2–5 min, re-run |
| `guard_stopped` | Re-run same command next window |

---

## Monthly schedule

| When | Action |
|------|--------|
| Days 1–14 | Monitor metrics; optional dry-run early in the month |
| **Mid-month (~day 14–16)** | **Dry-run** — check for shard imbalance |
| Imbalance in dry-run | **Execute** to even out shards (supervised, off-peak) |
| After day 21 | Execute only if metrics are still hot; migrations add load |
| Balanced | No execute needed |

Optional proactive cron (days 2–14) — only if you want scheduled execute runs without
waiting for the mid-month dry-run:

```bash
0 3 2-14 * * cd ~/hot-chunk-mover/enphase && \
  export MONGODB_URI='mongodb+srv://...' \
  python3 auto_oplog_hot_chunk_mover.py \
    --uri "$MONGODB_URI" \
    --ns enlighten_production.rollup.site_daily_time_series \
    --execute --out-dir runs \
  >> runs/cron.log 2>&1
```

---

## CLI reference

| Flag | Required | Default | Meaning |
|------|----------|---------|---------|
| `--uri` | yes* | `$MONGODB_URI` | Mongos connection string |
| `--ns` | no | `enlighten_production.rollup.site_daily_time_series` | Sharded collection |
| `--execute` | no | off | Move chunks (default: dry-run) |

\*You must `export MONGODB_URI=...` before running, or pass `--uri` on every command.

Optional: `--out-dir`, `--no-history`, `--force-balancer-off`.
