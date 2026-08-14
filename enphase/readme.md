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

## Before you run

1. Stop the native balancer: `sh.stopBalancer()` then `sh.getBalancerState()` must show stopped.
2. Dry-run during active write traffic.
3. Confirm hot shards in Step 3 match Atlas / CPU expectations.
4. Review the drain estimate in Step 4.
5. Execute in batches (100 moves/night by design).

---

## Artifacts

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

## Monthly cron (days 2–14)

```bash
0 3 2-14 * * cd ~/hot-chunk-mover/enphase && \
  MONGODB_URI='mongodb+srv://...' \
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
