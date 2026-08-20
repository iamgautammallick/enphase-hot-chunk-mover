#!/usr/bin/env python3
"""
auto_oplog_hot_chunk_mover.py — oplog-driven chunk rebalancer (Enphase rollups-3).

Samples per-shard write load from the oplog, ranks hot chunks, and migrates them
with moveChunk. Dry-run by default.

Usage:
  python3 auto_oplog_hot_chunk_mover.py --uri "$MONGODB_URI" --ns <namespace>
  python3 auto_oplog_hot_chunk_mover.py --uri "$MONGODB_URI" --ns <namespace> --execute

See readme.md for deployment, fixed production profile, and technical reference.
"""


import argparse
import json
import math
import os
import re
import socket
import sys
import time
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse

from bson import Timestamp
from bson.max_key import MaxKey
from bson.min_key import MinKey
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

DEFAULT_NS = "enlighten_production.rollup.site_daily_time_series"


# ---------------------------------------------------------------------------
# logging — simple, human-readable, mirrored to a run log file
# ---------------------------------------------------------------------------
_LOG_FH = None


def open_run_log(path: str) -> None:
    global _LOG_FH
    _LOG_FH = open(path, "a", encoding="utf-8")


def say(msg: str = "", indent: int = 0) -> None:
    line = ("   " * indent) + msg
    print(line, flush=True)
    if _LOG_FH:
        _LOG_FH.write(line + "\n")
        _LOG_FH.flush()


def step(n: int, total: int, title: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    say()
    say(f"[{ts} UTC] STEP {n}/{total} — {title}")
    say("-" * 60)


def verdict(ok: bool, msg: str) -> None:
    say(f"{'OK  ' if ok else 'WARN'} | {msg}", indent=1)


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def stamp() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def current_yyyymm() -> str:
    return utc_now().strftime("%Y%m")


def previous_yyyymm(month: str) -> str:
    y, m = int(month[:4]), int(month[4:])
    return f"{y - 1}12" if m == 1 else f"{y}{m - 1:02d}"


def parse_csv(text: str) -> List[str]:
    return [p.strip() for p in text.split(",") if p.strip()]


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:,.1f}{unit}"
        n /= 1024
    return f"{n:,.1f}TB"


# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------
def parse_credentials(mongos_uri: str) -> Tuple[str, str]:
    parsed = urlparse(mongos_uri.replace("mongodb+srv://", "mongodb://"))
    userinfo = ""
    if parsed.username:
        userinfo = f"{quote_plus(parsed.username)}:{quote_plus(parsed.password or '')}@"
    q = "authSource=admin&ssl=true&directConnection=true"
    if parsed.query:
        q = parsed.query + "&directConnection=true"
    return userinfo, q


def direct_uri(mongos_uri: str, hostport: str) -> str:
    userinfo, q = parse_credentials(mongos_uri)
    return f"mongodb://{userinfo}{hostport}/?{q}"


def list_shards(mongos: MongoClient) -> Dict[str, str]:
    return {
        d["_id"]: d["host"]
        for d in mongos["config"]["shards"].find({}, {"_id": 1, "host": 1})
    }


def member_hostports(shard_host_field: str) -> List[str]:
    if "/" in shard_host_field:
        _, members = shard_host_field.split("/", 1)
    else:
        members = shard_host_field
    return [m.strip() for m in members.split(",") if m.strip()]


def connect_primary(
    mongos_uri: str, shard_host_field: str, timeout_ms: int
) -> Tuple[Optional[MongoClient], str]:
    last_err = ""
    for hostport in member_hostports(shard_host_field):
        try:
            client = MongoClient(
                direct_uri(mongos_uri, hostport),
                serverSelectionTimeoutMS=timeout_ms,
            )
            hello = client.admin.command("hello")
            if hello.get("isWritablePrimary"):
                return client, hello.get("me", hostport)
            client.close()
        except Exception as e:  # noqa: BLE001 — try next member
            last_err = str(e)
    return None, last_err or "no primary found"


# ---------------------------------------------------------------------------
# chunk map — locate which chunk owns a given _id (shard key is {_id: 1})
# ---------------------------------------------------------------------------
class ChunkMap:
    """All chunk boundaries for the namespace, loaded from config.chunks.

    Lets the sampler attribute every oplog entry to the chunk that owns the
    document, so heat can be ranked per CHUNK instead of per document. That is
    what makes bulk drains effective when load is spread across many documents
    rather than concentrated in a few megasites.
    """

    def __init__(self, mongos: MongoClient, ns: str):
        coll_doc = mongos["config"]["collections"].find_one({"_id": ns}) or {}
        uuid = coll_doc.get("uuid")
        match = {"uuid": uuid} if uuid is not None else {"ns": ns}
        self.chunks: List[Dict[str, Any]] = []
        for c in mongos["config"]["chunks"].find(
            match, {"min": 1, "max": 1, "shard": 1}
        ).sort("min", 1):
            self.chunks.append({
                "min": next(iter(c["min"].values())),
                "max": next(iter(c["max"].values())),
                "shard": c["shard"],
            })
        # bisect key: MinKey sorts before every string, "" does the same job
        self._mins = [
            "" if isinstance(c["min"], MinKey) else str(c["min"])
            for c in self.chunks
        ]

    def __len__(self) -> int:
        return len(self.chunks)

    def locate(self, doc_id: str) -> int:
        """Index of the chunk owning doc_id, or -1."""
        idx = bisect_right(self._mins, doc_id) - 1
        return idx if 0 <= idx < len(self.chunks) else -1

    def _contains(self, idx: int, doc_id: str) -> bool:
        c = self.chunks[idx]
        lo = "" if isinstance(c["min"], MinKey) else str(c["min"])
        if doc_id < lo:
            return False
        if isinstance(c["max"], MaxKey):
            return True
        return doc_id < str(c["max"])

    def locate_on_shard(self, doc_id: str, shard_id: str) -> int:
        """Chunk on shard_id containing doc_id, or -1.

        When attributing oplog heat, use the chunk that actually lives on the
        shard whose oplog was sampled — not the global bisect result, which can
        point at a parent range on another shard after partial moves/splits.
        """
        idx = bisect_right(self._mins, doc_id) - 1
        while idx >= 0:
            if self.chunks[idx]["shard"] != shard_id:
                idx -= 1
                continue
            if self._contains(idx, doc_id):
                return idx
            idx -= 1
        return -1

    def bounds_label(self, idx: int) -> str:
        c = self.chunks[idx]
        lo = "MinKey" if isinstance(c["min"], MinKey) else str(c["min"])
        hi = "MaxKey" if isinstance(c["max"], MaxKey) else str(c["max"])
        return f"[{lo} .. {hi})"


# ---------------------------------------------------------------------------
# oplog sampling (heat per shard + hottest _ids)
# ---------------------------------------------------------------------------
def sample_shard_oplog(
    primary: MongoClient,
    ns: str,
    since: Timestamp,
    max_scan: int,
    month: Optional[str],
    chunk_map: Optional[ChunkMap] = None,
    sampling_shard: Optional[str] = None,
) -> Dict[str, Any]:
    """One oplog pass: totals for the shard plus per-_id heat ranking.

    Entry sizes are computed server-side with $bsonSize and only
    {doc_id, bytes} is shipped to the client (~100 bytes per entry instead
    of the full multi-KB entry). The ts >= since predicate bounds the scan
    to the sampling window; the oplog has no ns index for anyone, so that
    window is the real scan limit. maxTimeMS caps the whole pass.
    """
    local = primary["local"]["oplog.rs"]
    pipeline = [
        {"$match": {"ns": ns, "op": "u", "ts": {"$gte": since}}},
        {"$limit": max_scan},
        {"$project": {
            "_id": 0,
            "doc_id": "$o2._id",
            "bytes": {"$bsonSize": "$$ROOT"},
            "t": "$ts",
        }},
    ]
    bytes_by_id: Dict[str, int] = defaultdict(int)
    count_by_id: Dict[str, int] = defaultdict(int)
    total_bytes = 0
    total_ops = 0
    first_ts = None
    last_ts = None

    for row in local.aggregate(pipeline, maxTimeMS=300000):
        t = row.get("t")
        if t is not None:
            if first_ts is None:
                first_ts = t.time
            last_ts = t.time
        doc_id = row.get("doc_id")
        if doc_id is None:
            continue
        sid = str(doc_id)
        nbytes = row["bytes"]
        total_bytes += nbytes
        total_ops += 1
        if month and not sid.endswith(f":{month}"):
            continue
        bytes_by_id[sid] += nbytes
        count_by_id[sid] += 1

    # Rate must be computed over the span the sample actually covers: on a
    # busy cluster the max_scan cap is hit long before the window ends, and
    # dividing by the full window understates every shard (and distorts the
    # shard-to-shard comparison, since hotter shards fill the cap faster).
    sampled_seconds = max(1, (last_ts - first_ts)) if first_ts is not None else 0
    ranked = sorted(bytes_by_id.items(), key=lambda x: -x[1])

    # Roll the per-document heat up to the owning chunk. Even when no single
    # document is hot, some chunks carry many active documents and are worth
    # moving as a unit (plan-mode=chunk).
    hot_chunks: List[Dict[str, Any]] = []
    if chunk_map is not None and len(chunk_map):
        cb: Dict[int, int] = defaultdict(int)
        co: Dict[int, int] = defaultdict(int)
        cd: Dict[int, int] = defaultdict(int)
        ctop: Dict[int, Any] = {}
        locate = (
            (lambda sid: chunk_map.locate_on_shard(sid, sampling_shard))
            if sampling_shard
            else chunk_map.locate
        )
        for sid, b in bytes_by_id.items():
            idx = locate(sid)
            if idx < 0:
                continue
            cb[idx] += b
            co[idx] += count_by_id[sid]
            cd[idx] += 1
            if idx not in ctop or b > ctop[idx][1]:
                ctop[idx] = (sid, b)
        for idx, b in sorted(cb.items(), key=lambda x: -x[1])[:3000]:
            owner = chunk_map.chunks[idx]["shard"]
            hot_chunks.append({
                "chunk_idx": idx,
                "bounds": chunk_map.bounds_label(idx),
                "shard": owner,
                "sampled_on": sampling_shard or owner,
                "oplog_bytes": b,
                "oplog_updates": co[idx],
                "active_docs": cd[idx],
                "top_doc": ctop[idx][0],
            })

    return {
        "hot_chunks": hot_chunks,
        "total_bytes": total_bytes,
        "total_ops": total_ops,
        "sampled_seconds": sampled_seconds,
        "capped": total_ops >= max_scan,
        # cap the stored ranking: on production nearly every update hits a
        # distinct _id, and dumping every id into the artifacts is useless
        "hot_ids": [
            {"doc_id": d, "oplog_bytes": b, "oplog_updates": count_by_id[d]}
            for d, b in ranked[:2000]
        ],
        "distinct_ids": len(ranked),
    }


def sample_heat(
    mongos_uri: str,
    shard_map: Dict[str, str],
    shard_ids: List[str],
    ns: str,
    window_minutes: int,
    max_scan: int,
    month: Optional[str],
    timeout_ms: int,
    chunk_map: Optional[ChunkMap] = None,
) -> Dict[str, Dict[str, Any]]:
    since = Timestamp(int(time.time()) - window_minutes * 60, 0)
    heat: Dict[str, Dict[str, Any]] = {}
    for shard_id in shard_ids:
        primary, info = connect_primary(mongos_uri, shard_map[shard_id], timeout_ms)
        if primary is None:
            verdict(False, f"{shard_id}: cannot reach primary ({info})")
            heat[shard_id] = {"error": info, "total_bytes": 0, "total_ops": 0, "hot_ids": []}
            continue
        try:
            res = sample_shard_oplog(
                primary, ns, since, max_scan, month, chunk_map, shard_id,
            )
            res["primary"] = info
            span = res["sampled_seconds"] or (window_minutes * 60)
            res["bytes_per_sec"] = res["total_bytes"] / span
            heat[shard_id] = res
            capped_note = ""
            if res["capped"]:
                capped_note = (f" [sample capped at {max_scan:,} entries = "
                               f"{span}s of oplog]")
            say(f"{shard_id}: {res['total_ops']:,} updates wrote "
                f"{human_bytes(res['total_bytes'])} in {span}s "
                f"({human_bytes(res['bytes_per_sec'])}/s) across "
                f"{res.get('distinct_ids', 0):,} documents{capped_note}", indent=1)
        finally:
            primary.close()
    if any(h.get("capped") for h in heat.values() if "error" not in h):
        say("Note: rates above are computed over the seconds each sample "
            "actually covers, so shard-to-shard comparison stays fair even "
            "when the --max-scan cap is hit before the full window.", indent=1)
    return heat


def heat_concentration(
    heat: Dict[str, Dict[str, Any]], sources: List[str], top_n: int
) -> Dict[str, Dict[str, Any]]:
    """How much of each source shard's sampled oplog bytes the top-N _ids carry.

    This is the go / no-go signal for targeted moves: a high share (say >20%)
    means moving a few hundred _ids meaningfully cools the shard; a low share
    means heat is diffuse and a bulk drain (ShardRemovalBalancer) is the right
    tool instead.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for shard_id in sources:
        h = heat.get(shard_id, {})
        total = h.get("total_bytes", 0)
        ids = h.get("hot_ids", [])
        top = ids[:top_n]
        top_bytes = sum(r["oplog_bytes"] for r in top)
        out[shard_id] = {
            "sampled_total_bytes": total,
            "distinct_ids": h.get("distinct_ids", len(ids)),
            "top_n": len(top),
            "top_n_bytes": top_bytes,
            "top_n_share_pct": round(100 * top_bytes / total, 1) if total else 0.0,
        }
    return out


def imbalance_ratio(heat: Dict[str, Dict[str, Any]], sources: List[str]) -> float:
    """max(source bytes/sec) / mean(all sampled shards). 1.0 == perfectly even."""
    rates = {s: h.get("bytes_per_sec", 0.0) for s, h in heat.items() if "error" not in h}
    if not rates or not sources:
        return 0.0
    mean = sum(rates.values()) / len(rates)
    if mean <= 0:
        return 0.0
    return max(rates.get(s, 0.0) for s in sources) / mean


def cluster_imbalance_ratio(
    heat: Dict[str, Dict[str, Any]], hot_factor: float,
) -> float:
    """max(bytes/sec) / mean across all shards; 0 if no shard >= hot_factor x mean."""
    rates = {s: h.get("bytes_per_sec", 0.0) for s, h in heat.items() if "error" not in h}
    if not rates:
        return 0.0
    mean = sum(rates.values()) / len(rates)
    if mean <= 0:
        return 0.0
    hot_rates = [r for r in rates.values() if r >= hot_factor * mean]
    if not hot_rates:
        return 0.0
    return max(rates.values()) / mean


def detect_hot_cold(
    heat: Dict[str, Dict[str, Any]],
    hot_factor: float,
    max_sources: int,
    min_dests: int,
) -> Tuple[List[str], List[str]]:
    """Pick hot sources and cold destinations from measured heat.

    A shard is HOT when its oplog bytes/sec exceeds hot_factor x cluster mean.
    Destinations are the coldest non-hot shards (at least min_dests of them,
    all at-or-below the mean when possible). Whatever is hot next month gets
    picked automatically — nothing is hardcoded.
    """
    rates = {
        s: h.get("bytes_per_sec", 0.0) for s, h in heat.items() if "error" not in h
    }
    if not rates:
        return [], []
    mean = sum(rates.values()) / len(rates)
    ordered = sorted(rates.items(), key=lambda x: -x[1])

    say()
    say(f"Fair share would be {human_bytes(mean)}/s of writes per shard. Actual:")
    # >= not >: a shard sitting exactly on the threshold is a hit, not a miss
    # (a real prod run measured shard-1 at exactly 1.30x and slipped through)
    for s, r in ordered:
        mult = r / mean if mean else 0.0
        if mean and r >= hot_factor * mean:
            note = f"<< HOT — doing {mult:.1f}x its fair share"
        elif r <= mean:
            note = "cold"
        else:
            note = ""
        say(f"{s:26} {human_bytes(r):>10}/s  {mult:4.1f}x  {note}", indent=1)

    hot = [s for s, r in ordered if mean and r >= hot_factor * mean][:max_sources]
    cold_pool = [s for s, _ in reversed(ordered) if s not in hot]
    below_mean = [s for s in cold_pool if rates[s] <= mean]
    dests = below_mean if len(below_mean) >= min_dests else cold_pool[:max(min_dests, len(below_mean))]
    return hot, dests


# ---------------------------------------------------------------------------
# cluster snapshot (the before/after evidence)
# ---------------------------------------------------------------------------
def chunk_counts(mongos: MongoClient, ns: str) -> Dict[str, int]:
    coll_doc = mongos["config"]["collections"].find_one({"_id": ns}) or {}
    uuid = coll_doc.get("uuid")
    match = {"uuid": uuid} if uuid is not None else {"ns": ns}
    counts = {}
    for row in mongos["config"]["chunks"].aggregate(
        [{"$match": match}, {"$group": {"_id": "$shard", "n": {"$sum": 1}}}]
    ):
        counts[row["_id"]] = row["n"]
    return counts


def storage_stats(mongos: MongoClient, ns: str) -> Dict[str, Dict[str, Any]]:
    """Per-shard doc count / logical size / avg obj size via $collStats."""
    dbn, cn = ns.split(".", 1)
    out = {}
    try:
        for row in mongos[dbn][cn].aggregate([{"$collStats": {"storageStats": {}}}]):
            st = row.get("storageStats", {})
            out[row.get("shard", "?")] = {
                "count": st.get("count"),
                "size_bytes": st.get("size"),
                "avg_obj_bytes": st.get("avgObjSize"),
            }
    except PyMongoError as e:
        out["error"] = {"count": None, "size_bytes": None, "avg_obj_bytes": str(e)}
    return out


def balancer_state(mongos: MongoClient) -> Dict[str, Any]:
    try:
        status = mongos.admin.command({"balancerStatus": 1})
    except OperationFailure:
        status = {}
    settings = mongos["config"]["settings"].find_one({"_id": "balancer"}) or {}
    return {
        "mode": status.get("mode"),
        "inBalancerRound": status.get("inBalancerRound"),
        "settings": {k: settings.get(k) for k in ("mode", "stopped", "activeWindow") if k in settings},
    }


def take_snapshot(
    mongos: MongoClient,
    mongos_uri: str,
    shard_map: Dict[str, str],
    ns: str,
    heat_shards: List[str],
    window_minutes: int,
    max_scan: int,
    month: Optional[str],
    timeout_ms: int,
    label: str,
    chunk_map: Optional[ChunkMap] = None,
) -> Dict[str, Any]:
    say(f"Taking '{label}' snapshot (chunk counts, storage stats, write heat)...")
    snap = {
        "label": label,
        "taken_at": utc_now().isoformat(),
        "ns": ns,
        "month_filter": month,
        "chunks_per_shard": chunk_counts(mongos, ns),
        "storage_per_shard": storage_stats(mongos, ns),
        "balancer": balancer_state(mongos),
    }
    say(f"Watching the last {window_minutes} min of writes on all "
        f"{len(heat_shards)} shards (this reads each shard's oplog):")
    snap["oplog_heat"] = sample_heat(
        mongos_uri, shard_map, heat_shards, ns, window_minutes, max_scan, month,
        timeout_ms, chunk_map,
    )
    return snap


# ---------------------------------------------------------------------------
# megasite seeding from last month
# ---------------------------------------------------------------------------
def prev_month_megasite_seeds(
    mongos: MongoClient,
    ns: str,
    month: str,
    sample_size: int,
    top_k: int,
) -> List[Dict[str, Any]]:
    """Predict this month's megasites from last month's biggest documents.

    Megasite identity is physical (number of panels/channels), so a site that
    produced a 4 MB document last month will do it again this month. Sampling
    last month's completed docs by $bsonSize is oplog-independent — it works on
    day 1 and is immune to the solar day/night sampling blind spot.
    """
    prev = previous_yyyymm(month)
    dbn, cn = ns.split(".", 1)
    say(f"Also checking last month ({prev}) for known megasites — big documents "
        f"last month usually become big again this month...")
    try:
        rows = list(mongos[dbn][cn].aggregate([
            {"$match": {"_id": {"$regex": f":{prev}$"}}},
            {"$sample": {"size": sample_size}},
            {"$project": {"_id": 1, "bytes": {"$bsonSize": "$$ROOT"}}},
            {"$sort": {"bytes": -1}},
            {"$limit": top_k},
        ], allowDiskUse=True, maxTimeMS=300000))
    except PyMongoError as e:
        verdict(False, f"could not sample last month ({e}); continuing with live heat only")
        return []
    seeds = []
    for r in rows:
        site_id = str(r["_id"]).rsplit(":", 1)[0]
        seeds.append({
            "doc_id": f"{site_id}:{month}",
            "prev_doc_id": str(r["_id"]),
            "prev_bytes": r["bytes"],
        })
    if seeds:
        say(f"Found {len(seeds)} likely megasites (last-month sizes "
            f"{human_bytes(seeds[-1]['prev_bytes'])} to "
            f"{human_bytes(seeds[0]['prev_bytes'])})", indent=1)
    return seeds


# ---------------------------------------------------------------------------
# plan + move + verify
# ---------------------------------------------------------------------------
def explain_owner(mongos: MongoClient, ns: str, doc_id: str) -> Optional[str]:
    dbn, cn = ns.split(".", 1)
    expl = mongos[dbn][cn].find({"_id": doc_id}, {"_id": 1}).explain()
    plan = expl.get("queryPlanner", {}).get("winningPlan", {})
    for holder in (plan, plan.get("inputStage", {})):
        shards = holder.get("shards")
        if shards:
            return shards[0].get("shardName")
    return None


def build_plan(
    mongos: MongoClient,
    heat: Dict[str, Dict[str, Any]],
    ns: str,
    sources: List[str],
    dests: List[str],
    top_n: int,
    seeds: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Rank hot _ids from source-shard samples, confirm ownership, assign dests.

    Candidates come from two signals:
      1. live oplog heat on the source shards (hottest writers right now)
      2. optional megasite seeds predicted from last month's document sizes
         (covers sites that were dark/quiet during the sampling window)
    """
    best: Dict[str, Dict[str, Any]] = {}
    for shard_id in sources:
        for row in heat.get(shard_id, {}).get("hot_ids", [])[: top_n * 3]:
            prev = best.get(row["doc_id"])
            if prev is None or row["oplog_bytes"] > prev["oplog_bytes"]:
                best[row["doc_id"]] = {**row, "sampled_on": shard_id, "signal": "oplog_heat"}

    ranked = sorted(best.values(), key=lambda r: -r["oplog_bytes"])
    for seed in seeds or []:
        if seed["doc_id"] not in best:
            ranked.append({
                "doc_id": seed["doc_id"],
                "oplog_bytes": 0,
                "oplog_updates": 0,
                "sampled_on": None,
                "signal": "prev_month_megasite",
                "prev_bytes": seed["prev_bytes"],
            })

    moves: List[Dict[str, Any]] = []
    seed_hits = 0
    for row in ranked:
        if len(moves) >= top_n:
            break
        owner = explain_owner(mongos, ns, row["doc_id"])
        if owner not in sources:
            continue  # already moved / owned elsewhere / doc not created yet
        dest = dests[len(moves) % len(dests)]
        moves.append({**row, "owner_shard": owner, "to_shard": dest})
        if row["signal"] == "prev_month_megasite":
            seed_hits += 1
    if seeds:
        covered = sum(1 for s in seeds if s["doc_id"] in best)
        if seed_hits:
            say(f"{seed_hits} megasite(s) from last month's list added to the plan — "
                f"they were quiet during the sample window but will likely heat up",
                indent=1)
        else:
            say(f"no extra moves needed from last month's megasite list "
                f"({covered} of them were already caught by the live measurement)",
                indent=1)
    return moves


def build_chunk_plan(
    heat: Dict[str, Dict[str, Any]],
    sources: List[str],
    dests: List[str],
    limit: int,
    exclude: Optional[set] = None,
    fair_sources: bool = False,
) -> List[Dict[str, Any]]:
    """Rank whole chunks on the source shards by aggregated oplog heat.

    Effective when the write load is spread across many documents (no
    megasites): a chunk carries ~50 documents, so its aggregate heat is what
    actually leaves the shard when it is moved. moveChunk is still issued via
    a representative document inside the chunk (top_doc), so execution,
    verification and ChunkTooBig handling are identical to doc mode.

    fair_sources: split limit evenly across sources so one shard's hot chunks
    do not consume the entire move budget (shard-1 got 0 moves in a prod run).
    """
    cands: List[Dict[str, Any]] = []
    seen: set = set()

    def collect_for_shard(shard_id: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for row in heat.get(shard_id, {}).get("hot_chunks", []):
            sampled_on = row.get("sampled_on", row["shard"])
            if sampled_on not in sources or row["chunk_idx"] in seen:
                continue
            if exclude and row["bounds"] in exclude:
                continue
            seen.add(row["chunk_idx"])
            rows.append(row)
        return rows

    if fair_sources and len(sources) > 1:
        per = max(1, limit // len(sources))
        for shard_id in sources:
            shard_rows = sorted(
                collect_for_shard(shard_id), key=lambda r: -r["oplog_bytes"]
            )[:per]
            cands.extend(shard_rows)
        cands.sort(key=lambda r: -r["oplog_bytes"])
        cands = cands[:limit]
    else:
        for shard_id in sources:
            cands.extend(collect_for_shard(shard_id))
        cands.sort(key=lambda r: -r["oplog_bytes"])
        cands = cands[:limit]

    moves: List[Dict[str, Any]] = []
    for row in cands:
        dest = dests[len(moves) % len(dests)]
        moves.append({
            "doc_id": row["top_doc"],
            "chunk_bounds": row["bounds"],
            "oplog_bytes": row["oplog_bytes"],
            "oplog_updates": row["oplog_updates"],
            "active_docs": row["active_docs"],
            "signal": "chunk_heat",
            "sampled_on": row.get("sampled_on", row["shard"]),
            "owner_shard": row["shard"],
            "to_shard": dest,
        })
    return moves


def drain_estimate(heat: Dict[str, Dict[str, Any]], sources: List[str]) -> None:
    """Print how many hottest chunks each source must shed to reach fair share."""
    rates = {
        s: h["bytes_per_sec"] for s, h in heat.items()
        if "bytes_per_sec" in h
    }
    if not rates:
        return
    mean = sum(rates.values()) / len(rates)
    for s in sources:
        need = rates.get(s, 0) - mean
        if need <= 0:
            continue
        span = max(1, heat[s].get("sampled_seconds", 1))
        cum = 0.0
        n = 0
        covered = False
        for row in heat[s].get("hot_chunks", []):
            cum += row["oplog_bytes"] / span
            n += 1
            if cum >= need:
                covered = True
                break
        if covered:
            say(f"{s}: shedding {human_bytes(need)}/s to reach fair share takes "
                f"its ~{n} hottest chunks (they carry {human_bytes(cum)}/s "
                f"combined)", indent=1)
        elif n:
            say(f"{s}: its {n} measured hottest chunks carry only "
                f"{human_bytes(cum)}/s of the {human_bytes(need)}/s it needs "
                f"to shed — plan multiple runs/rounds", indent=1)


_CHUNK_TOO_BIG_RE = re.compile(
    r"maximum number of documents for a chunk is (\d+).*?"
    r"Found (\d+) documents in chunk", re.DOTALL)


def move_chunk(
    mongos: MongoClient, ns: str, doc_id: str, to_shard: str, max_splits: int = 6
) -> Dict[str, Any]:
    """moveChunk with split-and-retry on ChunkTooBig.

    This cluster runs with autosplit disabled and 288MB chunksize, so chunks
    have grown far past the migration limit (shard-1 averages ~900MB). The
    ChunkTooBig error reports the doc count and the per-migration cap, so we
    compute how many median-splits the piece holding the hot document needs
    (each split halves it) and do them all before retrying, instead of paying
    a failed moveChunk round-trip per split. Each split is metadata-only and
    instant. Never uses forceJumbo (which would block writes).
    """
    cmd = {"moveChunk": ns, "find": {"_id": doc_id}, "to": to_shard}
    splits_done = 0
    while True:
        try:
            return mongos.admin.command(cmd)
        except OperationFailure as e:
            msg = str(e)
            if "ChunkTooBig" not in msg or splits_done >= max_splits:
                raise
            m = _CHUNK_TOO_BIG_RE.search(msg)
            if m:
                cap, found = int(m.group(1)), int(m.group(2))
                # halvings until found/2^k fits, with 30% headroom for the
                # uneven splits and doc-size variance seen in production
                need = max(1, math.ceil(math.log2(found / (cap * 0.7))))
            else:
                need = 1
            need = min(need, max_splits - splits_done)
            say(f"chunk too big to move in one piece — splitting it "
                f"{need}x (done {splits_done}/{max_splits}) and retrying",
                indent=2)
            for _ in range(need):
                mongos.admin.command({"split": ns, "find": {"_id": doc_id}})
                splits_done += 1


def execute_moves(
    mongos: MongoClient,
    ns: str,
    moves: List[Dict[str, Any]],
    limit: int,
    sleep_ms: int,
    dry_run: bool,
) -> List[Dict[str, Any]]:
    todo = moves[:limit]
    results = []
    say()
    if dry_run:
        say(f"DRY-RUN: would move {len(todo)} chunk(s). Nothing is changed. "
            f"(planned {len(moves)}, capped at --limit {limit})")
    else:
        say(f"Moving {len(todo)} chunk(s) now "
            f"(planned {len(moves)}, capped at --limit {limit}):")
    for i, m in enumerate(todo, 1):
        doc_id, dest = m["doc_id"], m["to_shard"]
        if m.get("signal") == "chunk_heat":
            label = (f"chunk {m['chunk_bounds']}: {m['owner_shard']} -> {dest}  "
                     f"(chunk caused {human_bytes(m['oplog_bytes'])} of writes / "
                     f"{m['oplog_updates']} updates across {m['active_docs']} "
                     f"active docs)")
        else:
            label = (f"{doc_id}: {m['owner_shard']} -> {dest}  "
                     f"(caused {human_bytes(m['oplog_bytes'])} of writes / "
                     f"{m['oplog_updates']} updates in the sample window)")
        say(f"[{i}/{len(todo)}] {label}", indent=1)
        rec: Dict[str, Any] = {**m, "dry_run": dry_run}
        if dry_run:
            rec.update({"ok": True, "verified": None})
        else:
            try:
                res = move_chunk(mongos, ns, doc_id, dest)
                rec["ok"] = res.get("ok") == 1
                owner_after = explain_owner(mongos, ns, doc_id)
                rec["owner_after"] = owner_after
                rec["verified"] = owner_after == dest
                if rec["verified"]:
                    say(f"moved and verified — {doc_id} now lives on {owner_after}", indent=2)
                else:
                    verdict(False, f"move reported ok but owner is {owner_after}, "
                                   f"expected {dest} — check manually")
            except PyMongoError as e:
                rec.update({"ok": False, "error": str(e), "verified": False})
                # keep the console readable; the full error is in results.json
                short = str(e).split(", full error:")[0]
                if len(short) > 220:
                    short = short[:220] + "..."
                verdict(False, f"move failed (will usually succeed on a later "
                               f"round/run): {short}")
            if sleep_ms and i < len(todo):
                time.sleep(sleep_ms / 1000.0)
        results.append(rec)
    return results


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
def write_report(
    path: str,
    before: Dict[str, Any],
    after: Optional[Dict[str, Any]],
    all_results: List[Dict[str, Any]],
    args: argparse.Namespace,
    rounds_run: int,
    sources: List[str],
    dests: List[str],
) -> None:
    lines: List[str] = []
    add = lines.append
    ok_moves = [r for r in all_results if r.get("ok") and not r.get("dry_run")]
    add(f"# Hot-chunk mover report — {before['ns']}")
    add("")
    add(f"- Run at: {before['taken_at']}  |  rounds: {rounds_run}  |  "
        f"mode: {'DRY-RUN' if not args.execute else 'EXECUTE'}")
    mode = "auto-detected" if args.source_shards.strip().lower() == "auto" else "manual"
    add(f"- Sources ({mode}): {', '.join(sources) or 'none'}  ->  "
        f"Dests: {', '.join(dests) or 'none'}")
    add(f"- Month filter: {before.get('month_filter')}  |  "
        f"heat window: {args.window_minutes}m")
    add(f"- Moves attempted: {len([r for r in all_results if not r.get('dry_run')])}, "
        f"succeeded: {len(ok_moves)}, "
        f"verified: {len([r for r in ok_moves if r.get('verified')])}")
    add("")

    def table(title: str, key: str, fmt) -> None:
        add(f"## {title}")
        add("")
        shard_ids = sorted(
            set(before.get(key, {})) | set((after or {}).get(key, {}))
        )
        add("| shard | before | after | delta |")
        add("|-------|--------|-------|-------|")
        for s in shard_ids:
            b = before.get(key, {}).get(s)
            a = (after or {}).get(key, {}).get(s) if after else None
            add(f"| {s} | {fmt(b)} | {fmt(a)} | {fmt_delta(b, a, fmt)} |")
        add("")

    def fmt_delta(b, a, fmt):
        try:
            if b is None or a is None:
                return "-"
            return f"{fmt(a - b) if isinstance(b, (int, float)) else '-'}"
        except TypeError:
            return "-"

    table("Chunks per shard", "chunks_per_shard", lambda v: f"{v:,}" if isinstance(v, int) else "-")

    add("## Docs & size per shard ($collStats)")
    add("")
    add("| shard | docs before | docs after | Δ docs | size before | size after |")
    add("|-------|------------|-----------|--------|-------------|------------|")
    b_st = before.get("storage_per_shard", {})
    a_st = (after or {}).get("storage_per_shard", {})
    for s in sorted(set(b_st) | set(a_st)):
        b, a = b_st.get(s, {}), a_st.get(s, {})
        bc, ac = b.get("count"), a.get("count")
        d = f"{ac - bc:+,}" if isinstance(bc, int) and isinstance(ac, int) else "-"
        add(f"| {s} | {bc if bc is not None else '-'} | {ac if ac is not None else '-'} | {d} "
            f"| {human_bytes(b.get('size_bytes') or 0)} | {human_bytes(a.get('size_bytes') or 0)} |")
    add("")

    add("## Oplog heat (bytes/sec, sampled window)")
    add("")
    add("| shard | before | after | note |")
    add("|-------|--------|-------|------|")
    b_h = before.get("oplog_heat", {})
    a_h = (after or {}).get("oplog_heat", {})
    for s in sorted(set(b_h) | set(a_h)):
        bh = b_h.get(s, {}).get("bytes_per_sec")
        ah = a_h.get(s, {}).get("bytes_per_sec")
        note = "source" if s in sources else ("dest" if s in dests else "")
        add(f"| {s} | {human_bytes(bh) + '/s' if bh is not None else '-'} "
            f"| {human_bytes(ah) + '/s' if ah is not None else '-'} | {note} |")
    add("")

    if ok_moves:
        add("## Moves executed")
        add("")
        add("| chunk / _id | from | to | oplog bytes | verified |")
        add("|-------------|------|----|-------------|----------|")
        for r in ok_moves:
            label = r.get("chunk_bounds") or r["doc_id"]
            add(f"| {label} | {r['owner_shard']} | {r['to_shard']} "
                f"| {human_bytes(r['oplog_bytes'])} | {r.get('verified')} |")
        add("")

    add("_Success criteria (PS get-well plan): per-shard write heat converging, "
        "hot-shard CPU < 50%, dirty cache < 10% at month-end._")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


# ---------------------------------------------------------------------------
# run history collection
# ---------------------------------------------------------------------------
def compact_heat(heat: Optional[Dict[str, Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    """Per-shard heat reduced to the numbers worth querying later."""
    if heat is None:
        return None
    return {
        s: {
            "bytes_per_sec": round(h.get("bytes_per_sec", 0.0), 1),
            "updates": h.get("total_ops", 0),
            "distinct_docs": h.get("distinct_ids", len(h.get("hot_ids", []))),
            "sampled_seconds": h.get("sampled_seconds", 0),
            "capped": h.get("capped", False),
        }
        for s, h in heat.items()
        if "error" not in h
    }


def record_run_history(mongos: MongoClient, history_ns: str, doc: Dict[str, Any]) -> None:
    """Insert one summary document per run. Never fails the run itself."""
    dbn, cn = history_ns.split(".", 1)
    try:
        mongos[dbn][cn].insert_one(doc)
        verdict(True, f"run recorded in {history_ns} (run_id: {doc['run_id']})")
    except PyMongoError as e:
        verdict(False, f"could not record run history in {history_ns}: {e} "
                       f"(local artifacts are unaffected)")


# ---------------------------------------------------------------------------
# balancer guard
# ---------------------------------------------------------------------------
def ensure_balancer_off(mongos: MongoClient, force: bool) -> bool:
    """Return True if safe to proceed; False if this run must stop (logged)."""
    info = balancer_state(mongos)
    mode_off = info.get("mode") == "off" or info["settings"].get("stopped") \
        or info["settings"].get("mode") == "off"
    if mode_off:
        verdict(True, "MongoDB's own balancer is OFF — safe to do targeted moves")
    else:
        verdict(False, "MongoDB's own balancer is ON — it would undo our targeted moves")
    if info.get("inBalancerRound") and not force:
        say()
        say("ERROR: Native balancer is mid-round. This run stopped.")
        say("Wait for the current balancer round to finish, then:", indent=1)
        say("  sh.stopBalancer()", indent=1)
        say("  sh.getBalancerState()   // must show stopped", indent=1)
        return False
    if not mode_off:
        if force:
            say("Stopping the native balancer (--force-balancer-off given)...", indent=1)
            mongos.admin.command({"balancerStop": 1, "maxTimeMS": 60000})
            verdict(True, "native balancer stopped")
        else:
            say()
            say("ERROR: Native MongoDB balancer is ON. This run stopped.")
            say("Hot-chunk moves and the native balancer conflict — chunks may be "
                "moved twice or undone.", indent=1)
            say("In mongosh:", indent=1)
            say("  sh.stopBalancer()", indent=1)
            say("  sh.getBalancerState()   // must show stopped", indent=1)
            say("Then re-run this script.", indent=1)
            return False
    return True


# ---------------------------------------------------------------------------
# zero-config standalone profile (customer deliverable)
# ---------------------------------------------------------------------------
# ZERO_CONFIG_STANDALONE is True in generated customer standalone builds.
ZERO_CONFIG_STANDALONE = True

STANDALONE_SCRIPT_NAMES = frozenset()

PROD_ZERO_CONFIG = {
    "plan_mode": "chunk",
    "source_shards": "auto",
    "dest_shards": "auto",
    "hot_factor": 1.0,
    "max_sources": 3,
    "min_dests": 3,
    "month": "current",
    "window_minutes": 15,
    "max_scan": 800000,
    "limit": 100,
    "max_moves_total": 100,
    "max_runtime_min": 240,
    "target_imbalance": 1.0,
    "sleep_ms": 3000,
    "round_cooldown_sec": 90,
    "timeout_ms": 20000,
    "history_ns": "hotmover.runs",
    "out_dir": "runs",
}

LAB_ZERO_CONFIG_OVERRIDES = {
    "window_minutes": 5,
    "max_scan": 50000,
    "limit": 50,
    "max_moves_total": 50,
    "max_runtime_min": 60,
    "sleep_ms": 1000,
    "round_cooldown_sec": 30,
    "out_dir": "runs/lab",
}


def zero_config_enabled(argv: List[str]) -> bool:
    if ZERO_CONFIG_STANDALONE:
        return True
    if "--zero-config" in argv:
        return True
    return os.path.basename(sys.argv[0]) in STANDALONE_SCRIPT_NAMES


def pick_zero_config_profile(uri: str) -> Tuple[str, Dict[str, Any]]:
    profile = dict(PROD_ZERO_CONFIG)
    if "potqs.mongodb.net" in uri:
        profile.update(LAB_ZERO_CONFIG_OVERRIDES)
        return "lab (potqs)", profile
    return "production", profile


def apply_zero_config(args: argparse.Namespace, uri: str) -> str:
    name, profile = pick_zero_config_profile(uri)
    for key, val in profile.items():
        setattr(args, key, val)
    args.auto = True
    args.fair_sources = True
    return name


def print_zero_config_profile(name: str, profile: Dict[str, Any], execute: bool) -> None:
    mode = "EXECUTE" if execute else "DRY-RUN"
    tool = os.path.basename(sys.argv[0])
    print(f"{tool} — fixed profile", flush=True)
    print(f"Profile    : {name}", flush=True)
    print(f"Mode       : {mode}", flush=True)
    for key, val in profile.items():
        print(f"  {key}: {val}", flush=True)
    print("  fair-sources: on", flush=True)
    print("  auto: on", flush=True)
    print(flush=True)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    zero_config = zero_config_enabled(argv)
    if zero_config:
        argv = [a for a in argv if a != "--zero-config"]

    ap = argparse.ArgumentParser(
        description="Oplog-heat chunk mover with before/after evidence (dry-run by default).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument(
        "--zero-config",
        action="store_true",
        help="apply the fixed production rollups-3 profile (--auto, fair-sources, "
             "hot-factor/target 1.0, etc.). Only --uri, --ns and --execute are "
             "needed on the command line.",
    )
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", ""), help="mongos URI")
    ap.add_argument("--ns", default=DEFAULT_NS)
    ap.add_argument("--source-shards", default="auto",
                    help="'auto' = detect hot shards from measured oplog heat, "
                         "or an explicit comma-separated list")
    ap.add_argument("--dest-shards", default="auto",
                    help="'auto' = coldest non-hot shards, or an explicit list")
    ap.add_argument("--hot-factor", type=float, default=1.3,
                    help="auto mode: shard is HOT when bytes/sec > factor x cluster mean")
    ap.add_argument("--max-sources", type=int, default=3,
                    help="auto mode: at most this many hot shards drained per run")
    ap.add_argument("--min-dests", type=int, default=3,
                    help="auto mode: at least this many destination shards")
    ap.add_argument("--month", default="current",
                    help="'current', explicit YYYYMM, or 'none' to disable the filter")
    ap.add_argument("--window-minutes", type=int, default=15)
    ap.add_argument("--plan-mode", choices=["chunk", "doc"], default="chunk",
                    help="chunk: rank whole chunks by their aggregate write "
                         "heat (default — effective even when no single "
                         "document is hot). doc: rank individual hot "
                         "documents (megasite hunting)")
    ap.add_argument("--top-n", type=int, default=40,
                    help="max hot _ids per round (doc mode only)")
    ap.add_argument("--limit", type=int, default=20, help="max moves per round")
    ap.add_argument("--rounds", type=int, default=1,
                    help="convergence rounds (re-sample heat between rounds)")
    ap.add_argument("--auto", action="store_true",
                    help="keep running rounds (measure -> move -> re-measure) "
                         "until the imbalance is below --target-imbalance, "
                         "ignoring --rounds. Guarded by --max-moves-total and "
                         "--max-runtime-min")
    ap.add_argument("--max-moves-total", type=int, default=500,
                    help="[--auto] stop after this many successful moves in one "
                         "run (safety ceiling)")
    ap.add_argument("--max-runtime-min", type=int, default=240,
                    help="[--auto] stop starting new rounds after this many "
                         "minutes (safety ceiling)")
    ap.add_argument("--target-imbalance", type=float, default=1.25,
                    help="stop when max(source heat)/mean(heat) drops below this")
    ap.add_argument("--max-scan", type=int, default=50000, help="oplog entries per shard sample")
    ap.add_argument("--seed", action="store_true",
                    help="ALSO seed move candidates from last month's biggest "
                         "documents (megasite prediction). Off by default: the "
                         "unanchored _id regex + $sample is expensive on very "
                         "large collections. Useful early in the month before "
                         "live heat has built up.")
    ap.add_argument("--seed-sample", type=int, default=2000,
                    help="docs sampled from previous month for the megasite seed")
    ap.add_argument("--seed-top", type=int, default=100,
                    help="biggest previous-month docs used as megasite seeds")
    ap.add_argument("--sleep-ms", type=int, default=1000)
    ap.add_argument("--round-cooldown-sec", type=int, default=0,
                    help="[--auto] pause after each execute round before re-sampling "
                         "(orphan cleanup). Standalone profile uses 90s for production.")
    ap.add_argument("--timeout-ms", type=int, default=20000)
    ap.add_argument("--out-dir", default=".", help="artifact directory")
    ap.add_argument("--history-ns", default="hotmover.runs",
                    help="collection (db.coll) that receives one summary "
                         "document per run, via the same mongos")
    ap.add_argument("--no-history", action="store_true",
                    help="do not write the run summary to --history-ns")
    ap.add_argument("--fair-sources", action="store_true",
                    help="chunk mode: split --limit evenly across source shards")
    ap.add_argument("--execute", action="store_true", help="run moveChunk (default: dry-run)")
    ap.add_argument("--force-balancer-off", action="store_true")
    args = ap.parse_args(argv)

    # Strip stray whitespace (e.g. from a shell line-continuation) so a value
    # like "db.coll " can never make the oplog ns filter match nothing.
    for _name in ("ns", "source_shards", "dest_shards", "month", "history_ns", "uri"):
        setattr(args, _name, getattr(args, _name).strip())

    if zero_config or args.zero_config:
        profile_name = apply_zero_config(args, args.uri)
        _, profile = pick_zero_config_profile(args.uri)
        print_zero_config_profile(profile_name, profile, args.execute)
        os.makedirs(args.out_dir, exist_ok=True)

    if not args.uri:
        print("Set --uri or MONGODB_URI (mongos).", file=sys.stderr)
        return 2

    month = None if args.month == "none" else (
        current_yyyymm() if args.month == "current" else args.month
    )
    auto_sources = args.source_shards.strip().lower() == "auto"
    auto_dests = args.dest_shards.strip().lower() == "auto"
    sources = [] if auto_sources else parse_csv(args.source_shards)
    dests = [] if auto_dests else parse_csv(args.dest_shards)
    if sources and dests and set(sources) & set(dests):
        print("ERROR: source and dest shards overlap.", file=sys.stderr)
        return 2

    os.makedirs(args.out_dir, exist_ok=True)
    run_stamp = stamp()

    def artifact(name: str) -> str:
        return os.path.join(args.out_dir, f"hotmover_{run_stamp}_{name}")

    open_run_log(artifact("run.log"))
    run_started = utc_now()
    mode = "EXECUTE (chunks WILL be moved)" if args.execute else \
           "DRY-RUN (read-only preview, nothing will be moved)"
    say("=" * 60)
    say("HOT-CHUNK MOVER — spread write load away from hot shards")
    say("=" * 60)
    if args.auto:
        mode += f" | AUTO: repeat until imbalance < {args.target_imbalance}x"
    say(f"Mode       : {mode}")
    say(f"Plan by    : {'whole-chunk heat (plan-mode=chunk)' if args.plan_mode == 'chunk' else 'hottest documents (plan-mode=doc)'}")
    say(f"Collection : {args.ns}")
    say(f"Month      : {month or 'all months'}")
    say(f"Run log    : {artifact('run.log')}")

    step(1, 6, "Safety checks")
    mongos = MongoClient(args.uri, serverSelectionTimeoutMS=args.timeout_ms)
    if mongos.admin.command("hello").get("msg") != "isdbgrid":
        say("ERROR: --uri must point at a mongos (the cluster router).")
        return 2
    verdict(True, "connected to a mongos")

    shard_map = list_shards(mongos)
    say(f"cluster has {len(shard_map)} shards: {', '.join(sorted(shard_map))}", indent=1)
    missing = [s for s in sources + dests if s not in shard_map]
    if missing:
        say(f"ERROR: shard names {missing} do not exist in this cluster.")
        return 2

    # the namespace must exist as a sharded collection on THIS cluster —
    # a wrong --ns (e.g. prod name on a QA cluster) otherwise looks like a
    # perfectly balanced cluster with zero writes
    ns_entry = mongos["config"]["collections"].find_one({"_id": args.ns})
    if ns_entry is None:
        say(f"ERROR: '{args.ns}' is not a sharded collection on this cluster.")
        tail = args.ns.rsplit(".", 1)[-1]
        candidates = [
            c["_id"] for c in mongos["config"]["collections"].find(
                {"_id": {"$regex": f"{tail}$"}}, {"_id": 1}).limit(5)
        ]
        if candidates:
            say(f"Did you mean one of these? {', '.join(candidates)}", indent=1)
        else:
            say("Check the database name for this environment: "
                "db.getSiblingDB('config').collections.find({}, {_id: 1})", indent=1)
        return 2
    verdict(True, f"'{args.ns}' exists and is sharded on this cluster")

    if not ensure_balancer_off(mongos, force=args.force_balancer_off):
        say()
        say(f"Full log: {artifact('run.log')}")
        if not args.no_history:
            record_run_history(mongos, args.history_ns, {
                "run_id": run_stamp,
                "started_at": run_started,
                "finished_at": utc_now(),
                "host": socket.gethostname(),
                "mode": "execute" if args.execute else "dry_run",
                "outcome": "balancer_blocked",
                "ns": args.ns,
                "month": month,
                "balancer": balancer_state(mongos),
                "artifacts_dir": os.path.abspath(args.out_dir),
            })
        return 2

    step(2, 6, "Measure the current state (the 'before' picture)")
    # Always sample every shard: auto-detection, the imbalance metric, and the
    # report all benefit from the full picture.
    heat_shards = sorted(shard_map)
    chunk_map = None
    if args.plan_mode == "chunk":
        chunk_map = ChunkMap(mongos, args.ns)
        say(f"Loaded {len(chunk_map):,} chunk boundaries — heat will be "
            f"aggregated per chunk (plan-mode=chunk)")
    before = take_snapshot(
        mongos, args.uri, shard_map, args.ns, heat_shards,
        args.window_minutes, args.max_scan, month, args.timeout_ms, "before",
        chunk_map,
    )
    with open(artifact("before.json"), "w", encoding="utf-8") as fh:
        json.dump(before, fh, indent=2, default=str)

    total_updates = sum(
        h.get("total_ops", 0) for h in before["oplog_heat"].values() if "error" not in h
    )
    if total_updates == 0:
        say()
        verdict(False, f"ZERO updates to {args.ns} were seen on any shard in the "
                       f"last {args.window_minutes} min. The tool measures live "
                       "write load — with no writes it cannot find a hot shard.")
        say("Likely causes: nothing is writing right now (quiet window, QA feed "
            "stopped), or the writes go to a different namespace.", indent=1)
        say("Re-run while the update workload is active, or verify recent writes "
            "on a shard primary:", indent=1)
        say(f"  db.getSiblingDB('local').oplog.rs.find("
            f"{{ns: '{args.ns}'}}).sort({{$natural: -1}}).limit(1)", indent=1)

    step(3, 6, "Decide which shards are HOT (too much write load) and which are cold")
    det_hot, det_cold = detect_hot_cold(
        before["oplog_heat"], args.hot_factor, args.max_sources, args.min_dests
    )
    if auto_sources:
        sources = det_hot
    if auto_dests:
        dests = [s for s in det_cold if s not in sources]
    say()
    if sources:
        src_label = "auto-detected" if auto_sources else "OPERATOR-SPECIFIED"
        say(f"Will move load OFF : {', '.join(sources)}  ({src_label})", indent=1)
        say(f"Will move load TO  : {', '.join(dests)}  "
            f"({'auto: coldest shards' if auto_dests else 'operator-specified'})",
            indent=1)
        if not auto_sources:
            say("Hot-factor threshold is bypassed for operator-specified "
                "sources — their busiest documents will be moved regardless "
                "of the measured multiple.", indent=1)

    if not sources:
        say()
        verdict(True, f"no shard is doing {args.hot_factor}x or more of its fair share — "
                      "the cluster is balanced. Nothing to do this run.")
        write_report(artifact("report.md"), before, None, [], args, 0, sources, dests)
        if not args.no_history:
            record_run_history(mongos, args.history_ns, {
                "run_id": run_stamp,
                "started_at": run_started,
                "finished_at": utc_now(),
                "host": socket.gethostname(),
                "mode": "execute" if args.execute else "dry_run",
                "outcome": "balanced_noop",
                "ns": args.ns,
                "month": month,
                "hot_factor": args.hot_factor,
                "heat_before": compact_heat(before["oplog_heat"]),
                "moves_ok": 0,
                "moves_failed": 0,
                "artifacts_dir": os.path.abspath(args.out_dir),
            })
        return 0
    if not dests:
        say("ERROR: no destination shards available to receive chunks.")
        return 2

    ratio = imbalance_ratio(before["oplog_heat"], sources)
    ratio_before = ratio
    sources_before = list(sources)
    say()
    say(f"Imbalance score: {ratio:.2f}x — the hottest shard is doing {ratio:.2f}x "
        f"the average shard's write work. We aim for below {args.target_imbalance}x.", indent=1)

    if args.execute and args.auto and ratio and ratio < args.target_imbalance:
        say()
        verdict(True, f"imbalance {ratio:.2f}x is already below target "
                      f"{args.target_imbalance}x — skipping moves this run.")
        write_report(artifact("report.md"), before, None, [], args, 0, sources, dests)
        if not args.no_history:
            record_run_history(mongos, args.history_ns, {
                "run_id": run_stamp,
                "started_at": run_started,
                "finished_at": utc_now(),
                "host": socket.gethostname(),
                "mode": "execute",
                "outcome": "balanced",
                "ns": args.ns,
                "month": month,
                "hot_factor": args.hot_factor,
                "imbalance_before": round(ratio_before, 3),
                "imbalance_after": round(ratio_before, 3),
                "heat_before": compact_heat(before["oplog_heat"]),
                "moves_ok": 0,
                "moves_failed": 0,
                "artifacts_dir": os.path.abspath(args.out_dir),
            })
        return 0

    step(4, 6, "Choose which documents to move")
    conc = heat_concentration(before["oplog_heat"], sources, args.top_n)
    say("Checking whether a few busy documents cause most of the load:")
    for shard_id, c in conc.items():
        say(f"{shard_id}: its {c['top_n']} busiest documents cause "
            f"{c['top_n_share_pct']}% of all its writes "
            f"({c['distinct_ids']:,} documents were written in total)", indent=1)
    # A capped sample that spans less than one app update cycle (~5 min for
    # rollups) sees each document at most once, so the top-N ranking and the
    # concentration figure below are unreliable — flag that before judging.
    UPDATE_CYCLE_SECS = 300
    short_spans = {
        s: before["oplog_heat"][s]
        for s in sources
        if before["oplog_heat"].get(s, {}).get("capped")
        and before["oplog_heat"][s].get("sampled_seconds", 0) < UPDATE_CYCLE_SECS
    }
    if short_spans:
        needed = 0
        for s, h in short_spans.items():
            rate = h["total_ops"] / max(h["sampled_seconds"], 1)
            needed = max(needed, int(rate * args.window_minutes * 60 * 1.1))
        needed = ((needed // 50000) + 1) * 50000
        for s, h in short_spans.items():
            say(f"WARN | {s}: the --max-scan cap limited its sample to "
                f"{h['sampled_seconds']}s of oplog — shorter than one ~5-min "
                f"update cycle, so most documents appear at most once and the "
                f"'busiest documents' ranking is close to random.", indent=1)
        say(f"WARN | concentration figures above are NOT trustworthy for this "
            f"run. Re-run with --max-scan {needed} to cover the full "
            f"{args.window_minutes}-min window before deciding.", indent=1)
    if conc and all(c["top_n_share_pct"] < 10 for c in conc.values()):
        if short_spans:
            verdict(False, "cannot tell yet whether load is concentrated — the "
                           "sample was too short (see WARN above). Re-run with a "
                           "higher --max-scan before drawing conclusions.")
        elif args.plan_mode == "chunk":
            say("Load is spread across many documents (no megasites) — that is "
                "exactly what plan-mode=chunk is for: it ranks whole chunks by "
                "their combined write heat instead of hunting single documents.",
                indent=1)
        else:
            verdict(False, "load is spread thinly across many documents — moving a few "
                           "chunks won't help much. Re-run with --plan-mode chunk "
                           "(ranks whole chunks by aggregate heat) or use a bulk "
                           "drain (ShardRemovalBalancer).")
    else:
        verdict(True, "load is concentrated in a few documents — moving those "
                      "chunks will meaningfully cool the hot shard(s)")

    if args.plan_mode == "chunk":
        say()
        say("How much has to move to fix the imbalance:")
        drain_estimate(before["oplog_heat"], sources)

    seeds: List[Dict[str, Any]] = []
    if month and args.seed:
        if args.plan_mode == "chunk":
            say("Note: --seed (previous-month megasite list) only applies to "
                "--plan-mode doc; ignoring it in chunk mode.", indent=1)
        else:
            seeds = prev_month_megasite_seeds(
                mongos, args.ns, month, args.seed_sample, args.seed_top
            )

    step(5, 6, "Move the chunks" if args.execute else "Preview the moves (dry-run)")
    all_results: List[Dict[str, Any]] = []
    all_plans: List[List[Dict[str, Any]]] = []
    heat = before["oplog_heat"]
    rounds_run = 0
    failed_keys: set = set()
    max_rounds = 1000 if args.auto else args.rounds
    deadline = time.time() + args.max_runtime_min * 60
    guard_stop = None

    for rnd in range(1, max_rounds + 1):
        # the imbalance target gates auto-detected sources and --auto runs;
        # when the operator named the source shards explicitly (and is not in
        # --auto), drain their top documents regardless
        if (auto_sources or args.auto) and ratio and ratio < args.target_imbalance and not seeds:
            say(f"Imbalance ({ratio:.2f}x) is already below the "
                f"{args.target_imbalance}x target — no more rounds needed.")
            break
        if args.auto:
            moves_so_far = sum(1 for r in all_results if r.get("ok"))
            if moves_so_far >= args.max_moves_total:
                guard_stop = (f"--max-moves-total guard: {moves_so_far} moves "
                              f"done (ceiling {args.max_moves_total})")
                say(f"Stopping: {guard_stop}. Re-run to continue the drain.")
                break
            if time.time() > deadline:
                guard_stop = (f"--max-runtime-min guard: exceeded "
                              f"{args.max_runtime_min} min")
                say(f"Stopping: {guard_stop}. Re-run to continue the drain.")
                break
        rounds_run = rnd
        if args.auto or args.rounds > 1:
            say()
            if args.auto:
                say(f"--- round {rnd} (auto: continue until below "
                    f"{args.target_imbalance}x) ---")
            else:
                say(f"--- round {rnd} of {args.rounds} ---")
        if args.plan_mode == "chunk":
            if failed_keys:
                say(f"(skipping {len(failed_keys)} chunk(s) that failed earlier "
                    f"in this run — they usually succeed on the next run)", indent=1)
            moves = build_chunk_plan(
                heat, sources, dests, args.limit,
                exclude=failed_keys, fair_sources=args.fair_sources,
            )
        else:
            moves = build_plan(
                mongos, heat, args.ns, sources, dests, args.top_n,
                seeds=seeds if rnd == 1 else None,
            )
        # seeds are a round-1-only signal; clear so later rounds (and the
        # early-stop check above) are driven purely by measured heat
        seeds = []
        all_plans.append(moves)
        if not moves:
            if args.plan_mode == "chunk":
                say("No movable hot chunks left on the source shard(s) — stopping.")
            else:
                say("None of the busy documents live on a hot shard right now — stopping.")
            break
        round_limit = args.limit
        if args.auto:
            moves_so_far = sum(1 for r in all_results if r.get("ok"))
            remaining = args.max_moves_total - moves_so_far
            if remaining <= 0:
                guard_stop = (
                    f"--max-moves-total guard: {moves_so_far} moves "
                    f"done (ceiling {args.max_moves_total})"
                )
                say(f"Stopping: {guard_stop}. Re-run to continue the drain.")
                break
            round_limit = min(args.limit, remaining)
            if len(moves) > round_limit:
                moves = moves[:round_limit]
        results = execute_moves(
            mongos, args.ns, moves, round_limit, args.sleep_ms, dry_run=not args.execute
        )
        all_results.extend(results)
        if args.auto and not args.execute:
            break
        if args.auto:
            moves_so_far = sum(1 for r in all_results if r.get("ok"))
            if moves_so_far >= args.max_moves_total:
                guard_stop = (
                    f"--max-moves-total guard: {moves_so_far} moves "
                    f"done (ceiling {args.max_moves_total})"
                )
                say(f"Stopping: {guard_stop}. Re-run to continue the drain.")
                break
        for r in results:
            if not r.get("dry_run") and not r.get("ok"):
                failed_keys.add(r.get("chunk_bounds") or r["doc_id"])
        if not args.execute:
            break  # dry-run: one planning round is enough
        cooldown = args.round_cooldown_sec
        if cooldown and rnd < max_rounds:
            say(f"Waiting {cooldown}s for orphan cleanup before re-sampling...", indent=1)
            time.sleep(cooldown)
        # re-sample heat for convergence check / next round
        say()
        say("Re-measuring write load to see the effect of the moves...")
        if chunk_map is not None:
            # chunk boundaries changed owners (and may have been split) — reload
            chunk_map = ChunkMap(mongos, args.ns)
        heat = sample_heat(
            args.uri, shard_map, heat_shards, args.ns,
            args.window_minutes, args.max_scan, month, args.timeout_ms, chunk_map,
        )
        # re-detect: the hot set can shift as moves land (Feb 2025 lesson —
        # draining shard-1 too far made shards 2/3 hot)
        det_hot, det_cold = detect_hot_cold(
            heat, args.hot_factor, args.max_sources, args.min_dests
        )
        if auto_sources:
            sources = det_hot
        if auto_dests:
            dests = [s for s in det_cold if s not in sources]
        if not sources:
            verdict(True, "no shard is above the hot threshold anymore — done.")
            break
        ratio = imbalance_ratio(heat, sources)
        say(f"Imbalance is now {ratio:.2f}x; still hot: {', '.join(sources)}", indent=1)

    with open(artifact("plan.json"), "w", encoding="utf-8") as fh:
        json.dump({"heat_concentration": conc, "rounds": all_plans}, fh, indent=2, default=str)
    with open(artifact("results.json"), "w", encoding="utf-8") as fh:
        json.dump(all_results, fh, indent=2, default=str)

    step(6, 6, "Wrap up — results and report")
    after = None
    if args.execute and any(not r.get("dry_run") for r in all_results):
        after = take_snapshot(
            mongos, args.uri, shard_map, args.ns, heat_shards,
            args.window_minutes, args.max_scan, month, args.timeout_ms, "after",
        )
        with open(artifact("after.json"), "w", encoding="utf-8") as fh:
            json.dump(after, fh, indent=2, default=str)

    report_path = artifact("report.md")
    write_report(report_path, before, after, all_results, args, rounds_run, sources, dests)

    ok = sum(1 for r in all_results if r.get("ok"))
    failed = sum(1 for r in all_results if not r.get("dry_run") and not r.get("ok"))

    # Final ratio measured over the ORIGINAL hot set against the freshest heat
    # sample — `sources` may have emptied once nothing is hot anymore.
    final_heat = after["oplog_heat"] if after else heat
    final_ratio_sources = imbalance_ratio(final_heat, sources_before)
    final_ratio_cluster = cluster_imbalance_ratio(final_heat, args.hot_factor)

    if not args.no_history:
        record_run_history(mongos, args.history_ns, {
            "run_id": run_stamp,
            "started_at": run_started,
            "finished_at": utc_now(),
            "host": socket.gethostname(),
            "mode": "execute" if args.execute else "dry_run",
            "outcome": (
                "planned" if not args.execute
                else "balanced" if final_ratio_cluster < args.target_imbalance
                else "guard_stopped" if guard_stop
                else "moved"
            ),
            **({"guard_stop": guard_stop} if guard_stop else {}),
            "ns": args.ns,
            "month": month,
            "params": {
                "plan_mode": args.plan_mode,
                "auto": args.auto,
                "window_minutes": args.window_minutes,
                "max_scan": args.max_scan,
                "top_n": args.top_n,
                "limit": args.limit,
                "rounds": args.rounds,
                "hot_factor": args.hot_factor,
                "target_imbalance": args.target_imbalance,
            },
            "sources": sources_before,
            "dests": dests,
            "imbalance_before": round(ratio_before, 3) if ratio_before else None,
            "imbalance_after": round(final_ratio_cluster, 3) if (args.execute and final_ratio_cluster) else None,
            "heat_before": compact_heat(before["oplog_heat"]),
            "heat_after": compact_heat(after["oplog_heat"]) if after else None,
            "heat_concentration": conc,
            "rounds_run": rounds_run,
            "moves_ok": ok,
            "moves_failed": failed,
            "moves": [
                {
                    "doc_id": r["doc_id"],
                    **({"chunk_bounds": r["chunk_bounds"]} if r.get("chunk_bounds") else {}),
                    **({"active_docs": r["active_docs"]} if r.get("active_docs") else {}),
                    "from": r["owner_shard"],
                    "to": r["to_shard"],
                    "oplog_bytes": r["oplog_bytes"],
                    "signal": r.get("signal"),
                    "ok": r.get("ok"),
                    "verified": r.get("verified"),
                    **({"error": r["error"][:500]} if r.get("error") else {}),
                }
                for r in all_results
            ],
            "artifacts_dir": os.path.abspath(args.out_dir),
        })
    say()
    say("=" * 60)
    say("SUMMARY")
    say("=" * 60)
    if args.execute:
        say(f"Chunks moved successfully : {ok} of {len(all_results)}")
        if failed:
            verdict(False, f"{failed} move(s) FAILED — see {artifact('results.json')}")
        if args.auto:
            if guard_stop:
                say(f"AUTO run stopped by a safety guard ({guard_stop}). "
                    f"The cluster is not yet balanced — simply re-run the same "
                    f"command to continue the drain.")
            elif final_ratio_cluster < args.target_imbalance:
                verdict(True, f"AUTO run finished: cluster imbalance "
                              f"{final_ratio_cluster:.2f}x is below "
                              f"{args.target_imbalance}x.")
            else:
                verdict(False, f"AUTO run finished but cluster imbalance is "
                               f"{final_ratio_cluster:.2f}x (target < "
                               f"{args.target_imbalance}x). Re-run to continue.")
        if after or args.execute:
            say(f"Imbalance before -> after (cluster) : "
                f"{cluster_imbalance_ratio(before['oplog_heat'], args.hot_factor):.2f}x -> "
                f"{final_ratio_cluster:.2f}x (target < {args.target_imbalance}x)")
            if sources_before != sources:
                say(f"Original hot sources imbalance : "
                    f"{imbalance_ratio(before['oplog_heat'], sources_before):.2f}x -> "
                    f"{final_ratio_sources:.2f}x", indent=1)
    else:
        say(f"This was a DRY-RUN. {len(all_results)} move(s) are planned; "
            f"nothing was changed.")
        say()
        say("Next step: review the report below, then re-run the same command "
            "with --execute to actually move the chunks.")
    say()
    say(f"Full report : {report_path}")
    say(f"All files   : {artifact('*')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
