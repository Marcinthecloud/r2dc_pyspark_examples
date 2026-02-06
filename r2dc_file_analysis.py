#!/usr/bin/env python3
"""
R2 Data Catalog - Iceberg File Analysis & Compaction Advisor
Analyzes Iceberg table metadata to determine if data compaction or
metadata compaction should be run.

Usage:
  python3 r2dc_file_analysis.py <namespace.table>
  python3 r2dc_file_analysis.py --all <namespace>
  python3 r2dc_file_analysis.py <namespace.table> --target-file-size 128
  python3 r2dc_file_analysis.py <namespace.table> --json results.json

Examples:
  python3 r2dc_file_analysis.py aggregation_tests.sales_data
  python3 r2dc_file_analysis.py --all aggregation_tests
  python3 r2dc_file_analysis.py aggregation_tests.sales_data --target-file-size 64 --json output.json
"""

from r2dc_spark_config import get_spark_session
from datetime import datetime
import argparse
import json
import sys
import math
import textwrap

# ---------------------------------------------------------------------------
# Configurable thresholds (MB unless noted)
# ---------------------------------------------------------------------------
DEFAULT_TARGET_FILE_SIZE_MB = 128  # Iceberg default target file size

# Small file thresholds
SMALL_FILE_THRESHOLD_MB = 8       # Files below this are "small"
TINY_FILE_THRESHOLD_MB = 1        # Files below this are "tiny"

# Scoring boundaries
# -- Data compaction
SMALL_FILE_RATIO_YELLOW = 0.30    # >30% small files = YELLOW
SMALL_FILE_RATIO_RED = 0.60       # >60% small files = RED
FILES_PER_PARTITION_YELLOW = 10   # >10 files per partition = YELLOW
FILES_PER_PARTITION_RED = 50      # >50 files per partition = RED
FILE_SIZE_CV_YELLOW = 0.80        # coefficient of variation >0.8 = YELLOW
FILE_SIZE_CV_RED = 1.50           # coefficient of variation >1.5 = RED
DELETE_FILE_RATIO_YELLOW = 0.05   # >5% delete files = YELLOW
DELETE_FILE_RATIO_RED = 0.20      # >20% delete files = RED

# -- Metadata compaction
SNAPSHOT_COUNT_YELLOW = 20        # >20 snapshots = YELLOW
SNAPSHOT_COUNT_RED = 100          # >100 snapshots = RED
MANIFESTS_PER_SNAPSHOT_YELLOW = 10
MANIFESTS_PER_SNAPSHOT_RED = 50
TOTAL_MANIFEST_SIZE_YELLOW_MB = 50
TOTAL_MANIFEST_SIZE_RED_MB = 200

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
GREEN = "GREEN"
YELLOW = "YELLOW"
RED = "RED"

# Color support - auto-detect TTY, override with --no-color
USE_COLOR = sys.stdout.isatty()

def _score(value, yellow_threshold, red_threshold):
    """Return GREEN/YELLOW/RED based on thresholds."""
    if value >= red_threshold:
        return RED
    elif value >= yellow_threshold:
        return YELLOW
    return GREEN

def _score_label(score):
    """Terminal-friendly colored badge label."""
    if not USE_COLOR:
        return f"{score:>6}"
    colors = {GREEN: "\033[42;30m", YELLOW: "\033[43;30m", RED: "\033[41;97m"}
    reset = "\033[0m"
    return f"{colors.get(score, '')} {score} {reset}"

def _section_title(title):
    """Orange-colored section title."""
    if not USE_COLOR:
        return title
    return f"\033[38;5;208m{title}\033[0m"

def _kv_line(label, value, width=70):
    """Format a key-value line with right-aligned value."""
    padding = width - len(label) - len(str(value))
    if padding < 2:
        padding = 2
    return f"  {label}{'.' * padding}{value}"

def _fmt_bytes(b):
    """Human-readable byte string."""
    if b is None:
        return "N/A"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"

def _safe_div(a, b, default=0):
    if b is None or b == 0:
        return default
    return a / b

def _format_partition_key(partition_data):
    """Turn a Spark Row partition into a readable string like 'sale_timestamp_day=2024-02-06'."""
    if partition_data is None:
        return "unpartitioned"
    try:
        d = partition_data.asDict()
        parts = []
        for k, v in d.items():
            parts.append(f"{k}={v}")
        return ", ".join(parts) if parts else "unpartitioned"
    except Exception:
        return str(partition_data)

# ---------------------------------------------------------------------------
# Metadata collection
# ---------------------------------------------------------------------------

def collect_snapshot_stats(spark, table_name):
    """Query the snapshots metadata table."""
    try:
        df = spark.sql(f"SELECT * FROM {table_name}.snapshots")
        rows = df.collect()
    except Exception as e:
        print(f"  Warning: Could not read snapshots metadata: {e}")
        return {"snapshot_count": 0, "snapshots": [], "error": str(e)}

    snapshots = []
    for r in rows:
        snap = {
            "committed_at": str(r["committed_at"]) if r["committed_at"] else None,
            "snapshot_id": r["snapshot_id"],
            "operation": r["operation"] if "operation" in r.asDict() else None,
        }
        # manifest_list may or may not exist depending on Iceberg version
        if "manifest_list" in r.asDict():
            snap["manifest_list"] = r["manifest_list"]
        snapshots.append(snap)

    oldest = min((s["committed_at"] for s in snapshots if s["committed_at"]), default=None)
    newest = max((s["committed_at"] for s in snapshots if s["committed_at"]), default=None)

    return {
        "snapshot_count": len(snapshots),
        "oldest_snapshot": oldest,
        "newest_snapshot": newest,
        "snapshots": snapshots,
    }


def collect_manifest_stats(spark, table_name):
    """Query the manifests metadata table."""
    try:
        df = spark.sql(f"SELECT * FROM {table_name}.manifests")
        rows = df.collect()
    except Exception as e:
        print(f"  Warning: Could not read manifests metadata: {e}")
        return {"manifest_count": 0, "error": str(e)}

    total_size = 0
    data_file_counts = []
    delete_file_counts = []

    for r in rows:
        rd = r.asDict()
        length = rd.get("length", 0) or 0
        total_size += length

        added_data = rd.get("added_data_files_count", 0) or 0
        existing_data = rd.get("existing_data_files_count", 0) or 0
        data_file_counts.append(added_data + existing_data)

        added_del = rd.get("added_delete_files_count", 0) or 0
        existing_del = rd.get("existing_delete_files_count", 0) or 0
        delete_file_counts.append(added_del + existing_del)

    manifest_count = len(rows)
    avg_data_files = _safe_div(sum(data_file_counts), manifest_count)
    total_delete_refs = sum(delete_file_counts)

    return {
        "manifest_count": manifest_count,
        "total_manifest_size_bytes": total_size,
        "total_manifest_size_human": _fmt_bytes(total_size),
        "avg_data_files_per_manifest": round(avg_data_files, 2),
        "total_delete_file_refs": total_delete_refs,
    }


def collect_data_file_stats(spark, table_name, target_file_size_mb):
    """Query the files metadata table for current snapshot."""
    try:
        df = spark.sql(f"SELECT * FROM {table_name}.files")
        rows = df.collect()
    except Exception as e:
        print(f"  Warning: Could not read files metadata: {e}")
        return {"total_data_files": 0, "error": str(e)}

    small_threshold = SMALL_FILE_THRESHOLD_MB * 1024 * 1024
    tiny_threshold = TINY_FILE_THRESHOLD_MB * 1024 * 1024

    total_files = 0
    total_size = 0
    total_records = 0
    delete_files = 0
    small_files = 0
    tiny_files = 0
    sizes = []

    # Partition-level stats: key = partition string repr
    partitions = {}

    for r in rows:
        rd = r.asDict()

        # Content type: 0 = DATA, 1 = POSITION_DELETES, 2 = EQUALITY_DELETES
        content = rd.get("content", 0)
        file_size = rd.get("file_size_in_bytes", 0) or 0
        record_count = rd.get("record_count", 0) or 0

        if content != 0:
            delete_files += 1
            continue

        total_files += 1
        total_size += file_size
        total_records += record_count
        sizes.append(file_size)

        if file_size < small_threshold:
            small_files += 1
        if file_size < tiny_threshold:
            tiny_files += 1

        partition_data = rd.get("partition", None)
        partition_key = _format_partition_key(partition_data)

        if partition_key not in partitions:
            partitions[partition_key] = {
                "file_count": 0,
                "total_size": 0,
                "total_records": 0,
                "sizes": [],
                "small_files": 0,
                "tiny_files": 0,
                "delete_files": 0,
            }

        p = partitions[partition_key]
        p["file_count"] += 1
        p["total_size"] += file_size
        p["total_records"] += record_count
        p["sizes"].append(file_size)
        if file_size < small_threshold:
            p["small_files"] += 1
        if file_size < tiny_threshold:
            p["tiny_files"] += 1

    for r in rows:
        rd = r.asDict()
        content = rd.get("content", 0)
        if content == 0:
            continue
        partition_data = rd.get("partition", None)
        partition_key = _format_partition_key(partition_data)
        if partition_key in partitions:
            partitions[partition_key]["delete_files"] += 1

    def _distribution(values):
        if not values:
            return {"min": 0, "max": 0, "avg": 0, "median": 0, "std_dev": 0, "cv": 0}
        values_sorted = sorted(values)
        n = len(values_sorted)
        avg = sum(values_sorted) / n
        median = values_sorted[n // 2]
        variance = sum((x - avg) ** 2 for x in values_sorted) / n if n > 1 else 0
        std_dev = math.sqrt(variance)
        cv = _safe_div(std_dev, avg) if avg > 0 else 0
        return {
            "min": values_sorted[0],
            "max": values_sorted[-1],
            "avg": avg,
            "median": median,
            "std_dev": std_dev,
            "cv": round(cv, 4),
            "p10": values_sorted[int(n * 0.1)] if n > 10 else values_sorted[0],
            "p90": values_sorted[int(n * 0.9)] if n > 10 else values_sorted[-1],
        }

    size_dist = _distribution(sizes)
    small_file_ratio = _safe_div(small_files, total_files)
    tiny_file_ratio = _safe_div(tiny_files, total_files)
    delete_file_ratio = _safe_div(delete_files, total_files + delete_files)

    target_bytes = target_file_size_mb * 1024 * 1024
    ideal_file_count = max(1, math.ceil(total_size / target_bytes))
    write_amplification = _safe_div(total_files, ideal_file_count)

    # Per-partition stats
    partition_summaries = {}
    files_per_partition_values = []
    for pk, pdata in partitions.items():
        pdist = _distribution(pdata["sizes"])
        p_small_ratio = _safe_div(pdata["small_files"], pdata["file_count"])
        partition_summaries[pk] = {
            "file_count": pdata["file_count"],
            "total_size_bytes": pdata["total_size"],
            "total_size_human": _fmt_bytes(pdata["total_size"]),
            "total_records": pdata["total_records"],
            "avg_file_size_bytes": round(pdist["avg"]),
            "avg_file_size_human": _fmt_bytes(pdist["avg"]),
            "median_file_size_bytes": pdist["median"],
            "median_file_size_human": _fmt_bytes(pdist["median"]),
            "min_file_size_human": _fmt_bytes(pdist["min"]),
            "max_file_size_human": _fmt_bytes(pdist["max"]),
            "file_size_cv": pdist["cv"],
            "small_files": pdata["small_files"],
            "tiny_files": pdata["tiny_files"],
            "small_file_ratio": round(p_small_ratio, 4),
            "delete_files": pdata["delete_files"],
        }
        files_per_partition_values.append(pdata["file_count"])

    files_per_partition_dist = _distribution(files_per_partition_values) if files_per_partition_values else {}

    compactable_partitions = sum(1 for p in partitions.values() if p["file_count"] > 1)
    non_compactable_partitions = len(partitions) - compactable_partitions

    # Small files that are actually compactable (in partitions with >1 file)
    compactable_small_files = 0
    for pdata in partitions.values():
        if pdata["file_count"] > 1:
            compactable_small_files += pdata["small_files"]

    compactable_small_file_ratio = _safe_div(compactable_small_files, total_files)

    single_file_small_partitions = sum(
        1 for p in partitions.values()
        if p["file_count"] == 1 and p["small_files"] == 1
    )
    over_partition_ratio = _safe_div(single_file_small_partitions, len(partitions))

    avg_partition_size = _safe_div(total_size, len(partitions)) if partitions else 0

    return {
        "total_data_files": total_files,
        "total_delete_files": delete_files,
        "total_data_size_bytes": total_size,
        "total_data_size_human": _fmt_bytes(total_size),
        "total_records": total_records,
        "small_files": small_files,
        "tiny_files": tiny_files,
        "small_file_ratio": round(small_file_ratio, 4),
        "tiny_file_ratio": round(tiny_file_ratio, 4),
        "delete_file_ratio": round(delete_file_ratio, 4),
        "compactable_small_files": compactable_small_files,
        "compactable_small_file_ratio": round(compactable_small_file_ratio, 4),
        "file_size_distribution": {
            "min_bytes": size_dist.get("min", 0),
            "max_bytes": size_dist.get("max", 0),
            "avg_bytes": round(size_dist.get("avg", 0)),
            "median_bytes": size_dist.get("median", 0),
            "std_dev_bytes": round(size_dist.get("std_dev", 0)),
            "cv": size_dist.get("cv", 0),
            "p10_bytes": size_dist.get("p10", 0),
            "p90_bytes": size_dist.get("p90", 0),
            "min_human": _fmt_bytes(size_dist.get("min", 0)),
            "max_human": _fmt_bytes(size_dist.get("max", 0)),
            "avg_human": _fmt_bytes(size_dist.get("avg", 0)),
            "median_human": _fmt_bytes(size_dist.get("median", 0)),
            "p10_human": _fmt_bytes(size_dist.get("p10", 0)),
            "p90_human": _fmt_bytes(size_dist.get("p90", 0)),
        },
        "partition_count": len(partitions),
        "compactable_partitions": compactable_partitions,
        "non_compactable_partitions": non_compactable_partitions,
        "files_per_partition": {
            "min": files_per_partition_dist.get("min", 0),
            "max": files_per_partition_dist.get("max", 0),
            "avg": round(files_per_partition_dist.get("avg", 0), 2),
            "median": files_per_partition_dist.get("median", 0),
        },
        "over_partitioning": {
            "single_file_small_partitions": single_file_small_partitions,
            "over_partition_ratio": round(over_partition_ratio, 4),
            "avg_partition_size_bytes": round(avg_partition_size),
            "avg_partition_size_human": _fmt_bytes(avg_partition_size),
        },
        "ideal_file_count": ideal_file_count,
        "write_amplification": round(write_amplification, 2),
        "target_file_size_mb": target_file_size_mb,
        "partitions": partition_summaries,
    }


def collect_history_stats(spark, table_name):
    """Query the history metadata table."""
    try:
        df = spark.sql(f"SELECT * FROM {table_name}.history")
        rows = df.collect()
    except Exception as e:
        print(f"  Warning: Could not read history metadata: {e}")
        return {"history_entries": 0, "error": str(e)}

    entries = []
    for r in rows:
        rd = r.asDict()
        entries.append({
            "made_current_at": str(rd.get("made_current_at", "")),
            "snapshot_id": rd.get("snapshot_id"),
            "is_current_ancestor": rd.get("is_current_ancestor"),
        })

    return {
        "history_entries": len(entries),
        "entries": entries,
    }


# Scoring 
def compute_scores(snapshot_stats, manifest_stats, file_stats):
    """Compute compaction scores from collected metrics.

    Key insight: compaction merges files WITHIN a partition. A partition with
    only 1 file - no matter how small - cannot benefit from compaction. If most
    partitions have a single tiny file the real problem is over-partitioning
    (partition granularity is too fine), not a lack of compaction.
    """

    scores = {}
    compactable_partitions = file_stats.get("compactable_partitions", 0)
    partition_count = file_stats.get("partition_count", 0)

    compactable_small_ratio = file_stats.get("compactable_small_file_ratio", 0)
    total_small = file_stats.get("small_files", 0)
    compactable_small = file_stats.get("compactable_small_files", 0)
    scores["small_file_ratio"] = {
        "value": file_stats.get("small_file_ratio", 0),
        "compactable_value": compactable_small_ratio,
        "total_small_files": total_small,
        "compactable_small_files": compactable_small,
        "score": _score(compactable_small_ratio, SMALL_FILE_RATIO_YELLOW, SMALL_FILE_RATIO_RED),
        "detail": (
            f"{total_small} small files (<{SMALL_FILE_THRESHOLD_MB}MB), "
            f"{compactable_small} compactable (in partitions with >1 file)"
        ),
    }

    tiny_ratio = file_stats.get("tiny_file_ratio", 0)
    scores["tiny_file_ratio"] = {
        "value": tiny_ratio,
        "score": GREEN,  
        "detail": f"{file_stats.get('tiny_files', 0)} files are <{TINY_FILE_THRESHOLD_MB}MB ({tiny_ratio*100:.1f}% of all files)",
    }

    avg_fpp = file_stats.get("files_per_partition", {}).get("avg", 0)
    max_fpp = file_stats.get("files_per_partition", {}).get("max", 0)
    scores["files_per_partition"] = {
        "avg": avg_fpp,
        "max": max_fpp,
        "compactable_partitions": compactable_partitions,
        "total_partitions": partition_count,
        "score": _score(avg_fpp, FILES_PER_PARTITION_YELLOW, FILES_PER_PARTITION_RED),
        "detail": f"Avg {avg_fpp:.1f} files/partition, max {max_fpp}, {compactable_partitions}/{partition_count} partitions have >1 file",
    }

    cv = file_stats.get("file_size_distribution", {}).get("cv", 0)
    scores["file_size_uniformity"] = {
        "value": cv,
        "score": _score(cv, FILE_SIZE_CV_YELLOW, FILE_SIZE_CV_RED),
        "detail": f"Coefficient of variation: {cv:.2f} (lower=more uniform)",
    }

    del_ratio = file_stats.get("delete_file_ratio", 0)
    scores["delete_files"] = {
        "value": del_ratio,
        "total_delete_files": file_stats.get("total_delete_files", 0),
        "score": _score(del_ratio, DELETE_FILE_RATIO_YELLOW, DELETE_FILE_RATIO_RED),
        "detail": f"{file_stats.get('total_delete_files', 0)} delete files ({del_ratio*100:.1f}% ratio)",
    }

    wa = file_stats.get("write_amplification", 1)
    wa_score = GREEN
    if compactable_partitions > 0:
        wa_score = _score(wa, 3.0, 10.0)
    scores["write_amplification"] = {
        "value": wa,
        "ideal_file_count": file_stats.get("ideal_file_count", 0),
        "actual_file_count": file_stats.get("total_data_files", 0),
        "compactable_partitions": compactable_partitions,
        "score": wa_score,
        "detail": (
            f"{file_stats.get('total_data_files',0)} actual vs {file_stats.get('ideal_file_count',0)} ideal files ({wa:.1f}x)"
            if compactable_partitions > 0
            else f"{file_stats.get('total_data_files',0)} files across {partition_count} partitions (1 file/partition - compaction won't reduce)"
        ),
    }

    # --- Over-partitioning detection ---
    op = file_stats.get("over_partitioning", {})
    op_ratio = op.get("over_partition_ratio", 0)
    avg_part_size = op.get("avg_partition_size_bytes", 0)
    target_bytes = file_stats.get("target_file_size_mb", DEFAULT_TARGET_FILE_SIZE_MB) * 1024 * 1024

    is_over_partitioned = (op_ratio > 0.50 and avg_part_size < target_bytes * 0.5)
    op_score = RED if (op_ratio > 0.80 and avg_part_size < target_bytes * 0.25) else (
        YELLOW if is_over_partitioned else GREEN
    )
    scores["over_partitioning"] = {
        "over_partition_ratio": op_ratio,
        "single_file_small_partitions": op.get("single_file_small_partitions", 0),
        "avg_partition_size_bytes": op.get("avg_partition_size_bytes", 0),
        "avg_partition_size_human": op.get("avg_partition_size_human", "N/A"),
        "target_file_size_mb": file_stats.get("target_file_size_mb", DEFAULT_TARGET_FILE_SIZE_MB),
        "score": op_score,
        "detail": (
            f"{op.get('single_file_small_partitions',0)}/{partition_count} partitions have a single small file, "
            f"avg partition size {op.get('avg_partition_size_human','N/A')} vs {file_stats.get('target_file_size_mb', DEFAULT_TARGET_FILE_SIZE_MB)}MB target"
        ),
    }

    snap_count = snapshot_stats.get("snapshot_count", 0)
    scores["snapshot_count"] = {
        "value": snap_count,
        "score": _score(snap_count, SNAPSHOT_COUNT_YELLOW, SNAPSHOT_COUNT_RED),
        "detail": f"{snap_count} snapshots retained",
    }

    manifest_count = manifest_stats.get("manifest_count", 0)
    manifests_per_snap = _safe_div(manifest_count, snap_count) if snap_count > 0 else manifest_count
    scores["manifests_per_snapshot"] = {
        "value": round(manifests_per_snap, 2),
        "total_manifests": manifest_count,
        "score": _score(manifests_per_snap, MANIFESTS_PER_SNAPSHOT_YELLOW, MANIFESTS_PER_SNAPSHOT_RED),
        "detail": f"{manifest_count} manifests across {snap_count} snapshots ({manifests_per_snap:.1f} per snapshot)",
    }

    manifest_size_mb = manifest_stats.get("total_manifest_size_bytes", 0) / (1024 * 1024)
    scores["manifest_total_size"] = {
        "value_bytes": manifest_stats.get("total_manifest_size_bytes", 0),
        "value_human": manifest_stats.get("total_manifest_size_human", "N/A"),
        "score": _score(manifest_size_mb, TOTAL_MANIFEST_SIZE_YELLOW_MB, TOTAL_MANIFEST_SIZE_RED_MB),
        "detail": f"Total manifest size: {manifest_stats.get('total_manifest_size_human', 'N/A')}",
    }

    data_scores = [
        scores["small_file_ratio"]["score"],
        scores["files_per_partition"]["score"],
        scores["file_size_uniformity"]["score"],
        scores["delete_files"]["score"],
        scores["write_amplification"]["score"],
    ]
    meta_scores = [
        scores["snapshot_count"]["score"],
        scores["manifests_per_snapshot"]["score"],
        scores["manifest_total_size"]["score"],
    ]

    def _overall(score_list):
        if RED in score_list:
            return RED
        if YELLOW in score_list:
            return YELLOW
        return GREEN

    scores["data_compaction_verdict"] = _overall(data_scores)
    scores["metadata_compaction_verdict"] = _overall(meta_scores)

    return scores


# Terminal output
def _scoreboard_line(badge, label, short_value, width=70):
    """Format a scoreboard row: [BADGE] Label ..... value"""
    badge_str = _score_label(badge)
    # Account for ANSI codes in badge width for alignment
    visible_badge_len = len(badge) + 2  # " BADGE "
    label_part = f"  {badge_str}  {label}"
    visible_len = 2 + visible_badge_len + 2 + len(label)
    padding = width - visible_len - len(str(short_value))
    if padding < 2:
        padding = 2
    return f"{label_part}{'.' * padding}{short_value}"

def _verdict_line(label, badge, message):
    """Format a verdict line: LABEL [BADGE] message"""
    return f"  {label}  {_score_label(badge)}  {message}"


def print_report(table_name, snapshot_stats, manifest_stats, file_stats, history_stats, scores, target_file_size_mb):
    """Print a human-readable report to the terminal."""
    W = 72
    SEP = "  " + "\u2500" * (W - 2)

    # --- Header ---
    print()
    print("=" * W)
    print(f"  {_section_title(f'ICEBERG FILE ANALYSIS: {table_name}')}")
    print("=" * W)
    print(f"  Analysis Time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  Target File Size : {target_file_size_mb} MB")
    print("=" * W)

    # --- Table overview ---
    print(SEP)
    print(f"  {_section_title('TABLE OVERVIEW')}")
    print(SEP)
    print(_kv_line("Total Data Files", f"{file_stats.get('total_data_files', 0):,}", W))
    print(_kv_line("Total Delete Files", f"{file_stats.get('total_delete_files', 0):,}", W))
    print(_kv_line("Total Data Size", file_stats.get('total_data_size_human', 'N/A'), W))
    print(_kv_line("Total Records", f"{file_stats.get('total_records', 0):,}", W))
    print(_kv_line("Partition Count", f"{file_stats.get('partition_count', 0):,}", W))
    print(_kv_line("Snapshot Count", f"{snapshot_stats.get('snapshot_count', 0):,}", W))
    print(_kv_line("Manifest Count", f"{manifest_stats.get('manifest_count', 0):,}", W))

    # --- File size distribution ---
    dist = file_stats.get("file_size_distribution", {})
    print()
    print(SEP)
    print(f"  {_section_title('FILE SIZE DISTRIBUTION')}")
    print(SEP)
    print(_kv_line("Min", dist.get('min_human', 'N/A'), W))
    print(_kv_line("P10", dist.get('p10_human', 'N/A'), W))
    print(_kv_line("Median", dist.get('median_human', 'N/A'), W))
    print(_kv_line("Average", dist.get('avg_human', 'N/A'), W))
    print(_kv_line("P90", dist.get('p90_human', 'N/A'), W))
    print(_kv_line("Max", dist.get('max_human', 'N/A'), W))
    print(_kv_line("Std Dev", _fmt_bytes(dist.get('std_dev_bytes', 0)), W))
    print(_kv_line("CV", f"{dist.get('cv', 0):.4f}", W))

    # --- Small file analysis ---
    print()
    print(SEP)
    print(f"  {_section_title('SMALL FILE ANALYSIS')}")
    print(SEP)
    sf = file_stats.get('small_files', 0)
    sf_ratio = file_stats.get('small_file_ratio', 0)
    csf = file_stats.get('compactable_small_files', 0)
    csf_ratio = file_stats.get('compactable_small_file_ratio', 0)
    tf = file_stats.get('tiny_files', 0)
    tf_ratio = file_stats.get('tiny_file_ratio', 0)
    print(_kv_line(f"Small files (<{SMALL_FILE_THRESHOLD_MB}MB)", f"{sf:,} ({sf_ratio*100:.1f}%)", W))
    print(_kv_line("  - Compactable (>1 file/part)", f"{csf:,} ({csf_ratio*100:.1f}%)", W))
    print(_kv_line("  - Not compactable (1/part)", f"{sf - csf:,}", W))
    print(_kv_line(f"Tiny files (<{TINY_FILE_THRESHOLD_MB}MB)", f"{tf:,} ({tf_ratio*100:.1f}%)", W))
    print(_kv_line("Target file size", f"{target_file_size_mb} MB", W))
    print(_kv_line("Actual file count", f"{file_stats.get('total_data_files', 0):,}", W))
    print(_kv_line("Compactable partitions", f"{file_stats.get('compactable_partitions', 0):,} / {file_stats.get('partition_count', 0):,}", W))

    # --- Partition sizing ---
    op = file_stats.get("over_partitioning", {})
    print()
    print(SEP)
    print(f"  {_section_title('PARTITION SIZING')}")
    print(SEP)
    print(_kv_line("Avg partition size", op.get('avg_partition_size_human', 'N/A'), W))
    print(_kv_line("Target file size", f"{target_file_size_mb} MB", W))
    print(_kv_line("Single-file small partitions", f"{op.get('single_file_small_partitions', 0):,} / {file_stats.get('partition_count', 0):,}", W))
    print(_kv_line("Over-partition ratio", f"{op.get('over_partition_ratio', 0)*100:.1f}%", W))

    # --- Metadata health ---
    print()
    print(SEP)
    print(f"  {_section_title('METADATA HEALTH')}")
    print(SEP)
    print(_kv_line("Snapshots", f"{snapshot_stats.get('snapshot_count', 0):,}", W))
    print(_kv_line("Oldest Snapshot", str(snapshot_stats.get('oldest_snapshot', 'N/A')), W))
    print(_kv_line("Newest Snapshot", str(snapshot_stats.get('newest_snapshot', 'N/A')), W))
    print(_kv_line("Total Manifests", f"{manifest_stats.get('manifest_count', 0):,}", W))
    print(_kv_line("Total Manifest Size", manifest_stats.get('total_manifest_size_human', 'N/A'), W))
    print(_kv_line("Avg Data Files/Manifest", f"{manifest_stats.get('avg_data_files_per_manifest', 0):.1f}", W))
    print(_kv_line("Delete File Refs", f"{manifest_stats.get('total_delete_file_refs', 0):,}", W))
    print(_kv_line("History Entries", f"{history_stats.get('history_entries', 0):,}", W))

    # --- Per-partition detail (top 10 worst) ---
    parts = file_stats.get("partitions", {})
    if parts:
        sorted_parts = sorted(parts.items(), key=lambda x: x[1]["file_count"], reverse=True)
        show_count = min(10, len(sorted_parts))
        print()
        print(SEP)
        print(f"  {_section_title(f'TOP {show_count} PARTITIONS BY FILE COUNT')}")
        print(SEP)
        print(f"  {'Partition':<35} {'Files':>6} {'Size':>10} {'Avg':>10} {'Sm%':>5} {'CV':>5}")
        ln = "\u2500"
        print(f"  {ln*35} {ln*6} {ln*10} {ln*10} {ln*5} {ln*5}")
        for pk, pdata in sorted_parts[:show_count]:
            display_key = pk if len(pk) <= 34 else pk[:31] + "..."
            small_pct = pdata["small_file_ratio"] * 100
            print(f"  {display_key:<35} {pdata['file_count']:>6} {pdata['total_size_human']:>10} {pdata['avg_file_size_human']:>10} {small_pct:>4.0f}% {pdata['file_size_cv']:>5.2f}")

    # --- Scoreboard ---
    print()
    print(SEP)
    print(f"  {_section_title('COMPACTION SCOREBOARD')}")
    print(SEP)
    print()

    # Build short summaries for right-aligned display
    s = scores
    print(f"  DATA COMPACTION INDICATORS:")
    print(_scoreboard_line(s['small_file_ratio']['score'], "Compactable Small Files", f"{s['small_file_ratio']['compactable_small_files']} compactable", W))
    print(_scoreboard_line(s['files_per_partition']['score'], "Files Per Partition", f"Avg {s['files_per_partition']['avg']:.1f} files/part", W))
    print(_scoreboard_line(s['file_size_uniformity']['score'], "File Size Uniformity", f"CV: {s['file_size_uniformity']['value']:.2f}", W))
    print(_scoreboard_line(s['delete_files']['score'], "Delete Files", f"{s['delete_files']['total_delete_files']} delete files", W))
    wa_summary = f"{s['files_per_partition']['avg']:.0f} file/partition" if s['write_amplification']['compactable_partitions'] == 0 else f"{s['write_amplification']['value']:.1f}x amplification"
    print(_scoreboard_line(s['write_amplification']['score'], "Write Amplification", wa_summary, W))
    print()

    print(f"  PARTITION HEALTH:")
    op_summary = f"{s['over_partitioning']['single_file_small_partitions']}/{s['over_partitioning'].get('single_file_small_partitions',0) + file_stats.get('compactable_partitions',0)} single-file small"
    print(_scoreboard_line(s['over_partitioning']['score'], "Over-Partitioning", op_summary, W))
    print(_scoreboard_line(s['tiny_file_ratio']['score'], "Tiny Files (info)", f"{file_stats.get('tiny_files',0)} files <{TINY_FILE_THRESHOLD_MB}MB", W))
    print()

    print(f"  METADATA COMPACTION:")
    print(_scoreboard_line(s['snapshot_count']['score'], "Snapshot Count", f"{s['snapshot_count']['value']} snapshots retained", W))
    print(_scoreboard_line(s['manifests_per_snapshot']['score'], "Manifests/Snapshot", f"{s['manifests_per_snapshot']['value']:.1f} per snapshot", W))
    print(_scoreboard_line(s['manifest_total_size']['score'], "Manifest Total Size", manifest_stats.get('total_manifest_size_human', 'N/A'), W))

    # --- Verdicts ---
    print()
    print(SEP)
    dv = scores["data_compaction_verdict"]
    mv = scores["metadata_compaction_verdict"]
    dv_msg = {GREEN: "No compaction needed", YELLOW: "Consider compaction", RED: "Compaction recommended"}
    mv_msg = {GREEN: "Metadata is healthy", YELLOW: "Expire snapshots", RED: "Expire snapshots and rewrite manifests"}
    print(_verdict_line("DATA COMPACTION", dv, dv_msg.get(dv, "")))
    print(_verdict_line("META COMPACTION", mv, mv_msg.get(mv, "")))
    print(SEP)

    # --- Recommendations ---
    print()
    print(f"  {_section_title('RECOMMENDATIONS')}")
    print(SEP)
    recs = _build_recommendations(scores, file_stats, snapshot_stats, manifest_stats, table_name)
    if not recs:
        print("  No action needed. Table is healthy.")
    for i, rec in enumerate(recs, 1):
        lines = textwrap.wrap(rec, width=W - 6)
        print(f"  {i}. {lines[0]}")
        for line in lines[1:]:
            print(f"     {line}")


def _build_recommendations(scores, file_stats, snapshot_stats, manifest_stats, table_name):
    """Generate actionable recommendation strings."""
    recs = []

    if scores["over_partitioning"]["score"] != GREEN:
        op = file_stats.get("over_partitioning", {})
        recs.append(
            f"Table appears over-partitioned: {op.get('single_file_small_partitions',0):,} of "
            f"{file_stats.get('partition_count',0):,} partitions contain a single small file "
            f"(avg partition size: {op.get('avg_partition_size_human','N/A')}). "
            f"Compaction cannot merge files across partitions. Consider using a coarser "
            f"partition granularity (e.g., months() instead of days())."
        )

    if scores["small_file_ratio"]["score"] != GREEN:
        compactable = file_stats.get("compactable_small_files", 0)
        recs.append(
            f"Run data compaction to merge {compactable:,} small files in partitions "
            f"that have multiple files."
        )

    if scores["delete_files"]["score"] != GREEN:
        recs.append(
            f"Run compaction to merge {file_stats.get('total_delete_files',0):,} delete files "
            f"back into data files."
        )

    if scores["write_amplification"]["score"] != GREEN:
        recs.append(
            f"Write amplification is {file_stats.get('write_amplification',0):.1f}x. "
            f"Compaction would reduce files in the {file_stats.get('compactable_partitions',0):,} "
            f"partitions that have >1 file."
        )

    # If data compaction is GREEN but there are still lots of small files,
    # explain why compaction isn't the answer.
    if (scores["data_compaction_verdict"] == GREEN
            and file_stats.get("small_file_ratio", 0) > 0.50
            and file_stats.get("compactable_partitions", 0) == 0):
        recs.append(
            f"Note: {file_stats.get('small_files',0):,} files are small but each partition "
            f"has only 1 file. Compaction will not reduce file count. The small file sizes "
            f"are caused by the partition granularity, not by lack of compaction."
        )

    if scores["snapshot_count"]["score"] != GREEN:
        recs.append(
            f"Expire old snapshots ({snapshot_stats.get('snapshot_count',0):,} currently retained). "
            f"Run: CALL system.expire_snapshots(table => '{table_name}', older_than => TIMESTAMP '...')"
        )

    if scores["manifests_per_snapshot"]["score"] != GREEN:
        recs.append(
            f"Rewrite manifests to reduce count ({manifest_stats.get('manifest_count',0):,} total). "
            f"Run: CALL system.rewrite_manifests(table => '{table_name}')"
        )

    if scores["manifest_total_size"]["score"] != GREEN:
        recs.append(
            f"Manifest metadata is large ({manifest_stats.get('total_manifest_size_human','N/A')}). "
            f"Expire snapshots and rewrite manifests to reduce size."
        )

    return recs


# JSON export
def build_json_output(table_name, snapshot_stats, manifest_stats, file_stats, history_stats, scores, target_file_size_mb):
    """Build the full JSON output dict with raw metrics and scores."""

    snapshot_summary = {k: v for k, v in snapshot_stats.items() if k != "snapshots"}
    history_summary = {k: v for k, v in history_stats.items() if k != "entries"}

    return {
        "analysis_time": datetime.now().isoformat(),
        "table_name": table_name,
        "target_file_size_mb": target_file_size_mb,
        "overview": {
            "total_data_files": file_stats.get("total_data_files", 0),
            "total_delete_files": file_stats.get("total_delete_files", 0),
            "total_data_size_bytes": file_stats.get("total_data_size_bytes", 0),
            "total_data_size_human": file_stats.get("total_data_size_human", "N/A"),
            "total_records": file_stats.get("total_records", 0),
            "partition_count": file_stats.get("partition_count", 0),
            "snapshot_count": snapshot_stats.get("snapshot_count", 0),
            "manifest_count": manifest_stats.get("manifest_count", 0),
        },
        "file_size_distribution": file_stats.get("file_size_distribution", {}),
        "files_per_partition": file_stats.get("files_per_partition", {}),
        "small_file_analysis": {
            "small_file_count": file_stats.get("small_files", 0),
            "small_file_ratio": file_stats.get("small_file_ratio", 0),
            "compactable_small_files": file_stats.get("compactable_small_files", 0),
            "compactable_small_file_ratio": file_stats.get("compactable_small_file_ratio", 0),
            "small_file_threshold_mb": SMALL_FILE_THRESHOLD_MB,
            "tiny_file_count": file_stats.get("tiny_files", 0),
            "tiny_file_ratio": file_stats.get("tiny_file_ratio", 0),
            "tiny_file_threshold_mb": TINY_FILE_THRESHOLD_MB,
            "compactable_partitions": file_stats.get("compactable_partitions", 0),
            "non_compactable_partitions": file_stats.get("non_compactable_partitions", 0),
            "ideal_file_count": file_stats.get("ideal_file_count", 0),
            "write_amplification": file_stats.get("write_amplification", 0),
        },
        "over_partitioning": file_stats.get("over_partitioning", {}),
        "metadata_health": {
            "snapshot_count": snapshot_summary.get("snapshot_count", 0),
            "oldest_snapshot": snapshot_summary.get("oldest_snapshot"),
            "newest_snapshot": snapshot_summary.get("newest_snapshot"),
            "manifest_count": manifest_stats.get("manifest_count", 0),
            "total_manifest_size_bytes": manifest_stats.get("total_manifest_size_bytes", 0),
            "total_manifest_size_human": manifest_stats.get("total_manifest_size_human", "N/A"),
            "avg_data_files_per_manifest": manifest_stats.get("avg_data_files_per_manifest", 0),
            "total_delete_file_refs": manifest_stats.get("total_delete_file_refs", 0),
            "history_entries": history_summary.get("history_entries", 0),
        },
        "partitions": file_stats.get("partitions", {}),
        "scores": {k: v for k, v in scores.items()},
        "recommendations": _build_recommendations(
            scores, file_stats,
            snapshot_stats, manifest_stats,
            table_name,
        ),
    }


# Main
def analyze_table(spark, table_name, target_file_size_mb, json_path=None):
    """Run full analysis on a single table."""
    print(f"\n  Collecting snapshots metadata...")
    snapshot_stats = collect_snapshot_stats(spark, table_name)

    print(f"  Collecting manifests metadata...")
    manifest_stats = collect_manifest_stats(spark, table_name)

    print(f"  Collecting data file metadata...")
    file_stats = collect_data_file_stats(spark, table_name, target_file_size_mb)

    print(f"  Collecting history metadata...")
    history_stats = collect_history_stats(spark, table_name)

    print(f"  Computing scores...")
    scores = compute_scores(snapshot_stats, manifest_stats, file_stats)

    print_report(table_name, snapshot_stats, manifest_stats, file_stats, history_stats, scores, target_file_size_mb)

    if json_path:
        output = build_json_output(table_name, snapshot_stats, manifest_stats, file_stats, history_stats, scores, target_file_size_mb)
        with open(json_path, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"  JSON results saved to: {json_path}")

    return scores


def main():
    parser = argparse.ArgumentParser(
        description="Iceberg File Analysis & Compaction Advisor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 r2dc_file_analysis.py aggregation_tests.sales_data
  python3 r2dc_file_analysis.py --all aggregation_tests
  python3 r2dc_file_analysis.py aggregation_tests.sales_data --target-file-size 64
  python3 r2dc_file_analysis.py aggregation_tests.sales_data --json analysis.json
        """,
    )
    parser.add_argument("table", nargs="?", help="Fully qualified table name (namespace.table)")
    parser.add_argument("--all", metavar="NAMESPACE", help="Analyze all tables in a namespace")
    parser.add_argument(
        "--target-file-size", type=int, default=DEFAULT_TARGET_FILE_SIZE_MB,
        help=f"Target file size in MB for compaction scoring (default: {DEFAULT_TARGET_FILE_SIZE_MB})",
    )
    parser.add_argument("--json", metavar="PATH", help="Save detailed results to a JSON file")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")

    args = parser.parse_args()

    if args.no_color:
        global USE_COLOR
        USE_COLOR = False

    if not args.table and not args.all:
        parser.print_help()
        sys.exit(1)

    spark = get_spark_session("IcebergFileAnalysis")

    try:
        if args.all:
            namespace = args.all
            print(f"\nScanning all tables in namespace: {namespace}")
            tables_df = spark.sql(f"SHOW TABLES IN {namespace}")
            tables = [row["tableName"] for row in tables_df.collect()]

            if not tables:
                print(f"  No tables found in namespace '{namespace}'")
                sys.exit(0)

            print(f"  Found {len(tables)} table(s): {', '.join(tables)}")

            all_results = {}
            for tbl in tables:
                fq_name = f"{namespace}.{tbl}"
                print(f"\n{'#' * 80}")
                print(f"  ANALYZING: {fq_name}")
                print(f"{'#' * 80}")
                scores = analyze_table(spark, fq_name, args.target_file_size)
                all_results[fq_name] = scores

            if args.json:
                print(f"\n  Note: Use per-table --json for full details.")

            print(f"\n{'=' * 80}")
            print(f"  MULTI-TABLE SUMMARY ({namespace})")
            print(f"{'=' * 80}")
            print(f"  {'Table':<40} {'Data':>10} {'Metadata':>10}")
            print(f"  {'-'*40} {'-'*10} {'-'*10}")
            for tbl, sc in all_results.items():
                dv = sc["data_compaction_verdict"]
                mv = sc["metadata_compaction_verdict"]
                print(f"  {tbl:<40} {_score_label(dv):>19} {_score_label(mv):>19}")
            print(f"{'=' * 80}")

        else:
            analyze_table(spark, args.table, args.target_file_size, args.json)

    except Exception as e:
        print(f"\nError during analysis: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
