"""Judge captured ordinary SQL evidence; never supplies block authority.

SOURCE_ENTRIES_PRESENT is only a source census, not a native run verdict.
"""
import argparse
import json
from pathlib import Path
import re
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value):
    require(type(value) is int or isinstance(value, str), "missing integer")
    require(re.fullmatch(r"0|[1-9][0-9]*", str(value)) is not None, "invalid integer")
    return int(value)


def mapping(value):
    require(isinstance(value, dict), "missing object")
    return value


def source_precheck(root):
    # These are original production entry names, not replacement implementations.
    entries = (
        ("src/backend/cluster/cluster_gcs_block.c", "cluster_gcs_resource_x_target_acquire_reobserve_exact", "E/C"),
        ("src/backend/storage/buffer/bufmgr.c", "cluster_bufmgr_pcm_x_writer_prepare_target", "E/C"),
        ("src/backend/storage/buffer/bufmgr.c", "cluster_bufmgr_pcm_x_writer_activate", "E/C"),
        ("src/backend/access/heap/heapam.c", "cluster_heap_vm_mutation_prepare", "K/C"),
        ("src/backend/access/heap/heapam.c", "cluster_heap_vm_mutation_lock", "K/C"),
        ("src/backend/access/heap/visibilitymap.c", "visibilitymap_set_empty", "K/C"),
        ("src/backend/access/heap/visibilitymap.c", "visibilitymap_clear_locked", "K/C"),
        ("src/backend/access/heap/heapam.c", "heap_insert", "K/C"),
    )
    missing = []
    for path, name, owner in entries:
        file = Path(root) / path
        source = file.read_text() if file.is_file() else ""
        # Anchored C definitions: a prototype or comment mentioning the name
        # must not pass. This is a bounded census, not a call-graph proof.
        source = re.sub(r"/\*.*?\*/|//[^\n]*", "", source, flags=re.S)
        if not re.search(r"^" + name + r"\([^;{}]*\)\s*\{", source, re.M):
            missing.append({"file": path, "function": name, "owner": owner})
    return {"status": "SOURCE_RED" if missing else "SOURCE_ENTRIES_PRESENT", "missing": missing}


def evaluate(evidence):
    e = mapping(evidence)
    require(type(e.get("version")) is int and e["version"] == 1, "invalid evidence version")
    require(type(e.get("writer_node")) is int and e["writer_node"] == 2, "wrong writer node")
    heap = e.get("heap_tag", "")
    match = re.fullmatch(r"([1-9][0-9]*/[1-9][0-9]*/[1-9][0-9]*)/0/0", heap)
    require(match is not None, "one exact heap block-zero tag required")
    locator = match[1]
    require(e.get("vm_tag") == locator + "/2/0", "wrong VM tag")
    index = e.get("index_locator", "")
    require(re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*/[1-9][0-9]*", index) is not None
            and index != locator and index.rsplit("/", 1)[0] == locator.rsplit("/", 1)[0], "wrong index locator")
    for stage in ("published", "retained", "cleared"):
        view = mapping(e.get(stage))
        expected = stage != "cleared"
        require(view.get("all_visible") is expected and view.get("all_frozen") is expected,
                "missing exact VM flags at " + stage)
    before, after = mapping(e.get("before")), mapping(e.get("after"))
    require(before.get("vm_stats_available") == after.get("vm_stats_available") == "true", "VM metrics unavailable")
    require(after.get("vm_observed_tag_available") == "true"
            and after.get("vm_observed_tag") == e["vm_tag"], "metrics bound to another VM")
    if before.get("vm_observed_tag_available") == "true":
        require(before.get("vm_observed_tag") == e["vm_tag"], "old metrics bound to another VM")
    else:
        require(before.get("vm_observed_tag_available") == "false", "unknown old metrics tag")
    counters = ("vm_x_acquisition_completed_count", "vm_remote_shared_source_count",
                "vm_inter_node_transfer_count", "vm_all_visible_clear_count", "vm_other_tag_observations")
    delta = {key: integer(after.get(key)) - integer(before.get(key)) for key in counters}
    require(all(value >= 0 for value in delta.values()), "counter rollback")
    if before.get("vm_observed_tag_available") == "false":
        require(all(integer(before[key]) == 0 for key in counters), "unbound nonzero baseline")
    require(delta["vm_other_tag_observations"] == 0, "contaminated exact-tag interval")
    require(delta["vm_x_acquisition_completed_count"] > 0, "no actual native X completion")
    require(delta["vm_remote_shared_source_count"] + delta["vm_inter_node_transfer_count"] > 0,
            "no actual remote VM source")
    require(delta["vm_all_visible_clear_count"] > 0, "no actual AV clear")
    xid = integer(e.get("xid"))
    require(3 <= xid <= 0xffffffff, "invalid original WAL xid")
    wal = e.get("wal")
    require(isinstance(wal, list) and len(wal) > 0, "missing native WAL")
    heap_records, index_records, commits = [], [], []
    previous_end = 0
    for record in sorted(wal, key=lambda r: integer(mapping(r).get("start"))):
        record = mapping(record)
        start, end = integer(record.get("start")), integer(record.get("end"))
        require(start > 0 and start >= previous_end and end > start, "duplicate or overlapping WAL")
        previous_end = end
        require(integer(record.get("xid")) == xid, "WAL xid mismatch")
        rm, kind = record.get("rm"), record.get("type")
        refs = record.get("block_ref")
        # pg_walinspect returns SQL NULL for records without block references.
        require("block_ref" in record and (refs is None or isinstance(refs, str)),
                "missing original WAL block references")
        blocks = re.findall(r"blkref #[0-9]+: rel ([0-9]+/[0-9]+/[0-9]+) fork (\w+) blk ([0-9]+)\b", refs or "")
        if rm == "Heap" and kind in ("INSERT", "INSERT+INIT"):
            flags = re.search(r"\bflags: 0x([0-9A-Fa-f]{2})\b", record.get("description", ""))
            require(flags is not None and int(flags[1], 16) & 1, "heap WAL omitted AV clear")
            require((locator, "main", "0") in blocks and (locator, "vm", "0") in blocks,
                    "heap WAL must bind the exact heap and VM together")
            heap_records.append(record)
        elif rm == "Btree" and kind == "INSERT_LEAF":
            require(any(rel == index and fork == "main" for rel, fork, block in blocks), "wrong index WAL")
            index_records.append(record)
        elif rm == "Transaction" and kind == "COMMIT":
            require(refs is None or refs == "", "unexpected COMMIT block reference")
            commits.append(record)
    require(len(heap_records) == len(index_records) == len(commits) == 1,
            "one original heap clear, index insert and COMMIT required")
    h, i, c = heap_records[0], index_records[0], commits[0]
    require(integer(h["end"]) <= integer(i["start"]) and integer(i["end"]) <= integer(c["start"]),
            "heap clear WAL must precede index WAL and COMMIT")
    require(e.get("uncommitted_count") == "0", "uncommitted row became visible")
    peers = e.get("peers")
    require(isinstance(peers, list) and len(peers) == 4, "all four members required")
    require(all(type(mapping(p).get("node")) is int for p in peers)
            and sorted(p["node"] for p in peers) == [0, 1, 2, 3], "duplicate or missing member")
    for peer in peers:
        require(peer.get("rows") == "42|7", "wrong committed index result")
        require(isinstance(peer.get("plan"), str) and re.search(
            r"^Index Scan using k_vm_native_pkey on k_vm_native(?:\s|$)", peer["plan"]), "ordinary index scan required")
    require(e.get("normal_stop") is True, "normal stop not proven")
    return {"status": "NATIVE_CHAIN_OBSERVED", "heap_wal_lsn": h["start"],
            "index_wal_lsn": i["start"], "commit_lsn": c["start"], "vm_deltas": delta}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path)
    args = parser.parse_args()
    try:
        result = source_precheck(args.source_root) if args.source_root else evaluate(json.load(sys.stdin))
    except (ValueError, TypeError, OSError, KeyError) as error:
        result = {"status": "REFUSED", "reason": str(error)}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in ("NATIVE_CHAIN_OBSERVED", "SOURCE_ENTRIES_PRESENT") else 1


if __name__ == "__main__":
    sys.exit(main())
