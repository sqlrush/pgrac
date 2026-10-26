"""Validate native slot wait evidence without supplying allocator state.

Author: SqlRush <sqlrush@gmail.com>
"""
import json
import re
import sys

UINT64_MAX = (1 << 64) - 1
STABLE_COUNTERS = (
    "tt_capacity_pool_wait_count", "tt_capacity_cancel_count",
    "segment_switch_count", "autoextend_count", "segment_reuse_count",
    "record_alloc_count",
)
WAIT_COUNTER = "tt_slot_active_full_wait_count"


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def integer(value, low, high):
    require(type(value) is int and low <= value <= high, "invalid integer field")
    return value


def decimal(value, low=0, high=UINT64_MAX):
    # Native uint64 values stay strings across the Perl/JSON bridge.
    require(type(value) is str and re.fullmatch(r"0|[1-9][0-9]*", value),
            "missing or nondecimal text field")
    return integer(int(value), low, high)


def idle(request):
    snapshot = request["snapshot"]
    require(snapshot["available"] is True, "allocator census unavailable")
    integer(snapshot["origin"], 0, 127)
    integer(snapshot["segment_id"], 1, (1 << 32) - 1)
    decimal(snapshot["binding_generation"], 1, UINT64_MAX - 1)
    slots = snapshot["slots"]
    require(type(slots) is list and len(slots) == 48, "not exactly 48 slots")
    offsets = set()
    for slot in slots:
        offset = integer(slot["offset"], 0, 47)
        require(offset not in offsets, "duplicate slot")
        offsets.add(offset)
        require(type(slot["status"]) is int and slot["status"] == 0,
                "preparatory terminal slot has not been cleared")
        integer(slot["xid"], 0, 0)
        integer(slot["wrap"], 0, 65534)
        require(decimal(slot["commit_scn"]) == 0, "FREE slot has terminal SCN")


def full(request):
    snapshot, bindings = request["snapshot"], request["bindings"]
    require(snapshot["available"] is True, "allocator census unavailable")
    origin = integer(snapshot["origin"], 0, 127)
    segment = integer(snapshot["segment_id"], 1, (1 << 32) - 1)
    decimal(snapshot["binding_generation"], 1, UINT64_MAX - 1)
    slots = snapshot["slots"]
    require(type(slots) is list and len(slots) == 48, "not exactly 48 slots")
    require(type(bindings) is list and len(bindings) == 48, "not 48 published owners")
    by_slot, xids = {}, set()
    for slot in slots:
        offset = integer(slot["offset"], 0, 47)
        xid = integer(slot["xid"], 3, (1 << 32) - 1)
        integer(slot["wrap"], 0, 65534)
        require(type(slot["status"]) is int and slot["status"] == 1,
                "slot is not ACTIVE")
        require(decimal(slot["commit_scn"]) == 0, "ACTIVE slot has terminal SCN")
        require(offset not in by_slot and xid not in xids, "duplicate slot or XID")
        by_slot[offset] = slot
        xids.add(xid)
    seen, pids, generations = {}, set(), set()
    for binding in bindings:
        require(binding["available"] is True and binding["wait_scope"] is False,
                "owner has no published binding or remains in a wait scope")
        pid = integer(binding["pid"], 1, (1 << 31) - 1)
        offset = integer(binding["slot_offset"], 0, 47)
        require(pid not in pids and offset not in seen, "duplicate owner or binding")
        require(integer(binding["origin"], 0, 127) == origin and
                integer(binding["segment_id"], 1, (1 << 32) - 1) == segment,
                "binding names a different origin or segment")
        generations.add(integer(binding["segment_generation"], 0, (1 << 32) - 2))
        require(integer(binding["xid"], 3, (1 << 32) - 1) == by_slot[offset]["xid"] and
                integer(binding["slot_wrap"], 0, 65534) == by_slot[offset]["wrap"],
                "binding does not name the actual slot owner")
        decimal(binding["active_lsn"], 1)
        seen[offset] = binding
        pids.add(pid)
    require(len(generations) == 1, "one physical segment has inconsistent generations")
    return seen[0]["pid"]


def counters(sample, origin):
    require(sample["tt_capacity_stats_available"] == "true", "capacity counters unavailable")
    require(decimal(sample["tt_capacity_origin_node_id"], 0, 127) == origin,
            "counters belong to another origin")
    return {key: decimal(sample[key]) for key in (WAIT_COUNTER,) + STABLE_COUNTERS}


def blocked(request):
    before, after = request["before"], request["after"]
    selected = full(before)
    require(full(after) == selected and before == after,
            "allocator or canonical binding changed while blocked")
    pid = integer(request["waiter_pid"], 1, (1 << 31) - 1)
    xid = integer(request["waiter_xid"], 3, (1 << 32) - 1)
    require(all(b["pid"] != pid and b["xid"] != xid for b in before["bindings"]),
            "waiter already owns an ACTIVE binding")
    activity = request["activity"]
    require(integer(activity["pid"], 1, (1 << 31) - 1) == pid and
            decimal(activity["xid"], 3, (1 << 32) - 1) == xid and
            activity["wait_event"] == "ClusterUndoTxSlotWait" and
            activity["wait_event_type"] == "Cluster: Undo" and
            activity["blockers"] == [selected], "not the exact native owner wait")
    locks = request["locks"]
    require(type(locks) is list, "missing native lock rows")
    require(all(type(row) is dict for row in locks), "native lock row is not an object")
    waiting = [row for row in locks if row.get("pid") == pid and
               row.get("locktype") == "virtualxid" and row.get("mode") == "ShareLock" and
               row.get("granted") is False]
    require(len(waiting) == 1, "not one native ungranted VXID ShareLock")
    tag = waiting[0]["virtualxid"]
    require(type(tag) is str and re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", tag),
            "invalid native VXID")
    owners = [row for row in locks if row.get("pid") == selected and
              row.get("locktype") == "virtualxid" and row.get("mode") == "ExclusiveLock" and
              row.get("granted") is True and row.get("virtualxid") == tag]
    require(len(owners) == 1, "missing or ambiguous exact native VXID owner")
    origin = before["snapshot"]["origin"]
    old = counters(request["counters_before"], origin)
    new = counters(request["counters_after"], origin)
    require(new[WAIT_COUNTER] > old[WAIT_COUNTER], "ACTIVE wait counter did not advance")
    require(all(new[key] == old[key] for key in STABLE_COUNTERS),
            "wait changed supply, allocation, or cancellation counters")
    return selected


def evaluate(request):
    try:
        require(type(request) is dict, "request is not an object")
        stage = request["stage"]
        require(stage in ("idle", "full", "blocked"), "unknown observation stage")
        if stage == "idle":
            idle(request)
            return {"passed": True, "reason": "all 48 actual CURRENT slots are FREE"}
        selected = full(request) if stage == "full" else blocked(request)
        return {"passed": True, "reason": "exact native evidence", "selected_pid": selected}
    except (KeyError, TypeError, ValueError, IndexError) as error:
        return {"passed": False, "reason": str(error) or "malformed native evidence"}


if __name__ == "__main__":
    try:
        result = evaluate(json.load(sys.stdin))
    except (ValueError, TypeError) as error:
        result = {"passed": False, "reason": str(error)}
    print(json.dumps(result, sort_keys=True))
