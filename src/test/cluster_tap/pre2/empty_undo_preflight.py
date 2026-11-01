"""Join native empty-undo preflight and original terminal-cleanup evidence.

Author: SqlRush <sqlrush@gmail.com>
"""
import json
import sys

from undo_slot_wait import UINT64_MAX, decimal, idle, integer, require


WRITES = ("zero_update", "empty_copy", "conflict_nothing", "zero_row_lock")
CONTROLS = ("pure_select", "explain_only")
TRIGGER = "statement_trigger_error"
COUNTERS = ("tt_slot_active_full_wait_count", "tt_capacity_pool_wait_count",
            "tt_capacity_cancel_count", "segment_switch_count", "autoextend_count",
            "segment_reuse_count", "record_alloc_count")


def counter_values(observation):
    sample = observation["counters"]
    require(sample["tt_capacity_stats_available"] == "true", "capacity counters unavailable")
    require(decimal(sample["tt_capacity_origin_node_id"], 0, 127) == 0,
            "counters belong to another origin")
    return {key: decimal(sample[key]) for key in COUNTERS}


def backend(observation, pid):
    require(type(observation) is dict, "missing backend observation")
    require(integer(observation["pid"], 1, (1 << 31) - 1) == pid,
            "observation belongs to another backend")


def unbound(observation, pid):
    backend(observation, pid)
    binding = observation["binding"]
    require(binding["available"] is False and binding["wait_scope"] is False
            and observation["top"] == "none", "backend retains a top, binding or wait scope")
    idle({"snapshot": observation["snapshot"]})
    require(observation["snapshot"]["origin"] == 0, "CURRENT belongs to another origin")


def current_identity(snapshot):
    return tuple(snapshot[key] for key in ("origin", "segment_id", "binding_generation"))


def bound(observation, pid, before):
    backend(observation, pid)
    binding, snapshot = observation["binding"], observation["snapshot"]
    require(binding["available"] is True and binding["wait_scope"] is False,
            "no published binding or a live wait scope")
    require(integer(binding["pid"], 1, (1 << 31) - 1) == pid, "binding names another backend")
    xid = integer(binding["xid"], 3, (1 << 32) - 1)
    require(decimal(observation["top"], 3) & ((1 << 32) - 1) == xid,
            "published raw XID does not match the native top FullXid")
    require(integer(binding["origin"], 0, 127) == 0 and
            integer(binding["segment_id"], 1, (1 << 32) - 1) == before["segment_id"],
            "binding changed origin or CURRENT")
    integer(binding["segment_generation"], 0, (1 << 32) - 2)
    offset = integer(binding["slot_offset"], 0, 47)
    wrap = integer(binding["slot_wrap"], 0, 65534)
    decimal(binding["active_lsn"], 1, UINT64_MAX - 1)
    integer(snapshot["origin"], 0, 127)
    integer(snapshot["segment_id"], 1, (1 << 32) - 1)
    require(snapshot["available"] is True and current_identity(snapshot) == current_identity(before),
            "CURRENT identity changed")
    slots = snapshot["slots"]
    require(type(slots) is list and len(slots) == 48, "not exactly 48 actual slots")
    original = {slot["offset"]: slot for slot in before["slots"]}
    seen = set()
    for slot in slots:
        index = integer(slot["offset"], 0, 47)
        require(index not in seen, "duplicate actual slot")
        seen.add(index)
        integer(slot["status"], 0, 3)
        integer(slot["xid"], 0, (1 << 32) - 1)
        integer(slot["wrap"], 0, 65534)
        decimal(slot["commit_scn"])
        expected = dict(original[index])
        if index == offset:
            expected.update(status=1, xid=xid, wrap=wrap, commit_scn="0")
        require(slot == expected, "not the exact one ACTIVE slot with 47 unchanged FREE slots")


def statement(observation, error, state):
    result = observation["result"]
    require(type(result["error"]) is int and result["error"] == error
            and result["sqlstate"] == state, "unexpected native statement result")


def evaluate(request):
    try:
        require(type(request) is dict, "request is not an object")
        kind, finish = request["kind"], request["finish"]
        require(kind in WRITES + CONTROLS + (TRIGGER,), "unknown SQL case")
        require(finish in ("commit", "abort") and (kind != TRIGGER or finish == "abort"),
                "invalid terminal operation")
        pid = integer(request["pid"], 1, (1 << 31) - 1)
        before, first, repeated, after = (request[key] for key in
                                           ("before", "first", "repeated", "after"))
        unbound(before, pid)
        unbound(after, pid)
        require(current_identity(after["snapshot"]) == current_identity(before["snapshot"]),
                "terminal cleanup changed CURRENT identity")
        baseline = counter_values(before)
        if kind in CONTROLS:
            for observation in (first, repeated):
                unbound(observation, pid)
                require(observation["snapshot"] == before["snapshot"],
                        "pure read changed actual slots")
                statement(observation, 0, "00000")
        else:
            bound(first, pid, before["snapshot"])
            statement(first, 1 if kind == TRIGGER else 0, "P0001" if kind == TRIGGER else "00000")
            if kind == TRIGGER:
                require(repeated is None, "trigger error was treated as repeatable successful work")
            else:
                bound(repeated, pid, before["snapshot"])
                statement(repeated, 0, "00000")
                require(all(repeated[key] == first[key] for key in ("top", "binding", "snapshot")),
                        "repeated preflight replaced its original binding or slot")
        for observation in (first, after) + (() if repeated is None else (repeated,)):
            require(counter_values(observation) == baseline,
                    "undo emission, waiting, cancellation or supply counters changed")
        require(request["data"] == ["data|1|100"] * 4, "four-node unchanged data ledger missing")
        return {"passed": True, "reason": "exact empty-undo binding and original terminal cleanup"}
    except (KeyError, TypeError, ValueError, IndexError) as error:
        return {"passed": False, "reason": str(error) or "malformed native evidence"}


if __name__ == "__main__":
    try:
        result = evaluate(json.load(sys.stdin))
    except (ValueError, TypeError) as error:
        result = {"passed": False, "reason": str(error)}
    print(json.dumps(result, sort_keys=True))
