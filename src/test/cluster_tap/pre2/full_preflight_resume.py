"""Validate full-slot preflight resume evidence without authorizing allocation.

Author: SqlRush <sqlrush@gmail.com>
"""
import json
import sys

from empty_undo_preflight import (TRIGGER, WRITES, backend, bound, counter_values,
                                  current_identity, statement)
from undo_slot_wait import blocked, decimal, full, integer, require


def evaluate(request):
    try:
        require(type(request) is dict, "request is not an object")
        kind, mode = request["kind"], request["mode"]
        require(kind in WRITES + (TRIGGER,) and mode in ("commit", "abort"),
                "unknown caller or owner terminal operation")
        waiting = request["blocked"]
        selected = blocked(waiting)
        before = waiting["before"]
        original = next(row for row in before["bindings"] if row["pid"] == selected)
        pid = waiting["waiter_pid"]
        first, repeated, after = (request[key] for key in ("first", "repeated", "after"))
        bound(first, pid, before["snapshot"])
        binding = first["binding"]
        require(binding["xid"] == waiting["waiter_xid"] and
                binding["slot_offset"] == original["slot_offset"] and
                binding["slot_wrap"] != original["slot_wrap"] and
                binding["segment_generation"] == original["segment_generation"],
                "waiter did not acquire the exact retired slot")
        remaining = request["remaining"]
        require(remaining == [row for row in before["bindings"] if row["pid"] != selected],
                "remaining47 actual bindings changed")
        full({"snapshot": first["snapshot"], "bindings": remaining + [binding]})
        statement(first, 1 if kind == TRIGGER else 0, "P0001" if kind == TRIGGER else "00000")
        if kind == TRIGGER:
            require(repeated is None, "trigger ERROR must not repeat")
        else:
            bound(repeated, pid, before["snapshot"])
            statement(repeated, 0, "00000")
            require(all(repeated[key] == first[key] for key in ("top", "binding", "snapshot")),
                    "repeated preflight replaced its original binding")
        backend(after, pid)
        require(after["top"] == "none" and after["binding"]["available"] is False and
                after["binding"]["wait_scope"] is False, "waiter retains top, binding or scope")
        snapshot = after["snapshot"]
        integer(snapshot["origin"], 0, 127)
        integer(snapshot["segment_id"], 1, (1 << 32) - 1)
        decimal(snapshot["binding_generation"], 1, (1 << 64) - 2)
        require(snapshot["available"] is True and
                current_identity(snapshot) == current_identity(before["snapshot"]),
                "retirement changed CURRENT")
        require(type(snapshot["slots"]) is list and len(snapshot["slots"]) == 48,
                "retirement snapshot is not48 slots")
        old_slots = {slot["offset"]: slot for slot in before["snapshot"]["slots"]}
        seen = set()
        for slot in snapshot["slots"]:
            offset = integer(slot["offset"], 0, 47)
            require(offset not in seen, "duplicate retired slot")
            seen.add(offset)
            integer(slot["status"], 0, 3)
            integer(slot["xid"], 0, (1 << 32) - 1)
            integer(slot["wrap"], 0, 65534)
            decimal(slot["commit_scn"])
            if offset == original["slot_offset"]:
                require(slot["status"] == 0 and slot["xid"] == 0 and slot["commit_scn"] == "0",
                        "resumed waiter slot has not actually become FREE")
            else:
                require(slot == old_slots[offset], "remaining47 actual slots changed")
        baseline = counter_values({"counters": waiting["counters_after"]})
        resumed = counter_values(first)
        # Native owner completion may precede terminal release/GC. The original
        # selected-segment CV can run before this first successful binding.
        pool_wait = "tt_capacity_pool_wait_count"
        require(resumed[pool_wait] >= baseline[pool_wait] and
                all(resumed[key] == baseline[key] for key in baseline if key != pool_wait),
                "first resume changed a forbidden counter or lost terminal CV counts")
        for observation in (after,) + (() if repeated is None else (repeated,)):
            require(counter_values(observation) == resumed,
                    "resume emitted undo or changed wait, cancellation or supply counters")
        require(request["data"] == ["data|1|100"] * 4, "four-peer unchanged data ledger missing")
        return {"passed": True, "reason": "exact waited slot, repeated receipt and original retirement"}
    except (KeyError, TypeError, ValueError, IndexError, StopIteration) as error:
        return {"passed": False, "reason": str(error) or "malformed resume evidence"}


if __name__ == "__main__":
    try:
        result = evaluate(json.load(sys.stdin))
    except (ValueError, TypeError) as error:
        result = {"passed": False, "reason": str(error)}
    print(json.dumps(result, sort_keys=True))
