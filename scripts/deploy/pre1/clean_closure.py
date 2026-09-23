"""Conjoin native shutdown facts without granting automatic restart permission.

Author: SqlRush <sqlrush@gmail.com>

This pure classifier consumes controller-bound observations. Callers must capture
the log cursor and voting slots before dispatching all four exact fast stops,
then collect fresh logs, native controls, processes and strict direct-I/O slots.
A saved result never substitutes for the next operation's physical recheck.
"""
import re

from lifecycle import reconcile

PROTOCOL_MARKER = ("cluster normal-stop: protocol closed after shutdown checkpoint "
                   "and WAL STOPPED; auxiliary exit and voting clear still pending")
DEBT_KEYS = tuple("pcm." + key for key in (
    "pcm_grd_wait_refcount", "pcm_grd_transport_refcount", "resource_x_retained_debt_count",
    "resource_x_active_debt_count", "resource_x_local_owner_debt_count",
    "resource_x_evicting_debt_count", "resource_x_invalid_debt_count")) + ("gcs.outstanding_count",)


def uint(value):
    return type(value) is int and 0 <= value <= 18446744073709551615


def keyed(items, key, expected):
    if (type(items) is not list or len(items) != len(expected)
            or any(type(item) is not dict or type(item.get(key)) is not int for item in items)
            or {item[key] for item in items} != set(expected)):
        raise ValueError("incomplete or duplicate evidence set")
    return {item[key]: item for item in items}


def protocol_closed(nodes, binary_sha256, starts, logs):
    started = keyed([dict(record, node_id=record["node"]["node_id"]) for record in starts],
                    "node_id", range(4))
    tails = keyed(logs, "node_id", range(4))
    for node in nodes:
        n = node["node_id"]
        start, log = started[n], tails[n]
        proc = start["process_identity"]
        if (start["node"] != node or proc["exe_sha256"] != binary_sha256
                or any(not uint(proc[key]) or proc[key] == 0 for key in ("pid", "starttime"))
                or log["boot_id"] != node["boot_id"] or log["process_identity"] != proc
                or not uint(start["incarnation"]) or not start["incarnation"]):
            return False
        before, after = log["before"], log["after"]
        if (any(not uint(record[key]) for record in (before, after) for key in ("dev", "ino", "size"))
                or (before["dev"], before["ino"]) != (after["dev"], after["ino"])
                or after["size"] <= before["size"] or type(log["raw"]) is not str
                or len(log["raw"].encode()) != after["size"] - before["size"]
                or PROTOCOL_MARKER not in log["raw"]
                or re.search(r"\b(?:ERROR|FATAL|PANIC):", log["raw"])):
            return False
    return True


def slots_cleared(starts, before_votes, after_votes):
    before = keyed(before_votes, "index", range(3))
    after = keyed(after_votes, "index", range(3))
    incarnations = {record["node"]["node_id"]: record["incarnation"] for record in starts}
    if len(incarnations) != 4 or any(not uint(v) or not v for v in incarnations.values()):
        return False
    if len({before[d]["wwid"] for d in range(3)}) != 3:
        return False
    for disk in range(3):
        old, new = before[disk], after[disk]
        for record in (old, new):
            if (record["status"] != "OBSERVED" or record["direct"] is not True
                    or record["read_only"] is not True or record["strict_authority"] is not True
                    or record["deployment_qualified"] is not False or record["read_bytes"] != 525824
                    or record["logical_sector"] != 512):
                return False
        if any(new[key] != old[key] for key in ("wwid", "major", "minor", "capacity", "logical_sector")):
            return False
        old_slots = keyed(old["members"], "node_id", range(128))
        new_slots = keyed(new["members"], "node_id", range(128))
        for n in range(128):
            a, b = old_slots[n], new_slots[n]
            if (a["valid"] is not True or b["valid"] is not True
                    or any(not uint(slot[key]) for slot in (a, b)
                           for key in ("incarnation", "flags", "generation", "epoch"))
                    or b["flags"] != 0):
                return False
            if n < 4 and (a["incarnation"] != incarnations[n] or b["incarnation"] != incarnations[n]
                          or not a["flags"] & 1 or b["generation"] <= a["generation"]
                          or b["epoch"] < a["epoch"]):
                return False
            if n >= 4 and a["flags"] != 0:
                return False
    return True


def verify_closure(nodes, binary_sha256, observations, system_identifier,
                   starts, logs, before_votes, after_votes, terminal_states, *, post_stop_not_before_ns):
    result = reconcile(nodes, binary_sha256, observations, system_identifier)
    result.update(kind="pre1-clean-stop-observation", protocol_closed=False,
                  persistent_closure=False, clean_stop_proven=False, restart_allowed=False)
    # All timestamps here belong to this controller, never to a guest clock.
    # The caller establishes this lower bound after all stop commands finish.
    # Prior clean controls/process censuses on the same VM boot cannot witness
    # the current stop, even when their node and database identities still match.
    if (not uint(post_stop_not_before_ns) or post_stop_not_before_ns == 0
            or any(record["begin_monotonic_ns"] < post_stop_not_before_ns
                   for record in result["observations"])):
        result.update(status="ERROR", state="OBSERVATION_INCOMPLETE", data_clean=False,
                      process_gone=False, closure_evidence_error="PRE_STOP_OBSERVATION")
        return result
    result["post_stop_not_before_ns"] = post_stop_not_before_ns
    try:
        result["protocol_closed"] = protocol_closed(nodes, binary_sha256, starts, logs)
        debt_free = (type(terminal_states) is list and len(terminal_states) == 4
                     and all(state[key] == "0" for state in terminal_states for key in DEBT_KEYS))
        result["persistent_closure"] = debt_free and slots_cleared(starts, before_votes, after_votes)
    except (KeyError, TypeError, ValueError, UnicodeError):
        result["closure_evidence_error"] = "CLOSURE_EVIDENCE_INCOMPLETE"
    complete = (result["status"] == "PASS" and result["data_clean"] and result["process_gone"]
                and result["protocol_closed"] and result["persistent_closure"])
    result["clean_stop_proven"] = complete
    if complete:
        result.update(state="CLEAN_STOPPED", pending=[])
    elif result["data_clean"] and result["process_gone"]:
        result.update(state="DATA_CLEAN_CLOSURE_INCOMPLETE",
                      pending=[key for key, field in (("PROTOCOL_CLOSED", "protocol_closed"),
                                                     ("PERSISTENT_CLOSURE", "persistent_closure"))
                               if not result[field]])
    return result
