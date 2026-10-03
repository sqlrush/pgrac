"""Strict checks over real product observations; no product actions here."""
import json
import sys


def require(value, message):
    if not value:
        raise ValueError(message)


def check_shutdown(receipt, sysid, writers):
    require(writers and len({w["node"] for w in writers}) == len(writers), "empty or duplicate closing set")
    require(receipt["system_identifier"] == sysid, "shutdown system identity changed")
    require(receipt["final_checkpoint"] is True, "final checkpoint is not durable")
    require(receipt["all_processes_exited"] is True, "a cluster process survived shutdown")
    actual = receipt["writers"]
    require(len(actual) == len(writers), "incomplete CLOSED writer set")
    for before in writers:
        matches = [w for w in actual if w.get("node") == before["node"]]
        require(len(matches) == 1, "duplicate or missing writer")
        require(matches[0].get("state") == "CLOSED", "writer is not CLOSED")
        for field in ("thread", "incarnation", "wal_generation", "boot"):
            require(matches[0].get(field) == before[field], "CLOSED identity differs: " + field)


def check_recovery(trace, duty, token, terminal_proof, recoverers, victim, identities):
    require(len(set(recoverers)) == 2 and victim not in recoverers, "not two survivors")
    for field, expected in (("duty", duty), ("token", token),
                            ("terminal_proof", terminal_proof),
                            ("recoverers", recoverers), ("victim", victim), ("identities", identities)):
        require(trace[field] == expected, "wrong recovery " + field)
    require(all(isinstance(s, str) and s for s in (duty, token, terminal_proof)),
            "missing exact recovery identity")
    require(trace["trace_complete"] is True and trace["overflow"] is False,
            "incomplete recovery journal")
    held, wrote, released_ir, retired = set(), set(), set(), set()
    complete, deferred, rejected, interleaved, closed = False, False, False, False, False
    x_holder = None
    first, second = recoverers
    boots = {w["node"]: w["boot"] for w in identities["recoverer_writers"]}
    require(set(boots) == set(recoverers), "missing exact survivor boot identities")
    for seq, event in enumerate(trace["events"], 1):
        require(not closed, "events after closed observation")
        require(event["seq"] == seq, "gap/reorder in causal observation journal")
        require(event["duty"] == duty and event["token"] == token, "mixed recovery window")
        kind, node = event["kind"], event["node"]
        require(node in recoverers, "not a surviving recoverer")
        require(event["boot"] == boots[node], "event belongs to a different survivor boot")
        if kind == "walr_s_acquired":
            require(not complete and x_holder is None and node not in held,
                    "incompatible WALR-S acquisition")
            held.add(node)
        elif kind == "data_write":
            require(not complete, "DATA write after RECOVERY_COMPLETE")
            require(node in held and node not in released_ir, "write outside WALR-S/IR")
            wrote.add(node)
            interleaved |= node == second and first in released_ir
        elif kind == "ir_released":
            require(node in held and node in wrote and node not in released_ir,
                    "unconfirmed or duplicate IR release")
            released_ir.add(node)
        elif kind == "completion_deferred":
            require(node == first and released_ir == {first} and held == set(recoverers)
                    and not complete, "not an actual conflicting finalizer")
            deferred = True
        elif kind == "walr_s_released":
            require(node in held and node in released_ir and x_holder != node,
                    "WALR release before DATA/IR retirement")
            held.remove(node)
        elif kind == "complete_x_acquired":
            require(node == first and held == {first} and released_ir == set(recoverers)
                    and deferred and interleaved and x_holder is None and not complete,
                    "completion X overlaps another WALR-S executor")
            x_holder = node
        elif kind == "recovery_complete":
            require(x_holder == node and not complete and event["durable_readback"] is True,
                    "completion lacks exclusive durable proof")
            complete = True
        elif kind == "complete_x_released":
            require(complete and x_holder == node, "unmatched completion X release")
            x_holder = None
        elif kind == "stale_write_rejected":
            require(complete and node == second and event["reason"] == "RECOVERY_COMPLETE",
                    "old write was not rejected by the closed window")
            rejected = True
        elif kind == "worker_retired":
            require(complete and node not in held and node not in retired,
                    "recoverer still owns write permission")
            retired.add(node)
        elif kind == "observation_closed":
            require(complete and rejected and retired == set(recoverers) and not held
                    and x_holder is None, "observation closed with a live executor")
            closed = True
        else:
            raise ValueError("unknown recovery event: " + kind)
    require(closed and wrote == set(recoverers), "missing final recovery evidence")


def check_scache(before, first, repeat, target, holder, reader):
    require(holder != reader, "read is local")
    require(target.get("fork") == "main" and target.get("block") == 0
            and target.get("space") and target.get("path"), "incomplete target identity")
    for sample in (before, first, repeat):
        require(sample["target"] == target, "observation for another block/incarnation")
        require(type(sample["ships"]) is int and sample["ships"] >= 0, "invalid ship count")
    require(before["x_holder"] == holder and not before["s_holders"], "no initial remote X")
    for sample in (first, repeat):
        require(sample["x_holder"] is None and len(sample["s_holders"]) == 2
                and set(sample["s_holders"]) == {holder, reader}, "one-shot is not durable S")
    require(first["ships"] > before["ships"], "no real cross-node block delivery")
    require(repeat["ships"] == first["ships"], "repeated read shipped another image")


if __name__ == "__main__":
    request = json.load(sys.stdin)
    checks = {"shutdown": check_shutdown, "recovery": check_recovery, "scache": check_scache}
    checks[request["check"]](*request["args"])
    print(json.dumps({"checked": request["check"]}))
