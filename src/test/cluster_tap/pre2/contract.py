"""Assertions over observations collected at real product barriers.

The adapter must collect these at synchronized product boundaries, never sort
wall-clock timestamps from different hosts to invent a causal order.
"""
import re

if not __debug__:
    raise RuntimeError("acceptance requires enabled Python assertions")


def check_wal(events, require_release=False):
    contributions, receipts, eligible, proofs, retired, released = {}, {}, {}, {}, set(), set()
    assert events, "no WAL observations"
    for event in events:
        kind = event["kind"]
        if kind == "contribution":
            key = event["id"]
            assert key not in contributions, "duplicate contribution"
            for field in ("tag", "space", "writer", "version", "wal"):
                assert isinstance(event[field], str) and event[field], field
            assert event["wal"] not in released, "contribution uses an already released WAL identity"
            contributions[key] = event
        elif kind == "data":
            assert event["id"] not in receipts, "duplicate receipt"
            assert event["fsync_completed"] is True, "DATA not durable"
            assert re.fullmatch(r"[0-9a-f]{64}", event["sha256"]), "DATA digest missing"
            assert event["sha256"] == event["readback_sha256"], "DATA readback differs"
            assert event["version"], "DATA version missing"
            ancestry = {(a["writer"], a["version"]) for a in event["ancestors"]}
            assert len(ancestry) == len(event["ancestors"]), "duplicate DATA ancestry"
            receipts[event["id"]] = event
            eligible[event["id"]] = {
                key for key, before in contributions.items()
                if (before["writer"], before["version"]) in ancestry
            }
        elif kind == "coverage":
            assert event["id"] not in proofs and event["receipt"] in receipts, "coverage before DATA"
            data = receipts[event["receipt"]]
            ids = event["contributions"]
            assert ids and len(set(ids)) == len(ids), "empty or duplicate coverage"
            assert len(ids) == len(event["versions"]) == len(event["writers"]), "incomplete ancestry"
            for key, version, writer in zip(ids, event["versions"], event["writers"]):
                assert key in contributions, "coverage of unseen contribution"
                assert key in eligible[event["receipt"]], "old DATA cannot cover later or unrelated contribution"
                before = contributions[key]
                assert before["tag"] == data["tag"] and before["space"] == data["space"], "wrong object"
                assert before["version"] == version and before["writer"] == writer, "wrong ancestry"
            proofs[event["id"]] = event
        elif kind == "retire":
            proof = proofs.get(event["proof"])
            assert proof and event["contribution"] in proof["contributions"], "retired without exact proof"
            assert event["contribution"] not in retired, "duplicate retirement"
            retired.add(event["contribution"])
        elif kind == "wal_release":
            affected = {key for key, value in contributions.items() if value["wal"] == event["wal"]}
            assert affected and affected <= retired, "WAL released with uncovered contribution"
            assert event["wal"] not in released, "duplicate release"
            released.add(event["wal"])
        else:
            raise AssertionError("unknown WAL observation: " + str(kind))
    assert contributions, "no contribution was observed"
    assert not require_release or released, "WAL release boundary was not exercised"
    return {value["wal"] for key, value in contributions.items() if key not in retired}


def check_reset(events, duty, survivors):
    recovered, opened, resets = False, False, {}
    assert survivors, "no survivors"
    for event in events:
        assert event["duty"] == duty, "wrong recovery duty"
        if event["kind"] == "catalog_recovered":
            assert not recovered and not opened, "duplicate or late recovery"
            recovered = True
        elif event["kind"] == "reset_all":
            assert recovered and not opened, "RESET outside recovery/open boundary"
            node = event["node"]
            assert node in survivors and event["boot"] == survivors[node], "stale survivor"
            assert node not in resets and event["barrier"], "duplicate or unbound RESET"
            resets[node] = event["barrier"]
        elif event["kind"] == "open":
            assert recovered and not opened, "open before catalog recovery"
            assert set(resets) == set(survivors), "open before all survivor RESETs"
            assert all(barrier == event["barrier"] for barrier in resets.values()), "wrong RESET barrier"
            opened = True
        else:
            raise AssertionError("unknown recovery observation")
    assert opened, "recovery never opened"


def check_membership(events, leave, failure, drain_ms, recovery_ms):
    expected = ["reserve", "drain", "peer_failed", "abort", "abort_closed",
                "fail_stop_reserve", "fail_stop_complete"]
    assert [e["kind"] for e in events] == expected, "missing or reordered member transition"
    assert drain_ms > 0 and recovery_ms > 0, "missing existing product deadlines"
    times = [e["at_ms"] for e in events]
    assert all(isinstance(t, (int, float)) and t >= 0 for t in times), "invalid elapsed time"
    assert times == sorted(times), "non-monotone local observation times"
    assert all(e["request"] == leave for e in events[:5]), "LEAVE identity changed"
    assert all(e["request"] == failure for e in events[5:]), "FAIL_STOP identity changed"
    assert leave != failure, "different operations share a key"
    assert times[4] - times[0] <= drain_ms, "LEAVE exceeded its existing bounded deadline"
    assert times[6] - times[2] <= recovery_ms, "planned operation delayed failure recovery"
