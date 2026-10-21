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


WORKLOAD_BARRIER_ORDER = ("IDENTITY_SEAL", "BASELINE", "R/IMG", "I/TT",
                          "H/HOT", "Q/RECEIPT", "STOP_SEAL")
WORKLOAD_LANES = ("R/IMG", "I/TT", "H/HOT", "Q/RECEIPT")
WORKLOAD_LANE_DOMAINS = {"R/IMG": "r", "I/TT": "i", "H/HOT": "h", "Q/RECEIPT": "q"}
PASS_FOCUSED_DIAGNOSTIC = "PASS_FOCUSED_DIAGNOSTIC"
NOT_TRIGGERED = "NOT_TRIGGERED"
ROOT_OPEN = "ROOT_OPEN"


def _workload_identity_key(value):
    return tuple(value[field] for field in
                 ("node", "pid", "procno", "sequence_domain"))


def _workload_require_identity(value):
    assert isinstance(value, dict), "identity is not an object"
    for field in ("node", "sequence_domain"):
        assert isinstance(value.get(field), str) and value[field], field
    for field in ("pid", "procno"):
        assert isinstance(value.get(field), int) and value[field] >= 0, field
    return _workload_identity_key(value)


def _workload_require_snapshots(snapshots, identities, seq, gaps):
    assert isinstance(snapshots, list) and snapshots, "missing barrier snapshots"
    expected = set(identities)
    actual = set()
    for snapshot in snapshots:
        key = _workload_require_identity(snapshot)
        assert key not in actual, "duplicate barrier identity"
        actual.add(key)
        assert key in expected, "barrier identity was not sealed"
        assert isinstance(snapshot.get("last_seq"), int) and snapshot["last_seq"] >= 0
        assert isinstance(snapshot.get("gap"), bool), "barrier gap flag missing"
        assert snapshot["last_seq"] == seq[key], "barrier last sequence is not a prefix"
        assert snapshot["gap"] is gaps[key], "barrier gap flag was not preserved"
    assert actual == expected, "barrier does not cover every process/domain"


def _workload_require_locator(value):
    assert isinstance(value, dict), "relation locator is not an object"
    fields = ("spcOid", "dbOid", "relNumber", "forkNum", "blockNum")
    for field in fields:
        assert isinstance(value.get(field), int) and value[field] >= 0, field
    return tuple(value[field] for field in fields)


def check_workload_contract(records):
    """Check one deterministic R/IMG, I/TT, H/HOT and Q/RECEIPT capture.

    The input is the raw append order from the existing product log files.  A
    barrier is a prefix seal for each independent process/sequence domain;
    sequence numbers are never compared between domains and deferred records
    after a lane barrier cannot retroactively satisfy that lane.
    """
    assert isinstance(records, list) and records, "no workload observations"
    identities = []
    identity_set = set()
    seq = {}
    gaps = {}
    lane_events = {lane: [] for lane in WORKLOAD_LANES}
    all_events = []
    relation_sets = {lane: set() for lane in WORKLOAD_LANES}
    key_sets = {lane: set() for lane in WORKLOAD_LANES}
    barriers = []
    barrier_positions = {}
    receipt_keys = set()
    q_nonempty_state = False
    q_cleanup = False
    stop_state = None
    stopped = False

    for position, record in enumerate(records):
        assert isinstance(record, dict), "observation is not an object"
        kind = record.get("kind")
        if stopped:
            raise AssertionError("STOP_SEAL is final")
        if kind == "barrier":
            name = record.get("name")
            assert len(barriers) < len(WORKLOAD_BARRIER_ORDER), "duplicate barrier"
            assert name == WORKLOAD_BARRIER_ORDER[len(barriers)], "barrier order changed"
            if name == "IDENTITY_SEAL":
                raw_identities = record.get("identities")
                assert isinstance(raw_identities, list) and raw_identities, "identity seal missing"
                for identity in raw_identities:
                    key = _workload_require_identity(identity)
                    assert key not in identity_set, "duplicate process/domain identity"
                    identity_set.add(key)
                    identities.append(key)
                    seq[key] = 0
                    gaps[key] = False
                for field in ("source_sha", "build_sha"):
                    assert isinstance(record.get(field), str) and record[field], field
                for field in ("binary_sha256", "config_sha256"):
                    assert re.fullmatch(r"[0-9a-f]{64}", record.get(field, "")), field
            else:
                assert identities, "barrier precedes identity seal"
            _workload_require_snapshots(record.get("snapshots"), identities, seq, gaps)
            if name == "I/TT":
                assert record.get("owner") == "E", "I/TT barrier is not owned by E"
            barriers.append(name)
            barrier_positions[name] = position
            if name == "STOP_SEAL":
                stop_state = record.get("stop_state")
                assert isinstance(stop_state, dict), "STOP_SEAL state missing"
                for field in ("pending", "receipt", "grd", "pcm", "tt"):
                    assert isinstance(stop_state.get(field), int) and stop_state[field] >= 0, field
                stopped = True
            continue

        assert kind == "event", "unknown workload observation"
        assert identities and "BASELINE" in barrier_positions, "event before baseline"
        lane = record.get("lane")
        assert lane in WORKLOAD_LANES, "unknown workload lane"
        event_id = record.get("event_id")
        assert isinstance(event_id, str) and event_id and event_id not in {e["event_id"] for e in all_events}
        for field in ("phase", "node", "sequence_domain", "logical_key"):
            assert isinstance(record.get(field), str) and record[field], field
        for field in ("pid", "procno", "event_seq"):
            assert isinstance(record.get(field), int) and record[field] >= 0, field
        identity = (record["node"], record["pid"], record["procno"], record["sequence_domain"])
        assert identity in identity_set, "event identity was not sealed"
        locator = _workload_require_locator(record.get("relation_locator"))
        key = record["logical_key"]
        deferred = record.get("deferred", False)
        assert isinstance(deferred, bool), "deferred marker is not boolean"
        assert isinstance(record.get("actual"), bool) and record["actual"] is True, "event is not actual"
        assert record["sequence_domain"] == WORKLOAD_LANE_DOMAINS[lane], \
            "lane and sequence domain differ"
        assert lane not in barrier_positions or deferred, "late non-deferred lane event"
        assert deferred == (lane in barrier_positions), "deferred event is not late"
        previous = seq[identity]
        assert record["event_seq"] > previous, "duplicate or reordered event sequence"
        if record["event_seq"] != previous + 1:
            gaps[identity] = True
        seq[identity] = record["event_seq"]
        all_events.append(record)

        if lane == "I/TT":
            assert record.get("owner") == "E", "I/TT authority is not E"
        if lane == "Q/RECEIPT":
            receipt = record.get("receipt_key")
            assert isinstance(receipt, str) and receipt, "Q receipt key missing"
            receipt_keys.add(receipt)
            state = record.get("state")
            if state is not None:
                assert isinstance(state, dict), "Q state is not an object"
                for field in ("pending", "receipt", "grd", "pcm", "tt"):
                    assert isinstance(state.get(field), int) and state[field] >= 0, field
                    q_nonempty_state = q_nonempty_state or state[field] > 0
            if record["phase"] == "state_clear":
                assert record.get("cleanup_proof") == receipt, "Q state clear lacks receipt proof"
                q_cleanup = True

        relation_sets[lane].add(locator)
        key_sets[lane].add(key)
        if not deferred:
            lane_events[lane].append(record)

    assert barriers == list(WORKLOAD_BARRIER_ORDER), "workload barriers incomplete"
    assert len(receipt_keys) <= 1, "Q receipt identity changed"
    assert stop_state is not None, "STOP_SEAL missing"
    if q_nonempty_state:
        assert all(stop_state[field] == 0 for field in ("pending", "receipt", "grd", "pcm", "tt")) or not q_cleanup, \
            "STOP_SEAL state is inconsistent"
    for left_index, left in enumerate(WORKLOAD_LANES):
        for right in WORKLOAD_LANES[left_index + 1:]:
            assert not relation_sets[left] & relation_sets[right], "lanes share a relation locator"
            assert not key_sets[left] & key_sets[right], "lanes share a logical key"

    statuses = {}
    for lane in WORKLOAD_LANES:
        events = lane_events[lane]
        if not events:
            statuses[lane] = NOT_TRIGGERED
            continue
        phases = {event["phase"] for event in events}
        if lane == "R/IMG":
            complete = {"seed", "request", "serve", "reply", "consume"} <= phases
        elif lane == "I/TT":
            complete = {"admit", "publish", "reread"} <= phases
        elif lane == "H/HOT":
            paths = {"hot", "non_hot", "index_delete", "prune", "full"}
            complete = "update" in phases and any(
                event["phase"] in paths and event["actual"] for event in events
            )
        else:
            complete = {"mutation", "prepare", "publish", "apply_consume", "close", "ack",
                        "certificate_retire"} <= phases
        lane_domains = {event["sequence_domain"] for event in events}
        lane_has_gap = any(gaps[identity] for identity in identity_set
                           if identity[3] in lane_domains)
        statuses[lane] = PASS_FOCUSED_DIAGNOSTIC if complete and not lane_has_gap else ROOT_OPEN

    if q_nonempty_state and not q_cleanup:
        overall = ROOT_OPEN
    elif any(status == ROOT_OPEN for status in statuses.values()):
        overall = ROOT_OPEN
    elif all(status == PASS_FOCUSED_DIAGNOSTIC for status in statuses.values()):
        overall = PASS_FOCUSED_DIAGNOSTIC
    else:
        overall = NOT_TRIGGERED
    sequences = {}
    for identity in identity_set:
        domain = identity[3]
        current = sequences.setdefault(domain, {"last_seq": 0, "gap": False})
        current["last_seq"] = max(current["last_seq"], seq[identity])
        current["gap"] = current["gap"] or gaps[identity]
    return {"overall": overall, "lanes": statuses, "barriers": barriers,
            "sequences": sequences}
