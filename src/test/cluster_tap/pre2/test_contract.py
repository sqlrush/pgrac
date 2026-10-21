"""Harness-only negative tests. These do not count as cluster acceptance."""
import copy
import unittest
import contract
from contract import check_wal, check_reset, check_membership


def wal_events():
    return [
        dict(kind="contribution", id="a", tag="1/2/3/0/0", space="space1",
             writer="1/7/2", version="v1", wal="/tmp/wal1"),
        dict(kind="data", id="d", tag="1/2/3/0/0", space="space1", version="v2",
             fsync_completed=True, readback_sha256="a" * 64, sha256="a" * 64,
             ancestors=[dict(writer="1/7/2", version="v1")]),
        dict(kind="coverage", id="p", receipt="d", contributions=["a"],
             versions=["v1"], writers=["1/7/2"]),
        dict(kind="retire", contribution="a", proof="p"),
        dict(kind="wal_release", wal="/tmp/wal1"),
    ]


WORKLOAD_LANES = ("R/IMG", "I/TT", "H/HOT", "Q/RECEIPT")


def workload_events(include_i=True, late_i=False, cleanup=True):
    identities = [
        dict(node="n1", pid=101, procno=1, sequence_domain="r"),
        dict(node="n1", pid=101, procno=1, sequence_domain="i"),
        dict(node="n1", pid=101, procno=1, sequence_domain="h"),
        dict(node="n1", pid=101, procno=1, sequence_domain="q"),
    ]
    seq = {tuple(item[field] for field in ("node", "pid", "procno", "sequence_domain")): 0
           for item in identities}
    gaps = {key: False for key in seq}
    events = []

    def snapshots():
        return [dict(item, last_seq=seq[tuple(item[field] for field in
                                             ("node", "pid", "procno", "sequence_domain"))],
                     gap=gaps[tuple(item[field] for field in
                                   ("node", "pid", "procno", "sequence_domain"))])
                for item in identities]

    def barrier(name, **extra):
        event = dict(kind="barrier", name=name, snapshots=snapshots())
        event.update(extra)
        events.append(event)

    def add_event(lane, phase, relation, key, **extra):
        domain = {"R/IMG": "r", "I/TT": "i", "H/HOT": "h", "Q/RECEIPT": "q"}[lane]
        identity = next(item for item in identities if item["sequence_domain"] == domain)
        identity_key = tuple(identity[field] for field in
                             ("node", "pid", "procno", "sequence_domain"))
        seq[identity_key] += 1
        event = dict(kind="event", event_id="{}-{}".format(domain, seq[identity_key]),
                     lane=lane, phase=phase, node=identity["node"], pid=identity["pid"],
                     procno=identity["procno"], sequence_domain=domain,
                     event_seq=seq[identity_key], relation_locator=relation,
                     logical_key=key)
        event.update(extra)
        events.append(event)

    relations = {
        "R/IMG": dict(spcOid=11, dbOid=22, relNumber=301, forkNum=0, blockNum=7),
        "I/TT": dict(spcOid=11, dbOid=22, relNumber=302, forkNum=0, blockNum=8),
        "H/HOT": dict(spcOid=11, dbOid=22, relNumber=303, forkNum=0, blockNum=9),
        "Q/RECEIPT": dict(spcOid=11, dbOid=22, relNumber=304, forkNum=0, blockNum=10),
    }
    barrier("IDENTITY_SEAL", identities=identities, source_sha="source-commit",
            build_sha="build-id", binary_sha256="c" * 64, config_sha256="d" * 64)
    barrier("BASELINE")
    add_event("R/IMG", "seed", relations["R/IMG"], "r-key", actual=True)
    add_event("R/IMG", "request", relations["R/IMG"], "r-key", actual=True)
    add_event("R/IMG", "serve", relations["R/IMG"], "r-key", actual=True)
    add_event("R/IMG", "reply", relations["R/IMG"], "r-key", actual=True)
    add_event("R/IMG", "consume", relations["R/IMG"], "r-key", actual=True)
    barrier("R/IMG")
    if include_i:
        add_event("I/TT", "admit", relations["I/TT"], "i-key", owner="E", actual=True)
        add_event("I/TT", "publish", relations["I/TT"], "i-key", owner="E", actual=True)
        add_event("I/TT", "reread", relations["I/TT"], "i-key", owner="E", actual=True)
    barrier("I/TT", owner="E")
    if late_i:
        add_event("I/TT", "publish", relations["I/TT"], "i-key", owner="E",
                  actual=True, deferred=True)
    add_event("H/HOT", "update", relations["H/HOT"], "h-key", actual=True)
    add_event("H/HOT", "hot", relations["H/HOT"], "h-key", actual=True)
    add_event("H/HOT", "prune", relations["H/HOT"], "h-key", actual=True)
    barrier("H/HOT")
    add_event("Q/RECEIPT", "mutation", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True,
              state=dict(pending=1, receipt=1, grd=1, pcm=1, tt=1))
    add_event("Q/RECEIPT", "prepare", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True)
    add_event("Q/RECEIPT", "publish", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True)
    add_event("Q/RECEIPT", "apply_consume", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True)
    add_event("Q/RECEIPT", "close", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True)
    add_event("Q/RECEIPT", "ack", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True)
    add_event("Q/RECEIPT", "certificate_retire", relations["Q/RECEIPT"], "q-key",
              receipt_key="receipt-1", actual=True)
    if cleanup:
        add_event("Q/RECEIPT", "state_clear", relations["Q/RECEIPT"], "q-key",
                  receipt_key="receipt-1", actual=True, cleanup_proof="receipt-1")
    barrier("Q/RECEIPT")
    barrier("STOP_SEAL", stop_state=dict(pending=0, receipt=0, grd=0, pcm=0, tt=0))
    return events


class EvidenceContract(unittest.TestCase):
    def test_wal_requires_exact_durable_coverage_before_release(self):
        good = wal_events()
        check_wal(good, require_release=True)
        for index in (1, 2, 3):
            with self.subTest(missing=index), self.assertRaises(AssertionError):
                check_wal(good[:index] + good[index + 1:], require_release=True)
        for index, field, value in [(1, "fsync_completed", False),
                                    (1, "space", "new-space"),
                                    (1, "readback_sha256", "b" * 64),
                                    (2, "versions", ["stale"]),
                                    (2, "writers", ["1/8/2"])]:
            broken = copy.deepcopy(good)
            broken[index][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                check_wal(broken, require_release=True)

    def test_redirty_and_late_receipt_cannot_retire_new_contribution(self):
        events = wal_events()
        again = dict(events[0], id="b", version="v3")
        with self.assertRaises(AssertionError):
            check_wal(events[:3] + [again] + events[3:], require_release=True)
        with self.assertRaises(AssertionError):
            check_wal(events[:3] + [again, dict(kind="retire", contribution="b", proof="p")])
        check_wal(events[:3] + [again] + events[3:4])

    def test_empty_or_duplicate_evidence_is_not_a_positive_run(self):
        for bad in ([], [wal_events()[0]] * 2, wal_events() + [wal_events()[1]]):
            with self.subTest(events=bad), self.assertRaises(AssertionError):
                check_wal(bad, require_release=True)

    def test_new_proof_cannot_make_old_data_cover_redirty_or_an_unrelated_ancestor(self):
        events = wal_events()
        again = dict(events[0], id="b", version="v3")
        proof = dict(events[2], contributions=["a", "b"], versions=["v1", "v3"],
                     writers=["1/7/2", "1/7/2"])
        bad = events[:2] + [again, proof, events[3],
                           dict(kind="retire", contribution="b", proof="p"), events[4]]
        with self.assertRaises(AssertionError):
            check_wal(bad, require_release=True)
        unrelated = copy.deepcopy(events)
        unrelated[1]["ancestors"] = [dict(writer="9/9/9", version="unrelated")]
        with self.assertRaises(AssertionError):
            check_wal(unrelated, require_release=True)

    def test_reset_is_after_recovery_and_before_open_for_every_survivor(self):
        trace = [dict(kind="catalog_recovered", duty="d1"),
                 dict(kind="reset_all", duty="d1", node=0, boot="b0", barrier="r1"),
                 dict(kind="reset_all", duty="d1", node=2, boot="b2", barrier="r1"),
                 dict(kind="open", duty="d1", barrier="r1")]
        check_reset(trace, "d1", {0: "b0", 2: "b2"})
        for bad in (trace[1:], trace[:2] + trace[3:], [trace[0], trace[3]] + trace[1:3]):
            with self.subTest(trace=bad), self.assertRaises(AssertionError):
                check_reset(bad, "d1", {0: "b0", 2: "b2"})
        stale = copy.deepcopy(trace)
        stale[2]["boot"] = "old"
        with self.assertRaises(AssertionError):
            check_reset(stale, "d1", {0: "b0", 2: "b2"})

    def test_leave_failure_has_bounded_abort_then_fail_stop(self):
        trace = [dict(kind="reserve", request="leave1", at_ms=0),
                 dict(kind="drain", request="leave1", at_ms=10),
                 dict(kind="peer_failed", request="leave1", at_ms=20),
                 dict(kind="abort", request="leave1", at_ms=25),
                 dict(kind="abort_closed", request="leave1", at_ms=30),
                 dict(kind="fail_stop_reserve", request="failure2", at_ms=31),
                 dict(kind="fail_stop_complete", request="failure2", at_ms=40)]
        check_membership(trace, "leave1", "failure2", 50, 100)
        for changed in ("missing_closed", "late_abort", "wrong_identity", "missing_recovery"):
            bad = copy.deepcopy(trace)
            if changed == "missing_closed":
                del bad[4]
            elif changed == "late_abort":
                for event in bad[3:]:
                    event["at_ms"] += 1000
            elif changed == "wrong_identity":
                bad[4]["request"] = "other"
            else:
                bad.pop()
            with self.subTest(changed=changed), self.assertRaises(AssertionError):
                check_membership(bad, "leave1", "failure2", 50, 100)


class WorkloadBarrierContract(unittest.TestCase):
    def test_full_workload_returns_focused_pass_for_each_lane(self):
        result = contract.check_workload_contract(workload_events())
        self.assertEqual(result["overall"], "PASS_FOCUSED_DIAGNOSTIC")
        self.assertEqual(set(result["lanes"]), set(WORKLOAD_LANES))
        self.assertTrue(all(status == "PASS_FOCUSED_DIAGNOSTIC"
                            for status in result["lanes"].values()))

    def test_identity_and_lane_objects_are_sealed_and_disjoint(self):
        good = workload_events()
        identity = copy.deepcopy(good)
        identity[0].pop("binary_sha256")
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(identity)
        for field in ("relation_locator", "logical_key"):
            bad = copy.deepcopy(good)
            r_event = next(event for event in bad
                           if event.get("kind") == "event" and event["lane"] == "R/IMG")
            q_event = next(event for event in bad
                           if event.get("kind") == "event" and event["lane"] == "Q/RECEIPT")
            q_event[field] = copy.deepcopy(r_event[field])
            with self.subTest(field=field), self.assertRaises(AssertionError):
                contract.check_workload_contract(bad)

    def test_sequence_domains_are_independent_but_gaps_remain_root_open(self):
        result = contract.check_workload_contract(workload_events())
        self.assertNotEqual(result["sequences"]["r"]["last_seq"],
                            result["sequences"]["i"]["last_seq"])
        bad = workload_events()
        r_events = [event for event in bad
                    if event.get("kind") == "event" and event["lane"] == "R/IMG"]
        for event in r_events[1:]:
            event["event_seq"] += 1
        for barrier in bad:
            if barrier.get("kind") == "barrier" and barrier["name"] not in ("IDENTITY_SEAL", "BASELINE"):
                snapshot = next(item for item in barrier["snapshots"]
                                if item["sequence_domain"] == "r")
                snapshot["last_seq"] += 1
                snapshot["gap"] = True
        result = contract.check_workload_contract(bad)
        self.assertEqual(result["lanes"]["R/IMG"], "ROOT_OPEN")

    def test_missing_or_late_deferred_lane_does_not_become_positive(self):
        missing = contract.check_workload_contract(workload_events(include_i=False))
        self.assertEqual(missing["lanes"]["I/TT"], "NOT_TRIGGERED")
        late = contract.check_workload_contract(workload_events(include_i=False, late_i=True))
        self.assertEqual(late["lanes"]["I/TT"], "NOT_TRIGGERED")
        partial = workload_events()
        i_events = [event for event in partial
                    if event.get("kind") == "event" and event["lane"] == "I/TT"]
        partial.remove(i_events[-1])
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(partial)

    def test_i_tt_owner_and_q_receipt_identity_are_checked(self):
        wrong_owner = workload_events()
        next(event for event in wrong_owner
             if event.get("kind") == "event" and event["lane"] == "I/TT")["owner"] = "C"
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(wrong_owner)
        wrong_domain = workload_events()
        next(event for event in wrong_domain
             if event.get("kind") == "event" and event["lane"] == "R/IMG")["sequence_domain"] = "i"
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(wrong_domain)
        wrong_barrier_owner = workload_events()
        next(event for event in wrong_barrier_owner
             if event.get("kind") == "barrier" and event["name"] == "I/TT")["owner"] = "C"
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(wrong_barrier_owner)
        wrong_receipt = workload_events()
        next(event for event in wrong_receipt
             if event.get("kind") == "event" and event["phase"] == "ack")["receipt_key"] = "other"
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(wrong_receipt)

    def test_stop_seal_cannot_hide_pending_state_and_cannot_be_followed(self):
        result = contract.check_workload_contract(workload_events(cleanup=False))
        self.assertEqual(result["overall"], "ROOT_OPEN")
        late = workload_events() + [dict(kind="event", event_id="late", lane="Q/RECEIPT",
                                         phase="ack", node="n1", pid=101, procno=1,
                                         sequence_domain="q", event_seq=99,
                                         relation_locator=dict(spcOid=11, dbOid=22,
                                                               relNumber=304, forkNum=0,
                                                               blockNum=10),
                                         logical_key="q-key", receipt_key="receipt-1",
                                         actual=True, deferred=True)]
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(late)

    def test_barriers_must_be_ordered_and_snapshots_must_match(self):
        bad_order = workload_events()
        baseline = next(event for event in bad_order
                        if event.get("kind") == "barrier" and event["name"] == "BASELINE")
        r_barrier = next(event for event in bad_order
                         if event.get("kind") == "barrier" and event["name"] == "R/IMG")
        baseline_index, r_barrier_index = bad_order.index(baseline), bad_order.index(r_barrier)
        bad_order[baseline_index], bad_order[r_barrier_index] = r_barrier, baseline
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(bad_order)
        bad_snapshot = workload_events()
        r_barrier = next(event for event in bad_snapshot
                         if event.get("kind") == "barrier" and event["name"] == "R/IMG")
        r_barrier["snapshots"][0]["last_seq"] = 0
        with self.assertRaises(AssertionError):
            contract.check_workload_contract(bad_snapshot)


if __name__ == "__main__":
    unittest.main()
