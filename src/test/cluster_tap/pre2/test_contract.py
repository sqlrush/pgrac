"""Harness-only negative tests. These do not count as cluster acceptance."""
import copy
import unittest
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


if __name__ == "__main__":
    unittest.main()
