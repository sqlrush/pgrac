"""Reject incomplete native slot/lock evidence instead of inferring supply.

Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import unittest

from undo_slot_wait import evaluate


def full_sample():
    # Independent literal native field contract: 48 ACTIVE owners, one segment.
    slots = [dict(offset=i, xid=200 + i, wrap=7, status=1, commit_scn="0")
             for i in range(48)]
    bindings = [dict(available=True, wait_scope=False, pid=1000 + i,
                     xid=200 + i, origin=0, segment_id=9, segment_generation=0,
                     slot_offset=i, slot_wrap=7, active_lsn="4096")
                for i in range(48)]
    return dict(stage="full", snapshot=dict(available=True, origin=0,
                segment_id=9, binding_generation="18446744073709551614",
                slots=slots), bindings=bindings)


def blocked_sample():
    sample = full_sample()
    counters = dict(tt_capacity_stats_available="true",
                    tt_capacity_origin_node_id="0",
                    tt_slot_active_full_wait_count="18446744073709551612",
                    tt_capacity_pool_wait_count="2", tt_capacity_cancel_count="3",
                    segment_switch_count="4", autoextend_count="5",
                    segment_reuse_count="6", record_alloc_count="100")
    after = dict(counters, tt_slot_active_full_wait_count="18446744073709551613")
    return dict(stage="blocked", before=sample, after=copy.deepcopy(sample),
                counters_before=counters, counters_after=after,
                waiter_pid=9000, waiter_xid=500,
                activity=dict(pid=9000, xid="500", wait_event="ClusterUndoTxSlotWait",
                              wait_event_type="Cluster: Undo", blockers=[1000]),
                locks=[dict(pid=9000, locktype="virtualxid", mode="ShareLock",
                            granted=False, virtualxid="4/73"),
                       dict(pid=1000, locktype="virtualxid", mode="ExclusiveLock",
                            granted=True, virtualxid="4/73")])


def idle_sample():
    return dict(stage="idle", snapshot=dict(available=True, origin=0, segment_id=9,
                binding_generation="18446744073709551614",
                slots=[dict(offset=i, xid=0, wrap=7, status=0, commit_scn="0")
                       for i in range(48)]))


class UndoSlotEvidenceTests(unittest.TestCase):
    def reject(self, value):
        result = evaluate(value)
        self.assertIs(result["passed"], False, result)
        self.assertTrue(result["reason"])

    def test_full_sample_joins_all_actual_bindings_and_identifies_slot_zero(self):
        sample = full_sample()
        sample["bindings"].reverse()
        result = evaluate(sample)
        self.assertIs(result["passed"], True, result)
        self.assertEqual(result["selected_pid"], 1000)

    def test_preparatory_terminal_slots_must_be_really_free_before_filling(self):
        sample = idle_sample()
        self.assertIs(evaluate(sample)["passed"], True)
        for field, value in (("status", 1), ("status", 2), ("status", 3),
                             ("xid", 200), ("xid", False), ("offset", 0),
                             ("commit_scn", "1"), ("wrap", 65535)):
            sample = idle_sample()
            sample["snapshot"]["slots"][47][field] = value
            self.reject(sample)
        for field, value in (("available", False), ("slots", []),
                             ("binding_generation", "0"), ("segment_id", 0)):
            sample = idle_sample()
            sample["snapshot"][field] = value
            self.reject(sample)

    def test_client_count_does_not_replace_full_slots(self):
        for size in (0, 47, 49):
            sample = full_sample()
            sample["snapshot"]["slots"] = sample["snapshot"]["slots"][:size]
            if size == 49:
                sample["snapshot"]["slots"].append(dict(sample["snapshot"]["slots"][0]))
            self.reject(sample)

    def test_slot_identity_status_and_generation_must_be_exact(self):
        faults = [("xid", 2), ("xid", True), ("xid", 200), ("offset", 0),
                  ("wrap", 65535), ("status", 0), ("status", 2), ("status", 255),
                  ("commit_scn", "1"), ("commit_scn", 0)]
        for field, value in faults:
            sample = full_sample()
            sample["snapshot"]["slots"][47][field] = value
            self.reject(sample)
        for field, value in (("available", False), ("origin", -1), ("segment_id", 0),
                             ("binding_generation", "0"),
                             ("binding_generation", "18446744073709551615"),
                             ("binding_generation", 1)):
            sample = full_sample()
            sample["snapshot"][field] = value
            self.reject(sample)

    def test_missing_duplicate_or_mismatched_canonical_binding_refuses(self):
        for field, value in (("available", False), ("wait_scope", True), ("xid", 200),
                             ("pid", 1000), ("segment_id", 10), ("origin", 1),
                             ("slot_offset", 0), ("slot_wrap", 8),
                             ("segment_generation", 4294967295), ("active_lsn", "0")):
            sample = full_sample()
            sample["bindings"][47][field] = value
            self.reject(sample)
        sample = full_sample()
        sample["bindings"].pop()
        self.reject(sample)
        sample = full_sample()
        del sample["bindings"][0]["active_lsn"]
        self.reject(sample)

    def test_actual_native_vxid_edge_and_unchanged_allocator_pass(self):
        result = evaluate(blocked_sample())
        self.assertIs(result["passed"], True, result)
        self.assertEqual(result["selected_pid"], 1000)

    def test_count_or_wait_name_without_native_edge_is_not_a_wait_proof(self):
        for field, value in (("xid", "501"), ("pid", 9001),
                             ("wait_event", "ClusterUndoCapacityWait"),
                             ("wait_event_type", "Lock"), ("blockers", []),
                             ("blockers", [1001]), ("blockers", [1000, 1001]),
                             ("blockers", [0])):
            sample = blocked_sample()
            sample["activity"][field] = value
            self.reject(sample)
        for value in ([], [dict(pid=9000, locktype="virtualxid", mode="ShareLock",
                               granted=False, virtualxid="4/73")]):
            sample = blocked_sample()
            sample["locks"] = value
            self.reject(sample)

    def test_wrong_grant_identity_or_lock_kind_cannot_impersonate_vxid_owner(self):
        for field, value in (("pid", 1001), ("granted", False), ("mode", "ShareLock"),
                             ("locktype", "transactionid"), ("virtualxid", "4/74")):
            sample = blocked_sample()
            sample["locks"][1][field] = value
            self.reject(sample)
        sample = blocked_sample()
        sample["locks"].append(dict(sample["locks"][1]))
        self.reject(sample)

    def test_rebinding_or_writing_while_blocked_is_not_success(self):
        for field in ("segment_switch_count", "autoextend_count", "segment_reuse_count",
                      "record_alloc_count", "tt_capacity_pool_wait_count",
                      "tt_capacity_cancel_count"):
            sample = blocked_sample()
            sample["counters_after"][field] = str(int(sample["counters_after"][field]) + 1)
            self.reject(sample)
        for group, field, value in (("snapshot", "binding_generation", "2"),
                                    ("snapshot", "segment_id", 10)):
            sample = blocked_sample()
            sample["after"][group][field] = value
            self.reject(sample)
        sample = blocked_sample()
        sample["after"]["snapshot"]["slots"][0]["wrap"] = 8
        self.reject(sample)

    def test_missing_unavailable_non_uint_or_nonadvancing_counters_refuse(self):
        for value in ("false", False, None):
            sample = blocked_sample()
            sample["counters_after"]["tt_capacity_stats_available"] = value
            self.reject(sample)
        for value in ("18446744073709551612", "18446744073709551611", "1.0", "-1",
                      "18446744073709551616", True):
            sample = blocked_sample()
            sample["counters_after"]["tt_slot_active_full_wait_count"] = value
            self.reject(sample)
        sample = blocked_sample()
        del sample["counters_before"]["record_alloc_count"]
        self.reject(sample)
        sample = blocked_sample()
        sample["counters_after"]["tt_capacity_origin_node_id"] = "1"
        self.reject(sample)

    def test_already_bound_waiter_or_malformed_evidence_refuses(self):
        sample = blocked_sample()
        sample["waiter_xid"] = 200
        self.reject(sample)
        for value in (None, [], {}, {"stage": "unknown"}):
            self.reject(value)

    def test_malformed_native_lock_rows_refuse_without_crashing(self):
        for value in (None, [], "owner", 1):
            sample = blocked_sample()
            sample["locks"] = [value]
            self.reject(sample)


if __name__ == "__main__":
    unittest.main()
