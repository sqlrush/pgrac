"""Four-fact shutdown evidence tests; fixture outputs cannot start a database.
Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import importlib
import time
import unittest

import test_lifecycle as lifecycle_tests


class CleanClosureTests(unittest.TestCase):
    def setUp(self):
        self.mod = importlib.import_module("clean_closure")
        base = lifecycle_tests.LifecycleTests()
        base.setUp()
        self.addCleanup(base.doCleanups)
        self.nodes, self.binary = base.nodes, base.binary
        self.post_stop_not_before_ns = time.monotonic_ns()
        self.observations = base.observations("shut down")
        self.starts, self.logs, self.states = [], [], []
        for n, node in enumerate(self.nodes):
            identity = dict(pid=100+n, starttime=200+n, exe_sha256=self.binary)
            self.starts.append(dict(node=node, process_identity=identity,
                                    incarnation=1000+n))
            raw = "new LOG: "+self.mod.PROTOCOL_MARKER+"\n"
            self.logs.append(dict(node_id=n, boot_id=node["boot_id"],
                                  process_identity=identity,
                                  before=dict(dev=10, ino=20+n, size=1000),
                                  after=dict(dev=10, ino=20+n, size=1000+len(raw.encode())),
                                  raw=raw))
            self.states.append({key: "0" for key in self.mod.DEBT_KEYS})
        self.before_votes, self.after_votes = [], []
        for disk in range(3):
            before = dict(index=disk, wwid="360014056bfe%d00009214000800000000" % disk,
                          status="OBSERVED", read_only=True, direct=True, strict_authority=True,
                          deployment_qualified=False, read_bytes=525824,
                          major=8, minor=16*disk, capacity=16777216, logical_sector=512,
                          members=[dict(node_id=n, valid=True, incarnation=1000+n if n<4 else 0,
                                        flags=1 if n<4 else 0, generation=10, epoch=2)
                                   for n in range(128)])
            after = copy.deepcopy(before)
            for slot in after["members"]:
                slot.update(flags=0, generation=11)
            self.before_votes.append(before)
            self.after_votes.append(after)

    def run_closure(self):
        return self.mod.verify_closure(self.nodes, self.binary, self.observations,
                                       "7654321000123456789", self.starts, self.logs,
                                       self.before_votes, self.after_votes, self.states,
                                       post_stop_not_before_ns=self.post_stop_not_before_ns)

    def test_prior_observations_are_not_evidence_of_this_stop(self):
        for old in self.observations:
            old.update(begin_monotonic_ns=1, end_monotonic_ns=2)
        result = self.run_closure()
        self.assertFalse(result["clean_stop_proven"])
        self.assertFalse(result["data_clean"])
        self.assertFalse(result["process_gone"])
        self.assertEqual(result["state"], "OBSERVATION_INCOMPLETE")

    def test_all_four_facts_are_required(self):
        result = self.run_closure()
        self.assertEqual(result["state"], "CLEAN_STOPPED")
        self.assertTrue(result["data_clean"])
        self.assertTrue(result["process_gone"])
        self.assertTrue(result["protocol_closed"])
        self.assertTrue(result["persistent_closure"])
        # A stored observation never substitutes for restart's physical recheck.
        self.assertTrue(result["clean_stop_proven"])
        self.assertFalse(result["restart_allowed"])

    def test_old_missing_or_replaced_log_does_not_close_protocol(self):
        for kind in ("old", "missing", "rotated", "wrong-start", "error", "short"):
            logs = copy.deepcopy(self.logs)
            if kind == "old":
                self.logs[0]["after"]["size"] = 1000
            elif kind == "missing":
                self.logs[0]["raw"] = ""
            elif kind == "rotated":
                self.logs[0]["after"]["ino"] += 1
            elif kind == "wrong-start":
                self.logs[0]["process_identity"] = dict(pid=100, starttime=1, exe_sha256=self.binary)
            elif kind == "error":
                self.logs[0]["raw"] += "new FATAL: normal shutdown self-slot clear failed\n"
            else:
                self.logs.pop()
            with self.subTest(kind=kind):
                result = self.run_closure()
                self.assertFalse(result["clean_stop_proven"])
                self.assertTrue(result["data_clean"])
            self.logs = logs

    def test_live_invalid_foreign_or_fresh_slot_cannot_prove_own_clear(self):
        for field, value in (("flags", 1), ("valid", False), ("incarnation", 0),
                             ("incarnation", 9000), ("generation", 10), ("epoch", 1)):
            votes = copy.deepcopy(self.after_votes)
            self.after_votes[1]["members"][2][field] = value
            with self.subTest(field=field, value=value):
                result = self.run_closure()
                self.assertFalse(result["persistent_closure"])
                self.assertFalse(result["clean_stop_proven"])
            self.after_votes = votes

    def test_missing_disk_or_different_device_or_nonstrict_read_is_not_clear(self):
        for kind in ("missing", "duplicate", "wwid", "capacity", "direct", "debt", "missing-debt"):
            saved = copy.deepcopy((self.after_votes, self.states))
            if kind == "missing":
                self.after_votes.pop()
            elif kind == "duplicate":
                self.after_votes[1] = self.after_votes[0]
            elif kind in ("wwid", "capacity", "direct"):
                self.after_votes[0][kind] = {"wwid": "different", "capacity": 1, "direct": False}[kind]
            elif kind == "debt":
                self.states[0][self.mod.DEBT_KEYS[0]] = "1"
            else:
                self.states[0].pop(self.mod.DEBT_KEYS[0])
            with self.subTest(kind=kind):
                self.assertFalse(self.run_closure()["clean_stop_proven"])
            self.after_votes, self.states = saved


if __name__ == "__main__":
    unittest.main()
