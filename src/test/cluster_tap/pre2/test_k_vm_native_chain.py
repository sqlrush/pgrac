"""Offline judge tests; these records are fixtures, not native run evidence."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest

from k_vm_native_chain import evaluate, source_precheck


def sample():
    return {
        "version": 1, "writer_node": 2, "xid": "710", "heap_tag": "1663/5/900/0/0",
        "vm_tag": "1663/5/900/2/0", "index_locator": "1663/5/901",
        "published": {"all_visible": True, "all_frozen": True},
        "retained": {"all_visible": True, "all_frozen": True},
        "cleared": {"all_visible": False, "all_frozen": False},
        "before": {"vm_stats_available": "true", "vm_observed_tag_available": "false",
                   "vm_other_tag_observations": "0", "vm_x_acquisition_completed_count": "0",
                   "vm_remote_shared_source_count": "0", "vm_inter_node_transfer_count": "0",
                   "vm_all_visible_clear_count": "0"},
        "after": {"vm_stats_available": "true", "vm_observed_tag_available": "true",
                  "vm_observed_tag": "1663/5/900/2/0", "vm_other_tag_observations": "0",
                  "vm_x_acquisition_completed_count": "1", "vm_remote_shared_source_count": "1",
                  "vm_inter_node_transfer_count": "0", "vm_all_visible_clear_count": "1"},
        "wal": [
            {"start": "100", "end": "180", "xid": "710", "rm": "Heap", "type": "INSERT",
             "description": "off: 1, flags: 0x01",
             "block_ref": "blkref #0: rel 1663/5/900 fork main blk 0, blkref #1: rel 1663/5/900 fork vm blk 0"},
            {"start": "180", "end": "220", "xid": "710", "rm": "Btree", "type": "INSERT_LEAF",
             "description": "off: 1", "block_ref": "blkref #0: rel 1663/5/901 fork main blk 1"},
            {"start": "220", "end": "260", "xid": "710", "rm": "Transaction", "type": "COMMIT",
             "description": "", "block_ref": None}],
        "uncommitted_count": "0", "peers": [{"node": n, "rows": "42|7",
            "plan": "Index Scan using k_vm_native_pkey on k_vm_native"} for n in range(4)],
        "normal_stop": True,
    }


class NativeChainJudge(unittest.TestCase):
    def reject(self, evidence):
        with self.assertRaises(ValueError):
            evaluate(evidence)

    def test_complete_literal_chain_is_observation_only(self):
        self.assertEqual(evaluate(sample())["status"], "NATIVE_CHAIN_OBSERVED")

    def test_commit_null_refs_never_replace_heap_or_index_refs(self):
        e = sample(); e["wal"][2]["block_ref"] = ""
        self.assertEqual(evaluate(e)["status"], "NATIVE_CHAIN_OBSERVED")
        for index in (0, 1):
            e = sample(); e["wal"][index]["block_ref"] = None; self.reject(e)
        e = sample(); del e["wal"][2]["block_ref"]; self.reject(e)
        e = sample(); e["wal"][2]["block_ref"] = "blkref #0: rel 1663/5/900 fork main blk 0"
        self.reject(e)

    def test_unknown_or_missing_vm_is_never_positive(self):
        for stage in ("published", "retained", "cleared"):
            for value in (None, {}, {"all_visible": "true", "all_frozen": True}):
                e = sample(); e[stage] = value; self.reject(e)

    def test_empty_publication_and_peer_old_one_are_required(self):
        for stage in ("published", "retained"):
            e = sample(); e[stage]["all_visible"] = False; self.reject(e)

    def test_clear_is_required_even_after_successful_commit(self):
        e = sample(); e["cleared"]["all_visible"] = True; self.reject(e)

    def test_cached_hits_and_joins_do_not_replace_native_completion(self):
        e = sample(); e["after"]["vm_x_acquisition_completed_count"] = "0"
        e["after"]["vm_cached_x_hit_count"] = "99"; e["after"]["vm_head_join_count"] = "99"
        self.reject(e)

    def test_local_completion_is_not_remote_transfer(self):
        e = sample(); e["after"]["vm_remote_shared_source_count"] = "0"; self.reject(e)

    def test_real_remote_x_completion_is_also_accepted(self):
        e = sample(); e["after"]["vm_remote_shared_source_count"] = "0"
        e["after"]["vm_inter_node_transfer_count"] = "1"
        self.assertEqual(evaluate(e)["status"], "NATIVE_CHAIN_OBSERVED")

    def test_wrong_tag_other_observations_or_counter_rollback_refuse(self):
        for key, value in (("vm_observed_tag", "1663/5/902/2/0"),
                           ("vm_other_tag_observations", "1"),
                           ("vm_all_visible_clear_count", "0"),
                           ("vm_x_acquisition_completed_count", "-1")):
            e = sample(); e["after"][key] = value; self.reject(e)

    def test_missing_or_noninteger_metrics_refuse(self):
        for key in ("vm_x_acquisition_completed_count", "vm_all_visible_clear_count"):
            e = sample(); del e["after"][key]; self.reject(e)
            e = sample(); e["after"][key] = True; self.reject(e)

    def test_heap_wal_requires_exact_heap_and_vm_blocks(self):
        for ref in ("blkref #0: rel 1663/5/900 fork main blk 0",
                    "blkref #0: rel 1663/5/900 fork main blk 0, blkref #1: rel 1663/5/902 fork vm blk 0",
                    "blkref #0: rel 1663/5/900 fork main blk 1, blkref #1: rel 1663/5/900 fork vm blk 0"):
            e = sample(); e["wal"][0]["block_ref"] = ref; self.reject(e)

    def test_heap_wal_must_record_all_visible_clear(self):
        for desc in ("off: 1, flags: 0x00", "", "flags: 0x02"):
            e = sample(); e["wal"][0]["description"] = desc; self.reject(e)

    def test_other_xid_or_index_wal_cannot_satisfy_chain(self):
        for index in range(3):
            e = sample(); e["wal"][index]["xid"] = "711"; self.reject(e)
        e = sample(); e["wal"][1]["block_ref"] = "blkref #0: rel 1663/5/902 fork main blk 1"; self.reject(e)

    def test_clear_wal_precedes_index_and_commit(self):
        for permutation in ((1, 0, 2), (0, 2, 1)):
            e = sample()
            for i, old in enumerate(permutation):
                e["wal"][old]["start"] = str(100 + i * 100)
                e["wal"][old]["end"] = str(180 + i * 100)
            self.reject(e)

    def test_duplicate_missing_overlapping_wal_refuses(self):
        for index in range(3):
            e = sample(); del e["wal"][index]; self.reject(e)
        e = sample(); e["wal"].append(copy.deepcopy(e["wal"][0])); self.reject(e)
        e = sample(); e["wal"][1]["start"] = "150"; self.reject(e)

    def test_uncommitted_visibility_and_all_four_index_results_are_checked(self):
        e = sample(); e["uncommitted_count"] = "1"; self.reject(e)
        for key, value in (("rows", "42|8"), ("plan", "Seq Scan on k_vm_native"), ("node", 2)):
            e = sample(); e["peers"][1][key] = value; self.reject(e)
        for plan in ("Bitmap Index Scan using k_vm_native_pkey on k_vm_native",
                     "Index Scan using k_vm_native_pkey on k_vm_native_other"):
            e = sample(); e["peers"][1]["plan"] = plan; self.reject(e)
        e = sample(); e["peers"].pop(); self.reject(e)

    def test_normal_stop_and_exact_input_domain_are_required(self):
        for key, value in (("normal_stop", False), ("normal_stop", "true"), ("version", 2),
                           ("writer_node", 0), ("heap_tag", "1663/5/900/0/1"),
                           ("vm_tag", "1663/5/900/0/0")):
            e = sample(); e[key] = value; self.reject(e)

    def test_source_census_uses_actual_k_functions(self):
        root = Path(__file__).resolve().parents[4]
        self.assertEqual(source_precheck(root)["status"], "SOURCE_ENTRIES_PRESENT")

    def test_cli_refuses_invalid_json_without_native_success(self):
        r = subprocess.run([sys.executable, "-B", str(Path(__file__).with_name("k_vm_native_chain.py"))],
                           input="{}", text=True, capture_output=True)
        self.assertEqual(r.returncode, 1)
        self.assertEqual(json.loads(r.stdout)["status"], "REFUSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
