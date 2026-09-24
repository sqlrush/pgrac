"""Actual C authentication plus local registry binding, without target actions.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_target_registry.py /absolute/path/pgrac-fenced-map-verify
"""

import ast
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
from target_registry import load_registry


class TargetRegistryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo = Path(__file__).resolve().parents[4]
        text = (repo / "src/test/cluster_unit/data/pgrac_fence_map_v2_fixture.h").read_text()
        literals = text.split("static const char fenced_config_v2[]", 1)[1]
        config = "".join(ast.literal_eval(value) for value in re.findall(r'"(?:[^"\\]|\\.)*"', literals))
        cls.packet = bytes.fromhex(dict(line.split("=", 1) for line in config.splitlines())["node.2.adapter_data"])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-registry-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        binary = self.directory / "verifier"
        shutil.copyfile(EXECUTABLE, binary)
        binary.chmod(0o700)
        self.registry = self.directory / "registry.json"
        routes = []
        for i in range(4):
            file = self.directory / f"disk{i}"
            file.write_bytes(b"d" * 4096)
            info = file.stat()
            start = 32 + 174 + 176 * i
            route_digest = hashlib.sha256(b"PGRAC-TARGET-ROUTE-V1\0" + i.to_bytes(4, "little")
                                          + self.packet[start:start + 176]).hexdigest()
            routes.append({"ordinal": i, "digest": route_digest, "kind": 1,
                           "initiator": "iqn.2026-09.test:guest0", "credential_ref": "cred-1",
                           "target": "iqn.2026-09.test:target", "endpoint": "10.0.0.10:3260",
                           "tpg": i + 1, "lun_wwid": "wwid-1", "lun_serial": "serial-1",
                           "backstore_uuid": f"{16 + i:02x}" * 16, "media_kind": 1 if i < 3 else 2,
                           "filesystem_uuid": f"{32 + i:02x}" * 16 if i < 3 else "00" * 16,
                           "roles": 1 << i, "mapped_lun": 0, "tpg_lun": 10 + i,
                           "storage_path": f"/sys/kernel/config/target/core/fileio_{i}/disk{i}",
                           "file_path": str(file), "device": info.st_dev, "inode": info.st_ino, "size": 4096})
        self.document = {"version": 1, "system_identifier": 123456789, "mapping_generation": 7,
                         "map_public_key": "c050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a",
                         "verifier": {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()},
                         "nodes": [{"node_id": 2,
                                    "protected_set_digest": "84dc62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828",
                                    "signed_map": self.packet.hex(), "routes": routes}]}
        self.write()

    def write(self):
        self.registry.write_text(json.dumps(self.document, sort_keys=True, separators=(",", ":")) + "\n")
        self.registry.chmod(0o600)

    def load(self):
        return load_registry(str(self.registry), time.monotonic_ns() + 5_000_000_000,
                             owner_uid=os.geteuid())

    def load_ok(self):
        try:
            return self.load()
        except TargetJournalError as error:
            self.fail(f"complete authenticated registry refused: {error}")

    def test_complete_registry_preserves_native_locators_and_derived_identity(self):
        registry = self.load_ok()
        node = registry.node(2)
        self.assertEqual(len(node.bindings), 4)
        self.assertEqual(tuple(b.tpg for b in node.bindings), (1, 2, 3, 4))
        self.assertEqual(tuple(b.tpg_lun for b in node.bindings), (10, 11, 12, 13))
        self.assertEqual(tuple(b.portal for b in node.bindings), ("10.0.0.10",) * 4)
        identity = registry.drain_identity(2, "aa" * 16, 3, "bb" * 16, "cc" * 16)
        self.assertEqual(identity.guest_uuid, "05" * 16)
        self.assertEqual(identity.mapping_generation, 7)
        self.assertEqual(identity.protected_set_digest, self.document["nodes"][0]["protected_set_digest"])
        self.assertEqual(identity.route_digests, tuple(b.digest for b in node.bindings))
        self.assertEqual((identity.operation_id, identity.attempt, identity.daemon_boot_id, identity.target_boot_id),
                         ("aa" * 16, 3, "bb" * 16, "cc" * 16))
        expected_inventory = hashlib.sha256((json.dumps(self.document, sort_keys=True, ensure_ascii=True,
                                                      separators=(",", ":")) + "\n").encode("ascii")).hexdigest()
        self.assertEqual(registry.inventory_digest, expected_inventory)
        with self.assertRaises(TargetJournalError):
            registry.node(1)
        with self.assertRaises(TargetJournalError):
            registry.drain_identity(2, "aa" * 16, 0, "bb" * 16, "cc" * 16)

    def test_all_signed_logical_metadata_must_match(self):
        route = self.document["nodes"][0]["routes"][0]
        for field, value in (("kind", 2), ("initiator", "iqn.2026-09.test:other"),
                             ("credential_ref", "cred-2"), ("target", "iqn.2026-09.test:other"),
                             ("endpoint", "10.0.0.11:3260"), ("tpg", 2), ("lun_wwid", "wwid-2"),
                             ("lun_serial", "serial-2"), ("backstore_uuid", "bb" * 16),
                             ("media_kind", 2), ("filesystem_uuid", "ab" * 16),
                             ("roles", 2), ("roles", True), ("digest", "ab" * 32)):
            old = route[field]
            route[field] = value
            self.write()
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.load()
            route[field] = old

    def test_missing_duplicate_reordered_and_unexpected_routes_refuse(self):
        node = self.document["nodes"][0]
        original = node["routes"]
        for routes in (original[:3], original + [original[0]], [original[0]] * 4,
                       list(reversed(original))):
            node["routes"] = routes
            self.write()
            with self.subTest(routes=len(routes)), self.assertRaises(TargetJournalError):
                self.load()
        node["routes"] = original
        node["routes"][0]["unverified_alias"] = "somewhere"
        self.write()
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_native_locator_shape_cannot_smuggle_alternate_paths(self):
        route = self.document["nodes"][0]["routes"][0]
        for field, value in (("mapped_lun", True), ("mapped_lun", 256), ("tpg_lun", -1),
                             ("storage_path", "/tmp/not-configfs"), ("file_path", "/tmp/../elsewhere"),
                             ("file_path", "relative"), ("device", -1), ("inode", 0), ("size", 0)):
            old = route[field]
            route[field] = value
            self.write()
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.load()
            route[field] = old

    def test_config_file_ownership_mode_link_and_missing_are_not_trusted(self):
        self.registry.chmod(0o644)
        with self.assertRaises(TargetJournalError):
            self.load()
        self.registry.chmod(0o600)
        with self.assertRaises(TargetJournalError):
            load_registry(str(self.registry), time.monotonic_ns() + 5_000_000_000,
                          owner_uid=os.geteuid() + 1)
        link = self.directory / "alias"
        os.link(self.registry, link)
        with self.assertRaises(TargetJournalError):
            self.load()
        link.unlink()
        self.registry.rename(link)
        self.registry.symlink_to(link)
        with self.assertRaises(TargetJournalError):
            self.load()
        self.registry.unlink()
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_malformed_duplicate_unknown_and_oversized_configuration_refuse(self):
        original = self.registry.read_text()
        for contents in ("{", original.replace('"version":1', '"version":1,"version":1'),
                         original.replace('"version":1', '"version":NaN'), " " * (4 * 1024 * 1024 + 1)):
            self.registry.write_text(contents)
            with self.subTest(size=len(contents)), self.assertRaises(TargetJournalError):
                self.load()
        self.document["shadow_authority"] = True
        self.write()
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_replaceable_ancestor_cannot_supply_an_old_owner_owned_registry(self):
        parent = self.directory / "replaceable"
        parent.mkdir(mode=0o700)
        old = self.registry
        self.registry = parent / "registry.json"
        self.write()
        # A valid signature and 0600 leaf do not authorize a directory in which
        # another user could rename a previously genuine configuration back in.
        self.assertEqual(self.load_ok().node(2).mapping.mapping_generation, 7)
        for mode in (0o777, 0o770):
            parent.chmod(mode)
            with self.subTest(mode=mode), self.assertRaises(TargetJournalError):
                self.load()
        parent.chmod(0o700)
        self.assertEqual(self.load_ok().node(2).mapping.mapping_generation, 7)
        self.registry = old

    def test_wrong_key_signature_or_global_binding_cannot_load(self):
        for field, value in (("system_identifier", 8), ("mapping_generation", 8),
                             ("map_public_key", "ab" * 32)):
            old = self.document[field]
            self.document[field] = value
            self.write()
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.load()
            self.document[field] = old
        self.document["nodes"][0]["signed_map"] = self.packet[:-1].hex() + "00"
        self.write()
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_missing_duplicate_and_excess_nodes_refuse(self):
        node = deepcopy(self.document["nodes"][0])
        for nodes in ([], [node, node], [node] * 5):
            self.document["nodes"] = nodes
            self.write()
            with self.subTest(nodes=len(nodes)), self.assertRaises(TargetJournalError):
                self.load()

    def test_registry_changes_change_journal_inventory_identity(self):
        first = self.load_ok()
        # Local native binding changes cannot silently inherit an old journal.
        self.document["nodes"][0]["routes"][0]["tpg_lun"] = 11
        self.write()
        second = self.load_ok()
        self.assertNotEqual(first.inventory_digest, second.inventory_digest)
        from target_journal import TargetJournal
        with TargetJournal(self.directory, first.inventory_digest, initialize=True, owner_uid=os.geteuid()):
            pass
        with self.assertRaises(TargetJournalError):
            TargetJournal(self.directory, second.inventory_digest, owner_uid=os.geteuid())


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute verifier path")
    EXECUTABLE = sys.argv[1]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
