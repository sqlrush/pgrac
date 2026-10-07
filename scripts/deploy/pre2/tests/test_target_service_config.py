"""Protected target listener configuration, without network/target actions.

Author: SqlRush <sqlrush@gmail.com>
"""

from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
try:
    from target_service import load_service_config
except ModuleNotFoundError:
    load_service_config = None


class ServiceConfigTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(callable(load_service_config), "protected target service configuration is missing")
        temporary = tempfile.TemporaryDirectory(prefix="pgrac-service-config-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.path = self.directory / "service.json"
        self.document = {"version": 1, "listen_address": "127.0.0.1", "listen_port": 19463,
                         "state_directory": str(self.directory / "state"),
                         "registry_path": str(self.directory / "registry.json"),
                         "template_path": str(self.directory / "closed-template.json"),
                         "tls": {"ca_certificate": str(self.directory / "ca.pem"),
                                 "certificate": str(self.directory / "server.pem"),
                                 "private_key": str(self.directory / "server.key")},
                         "peer_pins": ["ab" * 32], "target_boot_id": "bc" * 16,
                         "command_timeout_ms": 120000, "service_mode": "normal"}
        self.write()

    def write(self):
        self.path.write_text(json.dumps(self.document))
        self.path.chmod(0o600)

    def load(self):
        return load_service_config(str(self.path), owner_uid=os.geteuid())

    def test_explicit_config_becomes_typed_frozen_owner_inputs(self):
        config = self.load()
        self.assertEqual(config.listen_address, "127.0.0.1")
        self.assertEqual(config.listen_port, 19463)
        self.assertEqual(config.peer_pins, ("ab" * 32,))
        self.assertEqual(config.target_boot_id, "bc" * 16)
        self.assertEqual(config.command_timeout_ms, 120000)
        self.assertEqual(config.tls.private_key, str(self.directory / "server.key"))
        self.assertEqual(config.tls.owner_uid, os.geteuid())
        self.assertEqual(config.state_directory, str(self.directory / "state"))
        self.assertEqual(config.service_mode, "normal")
        self.document["service_mode"] = "closed-reconcile"
        self.write()
        self.assertEqual(self.load().service_mode, "closed-reconcile")

    def test_bad_network_identity_or_envelope_never_defaults(self):
        original = deepcopy(self.document)
        for key, value in (("listen_address", "localhost"), ("listen_address", "0.0.0.0"),
                           ("listen_address", "224.1.1.1"), ("listen_address", "::1"),
                           ("listen_port", 0), ("listen_port", 65536), ("listen_port", True),
                           ("command_timeout_ms", 0), ("command_timeout_ms", 600001),
                           ("command_timeout_ms", True), ("version", True),
                           ("target_boot_id", "00" * 16), ("target_boot_id", "bad"),
                           ("peer_pins", []), ("peer_pins", ["ab" * 32] * 2),
                           ("peer_pins", ["00" * 32]), ("peer_pins", "ab" * 32),
                           ("service_mode", True), ("service_mode", "auto")):
            self.document = {**original, key: value}
            self.write()
            with self.subTest(key=key, value=value), self.assertRaises(TargetJournalError):
                self.load()

    def test_missing_duplicate_unknown_fields_refuse(self):
        for key in tuple(self.document):
            original = deepcopy(self.document)
            del self.document[key]
            self.write()
            with self.subTest(key=key), self.assertRaises(TargetJournalError):
                self.load()
            self.document = original
        self.document["shell_command"] = "not-allowed"
        self.write()
        with self.assertRaises(TargetJournalError):
            self.load()
        self.path.write_text('{"version":1,"version":1}')
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_unsafe_paths_and_tls_fields_refuse(self):
        original = deepcopy(self.document)
        for key in ("state_directory", "registry_path", "template_path"):
            for path in ("relative", "/tmp/../other", "/", "//alternate", "/tmp/x\0y"):
                self.document = {**original, key: path}
                self.write()
                with self.subTest(key=key, path=path), self.assertRaises(TargetJournalError):
                    self.load()
        self.document = original
        for tls in ({}, {**original["tls"], "password": "secret"},
                    {**original["tls"], "private_key": "relative"}):
            self.document["tls"] = tls
            self.write()
            with self.assertRaises(TargetJournalError):
                self.load()

    def test_world_readable_or_symlink_config_refuses_without_echo(self):
        self.path.chmod(0o644)
        with self.assertRaises(TargetJournalError) as caught:
            self.load()
        self.assertEqual(str(caught.exception), "TARGET_SERVICE_CONFIG")
        self.path.chmod(0o600)
        original = self.directory / "real.json"
        self.path.rename(original)
        self.path.symlink_to(original)
        with self.assertRaises(TargetJournalError):
            self.load()


if __name__ == "__main__":
    unittest.main()
