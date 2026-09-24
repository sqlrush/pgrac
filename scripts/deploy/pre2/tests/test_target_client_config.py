"""Protected provider-client configuration, independent of native power/storage.

Author: SqlRush <sqlrush@gmail.com>
"""

import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
try:
    from target_client_runtime import load_client_config
except ImportError:
    load_client_config = None


class ClientConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="pgrac-client-config-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.path = self.directory / "client.json"
        self.document = dict(version=1, registry_path=str(self.directory / "registry.json"),
            endpoint=dict(address="127.0.0.1", port=45678, server_name="target.example",
                          certificate_sha256="ab" * 32, inventory_digest="cd" * 32),
            tls=dict(ca_certificate=str(self.directory / "server.pem"),
                     certificate=str(self.directory / "client.pem"),
                     private_key=str(self.directory / "client.key")))
        self.write()

    def write(self):
        self.path.write_text(json.dumps(self.document))
        self.path.chmod(0o600)

    def load(self):
        self.assertTrue(callable(load_client_config), "protected client configuration is missing")
        return load_client_config(str(self.path), owner_uid=os.geteuid())

    def test_exact_configuration_preserves_endpoint_and_credential_owner(self):
        config = self.load()
        self.assertEqual(config.endpoint.address, "127.0.0.1")
        self.assertEqual(config.endpoint.port, 45678)
        self.assertEqual(config.endpoint.server_name, "target.example")
        self.assertEqual(config.registry_path, str(self.directory / "registry.json"))
        self.assertEqual(config.tls.owner_uid, os.geteuid())

    def test_schema_never_allows_request_selected_route_or_endpoint(self):
        self.load()
        original = copy.deepcopy(self.document)
        for key, value in (("route_count", 1), ("retry", True), ("command", "anything")):
            self.document = {**original, key: value}
            self.write()
            with self.subTest(key=key), self.assertRaises(TargetJournalError):
                self.load()
        for key, value in (("address", "localhost"), ("address", "0.0.0.0"),
                           ("address", "224.0.0.1"), ("address", "255.255.255.255"),
                           ("port", True), ("port", 0), ("port", 65536),
                           ("server_name", "name\nprivate"), ("server_name", "a..b"),
                           ("certificate_sha256", "00" * 32), ("inventory_digest", "A" * 64)):
            self.document = copy.deepcopy(original)
            self.document["endpoint"][key] = value
            self.write()
            with self.subTest(key=key, value=value), self.assertRaises(TargetJournalError):
                self.load()

    def test_duplicate_unknown_keys_and_path_escape_refuse(self):
        self.load()
        original = copy.deepcopy(self.document)
        for key in ("registry_path", "private_key"):
            self.document = copy.deepcopy(original)
            if key == "registry_path":
                self.document[key] = "/a/../b"
            else:
                self.document["tls"][key] = "relative"
            self.write()
            with self.assertRaises(TargetJournalError):
                self.load()
        self.document = original
        self.write()
        self.path.write_text(self.path.read_text().replace('"version": 1', '"version": 1,"version": 1'))
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_unsafe_files_and_parent_refuse_without_disclosing_paths(self):
        self.load()
        for mode in (0o644, 0o640, 0o400):
            self.path.chmod(mode)
            with self.assertRaisesRegex(TargetJournalError, "^TARGET_CLIENT_CONFIG$"):
                self.load()
        self.path.chmod(0o600)
        alias = self.directory / "alias"
        os.link(self.path, alias)
        with self.assertRaises(TargetJournalError):
            self.load()
        alias.unlink()
        self.directory.chmod(0o777)
        try:
            with self.assertRaises(TargetJournalError):
                self.load()
        finally:
            self.directory.chmod(0o700)


if __name__ == "__main__":
    unittest.main()
