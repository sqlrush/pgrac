"""Local lifecycle doubles only; no server, startup or runtime PASS."""
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

import acceptance
from acceptance import Fixture


class UnitFixture(Fixture):
    def __init__(self, root):
        self.root, self.bindir = root, root / "bin"
        self.bindir.mkdir()
        (self.bindir / "postgres").write_bytes(b"unit executable identity")
        adapter = root / "adapter"
        adapter.write_bytes(b"unit adapter identity")
        self.driver = SimpleNamespace(executable=adapter)
        self.layout = dict(nodes=[dict(id=n, data_dir=str(root / f"node{n}"))
                                  for n in range(4)])
        self.sockets, self.calls = [], []
        self.started = False
        self.identities = ["123|202610220"] * 4
        self.modes = ["on|on|on"] * 4

    def call(self, op, **args):
        self.calls.append(op)
        if op == "fresh_init":
            for node in self.layout["nodes"]:
                data = Path(node["data_dir"])
                data.mkdir()
                (data / "postgresql.conf").write_text("# unit config\n")
        elif op == "root_observation":
            return dict(system_identifier="123", root_digest="a" * 64, generation=1)
        elif op != "shared_start":
            raise AssertionError(f"unexpected unit operation {op}")
        return {}

    def sql(self, node, query, db="postgres"):
        if "pg_control_system()" in query:
            return self.identities[node]
        if "current_setting" in query:
            return self.modes[node]
        if query == "SHOW cluster.node_id":
            return str(node)
        raise AssertionError(f"unexpected unit SQL {query}")


class CatalogPrecheckTest(unittest.TestCase):
    def fixture(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        return UnitFixture(Path(temp.name))

    def test_current_catalog_on_all_members_reaches_root_validation(self):
        fixture = self.fixture()
        fixture.init()
        self.assertEqual(fixture.calls, ["fresh_init", "shared_start", "root_observation"])
        self.assertEqual(fixture.identity["system_identifier"], "123")
        self.assertTrue((fixture.root / "identity.json").is_file())

    def test_homogeneous_legacy_catalog_is_rejected(self):
        fixture = self.fixture()
        fixture.identities = ["123|202609290"] * 4
        with self.assertRaisesRegex(AssertionError, "PRE2 format activation"):
            fixture.init()
        self.assertNotIn("root_observation", fixture.calls)

    def test_last_member_requires_the_exact_current_catalog(self):
        for catalog in ("202609290", "202610221", "", "unknown", "0202610220"):
            with self.subTest(catalog=catalog):
                fixture = self.fixture()
                fixture.identities[-1] = f"123|{catalog}"
                with self.assertRaisesRegex(AssertionError, "PRE2 format activation"):
                    fixture.init()
                self.assertTrue(fixture.started)
                self.assertNotIn("root_observation", fixture.calls)
                self.assertFalse((fixture.root / "identity.json").exists())

    def test_current_catalog_does_not_bypass_other_identity_gates(self):
        for identity in ("0|202610220", "456|202610220", "123", "123|202610220|extra"):
            with self.subTest(identity=identity):
                fixture = self.fixture()
                fixture.identities[-1] = identity
                with self.assertRaises((AssertionError, ValueError)):
                    fixture.init()
                self.assertNotIn("root_observation", fixture.calls)
        fixture = self.fixture()
        fixture.modes[-1] = "on|off|on"
        with self.assertRaises(AssertionError):
            fixture.init()
        self.assertNotIn("root_observation", fixture.calls)

    def test_source_macro_is_exact_and_can_advance(self):
        fixture = self.fixture()
        header = fixture.root / "catversion.h"
        header.write_text("#define CATALOG_VERSION_NO 209901010\n"
                          "#define CATALOG_VERSION_NO_PRIOR 1\n")
        self.assertEqual(acceptance.catalog_version(header), "209901010")

    def test_missing_invalid_or_ambiguous_source_macro_is_rejected(self):
        fixture = self.fixture()
        header = fixture.root / "catversion.h"
        for source in ("#define CATALOG_VERSION_NO_PRIOR 209901010\n",
                       "#define CATALOG_VERSION_NO 0\n",
                       "#define CATALOG_VERSION_NO 0209901010\n",
                       "#define CATALOG_VERSION_NO unknown\n",
                       "#define CATALOG_VERSION_NO 209901010\n" * 2,
                       "#define CATALOG_VERSION_NO unknown\n#define CATALOG_VERSION_NO 209901010\n"):
            with self.subTest(source=source):
                header.write_text(source)
                with self.assertRaisesRegex(ValueError, "invalid source catalog version"):
                    acceptance.catalog_version(header)
        with self.assertRaises(OSError):
            acceptance.catalog_version(fixture.root / "missing.h")


if __name__ == "__main__":
    unittest.main()
