#!/usr/bin/env python3
"""Unit tests for the Resource-X SEND-C1 9+2 generated gate."""

from __future__ import annotations

import importlib.util
import pathlib
import os
import shlex
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
CHECKER_PATH = ROOT / "src" / "tools" / "check_resource_x_send_c1.py"
SPEC = importlib.util.spec_from_file_location("resource_x_send_c1", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {CHECKER_PATH}")
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


HEADER = (
    "id\tname\troot\tproducer_chain\tconsumer_chain\tobservable\t"
    "positive_witness\tnegative_witness\tforbidden_symbol\tmutation_edge\t"
    "mutation_witness\tmutation_result\tpositive_assertion\tnegative_assertion\n"
)


class SendC1GateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tempdir.name)
        self.backend = self.root / "src" / "backend" / "cluster"
        self.tests = self.root / "src" / "test" / "cluster_unit"
        self.backend.mkdir(parents=True)
        self.tests.mkdir(parents=True)
        self.manifest = self.tests / "resource-x-send-c1.tsv"
        self.artifact = self.tests / "send-c1-9-plus-2.json"
        self._write_fixture()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _rows(self) -> list[list[str]]:
        rows = []
        for number in range(1, 12):
            rows.append(
                [
                    f"C1-{number}",
                    f"fixture-{number}",
                    f"root_{number}",
                    f"root_{number}>producer_{number}",
                    f"root_{number}>producer_{number}>consumer_{number}",
                    f"observable_{number}",
                    f"src/test/cluster_unit/test_send_c1.c::test_positive_{number}",
                    f"src/test/cluster_unit/test_send_c1.c::test_negative_{number}",
                    CHECKER.BOUND_ONLY_PROBE if number == 10 else "-",
                    f"producer_{number}>consumer_{number}",
                    "positive",
                    "false",
                    f"observable_{number}",
                    f"observable_{number}",
                ]
            )
        return rows

    def _write_manifest(self, rows: list[list[str]]) -> None:
        body = HEADER + "".join("\t".join(row) + "\n" for row in rows)
        self.manifest.write_text(body, encoding="utf-8")

    def _write_fixture(self) -> None:
        production = []
        tests = [
            "#define UT_TEST(name) static void name(void)",
            "#define UT_RUN(name) name()",
            "#define UT_ASSERT(value) ((void) (value))",
        ]
        runs = []
        for number in range(1, 12):
            production.extend(
                [
                    f"unsigned observable_{number};",
                    f"static void consumer_{number}(void) {{ observable_{number}++; }}",
                    f"static void producer_{number}(void) {{ consumer_{number}(); }}",
                    f"void root_{number}(void) {{ producer_{number}(); }}",
                ]
            )
            tests.extend(
                [
                    f"UT_TEST(test_positive_{number}) {{ UT_ASSERT(observable_{number} >= 0); root_{number}(); }}",
                    f"UT_TEST(test_negative_{number}) {{ UT_ASSERT(observable_{number} >= 0); consumer_{number}(); }}",
                ]
            )
            runs.extend(
                [
                    f"UT_RUN(test_positive_{number});",
                    f"UT_RUN(test_negative_{number});",
                ]
            )
        tests.append("int main(void) { " + " ".join(runs) + " return 0; }")
        (self.backend / "resource_x_fixture.c").write_text(
            "\n".join(production) + "\n", encoding="utf-8"
        )
        (self.tests / "test_send_c1.c").write_text(
            "\n".join(tests) + "\n", encoding="utf-8"
        )
        self._write_manifest(self._rows())

    def test_exact_eleven_rows_generate_and_verify_fresh_artifact(self) -> None:
        report = CHECKER.audit_product(self.root, self.manifest)
        self.artifact.write_text(CHECKER.canonical_json(report), encoding="utf-8")
        CHECKER.verify_artifact(report, self.artifact)
        self.assertEqual([row["id"] for row in report["rows"]], CHECKER.REQUIRED_IDS)

    def test_duplicate_row_id_is_rejected(self) -> None:
        rows = self._rows()
        rows[-1][0] = "C1-10"
        self._write_manifest(rows)
        with self.assertRaisesRegex(CHECKER.GateError, "duplicate row id"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_missing_or_unit_only_root_is_rejected(self) -> None:
        rows = self._rows()
        rows[0][2] = "test_positive_1"
        rows[0][3] = "test_positive_1>producer_1"
        rows[0][4] = "test_positive_1>producer_1>consumer_1"
        self._write_manifest(rows)
        with self.assertRaisesRegex(CHECKER.GateError, "production root"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_removed_direct_edge_is_rejected(self) -> None:
        source = self.backend / "resource_x_fixture.c"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "void root_1(void) { producer_1(); }",
                "void root_1(void) { }",
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CHECKER.GateError, "missing direct edge"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_parser_only_consumer_is_rejected(self) -> None:
        rows = self._rows()
        rows[0][4] = "root_1>producer_1>cluster_resource_x_wire_decode"
        self._write_manifest(rows)
        source = self.backend / "resource_x_fixture.c"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "static void producer_1(void) { consumer_1(); }",
                "static void cluster_resource_x_wire_decode(void) { }\n"
                "static void producer_1(void) { cluster_resource_x_wire_decode(); }",
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CHECKER.GateError, "parser-only consumer"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_forbidden_bound_only_wrapper_is_rejected(self) -> None:
        source = self.backend / "resource_x_fixture.c"
        source.write_text(
            source.read_text(encoding="utf-8")
            + f"void {CHECKER.BOUND_ONLY_PROBE}(void) {{ observable_10++; }}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CHECKER.GateError, "forbidden symbol"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_mutation_edge_must_be_one_of_the_row_direct_edges(self) -> None:
        rows = self._rows()
        rows[0][9] = "root_1>consumer_1"
        self._write_manifest(rows)
        with self.assertRaisesRegex(CHECKER.GateError, "mutation edge"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_named_behavioral_assertion_must_be_in_witness_body(self) -> None:
        rows = self._rows()
        rows[0][12] = "missing_positive_assertion"
        self._write_manifest(rows)
        with self.assertRaisesRegex(CHECKER.GateError, "behavioral assertion"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_witness_must_exist_and_be_registered(self) -> None:
        test_source = self.tests / "test_send_c1.c"
        test_source.write_text(
            test_source.read_text(encoding="utf-8").replace(
                "UT_RUN(test_negative_6);", ""
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CHECKER.GateError, "not registered"):
            CHECKER.audit_product(self.root, self.manifest)

    def test_stale_generated_artifact_is_rejected(self) -> None:
        report = CHECKER.audit_product(self.root, self.manifest)
        self.artifact.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(CHECKER.GateError, "stale generated artifact"):
            CHECKER.verify_artifact(report, self.artifact)

    def test_each_named_dynamic_witness_must_pass_in_its_binary(self) -> None:
        report = CHECKER.audit_product(self.root, self.manifest)
        output = "".join(
            f"ok {number * 2 - 1} - test_positive_{number}\n"
            f"ok {number * 2} - test_negative_{number}\n"
            for number in range(1, 12)
        )
        CHECKER.verify_witness_outputs(report, {"test_send_c1": output})
        with self.assertRaisesRegex(CHECKER.GateError, "dynamic witness did not pass"):
            CHECKER.verify_witness_outputs(
                report,
                {"test_send_c1": output.replace("ok 12 - test_negative_6\n", "")},
            )

    def test_edge_mutation_must_fail_its_bound_witness(self) -> None:
        report = CHECKER.audit_product(self.root, self.manifest)
        row = report["rows"][0]
        all_green = {
            "test_send_c1": "ok 1 - test_positive_1\nok 2 - test_negative_1\n"
        }
        with self.assertRaisesRegex(CHECKER.GateError, "mutation survived"):
            CHECKER.verify_mutation_outputs(row, all_green)
        CHECKER.verify_mutation_outputs(
            row,
            {
                "test_send_c1": (
                    "not ok 1 - test_positive_1\nok 2 - test_negative_1\n"
                )
            },
        )

    def test_mutation_compiles_with_separate_source_and_build_roots(self) -> None:
        compiler = os.environ.get("CC", "cc")
        recipes = {
            "pcm_lock": ("version", "resource_x_identity", "resource_x_node_wire"),
            "lms_outbound": ("version",),
            "gcs_block": ("version", "pcm_x_convert", "resource_x_identity",
                          "resource_x_retry"),
        }
        for separate in (True, False):
            with self.subTest(separate_build=separate):
                build_root = self.root / "build" if separate else self.root
                unit_build = build_root / "src/test/cluster_unit"
                backend_build = build_root / "src/backend/cluster"
                unit_build.mkdir(parents=True, exist_ok=True)
                backend_build.mkdir(parents=True, exist_ok=True)
                empty_source = self.root / "empty.c"
                empty_source.write_text("/* link-only dependency fixture */\n")
                empty_object = unit_build / "cluster_unit_port_stubs.o"
                subprocess.run(shlex.split(compiler) + ["-c", str(empty_source),
                               "-o", str(empty_object)], check=True, capture_output=True)
                for library in ("common/libpgcommon_srv.a", "port/libpgport_srv.a"):
                    archive = build_root / "src" / library
                    archive.parent.mkdir(parents=True, exist_ok=True)
                    subprocess.run(["ar", "rcs", str(archive), str(empty_object)],
                                   check=True, capture_output=True)
                (self.tests / "unit_test.h").write_text("int producer(void);\n")
                (unit_build / "generated_fixture.h").write_text("#define EXPECTED 0\n")
                for module, dependencies in recipes.items():
                    with self.subTest(module=module):
                        source_relative = f"src/backend/cluster/cluster_{module}.c"
                        (self.root / source_relative).write_text(
                            "static int consumer(void) { return 1; }\n"
                            "int producer(void) { return consumer(); }\n")
                        binary_name = f"test_cluster_{module}"
                        unit_text = '#include "unit_test.h"\n#include "generated_fixture.h"\n'
                        if module == "gcs_block":
                            unit_text += '#include GCS_BLOCK_SOURCE_PATH\n'
                        unit_text += "int main(void) { return producer() != EXPECTED; }\n"
                        (self.tests / f"{binary_name}.c").write_text(unit_text)
                        for dependency in dependencies:
                            (backend_build / f"cluster_{dependency}.o").write_bytes(
                                empty_object.read_bytes())
                        row = {
                            "id": "C1-1", "mutation_edge": ["producer", "consumer"],
                            "production_sources": {"producer": source_relative},
                            "mutation_witness": "positive", "mutation_result": "0",
                            "positive_binary": binary_name,
                        }
                        with tempfile.TemporaryDirectory() as temporary:
                            executable, actual_name, _ = CHECKER._compile_mutation_binary(
                                row, self.root, unit_build, pathlib.Path(temporary),
                                compiler, "", "", "", "")
                            self.assertEqual(actual_name, binary_name)
                            self.assertEqual(subprocess.run([str(executable)],
                                             capture_output=True).returncode, 0)


if __name__ == "__main__":
    unittest.main()
