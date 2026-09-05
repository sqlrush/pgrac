#!/usr/bin/env python3
"""Negative and positive tests for the R11 source-removal census."""

from __future__ import annotations

import copy
import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
TOOL = ROOT / "src" / "tools" / "check_r11_source_removal_census.py"
MANIFEST = (
    ROOT
    / "src"
    / "test"
    / "cluster_unit"
    / "data"
    / "r11-source-removal-census-v1.json"
)


def canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


class R11SourceRemovalCensusTests(unittest.TestCase):
    def run_tool(self, manifest: pathlib.Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(TOOL),
                "--source-root",
                str(ROOT),
                "--manifest",
                str(manifest),
            ],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def mutated_manifest(self, mutate) -> pathlib.Path:
        value = json.loads(MANIFEST.read_text(encoding="utf-8"))
        mutate(value)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = pathlib.Path(directory.name) / "manifest.json"
        path.write_text(canonical_json(value), encoding="utf-8")
        return path

    def test_exact_manifest_is_green(self) -> None:
        result = self.run_tool(MANIFEST)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("R11 source-removal census: GREEN", result.stdout)

    def test_schema_tamper_fails_closed(self) -> None:
        path = self.mutated_manifest(
            lambda value: value.__setitem__("schema_version", "stale")
        )
        self.assertNotEqual(self.run_tool(path).returncode, 0)

    def test_l1_status_cannot_be_backfilled(self) -> None:
        path = self.mutated_manifest(
            lambda value: value["gates"]["L1"].__setitem__("status", "UNKNOWN")
        )
        self.assertNotEqual(self.run_tool(path).returncode, 0)

    def test_writer_layer_cannot_drop_a_forbidden_pattern(self) -> None:
        path = self.mutated_manifest(
            lambda value: value["layers"]["writer_root"][
                "forbidden_patterns"
            ].pop()
        )
        self.assertNotEqual(self.run_tool(path).returncode, 0)

    def test_l3_checkpoint_identity_is_exact(self) -> None:
        path = self.mutated_manifest(
            lambda value: value["gates"]["L3"].__setitem__(
                "checkpoint_commit", "0" * 40
            )
        )
        self.assertNotEqual(self.run_tool(path).returncode, 0)

    def test_l3_anchor_hash_is_not_advisory(self) -> None:
        def mutate(value: dict) -> None:
            anchors = copy.deepcopy(value["gates"]["L3"]["anchors"])
            anchors[0]["blob_sha256"] = "0" * 64
            value["gates"]["L3"]["anchors"] = anchors

        path = self.mutated_manifest(mutate)
        self.assertNotEqual(self.run_tool(path).returncode, 0)

    def test_current_product_snapshot_identity_is_exact(self) -> None:
        path = self.mutated_manifest(
            lambda value: value["current_product_snapshot"].__setitem__(
                "sha256", "0" * 64
            )
        )
        self.assertNotEqual(self.run_tool(path).returncode, 0)


if __name__ == "__main__":
    unittest.main()
