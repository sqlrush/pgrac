"""Frozen voting image checks; unit fixtures never authorize device writes.

Author: SqlRush <sqlrush@gmail.com>
"""

import copy
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError
from test_profile import fixture
from voting import IMAGE_BYTES, canonical_image, check_image, make_plan, publish_image


TOOLS = Path(__file__).resolve().parents[1]
ROOT = TOOLS.parents[2]


class VotingImageTests(unittest.TestCase):
    def test_all_three_images_match_existing_perl_formatter_byte_for_byte(self):
        with tempfile.TemporaryDirectory(prefix="pre1-vote-fixture-") as temp:
            for index in range(3):
                path = Path(temp) / str(index)
                subprocess.run([
                    "perl", "-I", str(ROOT / "src/test/perl"),
                    "-MPostgreSQL::Test::ClusterVotingDisk=format_voting_file",
                    "-e", "format_voting_file($ARGV[0], $ARGV[1], 128)", str(path), str(index)
                ], check=True, capture_output=True)
                actual = canonical_image(index)
                self.assertEqual(len(actual), 525824)
                self.assertEqual(actual, path.read_bytes())
                self.assertEqual(check_image(actual, index)["state"], "FRESH_INITIAL_IMAGE")

    def test_index_is_exact_integer_in_three_disk_set(self):
        for value in (-1, 3, True, False, 0.0, "0", None):
            for function in (canonical_image, lambda v: check_image(bytes(IMAGE_BYTES), v)):
                with self.subTest(value=value):
                    with self.assertRaises(PreflightError):
                        function(value)

    def test_exact_length_and_full_member_and_tail_validation(self):
        good = canonical_image(1)
        for image in (good[:-1], good + b"\0", bytes(IMAGE_BYTES)):
            with self.assertRaises(PreflightError):
                check_image(image, 1)
        for node in range(128):
            bad = bytearray(good)
            bad[node * 512 + 56] ^= 1
            with self.assertRaises(PreflightError):
                check_image(bad, 1)
        for offset in (128 * 512, IMAGE_BYTES - 1):
            bad = bytearray(good)
            bad[offset] = 1
            with self.assertRaises(PreflightError):
                check_image(bad, 1)

    def test_wrong_identity_and_valid_crc_live_state_not_fresh(self):
        from voting import crc32c
        for offset, value in ((0, 0), (4, 2), (8, 1), (48, 2), (16, 1), (40, 1)):
            image = bytearray(canonical_image(0))
            struct.pack_into("<I", image, offset, value)
            struct.pack_into("<I", image, 508, crc32c(image[:508]))
            with self.assertRaises(PreflightError):
                check_image(image, 0)

    def test_image_publish_never_replaces_existing_path_or_symlink(self):
        with tempfile.TemporaryDirectory(prefix="pre1-vote-output-") as temp:
            path = Path(temp) / "fresh.img"
            digest = publish_image(path, 0)
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(PreflightError):
                publish_image(path, 1)
            alias = Path(temp) / "alias"
            alias.symlink_to(path)
            with self.assertRaises(PreflightError):
                publish_image(alias, 1)
            self.assertEqual(path.read_bytes(), canonical_image(0))


class VotingPlanTests(unittest.TestCase):
    def test_plan_is_read_only_and_never_claims_media_formatted(self):
        profile = fixture()
        saved = copy.deepcopy(profile)
        plan = make_plan(profile)
        self.assertEqual(profile, saved)
        self.assertEqual(plan["status"], "PLANNED_NOT_EXECUTED")
        self.assertFalse(plan["deployment_qualified"])
        self.assertFalse(plan["device_writes_enabled"])
        self.assertEqual(plan["image_bytes"], 525824)
        self.assertEqual([v["index"] for v in plan["votes"]], [0, 1, 2])
        for vote in plan["votes"]:
            self.assertEqual(vote["image_sha256"], hashlib.sha256(canonical_image(vote["index"])).hexdigest())
        self.assertIn("FOUR_GUEST_DIRECT_READBACK", plan["pending"])
        self.assertIn("ALL_DATABASES_STOPPED", plan["pending"])

    def test_plan_rejects_nonfresh_or_short_or_unaligned_vote(self):
        for kind in ("nonfresh", "short", "unaligned"):
            profile = fixture()
            wwid = profile["votes"][0]["wwid"]
            allowed = next(v for v in profile["authorization"]["device_allowlist"] if v["wwid"] == wwid)
            if kind == "nonfresh":
                allowed["fresh"] = False
            else:
                size = 65536 if kind == "short" else 16777217
                profile["votes"][0]["size"] = allowed["size"] = size
            with self.assertRaises(PreflightError):
                make_plan(profile)

    def test_cli_rejects_bad_image_and_preserves_old_outputs(self):
        with tempfile.TemporaryDirectory(prefix="pre1-vote-cli-") as temp:
            path = Path(temp) / "fresh.img"
            argv = [sys.executable, "-B", str(TOOLS / "voting.py"), "image", "--index", "0", "--out", str(path)]
            result = subprocess.run(argv, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], "IMAGE_CREATED")
            result = subprocess.run(argv, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(path.read_bytes(), canonical_image(0))
            result = subprocess.run([sys.executable, "-B", str(TOOLS / "voting.py"),
                                     "check-image", "--index", "1", "--image", str(path)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout)["status"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
