#!/usr/bin/env python3
"""MVP CI evidence must be complete and tied to the exact release commit.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""

import copy
import unittest

import mvp_ci


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.sha = "a" * 40
        self.run = {
            "id": 7,
            "head_sha": self.sha,
            "path": ".github/workflows/fast.yml",
            "event": "push",
            "status": "completed",
            "conclusion": "success",
        }
        self.jobs = [
            {"name": "build", "status": "completed", "conclusion": "success"},
            {"name": "test", "status": "completed", "conclusion": "success"},
        ]

    def validate(self, run=None, jobs=None):
        mvp_ci.validate_run(
            "fast.yml", self.sha, run or self.run,
            self.jobs if jobs is None else jobs, ["build", "test"]
        )

    def test_complete_exact_commit_passes(self):
        self.validate()

    def test_wrong_commit_or_workflow_fails(self):
        for field, value in [("head_sha", "b" * 40), ("path", "legacy.yml")]:
            run = dict(self.run, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(run=run)

    def test_incomplete_or_failed_run_fails(self):
        for field, value in [("status", "in_progress"), ("conclusion", "failure")]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(run=dict(self.run, **{field: value}))

    def test_pr_head_sha_does_not_prove_tested_checkout_sha(self):
        for event in ["pull_request", "pull_request_target", "workflow_run"]:
            run = dict(self.run, event=event)
            with self.subTest(event=event), self.assertRaises(ValueError):
                self.validate(run=run)
            with self.subTest(event=event), self.assertRaises(ValueError):
                mvp_ci.latest_run([run], self.sha)

    def test_missing_or_duplicate_job_fails(self):
        for jobs in [[], self.jobs[:1], self.jobs + self.jobs[:1]]:
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                self.validate(jobs=jobs)

    def test_docs_only_skips_cannot_qualify_release(self):
        for conclusion in ["skipped", "failure", "cancelled", "timed_out", None]:
            jobs = copy.deepcopy(self.jobs)
            jobs[1]["conclusion"] = conclusion
            with self.subTest(conclusion=conclusion), self.assertRaises(ValueError):
                self.validate(jobs=jobs)

    def test_latest_failure_cannot_fall_back_to_old_green(self):
        later = dict(self.run, id=8, conclusion="failure")
        selected = mvp_ci.latest_run([self.run, later], self.sha)
        self.assertEqual(selected["id"], 8)
        with self.assertRaises(ValueError):
            self.validate(run=selected)

    def test_no_exact_candidate_run_fails(self):
        with self.assertRaises(ValueError):
            mvp_ci.latest_run([self.run], "b" * 40)

    def test_policy_preserves_strict_gates_and_defers_only_old_fast_cases(self):
        policy = mvp_ci.load_policy()
        self.assertEqual(set(policy["required_workflows"]), {"fast.yml", "nightly.yml"})
        self.assertEqual(len(policy["required_workflows"]["fast.yml"]), 5)
        self.assertEqual(len(policy["required_workflows"]["nightly.yml"]), 2)
        self.assertEqual(len(policy["smoke_tests"]), 6)
        self.assertIn("t/031_undo_publication_refusal.pl", policy["smoke_tests"])
        self.assertEqual(
            {name.split("_")[0] for name in policy["historical_fast_tests"]},
            {"t/226", "t/273", "t/333"},
        )
        self.assertEqual(policy["historical_workflow"], "legacy-extended.yml")


if __name__ == "__main__":
    unittest.main()
