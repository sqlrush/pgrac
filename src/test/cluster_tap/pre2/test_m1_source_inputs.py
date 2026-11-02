"""Captured controller-input checks; no database or native verdict is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import m1_regression as m1


class SourceInputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name)
        self.inputs = {}
        for name in ('src/test/cluster_tap/t/case.pl',
                     'src/test/cluster_tap/pre2/controller.py',
                     'src/test/perl/PostgreSQL/Test/Cluster.pm'):
            path = self.source/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(('captured input: '+name+'\n').encode())
            self.inputs[name] = hashlib.sha256(path.read_bytes()).hexdigest()

    def verify(self):
        check = getattr(m1, 'verify_source_inputs', None)
        self.assertTrue(callable(check), 'phase boundaries must verify captured source inputs')
        return check(self.source, self.inputs)

    def test_unchanged_inputs_pass_repeated_boundaries(self):
        self.verify()
        self.verify()

    def test_actual_child_changes_a_captured_input_between_boundaries(self):
        for name in self.inputs:
            with self.subTest(name=name):
                path = self.source/name
                original = path.read_bytes()
                expected = dict(self.inputs)
                self.verify()
                reply = subprocess.run([sys.executable, '-B', '-c',
                    'from pathlib import Path; import sys; '
                    'Path(sys.argv[1]).write_bytes(b"changed during phase")', str(path)],
                    check=True, capture_output=True)
                self.assertEqual(reply.returncode, 0)
                with self.assertRaisesRegex(ValueError, 'source input changed'):
                    self.verify()
                self.assertEqual(self.inputs, expected)
                path.write_bytes(original)
                self.verify()

    def test_changed_input_is_not_rebased_on_later_checks(self):
        name = next(iter(self.inputs))
        expected = dict(self.inputs)
        (self.source/name).write_bytes(b'changed before next phase')
        for _ in range(2):
            with self.assertRaisesRegex(ValueError, 'source input changed'):
                self.verify()
        self.assertEqual(self.inputs, expected)

    def test_deleted_input_cannot_pass(self):
        (self.source/next(iter(self.inputs))).unlink()
        with self.assertRaises((OSError, ValueError)):
            self.verify()

    def test_replaced_input_is_compared_to_original_bytes(self):
        name = next(iter(self.inputs))
        path = self.source/name
        original = path.read_bytes()
        replacement = self.source/'replacement'
        replacement.write_bytes(b'replaced captured source')
        replacement.replace(path)
        with self.assertRaisesRegex(ValueError, 'source input changed'):
            self.verify()
        # This contract binds bytes, not inode or an atomic filesystem view.
        replacement.write_bytes(original)
        replacement.replace(path)
        self.verify()

    def test_added_helpers_cannot_escape_the_original_inventory(self):
        expected = dict(self.inputs)
        for name in ('src/test/cluster_tap/pre2/nested/helper.py',
                     'src/test/perl/PostgreSQL/Test/Extra.pm'):
            with self.subTest(name=name):
                target = self.source/name
                target.parent.mkdir(parents=True, exist_ok=True)
                subprocess.run([sys.executable, '-B', '-c',
                    'from pathlib import Path; import sys; '
                    'Path(sys.argv[1]).write_bytes(b"new helper")', str(target)], check=True)
                with self.assertRaises(ValueError):
                    self.verify()
                self.assertEqual(self.inputs, expected)
                target.unlink()
                self.verify()

    def capture(self):
        capture = getattr(m1, 'capture_source_inputs', None)
        self.assertTrue(callable(capture), 'capture the original test helper trees')
        return capture(self.source)

    def test_capture_binds_both_test_trees_without_product_or_output_files(self):
        (self.source/'outside.c').write_bytes(b'not a test helper')
        (self.source/'results').mkdir()
        (self.source/'results/log').write_bytes(b'not an input')
        self.assertEqual(self.capture(), self.inputs)
        target = self.source/'src/test/cluster_tap/lib/Loader.pm'
        target.parent.mkdir()
        target.write_bytes(b'loader bytes')
        expected = dict(self.inputs)
        expected['src/test/cluster_tap/lib/Loader.pm'] = hashlib.sha256(b'loader bytes').hexdigest()
        self.assertEqual(self.capture(), expected)

    def test_symlink_and_nonregular_helpers_are_refused_before_reading(self):
        target = self.source/'src/test/cluster_tap/pre2/controller.py'
        original = target.read_bytes()
        outside = self.source/'outside.py'
        outside.write_bytes(original)
        target.unlink()
        target.symlink_to(outside)
        with self.assertRaises(ValueError):
            self.verify()
        target.unlink()
        target.write_bytes(original)
        tree = self.source/'src/test/perl'
        retained = self.source/'retained-perl'
        tree.rename(retained)
        tree.symlink_to(retained, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.capture()
        tree.unlink()
        retained.rename(tree)
        alias = tree/'alias'
        alias.symlink_to(tree/'PostgreSQL', target_is_directory=True)
        with self.assertRaises(ValueError):
            self.capture()
        alias.unlink()
        os.mkfifo(alias)
        with self.assertRaises(ValueError):
            self.capture()
        alias.unlink()
        self.verify()

    def test_missing_or_empty_helper_tree_cannot_supply_a_snapshot(self):
        tree = self.source/'src/test/perl'
        retained = self.source/'retained-perl'
        tree.rename(retained)
        with self.assertRaises((OSError, ValueError)):
            self.capture()
        tree.mkdir()
        with self.assertRaises(ValueError):
            self.capture()


class SourcePreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.source = root/'source'
        self.inputs = {}
        for name in ('src/test/cluster_tap/t/case.pl',
                     'src/test/perl/PostgreSQL/Test/ClusterPRE2.pm',
                     'src/test/cluster_tap/pre2/nested/helper.py',
                     *('src/test/cluster_tap/pre2/'+name for name in
                       ('m1_regression.py', 'run_legacy.py', 'run_multixact_lifecycle.py',
                        'run_single_shared_vacuum.py'))):
            path = self.source/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(name.encode())
            self.inputs[name] = hashlib.sha256(name.encode()).hexdigest()
        # These input bytes are never executed; preflight only checks files.
        for name in ('install/bin/postgres', 'build/src/test/regress/pg_regress',
                     'fixtures/bin/pg_config', 'install/lib/pgxs/src/test/regress/pg_regress'):
            path = root/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'preflight file input only')
            path.chmod(0o700)
        (root/'vendor').mkdir()
        self.args = argparse.Namespace(source=self.source, output=root/'output',
            install=root/'install', build=root/'build', fixture_install=root/'fixtures',
            vendor=root/'vendor', expected_postgres_sha256=hashlib.sha256(
                b'preflight file input only').hexdigest())
        self.selection = dict(phases=[dict(name='legacy', cases=['t/case.pl'])])

    def test_preflight_captures_helpers_and_preserves_required_case_check(self):
        self.assertEqual(m1.preflight(self.args, self.selection), self.inputs)
        self.assertFalse(self.args.output.exists())
        (self.source/'src/test/cluster_tap/t/case.pl').unlink()
        with self.assertRaises((OSError, ValueError)):
            m1.preflight(self.args, self.selection)

    def test_output_inside_source_is_refused_without_creating_it(self):
        self.args.output = self.source/'src/test/cluster_tap/results'
        with self.assertRaises(ValueError):
            m1.preflight(self.args, self.selection)
        self.assertFalse(self.args.output.exists())


if __name__ == '__main__':
    unittest.main()
