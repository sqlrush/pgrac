"""Captured controller-input checks; no database or native verdict is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import hashlib
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
                     'src/test/cluster_tap/pre2/controller.py'):
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
        with self.assertRaises(OSError):
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


if __name__ == '__main__':
    unittest.main()
