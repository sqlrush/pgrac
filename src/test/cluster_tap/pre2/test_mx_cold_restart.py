"""Cold-copy fault boundaries; these checks do not replace native TAP448.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import json
from pathlib import Path
import tempfile
import unittest

import mx_cold_restart as cold
from mx_cold_copy import sha


class ClearedVoteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.saved = self.root / 'saved'
        self.saved.mkdir()
        self.offset = 5 * 128 * 512
        self.images = []
        for index in range(3):
            image = self.saved / f'vote{index}.image'
            image.write_bytes(bytes([index + 1]) * (self.offset + 1024))
            self.images.append(dict(path=str(image), sha256=sha(image),
                                    size=image.stat().st_size))
        self.capture = self.saved / 'capture.json'
        self.capture.write_text(json.dumps(dict(votes=self.images)))

    def result(self, faults):
        names = {'global/' + name: name for name in cold.IMAGE_NAMES}
        return dict(refused=True, all_stopped=True, before=names, after=names,
                    nodes=[dict(configuration_mismatch=False, fatal=1,
                                limit_refusal=True, proven=0, ready=False)],
                    vote_faults=faults)

    def test_only_copied_activation_sector_changes(self):
        faults = cold.copy_cleared_votes(self.capture, self.root / 'fault')
        self.assertEqual(len(faults), 3)
        for original, fault in zip(self.images, faults):
            before = Path(original['path']).read_bytes()
            after = Path(fault['copy']).read_bytes()
            self.assertEqual(sha(original['path']), original['sha256'])
            self.assertEqual(after[:self.offset], before[:self.offset])
            self.assertEqual(after[self.offset:self.offset + 512], bytes(512))
            self.assertEqual(after[self.offset + 512:], before[self.offset + 512:])
        cold.verify(self.result(faults))

    def test_changed_capture_is_rejected_before_copying(self):
        Path(self.images[2]['path']).write_bytes(b'changed')
        destination = self.root / 'fault'
        with self.assertRaisesRegex(ValueError, 'saved voting image'):
            cold.copy_cleared_votes(self.capture, destination)
        self.assertFalse(destination.exists())

    def test_existing_destination_cannot_overwrite_originals(self):
        with self.assertRaisesRegex(ValueError, 'new'):
            cold.copy_cleared_votes(self.capture, self.saved)
        for original in self.images:
            self.assertEqual(sha(original['path']), original['sha256'])

    def test_missing_fault_receipts_cannot_pass(self):
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            cold.verify(self.result([]))

    def test_nonzero_copied_sector_cannot_pass(self):
        faults = cold.copy_cleared_votes(self.capture, self.root / 'fault')
        with Path(faults[0]['copy']).open('r+b') as image:
            image.seek(self.offset)
            image.write(b'x')
        with self.assertRaises((AssertionError, ValueError)):
            cold.verify(self.result(faults))

    def test_original_change_cannot_pass(self):
        faults = cold.copy_cleared_votes(self.capture, self.root / 'fault')
        Path(self.images[0]['path']).write_bytes(b'changed')
        with self.assertRaises((AssertionError, ValueError)):
            cold.verify(self.result(faults))


if __name__ == '__main__':
    unittest.main()
