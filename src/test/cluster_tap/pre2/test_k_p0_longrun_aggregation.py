"""Preserve error classification and allowances across evidence aggregation.

Uses the real producer and offline judge. Only the record threshold is
lowered so the overflow branch can be tested without a large log.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import p0_longrun_cycle as cycle  # noqa: E402
from p0_longrun_judge import judge  # noqa: E402
from test_p0_longrun_judge import build_run, clean_pieces  # noqa: E402

KNOWN = 'cluster TT slot allocator exhausted on segment 1 (48 slots full)'
UNKNOWN = 'cluster TT slot allocator exhausted on segment 1 (49 slots full)'
OTHER = 'cluster TT slot allocator exhausted on segment 2 (48 slots full)'


class AggregatedErrorIdentity(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.allow = self.root / 'allow.json'
        self.allow.write_text(json.dumps(dict(version=1, entries=[dict(
            id='one-known-message', reason='controlled classification fixture',
            message=r'^cluster TT slot allocator exhausted on segment 1 \(48 slots full\)$',
            sources=['client'])])), encoding='utf-8')

    def produced_run(self, name, last_message):
        root = self.root / name
        event = dict(source='client', node=0, connection=0, phase='load', stage='UPDATE',
                     severity='ERROR', sqlstate='XX000', message=KNOWN, detail=None,
                     hint=None, location=None, connection_lost=False,
                     backend_pid=123, row=1, unix=100.0)
        evidence = cycle.Evidence(root / 'producer', dict(cycle=1), [], root)
        try:
            # One full record, then two records through the production overflow
            # path. The total count remains three even when identities are lost.
            with patch.object(cycle, 'FULL_CLIENT_RECORDS', 1):
                evidence.client_events([event, event, dict(event, message=last_message)])
            self.assertEqual(evidence.client_error_count, 3)
        finally:
            evidence.close()
        retained = [json.loads(line) for line in
                    (root / 'producer/client-errors.jsonl').read_text().splitlines()]
        run = build_run(root / 'run', clean_pieces(), retained)
        journal_path = run / 'cycle1/evidence/steps.json'
        journal = json.loads(journal_path.read_text())
        load = next(step for step in journal['steps'] if step['name'] == 'load')
        load['detail']['client_errors'] = evidence.client_error_count
        journal_path.write_text(json.dumps(journal), encoding='utf-8')
        return run

    def test_unknown_numeric_variant_cannot_inherit_a_known_error_identity(self):
        # Digit normalization merges the literal 49 with the rule's literal 48.
        for permit in (False, True):
            with self.subTest(allow=permit):
                run = self.produced_run('unknown-%s' % permit, UNKNOWN)
                result = judge(run, self.allow if permit else None)
                self.assertEqual(result['verdict'], 'FAIL')
                self.assertFalse(result['zero_errors'])
                self.assertEqual(result['unclassified']['count'], 1)
                self.assertIn(UNKNOWN, result['unclassified']['first_raw'][0])
                self.assertEqual(result['totals']['allowed_records'], 2 if permit else 0)

    def test_unallowed_numeric_variant_cannot_inherit_another_messages_allowance(self):
        # Both messages have a real label, but only segment 1 is allowed.
        result = judge(self.produced_run('not-allowed', OTHER), self.allow)
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertFalse(result['zero_errors'])
        self.assertEqual(result['unclassified']['count'], 0)
        self.assertEqual(result['totals']['failing_client_records'], 1)
        self.assertEqual(result['totals']['allowed_records'], 2)
        self.assertEqual(result['cycles'][0]['client_records'], 3)

    def test_identical_allowed_messages_keep_their_full_occurrence_count(self):
        result = judge(self.produced_run('same-message', KNOWN), self.allow)
        self.assertEqual(result['verdict'], 'PASS')
        self.assertTrue(result['zero_errors'])
        self.assertEqual(result['unclassified']['count'], 0)
        self.assertEqual(result['totals']['allowed_records'], 3)
        self.assertEqual(result['cycles'][0]['client_records'], 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
