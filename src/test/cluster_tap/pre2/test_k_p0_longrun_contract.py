"""Contract checks for error classification and multiple normal-stop cycles.

These tests judge file fixtures, never start a server, and do not certify a
native run. PGRAC_LONGRUN_SOURCE can select the driver tree to be composed;
the default is this test's own directory.

Author: SqlRush <sqlrush@gmail.com>
IDENTIFICATION: src/test/cluster_tap/pre2/test_k_p0_longrun_contract.py
NOTES: Expected verdicts and stop counts are independent contract literals.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
DRIVER = Path(os.environ.get('PGRAC_LONGRUN_SOURCE', str(HERE))).resolve()
sys.path.insert(0, str(DRIVER))

from p0_longrun_judge import judge  # noqa: E402
from test_p0_longrun_judge import UNKNOWN, build_run, clean_pieces  # noqa: E402


class AcceptanceContract(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'run'
        self.root.mkdir()

    def unknown_with_allow(self, source: str) -> dict:
        pieces, clients = clean_pieces(), []
        if source == 'server':
            pieces['node1.load'] = (
                '2026-10-24 11:08:40.000 CST [8124] ERROR:  XX000: %s\n' % UNKNOWN)
        else:
            clients.append(dict(
                source='client', node=2, connection=0, phase='load', stage='UPDATE',
                severity='ERROR', sqlstate='XX000', message=UNKNOWN, detail=None,
                hint=None, location=None, connection_lost=False, backend_pid=8124,
                row=7, unix=101.0))
        run = build_run(self.root, pieces, clients)
        allow = self.root / 'allow.json'
        allow.write_text(json.dumps(dict(version=1, entries=[dict(
            id='unclassified', reason='contract negative case',
            message='^sample failure 4242 that no rule of the table quotes$',
            sources=[source], severities=['ERROR'])])), encoding='utf-8')
        return judge(run, allow)

    def assert_unknown_failed(self, result: dict, source: str) -> None:
        self.assertEqual(result['sections']['normal_stop'], dict(requested=1, passed=1))
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertFalse(result['zero_errors'])
        self.assertEqual(result['unclassified']['count'], 1)
        self.assertEqual(result['totals']['failing_' + source + '_records'], 1)
        self.assertEqual(result['totals']['allowed_records'], 0)
        self.assertIn(UNKNOWN, result['unclassified']['first_raw'][0])

    def test_unknown_server_error_is_not_excused_by_allow_list(self) -> None:
        self.assert_unknown_failed(self.unknown_with_allow('server'), 'server')

    def test_unknown_client_error_is_not_excused_by_allow_list(self) -> None:
        self.assert_unknown_failed(self.unknown_with_allow('client'), 'client')

    def three_cycles(self) -> Path:
        run = build_run(self.root, clean_pieces(), [])
        plan_path = run / 'plan.json'
        plan = json.loads(plan_path.read_text())
        first = plan['cycles'][0]
        plan['cycles'] = [dict(first, cycle=1), dict(first, cycle=2), dict(first, cycle=3)]
        plan_path.write_text(json.dumps(plan), encoding='utf-8')
        for number in (2, 3):
            cycle = run / ('cycle%d' % number)
            shutil.copytree(run / 'cycle1', cycle)
            journal_path = cycle / 'evidence/steps.json'
            journal = json.loads(journal_path.read_text())
            journal['cycle'] = number
            journal_path.write_text(json.dumps(journal), encoding='utf-8')
        return run

    def test_three_complete_normal_stops_are_all_counted(self) -> None:
        result = judge(self.three_cycles())
        self.assertEqual(result['verdict'], 'PASS')
        self.assertEqual(len(result['cycles']), 3)
        self.assertEqual(result['sections']['normal_stop'], dict(requested=3, passed=3))

    def test_failed_third_stop_cannot_reuse_two_successful_cycles(self) -> None:
        run = self.three_cycles()
        path = run / 'cycle3/evidence/steps.json'
        journal = json.loads(path.read_text())
        stop = next(step for step in journal['steps'] if step['name'] == 'stop')
        stop.update(status='FAIL', error='normal stop refused')
        path.write_text(json.dumps(journal), encoding='utf-8')
        result = judge(run)
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertEqual(result['sections']['normal_stop'], dict(requested=3, passed=2))
        self.assertTrue(any(failure.get('cycle') == 3 and failure.get('step') == 'stop'
                            for failure in result['failures']))

    def test_missing_third_stop_is_not_a_success(self) -> None:
        run = self.three_cycles()
        path = run / 'cycle3/evidence/steps.json'
        journal = json.loads(path.read_text())
        journal['steps'] = [step for step in journal['steps'] if step['name'] != 'stop']
        path.write_text(json.dumps(journal), encoding='utf-8')
        result = judge(run)
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertEqual(result['sections']['normal_stop'], dict(requested=3, passed=2))
        self.assertTrue(any(failure.get('cycle') == 3 and failure.get('status') == 'NOT_RUN'
                            for failure in result['failures']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
