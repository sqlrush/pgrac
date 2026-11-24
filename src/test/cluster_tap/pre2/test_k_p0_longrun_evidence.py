"""Retained acceptance evidence must cover the whole requested workload.

These tests damage real temporary run files, then invoke the offline judge.
They do not start servers or certify native behavior.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from p0_longrun_judge import judge
from p0_longrun_report import render
from test_p0_longrun_judge import SPEC, build_run, clean_pieces


class CompleteEvidence(unittest.TestCase):
    def run_files(self, **changes: object) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return build_run(Path(temporary.name), clean_pieces(), [], spec=dict(SPEC, **changes))

    def edit(self, run: Path, name: str, change) -> None:
        path = run / name
        document = json.loads(path.read_text())
        change(document)
        path.write_text(json.dumps(document))

    def refused(self, run: Path, code: str = '') -> dict:
        try:
            result = judge(run)
        except Exception as error:
            self.fail('judge lost the failure report: %s: %s' % (type(error).__name__, error))
        self.assertEqual(result['verdict'], 'FAIL')
        if code:
            self.assertIn(code, [failure['code'] for failure in result['failures']])
        self.assertIn('结论：FAIL', render(result))
        return result

    def test_missing_required_files_cannot_reuse_journal_pass(self) -> None:
        for name in ('load.json', 'verify.json', 'client-errors.jsonl', 'progress.jsonl',
                     'ledger-node0.txt', 'ledger-node1.txt', 'ledger-node2.txt',
                     'ledger-node3.txt'):
            with self.subTest(file=name):
                run = self.run_files()
                (run / 'cycle1/evidence' / name).unlink()
                result = self.refused(run, 'EVIDENCE_MISSING')
                self.assertTrue(any(name in f.get('what', '') for f in result['failures']))

    def test_required_reconciliation_follows_crash_and_restart_plan(self) -> None:
        for name in ('reverify', 'reverify_again'):
            with self.subTest(step=name):
                run = self.run_files(crash=True, restart_verify=True)
                self.assertEqual(judge(run)['verdict'], 'PASS')
                (run / 'cycle1/evidence' / (name + '.json')).unlink()
                self.refused(run, 'EVIDENCE_MISSING')

    def test_all_members_must_appear_once_in_each_evidence_source(self) -> None:
        sources = [('steps.json', 'members', 'id'), ('load.json', 'nodes', 'node'),
                   ('verify.json', 'members', 'node')]
        for name, field, identity in sources:
            for members in ([], [0, 1, 2], [0, 1, 2, 2], [0, 1, 2, 7]):
                with self.subTest(file=name, members=members):
                    run = self.run_files()

                    def change(document):
                        original = document[field]
                        document[field] = [dict(original[min(i, 3)], **{identity: node})
                                           for i, node in enumerate(members)]

                    self.edit(run, 'cycle1/evidence/' + name, change)
                    # A short journal already failed the old member-count gate.
                    # Keep that as a negative control, independent of its label.
                    code = '' if name == 'steps.json' and len(members) < 4 \
                        else 'EVIDENCE_INVALID'
                    self.refused(run, code)

    def test_truncated_logs_cannot_hide_recorded_errors(self) -> None:
        run = self.run_files()
        (run / 'cycle1/cluster/node3.log').write_text('')
        result = self.refused(run, 'EVIDENCE_INVALID')
        self.assertTrue(any('node3.log' in f.get('what', '') for f in result['failures']))

    def test_requested_time_and_connection_count_must_be_proven(self) -> None:
        patches = [dict(ran_requested_time=False), dict(requested_seconds=1.0),
                   dict(clients_per_node=1), dict(rows=1)]
        for values in patches:
            with self.subTest(values=values):
                run = self.run_files()
                self.edit(run, 'cycle1/evidence/load.json', lambda d: d.update(values))
                self.refused(run, 'EVIDENCE_INVALID')
        run = self.run_files()
        self.edit(run, 'cycle1/evidence/load.json',
                  lambda d: d['nodes'][3].update(connections=1))
        self.refused(run, 'EVIDENCE_INVALID')

    def test_load_commit_total_must_agree_with_per_member_counts(self) -> None:
        run = self.run_files()
        self.edit(run, 'cycle1/evidence/load.json', lambda d: d.update(commits=1))
        self.refused(run, 'EVIDENCE_INVALID')

    def test_recorded_elapsed_time_must_support_the_requested_duration(self) -> None:
        for elapsed in (0, 1.0, float('nan'), float('inf'), '180', True):
            with self.subTest(elapsed=elapsed):
                run = self.run_files()
                self.edit(run, 'cycle1/evidence/load.json',
                          lambda d: d.update(elapsed_seconds=elapsed))
                self.refused(run, 'EVIDENCE_INVALID')

    def test_elapsed_time_allows_only_the_recorders_rounding_precision(self) -> None:
        run = self.run_files(seconds=180.0004)
        self.edit(run, 'cycle1/evidence/load.json', lambda d: d.update(elapsed_seconds=180.0))
        self.assertEqual(judge(run)['verdict'], 'PASS')

    def test_invalid_member_structures_still_produce_failure_summary(self) -> None:
        for name, field in (('steps.json', 'members'), ('load.json', 'nodes'),
                            ('verify.json', 'members')):
            for value in (None, [None], ['bad'], [dict(node=0)]):
                with self.subTest(file=name, value=value):
                    run = self.run_files()
                    self.edit(run, 'cycle1/evidence/' + name,
                              lambda d: d.update({field: value}))
                    self.refused(run, 'EVIDENCE_INVALID')

    def test_complete_member_ids_do_not_replace_required_member_fields(self) -> None:
        cases = [('steps.json', 'members', 'logfile'), ('load.json', 'nodes', 'commits'),
                 ('load.json', 'nodes', 'connections'), ('load.json', 'nodes', 'anomalies'),
                 ('verify.json', 'members', 'status')]
        for name, field, key in cases:
            with self.subTest(file=name, missing=key):
                run = self.run_files()
                self.edit(run, 'cycle1/evidence/' + name, lambda d: d[field][2].pop(key))
                self.refused(run, 'EVIDENCE_INVALID')

    def test_invalid_member_fields_do_not_break_summary_rendering(self) -> None:
        cases = [('steps.json', 'members', 'logfile', None),
                 ('steps.json', 'members', 'logfile', ''),
                 ('verify.json', 'members', 'status', None),
                 ('verify.json', 'members', 'status', ['PASS'])]
        for name, field, key, value in cases:
            with self.subTest(file=name, field=key, value=value):
                run = self.run_files()
                self.edit(run, 'cycle1/evidence/' + name,
                          lambda d: d[field][2].update({key: value}))
                self.refused(run)

    def test_requested_crash_requires_crash_evidence(self) -> None:
        run = self.run_files(crash=True)
        self.edit(run, 'cycle1/evidence/load.json', lambda d: d.update(crash=None))
        self.refused(run, 'EVIDENCE_INVALID')

    def test_wrong_reconciliation_step_is_refused(self) -> None:
        run = self.run_files()
        self.edit(run, 'cycle1/evidence/verify.json', lambda d: d.update(step='reverify'))
        self.refused(run, 'EVIDENCE_INVALID')

    def test_damaged_documents_still_produce_failure_summary(self) -> None:
        for name in ('load.json', 'verify.json', 'client-errors.jsonl'):
            for text in ('{"partial":', '[]'):
                with self.subTest(file=name, text=text):
                    run = self.run_files()
                    (run / 'cycle1/evidence' / name).write_text(text)
                    self.refused(run, 'EVIDENCE_INVALID')

    def test_invalid_client_counts_cannot_cancel_other_errors(self) -> None:
        for count in (0, -5, True, 1.5):
            with self.subTest(count=count):
                run = self.run_files()
                event = dict(source='client', node=0, phase='load', severity='ERROR',
                             sqlstate='XX000', message='unclassified client failure', count=count)
                (run / 'cycle1/evidence/client-errors.jsonl').write_text(json.dumps(event))
                self.refused(run, 'EVIDENCE_INVALID')

    def test_bad_client_record_does_not_hide_later_errors(self) -> None:
        run = self.run_files()
        event = dict(source='client', node=2, phase='load', severity='ERROR',
                     sqlstate='XX000', message='unclassified client failure')
        (run / 'cycle1/evidence/client-errors.jsonl').write_text('{partial\n'
                                                               + json.dumps(event) + '\n')
        result = self.refused(run, 'EVIDENCE_INVALID')
        self.assertEqual(result['unclassified']['count'], 1)
        self.assertEqual(result['totals']['failing_client_records'], 1)

    def test_reconciliation_pass_cannot_contradict_its_measurements(self) -> None:
        for values in (dict(violation_count=1), dict(rows=1)):
            with self.subTest(values=values):
                run = self.run_files()
                self.edit(run, 'cycle1/evidence/verify.json',
                          lambda d: d['members'][3].update(values))
                self.refused(run, 'EVIDENCE_INVALID')

    def test_optional_reconciliation_is_not_required_when_not_requested(self) -> None:
        run = self.run_files()
        self.assertFalse((run / 'cycle1/evidence/reverify.json').exists())
        self.assertFalse((run / 'cycle1/evidence/reverify_again.json').exists())
        self.assertEqual(judge(run)['verdict'], 'PASS')


class ClientEvidenceCounts(unittest.TestCase):
    def event(self, known: bool = False, **changes: object) -> dict:
        return dict(dict(source='client', node=0, connection=0, phase='load',
                         stage='CONNECT' if not known else 'UPDATE',
                         severity='CONNECTION' if not known else 'ERROR',
                         sqlstate=None if not known else 'XX000',
                         message='connection refused' if not known else
                                 'cluster TT status unknown for xid 4242',
                         detail=None, hint=None, location=None, backend_pid=None,
                         connection_lost=not known, row=None, unix=100.5), **changes)

    def scenario(self, events: list, **changes: object):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        run = build_run(Path(temporary.name), clean_pieces(), events, spec=dict(SPEC, **changes))
        allow = run / 'allow.json'
        allow.write_text(json.dumps(dict(version=1, entries=[dict(
            id='known-only', reason='classified-error control',
            message='^cluster TT status unknown for xid 4242$')])))
        return run, allow

    def assert_incomplete(self, run: Path, allow: Path) -> None:
        result = judge(run, allow)
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertFalse(result['zero_errors'])
        self.assertTrue(any(f['code'] == 'EVIDENCE_INVALID'
                            and 'client' in f.get('what', '') for f in result['failures']))

    def test_clearing_the_only_client_error_never_turns_it_into_zero_errors(self) -> None:
        for text in ('', '\n \n'):
            with self.subTest(text=text):
                run, allow = self.scenario([self.event()])
                journal = run / 'cycle1/evidence/steps.json'
                saved = json.loads(journal.read_text())
                self.assertEqual(next(s for s in saved['steps'] if s['name'] == 'load')
                                 ['detail']['client_errors'], 1)
                load = run / 'cycle1/evidence/load.json'
                summary = json.loads(load.read_text())
                summary['nodes'][0]['connect_failures'] = 1
                load.write_text(json.dumps(summary))
                self.assertEqual(judge(run)['unclassified']['count'], 1)
                (run / 'cycle1/evidence/client-errors.jsonl').write_text(text)
                self.assert_incomplete(run, allow)

    def test_deleting_one_whole_unknown_line_cannot_leave_only_an_allowed_error(self) -> None:
        known = self.event(known=True)
        run, allow = self.scenario([self.event(), known])
        self.assertEqual(judge(run, allow)['unclassified']['count'], 1)
        (run / 'cycle1/evidence/client-errors.jsonl').write_text(json.dumps(known) + '\n')
        self.assert_incomplete(run, allow)

    def test_aggregated_occurrences_count_as_records_and_cannot_be_truncated(self) -> None:
        event = self.event(known=True, count=3)
        run, allow = self.scenario([event])
        result = judge(run, allow)
        self.assertEqual(result['verdict'], 'PASS')
        self.assertEqual(result['totals']['allowed_records'], 3)
        (run / 'cycle1/evidence/client-errors.jsonl').write_text(
            json.dumps(dict(event, count=2)) + '\n')
        self.assert_incomplete(run, allow)

    def test_the_load_counter_does_not_cover_later_steps(self) -> None:
        run, allow = self.scenario([self.event(known=True, phase='drop')])
        self.assertEqual(judge(run, allow)['verdict'], 'PASS')

    def test_missing_or_invalid_producer_counter_is_not_zero(self) -> None:
        for value in ('missing', None, -1, False, True, 1.5, '0'):
            with self.subTest(counter=value):
                run, allow = self.scenario([])
                path = run / 'cycle1/evidence/steps.json'
                saved = json.loads(path.read_text())
                load = next(step for step in saved['steps'] if step['name'] == 'load')
                if value == 'missing':
                    load['detail'].pop('client_errors')
                else:
                    load['detail']['client_errors'] = value
                path.write_text(json.dumps(saved))
                self.assert_incomplete(run, allow)

    def test_planned_crash_disconnects_do_not_invent_client_errors(self) -> None:
        run, allow = self.scenario([], crash=True)
        path = run / 'cycle1/evidence/load.json'
        saved = json.loads(path.read_text())
        saved['nodes'][0]['injected_disconnects'] = 1
        path.write_text(json.dumps(saved))
        self.assertEqual(judge(run, allow)['verdict'], 'PASS')


if __name__ == '__main__':
    unittest.main(verbosity=2)
