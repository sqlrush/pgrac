"""Commit-ledger reader checks; literal input files provide no native credit.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import m1_soak_ledger as ledger


class SoakLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def file(self, name, content):
        raw = content.encode() if isinstance(content, str) else content
        path = self.root/name
        path.write_bytes(raw)
        return dict(path=name, size_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())

    def capture(self, clients=16):
        summary = (f'number of clients: {clients}\nnumber of threads: 2\nquery mode: simple\n'
                   'maximum number of tries: 1\nduration: 3600 s\n'
                   f'number of transactions actually processed: {2*clients}\n'
                   'number of failed transactions: 0 (0.000%)\n')
        manifest = dict(version=1, clients=clients, duration_seconds=3600, pgbench_rc=0,
            workload=self.file('workload.sql', Path(ledger.__file__).with_name(
                'm1_soak.sql').read_bytes()),
            summary=self.file('summary.txt', summary), stderr=self.file('stderr.txt', ''),
            logs=[], before=[], after=[])
        # Client assignment is fixed per thread, as in actual pgbench doLog.
        for thread in range(2):
            rows = [f'{client} {seq} 11 0 1700000000 {seq}\n'
                    for seq in (1, 2) for client in range(thread, clients, 2)]
            manifest['logs'].append(self.file(f'pgbench_log.321.{thread}', ''.join(rows)))
        for node in range(4):
            manifest['before'].append(self.file(f'before-{node}.tsv', ''.join(
                f'{client}|0\n' for client in range(64))))
            manifest['after'].append(self.file(f'after-{node}.tsv', ''.join(
                f'{client}|{2 if client < clients else 0}\n' for client in range(64))))
        return manifest

    def check(self, manifest):
        path = self.root/'manifest.json'
        path.write_text(json.dumps(manifest))
        return ledger.read_ledger(path)

    def test_raw_ledger_matches_all_members_at_each_client_count(self):
        for clients in (16, 48, 64):
            with self.subTest(clients=clients):
                result = self.check(self.capture(clients))
                self.assertTrue(result['workload_ledger_passed'])
                self.assertEqual(result['commits_by_client'], [2]*clients)
                self.assertEqual(result['total_commits'], 2*clients)
                self.assertEqual(result['p07'], 'NOT_READY')
                self.assertEqual(result['stage_acceptance'], 'NOT_READY')
                self.assertIn('actual elapsed duration', result['missing_gates'])

    def test_gaps_duplicates_failures_and_nonraw_records_refuse(self):
        for replacement in ('0 0 11 0 1700000000 1\n',
                            '0 3 11 0 1700000000 1\n',
                            '0 1 failed 0 1700000000 1\n',
                            '0 1 skipped 0 1700000000 1\n',
                            '0 1 11 1 1700000000 1\n',
                            '16 1 11 0 1700000000 1\n',
                            '0 1 11 0 1700000000 1000000\n',
                            '0 1 11 0 1700000000 1 0\n',
                            '0 1 11 0 1700000000 1\n0 1 11 0 1700000000 2\n'):
            manifest = self.capture()
            manifest['logs'][0] = self.file('bad-log.txt', replacement)
            with self.subTest(record=replacement), self.assertRaises(ValueError):
                self.check(manifest)

    def test_client_cannot_span_logs_or_disappear_behind_summary(self):
        for mode in ('duplicate-file-client', 'missing-client', 'empty'):
            manifest = self.capture()
            if mode == 'duplicate-file-client':
                manifest['logs'][1] = self.file('another-log', '0 3 11 0 1700000000 3\n')
            elif mode == 'missing-client':
                lines = (self.root/manifest['logs'][0]['path']).read_text().splitlines(True)
                manifest['logs'][0] = self.file('missing-client.log',
                    ''.join(line for line in lines if not line.startswith('0 ')))
            else:
                manifest['logs'] = []
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.check(manifest)

    def test_summary_error_rc_and_shortened_duration_cannot_pass(self):
        changes = [('number of clients: 16', 'number of clients: 48'),
                   ('number of threads: 2', 'number of threads: 3'),
                   ('query mode: simple', 'query mode: prepared'),
                   ('maximum number of tries: 1', 'maximum number of tries: 2'),
                   ('duration: 3600 s', 'duration: 180 s'),
                   ('actually processed: 32', 'actually processed: 31'),
                   ('failed transactions: 0', 'failed transactions: 1')]
        for before, after in changes:
            manifest = self.capture()
            summary = (self.root/'summary.txt').read_text().replace(before, after)
            manifest['summary'] = self.file('bad-summary', summary)
            with self.subTest(change=before), self.assertRaises(ValueError):
                self.check(manifest)
        for field, value in [('pgbench_rc', 1), ('clients', True), ('clients', 32),
                             ('duration_seconds', 180), ('version', 2)]:
            manifest = self.capture()
            manifest[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check(manifest)
        manifest = self.capture()
        manifest['stderr'] = self.file('error.txt', 'pgbench: error: client 0 aborted\n')
        with self.assertRaises(ValueError):
            self.check(manifest)

    def test_all_rows_and_nodes_match_including_inactive_clients(self):
        for kind, content in (
                ('after', '0|1\n'), ('after', '0|3\n'), ('after', '0|-1\n'),
                ('after', '16|1\n'), ('before', '0|1\n')):
            manifest = self.capture()
            name = manifest[kind][3]['path']
            rows = (self.root/name).read_text().splitlines(True)
            client = int(content.split('|')[0])
            rows[client] = content
            manifest[kind][3] = self.file('bad-rows', ''.join(rows))
            with self.subTest(kind=kind, content=content), self.assertRaises(ValueError):
                self.check(manifest)
        for rows in ('0|2\n', ''.join(f'{i}|2\n' for i in [0]*64)):
            manifest = self.capture()
            manifest['after'][0] = self.file('incomplete-rows', rows)
            with self.assertRaises(ValueError):
                self.check(manifest)

    def test_changed_aliased_and_escaped_inputs_refuse(self):
        manifest = self.capture()
        (self.root/'summary.txt').write_text('changed')
        with self.assertRaises(ValueError):
            self.check(manifest)
        manifest = self.capture()
        manifest['after'][1] = copy.deepcopy(manifest['after'][0])
        with self.assertRaises(ValueError):
            self.check(manifest)
        manifest = self.capture()
        os.link(self.root/'after-0.tsv', self.root/'alias')
        manifest['after'][1] = dict(manifest['after'][0], path='alias')
        with self.assertRaises(ValueError):
            self.check(manifest)
        manifest = self.capture()
        (self.root/'outside').symlink_to(self.root/'after-0.tsv')
        manifest['after'][1] = dict(manifest['after'][0], path='outside')
        with self.assertRaises(ValueError):
            self.check(manifest)
        manifest = self.capture()
        manifest['after'][0]['path'] = '../after-0.tsv'
        with self.assertRaises(ValueError):
            self.check(manifest)

    def test_replaced_open_input_and_changed_workload_cannot_pass(self):
        manifest = self.capture()
        descriptor = manifest['logs'][0]
        path = self.root/descriptor['path']

        def replace_after_read(lines):
            raw = ''.join(lines).encode()
            path.unlink()
            path.write_bytes(raw)
            return raw

        with self.assertRaisesRegex(ValueError, 'changed during parsing'):
            ledger.BoundFiles(self.root).read(descriptor, replace_after_read)
        manifest = self.capture()
        manifest['workload'] = self.file('different.sql', 'SELECT 1;\n')
        with self.assertRaisesRegex(ValueError, 'workload differs'):
            self.check(manifest)
        manifest = self.capture()
        duplicate = (self.root/'summary.txt').read_text() + 'number of clients: 16\n'
        manifest['summary'] = self.file('duplicate-summary.txt', duplicate)
        with self.assertRaises(ValueError):
            self.check(manifest)

    def test_cli_reports_subcheck_only_and_real_file_failure(self):
        manifest = self.capture()
        path = self.root/'manifest.json'
        path.write_text(json.dumps(manifest))
        command = [sys.executable, '-B', str(Path(ledger.__file__)), str(path)]
        reply = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertTrue(json.loads(reply.stdout)['workload_ledger_passed'])
        self.assertEqual(json.loads(reply.stdout)['p07'], 'NOT_READY')
        (self.root/'after-0.tsv').write_text('truncated')
        reply = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(reply.returncode, 1)
        self.assertFalse(json.loads(reply.stdout)['workload_ledger_passed'])
        self.assertEqual(json.loads(reply.stdout)['p07'], 'NOT_READY')


if __name__ == '__main__':
    unittest.main()
