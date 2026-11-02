"""Closed log-file intake only; no native database result is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class SoakMemberLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.paths = [self.root/f'node{node}.log' for node in range(4)]
        for path in self.paths:
            path.write_bytes(b'2026-11-02 12:00:00.000 UTC [12] LOG:  00000: ready\n')

    def reader(self):
        try:
            return importlib.import_module('m1_soak_member_logs')
        except ModuleNotFoundError as error:
            self.fail('closed native member error intake missing: '+str(error))

    def test_four_clean_files_are_byte_bound_without_native_credit(self):
        result = self.reader().check_logs(self.paths)
        self.assertTrue(result['member_logs_clean'])
        self.assertEqual(result['p07'], 'NOT_READY')
        self.assertEqual(result['stage_acceptance'], 'NOT_READY')
        self.assertEqual(result['identity_binding'], 'DECLARED_ONLY')
        self.assertEqual([row['declared_node'] for row in result['members']], [0, 1, 2, 3])
        for row, path in zip(result['members'], self.paths):
            self.assertEqual(row['sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(row['size_bytes'], path.stat().st_size)
            self.assertEqual(row['counts'], dict(ERROR=0, FATAL=0, PANIC=0))
            self.assertEqual(row['first_by_severity'], {})

    def test_every_original_severity_is_rejected_on_each_declared_member(self):
        clean = self.paths[0].read_bytes()
        for node in range(4):
            for severity in ('ERROR', 'FATAL', 'PANIC'):
                with self.subTest(node=node, severity=severity):
                    raw = f'2026-11-02 12:00:01.123 UTC [71] {severity}:  57014: cancelled\n'.encode()
                    self.paths[node].write_bytes(clean+raw+raw)
                    result = self.reader().check_logs(self.paths)
                    self.assertFalse(result['member_logs_clean'])
                    row = result['members'][node]
                    self.assertEqual(row['counts'][severity], 2)
                    first = row['first_by_severity'][severity]
                    self.assertEqual(first, dict(line=2, byte_offset=len(clean),
                        byte_length=len(raw), sha256=hashlib.sha256(raw).hexdigest(), raw_hex=raw.hex()))
                    self.paths[node].write_bytes(clean)

    def test_message_text_and_statement_literals_are_not_error_headers(self):
        self.paths[0].write_bytes(b'2026-11-02 12:00:00.000 UTC [12] LOG:  ERROR: quoted text\n'
            b'2026-11-02 12:00:00.001 UTC [12] STATEMENT: SELECT \'PANIC: FATAL:\';\n')
        self.assertTrue(self.reader().check_logs(self.paths)['member_logs_clean'])

    def test_first_records_stay_in_file_order_and_counts_are_not_capped(self):
        raw = b'2026-11-02 12:00:00.000 UTC [12] ERROR:  error\n'
        self.paths[0].write_bytes(raw*100)
        result = self.reader().check_logs(self.paths)
        row = result['members'][0]
        self.assertEqual(row['counts']['ERROR'], 100)
        self.assertEqual(len(row['first_by_severity']), 1)
        self.assertEqual(row['first_by_severity']['ERROR']['line'], 1)

    def test_missing_nonregular_and_aliased_inputs_cannot_look_clean(self):
        reader = self.reader()
        for paths in [self.paths[:3], [self.paths[0]]*4]:
            with self.assertRaises(ValueError):
                reader.check_logs(paths)
        link = self.root/'link.log'
        link.symlink_to(self.paths[0])
        with self.assertRaises((ValueError, OSError)):
            reader.check_logs([link, *self.paths[1:]])
        hard = self.root/'hard.log'
        os.link(self.paths[0], hard)
        with self.assertRaises(ValueError):
            reader.check_logs([self.paths[0], hard, *self.paths[2:]])
        with self.assertRaises((ValueError, OSError)):
            reader.check_logs([self.root, *self.paths[1:]])
        self.paths[0].unlink()
        with self.assertRaises(OSError):
            reader.check_logs(self.paths)

    def test_long_first_line_keeps_full_hash_but_bounds_raw_excerpt(self):
        raw = b'2026-11-02 12:00:00.000 UTC [12] ERROR:  '+b'x'*5000+b'\n'
        self.paths[0].write_bytes(raw)
        result = self.reader().check_logs(self.paths)
        self.assertFalse(result['member_logs_clean'])
        first = result['members'][0]['first_by_severity']['ERROR']
        self.assertEqual(first['byte_length'], len(raw))
        self.assertEqual(first['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(first['raw_hex'], raw[:4096].hex())
        self.assertTrue(first['raw_truncated'])

    def test_incomplete_or_non_utf8_log_cannot_supply_a_clean_result(self):
        reader = self.reader()
        for data in (b'2026-11-02 12:00:00 UTC [12] LOG:  partial', b'bad \xff\n'):
            with self.subTest(data=data):
                self.paths[0].write_bytes(data)
                with self.assertRaises(ValueError):
                    reader.check_logs(self.paths)

    def test_extended_preflight_captures_the_log_reader_and_its_grammar(self):
        # Filesystem identity only: these placeholder tool bytes are not executed.
        import m1_regression as regression
        install, build, fixtures = (self.root/name for name in ('install', 'build', 'fixtures'))
        for path in (install/'bin/postgres', build/'src/test/regress/pg_regress',
                     fixtures/'bin/pg_config', install/'lib/pgxs/src/test/regress/pg_regress'):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'identity only, never executed')
            path.chmod(0o700)
        vendor = self.root/'vendor'
        vendor.mkdir()
        args = argparse.Namespace(source=Path(__file__).resolve().parents[4],
            output=self.root/'new-output', install=install, build=build,
            fixture_install=fixtures, vendor=vendor,
            expected_postgres_sha256=hashlib.sha256((install/'bin/postgres').read_bytes()).hexdigest())
        hashes = regression.preflight(args, dict(phases=[dict(name='soak',
            cases=['t/483_pre2_m1_soak_workload.pl'])]))
        for name in ('m1_soak_member_logs.py', 'workload_log.py'):
            with self.subTest(name=name):
                self.assertIn('src/test/cluster_tap/pre2/'+name, hashes)
        self.assertFalse(args.output.exists())

    def test_actual_cli_has_nonzero_status_for_member_error(self):
        script = Path(__file__).with_name('m1_soak_member_logs.py')
        self.assertTrue(script.is_file(), 'native member error CLI missing')
        command = [sys.executable, '-B', str(script), *map(str, self.paths)]
        reply = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertTrue(json.loads(reply.stdout)['member_logs_clean'])
        self.paths[2].write_bytes(b'2026-11-02 12:00:01.000 UTC [15] FATAL:  stopped\n')
        reply = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(reply.returncode, 1, reply.stderr)
        self.assertFalse(json.loads(reply.stdout)['member_logs_clean'])


if __name__ == '__main__':
    unittest.main()
