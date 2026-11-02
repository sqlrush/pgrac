"""Receipt-file validation only; declared files supply no native runtime credit.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import m1_build_matrix as matrix
import m1_regression as regression


class BuildReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.source, self.receipt = root/'source', root/'matrix'
        self.receipt.mkdir()
        original = Path(__file__).resolve().parents[4]
        for name in ('src/test/regress/parallel_schedule',
                     'src/test/cluster_regress/parallel_schedule'):
            target = self.source/name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((original/name).read_bytes())
        args = argparse.Namespace(source=self.source, output=self.receipt)
        for name in ('enabled-assert', 'enabled-release', 'disabled-assert', 'disabled-release'):
            setattr(args, name.replace('-', '_'), root/name)
        # The original read-only plan is a file schema, not an executed matrix.
        self.plan = matrix.plan(args)
        self.result = dict(matrix_passed=True, r01='NOT_READY', stage_acceptance='NOT_READY',
            build_binding='GENERATED_CONFIG_AND_COMMAND_ONLY', source_commit='NOT_ATTESTED',
            error=None, profiles=[])
        for selected in self.plan['profiles']:
            name = selected['profile']
            build = Path(selected['build'])
            (build/'src/include').mkdir(parents=True)
            config = build/'src/include/pg_config.h'
            config.write_text(''.join('#define '+key+' 1\n' if value else '/* #undef '+key+' */\n'
                for key, value in [('USE_PGRAC_CLUSTER', selected['cluster']),
                                   ('USE_ASSERT_CHECKING', selected['cassert'])]))
            global_file = build/'src/Makefile.global'
            global_file.write_text('abs_top_srcdir = '+str(self.source.resolve())+
                '\nabs_top_builddir = '+str(build.resolve())+'\nvpath_build = yes\n'+
                'enable_pgrac_cluster = '+('yes' if selected['cluster'] else 'no')+'\n')
            binary = build/'src/backend/postgres'
            binary.parent.mkdir()
            binary.write_bytes(('never executed file input: '+name).encode())
            binary.chmod(0o700)
            record = dict(profile=name, passed=True, postgres_sha256=self.digest(binary),
                identity=dict(cluster=selected['cluster'], cassert=selected['cassert'],
                    makefile_global_sha256=self.digest(global_file),
                    pg_config_h_sha256=self.digest(config)), stages=[])
            (self.receipt/name).mkdir()
            for stage in selected['stages']:
                kind = stage['kind']
                if kind == 'build':
                    text = 'declared build log; no build was run\n'
                elif kind == 'cluster-unit':
                    text = '# Running test_cluster_terminal_ref_census\nok 1 - file input\n1..1\n# All 1 binaries passed\n'
                else:
                    names = self.plan['cluster_tests' if kind == 'cluster-sql' else 'pg_tests']
                    text = ''.join('ok %d - %s 1 ms\n' % (i, name)
                                   for i, name in enumerate(names, 1))
                    text += '1..%d\n# All %d tests passed.\n' % (len(names), len(names))
                path = self.receipt/name/(kind+'.log')
                path.write_text(text)
                record['stages'].append(dict(stage, rc=0, passed=True,
                                            log_sha256=self.digest(path)))
            self.result['profiles'].append(record)
        self.write('plan.json', self.plan)
        self.write('result.json', self.result)
        self.write('source-sha256.json', {str(p.relative_to(self.source)):self.digest(p)
                                        for p in self.source.rglob('*') if p.is_file()})
        self.args = argparse.Namespace(source=self.source, build=Path(self.plan['profiles'][0]['build']),
            expected_postgres_sha256=self.result['profiles'][0]['postgres_sha256'],
            build_matrix=self.receipt)

    @staticmethod
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def write(self, name, value):
        (self.receipt/name).write_text(json.dumps(value, indent=2)+'\n')

    def capture(self):
        read = getattr(matrix, 'receipt_binding', None)
        self.assertTrue(callable(read), 'native regression needs the original build receipt reader')
        return read(self.args)

    def test_valid_receipt_matches_only_observed_source_and_enabled_binary(self):
        result = self.capture()
        self.assertEqual(result['profile'], 'enabled-assert')
        self.assertEqual(result['postgres_sha256'], self.args.expected_postgres_sha256)
        self.assertEqual(result['source_commit'], 'NOT_ATTESTED')
        self.assertEqual(result['files_sha256']['result.json'], self.digest(self.receipt/'result.json'))
        self.assertFalse((self.receipt/'native-results').exists())
        self.args.build_matrix = None
        self.assertIsNone(self.capture())

    def test_incomplete_failed_or_changed_original_stages_cannot_match(self):
        for key, value in [('matrix_passed', False), ('error', 'first failure'),
                           ('r01', 'PASS'), ('stage_acceptance', 'PASS')]:
            with self.subTest(key=key):
                self.write('result.json', dict(self.result, **{key:value}))
                with self.assertRaises(ValueError):
                    self.capture()
        self.write('result.json', self.result)
        for mutate in (lambda r:r['profiles'].pop(),
                       lambda r:r['profiles'][0].update(passed=False),
                       lambda r:r['profiles'][0]['stages'].pop(),
                       lambda r:r['profiles'][0]['stages'][0].update(rc=1),
                       lambda r:r['profiles'][0]['stages'][0].update(passed=False),
                       lambda r:r['profiles'][0]['stages'][0]['command'].append('TESTS=subset')):
            changed = copy.deepcopy(self.result)
            mutate(changed)
            self.write('result.json', changed)
            with self.assertRaises(ValueError):
                self.capture()
        self.write('result.json', self.result)
        changed = copy.deepcopy(self.plan)
        changed['profiles'][0]['stages'][0]['command'].append('TESTS=subset')
        self.write('plan.json', changed)
        with self.assertRaises(ValueError):
            self.capture()

    def test_log_hash_and_original_no_failure_or_skip_judges_are_both_required(self):
        for kind in ('cluster-unit', 'cluster-sql', 'pg-sql'):
            with self.subTest(kind=kind):
                log = self.receipt/'enabled-assert'/(kind+'.log')
                original = log.read_bytes()
                log.write_bytes(original+b'changed bytes\n')
                with self.assertRaises(ValueError):
                    self.capture()
                for raw in (original.replace(b'ok 1 ', b'not ok 1 ', 1),
                            original+b'# SKIP unqualified\n'):
                    log.write_bytes(raw)
                    result = copy.deepcopy(self.result)
                    for stage in result['profiles'][0]['stages']:
                        if stage['kind'] == kind:
                            stage['log_sha256'] = self.digest(log)
                    self.write('result.json', result)
                    with self.assertRaises(ValueError):
                        self.capture()
                log.write_bytes(original)
                self.write('result.json', self.result)

    def test_source_config_binary_and_selected_enabled_build_must_still_match(self):
        paths = [self.source/'src/test/regress/parallel_schedule',
                 self.args.build/'src/include/pg_config.h',
                 self.args.build/'src/backend/postgres']
        for path in paths:
            with self.subTest(path=path):
                original = path.read_bytes()
                path.write_bytes(original+b'changed input\n')
                with self.assertRaises(ValueError):
                    self.capture()
                path.write_bytes(original)
        added = self.source/'new.c'
        added.write_bytes(b'additional source')
        with self.assertRaises(ValueError):
            self.capture()
        added.unlink()
        self.args.expected_postgres_sha256 = 'f'*64
        with self.assertRaises(ValueError):
            self.capture()
        self.args.expected_postgres_sha256 = self.result['profiles'][2]['postgres_sha256']
        self.args.build = Path(self.plan['profiles'][2]['build'])
        with self.assertRaises(ValueError):
            self.capture()

    def test_missing_or_symlink_receipt_input_cannot_supply_evidence(self):
        path = self.receipt/'enabled-assert/pg-sql.log'
        retained = self.receipt/'retained.log'
        path.rename(retained)
        with self.assertRaises((OSError, ValueError)):
            self.capture()
        path.symlink_to(retained)
        with self.assertRaises(ValueError):
            self.capture()

    def test_phase_recheck_rejects_real_child_receipt_change_without_rebase(self):
        expected = self.capture()
        verify = getattr(regression, 'verify_matrix_binding', None)
        self.assertTrue(callable(verify), 'verify the same matrix receipt at phase boundaries')
        verify(self.args, expected)
        subprocess.run([sys.executable, '-B', '-c',
            'from pathlib import Path; import sys; p=Path(sys.argv[1]); '
            'p.write_bytes(p.read_bytes()+b"\\n")', str(self.receipt/'result.json')], check=True)
        for _ in range(2):
            with self.assertRaises(ValueError):
                verify(self.args, expected)
        self.assertEqual(expected['files_sha256']['result.json'],
                         hashlib.sha256(json.dumps(self.result, indent=2).encode()+b'\n').hexdigest())

    def test_plan_accepts_unavailable_optional_receipt_without_opening_it(self):
        source = Path(__file__).resolve().parents[4]
        root = Path(self.temp.name)
        command = [sys.executable, '-B', str(Path(regression.__file__)), '--plan',
                   '--source', str(source), '--output', str(root/'native-output'),
                   '--build', str(self.args.build), '--install', str(root/'install'),
                   '--fixture-install', str(root/'fixtures'), '--vendor', str(root/'vendor'),
                   '--uid', '501', '--gid', '20', '--expected-postgres-sha256', 'a'*64,
                   '--build-matrix', str(root/'absent-receipt')]
        reply = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertEqual(json.loads(reply.stdout)['build_matrix'], str((root/'absent-receipt').resolve()))
        self.assertFalse((root/'native-output').exists())


if __name__ == '__main__':
    unittest.main()
