"""Build-matrix planner/config/parser checks only; no product tests are simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('m1_build_matrix.py')


class BuildMatrixTests(unittest.TestCase):
    def module(self):
        self.assertTrue(SCRIPT.is_file(), 'build/regression matrix entry is missing')
        spec = importlib.util.spec_from_file_location('m1_build_matrix', SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_plan_emits_original_commands_without_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = SCRIPT.resolve().parents[4]
            command = [sys.executable, '-B', str(SCRIPT), '--source', str(source),
                       '--output', str(root/'output'), '--plan']
            for profile in ('enabled-assert', 'enabled-release', 'disabled-assert', 'disabled-release'):
                command.extend(['--'+profile, str(root/profile)])
            reply = subprocess.run(command, text=True, capture_output=True)
            self.assertEqual(reply.returncode, 0, reply.stderr)
            plan = json.loads(reply.stdout)
            self.assertFalse((root/'output').exists())
        self.assertEqual(plan['status'], 'NOT_RUN')
        self.assertEqual(plan['r01'], 'NOT_READY')
        self.assertEqual(plan['stage_acceptance'], 'NOT_READY')
        self.assertEqual([(p['profile'], [s['kind'] for s in p['stages']]) for p in plan['profiles']], [
            ('enabled-assert', ['build', 'cluster-unit', 'cluster-sql', 'pg-sql']),
            ('enabled-release', ['build', 'cluster-unit', 'cluster-sql', 'pg-sql']),
            ('disabled-assert', ['build', 'pg-sql']),
            ('disabled-release', ['build', 'pg-sql'])])
        for profile in plan['profiles']:
            build = profile['build']
            commands = {step['kind']: step['command'] for step in profile['stages']}
            self.assertEqual(commands['build'], ['make', '-C', build, 'all'])
            self.assertEqual(commands['pg-sql'], ['make', '-C', build, 'check'])
            if profile['profile'].startswith('enabled'):
                self.assertEqual(commands['cluster-unit'],
                    ['make', '-C', build+'/src/test/cluster_unit', 'check'])
                self.assertEqual(commands['cluster-sql'],
                    ['make', '-C', build+'/src/test/cluster_regress', 'check'])
        self.assertEqual(len(plan['pg_tests']), 219)
        self.assertEqual(len(plan['cluster_tests']), 13)

    def test_actual_generated_profile_identity_is_required(self):
        matrix = self.module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'source'
            source.mkdir()
            build = root/'build'
            (build/'src/include').mkdir(parents=True)
            global_file = build/'src/Makefile.global'
            header = build/'src/include/pg_config.h'
            global_file.write_text('abs_top_srcdir = '+str(source)+'\nabs_top_builddir = '
                                   +str(build)+'\nvpath_build = yes\nenable_pgrac_cluster = yes\n')
            header.write_text('#define USE_PGRAC_CLUSTER 1\n#define USE_ASSERT_CHECKING 1\n')
            self.assertEqual(matrix.profile_identity(build, source, True, True)['cluster'], True)
            original = global_file.read_text()
            global_file.write_text(original.replace('vpath_build = yes', 'vpath_build = no'))
            with self.assertRaises(ValueError):
                matrix.profile_identity(build, source, True, True)
            global_file.write_text(original)
            for enabled, assertion in ((False, True), (True, False)):
                with self.assertRaises(ValueError):
                    matrix.profile_identity(build, source, enabled, assertion)
            header.write_text('/* #undef USE_PGRAC_CLUSTER */\n/* #undef USE_ASSERT_CHECKING */\n')
            global_file.write_text(global_file.read_text().replace(
                'enable_pgrac_cluster = yes', 'enable_pgrac_cluster = no'))
            self.assertFalse(matrix.profile_identity(build, source, False, False)['cassert'])
            (root/'other-source').mkdir()
            with self.assertRaises(ValueError):
                matrix.profile_identity(build, root/'other-source', False, False)
            header.write_text('')
            with self.assertRaises(ValueError):
                matrix.profile_identity(build, source, False, False)

    def test_sql_receipts_need_exact_names_plan_and_original_success(self):
        matrix = self.module()
        raw = 'ok 1     - first 12 ms\nok 2     + second 7 ms\n1..2\n# All 2 tests passed.\n'
        self.assertTrue(matrix.sql_passed(raw, 0, ['first', 'second']))
        for text, rc in ((raw, 1), (raw.replace('1..2', '1..1'), 0),
                         (raw.replace('+ second', '+ first'), 0),
                         (raw.replace('ok 2', 'not ok 2'), 0),
                         (raw+'1..2\n', 0), (raw+'Bail out! incomplete\n', 0),
                         (raw.replace('second 7 ms', 'second 7 ms # SKIP missing'), 0),
                         ('1..0 # SKIP disabled\n', 0), ('# All 2 tests passed.\n', 0)):
            with self.subTest(text=text, rc=rc):
                self.assertFalse(matrix.sql_passed(text, rc, ['first', 'second']))

    def test_full_unit_loop_cannot_omit_census_or_hide_a_failed_binary(self):
        matrix = self.module()
        raw = ('# Running test_cluster_terminal_ref_census\nok 1 - census\n1..1\n'
               '# Running test_cluster_other\nok 1 - other\n1..1\n# All 2 binaries passed\n')
        self.assertTrue(matrix.units_passed(raw, 0))
        for text, rc in ((raw, 1), (raw.replace('All 2', 'All 3'), 0),
                         (raw.replace('terminal_ref_census', 'different'), 0),
                         (raw.replace('test_cluster_other', 'test_cluster_terminal_ref_census'), 0),
                         (raw+'# FAILED-BINARY: test_cluster_other (exit 1)\n', 0),
                         ('# cluster tests skipped (rebuild with --enable-cluster to run)\n', 0)):
            with self.subTest(text=text, rc=rc):
                self.assertFalse(matrix.units_passed(text, rc))

    def test_schedule_subsets_duplicates_and_external_overrides_are_refused(self):
        matrix = self.module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'parallel_schedule'
            path.write_text('# original\ntest: first second\ntest: collate.icu.utf8\n')
            self.assertEqual(matrix.schedule(path), ['first', 'second', 'collate.icu.utf8'])
            for text in ('test: first first\n', 'ignore: first\ntest: first\n', '', 'test: ../first\n'):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    matrix.schedule(path)
        for key in ('MAKEFLAGS', 'MAKEOVERRIDES', 'MAKEFILES', 'GNUMAKEFLAGS', 'MFLAGS', 'TESTS', 'REGRESS',
                    'REGRESS_OPTS', 'EXTRA_REGRESS_OPTS', 'PG_TEST_EXTRA', 'PYTHONOPTIMIZE'):
            with self.subTest(key=key), self.assertRaises(ValueError):
                matrix.validate_environment({key: 'override'})
        matrix.validate_environment({})

    def test_source_fingerprint_detects_change_and_refuses_symlink_inputs(self):
        matrix = self.module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'source.c').write_text('int before;\n')
            before = matrix.source_fingerprint(root)
            (root/'source.c').write_text('int after;\n')
            self.assertNotEqual(matrix.source_fingerprint(root), before)
            (root/'alias.c').symlink_to(root/'source.c')
            with self.assertRaises(ValueError):
                matrix.source_fingerprint(root)


if __name__ == '__main__':
    unittest.main()
