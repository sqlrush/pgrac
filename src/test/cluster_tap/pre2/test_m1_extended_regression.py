"""Controller/artifact checks only; no native database result is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import m1_regression as m1
import run_multixact_lifecycle as native


class ExtendedRegressionTests(unittest.TestCase):
    def args(self, root, suite):
        return argparse.Namespace(source=Path(m1.__file__).resolve().parents[4],
            output=root/'results', build=root/'build', install=root/'install',
            fixture_install=root/'fixtures', vendor=root/'vendor', uid=501, gid=20,
            expected_postgres_sha256='a'*64, suite=suite)

    def test_extended_plan_keeps_core_and_adds_original_native_suites(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            core = m1.plan(self.args(root, 'core'))
            extended = m1.plan(self.args(root, 'extended'))
            self.assertEqual(extended['phases'][:3], core['phases'])
            self.assertEqual([p['name'] for p in extended['phases']],
                ['legacy', 'four-member', 'single-member', 'idle-freeze', 'retention', 'soak'])
            for phase, suite in zip(extended['phases'][3:],
                                   ['m1-idle-freeze', 'm1-retention', 'm1-soak-workload']):
                command = phase['command']
                self.assertEqual(command[command.index('--suite')+1], suite)
                self.assertEqual(command[command.index('--output')+1],
                                 str(root.resolve()/'results'/phase['name']))
                self.assertEqual(phase['cases'], [run['case'] for run in native.case_runs(suite, None)])
                self.assertEqual(command[command.index('--expected-postgres-sha256')+1], 'a'*64)
                self.assertNotIn('--cases', command)
            self.assertEqual(extended['status'], 'NOT_RUN')
            self.assertEqual(extended['stage_acceptance'], 'NOT_READY')
            self.assertEqual(extended['subcheck_gates'], dict(p01='NOT_READY', p02='NOT_READY', p07='NOT_READY'))
            self.assertFalse((root/'results').exists())

    def test_extended_cli_is_explicit_and_plan_starts_no_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.args(root, 'extended')
            command = [sys.executable, '-B', str(Path(m1.__file__)), '--plan', '--suite', 'extended']
            for name in ('source', 'output', 'build', 'install', 'fixture_install', 'vendor',
                         'uid', 'gid', 'expected_postgres_sha256'):
                command += ['--'+name.replace('_', '-'), str(getattr(args, name))]
            reply = subprocess.run(command, text=True, capture_output=True)
            self.assertEqual(reply.returncode, 0, reply.stderr)
            self.assertEqual(len(json.loads(reply.stdout)['phases']), 6)
            self.assertFalse((root/'results').exists())
            with self.assertRaises(ValueError):
                m1.plan(self.args(root, 'unknown'))

    def artifact(self, root, suite, output):
        # Literal result schema and actual prove parser output, not a server.
        runs = native.case_runs(suite, None)
        scopes = {'m1-idle-freeze': ('tuple-freeze-and-clean-restart', 'p01'),
                  'm1-retention': ('retained-reader-values-and-normal-stop', 'p02'),
                  'm1-soak-workload': ('workload-ledger-and-normal-stop', 'p07')}
        scope, gate = scopes[suite]
        result = dict(complete=True, binary_sha256='a'*64, cleanup_errors=[], error=None,
                      scope=scope, stage_acceptance='NOT_READY', cases=[])
        result[gate] = 'NOT_READY'
        for run in runs:
            case = dict(case=run['case'], passed=True, rc=0)
            if run['clients'] is not None:
                case['clients'] = run['clients']
            result['cases'].append(case)
            directory = root/run['directory']
            directory.mkdir()
            (directory/'prove.log').write_text(output)
        (root/'result.json').write_text(json.dumps(result))
        return result, runs, gate

    def prove_output(self, root):
        script = root/'parser-input.pl'
        script.write_text('use Test::More; pass("parser input only"); done_testing();')
        reply = subprocess.run(['prove', '-v', str(script)], text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertEqual(reply.returncode, 0, reply.stdout)
        return reply.stdout

    def test_new_phase_readers_reject_identity_scope_debt_and_gate_claims(self):
        for name, suite in [('idle-freeze', 'm1-idle-freeze'), ('retention', 'm1-retention'),
                            ('soak', 'm1-soak-workload')]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result, _, gate = self.artifact(root, suite, self.prove_output(root))
                self.assertTrue(m1.phase_passed(name, root, 0, 'a'*64))
                self.assertFalse(m1.phase_passed(name, root, 124, 'a'*64))
                for key, value in [('binary_sha256', 'b'*64), ('scope', 'another subcheck'),
                                   ('complete', False), ('cleanup_errors', ['live actor']),
                                   ('error', 'cancelled'), (gate, 'PASS'), ('stage_acceptance', 'PASS')]:
                    changed = dict(result, **{key: value})
                    (root/'result.json').write_text(json.dumps(changed))
                    self.assertFalse(m1.phase_passed(name, root, 0, 'a'*64), key)
                for key, value in [('passed', False), ('rc', 124)]:
                    changed = json.loads(json.dumps(result))
                    changed['cases'][0][key] = value
                    (root/'result.json').write_text(json.dumps(changed))
                    self.assertFalse(m1.phase_passed(name, root, 0, 'a'*64), key)

    def test_soak_requires_distinct_16_48_64_points_and_each_original_tap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, runs, _ = self.artifact(root, 'm1-soak-workload', self.prove_output(root))
            for cases in [result['cases'][:1], result['cases'][::-1],
                          [result['cases'][0]]*3]:
                (root/'result.json').write_text(json.dumps(dict(result, cases=cases)))
                self.assertFalse(m1.phase_passed('soak', root, 0, 'a'*64))
            (root/'result.json').write_text(json.dumps(result))
            target = root/runs[1]['directory']/'prove.log'
            original = target.read_text()
            for text in ['', original.replace('ok 1', 'not ok 1'),
                         original.replace('ok 1 -', 'ok 1 # SKIP missing dependency -')]:
                target.write_text(text)
                self.assertFalse(m1.phase_passed('soak', root, 0, 'a'*64))
            target.unlink()
            with self.assertRaises(OSError):
                m1.phase_passed('soak', root, 0, 'a'*64)


if __name__ == '__main__':
    unittest.main()
