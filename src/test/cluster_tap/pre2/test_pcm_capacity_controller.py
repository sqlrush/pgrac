"""Preparation/cleanup checks; no compiler, server or namespace is executed.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import local_quorum
import run_multixact_lifecycle as native


class CapacityPreparationTests(unittest.TestCase):
    def preparation(self, suite, fail_probe=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = Path(__file__).resolve().parents[4]
            install = root/'install'
            (install/'bin').mkdir(parents=True)
            binary = b'identity input only; never executed as a server'
            (install/'bin/postgres').write_bytes(binary)
            vendor = root/'vendor'
            vendor.mkdir()
            args = argparse.Namespace(source=source, install=install,
                output=root/'output', vendor=vendor, suite=suite, cases=None,
                uid=501, gid=20, identity=None, archive=None,
                fixture_install=install,
                expected_postgres_sha256=hashlib.sha256(binary).hexdigest())
            events = []

            class QuorumBoundary:
                def __init__(self, *args):
                    self.env = {'LD_LIBRARY_PATH': ''}

                def start(self):
                    events.append('start')
                    raise RuntimeError('stop at native service boundary')

                def stop(self):
                    events.append('stop')

                def run(self, *args, **kwargs):
                    raise AssertionError('no SQL or case may run at this boundary')

            def compile_boundary(command, **kwargs):
                events.append([str(part) for part in command])
                if fail_probe and str(source/'src/test/cluster_tap/pre2/pcm_capacity_stats.c') in events[-1]:
                    raise subprocess.CalledProcessError(1, command)
                return subprocess.CompletedProcess(command, 0)

            with patch.object(native.sys, 'platform', 'linux'), \
                    patch.object(native.os, 'geteuid', return_value=0), \
                    patch.object(native.os, 'chown'), \
                    patch.object(native.pwd, 'getpwuid', return_value=argparse.Namespace(pw_gid=20)), \
                    patch.object(native.subprocess, 'check_output', return_value=str(source/'src/include')), \
                    patch.object(native.subprocess, 'run', side_effect=compile_boundary), \
                    patch.object(local_quorum, 'LocalQuorum', QuorumBoundary), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(native.run(args), 1)
            result = json.loads((args.output/'result.json').read_text())
            self.assertFalse(result['complete'])
            self.assertEqual(result['cases'], [])
            self.assertEqual(result['cleanup_errors'], [])
            self.assertEqual(events[-1], 'stop')
            commands = [event for event in events if isinstance(event, list)]
            for command in commands:
                self.assertEqual(command[:6], ['setpriv', '--reuid', '501', '--regid', '20', '--init-groups'])
                self.assertEqual(command[6], 'cc')
            probe = [command for command in commands
                     if str(source/'src/test/cluster_tap/pre2/pcm_capacity_stats.c') in command]
            return events, commands, probe, result

    def test_probe_prepared_before_service_only_for_capacity(self):
        for suite in ('multixact', 'p145-grd-capacity'):
            with self.subTest(suite=suite):
                events, commands, probe, result = self.preparation(suite)
                self.assertEqual(events[-2:], ['start', 'stop'])
                self.assertIn('stop at native service boundary', result['error'])
                self.assertEqual(len(commands), 3 if suite == 'p145-grd-capacity' else 2)
                self.assertEqual(len(probe), int(suite == 'p145-grd-capacity'))
                if probe:
                    self.assertTrue(probe[0][-1].endswith('/output/pcm_capacity_stats.so'))

    def test_probe_build_failure_keeps_original_cleanup_and_stops_before_service(self):
        events, commands, probe, result = self.preparation('p145-grd-capacity', fail_probe=True)
        self.assertEqual(len(probe), 1)
        self.assertEqual(len(commands), 3)
        self.assertNotIn('start', events)
        self.assertIn('CalledProcessError', result['error'])


if __name__ == '__main__':
    unittest.main()
