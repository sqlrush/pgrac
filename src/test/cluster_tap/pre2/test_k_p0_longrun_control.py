"""Controller unwind tests with process and namespace operations at the leaf.

No subprocess, database or namespace is started. The actual controller,
finish_cycle and failure records run against temporary file evidence.
Author: SqlRush <sqlrush@gmail.com>
IDENTIFICATION: src/test/cluster_tap/pre2/test_k_p0_longrun_control.py
NOTES: Error cleanup never establishes a normal-stop receipt.
"""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import p0_longrun as driver


class ChildUnwind(unittest.TestCase):
    def test_interrupt_reaps_cycle_process_before_propagating(self) -> None:
        child = Mock(pid=424242, returncode=None)
        child.poll.return_value = None
        child.wait.side_effect = [KeyboardInterrupt(), 0]
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(driver.subprocess, 'Popen', return_value=child), \
                    patch.object(driver.os, 'killpg') as kill:
                with self.assertRaises(KeyboardInterrupt):
                    driver.run_child(['fixture-child'], Path(directory) / 'child.log', 30)
                self.assertEqual(child.wait.call_count, 2)
                kill.assert_called_once_with(424242, signal.SIGTERM)

    def test_unreaped_process_is_reported_explicitly(self) -> None:
        child = Mock(pid=424242, returncode=None)
        child.wait.side_effect = [KeyboardInterrupt(),
                                 subprocess.TimeoutExpired('fixture-child', 10),
                                 subprocess.TimeoutExpired('fixture-child', 10)]
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(driver.subprocess, 'Popen', return_value=child), \
                    patch.object(driver.os, 'killpg'):
                try:
                    with self.assertRaisesRegex(RuntimeError, 'not reaped'):
                        driver.run_child(['fixture-child'], Path(directory) / 'child.log', 30)
                except KeyboardInterrupt:
                    self.fail('interruption escaped before checking that the child was reaped')

    def test_watchdog_retains_existing_term_then_kill_and_reap(self) -> None:
        child = Mock(pid=424242, returncode=None)
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired('fixture-child', 30),
                                  subprocess.TimeoutExpired('fixture-child', 10), 0]
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(driver.subprocess, 'Popen', return_value=child), \
                    patch.object(driver.os, 'killpg') as kill:
                result = driver.run_child(['fixture-child'], Path(directory) / 'child.log', 30)
                self.assertIn('controller watchdog', result)
                self.assertEqual(child.wait.call_count, 3)
                self.assertEqual([call.args for call in kill.call_args_list],
                                 [(424242, signal.SIGTERM), (424242, signal.SIGKILL)])


class ControllerUnwind(unittest.TestCase):
    def exercise(self, interrupted: bool, cleanup_fails: bool, unreaped: bool = False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name).resolve()
        output = base / 'output'
        formatter = base / 'formatter'
        formatter.write_text('not executed')
        args = driver.parser().parse_args([
            'run', '--install', str(base / 'install'), '--vendor', str(base / 'vendor'),
            '--output', str(output), '--vote-formatter', str(formatter),
            '--uid', str(os.getuid()), '--gid', str(os.getgid()),
            '--minutes', '10', '--cycles', '1'])
        events, nodes = [], []
        quorum = SimpleNamespace(env={'LD_LIBRARY_PATH': ''}, name='fixture')
        quorum.start = lambda: events.append('quorum-start')
        quorum.stop = lambda: events.append('quorum-stop')

        def start(q, vote_formatter, root, install, user, spec, conf):
            cycle = root / 'cycle1'
            cycle.mkdir()
            for number in range(4):
                data = cycle / ('data%d' % number)
                data.mkdir()
                (data / 'postmaster.pid').write_text('424242\n' + str(data) + '\n')
                log = cycle / ('node%d.log' % number)
                log.touch()
                nodes.append(dict(id=number, data_dir=str(data), logfile=str(log)))
            return dict(spec, cycle_root=str(cycle), layout=dict(nodes=nodes))

        def child(*unused):
            events.append('child-unreaped' if unreaped else 'child-reaped')
            if unreaped:
                raise RuntimeError('cycle process not reaped')
            if interrupted:
                raise KeyboardInterrupt()
            return 'cycle process exited with status 1'

        def cleanup(q, members, bindir):
            self.assertIs(q, quorum)
            self.assertIs(members, nodes)
            self.assertEqual(bindir, base / 'install/bin')
            events.append('database-cleanup')
            if cleanup_fails:
                raise RuntimeError('identity cleanup refused')
            for node in members:
                (Path(node['data_dir']) / 'postmaster.pid').unlink()

        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(driver, 'preflight', return_value=[]))
            stack.enter_context(patch.object(driver.os, 'chown'))
            stack.enter_context(patch.object(driver, 'identity', return_value={}))
            stack.enter_context(patch('local_quorum.LocalQuorum', return_value=quorum))
            stack.enter_context(patch('local_quorum.native_library_dir', return_value='/fixture'))
            stack.enter_context(patch.object(driver, 'start_cycle', side_effect=start))
            stack.enter_context(patch.object(driver, 'run_child', side_effect=child))
            stack.enter_context(patch('cohort_probe.cleanup', side_effect=cleanup))
            stack.enter_context(patch.object(driver, 'postmaster_alive', side_effect=lambda node:
                (Path(node['data_dir']) / 'postmaster.pid').exists()))
            stack.enter_context(patch.object(driver, 'finish', return_value=1))
            try:
                code = driver.command_run(args)
            except KeyboardInterrupt:
                self.fail('controller did not record the interrupted cycle and unwind it')
        self.assertEqual(code, 1)
        record = json.loads((output / 'controller.json').read_text())
        return events, record, output

    def test_interrupt_cleans_members_before_quorum_and_records_failure(self) -> None:
        events, record, output = self.exercise(interrupted=True, cleanup_fails=False)
        self.assertEqual(events, ['quorum-start', 'child-reaped', 'database-cleanup',
                                  'quorum-stop'])
        self.assertTrue(any('interrupt' in problem.lower() for problem in record['problems']))
        self.assertTrue((output / 'cycle1/forced-cleanup.json').is_file())

    def test_unreaped_child_keeps_quorum_even_after_member_cleanup(self) -> None:
        events, record, output = self.exercise(interrupted=False, cleanup_fails=False,
                                               unreaped=True)
        self.assertEqual(events, ['quorum-start', 'child-unreaped', 'database-cleanup'])
        self.assertTrue(any('not reaped' in problem for problem in record['problems']))
        self.assertTrue(any('retained' in problem for problem in record['problems']))
        self.assertTrue((output / 'cycle1/forced-cleanup.json').is_file())

    def test_surviving_member_keeps_quorum_and_failure_evidence(self) -> None:
        events, record, output = self.exercise(interrupted=False, cleanup_fails=True)
        self.assertEqual(events, ['quorum-start', 'child-reaped', 'database-cleanup'])
        self.assertTrue(any('survived' in problem for problem in record['problems']))
        self.assertTrue(any('retained' in problem for problem in record['problems']))
        self.assertTrue((output / 'cycle1/forced-cleanup.json').is_file())


if __name__ == '__main__':
    unittest.main(verbosity=2)
