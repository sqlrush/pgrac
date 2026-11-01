"""Run the workload controller with real owned children; no database workload.

Only external SQL/pgbench responses and the workload clock are controlled.
The failure caller, pipe framing, collector, graph and cleanup are unchanged.
"""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import p01_large_load_capture as load


CHILD = r'''
import json
import os
from pathlib import Path
import signal
import sys
import time

root = Path(__file__).parent
plan = json.loads((root / 'plan.json').read_text())
is_reader = Path(sys.argv[0]).name == 'psql'
node = int(sys.argv[sys.argv.index('-d') + 1].split('=')[1]) if is_reader else int(sys.argv[-1].split('=')[1])
kind = 'reader' if is_reader else 'worker'
(root / ('%s-%d.pid' % (kind, node))).write_text(str(os.getpid()))
def stopped(signum, frame):
    (root / ('%s-%d.stopped' % (kind, node))).write_text(str(signum))
    sys.exit(0)
signal.signal(signal.SIGTERM, stopped)
if is_reader:
    local_reads = 0
    with (root / ('queries-%d.jsonl' % node)).open('w') as log:
        for line in sys.stdin:
            sql = line.strip()
            if sql.startswith('\\echo '):
                print(sql[6:], flush=True)
                continue
            log.write(json.dumps(sql) + '\n')
            log.flush()
            if sql == 'SELECT test_pgrac_wait_graph_prepare();':
                if plan['scenario'] == 'prepare_error' and node == 0:
                    deadline = time.monotonic() + 5
                    while len(list(root.glob('queries-*.jsonl'))) != 4:
                        if time.monotonic() >= deadline:
                            sys.exit(2)
                        time.sleep(.005)
                print('f' if plan['scenario'] == 'prepare_error' else 't', flush=True)
            elif sql == 'EXECUTE pgrac_wg_local;':
                local_reads += 1
                if local_reads == 2:
                    # The first failure cut must precede owned-worker cleanup.
                    observed = dict(workers_alive=[], workers_stopped=[])
                    for n in range(4):
                        try:
                            os.kill(int((root / ('worker-%d.pid' % n)).read_text()), 0)
                            observed['workers_alive'].append(n)
                        except (OSError, ValueError):
                            pass
                        if (root / ('worker-%d.stopped' % n)).exists():
                            observed['workers_stopped'].append(n)
                    (root / ('at-capture-%d.json' % node)).write_text(json.dumps(observed))
                    if plan['scenario'] == 'query_error' and node == 1:
                        print('ERROR: diagnostic query refused', file=sys.stderr, flush=True)
                        sys.exit(1)
                snapshot = dict(version=2, node=node, epoch_begin=7, epoch_end=7,
                    atomic_graph=False, observed_at=100, owners=[], procs=[], rejected_slots=[],
                    stats=dict(available=True, scanned=16, busy=0, changed=0, dead=0,
                               owners=0, omitted_locks=int(plan['scenario'] == 'capped' and node == 0)),
                    ko=dict(status=0, omitted=0, rows=[]))
                print(json.dumps(snapshot), flush=True)
            elif sql.startswith('EXECUTE pgrac_wg_enqueue('):
                print(json.dumps(dict(kind='enqueue', node=node, status=1)), flush=True)
            elif sql.startswith('EXECUTE pgrac_wg_block('):
                print(json.dumps(dict(kind='pcm', node=node, status=2, omitted_blocks=0, blocks=[])), flush=True)
            elif not sql.startswith('PREPARE '):
                sys.exit(2)
else:
    while not (root / 'finish').exists() or (plan['scenario'] in ('client_error', 'early') and node != 0):
        time.sleep(.005)
    rc = 3 if plan['scenario'] == 'all_client_error' or (plan['scenario'] == 'client_error' and node == 0) else 0
    print('number of clients: 4\nnumber of threads: 4\nmaximum number of tries: 1\n'
          'duration: 180\nnumber of failed transactions: 0\n'
          'number of transactions actually processed: ' + ('0' if plan['scenario'] == 'empty' else '7'), flush=True)
    if plan['scenario'] == 'late_log_error':
        print('pgbench: error: original client refusal', file=sys.stderr, flush=True)
    sys.exit(rc)
'''


class WorkloadClock:
    """Advance only the controller; diagnostic pipes retain real time limits."""
    def __init__(self, root, scenario):
        self.root, self.scenario = root, scenario
        self.now, self.sleeps = 100.0, 0
        self.workers = []

    def time(self):
        return time.time()

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        if duration != .25:
            raise AssertionError('workload polling interval changed')
        deadline = time.monotonic() + 5
        while len(list(self.root.glob('worker-*.pid'))) != 4:
            if time.monotonic() >= deadline:
                raise AssertionError('owned workers did not start')
            time.sleep(.005)
        self.sleeps += 1
        if self.scenario in ('success', 'empty', 'client_error', 'early',
                             'all_client_error', 'all_early', 'late_log_error'):
            (self.root / 'finish').touch()
            self.now += 180 if self.scenario in ('success', 'empty', 'late_log_error') else .25
            targets = self.workers[:1] if self.scenario in ('client_error', 'early') else self.workers
            while any(process.poll() is None for process in targets):
                if time.monotonic() >= deadline:
                    raise AssertionError('owned worker did not reach its exit event')
                time.sleep(.005)
        elif self.scenario == 'watchdog':
            self.now += 241
        elif self.scenario == 'cancel':
            raise KeyboardInterrupt('caller cancelled')
        elif self.scenario == 'split_panic':
            with (self.root / 'server-2.log').open('a') as log:
                log.write('PAN' if self.sleeps == 1 else 'IC: original block refusal\n')
            self.now += .25
        else:
            if self.scenario == 'write_error':
                (self.root / 'output/wait-graph/capture.json').mkdir()
            with (self.root / 'server-2.log').open('a') as log:
                log.write('ERROR: original block refusal\n')
            self.now += .25


class FirstErrorTests(unittest.TestCase):
    def exercise(self, scenario):
        with tempfile.TemporaryDirectory(prefix='p130-first-error-') as directory:
            root = Path(directory)
            (root / 'plan.json').write_text(json.dumps(dict(scenario=scenario)))
            for name in ('psql', 'pgbench'):
                path = root / name
                path.write_text('#!' + sys.executable + '\n' + CHILD)
                path.chmod(0o700)
            config = []
            for n in range(4):
                log = root / ('server-%d.log' % n)
                log.write_text('ERROR: existing failure\n' if scenario == 'preexisting' and n == 0 else '')
                config.append(dict(node=n, connstr='node=%d' % n, logfile=str(log)))
            (root / 'config.json').write_text(json.dumps(config))
            # Resolve only the two external commands. Both Popen implementations
            # and all eight child process lifetimes remain real.
            clock = WorkloadClock(root, scenario)
            popen = load.subprocess.Popen
            def spawn(command, **kwargs):
                process = popen(command, **kwargs)
                if command[0] == str(root / 'pgbench'):
                    clock.workers.append(process)
                return process
            with patch.object(load.shutil, 'which', side_effect=lambda name: str(root / name)), \
                    patch.object(load.subprocess, 'Popen', side_effect=spawn), \
                    patch.object(load, 'time', clock), redirect_stdout(io.StringIO()):
                rc = load.run(root / 'config.json', root / 'output')
            result = json.loads((root / 'output/result.json').read_text())
            for pidfile in root.glob('*.pid'):
                with self.assertRaises(ProcessLookupError, msg=pidfile.name):
                    os.kill(int(pidfile.read_text()), 0)
            queries = [[json.loads(line) for line in path.read_text().splitlines()]
                       for path in sorted(root.glob('queries-*.jsonl'))]
            at_capture = [json.loads(path.read_text()) for path in sorted(root.glob('at-capture-*.json'))]
            evidence = root / 'output/wait-graph/wait-for-graph.json'
            graph = json.loads(evidence.read_text()) if evidence.is_file() else None
            retained = os.environ.get('E_P130_FIRST_ERROR_OUTPUT')
            if retained:
                shutil.copytree(root / 'output', Path(retained) / self._testMethodName)
            self.assertEqual(rc, 0 if result['status'] == 'PASS' else 1)
            return result, queries, at_capture, graph

    def failed_capture(self, scenario, error):
        result, queries, cuts, graph = self.exercise(scenario)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn(error, result['error'])
        self.assertIn('wait_graph', result, 'first failure must retain its diagnostic attempt')
        self.assertEqual(result['wait_graph']['status'], 'FAIL')
        self.assertEqual(result['wait_graph']['reason'], 'original_workload_failed')
        self.assertEqual(len(cuts), 4)
        for cut in cuts:
            self.assertEqual(cut['workers_stopped'], [])
        for sql in queries:
            self.assertEqual(sql.count('EXECUTE pgrac_wg_local;'), 3)
        return result, cuts, graph

    def test_server_error_captures_before_cleaning_four_live_workers(self):
        result, cuts, graph = self.failed_capture('server_error', 'ERROR: original block refusal')
        self.assertTrue(graph['complete'])
        self.assertIsNone(result['wait_graph']['capture_error'])
        for cut in cuts:
            self.assertEqual(cut['workers_alive'], [0, 1, 2, 3])
        self.assertTrue(all(w['forced_client_cleanup'] and w['elapsed_seconds'] < 240
                            for w in result['workers']))

    def test_split_panic_is_captured_before_watchdog(self):
        result, _, _ = self.failed_capture('split_panic', 'PANIC: original block refusal')
        self.assertTrue(all(w['elapsed_seconds'] < 240 for w in result['workers']))

    def test_nonzero_client_keeps_failure_and_collects_once(self):
        self.failed_capture('client_error', 'workload client exited nonzero')

    def test_early_client_keeps_failure_and_collects_once(self):
        self.failed_capture('early', 'workload client exited early')

    def test_all_nonzero_clients_still_collect_before_diagnostic_cleanup(self):
        self.failed_capture('all_client_error', 'workload client exited nonzero')

    def test_all_early_clients_still_collect_before_diagnostic_cleanup(self):
        self.failed_capture('all_early', 'workload client exited early')

    def test_completed_client_error_still_collects_before_diagnostic_cleanup(self):
        self.failed_capture('late_log_error', 'client error or abort')

    def test_caller_cancel_keeps_failure_and_collects_before_cleanup(self):
        result, cuts, _ = self.failed_capture('cancel', 'caller cancelled')
        for cut in cuts:
            self.assertEqual(cut['workers_alive'], [0, 1, 2, 3])
        self.assertIn('KeyboardInterrupt', result['error'])

    def test_observation_cap_is_incomplete_and_never_changes_failure(self):
        _, _, graph = self.failed_capture('capped', 'ERROR: original block refusal')
        self.assertFalse(graph['complete'])
        self.assertIn('omitted_locks', [row['reason'] for row in graph['unknown']])

    def test_partial_diagnostic_error_cannot_replace_original_error(self):
        result, queries, cuts, graph = self.exercise('query_error')
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('ERROR: original block refusal', result['error'])
        self.assertIsNotNone(graph, 'partial diagnostic cuts must remain on disk')
        self.assertFalse(graph['complete'])
        self.assertEqual(len(cuts), 4)
        self.assertEqual(queries[1].count('EXECUTE pgrac_wg_local;'), 2)
        self.assertEqual(result['wait_graph']['status'], 'FAIL')

    def test_capture_write_error_cannot_replace_original_error(self):
        result, _, _, graph = self.exercise('write_error')
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('ERROR: original block refusal', result['error'])
        self.assertIn('wait_graph', result)
        self.assertIn('IsADirectoryError', result['wait_graph']['capture_error'])
        self.assertIsNone(graph)

    def test_preexisting_error_does_not_open_diagnostic_or_workload(self):
        result, queries, cuts, graph = self.exercise('preexisting')
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('existing failure', result['error'])
        self.assertEqual(result['workers'], [])
        self.assertEqual(queries, [])
        self.assertEqual(cuts, [])
        self.assertIsNone(graph)
        self.assertIn('wait_graph', result)
        self.assertEqual(result['wait_graph']['capture_error'], 'diagnostic_session_not_ready')

    def test_prepare_refusal_is_explicitly_unavailable_and_reaps_readers(self):
        result, queries, cuts, graph = self.exercise('prepare_error')
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('prepare unavailable', result['error'])
        self.assertEqual(len(queries), 4)
        self.assertEqual(result['workers'], [])
        self.assertEqual(cuts, [])
        self.assertIsNone(graph)
        self.assertIn('wait_graph', result)
        self.assertEqual(result['wait_graph']['capture_error'], 'diagnostic_session_not_ready')

    def test_watchdog_keeps_its_reason_without_second_capture(self):
        result, queries, cuts, graph = self.exercise('watchdog')
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('watchdog expired', result['error'])
        self.assertEqual(result['wait_graph']['reason'], 'original_watchdog_expired')
        self.assertEqual(len(cuts), 4)
        self.assertTrue(graph['complete'])
        for sql in queries:
            self.assertEqual(sql.count('EXECUTE pgrac_wg_local;'), 3)

    def test_original_workload_and_success_judge_are_preserved(self):
        result, queries, cuts, graph = self.exercise('success')
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(result['commits'], 28)
        self.assertEqual(result['clients_total'], 16)
        self.assertNotIn('wait_graph', result)
        self.assertEqual(cuts, [])
        self.assertIsNone(graph)
        for worker in result['workers']:
            self.assertNotIn('forced_client_cleanup', worker)
            command = worker['command']
            for option, value in (('-c', '4'), ('-j', '4'), ('-T', '180'), ('-M', 'simple')):
                self.assertEqual(command[command.index(option) + 1], value)
            self.assertIn('--max-tries=1', command)
        for sql in queries:
            self.assertEqual(sql.count('EXECUTE pgrac_wg_local;'), 1)

    def test_zero_commits_still_fails_original_judge(self):
        self.failed_capture('empty', 'no successful transaction')


METHODS = (
    'test_server_error_captures_before_cleaning_four_live_workers',
    'test_split_panic_is_captured_before_watchdog',
    'test_nonzero_client_keeps_failure_and_collects_once',
    'test_early_client_keeps_failure_and_collects_once',
    'test_all_nonzero_clients_still_collect_before_diagnostic_cleanup',
    'test_all_early_clients_still_collect_before_diagnostic_cleanup',
    'test_completed_client_error_still_collects_before_diagnostic_cleanup',
    'test_caller_cancel_keeps_failure_and_collects_before_cleanup',
    'test_observation_cap_is_incomplete_and_never_changes_failure',
    'test_partial_diagnostic_error_cannot_replace_original_error',
    'test_capture_write_error_cannot_replace_original_error',
    'test_preexisting_error_does_not_open_diagnostic_or_workload',
    'test_prepare_refusal_is_explicitly_unavailable_and_reaps_readers',
    'test_watchdog_keeps_its_reason_without_second_capture',
    'test_original_workload_and_success_judge_are_preserved',
    'test_zero_commits_still_fails_original_judge',
)


def main():
    discovered = unittest.defaultTestLoader.getTestCaseNames(FirstErrorTests)
    if len(METHODS) != 16 or len(set(METHODS)) != 16 or set(discovered) != set(METHODS):
        print('Bail out! first-error method plan differs from discovery')
        return 1
    print('1..16', flush=True)
    failed = False
    for number, name in enumerate(METHODS, 1):
        result = unittest.TestResult()
        FirstErrorTests(name).run(result)
        ok = (result.testsRun == 1 and result.wasSuccessful() and not result.skipped
              and not result.expectedFailures and not result.unexpectedSuccesses)
        print('%s %d - %s' % ('ok' if ok else 'not ok', number, name), flush=True)
        for _, detail in result.failures + result.errors:
            for line in detail.splitlines():
                print('# ' + line)
        failed |= not ok
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
