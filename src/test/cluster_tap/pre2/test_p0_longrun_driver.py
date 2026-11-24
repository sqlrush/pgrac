"""Cycle sequencing, fixture adapter use and argument handling of the driver.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Dict, List, Optional
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import p0_longrun  # noqa: E402
from p0_longrun_cluster import CohortProvider, ProviderError, node_layout  # noqa: E402
from p0_longrun_cycle import planned_steps, run_cycle  # noqa: E402
from p0_longrun_judge import SELFTEST, judge  # noqa: E402
from p0_longrun_preflight import failed, formatter_build_command, preflight  # noqa: E402
from p0_longrun_testing import FakeProvider, FakeServer, error  # noqa: E402


def arguments(*argv: str):
    return p0_longrun.parser().parse_args(['run', '--install', '/i', '--output', '/o', *argv])


def spec(**changes: object) -> Dict[str, object]:
    base = dict(cycle=1, clients_per_node=2, seconds=0.3, rows=64, drop_step=True, crash=False,
                restart_verify=False, grace_seconds=1.0, stall_abort_seconds=0.0)
    base.update(changes)
    return base


class Arguments(unittest.TestCase):
    def test_one_cycle_per_client_level(self) -> None:
        specs = p0_longrun.cycle_specs(arguments('--clients', '2,4,8', '--minutes', '60'))
        self.assertEqual([(s['cycle'], s['clients_per_node'], s['seconds']) for s in specs],
                         [(1, 2, 3600.0), (2, 4, 3600.0), (3, 8, 3600.0)])
        self.assertEqual([s['drop_step'] for s in specs], [True, False, False])
        self.assertFalse(any(s['crash'] or s['restart_verify'] for s in specs))

    def test_one_level_repeated_and_optional_steps(self) -> None:
        specs = p0_longrun.cycle_specs(arguments('--clients', '4', '--cycles', '3', '--minutes',
                                                 '3', '--crash-cycle', '2', '--no-drop-step'))
        self.assertEqual([s['clients_per_node'] for s in specs], [4, 4, 4])
        self.assertEqual([s['crash'] for s in specs], [False, True, False])
        self.assertFalse(any(s['drop_step'] for s in specs))
        self.assertEqual(specs[0]['seconds'], 180.0)

    def test_invalid_combinations_are_refused(self) -> None:
        for argv in (['--clients', '2,4', '--cycles', '3'], ['--clients', '32'],
                     ['--clients', 'four'], ['--minutes', '0'], ['--crash-cycle', '2']):
            with self.assertRaises(SystemExit):
                p0_longrun.cycle_specs(arguments(*argv))

    def test_default_settings_can_be_replaced_by_name(self) -> None:
        self.assertEqual(p0_longrun.merged_conf([]), list(p0_longrun.DEFAULT_CONF))
        merged = p0_longrun.merged_conf(['autovacuum = on', 'cluster.lms_workers = 4'])
        self.assertEqual(merged, ['autovacuum = on', "shared_buffers = '16MB'",
                                  'log_error_verbosity = verbose', 'cluster.lms_workers = 4'])
        with self.assertRaises(SystemExit):
            p0_longrun.merged_conf(['autovacuum'])

    def test_planned_steps(self) -> None:
        self.assertEqual(planned_steps(spec()), ['init', 'start', 'seed', 'load', 'verify',
                                                 'drop', 'stop'])
        self.assertEqual(planned_steps(spec(crash=True, drop_step=False, restart_verify=True)),
                         ['init', 'start', 'seed', 'load', 'restart', 'reverify', 'stop',
                          'restart_again', 'reverify_again', 'stop_again'])


class Cycle(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.run = Path(self.tmp.name)
        self.root = self.run / 'cycle1'
        self.root.mkdir()
        self.server = FakeServer()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def execute(self, cycle_spec: Dict[str, object], fail: List[str] = ()):
        provider = FakeProvider(self.root / 'cluster', self.server, fail=fail)
        (self.root / 'cluster').mkdir()
        journal = run_cycle(cycle_spec, provider, self.server.connect, self.root)
        (self.run / 'plan.json').write_text(json.dumps(dict(mode=SELFTEST, members=1,
                                                            cycles=[cycle_spec])))
        return provider, journal, judge(self.run)

    def statuses(self, journal: Dict[str, object]) -> Dict[str, str]:
        return {step['name']: step['status'] for step in journal['steps']}

    def test_clean_cycle_leaves_complete_evidence(self) -> None:
        provider, journal, result = self.execute(spec())
        self.assertEqual(set(self.statuses(journal).values()), {'PASS'})
        self.assertEqual(provider.calls, ['init', 'start', 'stop'])
        evidence = self.root / 'evidence'
        for name in ('steps.json', 'load.json', 'verify.json', 'ledger-node0.txt',
                     'progress.jsonl', 'client-errors.jsonl'):
            self.assertTrue((evidence / name).is_file(), name)
        self.assertEqual((evidence / 'client-errors.jsonl').read_text(), '')
        self.assertEqual(result['verdict'], 'SELFTEST_OK')
        self.assertFalse(result['acceptance_result'])
        self.assertEqual(len((evidence / 'ledger-node0.txt').read_text().splitlines()),
                         result['cycles'][0]['commits'])
        prefix = '2026-01-01 00:00:00.000 UTC [100] LOG:  fake '
        self.assertEqual(journal['steps'][-1]['log_start'],
                         {'0': len(prefix + 'init\n') + len(prefix + 'start\n')})

    def test_errors_are_recorded_and_the_cycle_still_stops_normally(self) -> None:
        self.server.failing_rows = {row: error('XX000', 'unquoted failure') for row in (1, 2, 3)}
        provider, journal, result = self.execute(spec(seconds=0.5))
        self.assertEqual(set(self.statuses(journal).values()), {'PASS'})
        self.assertEqual(result['verdict'], 'SELFTEST_FAILED')
        self.assertGreater(result['unclassified']['count'], 0)
        self.assertEqual(result['cycles'][0]['reconciliations'][0]['members'][0]['status'],
                         'PASS')

    def test_error_floods_keep_exact_counts_in_bounded_records(self) -> None:
        import p0_longrun_cycle
        self.server.failing_rows = {row: error('XX000', 'unquoted failure %d' % row)
                                    for row in range(1, 65)}
        saved, p0_longrun_cycle.FULL_CLIENT_RECORDS = p0_longrun_cycle.FULL_CLIENT_RECORDS, 25
        try:
            provider, journal, result = self.execute(spec(seconds=0.4))
        finally:
            p0_longrun_cycle.FULL_CLIENT_RECORDS = saved
        records = [json.loads(line) for line in (self.root / 'evidence/client-errors.jsonl')
                   .read_text().splitlines()]
        attempted = result['cycles'][0]['nodes'][0]['statement_errors']
        self.assertGreater(attempted, 100)
        self.assertEqual(len(records), 26)
        self.assertEqual(records[25]['count'], attempted - 25)
        self.assertEqual(result['totals']['failing_client_records'], attempted)
        self.assertEqual(result['unclassified']['count'], attempted)
        self.assertEqual(result['cycles'][0]['commits'], 0)
        self.assertIn('NODE_WITHOUT_COMMITS', [f['code'] for f in result['failures']])

    def test_failed_seed_skips_the_load_but_still_stops(self) -> None:
        connect, calls = self.server.connect, []

        def refusing(endpoint: object):
            conn = connect(endpoint)
            original = conn.execute
            conn.execute = lambda sql: (calls.append(sql), error('XX001', 'no SPACE identity')
                                        if sql.startswith('CREATE TABLE') else original(sql))[1]
            return conn
        self.server.connect = refusing
        provider, journal, result = self.execute(spec())
        self.assertEqual(self.statuses(journal), dict(
            init='PASS', start='PASS', seed='FAIL', load='NOT_RUN', verify='NOT_RUN',
            drop='NOT_RUN', stop='PASS'))
        self.assertEqual(provider.calls, ['init', 'start', 'stop'])
        codes = [f['code'] for f in result['failures']]
        self.assertIn('STEP_NOT_PASSED', codes)
        self.assertIn('ERRORS_NOT_ALLOWED', codes)

    def test_failed_start_runs_nothing_else_and_is_reported_as_left_running(self) -> None:
        provider, journal, result = self.execute(spec(), fail=['start'])
        self.assertEqual(self.statuses(journal)['start'], 'FAIL')
        self.assertEqual(set(list(self.statuses(journal).values())[2:]), {'NOT_RUN'})
        self.assertEqual(provider.calls, ['init', 'start'])
        self.assertEqual(result['verdict'], 'SELFTEST_FAILED')

    def test_failed_stop_is_never_a_pass(self) -> None:
        provider, journal, result = self.execute(spec(), fail=['stop'])
        self.assertEqual(self.statuses(journal)['stop'], 'FAIL')
        self.assertTrue(journal['left_running'])
        self.assertIn('LEFT_RUNNING', [f['code'] for f in result['failures']])

    def test_crash_cycle_restarts_and_reconciles(self) -> None:
        provider, journal, result = self.execute(spec(crash=True, drop_step=False))
        self.assertEqual(self.statuses(journal), dict(
            init='PASS', start='PASS', seed='PASS', load='PASS', restart='PASS',
            reverify='PASS', stop='PASS'))
        self.assertEqual(provider.calls, ['init', 'start', 'start', 'stop'])
        self.assertEqual(result['sections']['crash_restart_reconcile'], 'PASS')
        self.assertEqual(result['verdict'], 'SELFTEST_OK')

    def test_commit_lost_by_the_server_is_caught_by_the_ledger(self) -> None:
        provider = FakeProvider(self.root / 'cluster', self.server)
        (self.root / 'cluster').mkdir()
        connect = self.server.connect

        def losing(endpoint: object):
            conn = connect(endpoint)
            original = conn.execute

            def execute(sql: str):
                answer = original(sql)
                if sql.startswith('SELECT id,v'):     # one acknowledged commit vanished
                    answer.rows[0][1] = str(int(answer.rows[0][1]) - 1)
                return answer
            conn.execute = execute
            return conn
        journal = run_cycle(spec(), provider, losing, self.root)
        self.assertEqual(self.statuses(journal)['verify'], 'FAIL')
        self.assertEqual(self.statuses(journal)['stop'], 'PASS')
        answer = json.loads((self.root / 'evidence/verify.json').read_text())['members'][0]
        self.assertEqual(answer['violations'][0]['kind'], 'COMMITTED_MISSING')


class Adapter(unittest.TestCase):
    WRITERS = [dict(node=n, thread=n + 1, incarnation=7, wal_generation=3, boot='b%d' % n)
               for n in range(4)]

    def provider(self, replies: Dict[str, object], seen: Optional[List[Dict]] = None):
        def runner(argv, input, **kwargs):
            request = json.loads(input)
            if seen is not None:
                seen.append(dict(request, env=kwargs['env']))
            reply = replies[request['op']]
            reply = reply(request) if callable(reply) else reply
            return subprocess.CompletedProcess(argv, 0, json.dumps(reply), '')
        provider = CohortProvider(node_layout(Path('/run/x/cluster')), Path('/i/bin'),
                                  Path('/run/x/entry.json'), HERE, 'pgrac', Path('/run/x'),
                                  lambda endpoint: None, runner)
        provider.system_identifier, provider.writers = '7', self.WRITERS
        return provider

    def closed(self, **change: object) -> Dict[str, object]:
        writers = [dict(w, state='CLOSED') for w in self.WRITERS]
        writers[2].update(change)
        return dict(version=1, status='OK', system_identifier='7', final_checkpoint=True,
                    writers=writers)

    def test_requests_follow_the_existing_adapter_protocol(self) -> None:
        seen: List[Dict] = []
        provider = self.provider(dict(shared_stop=dict(version=1, status='OK',
                                                       all_processes_exited=True),
                                      shutdown_observation=self.closed()), seen)
        self.assertEqual(provider.stop(), dict(all_processes_exited=True, final_checkpoint=True,
                                               closed_writers=4))
        self.assertEqual([r['op'] for r in seen], ['shared_stop', 'shutdown_observation'])
        self.assertEqual(seen[0]['args'], dict(nodes=[0, 1, 2, 3]))
        self.assertEqual(seen[1]['args']['writers'], self.WRITERS)
        self.assertEqual((seen[0]['version'], seen[0]['layout']['nodes'][3]['port']), (1, 18003))
        self.assertEqual(seen[0]['env']['PGRAC_PRE2_ENTRY_FILE'], '/run/x/entry.json')
        self.assertFalse(provider.running)

    def test_stop_without_the_closed_proof_is_refused(self) -> None:
        for reply in (self.closed(state='OPEN'), self.closed(incarnation=8),
                      dict(self.closed(), final_checkpoint=False)):
            provider = self.provider(dict(
                shared_stop=dict(version=1, status='OK', all_processes_exited=True),
                shutdown_observation=reply))
            with self.assertRaisesRegex(ProviderError, 'CLOSED proof refused'):
                provider.stop()

    def test_adapter_refusals_are_errors(self) -> None:
        blocked = self.provider(dict(describe=dict(version=1, status='BLOCKED',
                                                   dependency='no entry file')))
        with self.assertRaisesRegex(ProviderError, 'BLOCKED: no entry file'):
            blocked.init()
        partial = self.provider(dict(describe=dict(version=1, status='OK',
                                                   capabilities=['fresh_init'])))
        with self.assertRaisesRegex(ProviderError, 'lacks shared_start'):
            partial.init()
        failing = self.provider(dict(shared_stop=dict(version=1, status='ERROR',
                                                      reason='missing normal shutdown record')))
        failing.running = True
        with self.assertRaisesRegex(ProviderError, 'missing normal shutdown record'):
            failing.stop()
        self.assertTrue(failing.running)

    def test_open_requires_every_member_on_one_root(self) -> None:
        root = dict(version=1, status='OK', system_identifier='7', root_digest='a' * 64,
                    generation=5)

        def opened(request: Dict[str, object]) -> Dict[str, object]:
            node = request['args']['node']
            return dict(root, phase='OPEN', members=[0, 1, 2, 3], writer=self.WRITERS[node])
        provider = self.provider(dict(open_observation=opened, root_observation=root))
        self.assertEqual(provider.wait_open(seconds=5)['generation'], 5)
        waiting = self.provider(dict(
            open_observation=lambda r: opened(r) if r['args']['node'] else dict(root,
                                                                               phase='WAIT'),
            root_observation=root))
        with self.assertRaisesRegex(ProviderError, 'not all members reached OPEN'):
            waiting.wait_open(seconds=0.3)


FAKE_ADAPTER = r"""
import json, os, sys
request = json.load(sys.stdin)
op, layout, args = request['op'], request['layout'], request.get('args', {})
nodes = layout['nodes']
reply = {}
def log(text):
    for node in nodes:
        with open(node['logfile'], 'a') as stream:
            stream.write('2026-01-01 00:00:00.000 UTC [%d] LOG:  00000: %s\n' % (100 + node['id'], text))
missing = [k for k in ('PGRAC_PRE2_BINDIR', 'PGRAC_PRE2_ENTRY_FILE', 'TESTLOGDIR') if not os.environ.get(k)]
if missing or request.get('version') != 1 or request.get('budget_seconds', 0) <= 0:
    print(json.dumps(dict(version=1, status='ERROR', reason='bad request %s' % missing)))
    sys.exit(0)
root = dict(system_identifier='7', root_digest='a' * 64, generation=5)
writer = lambda n: dict(node=n, thread=n + 1, incarnation=9, wal_generation=2, boot='boot%d' % n)
if op == 'describe':
    reply = dict(capabilities=['fresh_init', 'shared_start', 'shared_stop', 'root_observation',
                               'open_observation', 'shutdown_observation'])
elif op == 'fresh_init':
    os.makedirs(os.path.join(layout['root'], 'sockets'), exist_ok=True)
    for node in nodes:
        os.makedirs(os.path.join(node['data_dir'], 'global'))
        open(os.path.join(node['data_dir'], 'global', 'pg_control'), 'w').close()
elif op == 'shared_start':
    log('database system is ready to accept connections')
elif op == 'root_observation':
    reply = root
elif op == 'open_observation':
    reply = dict(root, phase='OPEN', members=[n['id'] for n in nodes], writer=writer(args['node']))
elif op == 'shared_stop':
    log('database system is shut down')
    reply = dict(all_processes_exited=True)
elif op == 'shutdown_observation':
    reply = dict(root, final_checkpoint=True,
                 writers=[dict(w, state='CLOSED') for w in args['writers']])
print(json.dumps(dict(version=1, status='OK', **reply)))
"""


class NativePlumbing(unittest.TestCase):
    """The native cycle path end to end, with a subprocess standing in for
    blackbox.py and an in-memory server; it checks wiring, not a database."""

    def test_four_member_cycle_through_the_adapter_protocol(self) -> None:
        from p0_longrun_cluster import catalog_version
        from p0_longrun_judge import NATIVE
        with tempfile.TemporaryDirectory() as tmp:
            run, pre2 = Path(tmp) / 'run', Path(tmp) / 'pre2'
            root = run / 'cycle1'
            root.mkdir(parents=True)
            pre2.mkdir()
            (pre2 / 'blackbox.py').write_text(FAKE_ADAPTER)
            server = FakeServer()
            provider = CohortProvider(node_layout(root / 'cluster'), Path('/i/bin'),
                                      root / 'entry.json', pre2, 'pgrac', root, server.connect)
            server.catalog_version = catalog_version(provider.catalog_header)
            cycle_spec = spec(cycle=1, restart_verify=True)
            journal = run_cycle(cycle_spec, provider, server.connect, root)
            (run / 'plan.json').write_text(json.dumps(dict(mode=NATIVE, members=4,
                                                           cycles=[cycle_spec])))
            result = judge(run)
            self.assertEqual({s['name']: s['status'] for s in journal['steps']},
                             {name: 'PASS' for name in planned_steps(cycle_spec)})
            self.assertEqual([m['logfile'] for m in journal['members']],
                             ['cluster/node%d.log' % n for n in range(4)])
            self.assertEqual(journal['steps'][1]['detail']['system_identifier'], '7')
            self.assertEqual(len(provider.writers), 4)
            self.assertEqual((result['verdict'], result['acceptance_result']), ('PASS', True))
            self.assertEqual(len(result['cycles'][0]['nodes']), 4)
            self.assertTrue(all(n['commits'] > 0 for n in result['cycles'][0]['nodes']))
            self.assertEqual(len(result['cycles'][0]['reconciliations']), 2)
            self.assertEqual(result['cycles'][0]['server_records'], 0)
            # One member log gains an error after the fact: the same evidence now fails.
            with (root / 'cluster/node2.log').open('a') as log:
                log.write('2026-01-01 00:00:01.000 UTC [102] ERROR:  53R97: cluster TT status '
                          'unknown for xid 9\n')
            again = judge(run)
            self.assertEqual(again['verdict'], 'FAIL')
            self.assertEqual(again['buckets']['P0-5']['server'], 1)


class Preflight(unittest.TestCase):
    def test_missing_prerequisites_are_listed_and_nothing_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'run'
            checks = preflight(Path(tmp) / 'install', output, p0_longrun.SOURCE, Path('/'),
                               1000, 1000, None, None)
            self.assertFalse(output.exists())
        missing = {check.name for check in failed(checks)}
        self.assertTrue({'product postgres', 'product libpq'} <= missing)
        self.assertNotIn('source tree', missing)
        self.assertNotIn('output is new', missing)

    def test_run_with_missing_prerequisites_is_blocked_before_anything_starts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output, capture = Path(tmp) / 'run', io.StringIO()
            with contextlib.redirect_stdout(capture), self.assertLogs('p0_longrun'):
                code = p0_longrun.main(['run', '--install', str(Path(tmp) / 'install'),
                                        '--output', str(output), '--minutes', '1'])
            self.assertFalse(output.exists())
        reply = json.loads(capture.getvalue())
        self.assertEqual((code, reply['status']), (2, 'BLOCKED'))
        self.assertIn('product postgres', [m['name'] for m in reply['missing']])

    def test_formatter_build_uses_the_candidate_source_and_install(self) -> None:
        command = formatter_build_command(Path('/s'), Path('/i'), Path('/o/native_vote'))
        self.assertEqual(command[8:11], [
            '/s/src/test/cluster_tap/pre2/native_vote.c',
            '/s/src/backend/cluster/cluster_voting_disk_io.c', '/i/lib/libpgport.a'])
        self.assertTrue((p0_longrun.SOURCE / 'src/test/cluster_tap/pre2/native_vote.c').is_file())
        self.assertTrue((p0_longrun.SOURCE
                         / 'src/backend/cluster/cluster_voting_disk_io.c').is_file())


if __name__ == '__main__':
    unittest.main()
