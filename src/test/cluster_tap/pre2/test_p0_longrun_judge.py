"""Judge and log reader against the retained sample in p0_longrun_sample/.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
from typing import Dict, List, Optional
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
SAMPLE = HERE / 'p0_longrun_sample'

from p0_longrun_cycle import planned_steps  # noqa: E402
from p0_longrun_judge import NATIVE, judge, load_allow_list  # noqa: E402
from p0_longrun_logs import parse_line, read_events  # noqa: E402
from p0_longrun_report import render  # noqa: E402

UNKNOWN = 'sample failure 4242 that no rule of the table quotes'
QUIET = '2026-10-24 11:06:00.000 CST [4805] LOG:  checkpoint complete: wrote 12 buffers\n'
SHUT = '2026-10-24 11:09:15.500 CST [4557] LOG:  database system is shut down\n'
SPEC = dict(cycle=1, clients_per_node=4, seconds=180.0, rows=131072, drop_step=True,
            crash=False, restart_verify=False, grace_seconds=60.0, stall_abort_seconds=300.0)


def sample_pieces() -> Dict[str, str]:
    return {p.name[:-4]: p.read_text() for p in SAMPLE.glob('node*.log')}


def sample_client_events() -> List[Dict[str, object]]:
    return [json.loads(line) for line in (SAMPLE / 'client-errors.jsonl').read_text()
            .splitlines()]


def build_run(root: Path, pieces: Dict[str, str], client: List[Dict[str, object]],
              commits: Optional[List[int]] = None, spec: Optional[Dict[str, object]] = None,
              stop_status: str = 'PASS') -> Path:
    """A run directory as the driver leaves it, from per-step log pieces."""
    spec = dict(spec or SPEC)
    commits = commits or [1200, 1100, 1000, 900]
    cycle = root / 'cycle1'
    (cycle / 'cluster').mkdir(parents=True)
    (cycle / 'evidence').mkdir()
    marks: Dict[str, Dict[str, int]] = {'start': {}, 'load': {}, 'stop': {}}
    for node in range(4):
        text, offset = '', 0
        for step in ('start', 'load', 'stop'):
            marks[step][str(node)] = offset
            piece = pieces.get('node%d.%s' % (node, step), '')
            text += piece
            offset += len(piece.encode())
        (cycle / 'cluster' / ('node%d.log' % node)).write_bytes(text.encode())
    start_of = dict(init='start', start='start', seed='load', load='load', verify='stop',
                    drop='stop', stop='stop')
    steps = [dict(name=name, status=stop_status if name == 'stop' else 'PASS',
                  started_unix=100.0, finished_unix=101.0, log_start=marks[start_of[name]],
                  error='normal stop refused' if name == 'stop' and stop_status != 'PASS'
                  else None)
             for name in planned_steps(spec)]
    journal = dict(cycle=1, planned=planned_steps(spec), steps=steps, left_running=False,
                   members=[dict(id=n, logfile='cluster/node%d.log' % n) for n in range(4)])
    (cycle / 'evidence/steps.json').write_text(json.dumps(journal))
    (cycle / 'evidence/client-errors.jsonl').write_text(
        ''.join(json.dumps(e) + '\n' for e in client))
    nodes = [dict(node=n, connections=4, commits=c, statement_errors=0, commit_errors=0,
                  in_doubt=0, anomalies=0, reconnects=0, connect_failures=0,
                  injected_disconnects=0) for n, c in enumerate(commits)]
    (cycle / 'evidence/load.json').write_text(json.dumps(dict(
        nodes=nodes, commits=sum(commits), elapsed_seconds=180.2, hangs=[], stall=None,
        crash=None, ran_requested_time=True)))
    (cycle / 'evidence/verify.json').write_text(json.dumps(dict(step='verify', members=[
        dict(node=n, status='PASS', rows=131072, violation_count=0, violations=[])
        for n in range(4)])))
    (root / 'plan.json').write_text(json.dumps(dict(mode=NATIVE, members=4, cycles=[spec])))
    (root / 'identity.json').write_text(json.dumps(dict(postgres_sha256='ab' * 32,
                                                        source_commit='0' * 40)))
    return root


def clean_pieces() -> Dict[str, str]:
    pieces = sample_pieces()
    for node in range(4):
        pieces['node%d.load' % node] = QUIET
        pieces['node%d.stop' % node] = SHUT
    return pieces


class JudgeSample(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'run'
        self.root.mkdir()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_sample_with_an_unquoted_error_is_refused_and_lists_it(self) -> None:
        result = judge(build_run(self.root, sample_pieces(), sample_client_events()))
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertFalse(result['zero_errors'])
        self.assertEqual(result['unclassified']['count'], 2)   # server record + client record
        self.assertTrue(any(UNKNOWN in raw for raw in result['unclassified']['first_raw']))
        counted = {name: (b['server'], b['client']) for name, b in result['buckets'].items()
                   if b['server'] or b['client']}
        self.assertEqual(counted, {'P0-1': (3, 0), 'P0-5': (1, 1), 'P0-7': (1, 0),
                                   'P0-8': (1, 0), 'P0-10': (1, 0), 'P0-11': (1, 1),
                                   'P0-12': (1, 0)})
        self.assertEqual(result['buckets']['P0-1']['rules'],
                         {'p0-1.normal-stop': 2, 'p0-1.stop-step-severe': 1})
        self.assertEqual(result['totals']['failing_server_records'], 10)
        self.assertEqual(result['totals']['failing_client_records'], 3)
        self.assertEqual([n['commits'] for n in result['cycles'][0]['nodes']],
                         [1200, 1100, 1000, 900])
        self.assertEqual(result['sections']['crash_restart_reconcile'], 'NOT_RUN')
        self.assertEqual(result['sections']['service_after_member_isolation'],
                         'NOT_IMPLEMENTED')

    def test_recognised_errors_alone_still_fail_without_an_allow_list(self) -> None:
        pieces = sample_pieces()
        pieces['node2.load'] = pieces['node2.load'].split('2026-10-24 11:08:40.000')[0]
        client = [e for e in sample_client_events() if UNKNOWN not in e['message']]
        result = judge(build_run(self.root, pieces, client))
        self.assertEqual(result['unclassified']['count'], 0)
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertIn('ERRORS_NOT_ALLOWED', [f['code'] for f in result['failures']])

    def test_run_without_any_error_passes(self) -> None:
        result = judge(build_run(self.root, clean_pieces(), []))
        self.assertEqual((result['verdict'], result['zero_errors'], result['failures']),
                         ('PASS', True, []))
        self.assertEqual(result['sections']['normal_stop'], dict(requested=1, passed=1))

    def test_one_unquoted_error_alone_refuses_an_otherwise_clean_run(self) -> None:
        pieces = clean_pieces()
        pieces['node1.load'] = ('2026-10-24 11:08:40.000 CST [8124] ERROR:  XX000: %s\n'
                                % UNKNOWN)
        result = judge(build_run(self.root, pieces, []))
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertEqual(result['unclassified']['count'], 1)
        self.assertIn('node 1 step load', result['unclassified']['first_raw'][0])
        self.assertIn(UNKNOWN, render(result))

    def test_allow_list_names_exact_errors_and_never_a_panic(self) -> None:
        pieces = clean_pieces()
        known = 'cluster TT status unknown for xid 4242'
        pieces['node1.load'] = ('2026-10-24 11:08:40.000 CST [8124] ERROR:  XX000: %s\n'
                                % known)
        allow = self.root / 'allow.json'
        allow.write_text(json.dumps(dict(version=1, entries=[dict(
            id='sample', reason='unit test', message='^cluster TT status unknown for xid 4242$')])))
        run = build_run(self.root, pieces, [])
        result = judge(run, allow)
        self.assertEqual(result['verdict'], 'PASS')
        self.assertEqual(result['allow_list']['entries'][0]['matched'], 1)
        self.assertTrue(result['zero_errors'])
        self.assertEqual(result['buckets']['P0-5']['allowed'], 1)
        self.assertEqual(result['unclassified']['count'], 0)
        # The same entry cannot excuse a PANIC carrying the same text.
        log = run / 'cycle1/cluster/node1.log'
        log.write_text(log.read_text().replace('ERROR:  XX000', 'PANIC:  XX000'))
        self.assertEqual(judge(run, allow)['verdict'], 'FAIL')
        for bad in (dict(id='a', reason='r', message='.*'),
                    dict(id='a', reason='r', message='x', severities=['PANIC']),
                    dict(id='a', reason='', message='x'), dict(id='a', message='x')):
            allow.write_text(json.dumps(dict(version=1, entries=[bad])))
            with self.assertRaises(ValueError):
                load_allow_list(allow)

    def test_lifecycle_and_bookkeeping_failures_refuse_a_quiet_run(self) -> None:
        cases = {
            'stop not passed': (dict(stop_status='FAIL'), None, 'STEP_NOT_PASSED'),
            'node without commits': (dict(commits=[5, 0, 5, 5]), None, 'NODE_WITHOUT_COMMITS'),
            'hung connection': ({}, ('load.json', 'hangs', [dict(node=1)]), 'HUNG_CONNECTIONS'),
            'ledger mismatch': ({}, ('verify.json', 'members', [dict(node=0, status='FAIL')]),
                                'LEDGER_MISMATCH'),
        }
        for name, (options, patch, code) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                run = build_run(Path(tmp), clean_pieces(), [], **options)
                if patch:
                    path = run / 'cycle1/evidence' / patch[0]
                    document = json.loads(path.read_text())
                    document[patch[1]] = patch[2]
                    path.write_text(json.dumps(document))
                result = judge(run)
                self.assertEqual(result['verdict'], 'FAIL')
                self.assertIn(code, [f['code'] for f in result['failures']])

    def test_missing_evidence_or_forced_cleanup_is_never_a_pass(self) -> None:
        run = build_run(self.root, clean_pieces(), [])
        (run / 'cycle1/forced-cleanup.json').write_text(json.dumps(dict(
            log_start={str(n): 0 for n in range(4)}, detail='forced')))
        self.assertIn('FORCED_CLEANUP', [f['code'] for f in judge(run)['failures']])
        (run / 'cycle1/forced-cleanup.json').unlink()
        (run / 'cycle1/evidence/steps.json').unlink()
        result = judge(run)
        self.assertEqual(result['verdict'], 'FAIL')
        self.assertEqual(result['failures'][0]['code'], 'EVIDENCE_MISSING')

    def test_summary_is_one_chinese_page_with_the_required_parts(self) -> None:
        text = render(judge(build_run(self.root, sample_pieces(), sample_client_events())))
        for needed in ('结论：FAIL', '零错误：否', '按编号的错误计数', '| P0-5 |',
                       '未分类错误：2 条', '1200 / 1100 / 1000 / 900', '正常停机',
                       '未执行（本次未请求）', '无（所有错误都算失败）'):
            self.assertIn(needed, text)
        self.assertLess(len(text.splitlines()), 80)


class LogReader(unittest.TestCase):
    def test_plain_and_verbose_records(self) -> None:
        plain = parse_line('2026-10-24 11:09:14.082 CST [7179] FATAL:  terminating connection '
                           'due to administrator command')
        self.assertEqual((plain['kind'], plain['pid'], plain['sqlstate']), ('FATAL', 7179, None))
        verbose = parse_line('2026-10-24 11:08:09.001 CST [6555] ERROR:  53R70: ITL slot '
                             'OVERFLOW on heap page (INITRANS=8 full)')
        self.assertEqual((verbose['sqlstate'], verbose['text']),
                         ('53R70', 'ITL slot OVERFLOW on heap page (INITRANS=8 full)'))
        self.assertIsNone(parse_line('\tWHERE v<>0'))
        self.assertIsNone(parse_line('free text without a severity'))

    def test_details_steps_and_crash_indicators(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / 'node.log'
            pieces = sample_pieces()
            first = pieces['node1.load'].encode()
            log.write_bytes(first + pieces['node3.stop'].encode()
                            + b'TRAP: failed Assert("x"), File: "a.c", Line: 1, PID: 9\n')
            events = sorted(read_events(log, 1, [(0, 'load'), (len(first), 'stop')]),
                            key=lambda e: e['offset'])
        self.assertEqual([(e['severity'], e['sqlstate'], e['phase']) for e in events],
                         [('ERROR', '0A000', 'load'), ('ERROR', '54000', 'load'),
                          ('ERROR', '53R70', 'load'), ('FATAL', None, 'stop'),
                          ('PANIC', None, 'stop'), ('CRASH', None, 'stop'),
                          ('CRASH', None, 'stop')])
        self.assertIn('last_status=-1', events[0]['detail'])
        self.assertIn('cluster_gcs_block.c:5982', events[0]['location'])
        self.assertIn('reason=NORMAL_STOP_PEER_FRONTENDS_PENDING', events[4]['detail'])
        self.assertTrue(events[6]['message'].startswith('TRAP: failed Assert'))


if __name__ == '__main__':
    unittest.main()
