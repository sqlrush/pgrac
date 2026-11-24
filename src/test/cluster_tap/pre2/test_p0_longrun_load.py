"""Workload client bookkeeping, checked against in-memory stand-ins.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
import sys
from typing import Dict, List
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import p0_longrun_load as load  # noqa: E402
from p0_longrun_testing import FakeServer, error  # noqa: E402

ENDPOINTS = [load.Endpoint(node, 'fake %d' % node) for node in range(4)]


def seeded(rows: int = 64) -> FakeServer:
    server = FakeServer()
    server.table = {row: 0 for row in range(1, rows + 1)}
    return server


def one(server: FakeServer, row: int = 3) -> load.Transaction:
    conn = server.connect(ENDPOINTS[0])
    conn.connect()
    return load.run_transaction(conn, row, lambda stage: None)


def drive(server: FakeServer, seconds: float = 0.4, **options: object):
    events: List[Dict[str, object]] = []
    samples: List[Dict[str, object]] = []
    result = load.run_load(ENDPOINTS, 2, seconds, len(server.table), server.connect, 'load',
                           events.extend, samples.append, progress_seconds=0.1, **options)
    return result, events, samples


class OneTransaction(unittest.TestCase):
    def test_acknowledged_commit(self) -> None:
        server = seeded()
        done = one(server)
        self.assertEqual((done.outcome, done.failures, server.table[3]), (load.COMMITTED, (), 1))

    def test_statement_error_is_rolled_back_and_kept(self) -> None:
        server = seeded()
        server.failing_rows[3] = error('53R97', 'cluster TT status unknown for xid 9')
        done = one(server)
        self.assertEqual((done.outcome, done.stage, server.table[3]), (load.ABORTED, 'UPDATE', 0))
        self.assertEqual(done.failures[0][1].sqlstate, '53R97')
        self.assertFalse(done.link_lost)

    def test_update_of_an_existing_row_reporting_zero_rows_is_a_wrong_answer(self) -> None:
        server = seeded()
        del server.table[3]
        done = one(server)
        self.assertEqual(done.outcome, load.ANOMALY)
        self.assertEqual(done.failures[0][1].severity, 'ANOMALY')
        self.assertIn('UPDATE 0', done.failures[0][1].message)

    def test_commit_answered_by_an_error_is_not_a_commit(self) -> None:
        server = seeded()
        server.commit_failure = error('53R40', 'cluster quorum lost or uncertain')
        done = one(server)
        self.assertEqual((done.outcome, done.stage, server.table[3]),
                         (load.COMMIT_FAILED, 'COMMIT', 0))

    def test_commit_without_an_answer_is_in_doubt(self) -> None:
        server = seeded()
        server.lose_commits_for.add(3)
        done = one(server)
        self.assertEqual((done.outcome, done.link_lost), (load.IN_DOUBT, True))


class Reconcile(unittest.TestCase):
    def check(self, observed: Dict[int, int], rows: int = 10, counted: int = 10):
        return load.reconcile(rows, Counter({1: 2, 2: 1}), Counter({2: 1}), counted, observed)

    def test_ledger_bounds(self) -> None:
        self.assertEqual(self.check({1: 2, 2: 1})['status'], 'PASS')
        self.assertEqual(self.check({1: 2, 2: 2})['status'], 'PASS')     # in-doubt landed
        missing = self.check({1: 1, 2: 1})
        self.assertEqual((missing['status'], missing['violations'][0]['kind']),
                         ('FAIL', 'COMMITTED_MISSING'))
        extra = self.check({1: 2, 2: 3})
        self.assertEqual(extra['violations'][0]['kind'], 'UNCOMMITTED_PRESENT')
        untouched = self.check({1: 2, 2: 1, 7: 1})
        self.assertEqual(untouched['violations'][0], dict(
            kind='UNCOMMITTED_PRESENT', row=7, value=1, acknowledged=0, in_doubt=0))
        self.assertEqual(self.check({1: 2, 2: 1}, counted=9)['violations'][0]['kind'],
                         'ROW_COUNT')
        self.assertEqual(self.check({1: 2, 2: 1, 11: 1})['violations'][0]['kind'],
                         'UNKNOWN_ROW')


class Workload(unittest.TestCase):
    def test_every_member_commits_and_the_ledger_equals_the_table(self) -> None:
        server = seeded()
        result, events, samples = drive(server)
        self.assertEqual(events, [])
        self.assertTrue(result.summary['ran_requested_time'])
        self.assertEqual([n['connections'] for n in result.summary['nodes']], [2, 2, 2, 2])
        self.assertTrue(all(n['commits'] > 0 for n in result.summary['nodes']))
        self.assertEqual(dict(result.acked), {k: v for k, v in server.table.items() if v})
        self.assertEqual(sum(len(v) for v in result.ledgers.values()),
                         result.summary['commits'])
        self.assertGreaterEqual(len(samples), 2)
        self.assertEqual(load.reconcile(64, result.acked, result.doubt, 64, {
            k: v for k, v in server.table.items() if v})['status'], 'PASS')

    def test_errors_do_not_stop_the_clients_and_every_error_is_reported(self) -> None:
        server = seeded()
        refused = error('54000', 'cluster TT slot allocator exhausted on segment 514 '
                                 '(48 slots full)', detail='A safe wait is not available.')
        for row in range(1, 65, 4):
            server.failing_rows[row] = refused
        result, events, _ = drive(server)
        failed = sum(n['statement_errors'] for n in result.summary['nodes'])
        self.assertGreater(failed, 0)
        self.assertEqual(len(events), failed)
        self.assertGreater(result.summary['commits'], 0)
        event = events[0]
        self.assertEqual((event['source'], event['phase'], event['stage'], event['sqlstate']),
                         ('client', 'load', 'UPDATE', '54000'))
        self.assertIn(event['row'], server.failing_rows)
        self.assertEqual(sum(server.table[r] for r in server.failing_rows), 0)

    def test_connection_that_never_returns_is_reported_as_hung(self) -> None:
        server = seeded()
        server.blocked_rows.update(range(1, 65))
        result, events, _ = drive(server, seconds=0.2, grace_seconds=0.2)
        self.assertEqual(len(result.summary['hangs']), 8)
        hang = result.summary['hangs'][0]
        self.assertEqual((hang['stage'], hang['cancel_sent'], hang['returned_after_cancel']),
                         ('UPDATE', True, True))
        self.assertEqual(result.summary['commits'], 0)

    def test_stall_ends_the_workload_early(self) -> None:
        server = seeded()
        server.blocked_rows.update(range(1, 65))
        result, _, _ = drive(server, seconds=30.0, grace_seconds=0.2, stall_abort_seconds=0.3)
        self.assertIsNotNone(result.summary['stall'])
        self.assertFalse(result.summary['ran_requested_time'])
        self.assertLess(result.summary['elapsed_seconds'], 10)

    def test_lost_connection_is_an_error_and_the_client_reconnects(self) -> None:
        server = seeded()
        server.lose_commits_for.add(5)
        result, events, _ = drive(server)
        doubt = sum(n['in_doubt'] for n in result.summary['nodes'])
        self.assertGreater(doubt, 0)
        self.assertEqual(sum(n['reconnects'] for n in result.summary['nodes']), doubt)
        self.assertEqual(len([e for e in events if e['connection_lost']]), doubt)
        self.assertEqual(set(result.doubt), {5})
        self.assertEqual(load.reconcile(64, result.acked, result.doubt, 64, {
            k: v for k, v in server.table.items() if v})['status'], 'PASS')

    def test_broken_client_thread_is_reported_and_never_silent(self) -> None:
        server = seeded()
        connect = server.connect

        def broken(endpoint: load.Endpoint):
            conn = connect(endpoint)
            if endpoint.node == 2:
                conn.execute = lambda sql: 1 / 0
            return conn
        events: List[Dict[str, object]] = []
        result = load.run_load(ENDPOINTS, 2, 0.3, 64, broken, 'load', events.extend,
                               lambda sample: None)
        self.assertEqual([n['anomalies'] for n in result.summary['nodes']], [0, 0, 2, 0])
        self.assertEqual(len(events), 2)
        self.assertEqual({(e['node'], e['severity']) for e in events}, {(2, 'ANOMALY')})
        self.assertIn('ZeroDivisionError', events[0]['message'])

    def test_requested_crash_is_not_counted_as_client_errors(self) -> None:
        server = seeded()
        result, events, _ = drive(server, crash=lambda: (server.down.set(), dict(killed=1))[1])
        self.assertEqual(result.summary['crash'], dict(killed=1))
        self.assertEqual(events, [])
        self.assertEqual(result.summary['hangs'], [])
        self.assertGreater(result.summary['commits'], 0)
        self.assertEqual(load.reconcile(64, result.acked, result.doubt, 64, {
            k: v for k, v in server.table.items() if v})['status'], 'PASS')


if __name__ == '__main__':
    unittest.main()
