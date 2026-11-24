"""Reconnect through the real worker, transaction runner and libpq binding.

Only the native libpq boundary is scripted. No server or VM is started.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from pathlib import Path
import queue
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p0_longrun_load import Endpoint, Worker  # noqa: E402
from p0_longrun_pq import Connection  # noqa: E402


class DisconnectingLibpq:
    """Lose COMMIT replies, then acknowledge one transaction and stop."""

    def __init__(self, stop, disconnects):
        self.stop, self.disconnects = stop, disconnects
        self.allocated, self.finished, self.freed_cancel = [], [], []
        self.connections, self.cancellations, self.bad = set(), set(), set()
        self.live_at_connect, self.results = [], {}
        self.result_id = 1000

    def PQconnectdb(self, _conninfo):
        self.live_at_connect.append(len(self.connections))
        handle = len(self.allocated) + 1
        self.allocated.append(handle)
        self.connections.add(handle)
        return handle

    def PQstatus(self, handle):
        return int(handle in self.bad)

    def PQbackendPID(self, handle):
        return 2000 + handle

    def PQgetCancel(self, handle):
        self.cancellations.add(3000 + handle)
        return 3000 + handle

    def PQfreeCancel(self, handle):
        self.cancellations.remove(handle)
        self.freed_cancel.append(handle)

    def PQfinish(self, handle):
        self.connections.remove(handle)
        self.finished.append(handle)

    def PQexec(self, handle, sql):
        if sql == b'COMMIT':
            if handle <= self.disconnects:
                self.bad.add(handle)
                return None
            self.stop.set()
        command = b'UPDATE 1' if sql.startswith(b'UPDATE ') else sql
        self.result_id += 1
        self.results[self.result_id] = command
        return self.result_id

    def PQerrorMessage(self, _handle):
        return b'server closed the connection unexpectedly'

    def PQresultStatus(self, _result):
        return 1  # PGRES_COMMAND_OK

    def PQcmdStatus(self, result):
        return self.results[result]

    def PQntuples(self, _result):
        return 0

    def PQclear(self, result):
        del self.results[result]


class ReconnectOwnership(unittest.TestCase):
    def run_worker(self, disconnects):
        stop, events = threading.Event(), queue.SimpleQueue()
        native = DisconnectingLibpq(stop, disconnects)
        worker = Worker(Endpoint(0, 'dbname=postgres'), 0, 1, 7,
                        lambda endpoint: Connection(native, endpoint.conninfo),
                        'load', events, stop, threading.Event())
        worker.run()
        records = []
        while not events.empty():
            records.append(events.get_nowait())
        return worker, native, records

    def test_reconnect_releases_old_handles_without_losing_error_or_ledger(self):
        # Omitting close before replacement leaves PGconn/PGcancel allocated
        # after each lost COMMIT, even though all client errors were recorded.
        for disconnects in (1, 20):
            with self.subTest(disconnects=disconnects):
                worker, native, records = self.run_worker(disconnects)
                self.assertEqual(worker.counts['reconnects'], disconnects)
                self.assertEqual(worker.counts['U'], disconnects)
                self.assertEqual(worker.counts['C'], 1)
                self.assertEqual(dict(worker.acked), {1: 1})
                self.assertEqual(dict(worker.doubt), {1: disconnects})
                self.assertEqual([entry[2] for entry in worker.ledger],
                                 ['U'] * disconnects + ['C'])
                self.assertEqual(len(records), disconnects)
                self.assertTrue(all(record['phase'] == 'load'
                                    and record['stage'] == 'COMMIT'
                                    and record['connection_lost']
                                    and record['message'] ==
                                    'server closed the connection unexpectedly'
                                    for record in records))
                self.assertEqual(native.connections, set())
                self.assertEqual(native.cancellations, set())
                self.assertEqual(native.live_at_connect, [0] * (disconnects + 1))
                self.assertEqual(native.finished, list(range(1, disconnects + 2)))
                self.assertEqual(native.freed_cancel,
                                 list(range(3001, 3002 + disconnects)))
                self.assertEqual(native.results, {})

    def test_normal_completion_releases_each_handle_once(self):
        worker, native, records = self.run_worker(0)
        self.assertEqual(worker.counts['C'], 1)
        self.assertEqual(records, [])
        self.assertEqual(native.connections, set())
        self.assertEqual(native.cancellations, set())
        self.assertEqual(native.finished, [1])
        self.assertEqual(native.freed_cancel, [3001])


if __name__ == '__main__':
    unittest.main(verbosity=2)
