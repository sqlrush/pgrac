"""Exercise the real binding's cross-thread cancellation lifetime.

Only native libpq calls are substituted; no server or VM is started.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p0_longrun_pq import Connection  # noqa: E402


class CancelLifetime(unittest.TestCase):
    def connection(self):
        calls = []
        native = SimpleNamespace(
            PQconnectdb=lambda _: 101,
            PQstatus=lambda _: 0,
            PQbackendPID=lambda _: 501,
            PQgetCancel=lambda _: 201,
            PQfreeCancel=lambda handle: calls.append(('free_cancel', handle)),
            PQfinish=lambda handle: calls.append(('finish', handle)))
        conn = Connection(native, 'dbname=postgres')
        self.assertIsNone(conn.connect())
        self.addCleanup(conn.close)
        return conn, native, calls

    def thread(self, operation):
        results, errors = [], []

        def run():
            try:
                results.append(operation())
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        return worker, results, errors

    def test_close_keeps_cancel_object_alive_until_dispatch_returns(self):
        # Removing lifetime protection permits native free during PQcancel.
        for dispatched in (0, 1):
            with self.subTest(dispatched=dispatched):
                conn, native, calls = self.connection()
                entered, release = threading.Event(), threading.Event()
                closing, closed = threading.Event(), threading.Event()

                def cancel(handle, _buffer, _size):
                    calls.append(('cancel_enter', handle))
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError('test did not release native cancellation')
                    calls.append(('cancel_return', handle))
                    return dispatched

                def close():
                    closing.set()
                    conn.close()
                    closed.set()

                native.PQcancel = cancel
                cancelling, results, cancel_errors = self.thread(conn.cancel)
                closer = None
                try:
                    self.assertTrue(entered.wait(2), 'native cancellation did not start')
                    closer, _, close_errors = self.thread(close)
                    self.assertTrue(closing.wait(2), 'close thread did not start')
                    # Give an unprotected close an opportunity to complete. The
                    # assertion below checks actual native-call ordering, not
                    # whether this scheduling interval elapsed.
                    closed.wait(0.2)
                finally:
                    release.set()
                    cancelling.join(2)
                    if closer is not None:
                        closer.join(2)
                self.assertFalse(cancelling.is_alive())
                self.assertFalse(closer.is_alive())
                self.assertEqual(cancel_errors + close_errors, [])
                self.assertEqual(results, [bool(dispatched)])
                conn.close()
                self.assertFalse(conn.cancel())
                self.assertEqual(calls, [
                    ('cancel_enter', 201), ('cancel_return', 201),
                    ('free_cancel', 201), ('finish', 101)])

    def test_cancel_can_interrupt_a_blocking_execute(self):
        # Holding the cancellation guard across PQexec would deadlock this path.
        conn, native, calls = self.connection()
        executing, cancelled = threading.Event(), threading.Event()

        def execute(_handle, _sql):
            executing.set()
            if not cancelled.wait(3):
                raise RuntimeError('query could not receive concurrent cancellation')
            return 301

        def cancel(handle, _buffer, _size):
            calls.append(('cancel', handle))
            cancelled.set()
            return 1

        native.PQexec = execute
        native.PQcancel = cancel
        native.PQresultStatus = lambda _: 7  # PGRES_FATAL_ERROR
        native.PQcmdStatus = lambda _: b''
        fields = {ord('V'): b'ERROR', ord('C'): b'57014',
                  ord('M'): b'canceling statement due to user request'}
        native.PQresultErrorField = lambda _, field: fields.get(field)
        native.PQclear = lambda handle: calls.append(('clear', handle))
        query, results, query_errors = self.thread(lambda: conn.execute('SELECT 1'))
        dispatcher = None
        try:
            self.assertTrue(executing.wait(2), 'query did not enter native execution')
            dispatcher, dispatched, cancel_errors = self.thread(conn.cancel)
            dispatcher.join(2)
            self.assertFalse(dispatcher.is_alive(), 'cancellation blocked behind PQexec')
        finally:
            cancelled.set()
            query.join(2)
            if dispatcher is not None:
                dispatcher.join(2)
        self.assertFalse(query.is_alive())
        self.assertFalse(dispatcher.is_alive())
        self.assertEqual(query_errors + cancel_errors, [])
        self.assertEqual(dispatched, [True])
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].ok)
        self.assertEqual(results[0].sqlstate, '57014')
        self.assertFalse(results[0].connection_lost)
        self.assertEqual(calls, [('cancel', 201), ('clear', 301)])


if __name__ == '__main__':
    unittest.main(verbosity=2)
