"""In-memory stand-ins used only by the long-run driver's own unit tests.

They imitate the statements the driver sends so that its bookkeeping can be
checked without a server. They say nothing about any database.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import re
import threading
from typing import Callable, Dict, List, Optional, Sequence, Set

from p0_longrun_load import Endpoint
from p0_longrun_pq import Result


def ok(command: str, rows: Optional[List[List[Optional[str]]]] = None) -> Result:
    return Result(True, command, rows or [], None, None, None, None, None, None, False)


def error(sqlstate: Optional[str], message: str, severity: str = 'ERROR',
          lost: bool = False, detail: Optional[str] = None) -> Result:
    return Result(False, '', [], severity if sqlstate else None, sqlstate, message, detail,
                  None, 'fake, fake.c:1' if sqlstate else None, lost)


class FakeServer:
    """One shared table reachable through every fake connection."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.table: Dict[int, int] = {}
        self.tables: Set[str] = set()
        self.failing_rows: Dict[int, Result] = {}
        self.blocked_rows: Set[int] = set()
        self.commit_failure: Optional[Result] = None
        self.lose_commits_for: Set[int] = set()
        self.down = threading.Event()
        self.release = threading.Event()
        self.statements = 0
        self.catalog_version = '0'

    def connect(self, endpoint: Endpoint) -> 'FakeConnection':
        return FakeConnection(self, endpoint)


class FakeConnection:
    def __init__(self, server: FakeServer, endpoint: Endpoint) -> None:
        self.server, self.endpoint = server, endpoint
        self.backend_pid: Optional[int] = None
        self.pending: Optional[int] = None
        self.failed = False
        self.open = False
        self.cancelled = threading.Event()

    def connect(self) -> Optional[str]:
        if self.server.down.is_set():
            return 'connection refused by the fake server'
        self.open, self.backend_pid = True, 4000 + self.endpoint.node
        return None

    def connected(self) -> bool:
        return self.open

    def cancel(self) -> bool:
        self.cancelled.set()
        return True

    def close(self) -> None:
        self.open = False

    def execute(self, sql: str) -> Result:
        server = self.server
        if server.down.is_set() or not self.open:
            self.open = False
            return error(None, 'server closed the connection unexpectedly', lost=True)
        with server.lock:
            server.statements += 1
        if sql == 'BEGIN':
            self.pending, self.failed = None, False
            return ok('BEGIN')
        if sql == 'ROLLBACK':
            self.pending = None
            return ok('ROLLBACK')
        if sql == 'COMMIT':
            row, self.pending = self.pending, None
            if self.failed:
                return ok('ROLLBACK')
            if row in server.lose_commits_for:
                self.open = False
                with server.lock:
                    server.table[row] += 1
                return error(None, 'server closed the connection unexpectedly', lost=True)
            if server.commit_failure is not None:
                return server.commit_failure
            if row is not None:
                with server.lock:
                    server.table[row] += 1
            return ok('COMMIT')
        match = re.fullmatch(r'UPDATE lab\.stop_load SET v=v\+1 WHERE id=(\d+)', sql)
        if match:
            row = int(match.group(1))
            if row in server.blocked_rows:
                while not (self.cancelled.is_set() or server.release.is_set()):
                    self.cancelled.wait(0.01)
                if self.cancelled.is_set():
                    self.failed = True
                    return error('57014', 'canceling statement due to user request')
            if row in server.failing_rows:
                self.failed = True
                return server.failing_rows[row]
            if row not in server.table:
                return ok('UPDATE 0')
            self.pending = row
            return ok('UPDATE 1')
        return self.setup(sql)

    def setup(self, sql: str) -> Result:
        server = self.server
        if sql.startswith(('CREATE SCHEMA', 'CREATE FUNCTION', 'CREATE TRIGGER')):
            return ok('CREATE')
        match = re.match(r'CREATE TABLE (\S+?)\(', sql)
        if match:
            server.tables.add(match.group(1))
            return ok('CREATE TABLE')
        match = re.match(r'INSERT INTO (\S+) SELECT g,(0|g) FROM generate_series\((\d+),(\d+)\) g',
                         sql)
        if match:
            if match.group(1) == 'lab.stop_load':
                with server.lock:
                    for row in range(int(match.group(3)), int(match.group(4)) + 1):
                        server.table[row] = 0
            return ok('INSERT 0 %d' % (int(match.group(4)) - int(match.group(3)) + 1))
        if sql.startswith('SELECT count(*),coalesce(sum(v),0),count(DISTINCT'):
            with server.lock:
                return ok('SELECT 1', [[str(len(server.table)), str(sum(server.table.values())),
                                        str(max(1, len(server.table) // 180))]])
        if sql == 'SELECT count(*) FROM lab.stop_load':
            with server.lock:
                return ok('SELECT 1', [[str(len(server.table))]])
        if sql == 'SELECT id,v FROM lab.stop_load WHERE v<>0':
            with server.lock:
                return ok('SELECT', [[str(k), str(v)] for k, v in sorted(server.table.items())
                                     if v])
        if sql == 'SELECT count(*),sum(v) FROM lab.drop_after_load':
            return ok('SELECT 1', [['64', '2080']])
        if 'FROM pg_control_system()' in sql:
            return ok('SELECT 1', [['7|' + server.catalog_version]])
        if sql.startswith("SELECT current_setting('cluster.enabled')"):
            return ok('SELECT 1', [['on|on|on|on|on']])
        if sql == 'SHOW cluster.node_id':
            return ok('SHOW', [[str(self.endpoint.node)]])
        if sql.startswith('DROP TABLE'):
            server.tables.discard(sql.split()[2])
            return ok('DROP TABLE')
        return error('42601', 'the fake server does not know: ' + sql)


class FakeProvider:
    """Lifecycle stand-in; writes one fake log line per lifecycle call."""
    kind = 'selftest-plain-single'

    def __init__(self, root: object, server: FakeServer, members: int = 1,
                 fail: Sequence[str] = ()) -> None:
        from pathlib import Path
        self.root, self.server, self.fail = Path(str(root)), server, set(fail)
        self.members = [dict(id=n, logfile=str(self.root / ('node%d.log' % n)))
                        for n in range(members)]
        self.running = False
        self.calls: List[str] = []

    def endpoints(self) -> List[Endpoint]:
        return [Endpoint(int(m['id']), 'fake %d' % m['id']) for m in self.members]

    def log(self, text: str) -> None:
        for member in self.members:
            with open(str(member['logfile']), 'a') as stream:
                stream.write('2026-01-01 00:00:00.000 UTC [100] %s\n' % text)

    def call(self, name: str, running: bool) -> Dict[str, object]:
        self.calls.append(name)
        if name in self.fail:
            raise RuntimeError('fake %s failure' % name)
        self.running = running
        self.log('LOG:  fake %s' % name)
        return dict(step=name)

    def init(self) -> Dict[str, object]:
        self.root.mkdir(parents=True, exist_ok=True)
        return self.call('init', False)

    def start(self) -> Dict[str, object]:
        self.server.down.clear()
        return self.call('start', True)

    def stop(self) -> Dict[str, object]:
        return self.call('stop', False)

    def crash(self) -> Dict[str, object]:
        self.server.down.set()
        self.running = False
        return dict(signal='SIGKILL', processes=1)


Connect = Callable[[Endpoint], FakeConnection]
