"""One ordinary PostgreSQL instance, for checking the long-run driver itself.

This exercises the workload client, the ledger, the log reader and the judge
on a machine without the cluster fixture. It is a single non-shared server:
a run on it says nothing about four-member behaviour and is never reported
as an acceptance result.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import time
from typing import Callable, Dict, List, Optional, Sequence

from p0_longrun_cluster import ProviderError, query_one
from p0_longrun_load import Endpoint
from p0_longrun_pq import quote_conninfo


class PlainProvider:
    kind = 'selftest-plain-single'

    def __init__(self, root: Path, bindir: Path, port: int, connect: Callable[[Endpoint], object],
                 extra_conf: Sequence[str] = ()) -> None:
        self.root, self.bindir, self.port = Path(root), Path(bindir), port
        self.connect, self.extra_conf = connect, tuple(extra_conf)
        self.data = self.root / 'node0' / 'pgdata'
        self.sockets = self.root / 'sockets'
        self.members: List[Dict[str, object]] = [dict(
            id=0, data_dir=str(self.data), port=port, host=str(self.sockets),
            logfile=str(self.root / 'node0.log'))]
        self.system_identifier: Optional[str] = None
        self.running = False

    def tool(self, name: str, argv: Sequence[str], seconds: float = 120.0) -> str:
        env = dict(os.environ, LC_ALL='C')
        for variable in ('PGDATA', 'PGPORT', 'PGHOST', 'PGUSER', 'PGDATABASE'):
            env.pop(variable, None)
        try:
            done = subprocess.run([str(self.bindir / name), *argv], text=True,
                                  capture_output=True, timeout=seconds, env=env)
        except subprocess.TimeoutExpired as error:
            raise ProviderError('%s exceeded %d seconds' % (name, seconds)) from error
        if done.returncode != 0:
            raise ProviderError('%s rc=%d: %s' % (name, done.returncode,
                                                  (done.stderr or done.stdout)[-600:]))
        return done.stdout

    def endpoints(self) -> List[Endpoint]:
        return [Endpoint(0, 'host=%s port=%d dbname=postgres connect_timeout=10'
                         % (quote_conninfo(self.sockets), self.port))]

    def init(self) -> Dict[str, object]:
        self.sockets.mkdir(parents=True)
        self.tool('initdb', ['-D', str(self.data), '-A', 'trust', '--no-locale', '-E', 'UTF8'])
        with (self.data / 'postgresql.conf').open('a') as conf:
            conf.write("\nlisten_addresses = ''\nport = %d\nunix_socket_directories = '%s'\n"
                       % (self.port, str(self.sockets).replace("'", "''")))
            conf.write(''.join(line + '\n' for line in self.extra_conf))
        return dict(members=1)

    def start(self) -> Dict[str, object]:
        self.running = True
        self.tool('pg_ctl', ['start', '-D', str(self.data), '-l',
                             str(self.members[0]['logfile']), '-w', '-t', '60'])
        conn = self.connect(self.endpoints()[0])
        error = conn.connect()
        if error is not None:
            raise ProviderError('server refused its first session: ' + error)
        try:
            sysid = query_one(conn, 'SELECT system_identifier::text FROM pg_control_system()',
                              'identity')
        finally:
            conn.close()
        if self.system_identifier not in (None, sysid):
            raise ProviderError('system identifier changed across restart')
        self.system_identifier = sysid
        return dict(system_identifier=sysid)

    def stop(self) -> Dict[str, object]:
        log = Path(str(self.members[0]['logfile']))
        offset = log.stat().st_size
        self.tool('pg_ctl', ['stop', '-D', str(self.data), '-m', 'fast', '-w', '-t', '60'])
        self.running = False
        with log.open('rb') as stream:
            stream.seek(offset)
            tail = stream.read().decode(errors='replace')
        if 'database system is shut down' not in tail:
            raise ProviderError('missing normal shutdown record')
        if (self.data / 'postmaster.pid').exists():
            raise ProviderError('postmaster.pid survived the normal stop')
        return dict(all_processes_exited=True, final_checkpoint=True, closed_writers=1)

    def crash(self) -> Dict[str, object]:
        """SIGKILL the postmaster and all of its children."""
        postmaster = int((self.data / 'postmaster.pid').read_text().splitlines()[0])
        table = subprocess.run(['ps', '-axo', 'pid=,ppid='], text=True, capture_output=True,
                               check=True, timeout=10).stdout
        parents = {int(a): int(b) for a, b in (line.split() for line in table.splitlines()
                                               if len(line.split()) == 2)}
        victims = [postmaster] + [pid for pid, parent in parents.items() if parent == postmaster]
        started = time.time()
        for pid in victims:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 30
        while True:
            alive = []
            for pid in victims:
                try:
                    os.kill(pid, 0)
                    alive.append(pid)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    alive.append(pid)
            if not alive:
                break
            if time.monotonic() >= deadline:
                raise ProviderError('a killed server process did not disappear')
            time.sleep(0.05)
        self.running = False
        return dict(signal='SIGKILL', postmasters=[postmaster], processes=len(victims),
                    started_unix=started, finished_unix=time.time())
