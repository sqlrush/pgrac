"""Lifecycle of the native four-member fixture for the long-run driver.

Creation, start, OPEN, normal stop and the CLOSED proof all go through the
existing blackbox.py adapter protocol and its native lifecycle observations;
this module adds no product action of its own except an explicit SIGKILL of
every server process for the optional crash step.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Callable, Dict, List, Optional, Sequence

from first_chain import check_shutdown
from p0_longrun_load import Endpoint
from p0_longrun_pq import quote_conninfo

REQUIRED_OPERATIONS = ('fresh_init', 'shared_start', 'shared_stop', 'root_observation',
                       'open_observation', 'shutdown_observation')


class ProviderError(RuntimeError):
    pass


def catalog_version(header: Path) -> str:
    found = re.findall(r'^\s*#\s*define[ \t]+CATALOG_VERSION_NO\b(.*)$',
                       Path(header).read_text(), re.M)
    if len(found) != 1 or not re.fullmatch(r'\s+[1-9][0-9]*\s*', found[0]):
        raise ProviderError('invalid source catalog version')
    return found[0].strip()


def node_layout(root: Path, count: int = 4, base_port: int = 18000) -> Dict[str, object]:
    """The member layout used by the existing local cohort probe."""
    root = Path(root)
    nodes = [dict(id=n, data_dir=str(root / ('node%d' % n) / 'pgdata'), port=base_port + n,
                  host=str(root / 'sockets'), ic_port=base_port + 1000 + n,
                  data_port=base_port + 2000 + 2 * n, logfile=str(root / ('node%d.log' % n)))
             for n in range(count)]
    return dict(name='p0_longrun', root=str(root), shared_data_dir=str(root / 'data'),
                wal_root=str(root / 'wal'), nodes=nodes, extra_conf=[])


def query_one(conn: object, sql: str, what: str) -> str:
    result = conn.execute(sql)
    if not result.ok or len(result.rows) != 1 or result.rows[0][0] is None:
        raise ProviderError('%s: %s' % (what, result.message or 'unexpected result'))
    return str(result.rows[0][0])


def positive(value: object) -> bool:
    return type(value) is int and value > 0


class CohortProvider:
    kind = 'native-four-member'

    def __init__(self, layout: Dict[str, object], bindir: Path, entry_file: Path, pre2: Path,
                 user: str, log_dir: Path, connect: Callable[[Endpoint], object],
                 runner: Callable[..., object] = subprocess.run,
                 catalog_header: Optional[Path] = None) -> None:
        self.layout, self.bindir, self.entry_file = layout, Path(bindir), Path(entry_file)
        self.pre2, self.user, self.log_dir = Path(pre2), user, Path(log_dir)
        self.catalog_header = catalog_header or Path(__file__).resolve().parents[3] \
            / 'include/catalog/catversion.h'
        self.connect, self.runner = connect, runner
        self.members: List[Dict[str, object]] = list(layout['nodes'])
        self.system_identifier: Optional[str] = None
        self.writers: List[Dict[str, object]] = []
        self.running = False

    def endpoints(self) -> List[Endpoint]:
        return [Endpoint(int(m['id']), 'host=%s port=%d dbname=postgres user=%s connect_timeout=10'
                         % (quote_conninfo(m['host']), int(m['port']), quote_conninfo(self.user)))
                for m in self.members]

    def adapter(self, op: str, args: Optional[Dict[str, object]] = None,
                budget: float = 120.0) -> Dict[str, object]:
        if budget <= 0:
            raise ProviderError('fixture operation deadline expired: ' + op)
        request = dict(version=1, op=op, layout=self.layout, args=args or {},
                       budget_seconds=budget)
        env = dict(os.environ, PGRAC_PRE2_BINDIR=str(self.bindir),
                   PGRAC_PRE2_ENTRY_FILE=str(self.entry_file), TESTLOGDIR=str(self.log_dir),
                   PATH=str(self.bindir) + os.pathsep + os.environ.get('PATH', ''),
                   PYTHONDONTWRITEBYTECODE='1')
        try:
            done = self.runner([sys.executable, str(self.pre2 / 'blackbox.py')],
                               input=json.dumps(request), text=True, capture_output=True,
                               timeout=budget + 30, env=env)
        except subprocess.TimeoutExpired as error:
            raise ProviderError('fixture adapter %s exceeded its budget' % op) from error
        try:
            reply = json.loads(done.stdout)
        except ValueError as error:
            raise ProviderError('fixture adapter %s gave no reply: %s'
                                % (op, (done.stderr or done.stdout)[-400:])) from error
        if not isinstance(reply, dict) or reply.get('version') != 1:
            raise ProviderError('invalid fixture adapter reply for ' + op)
        if reply.get('status') == 'BLOCKED':
            raise ProviderError('BLOCKED: ' + str(reply.get('dependency')))
        if reply.get('status') != 'OK':
            raise ProviderError('%s failed: %s' % (op, reply.get('reason', 'missing OK')))
        return reply

    def init(self) -> Dict[str, object]:
        capabilities = set(self.adapter('describe').get('capabilities', []))
        missing = [op for op in REQUIRED_OPERATIONS if op not in capabilities]
        if missing:
            raise ProviderError('BLOCKED: fixture adapter lacks ' + ', '.join(missing))
        self.adapter('fresh_init')
        for member in self.members:
            data = Path(str(member['data_dir']))
            if not (data / 'global/pg_control').is_file() or (data / 'backup_label').exists():
                raise ProviderError('fresh initializer did not create native control')
        return dict(members=len(self.members))

    def start(self) -> Dict[str, object]:
        self.running = True     # a failed start still needs explicit cleanup
        self.adapter('shared_start')
        wanted_catalog = catalog_version(self.catalog_header)
        identifiers = set()
        for endpoint in self.endpoints():
            conn = self.connect(endpoint)
            error = conn.connect()
            if error is not None:
                raise ProviderError('member %d refused its first session: %s'
                                    % (endpoint.node, error))
            try:
                identity = query_one(conn, "SELECT system_identifier::text || '|' || "
                                     'catalog_version_no FROM pg_control_system()', 'identity')
                sysid, _, catalog = identity.partition('|')
                if catalog != wanted_catalog or not re.fullmatch('[1-9][0-9]*', sysid):
                    raise ProviderError('member %d control identity is not this format'
                                        % endpoint.node)
                identifiers.add(sysid)
                mode = query_one(conn, "SELECT " + " || '|' || ".join(
                    "current_setting('%s')" % name for name in (
                        'cluster.enabled', 'cluster.shared_catalog', 'cluster.shared_config',
                        'cluster.controlfile_shared_authority', 'cluster.merged_recovery')),
                    'configuration')
                if mode != 'on|on|on|on|on':
                    raise ProviderError('member %d is not the shared configuration'
                                        % endpoint.node)
                if query_one(conn, 'SHOW cluster.node_id', 'node id') != str(endpoint.node):
                    raise ProviderError('configured member identity differs')
            finally:
                conn.close()
        if len(identifiers) != 1:
            raise ProviderError('members report different system identifiers')
        sysid = identifiers.pop()
        if self.system_identifier not in (None, sysid):
            raise ProviderError('system identifier changed across restart')
        self.system_identifier = sysid
        root = self.wait_open()
        return dict(system_identifier=sysid, root_generation=root['generation'],
                    root_digest=root['root_digest'], writers=self.writers)

    def root(self, budget: float) -> Dict[str, object]:
        root = self.adapter('root_observation', budget=budget)
        if (root.get('system_identifier') != self.system_identifier
                or not re.fullmatch('[0-9a-f]{64}', str(root.get('root_digest')))
                or not positive(root.get('generation'))):
            raise ProviderError('missing selected current root observation')
        return root

    def wait_open(self, seconds: float = 60.0) -> Dict[str, object]:
        """Every member OPEN on one selected root, with distinct exact writers."""
        deadline = time.monotonic() + seconds
        ids = [int(m['id']) for m in self.members]

        def left() -> float:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProviderError('not all members reached OPEN within %d seconds' % seconds)
            return remaining

        while True:
            writers, samples = [], []
            for node in ids:
                sample = self.adapter('open_observation', dict(node=node), left())
                if sample.get('phase') != 'OPEN':
                    continue
                writer = sample.get('writer')
                if (sample.get('system_identifier') != self.system_identifier
                        or sample.get('members') != ids or not isinstance(writer, dict)
                        or writer.get('node') != node or not writer.get('boot')
                        or not all(positive(writer.get(field)) for field in
                                   ('thread', 'incarnation', 'wal_generation'))):
                    raise ProviderError('OPEN lacks exact current identity on member %d' % node)
                writers.append(writer)
                samples.append(sample)
            if len(writers) == len(ids):
                if len({w['thread'] for w in writers}) != len(writers):
                    raise ProviderError('OPEN reused one WAL thread on multiple members')
                root = self.root(left())
                if all(s.get('root_digest') == root['root_digest']
                       and s.get('generation') == root['generation'] for s in samples):
                    left()      # a late success is still a deadline failure
                    self.writers = writers
                    return root
            left()
            time.sleep(0.1)

    def stop(self) -> Dict[str, object]:
        """Fast shutdown of every member, then the native CLOSED proof."""
        ids = [int(m['id']) for m in self.members]
        exited = self.adapter('shared_stop', dict(nodes=ids))
        self.running = False
        receipt = self.adapter('shutdown_observation', dict(nodes=ids, writers=self.writers))
        receipt['all_processes_exited'] = exited.get('all_processes_exited')
        try:
            check_shutdown(receipt, self.system_identifier, self.writers)
        except (ValueError, KeyError) as error:
            raise ProviderError('CLOSED proof refused: %s' % error) from error
        return dict(all_processes_exited=True, final_checkpoint=True,
                    closed_writers=len(receipt['writers']))

    def crash(self) -> Dict[str, object]:
        """SIGKILL every process of every member at once (Linux /proc)."""
        if not Path('/proc/self/stat').exists():
            raise ProviderError('the crash step needs Linux /proc')
        from cohort_probe import process_tree
        victims: Dict[int, tuple] = {}
        postmasters = []
        for member in self.members:
            lines = (Path(str(member['data_dir'])) / 'postmaster.pid').read_text().splitlines()
            pid = int(lines[0])
            exe = Path('/proc/%d/exe' % pid).resolve(strict=True)
            argv = Path('/proc/%d/cmdline' % pid).read_bytes().split(b'\0')
            if (exe != (self.bindir / 'postgres').resolve() or not any(
                    argv[i] == b'-D' and argv[i + 1] == os.fsencode(str(member['data_dir']))
                    for i in range(len(argv) - 1))):
                raise ProviderError('refusing to signal a process that is not this member')
            postmasters.append(pid)
            victims.update(process_tree(pid))
        handles = {}
        try:
            for pid, identity in victims.items():
                try:
                    handle = os.pidfd_open(pid)
                except ProcessLookupError:
                    continue
                parts = Path('/proc/%d/stat' % pid).read_text().rsplit(')', 1)[1].split()
                if (int(parts[1]), int(parts[19])) != identity:
                    os.close(handle)
                    raise ProviderError('a member process changed before the crash step')
                handles[pid] = handle
            started = time.time()
            # Postmasters first, so none of them reacts to a dying child.
            for pid in postmasters + [p for p in handles if p not in postmasters]:
                try:
                    signal.pidfd_send_signal(handles[pid], signal.SIGKILL)
                except (ProcessLookupError, KeyError):
                    pass
            deadline = time.monotonic() + 30
            while any(Path('/proc/%d' % pid).exists() for pid in handles):
                if time.monotonic() >= deadline:
                    raise ProviderError('a killed member process did not disappear')
                time.sleep(0.05)
        finally:
            for handle in handles.values():
                os.close(handle)
        self.running = False
        return dict(signal='SIGKILL', postmasters=postmasters, processes=len(handles),
                    started_unix=started, finished_unix=time.time())


def log_offsets(members: Sequence[Dict[str, object]]) -> Dict[str, int]:
    offsets = {}
    for member in members:
        path = Path(str(member['logfile']))
        offsets[str(member['id'])] = path.stat().st_size if path.exists() else 0
    return offsets
