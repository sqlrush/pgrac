#!/usr/bin/env python3
"""Real local PRE2 startup probe; preserves the first failure and all DATA.

Uses an installed candidate, native initdb and a real namespace quorum.
It does not certify fencing, recovery or successful normal shutdown.
Author: SqlRush <sqlrush@gmail.com>
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import signal
import subprocess
import time

from cohort_entry import CohortEntry, entry_profile
from local_quorum import LocalQuorum


def process_tree(root):
    identities = {}
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            parts = path.read_text().rsplit(')', 1)[1].split()
            identities[int(path.parent.name)] = (int(parts[1]), int(parts[19]))
        except (OSError, ValueError, IndexError):
            pass
    owned = {root}
    while True:
        expanded = owned | {pid for pid, (parent, _) in identities.items() if parent in owned}
        if owned == expanded:
            return {pid: identities[pid] for pid in owned if pid in identities}
        owned = expanded


def cleanup(q, nodes, bindir):
    # Error cleanup is recorded separately; it cannot establish a CLOSED chain.
    for node in nodes:
        pidfile = Path(node['data_dir'])/'postmaster.pid'
        if not pidfile.exists():
            continue
        lines = pidfile.read_text().splitlines()
        if len(lines) < 2 or Path(lines[1]).resolve() != Path(node['data_dir']).resolve():
            raise RuntimeError('cleanup DATA identity differs')
        pid = int(lines[0])
        try:
            command = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
            exe = Path(f'/proc/{pid}/exe').resolve(strict=True)
        except FileNotFoundError:
            continue
        if exe != (bindir/'postgres').resolve() or not any(
                command[i] == b'-D' and command[i+1] == os.fsencode(node['data_dir'])
                for i in range(len(command)-1)):
            raise RuntimeError('cleanup postmaster executable or DATA differs')
        handles = []
        for child, identity in process_tree(pid).items():
            try:
                fd = os.pidfd_open(child)
                parts = Path(f'/proc/{child}/stat').read_text().rsplit(')', 1)[1].split()
                if (int(parts[1]), int(parts[19])) != identity:
                    os.close(fd)
                    raise RuntimeError('cleanup child lifetime changed')
                handles.append(fd)
            except (ProcessLookupError, FileNotFoundError):
                pass
        try:
            q.run(q.ns(node['id'], [bindir/'pg_ctl', '-D', node['data_dir'], '-m', 'immediate',
                                   '-w', '-t', '5', 'stop'], database=True), check=False, timeout=8)
            for fd in handles:
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        finally:
            for fd in handles:
                os.close(fd)
    time.sleep(.2)


def probe(args):
    root = args.output.resolve()
    root.mkdir(parents=True, mode=0o700, exist_ok=False)
    os.chown(root, args.uid, args.gid)
    bindir = args.install.resolve()/'bin'
    result = dict(source=args.source, binary_sha256=hashlib.sha256((bindir/'postgres').read_bytes()).hexdigest(),
                  count=args.nodes, steps={}, first_error=None, cleanup='NOT_RUN')
    q = LocalQuorum(root/'quorum', args.vendor, args.nodes, args.uid, args.gid)
    q.env['LD_LIBRARY_PATH'] += ':' + str(args.install.resolve()/'lib')
    nodes = []
    try:
        q.start()
        result['steps']['storage_quorum'] = 'PASS'
        sockets = root/'sockets'
        sockets.mkdir()
        os.chown(sockets, args.uid, args.gid)
        nodes = [dict(id=n, data_dir=str(root/f'node{n}/pgdata'), port=18000+n,
                      host=str(sockets), ic_port=19000+n, data_port=20000+2*n,
                      logfile=str(root/f'node{n}.log')) for n in range(args.nodes)]
        layout = dict(name=q.name, root=str(root), shared_data_dir=str(root/'data'),
                      wal_root=str(root/'wal'), nodes=nodes, extra_conf=[])
        votes = []
        for n in range(3):
            path = root/f'vote{n}.image'
            q.run(['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups',
                   args.vote_formatter.resolve(), path, str(n)])
            votes.append(q.attach_vote(path))
            q.run(['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups',
                   args.vote_formatter.resolve(), '--attest', votes[-1], str(n)])
        result['steps']['fresh_votes'] = 'PASS'
        profile = entry_profile(root/'quorum/handles.json', args.vote_formatter.resolve(), votes)
        if args.operations:
            profile['operations'] = json.loads(args.operations.read_text())
        (root/'entry.json').write_text(json.dumps(profile, indent=2)+'\n')
        if args.tap_source:
            from cohort_scenarios import run_taps
            result['scenarios'] = run_taps(q, args, root/'entry.json')
        driver = CohortEntry(layout, bindir, profile)
        driver.phase_budget(120)
        driver.fresh_init()
        config = root/'request.conf'
        result['config_sha256'] = hashlib.sha256(config.read_bytes()).hexdigest()
        result['layout'] = layout
        result['steps']['native_cohort'] = 'PASS'
        result['steps']['StartupXLOG'] = 'NOT_REACHED'
        driver.shared_start()
        result['steps']['sql_connections'] = 'PASS'
        # Actual cross-node SQL is the next independent obligation; no SQL retry.
        for n, sql in [(0, 'CREATE TABLE p3b_cohort_smoke(id int PRIMARY KEY, v int); '
                           'INSERT INTO p3b_cohort_smoke VALUES(1,1)'),
                       (1, 'BEGIN; UPDATE p3b_cohort_smoke SET v=v+1 WHERE id=1; COMMIT'),
                       (0, 'SELECT v FROM p3b_cohort_smoke WHERE id=1')]:
            node = nodes[n]
            reply = q.run([bindir/'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
                           '-U', pwd.getpwuid(args.uid).pw_name, '-h', node['host'],
                           '-p', str(node['port']), '-d', 'postgres', '-c', sql], timeout=30)
        if reply.stdout.strip() != '2':
            raise RuntimeError('cross-node committed value is not 2')
        result['steps']['cross_node_commit'] = 'PASS'
        # OPEN needs native semantic/formation observation in addition to SQL.
        result['steps']['R4_OPEN'] = 'NEEDS_NATIVE_OBSERVATION'
    except Exception as error:
        result['first_error'] = type(error).__name__+': '+str(error)
    finally:
        errors = []
        for node in nodes:
            path = Path(node['logfile'])
            if path.exists() and 'database system was shut down at' in path.read_text(errors='replace'):
                result['steps']['StartupXLOG_node'+str(node['id'])] = 'PASS'
        reached = sum(result['steps'].get('StartupXLOG_node'+str(node['id'])) == 'PASS'
                      for node in nodes)
        # Entry is diagnostic only: it cannot prove recovery completion or OPEN.
        result['steps']['StartupXLOG'] = ('ENTRY_REACHED' if nodes and reached == len(nodes)
                                         else 'ENTRY_PARTIAL' if reached else 'NOT_REACHED')
        try:
            cleanup(q, nodes, bindir)
        except Exception as error:
            errors.append('database cleanup: '+str(error))
        try:
            q.stop()
        except Exception as error:
            errors.append('quorum cleanup: '+str(error))
        result['cleanup'] = 'FAILED: '+ '; '.join(errors) if errors else 'OWNED_PROCESSES_STOPPED'
        (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    # SQL success alone is not an OPEN or normal-restart acceptance result.
    return 1 if result['first_error'] or errors else 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--vendor', type=Path, required=True)
    parser.add_argument('--install', type=Path, required=True)
    parser.add_argument('--vote-formatter', type=Path, required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--nodes', type=int, choices=[2, 4], required=True)
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--gid', type=int, required=True)
    parser.add_argument('--tap-source', type=Path, help='run unchanged 431/432/433 against this entry (four nodes)')
    parser.add_argument('--pg-regress', type=Path, help='pg_regress from a matching independent test build')
    parser.add_argument('--operations', type=Path, help='JSON mapping to actual product observations; no literal results')
    raise SystemExit(probe(parser.parse_args()))
