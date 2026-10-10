#!/usr/bin/env python3
"""Start, inspect, exercise and normally stop the four-node PRE2 appliance."""

import argparse
import fcntl
import json
import os
import re
from pathlib import Path
import shlex
import time

import cluster
import guest

PGDATA = '/srv/pgrac/node/data'
SOCKET = '/srv/pgrac/node/log/socket'
LOG = '/srv/pgrac/node/log/postgres.log'
CONFIG = '/etc/pgrac/pre2-bootstrap.conf'
OUT = Path('/var/log/pgrac-appliance')


def sql(node, query, timeout=180):
    return guest.execute(node, ['/usr/sbin/runuser', '-u', 'pgrac', '--',
        '/opt/pgrac/bin/psql', '-X', '-v', 'ON_ERROR_STOP=1', '-h', SOCKET,
        '-U', 'pgrac', '-d', 'postgres', '-At', '-c', query], timeout=timeout)


def pg_state(node):
    return guest.execute(node, ['/usr/sbin/runuser', '-u', 'pgrac', '--',
        '/opt/pgrac/bin/pg_ctl', '-D', PGDATA, 'status'], check=False)


def start():
    cluster.boot_guests()
    cluster.services()
    guest.execute(0, ['/usr/sbin/pcs', 'resource', 'enable', 'pgrac-locking-clone', '--wait=120'], timeout=150)
    guest.execute(0, ['/usr/sbin/pcs', 'resource', 'enable', 'pgrac-data-clone', '--wait=120'], timeout=150)
    cluster.wait_storage()
    starttime = time.monotonic()
    offsets = {}
    def one(n):
        state = pg_state(n)
        if state['rc'] == 0:
            offsets[n] = 0
            return state
        if state['rc'] != 3:
            raise RuntimeError('unrecognized PostgreSQL state on node' + str(n))
        size = guest.execute(n, ['/usr/bin/stat', '-c', '%s', LOG], check=False)
        offsets[n] = int(size['stdout']) if size['rc'] == 0 else 0
        return guest.execute(n, ['/usr/sbin/runuser', '-u', 'pgrac', '--',
            '/opt/pgrac/bin/pg_ctl', '-D', PGDATA, '-l', LOG, '-W',
            '-o', '-c config_file=' + CONFIG, 'start'])
    cluster.all_nodes(one)
    end = time.monotonic() + 180
    ready = set()
    while time.monotonic() < end:
        for n in set(range(4)) - ready:
            try:
                startup = guest.execute(n, ['/usr/bin/tail', '-c', '+' + str(offsets[n] + 1), LOG])['stdout']
                if 'database system is ready to accept connections' in startup and sql(n, 'SELECT 1', 10)['stdout'].strip() == '1':
                    ready.add(n)
            except Exception:
                pass
        if len(ready) == 4:
            return {'status': 'STARTED', 'nodes': sorted(ready), 'database_start_seconds': time.monotonic() - starttime}
        time.sleep(1)
    raise RuntimeError('database startup did not complete; see node logs')


def status():
    return {'nodes': cluster.all_nodes(lambda n: {'node': n, 'postgres': pg_state(n),
        'quorum': guest.execute(n, ['/usr/sbin/corosync-quorumtool', '-s'], check=False),
        'filesystem': guest.execute(n, ['/usr/bin/findmnt', '/srv/pgrac/shared'], check=False)})}


def example():
    # Small ordinary transactions; this is a function check, not a performance run.
    begin = time.monotonic()
    before = cluster.all_nodes(lambda n: sql(n, 'SELECT count(*), count(DISTINCT id) FROM demo.accounts;'))
    if any(r['stdout'].strip() != '50000|50000' for r in before):
        raise RuntimeError('unexpected example row count / primary-key uniqueness')
    original = sql(0, 'SELECT id,balance FROM demo.accounts WHERE id BETWEEN 1 AND 4 ORDER BY id;')['stdout'].strip()
    expected = '\n'.join('%s|%d' % (line.split('|')[0], int(line.split('|')[1]) + 20)
                         for line in original.splitlines())
    def one(n):
        statements = ''.join('BEGIN; UPDATE demo.accounts SET balance=balance+1 WHERE id=%d; COMMIT;\n' % (n + 1)
                             for _ in range(20))
        return sql(n, statements)
    writes = cluster.all_nodes(one)
    after = cluster.all_nodes(lambda n: sql(n, 'SELECT id,balance FROM demo.accounts WHERE id BETWEEN 1 AND 4 ORDER BY id;'))
    rows = [r['stdout'].strip() for r in after]
    if len(set(rows)) != 1 or rows[0] != expected:
        raise RuntimeError('example rows differ between members or updates were not preserved')
    return {'status': 'EXAMPLE_PASS', 'transactions': 80, 'nodes': 4,
            'elapsed_seconds': time.monotonic()-begin, 'rows': rows[0], 'counts': before,
            'transaction_results': writes}


def stop(poweroff=False):
    cluster.manifest()
    states = cluster.all_nodes(pg_state)
    if any(row['rc'] not in (0, 3) for row in states):
        raise RuntimeError('cannot establish database state; no forced stop performed')
    offsets = cluster.all_nodes(lambda n: int(guest.execute(n, ['/usr/bin/stat', '-c', '%s', LOG])['stdout']))
    begin = time.monotonic()
    def one(n):
        if states[n]['rc'] == 3:
            return {'rc': 0, 'stdout': 'already stopped', 'stderr': '', 'seconds': 0, 'already_stopped': True}
        since = time.monotonic()
        result = guest.execute(n, ['/usr/sbin/runuser', '-u', 'pgrac', '--',
            '/opt/pgrac/bin/pg_ctl', '-D', PGDATA, '-m', 'fast', '-w', '-t', '30', 'stop'],
            timeout=45, check=False)
        result['seconds'] = time.monotonic() - since
        result['log'] = guest.execute(n, ['/usr/bin/tail', '-c', '+' + str(offsets[n] + 1), LOG])['stdout']
        result['normal_shutdown_record'] = 'database system is shut down' in result['log']
        result['error_events'] = re.findall(r'\b(?:ERROR|FATAL|PANIC):[^\n]*', result['log'])
        return result
    results = cluster.all_nodes(one)
    after = cluster.all_nodes(pg_state)
    ok = (all(row['rc'] == 0 and (row.get('already_stopped') or
              (row.get('normal_shutdown_record') and not row.get('error_events'))) for row in results)
          and all(row['rc'] == 3 for row in after))
    report = {'status': 'STOPPED' if ok else 'NORMAL_STOP_FAILED', 'nodes': results,
              'elapsed_seconds': time.monotonic()-begin, 'after': after}
    if not ok:
        return report
    if poweroff:
        # Prevent DLM/FS clones from observing a peer going away during OS shutdown.
        guest.execute(0, ['/usr/sbin/pcs', 'resource', 'disable', 'pgrac-data-clone', '--wait=120'], timeout=150)
        guest.execute(0, ['/usr/sbin/pcs', 'resource', 'disable', 'pgrac-locking-clone', '--wait=120'], timeout=150)
        cluster.all_nodes(lambda n: guest.execute(n, ['/usr/bin/systemctl', 'stop', 'pacemaker'], timeout=120))
        cluster.all_nodes(lambda n: guest.execute(n, ['/usr/bin/systemctl', 'stop', 'corosync-qdevice', 'corosync']))
        for name in guest.NAMES:
            guest.command(['virsh', 'shutdown', name])
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if all(guest.command(['virsh', 'domstate', name]).stdout.strip() == 'shut off' for name in guest.NAMES):
                report['guests'] = 'POWERED_OFF'
                return report
            time.sleep(2)
        raise RuntimeError('guest OS did not stop; parent must stay running')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'status', 'example', 'stop', 'sql'])
    parser.add_argument('--poweroff', action='store_true', help='stop cluster resources and guest OS after clean PG stop')
    parser.add_argument('--node', type=int, choices=range(4), default=0)
    parser.add_argument('--query', help='SQL for the selected node; do not put credentials in SQL argv')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('sudo is required')
    OUT.mkdir(mode=0o700, exist_ok=True)
    with open('/run/pgrac-appliance.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cluster.manifest()
        if args.action == 'sql':
            if not args.query:
                parser.error('--query is required')
            result = sql(args.node, args.query)
        elif args.action == 'stop':
            result = stop(args.poweroff)
        else:
            result = globals()[args.action]()
        text = json.dumps(result, ensure_ascii=False, indent=2)
        (OUT / (time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()) + '-' + args.action + '.json')).write_text(text+'\n')
        print(text)
        if result.get('status') == 'NORMAL_STOP_FAILED':
            raise SystemExit(1)


if __name__ == '__main__':
    main()
