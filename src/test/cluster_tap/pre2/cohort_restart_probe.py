#!/usr/bin/env python3
"""Same-DATA fixed-cohort restart diagnostic using native PRE2 commands.

An error-cleanup restart is reported separately from normal S17/S18 shutdown.
Never writes runtime voting, ROOT or control bytes. Keeps every failed round.
Author: SqlRush <sqlrush@gmail.com>
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time

from cohort_entry import CohortEntry, entry_profile
from cohort_probe import cleanup
from local_quorum import LocalQuorum


class SlowLastEntry(CohortEntry):
    delay = 0

    def command(self, tool, argv, **kwargs):
        if (self.delay and tool == 'pg_ctl' and argv[0] == 'start'
                and self.layout['nodes'][-1]['data_dir'] in argv):
            self.record(dict(delayed_node=self.layout['nodes'][-1]['id'], seconds=self.delay))
            time.sleep(min(self.delay, self.remaining()))
        return super().command(tool, argv, **kwargs)


def formation_evidence(nodes, offsets):
    # Native diagnostics are evidence, never a substitute for R4 OPEN.
    pattern = re.compile(r'snapshot=1 formation_epoch=(\d+) formation_generation=(\d+) '
                         r'member_state=3 admitted_floor=(\d+) missing_floor_node=-1 '
                         r'fence_result=0 fence_proven=1 fence_majority=(\d+)/(\d+) fence_epoch=(\d+)')
    found = {}
    for node in nodes:
        path = Path(node['logfile'])
        if not path.exists():
            continue
        with path.open('rb') as stream:
            stream.seek(offsets[node['id']])
            lines = stream.read().decode(errors='replace').splitlines()
        for line in lines:
            match = pattern.search(line)
            if match:
                epoch, generation, incarnation, agree, total, fence = map(int, match.groups())
                if epoch > 0 and generation > 0 and incarnation > 0 and agree > total // 2 and fence == epoch:
                    found[node['id']] = dict(epoch=epoch, generation=generation,
                                            incarnation=incarnation, line=line)
    uniform = (len(found) == len(nodes) and len({(v['epoch'], v['generation']) for v in found.values()}) == 1)
    return dict(uniform=uniform, members=found)


def probe(args):
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.chown(root, args.uid, args.gid)
    bindir = args.install.resolve()/'bin'
    q = LocalQuorum(root/'quorum', args.vendor, args.nodes, args.uid, args.gid)
    q.env['LD_LIBRARY_PATH'] += ':' + str(args.install.resolve()/'lib')
    result = dict(source=args.source, binary_sha256=hashlib.sha256((bindir/'postgres').read_bytes()).hexdigest(),
                  rounds=[], setup_error=None, cleanup_errors=[])
    nodes = []
    try:
        q.start()
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
        profile = entry_profile(root/'quorum/handles.json', args.vote_formatter.resolve(), votes)
        (root/'entry.json').write_text(json.dumps(profile, indent=2)+'\n')
        driver = SlowLastEntry(layout, bindir, profile)
        driver.phase_budget(120)
        driver.fresh_init()  # Once only: no formatting or initialization on restart.
        driver.delay = args.slow_last_seconds
        result['layout'] = layout
        result['config_sha256'] = hashlib.sha256((root/'request.conf').read_bytes()).hexdigest()
        prior_normal = None
        for round_id in range(2):
            offsets = {n['id']: Path(n['logfile']).stat().st_size if Path(n['logfile']).exists() else 0
                       for n in nodes}
            report = dict(round=round_id, start_kind='FRESH' if round_id == 0 else
                          'AFTER_NORMAL_STOP' if prior_normal else 'AFTER_ERROR_CLEANUP',
                          sql_ready=False, first_error=None, normal_stop=False,
                          R4_OPEN='BLOCKED: native OPEN observation entry not supplied')
            result['rounds'].append(report)
            try:
                driver.deadline = None  # Each boot has the original 60-second budget.
                driver.shared_start()
                report['sql_ready'] = True
                if round_id == 0:
                    driver.sql(nodes[0], 'CREATE TABLE p3b_restart_smoke(id int PRIMARY KEY, v int); '
                                         'INSERT INTO p3b_restart_smoke VALUES(1,1)')
                    driver.sql(nodes[1], 'BEGIN; UPDATE p3b_restart_smoke SET v=v+1 WHERE id=1; COMMIT')
                for node in nodes:
                    value = driver.sql(node, 'SELECT v FROM p3b_restart_smoke WHERE id=1').stdout.strip()
                    if value != '2':
                        raise RuntimeError(f'node{node["id"]} committed value is {value!r}, expected 2')
                report['cross_node_data'] = 'PASS'
                driver.deadline = None
                driver.shared_stop({})
                report['normal_stop'] = True
            except Exception as error:
                report['first_error'] = type(error).__name__+': '+str(error)
            finally:
                report['formation'] = formation_evidence(nodes, offsets)
                prior_normal = report['normal_stop']
                if not prior_normal:
                    cleanup(q, nodes, bindir)
                (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        first, second = result['rounds']
        f, s = first['formation'], second['formation']
        result['persistent_successor'] = bool(f['uniform'] and s['uniform'] and all(
            s['members'][n]['epoch'] > f['members'][n]['epoch'] and
            s['members'][n]['generation'] > f['members'][n]['generation'] and
            s['members'][n]['incarnation'] != f['members'][n]['incarnation'] for n in f['members']))
        result['S17_S18'] = 'BLOCKED: requires proven OPEN and both normal stops'
    except Exception as error:
        result['setup_error'] = type(error).__name__+': '+str(error)
    finally:
        try:
            cleanup(q, nodes, bindir)
        except Exception as error:
            result['cleanup_errors'].append('database: '+str(error))
        try:
            q.stop()
        except Exception as error:
            result['cleanup_errors'].append('quorum: '+str(error))
        (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    return 1 if result['setup_error'] or result['cleanup_errors'] or any(
        r['first_error'] for r in result['rounds']) else 2  # never claim OPEN/S18 from SQL alone


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('output', 'vendor', 'install', 'vote-formatter'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--nodes', type=int, choices=[2, 4], required=True)
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--gid', type=int, required=True)
    parser.add_argument('--slow-last-seconds', type=float, choices=[0, 3], default=0)
    raise SystemExit(probe(parser.parse_args()))
