#!/usr/bin/env python3
"""Run required native TAP cases with a disposable real four-member quorum.

Requires Linux root in an isolated development/CI VM. Retains all DATA/logs;
only the original controller's processes, namespace and loop handles are freed.
Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import subprocess
import sys

import mvp_ci


def tap_result(output, rc):
    plans = [int(n) for n in re.findall(r'^1\.\.(\d+)\s*$', output, re.M)]
    good = len(re.findall(r'^ok \d+\b', output, re.M))
    bad = len(re.findall(r'^not ok \d+\b', output, re.M))
    skipped = len(re.findall(r'^.*#\s*(?:skip|todo)\b', output, re.I | re.M))
    complete = bool(plans) and sum(plans) > 0 and sum(plans) == good + bad
    return dict(rc=rc, assertions=good, failed=bad, skipped=skipped, complete=complete,
                passed=rc == 0 and complete and bad == 0 and skipped == 0)


def cleanup_targets(layout, case, handles, uid, proc=Path('/proc')):
    """Validate the original cohort before the existing cleanup sends signals."""
    owned = (case/'data').resolve(strict=True)
    if not layout.resolve(strict=True).is_relative_to(owned):
        raise ValueError('cleanup layout is outside its registered case')
    nodes = json.loads(layout.read_text())['nodes']
    if [node['id'] for node in nodes] != list(range(4)) or len(handles) < 4:
        raise ValueError('cleanup node identities differ from the original cohort')
    for node in nodes:
        data = Path(node['data_dir']).resolve(strict=True)
        if data == owned or not data.is_relative_to(owned):
            raise ValueError('cleanup DATA is outside its registered case')
        pidfile = data/'postmaster.pid'
        if not pidfile.exists():
            continue
        lines = pidfile.read_text().splitlines()
        if len(lines) < 2 or Path(lines[1]).resolve() != data or int(lines[0]) <= 1:
            raise ValueError('cleanup postmaster DATA identity differs')
        process = proc/lines[0]
        try:
            if (process.stat().st_uid != uid
                    or (process/'ns/net').stat().st_ino != handles[node['id']]['net_inode']):
                raise ValueError('cleanup postmaster owner or namespace differs')
        except FileNotFoundError:
            pass  # Original cleanup independently checks executable/DATA/lifetime.
    return nodes


def run(args):
    if sys.platform != 'linux' or os.geteuid() != 0 or args.uid <= 0:
        raise ValueError('native smoke requires isolated Linux root and a nonroot database UID')
    account = pwd.getpwuid(args.uid)
    if account.pw_gid != args.gid:
        raise ValueError('database UID/GID identity differs')
    if not re.fullmatch(r'[0-9a-f]{40}', args.commit):
        raise ValueError('native smoke requires the full frozen source commit')
    source, install = args.source.resolve(strict=True), args.install.resolve(strict=True)
    sys.path.insert(0, str(source / 'src/test/cluster_tap/pre2'))
    from local_quorum import LocalQuorum
    from cohort_entry import entry_profile
    from cohort_probe import cleanup
    from native_lifecycle import ENTRY, OPERATIONS
    root = args.output.resolve()
    root.mkdir(mode=0o700, exist_ok=False)
    os.chown(root, args.uid, args.gid)
    bindir = install / 'bin'
    drop = ['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups']
    results = dict(source=args.commit, binary_sha256=hashlib.sha256((bindir/'postgres').read_bytes()).hexdigest(),
                   cases=[], complete=False, error=None, cleanup_errors=[])
    q = LocalQuorum(root/'quorum', args.vendor, 4, args.uid, args.gid)
    q.env['LD_LIBRARY_PATH'] += ':' + str(install/'lib')
    formatter = root/'native_vote'
    case_roots = []
    try:
        command = drop + ['cc', '-D_GNU_SOURCE', '-ffunction-sections', '-fdata-sections',
            '-I', str(install/'include/server'), '-I', str(source/'src/include'),
            str(source/'src/test/cluster_tap/pre2/native_vote.c'),
            str(source/'src/backend/cluster/cluster_voting_disk_io.c'),
            str(install/'lib/libpgport.a'), '-Wl,--gc-sections', '-o', str(formatter)]
        with (root/'formatter-build.log').open('x') as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        q.start()
        for case in mvp_ci.smoke_selection(mvp_ci.load_policy(), native=True):
            directory = root/Path(case).stem
            for path in (directory, directory/'log', directory/'data'):
                path.mkdir(mode=0o700)
                os.chown(path, args.uid, args.gid)
            case_roots.append(directory)
            votes = []
            for index in range(3):
                image = directory/f'vote{index}.image'
                q.run(drop + [formatter, image, str(index)])
                votes.append(q.attach_vote(image))
            profile = entry_profile(root/'quorum/handles.json', formatter, votes)
            profile['operations'] = {op: dict(ENTRY) for op in OPERATIONS}
            entry = directory/'entry.json'
            entry.write_text(json.dumps(profile, indent=2)+'\n')
            command = drop + ['env', 'PATH='+str(bindir)+':'+os.environ['PATH'],
                'PGRAC_PRE2_BINDIR='+str(bindir), 'PGRAC_PRE2_ENTRY_FILE='+str(entry),
                'PG_TEST_NOCLEAN=1', 'PYTHONDONTWRITEBYTECODE=1',
                'TESTLOGDIR='+str(directory/'log'), 'TESTDATADIR='+str(directory/'data'),
                'PG_REGRESS='+str(install/'lib/pgxs/src/test/regress/pg_regress'),
                'prove', '-v', '-I', str(source/'src/test/perl'),
                str(source/'src/test/cluster_tap'/case)]
            (directory/'command.json').write_text(json.dumps(command, indent=2)+'\n')
            try:
                reply = q.run(command, check=False, cwd=directory, timeout=1200)
                output, rc = reply.stdout + reply.stderr, reply.returncode
            except subprocess.TimeoutExpired as error:
                def decoded(value):
                    return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                output = decoded(error.stdout) + decoded(error.stderr) + '\nCONTROLLER DEADLINE\n'
                rc = 124
            (directory/'prove.log').write_text(output)
            verdict = dict(case=case, **tap_result(output, rc))
            results['cases'].append(verdict)
            print(json.dumps(verdict), flush=True)
        results['complete'] = True
    except Exception as error:
        results['error'] = repr(error)
    finally:
        try:
            for directory in case_roots:
                for layout in (directory/'data').rglob('blackbox-layout.json'):
                    nodes = cleanup_targets(layout, directory, q.nodes, args.uid)
                    if any((Path(n['data_dir'])/'postmaster.pid').exists() for n in nodes):
                        results['cleanup_errors'].append('live DATA required immediate error cleanup')
                        cleanup(q, nodes, bindir)
        except Exception as error:
            results['cleanup_errors'].append(repr(error))
        try:
            q.stop()
        except Exception as error:
            results['cleanup_errors'].append(repr(error))
        (root/'result.json').write_text(json.dumps(results, indent=2)+'\n')
    success = (results['complete'] and results['error'] is None
               and not results['cleanup_errors'] and results['cases']
               and all(case['passed'] for case in results['cases']))
    print(json.dumps(results), flush=True)
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('source', 'install', 'output', 'vendor'):
        parser.add_argument('--'+key, type=Path, required=True)
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--gid', type=int, required=True)
    parser.add_argument('--commit', required=True)
    return run(parser.parse_args())


if __name__ == '__main__':
    sys.exit(main())
