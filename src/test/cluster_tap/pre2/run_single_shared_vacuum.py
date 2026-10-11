#!/usr/bin/env python3
"""Run the single-member shared VACUUM case with real TLS storage quorum.

Requires an isolated Linux development VM and root. Native initdb and the
original formatter own all persistent authority. Keeps DATA, votes and logs;
only this invocation's transient namespace and loop handles are released.
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
import sys
import time

from local_quorum import LocalQuorum


def test_group_alive(group):
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            stat = path.read_text()
        except FileNotFoundError:
            continue
        fields = stat[stat.rfind(')') + 2:].split()
        if int(fields[2]) == group and int(fields[3]) == group and fields[0] != 'Z':
            return True
    return False


def finish_test_group(process):
    """Reap all test controllers before touching their database handles."""
    if test_group_alive(process.pid):
        os.killpg(process.pid, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while test_group_alive(process.pid) and time.monotonic() < deadline:
            time.sleep(.05)
        if test_group_alive(process.pid):
            os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=5)
    deadline = time.monotonic() + 5
    while test_group_alive(process.pid) and time.monotonic() < deadline:
        time.sleep(.05)
    if test_group_alive(process.pid):
        raise RuntimeError('test controller still live; database handles retained')


def run(args):
    if sys.platform != 'linux' or os.geteuid() != 0 or args.uid <= 0:
        raise ValueError('requires isolated Linux root and nonroot database UID')
    account = pwd.getpwuid(args.uid)
    root = args.output.resolve()
    root.mkdir(mode=0o700, exist_ok=False)
    os.chown(root, args.uid, account.pw_gid)
    for directory in ('log', 'data'):
        (root/directory).mkdir(mode=0o700)
        os.chown(root/directory, args.uid, account.pw_gid)
    source, install = args.source.resolve(strict=True), args.install.resolve(strict=True)
    fixture_install = args.fixture_install.resolve(strict=True)
    server_include = subprocess.check_output(
        [fixture_install/'bin/pg_config', '--includedir-server'], text=True).strip()
    drop = ['setpriv', '--reuid', str(args.uid), '--regid', str(account.pw_gid), '--init-groups']
    formatter = root/'native_vote'
    command = drop + ['cc', '-D_GNU_SOURCE', '-ffunction-sections', '-fdata-sections',
        '-I', server_include, '-I', str(source/'src/include'),
        str(source/'src/test/cluster_tap/pre2/native_vote.c'),
        str(source/'src/backend/cluster/cluster_voting_disk_io.c'),
        str(fixture_install/'lib/libpgport.a'), '-Wl,--gc-sections', '-o', str(formatter)]
    with (root/'formatter-build.log').open('x') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    quorum = LocalQuorum(root/'quorum', args.vendor, 1, args.uid, account.pw_gid)
    quorum.env['LD_LIBRARY_PATH'] += ':' + str(install/'lib')
    result = dict(postgres_sha256=hashlib.sha256((install/'bin/postgres').read_bytes()).hexdigest(),
                  rc=None, error=None, cleanup_errors=[])
    process = None
    try:
        quorum.start()
        votes = []
        for index in range(3):
            image = root/f'vote{index}.image'
            quorum.run(drop + [formatter, image, str(index)])
            loop = quorum.attach_vote(image)
            quorum.run(drop + [formatter, '--attest', loop, str(index)])
            votes.append(loop)
        fixture = root/'fixture.json'
        fixture.write_text(json.dumps(dict(name=quorum.name, count=1, votes=votes))+'\n')
        command = quorum.ns(0, drop + ['env', 'PATH='+str(install/'bin')+':'+os.environ['PATH'],
            'PGRAC_SINGLE_SHARED_FIXTURE='+str(fixture), 'PG_TEST_NOCLEAN=1',
            'TESTLOGDIR='+str(root/'log'), 'TESTDATADIR='+str(root/'data'),
            'PG_REGRESS='+str(install/'lib/pgxs/src/test/regress/pg_regress'),
            'prove', '-v', '-I', str(source/'src/test/perl'),
            str(source/'src/test/cluster_tap/t/461_pre2_single_shared_vacuum.pl')])
        (root/'command.json').write_text(json.dumps(command, indent=2)+'\n')
        with (root/'prove.log').open('x') as log:
            process = subprocess.Popen(command, env=quorum.env, cwd=root,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            result['rc'] = process.wait(timeout=300)
    except Exception as error:
        result['error'] = repr(error)
    finally:
        if process is not None:
            try:
                if process.poll() is not None and test_group_alive(process.pid):
                    result['error'] = result['error'] or 'test descendants survived leader exit'
                finish_test_group(process)
            except Exception as error:
                result['cleanup_errors'].append(repr(error))
        # Only DATA created by this case may be stopped. Never signal a PID
        # taken from an unverified or replaced postmaster.pid file.
        pidfiles = [] if result['cleanup_errors'] else (root/'data').glob('t_461_*/pgdata/postmaster.pid')
        for pidfile in pidfiles:
            data = pidfile.parent.resolve(strict=True)
            lines = pidfile.read_text().splitlines()
            try:
                if not data.is_relative_to((root/'data').resolve()) or len(lines) < 2:
                    raise ValueError('DATA ownership differs')
                process = Path('/proc')/lines[0]
                if not process.exists():
                    continue
                if (Path(lines[1]).resolve() != data or int(lines[0]) <= 1
                        or process.stat().st_uid != args.uid
                        or (process/'ns/net').stat().st_ino != quorum.nodes[0]['net_inode']
                        or str(data).encode() not in (process/'cmdline').read_bytes().split(b'\0')
                        or (process/'exe').resolve() != (install/'bin/postgres').resolve()):
                    raise ValueError('postmaster identity differs')
                reply = quorum.run(quorum.ns(0, drop + [install/'bin/pg_ctl', '-D', data,
                    '-m', 'fast', '-t', '30', 'stop']), check=False, timeout=35)
                if reply.returncode:
                    raise RuntimeError('normal error cleanup failed; live handles retained')
            except Exception as error:
                result['cleanup_errors'].append(repr(error))
        if not result['cleanup_errors']:
            try:
                quorum.stop()
            except Exception as error:
                result['cleanup_errors'].append(repr(error))
        (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))
    return 0 if result['rc'] == 0 and result['error'] is None and not result['cleanup_errors'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'install', 'fixture-install', 'vendor', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--uid', type=int, required=True)
    sys.exit(run(parser.parse_args()))
