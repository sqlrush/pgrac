#!/usr/bin/env python3
"""Run native MultiXact lifecycle TAPs with a disposable four-member quorum.

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
import shutil
import subprocess
import sys

MX_CASES = ['t/445_pre2_multixact_maintenance.pl',
            't/447_pre2_multixact_restart.pl',
            't/448_pre2_multixact_cleared_votes.pl']
M1_CASES = ['t/431_pre2_start_stop_restart.pl', *MX_CASES,
            't/453_pre2_vacuum_snapshot_cancel.pl',
            't/482_pre2_undo_active_slots.pl']
SOAK_CASE = 't/483_pre2_m1_soak_workload.pl'
RETENTION_CASE = 't/484_pre2_retained_snapshot.pl'
IDLE_FREEZE_CASE = 't/485_pre2_idle_freeze_restart.pl'
MX_CONTINUATION_CASE = 't/486_pre2_multixact_continuation.pl'


def select_cases(suite, cases):
    allowed = {'multixact': MX_CASES, 'm1': M1_CASES,
               'm1-soak-workload': [SOAK_CASE], 'm1-retention': [RETENTION_CASE],
               'm1-idle-freeze': [IDLE_FREEZE_CASE],
               'm1-mx-continuation': [MX_CONTINUATION_CASE]}[suite]
    selected = list(MX_CASES[1:] if suite == 'multixact' else allowed) if cases is None else list(cases)
    if (not selected or len(set(selected)) != len(selected)
            or any(case not in allowed for case in selected)):
        raise ValueError('only distinct supported native TAP cases are allowed')
    negative, positive = MX_CASES[2], MX_CASES[1]
    if negative in selected and (positive not in selected
            or selected.index(positive) > selected.index(negative)):
        raise ValueError('TAP448 requires this invocation to complete TAP447 first')
    return selected


def case_runs(suite, cases):
    selected = select_cases(suite, cases)
    if suite == 'm1-soak-workload':
        # Envelope includes formation, eight row cuts, log verification and
        # cleanup. The workload stays 3600s and original stop keeps its budget.
        return [dict(case=SOAK_CASE, clients=clients, timeout=5400,
                     directory=Path(SOAK_CASE).stem+'-'+str(clients))
                for clients in (16, 48, 64)]
    return [dict(case=case, clients=None, timeout=1200, directory=Path(case).stem)
            for case in selected]


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
    invocations = case_runs(getattr(args, 'suite', 'multixact'), args.cases)
    identity = json.loads(args.identity.read_text()) if args.identity else None
    if args.archive:
        with args.archive.open('rb') as archive:
            archive_sha = hashlib.file_digest(archive, 'sha256').hexdigest()
        if identity is None or archive_sha != identity['source_archive_sha256']:
            raise ValueError('source archive identity differs')
    source, install = args.source.resolve(strict=True), args.install.resolve(strict=True)
    sys.path.insert(0, str(source / 'src/test/cluster_tap/pre2'))
    from local_quorum import LocalQuorum
    from cohort_entry import entry_profile
    from cohort_probe import cleanup
    from native_lifecycle import ENTRY, OPERATIONS
    from mx_cold_copy import sha
    from mx_cold_restart import copy_cleared_votes, prepare
    root = args.output.resolve()
    root.mkdir(mode=0o700, exist_ok=False)
    os.chown(root, args.uid, args.gid)
    bindir = install / 'bin'
    actual_sha = hashlib.sha256((bindir/'postgres').read_bytes()).hexdigest()
    if args.expected_postgres_sha256 and actual_sha != args.expected_postgres_sha256:
        raise ValueError('installed postgres identity mismatch')
    fixture_install = args.fixture_install.resolve(strict=True) if args.fixture_install else install
    server_include = Path(subprocess.check_output([fixture_install/'bin/pg_config', '--includedir-server'], text=True).strip())
    pkglibdir = install/'lib'
    drop = ['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups']
    results = dict(source_identity=identity, binary_sha256=hashlib.sha256((bindir/'postgres').read_bytes()).hexdigest(),
                   cases=[], complete=False, error=None, cleanup_errors=[])
    if getattr(args, 'suite', 'multixact') == 'm1-soak-workload':
        results.update(scope='workload-ledger-and-normal-stop', p07='NOT_READY',
                       stage_acceptance='NOT_READY')
    if getattr(args, 'suite', 'multixact') == 'm1-retention':
        results.update(scope='retained-reader-values-and-normal-stop', p02='NOT_READY',
                       stage_acceptance='NOT_READY')
    if getattr(args, 'suite', 'multixact') == 'm1-idle-freeze':
        results.update(scope='tuple-freeze-and-clean-restart', p01='NOT_READY',
                       stage_acceptance='NOT_READY')
    if getattr(args, 'suite', 'multixact') == 'm1-mx-continuation':
        results.update(scope='multixact-continuation-and-live-members', x04='NOT_READY',
                       stage_acceptance='NOT_READY')
    q = LocalQuorum(root/'quorum', args.vendor, 4, args.uid, args.gid)
    q.env['LD_LIBRARY_PATH'] += ':' + str(install/'lib')
    formatter = root/'native_vote'
    case_roots = []
    try:
        command = drop + ['cc', '-D_GNU_SOURCE', '-ffunction-sections', '-fdata-sections',
            '-I', str(server_include), '-I', str(source/'src/include'),
            str(source/'src/test/cluster_tap/pre2/native_vote.c'),
            str(source/'src/backend/cluster/cluster_voting_disk_io.c'),
            str(fixture_install/'lib/libpgport.a'), '-Wl,--gc-sections', '-o', str(formatter)]
        with (root/'formatter-build.log').open('x') as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        command = drop + ['cc', '-shared', '-fPIC', '-I', str(server_include),
            str(source/'src/test/cluster_tap/pre2/mx_range.c'), '-o', str(root/'mx_range.so')]
        with (root/'probe-build.log').open('x') as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        q.start()
        for invocation in invocations:
            case = invocation['case']
            directory = root/invocation['directory']
            for path in (directory, directory/'log', directory/'data'):
                path.mkdir(mode=0o700)
                os.chown(path, args.uid, args.gid)
            case_roots.append(directory)
            votes = []
            cold_view = None
            cold_layout = None
            negative = Path(case).name == '448_pre2_multixact_cleared_votes.pl'
            original_votes = {}
            if negative:
                capture_path = root/'447_pre2_multixact_restart/first-stop-copy/capture.json'
                capture = json.loads(capture_path.read_text())
                original_votes = {path: sha(path) for path in capture['layout']['voting_disks']}
                faults = copy_cleared_votes(capture_path, directory/'cleared-votes')
                os.chown(directory/'cleared-votes', args.uid, args.gid)
                for fault in faults:
                    image = Path(fault['copy'])
                    os.chown(image, args.uid, args.gid)
                    votes.append(q.attach_vote(image))
            else:
                for index in range(3):
                    image = directory/f'vote{index}.image'
                    q.run(drop + [formatter, image, str(index)])
                    votes.append(q.attach_vote(image))
            if negative:
                cold_layout = prepare(capture_path, directory/'data/cold', votes, directory/'log', faults)
                for current, dirs, files in os.walk(directory/'data/cold', followlinks=False):
                    os.chown(current, args.uid, args.gid)
                    for name in files + dirs:
                        os.chown(Path(current)/name, args.uid, args.gid, follow_symlinks=False)
                cold_view = json.loads(cold_layout.with_name('mount-view.json').read_text())
                host_before = {item['original']: (Path(item['original']).stat().st_dev, Path(item['original']).stat().st_ino) for item in cold_view['bindings']}
                for n in range(4):
                    for item in cold_view['bindings']:
                        q.run(q.ns(n, ['mount', '--bind', item['copy'], item['original']]))
                host_after = {path: (Path(path).stat().st_dev, Path(path).stat().st_ino) for path in host_before}
                if host_before != host_after:
                    raise RuntimeError('private bind unexpectedly changed the host view')
                (directory/'namespace-bindings.json').write_text(json.dumps(dict(view=cold_view, host_before=host_before, host_after=host_after), indent=2)+'\n')
                votes = [item['original'] for item in cold_view['bindings'][1:]]
                for current, dirs, files in os.walk(directory/'log'):
                    os.chown(current, args.uid, args.gid)
                    for name in dirs + files:
                        os.chown(Path(current)/name, args.uid, args.gid)
            profile = entry_profile(root/'quorum/handles.json', formatter, votes)
            profile['operations'] = {op: dict(ENTRY) for op in OPERATIONS}
            entry = directory/'entry.json'
            entry.write_text(json.dumps(profile, indent=2)+'\n')
            command = drop + ['env', 'PATH='+str(bindir)+':'+os.environ['PATH'],
                'PGRAC_PRE2_BINDIR='+str(bindir), 'PGRAC_PRE2_ENTRY_FILE='+str(entry),
                'PGRAC_PRE2_MX_PROBE='+str(root/'mx_range.so'), 'PG_TEST_NOCLEAN=1', 'PYTHONDONTWRITEBYTECODE=1',
                'TESTLOGDIR='+str(directory/'log'), 'TESTDATADIR='+str(directory/'data'),
                'PG_REGRESS='+str(pkglibdir/'pgxs/src/test/regress/pg_regress'),
                'prove', '-v', '-I', str(source/'src/test/perl'),
                str(source/'src/test/cluster_tap'/case)]
            if cold_layout:
                command.insert(command.index('prove'), 'PGRAC_PRE2_COLD_LAYOUT='+str(cold_layout))
                command = q.ns(0, command)
            if invocation['clients'] is not None:
                command.insert(command.index('prove'),
                               'PGRAC_PRE2_SOAK_CLIENTS='+str(invocation['clients']))
            (directory/'command.json').write_text(json.dumps(command, indent=2)+'\n')
            try:
                reply = q.run(command, check=False, cwd=directory, timeout=invocation['timeout'])
                output, rc = reply.stdout + reply.stderr, reply.returncode
            except subprocess.TimeoutExpired as error:
                def decoded(value):
                    return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                output = decoded(error.stdout) + decoded(error.stderr) + '\nCONTROLLER DEADLINE\n'
                rc = 124
            (directory/'prove.log').write_text(output)
            verdict = dict(case=case, **tap_result(output, rc))
            if invocation['clients'] is not None:
                verdict['clients'] = invocation['clients']
            if negative:
                after_votes = {path: sha(path) for path in original_votes}
                unchanged = original_votes == after_votes
                (directory/'original-votes.json').write_text(json.dumps(dict(
                    before=original_votes, after=after_votes, unchanged=unchanged), indent=2)+'\n')
                verdict['original_votes_unchanged'] = unchanged
                verdict['passed'] = verdict['passed'] and unchanged
            results['cases'].append(verdict)
            print(json.dumps(verdict), flush=True)
            if not verdict['passed']:
                break
        results['complete'] = True
    except Exception as error:
        results['error'] = repr(error)
    finally:
        try:
            for directory in case_roots:
                cold_layout = directory/'data/cold/layout.json'
                if cold_layout.exists():
                    # These DATA names exist only in the registered private
                    # mount view; inspecting them from the host sees originals.
                    code = ('import json,sys;sys.path.insert(0,sys.argv[1]);'
                            'from mx_cold_restart import stop_copy;'
                            'r=stop_copy(*sys.argv[2:]);print(json.dumps(r));'
                            'assert r["all_stopped"]')
                    reply = q.run(q.ns(0, drop + ['python3', '-c', code,
                        str(source/'src/test/cluster_tap/pre2'), str(cold_layout),
                        str(directory/'entry.json'), str(bindir)]), check=False,
                        timeout=40)
                    (directory/'controller-cleanup.json').write_text(json.dumps(dict(
                        rc=reply.returncode, stdout=reply.stdout, stderr=reply.stderr), indent=2)+'\n')
                    if reply.returncode != 0:
                        raise RuntimeError('isolated cold-copy cleanup failed')
                    continue
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
    parser.add_argument('--suite', choices=['multixact', 'm1', 'm1-soak-workload', 'm1-retention',
                                           'm1-idle-freeze', 'm1-mx-continuation'], default='multixact')
    parser.add_argument('--cases', nargs='+')
    parser.add_argument('--fixture-install', type=Path)
    parser.add_argument('--expected-postgres-sha256')
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--gid', type=int, required=True)
    parser.add_argument('--identity', type=Path)
    parser.add_argument('--archive', type=Path)
    return run(parser.parse_args())


if __name__ == '__main__':
    sys.exit(main())
