#!/usr/bin/env python3
"""Compose existing undo/maintenance TAP controllers without changing their judges.

Runtime requires an authorized isolated Linux root environment, qualified native
components and the original frozen-binary workload prerequisites. --plan is
read-only and does not require Linux, installed files or database processes.
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

from m1_build_matrix import receipt_binding
from run_legacy import M1_CASES as LEGACY_NAMES
from run_multixact_lifecycle import M1_CASES as FOUR_MEMBER, case_runs, tap_result

LEGACY = ['t/'+name for name in LEGACY_NAMES.values()]
SINGLE_MEMBER = ['t/461_pre2_single_shared_vacuum.pl']
EXTENDED_NATIVE = {
    'mx-continuation': ('m1-mx-continuation', 'multixact-continuation-and-live-members', 'x04'),
    'idle-freeze': ('m1-idle-freeze', 'tuple-freeze-and-clean-restart', 'p01'),
    'retention': ('m1-retention', 'retained-reader-values-and-normal-stop', 'p02'),
    'soak': ('m1-soak-workload', 'workload-ledger-and-normal-stop', 'p07'),
}
SOURCE_INPUT_TREES = ('src/test/cluster_tap', 'src/test/perl')


def validate_environment(env):
    for key in ('PGRAC_PRE2_TEST_ADAPTER', 'PGRAC_PRE2_ENTRY_FILE',
                'PGRAC_SINGLE_SHARED_FIXTURE', 'PGRAC_PRE2_MX_PROBE',
                'PGRAC_PRE2_COLD_LAYOUT', 'PYTHONOPTIMIZE'):
        if env.get(key):
            raise ValueError('external test override is not allowed: '+key)
    if sys.flags.optimize:
        raise ValueError('Python assertions must remain enabled')


def strict_tap(output, rc):
    # prove owns TAP parsing; keep the native controller's no-skip requirement.
    return (tap_result(output, rc)['passed'] and 'Result: PASS' in output
            and not re.search(r'^\s*Bail out!', output, re.I | re.M))


def plan(args):
    suite = getattr(args, 'suite', 'core')
    if suite not in ('core', 'extended'):
        raise ValueError('unsupported M1 regression suite')
    scripts = args.source.resolve()/'src/test/cluster_tap/pre2'
    output = args.output.resolve()
    common = ['--source', str(args.source.resolve()), '--install', str(args.install.resolve()),
              '--output', '', '--vendor', str(args.vendor.resolve()), '--uid', str(args.uid),
              '--fixture-install', str(args.fixture_install.resolve())]
    four = [sys.executable, str(scripts/'run_multixact_lifecycle.py'), *common,
            '--gid', str(args.gid), '--suite', 'm1',
            '--expected-postgres-sha256', args.expected_postgres_sha256]
    single = [sys.executable, str(scripts/'run_single_shared_vacuum.py'), *common]
    four[four.index('--output')+1] = str(output/'four-member')
    single[single.index('--output')+1] = str(output/'single-member')
    legacy = ['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid),
              '--init-groups', sys.executable, str(scripts/'run_legacy.py'),
              '--source', str(args.source.resolve()), '--build', str(args.build.resolve()),
              '--install', str(args.install.resolve()), '--output', str(output/'legacy'),
              '--suite', 'm1', '--expected-postgres-sha256', args.expected_postgres_sha256]
    selection = dict(status='NOT_RUN', stage_acceptance='NOT_READY', phases=[
        dict(name='legacy', cases=LEGACY, command=legacy),
        dict(name='four-member', cases=FOUR_MEMBER, command=four),
        dict(name='single-member', cases=SINGLE_MEMBER, command=single)],
        not_covered=['shared positive physical reclamation', 'retention/recovery matrix',
                     '16/48/64 one-hour soak', 'full build and stage acceptance'])
    if suite == 'extended':
        for name, (native_suite, scope, _) in EXTENDED_NATIVE.items():
            command = list(four)
            command[command.index('--output')+1] = str(output/name)
            command[command.index('--suite')+1] = native_suite
            selection['phases'].append(dict(name=name, scope=scope,
                cases=[run['case'] for run in case_runs(native_suite, None)], command=command))
        selection.update(suite='extended',
            subcheck_gates=dict(x04='NOT_READY', p01='NOT_READY', p02='NOT_READY', p07='NOT_READY'),
            not_covered=['shared positive physical reclamation',
                         'complete X04 gap/occupied-page/concurrent issuance evidence',
                         'full-width F/H and catalog cutoff qualification',
                         'recycle progression after final reader release',
                         'complete P07 resource/generation acceptance',
                         'general recovery matrix', 'full build and stage acceptance'])
    if getattr(args, 'build_matrix', None) is not None:
        selection['build_matrix'] = str(args.build_matrix.resolve())
    return selection


def phase_passed(name, root, rc, expected_sha=None):
    if rc != 0:
        return False
    if name == 'legacy':
        cases = json.loads((root/'results.json').read_text())
        return (isinstance(cases, list) and [c['case'] for c in cases] == list(LEGACY_NAMES)
                and all(c['status'] == 'PASS' and c['binary_sha256'] == expected_sha
                        and strict_tap((root/c['case']/'prove.log').read_text(), c['rc'])
                        for c in cases))
    result = json.loads((root/'result.json').read_text())
    if result.get('error') is not None or result.get('cleanup_errors') != []:
        return False
    if name == 'four-member':
        cases = result.get('cases', [])
        return (result.get('complete') is True and result.get('binary_sha256') == expected_sha
                and [c['case'] for c in cases] == FOUR_MEMBER
                and all(c['passed'] and strict_tap(
                    (root/Path(c['case']).stem/'prove.log').read_text(), c['rc']) for c in cases))
    if name in EXTENDED_NATIVE:
        native_suite, scope, gate = EXTENDED_NATIVE[name]
        runs = case_runs(native_suite, None)
        cases = result.get('cases', [])
        if (not isinstance(expected_sha, str) or not re.fullmatch('[0-9a-f]{64}', expected_sha)
                or result.get('complete') is not True
                or result.get('binary_sha256') != expected_sha or result.get('scope') != scope
                or result.get(gate) != 'NOT_READY' or result.get('stage_acceptance') != 'NOT_READY'
                or len(cases) != len(runs)):
            return False
        for case, run in zip(cases, runs):
            if (case.get('case') != run['case'] or case.get('clients') != run['clients']
                    or case.get('passed') is not True or case.get('rc') != 0
                    or not strict_tap((root/run['directory']/'prove.log').read_text(), case['rc'])):
                return False
        return True
    if name == 'single-member':
        return (result.get('postgres_sha256') == expected_sha
                and strict_tap((root/'prove.log').read_text(), result.get('rc')))
    raise ValueError('unknown regression phase')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def capture_source_inputs(source):
    source = source.resolve(strict=True)
    result = {}
    for name in SOURCE_INPUT_TREES:
        directory = source
        for part in Path(name).parts:
            directory /= part
            if directory.is_symlink():
                raise ValueError('regression source input is a symlink: '+str(directory))
        if not directory.is_dir():
            raise ValueError('regression source input tree is missing: '+name)
        pending, count = [directory], 0
        while pending:
            for path in sorted(pending.pop().iterdir()):
                if path.is_symlink():
                    raise ValueError('regression source input is a symlink: '+str(path))
                if path.is_dir():
                    pending.append(path)
                elif path.is_file():
                    result[str(path.relative_to(source))] = sha(path)
                    count += 1
                else:
                    raise ValueError('regression source input is not regular: '+str(path))
        if not count:
            raise ValueError('regression source input tree is empty: '+name)
    return dict(sorted(result.items()))


def verify_source_inputs(source, expected):
    actual = capture_source_inputs(source)
    for name, digest in expected.items():
        if actual.get(name) != digest:
            raise ValueError('regression source input changed: '+name)
    if actual.keys() != expected.keys():
        raise ValueError('regression source input inventory changed')


def verify_matrix_binding(args, expected):
    if receipt_binding(args) != expected:
        raise ValueError('build matrix receipt binding changed')


def preflight(args, selection):
    validate_environment(os.environ)
    if not re.fullmatch('[0-9a-f]{64}', args.expected_postgres_sha256):
        raise ValueError('expected postgres SHA256 must be a full lowercase digest')
    if args.output.exists() or args.output.is_symlink():
        raise ValueError('output must be a new directory')
    if sha(args.install/'bin/postgres') != args.expected_postgres_sha256:
        raise ValueError('installed postgres identity mismatch')
    for path in (args.build/'src/test/regress/pg_regress', args.fixture_install/'bin/pg_config',
                 args.install/'lib/pgxs/src/test/regress/pg_regress'):
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError('required installed/build test tool is missing: '+str(path))
    if not args.vendor.is_dir():
        raise ValueError('native quorum vendor directory is missing')
    if args.output.resolve().is_relative_to(args.source.resolve()):
        raise ValueError('regression output must be outside the source tree')
    tap = args.source/'src/test/cluster_tap'
    files = [tap/case for phase in selection['phases'] for case in phase['cases']]
    files += [tap/'pre2'/name for name in ('m1_regression.py', 'run_legacy.py',
              'run_multixact_lifecycle.py', 'run_single_shared_vacuum.py')]
    if any(phase['name'] == 'soak' for phase in selection['phases']):
        files += [tap/'pre2'/name for name in ('m1_soak_member_logs.py', 'workload_log.py')]
    snapshot = capture_source_inputs(args.source)
    for path in files:
        if str(path.relative_to(args.source)) not in snapshot:
            raise ValueError('required regression source input is missing: '+str(path))
    return snapshot


def run(args):
    validate_environment(os.environ)
    selection = plan(args)
    if args.plan:
        print(json.dumps(selection, indent=2))
        return 0
    if sys.platform != 'linux' or os.geteuid() != 0 or args.uid <= 0:
        raise ValueError('runtime requires isolated Linux root and a nonroot database UID')
    if pwd.getpwuid(args.uid).pw_gid != args.gid:
        raise ValueError('database UID/GID identity differs')
    source_hashes = preflight(args, selection)
    matrix_binding = receipt_binding(args)
    root = args.output.resolve()
    root.mkdir(mode=0o700)
    os.chown(root, args.uid, args.gid)
    (root/'plan.json').write_text(json.dumps(selection, indent=2)+'\n')
    result = dict(status='FAIL', stage_acceptance='NOT_READY', phases=[],
                  postgres_sha256=args.expected_postgres_sha256, source_sha256=source_hashes,
                  source_input_trees=list(SOURCE_INPUT_TREES),
                  build_binding='UNKNOWN', error=None, not_covered=selection['not_covered'])
    if 'subcheck_gates' in selection:
        result.update(suite=selection['suite'], subcheck_gates=selection['subcheck_gates'])
    if matrix_binding is not None:
        result.update(build_matrix=matrix_binding,
                      build_binding='SOURCE_AND_BINARY_RECEIPT_MATCH', source_commit='NOT_ATTESTED')
    try:
        for phase in selection['phases']:
            verify_source_inputs(args.source, source_hashes)
            verify_matrix_binding(args, matrix_binding)
            if sha(args.install/'bin/postgres') != args.expected_postgres_sha256:
                raise ValueError('installed postgres changed before regression phase')
            # Existing controllers own their processes, deadlines and cleanup.
            with (root/(phase['name']+'.log')).open('x') as log:
                reply = subprocess.run(phase['command'], stdout=log, stderr=subprocess.STDOUT,
                                       env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
            record = dict(name=phase['name'], rc=reply.returncode, passed=False)
            result['phases'].append(record)
            verify_source_inputs(args.source, source_hashes)
            verify_matrix_binding(args, matrix_binding)
            passed = phase_passed(phase['name'], root/phase['name'], reply.returncode,
                                  args.expected_postgres_sha256)
            if sha(args.install/'bin/postgres') != args.expected_postgres_sha256:
                raise ValueError('installed postgres changed during regression')
            record['passed'] = passed
            if not passed:
                break
        if len(result['phases']) == len(selection['phases']) and all(
                phase['passed'] for phase in result['phases']):
            result['status'] = 'PASS'
    except Exception as error:
        result['error'] = repr(error)
    finally:
        (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    return 0 if result['status'] == 'PASS' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('source', 'build', 'install', 'fixture-install', 'vendor', 'output'):
        parser.add_argument('--'+key, type=Path, required=True)
    for key in ('uid', 'gid'):
        parser.add_argument('--'+key, type=int, required=True)
    parser.add_argument('--expected-postgres-sha256', required=True)
    parser.add_argument('--build-matrix', type=Path)
    parser.add_argument('--suite', choices=['core', 'extended'], default='core')
    parser.add_argument('--plan', action='store_true')
    try:
        return run(parser.parse_args())
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, str(error)+'\n')


if __name__ == '__main__':
    sys.exit(main())
