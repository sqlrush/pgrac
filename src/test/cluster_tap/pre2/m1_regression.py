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

from run_legacy import M1_CASES as LEGACY_NAMES
from run_multixact_lifecycle import M1_CASES as FOUR_MEMBER, tap_result

LEGACY = ['t/'+name for name in LEGACY_NAMES.values()]
SINGLE_MEMBER = ['t/461_pre2_single_shared_vacuum.pl']


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
    return dict(status='NOT_RUN', stage_acceptance='NOT_READY', phases=[
        dict(name='legacy', cases=LEGACY, command=legacy),
        dict(name='four-member', cases=FOUR_MEMBER, command=four),
        dict(name='single-member', cases=SINGLE_MEMBER, command=single)],
        not_covered=['shared positive physical reclamation', 'retention/recovery matrix',
                     '16/48/64 one-hour soak', 'full build and stage acceptance'])


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
    tap = args.source/'src/test/cluster_tap'
    files = [tap/case for phase in selection['phases'] for case in phase['cases']]
    files += [tap/'pre2'/name for name in ('m1_regression.py', 'run_legacy.py',
              'run_multixact_lifecycle.py', 'run_single_shared_vacuum.py')]
    return {str(path.relative_to(args.source)): sha(path) for path in files}


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
    root = args.output.resolve()
    root.mkdir(mode=0o700)
    os.chown(root, args.uid, args.gid)
    (root/'plan.json').write_text(json.dumps(selection, indent=2)+'\n')
    result = dict(status='FAIL', stage_acceptance='NOT_READY', phases=[],
                  postgres_sha256=args.expected_postgres_sha256, source_sha256=source_hashes,
                  build_binding='UNKNOWN', error=None, not_covered=selection['not_covered'])
    try:
        for phase in selection['phases']:
            if sha(args.install/'bin/postgres') != args.expected_postgres_sha256:
                raise ValueError('installed postgres changed before regression phase')
            # Existing controllers own their processes, deadlines and cleanup.
            with (root/(phase['name']+'.log')).open('x') as log:
                reply = subprocess.run(phase['command'], stdout=log, stderr=subprocess.STDOUT,
                                       env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
            record = dict(name=phase['name'], rc=reply.returncode, passed=False)
            result['phases'].append(record)
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
    parser.add_argument('--plan', action='store_true')
    try:
        return run(parser.parse_args())
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, str(error)+'\n')


if __name__ == '__main__':
    sys.exit(main())
