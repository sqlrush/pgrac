#!/usr/bin/env python3
"""Run original build/unit/SQL targets in four preconfigured VPATH profiles.

--plan is read-only. Runtime starts the original regression databases and needs
an authorized nonroot test environment. This subcheck cannot certify full R01.
Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

PROFILES = [('enabled-assert', True, True), ('enabled-release', True, False),
            ('disabled-assert', False, True), ('disabled-release', False, False)]


def validate_environment(env):
    for key in ('MAKEFLAGS', 'MAKEOVERRIDES', 'MAKEFILES', 'GNUMAKEFLAGS', 'MFLAGS', 'TESTS', 'REGRESS',
                'REGRESS_OPTS', 'EXTRA_REGRESS_OPTS', 'EXTRA_TESTS', 'PROVE_TESTS', 'PG_TEST_EXTRA',
                'PGOPTIONS', 'PYTHONOPTIMIZE'):
        if env.get(key):
            raise ValueError('external make/test override is not allowed: '+key)
    if sys.flags.optimize:
        raise ValueError('Python assertions must remain enabled')


def schedule(path):
    names = []
    for raw in path.read_text().splitlines():
        line = raw.split('#', 1)[0].strip()
        if not line:
            continue
        name = r'[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*'
        if not re.fullmatch(r'test:\s+'+name+r'(?:\s+'+name+r')*', line):
            raise ValueError('unsupported or incomplete original regression schedule')
        names.extend(line.split()[1:])
    if not names or len(names) != len(set(names)):
        raise ValueError('empty or duplicate original regression schedule')
    return names


def plan(args):
    source = args.source.resolve()
    pg = schedule(source/'src/test/regress/parallel_schedule')
    cluster = schedule(source/'src/test/cluster_regress/parallel_schedule')
    if len(pg) != 219 or len(cluster) != 13:
        raise ValueError('original schedule coverage changed; requalify this matrix')
    profiles = []
    for name, enabled, assertion in PROFILES:
        build = getattr(args, name.replace('-', '_')).resolve()
        stages = [dict(kind='build', command=['make', '-C', str(build), 'all'])]
        if enabled:
            stages.extend(dict(kind=kind, command=['make', '-C', str(build/directory), 'check'])
                          for kind, directory in [('cluster-unit', 'src/test/cluster_unit'),
                                                  ('cluster-sql', 'src/test/cluster_regress')])
        stages.append(dict(kind='pg-sql', command=['make', '-C', str(build), 'check']))
        profiles.append(dict(profile=name, cluster=enabled, cassert=assertion,
            build=str(build), stages=stages,
            not_applicable=[] if enabled else ['cluster-unit', 'cluster-sql']))
    return dict(status='NOT_RUN', r01='NOT_READY', stage_acceptance='NOT_READY',
        source=str(source), profiles=profiles, pg_tests=pg, cluster_tests=cluster,
        not_covered=['native M1 lifecycle and retention/recovery matrix',
                     'complete one-hour soak acceptance', 'full stage acceptance'])


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def profile_identity(build, source, enabled, assertion):
    global_file, header = build/'src/Makefile.global', build/'src/include/pg_config.h'
    text, config = global_file.read_text(), header.read_text()

    def setting(key):
        values = re.findall(r'^'+key+r'[ \t]*=[ \t]*(.+?)[ \t]*$', text, re.M)
        if len(values) != 1:
            raise ValueError('ambiguous or missing generated build setting: '+key)
        return values[0]

    def macro(key):
        values = re.findall(r'^#define '+key+r'\s+([^\n]+)$', config, re.M)
        undefined = re.findall(r'^/\* #undef '+key+r' \*/$', config, re.M)
        if len(values) + len(undefined) != 1 or (values and values != ['1']):
            raise ValueError('ambiguous or missing generated build macro: '+key)
        return bool(values)

    cluster, cassert = macro('USE_PGRAC_CLUSTER'), macro('USE_ASSERT_CHECKING')
    if (setting('vpath_build') != 'yes'
            or Path(setting('abs_top_srcdir')).resolve(strict=True) != source.resolve(strict=True)
            or Path(setting('abs_top_builddir')).resolve(strict=True) != build.resolve(strict=True)
            or setting('enable_pgrac_cluster') != ('yes' if enabled else 'no')
            or cluster != enabled or cassert != assertion):
        raise ValueError('actual build source/profile differs from matrix selection')
    return dict(cluster=cluster, cassert=cassert, makefile_global_sha256=sha(global_file),
                pg_config_h_sha256=sha(header))


def source_fingerprint(source):
    result = {}
    for root, directories, files in os.walk(source, followlinks=False):
        directories[:] = sorted(name for name in directories if name != '.git')
        for name in [*directories, *sorted(files)]:
            if name == '.git':
                continue
            path = Path(root)/name
            if path.is_symlink():
                raise ValueError('source snapshot contains a symlink: '+str(path))
            if path.is_dir():
                continue
            if not path.is_file():
                raise ValueError('source snapshot contains a nonregular input')
            result[str(path.relative_to(source))] = sha(path)
    if not result:
        raise ValueError('source snapshot is empty')
    return result


def sql_passed(output, rc, expected):
    if rc != 0 or re.search(r'^not ok\b|^Bail out!|#\s*(?:SKIP|TODO)\b', output, re.I | re.M):
        return False
    rows = re.findall(r'^ok\s+(\d+)\s+[+-]\s+(\S+)\s+[0-9.]+\s+ms\s*$', output, re.M)
    return (re.findall(r'^1\.\.(\d+)\s*$', output, re.M) == [str(len(expected))]
            and [int(row[0]) for row in rows] == list(range(1, len(expected)+1))
            and len(rows) == len(expected) and {row[1] for row in rows} == set(expected)
            and re.findall(r'^# All (\d+) tests passed\.$', output, re.M) == [str(len(expected))])


def units_passed(output, rc):
    rows = re.findall(r'^# Running (\S+)\s*$', output, re.M)
    return (rc == 0 and rows and len(rows) == len(set(rows))
            and 'test_cluster_terminal_ref_census' in rows
            and re.findall(r'^# All (\d+) binaries passed\s*$', output, re.M) == [str(len(rows))]
            and not re.search(r'^# FAILED-BINARY:|^not ok\b|^Bail out!|#\s*(?:SKIP|TODO)\b',
                              output, re.I | re.M))


def receipt_binding(args):
    requested = getattr(args, 'build_matrix', None)
    if requested is None:
        return None
    if requested.is_symlink():
        raise ValueError('build matrix receipt root is a symlink')
    root = requested.resolve(strict=True)
    files = {}

    def read(name):
        path = root
        for part in Path(name).parts:
            path /= part
            if path.is_symlink():
                raise ValueError('build matrix receipt input is a symlink: '+name)
        if not path.is_file():
            raise ValueError('build matrix receipt input is not regular: '+name)
        raw = path.read_bytes()
        files[name] = hashlib.sha256(raw).hexdigest()
        return raw

    try:
        saved = json.loads(read('plan.json'))
        result = json.loads(read('result.json'))
        snapshot = json.loads(read('source-sha256.json'))
        if (not isinstance(saved, dict) or not isinstance(result, dict)
                or result.get('matrix_passed') is not True or result.get('error') is not None
                or result.get('r01') != 'NOT_READY' or result.get('stage_acceptance') != 'NOT_READY'
                or result.get('source_commit') != 'NOT_ATTESTED'
                or result.get('build_binding') != 'GENERATED_CONFIG_AND_COMMAND_ONLY'
                or not isinstance(saved.get('profiles'), list)
                or len(saved['profiles']) != len(PROFILES)):
            raise ValueError('build matrix is incomplete or has an unsupported qualification')
        selection_args = argparse.Namespace(source=args.source, output=root)
        for profile, (name, _, _) in zip(saved['profiles'], PROFILES):
            if profile['profile'] != name:
                raise ValueError('build matrix profile order differs')
            setattr(selection_args, name.replace('-', '_'), Path(profile['build']))
        selection = plan(selection_args)
        if saved != selection or snapshot != source_fingerprint(args.source):
            raise ValueError('build matrix plan or complete source snapshot differs')
        records = result.get('profiles')
        if not isinstance(records, list) or len(records) != len(PROFILES):
            raise ValueError('build matrix profile results are incomplete')
        selected = []
        for profile, record in zip(selection['profiles'], records):
            name, build = profile['profile'], Path(profile['build'])
            if (record['profile'] != name or record.get('passed') is not True
                    or record['identity'] != profile_identity(build, args.source,
                        profile['cluster'], profile['cassert'])
                    or not isinstance(record.get('stages'), list)
                    or len(record['stages']) != len(profile['stages'])):
                raise ValueError('build matrix configured profile or stages differ: '+name)
            postgres = build/'src/backend/postgres'
            if (not postgres.is_file() or not os.access(postgres, os.X_OK)
                    or record['postgres_sha256'] != sha(postgres)):
                raise ValueError('build matrix postgres differs: '+name)
            for stage, recorded in zip(profile['stages'], record['stages']):
                kind = stage['kind']
                if (recorded['kind'] != kind or recorded['command'] != stage['command']
                        or type(recorded.get('rc')) is not int or recorded['rc'] != 0
                        or recorded.get('passed') is not True):
                    raise ValueError('original build matrix stage did not pass: '+name+'/'+kind)
                log_name = name+'/'+kind+'.log'
                text = read(log_name).decode(errors='replace')
                if recorded['log_sha256'] != files[log_name]:
                    raise ValueError('build matrix log hash differs: '+log_name)
                passed = (kind == 'build' or
                    (units_passed(text, 0) if kind == 'cluster-unit' else
                     sql_passed(text, 0, selection[
                         'cluster_tests' if kind == 'cluster-sql' else 'pg_tests'])))
                if not passed:
                    raise ValueError('original build matrix log judge failed: '+log_name)
            if build.resolve() == args.build.resolve():
                if (not profile['cluster']
                        or record['postgres_sha256'] != args.expected_postgres_sha256):
                    raise ValueError('native build must match an enabled matrix postgres')
                selected.append(name)
        if len(selected) != 1:
            raise ValueError('native build does not select exactly one matrix profile')
        return dict(profile=selected[0], postgres_sha256=args.expected_postgres_sha256,
            source_commit='NOT_ATTESTED', files_sha256=files, root=str(root))
    except (KeyError, TypeError, IndexError) as error:
        raise ValueError('incomplete build matrix receipt fields') from error


def run(args):
    validate_environment(os.environ)
    selection = plan(args)
    if args.plan:
        print(json.dumps(selection, indent=2))
        return 0
    if os.geteuid() == 0:
        raise ValueError('original regression databases require a nonroot test owner')
    source, output = args.source.resolve(strict=True), args.output.resolve()
    builds = [Path(profile['build']).resolve(strict=True) for profile in selection['profiles']]
    paths = [source, output, *builds]
    if (output.exists() or args.output.is_symlink()
            or any(a.is_relative_to(b) or b.is_relative_to(a)
                   for index, a in enumerate(paths) for b in paths[index+1:])):
        raise ValueError('source/build/output paths must be distinct nonoverlapping trees')
    identities = [profile_identity(build, source, profile['cluster'], profile['cassert'])
                  for build, profile in zip(builds, selection['profiles'])]
    snapshot = source_fingerprint(source)
    output.mkdir(mode=0o700)
    (output/'plan.json').write_text(json.dumps(selection, indent=2)+'\n')
    (output/'source-sha256.json').write_text(json.dumps(snapshot, indent=2)+'\n')
    result = dict(matrix_passed=False, r01='NOT_READY', stage_acceptance='NOT_READY',
        build_binding='GENERATED_CONFIG_AND_COMMAND_ONLY', source_commit='NOT_ATTESTED',
        profiles=[], error=None, not_covered=selection['not_covered'])
    try:
        for profile, build, identity in zip(selection['profiles'], builds, identities):
            directory = output/profile['profile']
            directory.mkdir(mode=0o700)
            record = dict(profile=profile['profile'], identity=identity, stages=[], passed=False)
            result['profiles'].append(record)
            postgres = build/'src/backend/postgres'
            binary_sha = None
            for stage in profile['stages']:
                if (profile_identity(build, source, profile['cluster'], profile['cassert']) != identity
                        or source_fingerprint(source) != snapshot):
                    raise ValueError('source or configured profile changed before stage')
                if binary_sha is not None and sha(postgres) != binary_sha:
                    raise ValueError('profile postgres changed between stages')
                log_path = directory/(stage['kind']+'.log')
                with log_path.open('x') as log:
                    reply = subprocess.run(stage['command'], stdout=log, stderr=subprocess.STDOUT,
                        env=dict(os.environ, LC_ALL='C', PYTHONDONTWRITEBYTECODE='1'))
                text = log_path.read_text(errors='replace')
                if stage['kind'] == 'build':
                    passed = reply.returncode == 0 and postgres.is_file() and os.access(postgres, os.X_OK)
                    if passed:
                        binary_sha = sha(postgres)
                        record['postgres_sha256'] = binary_sha
                elif stage['kind'] == 'cluster-unit':
                    passed = bool(units_passed(text, reply.returncode))
                else:
                    passed = sql_passed(text, reply.returncode, selection[
                        'cluster_tests' if stage['kind'] == 'cluster-sql' else 'pg_tests'])
                record['stages'].append(dict(kind=stage['kind'], command=stage['command'],
                    rc=reply.returncode, passed=passed, log_sha256=sha(log_path)))
                if (source_fingerprint(source) != snapshot
                        or profile_identity(build, source, profile['cluster'], profile['cassert']) != identity
                        or (binary_sha is not None and sha(postgres) != binary_sha)):
                    raise ValueError('source/profile/postgres changed during stage')
                if not passed:
                    raise ValueError('first failed original matrix stage: '+profile['profile']+'/'+stage['kind'])
            record['passed'] = True
        result['matrix_passed'] = True
    except Exception as error:
        result['error'] = repr(error)
    finally:
        (output/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    return 0 if result['matrix_passed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('source', 'output', *(name for name, _, _ in PROFILES)):
        parser.add_argument('--'+key, type=Path, required=True)
    parser.add_argument('--plan', action='store_true')
    try:
        return run(parser.parse_args())
    except (OSError, ValueError) as error:
        parser.exit(1, str(error)+'\n')


if __name__ == '__main__':
    sys.exit(main())
