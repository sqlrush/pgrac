#!/usr/bin/env python3
"""Four-member long-run driver: start, load, collect, normal stop, judge.

run       native four-member fixture on one Linux development VM (root)
judge     recompute the verdict of a retained run directory (any machine)
selftest  check the driver itself against one ordinary local instance
See P0_LONGRUN.md. Starting a run does not reserve a machine or satisfy any
separate scheduling requirement.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import pwd
import signal
import subprocess
import sys
import time
from typing import Dict, List, Mapping, Optional, Sequence

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
SOURCE = HERE.parents[3]

import p0_longrun_pq as pq                                      # noqa: E402
from p0_longrun_cluster import CohortProvider, log_offsets, node_layout  # noqa: E402
from p0_longrun_cycle import run_cycle, write_json              # noqa: E402
from p0_longrun_judge import NATIVE, SELFTEST, judge            # noqa: E402
from p0_longrun_load import TABLE, Endpoint                     # noqa: E402
from p0_longrun_preflight import (failed, formatter_build_command,  # noqa: E402
                                  preflight, sha256)
from p0_longrun_report import render                            # noqa: E402

LOG = logging.getLogger('p0_longrun')
DEFAULT_CONF = ('autovacuum = off', "shared_buffers = '16MB'", 'log_error_verbosity = verbose')
LIFECYCLE_ALLOWANCE = 1800      # controller watchdog beyond the workload time
HARNESS = sorted(p.name for p in HERE.glob('p0_longrun*.py'))


def cycle_specs(args: argparse.Namespace) -> List[Dict[str, object]]:
    """Expand --clients/--cycles into one specification per cycle."""
    try:
        levels = [int(v) for v in str(args.clients).split(',')]
    except ValueError as error:
        raise SystemExit('--clients takes integers, e.g. 4 or 2,4,8') from error
    count = args.cycles or len(levels)
    if len(levels) == 1:
        levels = levels * count
    if len(levels) != count or count < 1 or any(not 1 <= v <= 16 for v in levels):
        raise SystemExit('--clients must give 1..16 per member, once or once per cycle')
    seconds = args.minutes * 60.0
    if seconds <= 0 or args.rows < 1 or not 0 <= args.crash_cycle <= count:
        raise SystemExit('invalid duration, row count or crash cycle')
    return [dict(cycle=n + 1, clients_per_node=level, seconds=seconds, rows=args.rows,
                 drop_step=bool(args.drop_step and n == 0), crash=args.crash_cycle == n + 1,
                 restart_verify=bool(args.restart_verify), grace_seconds=args.grace_seconds,
                 stall_abort_seconds=args.stall_abort_seconds)
            for n, level in enumerate(levels)]


def merged_conf(extra: Sequence[str]) -> List[str]:
    """Default settings, each replaceable by a later --conf of the same name."""
    chosen: Dict[str, str] = {}
    for line in (*DEFAULT_CONF, *extra):
        name = line.split('=', 1)[0].strip()
        if '=' not in line or not name:
            raise SystemExit('--conf takes name = value')
        chosen[name] = line
    return list(chosen.values())


def identity(install: Path, formatter: Optional[Path], args: argparse.Namespace) -> Dict[str, object]:
    def commit() -> Optional[str]:
        try:
            done = subprocess.run(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True,
                                  capture_output=True, timeout=20)
        except (OSError, subprocess.TimeoutExpired):
            return None
        return done.stdout.strip() if done.returncode == 0 else None
    library = pq.find_library(install)
    return dict(postgres_sha256=sha256(install / 'bin/postgres'), libpq=str(library),
                libpq_sha256=sha256(library), install=str(install),
                formatter_sha256=sha256(formatter) if formatter else None,
                source_commit=commit(), source_commit_is_declared_only=True,
                harness={name: sha256(HERE / name) for name in HARNESS},
                arguments={k: str(v) for k, v in sorted(vars(args).items()) if k != 'handler'},
                started_unix=time.time())


def finish(run: Path, allow: Optional[Path]) -> int:
    result = judge(run, allow)
    write_json(run / 'result.json', result)
    (run / 'SUMMARY.md').write_text(render(result), encoding='utf-8')
    LOG.info('verdict %s; see %s', result['verdict'], run / 'SUMMARY.md')
    sys.stdout.write(json.dumps(dict(verdict=result['verdict'], result=str(run / 'result.json'),
                                     summary=str(run / 'SUMMARY.md'))) + '\n')
    return 0 if result['verdict'] in ('PASS', 'SELFTEST_OK') else 1


def connector(install: Path):
    library = pq.load(pq.find_library(install))
    return lambda endpoint: pq.Connection(library, endpoint.conninfo)


def command_cycle(args: argparse.Namespace) -> int:
    """One native cycle as the unprivileged database user (internal)."""
    spec = json.loads(Path(args.spec).read_text())
    root = Path(spec['cycle_root'])
    connect = connector(Path(spec['install']))
    provider = CohortProvider(spec['layout'], Path(spec['install']) / 'bin',
                              Path(spec['entry_file']), HERE, spec['user'], root, connect)
    run_cycle(spec, provider, connect, root)
    return 0


def postmaster_alive(node: Dict[str, object]) -> bool:
    try:
        pid = (Path(str(node['data_dir'])) / 'postmaster.pid').read_text().split()[0]
    except (OSError, IndexError):
        return False
    return (Path('/proc') / pid).exists()


def terminate_child(child: subprocess.Popen) -> None:
    """Stop the owned process group and reap it before member cleanup."""
    try:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=10)
    except BaseException as error:
        raise RuntimeError('cycle process not reaped: %s' % error) from error


def run_child(command: Sequence[str], log: Path, seconds: float) -> Optional[str]:
    """Run one cycle process in its own session; returns a problem or None."""
    with log.open('w') as stream:
        child = subprocess.Popen(list(command), stdout=stream, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        try:
            code = child.wait(timeout=seconds)
            return None if code == 0 else 'cycle process exited with status %d' % code
        except subprocess.TimeoutExpired:
            terminate_child(child)
            return 'cycle process exceeded the controller watchdog of %d seconds' % seconds
        except BaseException:
            terminate_child(child)
            raise


def blocked(reason: str, missing: Sequence[object] = ()) -> int:
    """Nothing about the product was exercised; never a verdict."""
    LOG.error('BLOCKED: %s', reason)
    sys.stdout.write(json.dumps(dict(status='BLOCKED', reason=reason,
                                     missing=list(missing))) + '\n')
    return 2


def start_cycle(quorum: object, formatter: Path, output: Path, install: Path, user: object,
                spec: Dict[str, object], conf: Sequence[str]) -> Dict[str, object]:
    """Fresh voting devices, entry file and specification of one cycle."""
    from cohort_entry import entry_profile
    from native_lifecycle import ENTRY, OPERATIONS
    root = output / ('cycle%d' % spec['cycle'])
    root.mkdir()
    os.chown(root, user.pw_uid, user.pw_gid)
    votes = []
    for index in range(3):     # every new cohort needs freshly formatted devices
        image = root / ('vote%d.image' % index)
        quorum.run([formatter, image, index])
        votes.append(quorum.attach_vote(image))
    entry = entry_profile(output / 'quorum/handles.json', formatter, votes)
    entry['operations'] = {operation: dict(ENTRY) for operation in OPERATIONS}
    write_json(root / 'entry.json', entry)
    layout = dict(node_layout(root / 'cluster'), name=quorum.name, extra_conf=list(conf))
    full = dict(spec, cycle_root=str(root), layout=layout, install=str(install),
                entry_file=str(root / 'entry.json'), user=user.pw_name)
    write_json(root / 'spec.json', full)
    return full


def finish_cycle(quorum: object, full: Mapping[str, object], install: Path) -> List[str]:
    """Error cleanup of members a cycle left running; never a normal stop."""
    from cohort_probe import cleanup
    nodes = full['layout']['nodes']
    if not any((Path(n['data_dir']) / 'postmaster.pid').exists() for n in nodes):
        return []
    problems, detail, marks = [], 'forced', log_offsets(nodes)
    try:
        cleanup(quorum, nodes, install / 'bin')
    except Exception as error:
        detail = 'forced cleanup failed: %s' % error
        problems.append(detail)
    write_json(Path(str(full['cycle_root'])) / 'forced-cleanup.json',
               dict(log_start=marks, detail=detail))
    if any(postmaster_alive(n) for n in nodes):
        problems.append('a member process survived cleanup')
    return problems


def command_run(args: argparse.Namespace) -> int:
    install, output = args.install.resolve(), args.output.absolute()
    vendor, formatter = args.vendor.resolve(), args.vote_formatter
    specs = cycle_specs(args)
    conf = merged_conf(args.conf)
    checks = preflight(install, output, SOURCE, vendor, args.uid, args.gid, formatter,
                       args.expect_postgres_sha256)
    for check in checks:
        LOG.info('preflight %-45s %s %s', check.name, 'ok' if check.ok else 'MISSING',
                 check.detail)
    if failed(checks):
        return blocked('prerequisites are missing', [c._asdict() for c in failed(checks)])
    if args.preflight_only:
        sys.stdout.write(json.dumps(dict(status='READY', cycles=specs, conf=conf)) + '\n')
        return 0
    from local_quorum import LocalQuorum, native_library_dir
    output.mkdir(mode=0o755)
    os.chown(output, args.uid, args.gid)
    user = pwd.getpwuid(args.uid)
    write_json(output / 'preflight.json', [c._asdict() for c in checks])
    if formatter is None:
        formatter = output / 'native_vote'
        built = subprocess.run(formatter_build_command(SOURCE, install, formatter),
                               capture_output=True, text=True, timeout=300)
        if built.returncode != 0:
            return blocked('the vote formatter did not build: ' + built.stderr[-600:])
    formatter = formatter.resolve()
    write_json(output / 'identity.json', identity(install, formatter, args))
    write_json(output / 'plan.json', dict(mode=NATIVE, members=4, cycles=specs, conf=conf))
    quorum = LocalQuorum(output / 'quorum', vendor, 4, args.uid, args.gid)
    quorum.env['LD_LIBRARY_PATH'] += ':' + str(install / 'lib')
    try:
        quorum.start()
    except Exception as error:
        try:
            quorum.stop()
        except Exception as cleanup_error:
            LOG.error('quorum cleanup: %s', cleanup_error)
        return blocked('the local quorum fixture did not start: %s' % error)
    problems: List[str] = []
    safe_to_stop = True
    prefix = ['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups',
              'env', 'HOME=' + user.pw_dir,
              'PATH=%s:%s' % (install / 'bin', os.environ.get('PATH', '')),
              'LD_LIBRARY_PATH=%s:%s' % (native_library_dir(vendor), install / 'lib'),
              'PYTHONDONTWRITEBYTECODE=1', sys.executable, str(Path(__file__).resolve())]
    try:
        for spec in specs:
            full = start_cycle(quorum, formatter, output, install, user, spec, conf)
            LOG.info('cycle %d: %d connection(s) per member for %d s', spec['cycle'],
                     spec['clients_per_node'], spec['seconds'])
            root = Path(str(full['cycle_root']))
            safe_to_stop, child_reaped = False, False
            try:
                try:
                    problem = run_child(prefix + ['cycle', '--spec', str(root / 'spec.json')],
                                        root / 'cycle.log',
                                        float(spec['seconds']) + LIFECYCLE_ALLOWANCE)
                    child_reaped = True
                except KeyboardInterrupt:
                    # run_child propagates interruption only after successful reaping.
                    child_reaped = True
                    raise
                if problem:
                    problems.append('cycle %d: %s' % (spec['cycle'], problem))
            finally:
                left = finish_cycle(quorum, full, install)
                problems += ['cycle %d: %s' % (spec['cycle'], p) for p in left]
                safe_to_stop = child_reaped and not left
            if not safe_to_stop:
                problems.append('later cycles were not run')
                break
    except KeyboardInterrupt:
        problems.append('controller interrupted; cycle did not complete')
    except Exception as error:
        problems.append('%s: %s' % (type(error).__name__, error))
    finally:
        if safe_to_stop:
            try:
                quorum.stop()
            except Exception as error:
                problems.append('quorum cleanup: %s' % error)
        else:
            problems.append('quorum retained: cycle process or member cleanup unconfirmed')
        write_json(output / 'controller.json', dict(problems=problems,
                                                    finished_unix=time.time()))
    return finish(output, args.allow)


def command_judge(args: argparse.Namespace) -> int:
    return finish(args.run.resolve(), args.allow)


def command_selftest(args: argparse.Namespace) -> int:
    """Whole driver path on one ordinary instance; never an acceptance run."""
    from p0_longrun_plain import PlainProvider
    install, output = args.install.resolve(), args.output.absolute()
    output.mkdir(mode=0o755)
    root = output / 'cycle1'
    root.mkdir()
    spec = dict(cycle=1, clients_per_node=args.clients, seconds=float(args.seconds),
                rows=args.rows, drop_step=True, crash=bool(args.crash),
                restart_verify=bool(args.restart_verify), grace_seconds=args.grace_seconds,
                stall_abort_seconds=0.0)
    write_json(output / 'identity.json', identity(install, None, args))
    write_json(output / 'plan.json', dict(mode=SELFTEST, members=1, cycles=[spec],
                                          conf=list(DEFAULT_CONF)))
    connect = connector(install)
    provider = PlainProvider(root / 'cluster', install / 'bin', args.port, connect,
                             ('cluster.enabled = off', 'autovacuum = off',
                              'log_error_verbosity = verbose'))

    def inject(endpoint: Endpoint) -> None:
        conn = connect(endpoint)
        if conn.connect() is None:
            conn.execute(
                "CREATE FUNCTION lab.selftest_fail() RETURNS trigger LANGUAGE plpgsql AS "
                "$$BEGIN IF NEW.id % 5 = 0 THEN RAISE EXCEPTION 'selftest injected failure' "
                "USING ERRCODE = 'P0LR1'; END IF; RETURN NEW; END$$")
            conn.execute('CREATE TRIGGER selftest_fail BEFORE UPDATE ON %s FOR EACH ROW '
                         'EXECUTE FUNCTION lab.selftest_fail()' % TABLE)
            conn.close()
    try:
        run_cycle(spec, provider, connect, root,
                  after_seed=inject if args.inject_error else None)
    finally:
        if provider.running:
            subprocess.run([str(install / 'bin/pg_ctl'), 'stop', '-D', str(provider.data),
                            '-m', 'immediate', '-w'], capture_output=True, timeout=60)
            write_json(root / 'forced-cleanup.json',
                       dict(log_start=log_offsets(provider.members), detail='forced'))
    return finish(output, args.allow)


def parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = top.add_subparsers(dest='command', required=True)

    def workload(sub: argparse.ArgumentParser, rows: int) -> None:
        sub.add_argument('--install', type=Path, required=True,
                         help='installation under test (contains bin/ and lib/)')
        sub.add_argument('--output', type=Path, required=True, help='new result directory')
        sub.add_argument('--rows', type=int, default=rows, help='rows of the shared table')
        sub.add_argument('--grace-seconds', type=float, default=60.0,
                         help='time clients get to return after the workload time')
        sub.add_argument('--restart-verify', action='store_true',
                         help='after the normal stop: restart, reconcile, stop again')
        sub.add_argument('--allow', type=Path, help='explicit allow-list (default: none)')

    run = commands.add_parser('run', help='native four-member run (root, Linux)')
    workload(run, 131072)
    run.add_argument('--clients', default='4',
                     help='connections per member: one value, or one per cycle (2,4,8)')
    run.add_argument('--cycles', type=int, default=0,
                     help='start/load/stop cycles (default: number of --clients values)')
    run.add_argument('--minutes', type=float, default=60.0, help='workload time per cycle')
    run.add_argument('--vendor', type=Path, default=Path('/'),
                     help='root of the unpacked Corosync packages')
    run.add_argument('--vote-formatter', type=Path,
                     help='native voting-disk formatter (built into the output if omitted)')
    run.add_argument('--uid', type=int, default=1000)
    run.add_argument('--gid', type=int, default=1000)
    run.add_argument('--conf', action='append', default=[], metavar='NAME = VALUE',
                     help='extra server setting; repeatable')
    run.add_argument('--no-drop-step', dest='drop_step', action='store_false',
                     help='omit the DROP TABLE step of the first cycle')
    run.add_argument('--crash-cycle', type=int, default=0, metavar='N',
                     help='in cycle N, SIGKILL every member under load, then restart '
                          'and reconcile (default 0: not run)')
    run.add_argument('--stall-abort-seconds', type=float, default=300.0,
                     help='end the workload early when no transaction completes for '
                          'this long (0 disables)')
    run.add_argument('--expect-postgres-sha256')
    run.add_argument('--preflight-only', action='store_true',
                     help='check prerequisites and exit without creating anything')
    run.set_defaults(handler=command_run)

    cycle = commands.add_parser('cycle', help=argparse.SUPPRESS)
    cycle.add_argument('--spec', type=Path, required=True)
    cycle.set_defaults(handler=command_cycle)

    again = commands.add_parser('judge', help='judge a retained run directory')
    again.add_argument('--run', type=Path, required=True)
    again.add_argument('--allow', type=Path)
    again.set_defaults(handler=command_judge)

    check = commands.add_parser('selftest', help='driver check on one ordinary instance')
    workload(check, 4096)
    check.add_argument('--clients', type=int, default=4)
    check.add_argument('--seconds', type=float, default=15.0)
    check.add_argument('--port', type=int, default=54329)
    check.add_argument('--crash', action='store_true')
    check.add_argument('--inject-error', action='store_true',
                       help='make every fifth row fail, to see the judge refuse the run')
    check.set_defaults(handler=command_selftest)
    return top


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format='%(asctime)s %(message)s')
    args = parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == '__main__':
    raise SystemExit(main())
