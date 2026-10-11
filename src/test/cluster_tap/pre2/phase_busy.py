#!/usr/bin/env python3
"""Exercise a real postmaster while LMON holds its original READY lock.

Run only in a disposable Linux test directory; no target memory is modified.
Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def finish_debugger(proc):
    """Reap only this test's debugger before attempting database cleanup."""
    actions = []
    if proc is None or proc.poll() is not None:
        return actions
    for name, stop in [('interrupt', lambda: proc.send_signal(signal.SIGINT)),
                       ('terminate', proc.terminate), ('kill', proc.kill)]:
        actions.append(name)
        stop()
        try:
            proc.wait(timeout=5)
            return actions
        except subprocess.TimeoutExpired:
            pass
    raise RuntimeError('the test debugger could not be reaped')


def run(args):
    root = args.output.resolve()
    root.mkdir(mode=0o700)
    data, sock = root/'data', root/'socket'
    sock.mkdir()
    bindir = args.bindir.resolve(strict=True)
    env = dict(os.environ, PATH=str(bindir)+':'+os.environ['PATH'])
    result = dict(passed=False, error=None, ready=False, stop_rc=None)
    with (root/'initdb.log').open('x') as log:
        subprocess.run([bindir/'initdb', '-D', data, '-A', 'trust', '--no-locale'],
                       env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    with (data/'postgresql.conf').open('a') as conf:
        conf.write("\nlisten_addresses = ''\nport = 58220\nshared_buffers = '16MB'\n"
                   "max_connections = 100\nmax_wal_senders = 0\nautovacuum = off\n"
                   "log_min_messages = debug1\nlog_line_prefix = '%m [%p] '\n"
                   "unix_socket_directories = '"+str(sock).replace("'", "''")+"'\n")
    observation = root/'observation.json'
    commands = '\n'.join([
        'set pagination off', 'set confirm off', 'set debuginfod enabled off',
        'set follow-fork-mode parent', 'set detach-on-fork on',
        'handle SIGUSR1 nostop noprint pass', 'handle SIGUSR2 nostop noprint pass',
        'handle SIGALRM nostop noprint pass', 'handle SIGPIPE nostop noprint pass',
        'python', 'import gdb, pathlib, json, time', 'seen = {}', 'try:',
        '    gdb.Breakpoint("cluster_postmaster_start_lmon", temporary=True)',
        '    gdb.execute("run")',
        '    seen["postmaster_pid"] = gdb.selected_inferior().pid',
        '    seen["postmaster_without_pgproc"] = int(gdb.parse_and_eval("MyProc")) == 0',
        '    if not seen["postmaster_without_pgproc"]:',
        '        raise RuntimeError("postmaster unexpectedly has a PGPROC")',
        '    gdb.execute("set follow-fork-mode child")',
        '    ready = gdb.Breakpoint("lmon_publish_status")',
        '    ready.condition = "status == 2"',
        '    gdb.execute("continue")',
        '    if gdb.newest_frame().name() != "lmon_publish_status":',
        '        raise RuntimeError("LMON READY producer was not reached")',
        '    ready.delete()',
        '    release = gdb.Breakpoint("LWLockRelease")',
        '    release.condition = "lock == &cluster_lmon_state->lwlock"',
        '    gdb.execute("continue")',
        '    if gdb.newest_frame().name() != "LWLockRelease":',
        '        raise RuntimeError("LMON original lock release was not reached")',
        '    seen["lmon_pid"] = gdb.selected_inferior().pid',
        '    seen["ready_status"] = int(gdb.parse_and_eval("cluster_lmon_state->status"))',
        '    seen["lmon_has_pgproc"] = int(gdb.parse_and_eval("MyProc")) != 0',
        '    gdb.execute("p *cluster_lmon_state")',
        '    if seen["ready_status"] != 2 or not seen["lmon_has_pgproc"]:',
        '        raise RuntimeError("LMON original READY lock is not established")',
        '    seen["hold_start"] = time.monotonic()',
        '    time.sleep(2.8)',
        '    seen["hold_elapsed"] = time.monotonic() - seen["hold_start"]',
        'finally:',
        '    pathlib.Path('+repr(str(observation))+').write_text(json.dumps(seen, indent=2)+"\\n")',
        '    if gdb.selected_inferior().pid:',
        '        gdb.execute("detach")',
        'end', 'quit', ''])
    (root/'commands.gdb').write_text(commands)
    proc = None
    try:
        with (root/'postgres-gdb.log').open('x') as log:
            proc = subprocess.Popen(['gdb', '-q', '--batch', '-x', str(root/'commands.gdb'),
                                     '--args', str(bindir/'postgres'), '-D', str(data)],
                                    env=env, stdin=subprocess.DEVNULL, stdout=log,
                                    stderr=subprocess.STDOUT)
            try:
                result['gdb_rc'] = proc.wait(timeout=70)
            except subprocess.TimeoutExpired:
                proc.send_signal(signal.SIGINT)
                result['gdb_rc'] = proc.wait(timeout=10)
                raise RuntimeError('debugger exceeded the unchanged phase budget')
        seen = json.loads(observation.read_text())
        result['observation'] = seen
        if result['gdb_rc'] != 0 or seen.get('hold_elapsed', 0) < 2.8:
            raise RuntimeError('the real READY-lock contention was not observed')
        deadline = time.monotonic()+60
        while time.monotonic() < deadline:
            pm = Path('/proc')/str(seen['postmaster_pid'])
            if not pm.exists() or pm.joinpath('stat').read_text().split(') ')[1].startswith('Z '):
                break
            # Observe readiness before connecting; startup probes themselves
            # must not manufacture connection FATALs in the acceptance log.
            text = (root/'postgres-gdb.log').read_text()
            if 'database system is ready to accept connections' in text:
                result['ready'] = True
                break
            time.sleep(0.1)
        if not result['ready']:
            raise RuntimeError('postmaster did not reach ready after the original lock release')
        reply = subprocess.run([bindir/'psql', '-XAt', '-h', sock, '-p', '58220', '-d', 'postgres',
                                '-v', 'ON_ERROR_STOP=1', '-c',
                                "SELECT value FROM pg_cluster_state WHERE category='phase' AND key='cluster_phase'"],
                               env=env, capture_output=True, text=True, timeout=10)
        (root/'phase-query.json').write_text(json.dumps(dict(rc=reply.returncode, stdout=reply.stdout,
                                                           stderr=reply.stderr), indent=2)+'\n')
        if reply.returncode != 0 or reply.stdout.strip() != 'running':
            raise RuntimeError('the real backend does not observe phase RUNNING')
        result['passed'] = True
    except Exception as error:
        result['error'] = repr(error)
    finally:
        try:
            result['debugger_cleanup'] = finish_debugger(proc)
            if (data/'postmaster.pid').exists():
                with (root/'stop.log').open('x') as log:
                    started = time.monotonic()
                    reply = subprocess.run([bindir/'pg_ctl', '-D', data, 'stop', '-m', 'fast', '-w', '-t', '30'],
                                           env=env, stdout=log, stderr=subprocess.STDOUT, timeout=35)
                    result['stop_rc'] = reply.returncode
                    result['stop_seconds'] = time.monotonic()-started
        except Exception as error:
            result['cleanup_error'] = repr(error)
            result['passed'] = False
        text = (root/'postgres-gdb.log').read_text() if (root/'postgres-gdb.log').exists() else ''
        result['passed'] = (result['passed'] and result['stop_rc'] == 0
                            and not result.get('debugger_cleanup')
                            and 'PANIC:' not in text and 'FATAL:' not in text)
        (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bindir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    raise SystemExit(run(parser.parse_args()))
