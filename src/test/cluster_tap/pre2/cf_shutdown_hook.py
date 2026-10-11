#!/usr/bin/env python3
"""Pause an isolated TAP checkpointer after its real CF X grant, then detach.

Requires noninteractive sudo/gdb in the disposable Linux test environment.
Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def execute(pid, root):
    # PGRAC: observe the original grant; never synthesize or edit an owner.
    commands = '\n'.join([
        'set pagination off', 'set confirm off', 'set debuginfod enabled off',
        'handle SIGUSR1 nostop noprint pass',
        'handle SIGUSR2 nostop noprint pass',
        'handle SIGALRM nostop noprint pass',
        'python', 'import gdb, pathlib, time',
        'try:',
        '    bp = gdb.Breakpoint("cluster_cf_lock")',
        '    bp.condition = "mode == 7"',
        '    pathlib.Path(' + repr(str(root/'armed')) + ').touch()',
        '    gdb.execute("continue")',
        '    if gdb.newest_frame().name() != "cluster_cf_lock":',
        '        raise RuntimeError("not stopped at the original CF acquire")',
        '    gdb.execute("finish")',
        '    hold = gdb.parse_and_eval("cf_hold_x")',
        '    print("ORIGINAL_CF_HOLD", hold)',
        '    if not hold["held"] or not hold["coordinated"] or hold["release_pending"]:',
        '        raise RuntimeError("original CF X grant was not established")',
        '    pathlib.Path(' + repr(str(root/'held')) + ').touch()',
        '    time.sleep(2.8)',
        'finally:',
        '    if gdb.selected_inferior().pid:',
        '        gdb.execute("detach")',
        'end', 'quit', ''])
    (root/'commands.gdb').write_text(commands)
    interrupted = False
    with (root/'gdb.log').open('x') as log:
        proc = subprocess.Popen(['sudo', '-n', 'gdb', '-q', '--batch',
                                 '-x', str(root/'commands.gdb'), '-p', str(pid)],
                                stdin=subprocess.DEVNULL, stdout=log,
                                stderr=subprocess.STDOUT)
        (root/'debugger.pid').write_text(str(proc.pid)+'\n')

        def interrupt(signum, frame):
            nonlocal interrupted
            interrupted = True
            if proc.poll() is None:
                subprocess.run(['sudo', '-n', 'kill', '-INT', str(proc.pid)],
                               check=False)

        signal.signal(signal.SIGUSR1, interrupt)
        try:
            rc = proc.wait(timeout=40)
        except subprocess.TimeoutExpired:
            interrupt(None, None)
            try:
                rc = proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # End only this attached debugger, never signal the database.
                subprocess.run(['sudo', '-n', 'kill', '-TERM', str(proc.pid)],
                               check=False)
                rc = proc.wait(timeout=5)
    result = dict(pid=pid, debugger_rc=rc, interrupted=interrupted,
                  held=(root/'held').is_file(), pause_seconds=2.8)
    (root/'result.json').write_text(json.dumps(result, indent=2)+'\n')


def main():
    action, raw_pid, raw_root = sys.argv[1:]
    pid = int(raw_pid)
    allowed = Path(os.environ['TESTLOGDIR']).resolve(strict=True)
    root = Path(raw_root).resolve()
    if pid <= 1 or root.parent != allowed:
        raise ValueError('hook is outside its exact TAP process/log scope')
    if action == 'start':
        expected = (Path(os.environ['PGRAC_PRE2_BINDIR'])/'postgres').resolve(strict=True)
        process = Path('/proc')/str(pid)
        if (process.stat().st_uid != os.getuid()
                or (process/'exe').resolve(strict=True) != expected):
            raise ValueError('checkpointer executable or UID differs')
        root.mkdir(mode=0o700)
        with (root/'worker.log').open('x') as log:
            child = subprocess.Popen([sys.executable, __file__, 'run', raw_pid, str(root)],
                                     stdin=subprocess.DEVNULL, stdout=log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
        (root/'worker.pid').write_text(str(child.pid)+'\n')
        return 0
    if action == 'run':
        execute(pid, root)
        return 0
    if action != 'finish':
        raise ValueError('unknown action')
    deadline = time.monotonic()+5
    while not (root/'result.json').exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if not (root/'result.json').exists():
        worker = int((root/'worker.pid').read_text())
        os.kill(worker, signal.SIGUSR1)
        deadline = time.monotonic()+12
        while not (root/'result.json').exists() and time.monotonic() < deadline:
            time.sleep(0.05)
    result = json.loads((root/'result.json').read_text())
    return 0 if result['debugger_rc'] == 0 and result['held'] and not result['interrupted'] else 1


if __name__ == '__main__':
    sys.exit(main())
