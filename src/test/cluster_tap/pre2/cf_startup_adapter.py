#!/usr/bin/env python3
"""Hold an actual Startup CF grant in an isolated native TAP cohort.

Only the test debugger pauses an original process; no database memory or
persistent authority is edited. All other commands use the normal adapter.
Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import blackbox
from cohort_entry import CohortEntry


class StartupContention(CohortEntry):
    debugger = None
    hook_root = None

    def run_product(self, argv, **kwargs):
        if (Path(argv[0]).name != 'pg_ctl' or argv[1:2] != ['start']
                or self.command_node('pg_ctl', argv[1:]) != 0):
            return super().run_product(argv, **kwargs)
        if self.debugger is not None:
            raise RuntimeError('the test debugger cannot be reused')
        stage = sys.argv[1]
        target = {'writer': 'ClusterStartupWriterBegin',
                  'checkpoint': 'ClusterStartupCheckpointPublish'}[stage]
        root = Path(os.environ['TESTLOGDIR']).resolve(strict=True)/('cf-startup-'+stage)
        root.mkdir(mode=0o700)
        self.hook_root = root
        node = self.layout['nodes'][0]
        # PGRAC: follow only the Startup fork, then observe a real granted X.
        commands = '\n'.join([
            'set pagination off', 'set confirm off', 'set debuginfod enabled off',
            'set follow-fork-mode parent', 'set detach-on-fork on',
            'handle SIGUSR1 nostop noprint pass', 'handle SIGUSR2 nostop noprint pass',
            'handle SIGALRM nostop noprint pass', 'handle SIGPIPE nostop noprint pass',
            'handle SIGHUP nostop noprint pass',
            'python', 'import gdb, os, pathlib, json, time', 'seen = {}', 'try:',
            '    pathlib.Path('+repr(str(root/'debugger.pid'))+').write_text(str(os.getpid()))',
            '    child = gdb.Breakpoint("StartChildProcess")',
            '    child.condition = "type == StartupProcess"',
            '    pathlib.Path('+repr(str(root/'armed'))+').touch()',
            '    gdb.execute("run")',
            '    if gdb.newest_frame().name() != "StartChildProcess":',
            '        raise RuntimeError("Startup fork was not reached")',
            '    seen["postmaster_pid"] = gdb.selected_inferior().pid',
            '    child.delete()',
            '    gdb.execute("set follow-fork-mode child")',
            '    stage = gdb.Breakpoint('+repr(target)+', temporary=True)',
            '    gdb.execute("continue")',
            '    if gdb.newest_frame().name() != '+repr(target)+':',
            '        raise RuntimeError("native startup consumer was not reached")',
            '    seen["startup_pid"] = gdb.selected_inferior().pid',
            '    seen["consumer"] = '+repr(target),
            '    grant = gdb.Breakpoint("cluster_cf_lock")',
            '    grant.condition = "mode == 7"',
            '    gdb.execute("continue")',
            '    if gdb.newest_frame().name() != "cluster_cf_lock":',
            '        raise RuntimeError("original CF acquire was not reached")',
            '    grant.delete()',
            '    gdb.execute("finish")',
            '    hold = gdb.parse_and_eval("cf_hold_x")',
            '    print("ORIGINAL_STARTUP_CF_HOLD", hold)',
            '    if not hold["held"] or not hold["coordinated"] or hold["release_pending"]:',
            '        raise RuntimeError("original coordinated CF X was not established")',
            '    seen["held"] = True',
            '    seen["hold_start"] = time.monotonic()',
            '    time.sleep(2.8)',
            '    seen["hold_elapsed"] = time.monotonic()-seen["hold_start"]',
            'finally:',
            '    pathlib.Path('+repr(str(root/'observation.json'))+').write_text(json.dumps(seen, indent=2))',
            '    if gdb.selected_inferior().pid:',
            '        gdb.execute("detach")',
            'end', 'quit', ''])
        (root/'commands.gdb').write_text(commands)
        command = self.namespace_command(0, ['gdb', '-q', '--batch', '-x',
            str(root/'commands.gdb'), '--args', str(self.bindir/'postgres'),
            '-D', node['data_dir']])
        with Path(node['logfile']).open('ab') as log:
            self.debugger = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, env=kwargs.get('env'))
        deadline = time.monotonic()+min(5, self.remaining())
        while not (root/'armed').exists() and time.monotonic() < deadline:
            if self.debugger.poll() is not None:
                raise RuntimeError('startup debugger exited before arming')
            time.sleep(0.01)
        if not (root/'armed').exists():
            raise RuntimeError('startup debugger did not arm within its fixture deadline')
        return subprocess.CompletedProcess(argv, 0, 'test debugger launched native postmaster\n', '')

    def finish_debugger(self):
        if self.debugger is None:
            return
        forced = []
        try:
            self.debugger.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # Signal only the exact debugger, never its database inferior.
            pid = int((self.hook_root/'debugger.pid').read_text())
            process = Path('/proc')/str(pid)
            for sig in ('INT', 'TERM', 'KILL'):
                if self.debugger.poll() is not None:
                    break
                if ((process/'exe').resolve(strict=True).name != 'gdb'
                        or process.stat().st_uid != os.getuid()):
                    raise RuntimeError('owned debugger identity changed')
                forced.append(sig)
                subprocess.run(['kill', '-'+sig, str(pid)], check=True)
                try:
                    self.debugger.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    continue
            if self.debugger.poll() is None:
                raise RuntimeError('owned debugger could not be reaped')
        result = dict(rc=self.debugger.returncode, forced=forced)
        (self.hook_root/'result.json').write_text(json.dumps(result)+'\n')
        if forced or self.debugger.returncode != 0:
            raise RuntimeError('startup debugger did not finish naturally')
        seen = json.loads((self.hook_root/'observation.json').read_text())
        if not seen.get('held') or seen.get('hold_elapsed', 0) < 2.8:
            raise RuntimeError('the original CF hold was not observed')

    def shared_start(self):
        try:
            return super().shared_start()
        finally:
            self.finish_debugger()


if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] not in ('writer', 'checkpoint'):
        raise ValueError('one native startup consumer is required')
    import cohort_entry
    cohort_entry.CohortEntry = StartupContention
    try:
        blackbox.main()
    except Exception as error:
        print(json.dumps(dict(version=1, status='ERROR', reason=str(error))))
