"""Opt-in Darwin observer component driver; never a shared-admission proof."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import sys
import time

from m2_run15_observer import Run15Observer


class BsdInfo(ctypes.Structure):
    # SDK sys/proc_info.h: proc_bsdinfo, PROC_PIDTBSDINFO=3.
    _fields_ = [(name, ctypes.c_uint32) for name in (
        'flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid', 'ruid', 'rgid',
        'svuid', 'svgid', 'reserved')] + [
        ('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32),
        *[(name, ctypes.c_uint32) for name in ('nfiles', 'pgid', 'jobc', 'tdev', 'tpgid')],
        ('nice', ctypes.c_int32), ('start_sec', ctypes.c_uint64), ('start_usec', ctypes.c_uint64)]


class MacObserver(Run15Observer):
    """Local test adapter; never selected by the Linux runtime factory."""
    def __init__(self, base, *args, **kwargs):
        if sys.platform != 'darwin':
            raise ValueError('Darwin component adapter requires Darwin')
        super().__init__(base, *args, proc_root=Path(base) / 'darwin-proc-projection', **kwargs)
        self.proc_root.mkdir(exist_ok=False)
        self.libproc = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        self.libproc.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                              ctypes.c_void_p, ctypes.c_int]
        self.libproc.proc_pidinfo.restype = ctypes.c_int
        self.libproc.proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        self.libproc.proc_pidpath.restype = ctypes.c_int

    def _kernel_identity(self, pid):
        before, after = BsdInfo(), BsdInfo()
        size = ctypes.sizeof(BsdInfo)
        if size != 136 or self.libproc.proc_pidinfo(pid, 3, 0, ctypes.byref(before), size) != size:
            raise OSError(ctypes.get_errno(), 'Darwin process identity unavailable')
        path = ctypes.create_string_buffer(4096)
        if self.libproc.proc_pidpath(pid, path, len(path)) <= 0:
            raise OSError(ctypes.get_errno(), 'Darwin executable path unavailable')
        if self.libproc.proc_pidinfo(pid, 3, 0, ctypes.byref(after), size) != size:
            raise OSError(ctypes.get_errno(), 'Darwin process recheck unavailable')
        names = ('pid', 'ppid', 'uid', 'start_sec', 'start_usec')
        if (any(getattr(before, name) != getattr(after, name) for name in names)
                or before.pid != pid or before.uid != self.uid
                or path.value.decode() != str(self.binary) or before.start_sec <= 0):
            raise ValueError('Darwin process identity changed or belongs to another installation')
        return dict(platform='Darwin', source='proc_pidinfo/proc_pidpath',
                    **{name: getattr(before, name) for name in names},
                    executable=path.value.decode())

    def _refresh(self, pid):
        native = self._kernel_identity(pid)
        process = self.proc_root / str(pid)
        process.mkdir(exist_ok=True)
        link = process / 'exe'
        if not link.is_symlink():
            link.symlink_to(self.binary)
        # Only identity fields are projected. This is not a Linux /proc sample.
        start = native['start_sec'] * 1000000 + native['start_usec']
        text = f"{pid} (Darwin identity projection) ? {native['ppid']} " + '0 ' * 17 + f'{start} 0\n'
        (process / 'stat').write_text(text)
        with (self.out / 'darwin-kernel-identities.jsonl').open('a') as output:
            output.write(json.dumps(native) + '\n')
        return native

    def _birth(self, pid):
        self._refresh(pid)
        return dict(super()._birth(pid), platform='Darwin', starttime_unit='epoch_microseconds')

    def _endpoint(self, pid, evidence):
        evidence.update(native_process=self._refresh(pid), producer_stat_projected=True)
        return super()._endpoint(pid, evidence)

    def _process_sample(self, pid):
        return dict(cpu_unknown='not collected by Darwin component fixture',
                    io_unknown='not collected by Darwin component fixture')

    def _command(self, pid):
        return ['/usr/bin/lldb', '--no-lldbinit', '--batch',
                '-O', 'settings set target.load-script-from-symbol-file false',
                '--attach-pid', str(pid), '-o', 'target variable MyProcPid cluster_node_id',
                '-o', 'thread backtrace all', '-o', 'process detach']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--manifest-sha', required=True)
    args = parser.parse_args()
    observer = MacObserver(args.base, args.base / 'frozen-identity.json',
                           args.manifest_sha, uid=os.getuid())
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        observer.step()
        if observer.records or observer.targets.get(4, {}).get('ended'):
            break
        time.sleep(0.05)
    summary = observer.summary()
    positive = next((r for r in observer.records if r['case'] == 4), {})
    trace = (observer.out / 'case-4-point-1.log')
    raw = trace.read_text() if trace.exists() else ''
    rows = {r['case']: r for r in observer.timeline}
    expected = observer.targets[4]['context']['producer_pid']
    result = dict(summary=summary, attachments=observer.attachments,
        positive_attach=positive.get('status') in ('PARTIAL_UNKNOWN', 'RAW_CAPTURED')
            and positive.get('returncode') == 0 and positive.get('attach_attempted')
            and bool(re.search(r'PostgresMain|exec_simple_query|LockAcquire', raw))
            and f'Process {expected} stopped' in raw,
        correct_endpoint=rows.get(4, {}).get('status') == 'SAMPLED'
            and rows[4].get('activity', {}).get('pid') == expected,
        negative_refusal=rows.get(8, {}).get('status') == 'IDENTITY_UNKNOWN'
            and rows[8].get('endpoint', {}).get('failed_predicates') == ['producer_parent_matches']
            and bool(rows[8]['endpoint'].get('producer_stat')))
    (args.base / 'OBSERVER-RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
