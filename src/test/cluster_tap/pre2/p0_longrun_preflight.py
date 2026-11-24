"""Read-only prerequisite checks for a native four-member long run.

Nothing is created, started or changed here; every check reports what it
looked at so a missing prerequisite is visible before any server starts.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import platform
import pwd
import shutil
import subprocess
import sys
from typing import List, NamedTuple, Optional, Sequence

SYSTEM_TOOLS = ('unshare', 'nsenter', 'setpriv', 'ip', 'losetup', 'sudo')
VENDOR_TOOLS = ('usr/sbin/corosync', 'usr/sbin/corosync-qdevice', 'usr/sbin/corosync-quorumtool',
                'usr/sbin/corosync-cmapctl', 'usr/sbin/corosync-qdevice-tool',
                'usr/sbin/corosync-qdevice-net-certutil', 'usr/bin/corosync-qnetd',
                'usr/bin/corosync-qnetd-certutil')
PRODUCT_TOOLS = ('postgres', 'initdb', 'pg_ctl', 'psql', 'pgrac-init')
MIN_FREE_BYTES = 8 << 30
MAX_SOCKET_PATH = 100


class Check(NamedTuple):
    name: str
    ok: bool
    detail: str


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def formatter_build_command(source: Path, install: Path, output: Path) -> List[str]:
    """The documented build of the native voting-disk formatter."""
    return ['cc', '-D_GNU_SOURCE', '-ffunction-sections', '-fdata-sections',
            '-I', str(install / 'include/server'), '-I', str(source / 'src/include'),
            str(source / 'src/test/cluster_tap/pre2/native_vote.c'),
            str(source / 'src/backend/cluster/cluster_voting_disk_io.c'),
            str(install / 'lib/libpgport.a'), '-Wl,--gc-sections', '-o', str(output)]


def running_servers() -> List[str]:
    found = []
    for entry in Path('/proc').iterdir():
        if entry.name.isdigit():
            try:
                if (entry / 'comm').read_text().strip() == 'postgres':
                    found.append(entry.name)
            except OSError:
                continue
    return found


def preflight(install: Path, output: Path, source: Path, vendor: Path, uid: int, gid: int,
              formatter: Optional[Path], expected_sha256: Optional[str]) -> List[Check]:
    checks: List[Check] = []

    def add(name: str, ok: bool, detail: object = '') -> None:
        checks.append(Check(name, bool(ok), str(detail)))

    add('linux', platform.system() == 'Linux', platform.platform())
    add('root', hasattr(os, 'geteuid') and os.geteuid() == 0,
        'namespaces and loop devices need root')
    add('python', sys.version_info >= (3, 9), sys.version.split()[0])
    for tool in SYSTEM_TOOLS:
        add('tool ' + tool, shutil.which(tool) is not None, shutil.which(tool) or 'not on PATH')
    for tool in VENDOR_TOOLS:
        add('quorum ' + tool, (vendor / tool).is_file(), vendor / tool)
    for tool in PRODUCT_TOOLS:
        add('product ' + tool, (install / 'bin' / tool).is_file(), install / 'bin' / tool)
    libraries = sorted(str(p) for p in (install / 'lib').glob('libpq.so*')) \
        if (install / 'lib').is_dir() else []
    add('product libpq', bool(libraries), libraries[:1] or install / 'lib')
    postgres = install / 'bin/postgres'
    if postgres.is_file():
        actual = sha256(postgres)
        add('postgres sha256', expected_sha256 in (None, actual),
            actual if expected_sha256 in (None, actual)
            else 'installed %s, expected %s' % (actual, expected_sha256))
    add('source tree', (source / 'src/include/catalog/catversion.h').is_file(), source)
    if formatter is not None:
        add('vote formatter', formatter.is_file() and os.access(formatter, os.X_OK), formatter)
    else:
        needed = formatter_build_command(source, install, Path('native_vote'))[8:11]
        missing = [p for p in needed if not Path(p).is_file()]
        add('vote formatter build inputs', shutil.which('cc') is not None and not missing,
            'cc: %s; missing inputs: %s' % (shutil.which('cc'), missing or 'none'))
    try:
        user = pwd.getpwuid(uid).pw_name
        add('database user', uid != 0, '%s (uid %d, gid %d)' % (user, uid, gid))
    except KeyError:
        add('database user', False, 'uid %d does not exist' % uid)
    if shutil.which('setpriv') and shutil.which('sudo') and platform.system() == 'Linux':
        done = subprocess.run(['setpriv', '--reuid', str(uid), '--regid', str(gid),
                               '--init-groups', 'sudo', '-n', 'true'],
                              capture_output=True, text=True, timeout=20)
        add('passwordless sudo for the database user', done.returncode == 0,
            done.stderr.strip() or 'ok')
    add('output is new', not os.path.lexists(output) and output.parent.is_dir(), output)
    socket = output / 'cycle99/cluster/sockets/.s.PGSQL.18003.lock'
    add('socket path length', len(str(socket)) <= MAX_SOCKET_PATH,
        '%d bytes (limit %d)' % (len(str(socket)), MAX_SOCKET_PATH))
    if output.parent.is_dir():
        free = shutil.disk_usage(output.parent).free
        add('free space', free >= MIN_FREE_BYTES, '%.1f GiB free' % (free / (1 << 30)))
    add('lock directory', Path('/run/lock').is_dir(), '/run/lock')
    if Path('/proc/self/stat').exists():
        servers = running_servers()
        add('no other postgres process', not servers, servers[:8] or 'none')
    return checks


def failed(checks: Sequence[Check]) -> List[Check]:
    return [check for check in checks if not check.ok]
