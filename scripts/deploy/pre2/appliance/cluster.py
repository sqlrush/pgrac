"""First-boot credentials and cluster services for the isolated PRE2 appliance."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
import time

import guest

ROOT = Path('/var/lib/pgrac-appliance')
CONFIG = Path('/etc/pgrac-appliance')


def manifest():
    if os.geteuid() != 0:
        raise RuntimeError('run through sudo')
    value = json.loads((CONFIG / 'manifest.json').read_text())
    if value['source_commit'] != '2d857abffc76a5cbfbc1b01c7d99b82ab0375f46':
        raise RuntimeError('unexpected appliance release')
    if len(value['nodes']) != 4:
        raise RuntimeError('incomplete appliance')
    for n, row in enumerate(value['nodes']):
        actual = guest.command(['virsh', '-c', 'qemu:///system', 'domuuid', guest.NAMES[n]]).stdout.strip()
        if row['name'] != guest.NAMES[n] or actual != row['uuid']:
            raise RuntimeError('appliance domain identity mismatch')
    return value


def all_nodes(fn):
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(fn, range(4)))


def wait_agents(seconds=300):
    deadline = time.monotonic() + seconds
    pending = set(range(4))
    while pending and time.monotonic() < deadline:
        for n in tuple(pending):
            try:
                guest.rpc(n, {'execute': 'guest-ping'})
                pending.remove(n)
            except Exception:
                pass
        if pending:
            time.sleep(2)
    if pending:
        raise RuntimeError('guest agent not ready: ' + repr(sorted(pending)))


def boot_guests():
    manifest()
    guest.command(['systemctl', 'start', 'libvirtd', 'chrony'])
    network = guest.command(['virsh', 'net-info', 'pgracdemo']).stdout
    if 'Active:         yes' not in network:
        guest.command(['virsh', 'net-start', 'pgracdemo'])
    guest.command(['systemctl', 'start', 'rtslib-fb-targetctl'])
    for name in guest.NAMES:
        state = guest.command(['virsh', 'domstate', name]).stdout.strip()
        if state == 'shut off':
            guest.command(['virsh', 'start', name])
        elif state != 'running':
            raise RuntimeError('unexpected VM state: ' + name + ': ' + state)
    wait_agents()


def write_private(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data.encode() if isinstance(data, str) else data)
    path.chmod(0o600)


def credentials():
    """Generate keys locally on first import, before any cluster service starts."""
    manifest()
    done = ROOT / 'credentials-ready'
    if done.exists():
        return
    for n in range(4):
        service = guest.execute(n, ['/usr/bin/systemctl', 'is-active', 'corosync'], check=False)
        pidfile = guest.execute(n, ['/usr/bin/test', '-e', '/srv/pgrac/node/data/postmaster.pid'], check=False)
        if service['rc'] not in (0, 3) or pidfile['rc'] not in (0, 1):
            raise RuntimeError('cannot establish stopped state before rekey')
        if service['rc'] == 0 or pidfile['rc'] == 0:
            raise RuntimeError('refuse rekey while cluster or database might be active')
    guest.command(['systemctl', 'stop', 'corosync-qnetd'])
    # These directories contain only this appliance\'s disposable credential databases.
    shutil.rmtree('/etc/corosync/qnetd/nssdb', ignore_errors=True)
    guest.command(['corosync-qnetd-certutil', '-i'])
    ca = Path('/etc/corosync/qnetd/nssdb/qnetd-cacert.crt').read_bytes()
    authkey = secrets.token_bytes(128)
    pacemaker_key = secrets.token_bytes(4096)
    keypath = CONFIG / 'fence_ed25519'
    for suffix in ('', '.pub'):
        Path(str(keypath) + suffix).unlink(missing_ok=True)
    guest.command(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(keypath)])
    if guest.command(['id', 'pgrac-fence'], check=False).returncode:
        guest.command(['useradd', '-m', '-s', '/bin/bash', '-G', 'libvirt', 'pgrac-fence'])
    else:
        guest.command(['usermod', '-a', '-G', 'libvirt', 'pgrac-fence'])
    # No password login, forwarding or user-supplied SSH configuration.
    sshdir = Path('/home/pgrac-fence/.ssh')
    sshdir.mkdir(mode=0o700, exist_ok=True)
    sshdir.chmod(0o700)
    public = keypath.with_suffix('.pub').read_text().strip()
    write_private(sshdir / 'authorized_keys', 'from="192.168.124.0/24",no-agent-forwarding,no-port-forwarding,no-X11-forwarding ' + public + '\n')
    guest.command(['chown', '-R', 'pgrac-fence:pgrac-fence', str(sshdir)])
    guest.command(['ssh-keygen', '-A'])
    hostkey = Path('/etc/ssh/ssh_host_ed25519_key.pub').read_text().strip()
    fencekey = keypath.read_bytes()
    wrapper = '''#!/bin/sh
exec /usr/bin/ssh -F /dev/null -o BatchMode=yes -o StrictHostKeyChecking=yes \\
 -o UserKnownHostsFile=/etc/pgrac/fence_known_hosts -o GlobalKnownHostsFile=/dev/null \\
 -o IdentitiesOnly=yes -o IdentityAgent=none -o ForwardAgent=no -o ForwardX11=no \\
 -o ConnectTimeout=10 -o ConnectionAttempts=1 "$@"
'''
    for n in range(4):
        guest.write_file(n, '/etc/corosync/authkey', authkey)
        guest.write_file(n, '/etc/pacemaker/authkey', pacemaker_key, 0o640)
        guest.execute(n, ['/usr/bin/chown', 'root:haclient', '/etc/pacemaker/authkey'])
        guest.write_file(n, '/etc/pgrac/fence_ed25519', fencekey)
        guest.write_file(n, '/etc/pgrac/fence_known_hosts', '192.168.124.1 ' + hostkey + '\n')
        guest.write_file(n, '/usr/local/libexec/pgrac-fence-ssh', wrapper, 0o755)
        guest.shell(n, 'rm -rf -- /etc/corosync/qdevice/net/nssdb; ssh-keygen -A')
        guest.write_file(n, '/etc/pgrac/qnetd-ca.crt', ca)
        guest.execute(n, ['/usr/sbin/corosync-qdevice-net-certutil', '-i', '-c', '/etc/pgrac/qnetd-ca.crt'])
    guest.execute(0, ['/usr/sbin/corosync-qdevice-net-certutil', '-r', '-n', 'pgracdemo'])
    request = CONFIG / 'qdevice.crq'
    write_private(request, guest.read_file(0, '/etc/corosync/qdevice/net/nssdb/qdevice-net-node.crq'))
    guest.command(['corosync-qnetd-certutil', '-s', '-c', str(request), '-n', 'pgracdemo'])
    cert = Path('/etc/corosync/qnetd/nssdb/cluster-pgracdemo.crt').read_bytes()
    guest.write_file(0, '/etc/pgrac/qdevice.crt', cert)
    guest.execute(0, ['/usr/sbin/corosync-qdevice-net-certutil', '-M', '-c', '/etc/pgrac/qdevice.crt'])
    p12 = guest.read_file(0, '/etc/corosync/qdevice/net/nssdb/qdevice-net-node.p12')
    for n in range(1, 4):
        guest.write_file(n, '/etc/pgrac/qdevice.p12', p12)
        guest.execute(n, ['/usr/sbin/corosync-qdevice-net-certutil', '-m', '-c', '/etc/pgrac/qdevice.p12'])
        guest.execute(n, ['/usr/bin/rm', '-f', '/etc/pgrac/qdevice.p12'])
    request.unlink()
    guest.command(['chown', '-R', 'coroqnetd:coroqnetd', '/etc/corosync/qnetd/nssdb'])
    keypath.unlink()  # Private copies are needed only by the four fencing agents.
    keypath.with_suffix('.pub').unlink()
    write_private(done, time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + '\n')


def connect_storage():
    value = manifest()
    target = value['iscsi_target']
    def connect(n):
        guest.execute(n, ['/usr/bin/systemctl', 'start', 'iscsid'])
        guest.execute(n, ['/usr/sbin/iscsiadm', '-m', 'discovery', '-t', 'sendtargets', '-p', '192.168.124.1'])
        existing = guest.execute(n, ['/usr/sbin/iscsiadm', '-m', 'session'], check=False)
        if target not in existing['stdout']:
            guest.execute(n, ['/usr/sbin/iscsiadm', '-m', 'node', '-T', target, '-p', '192.168.124.1', '--login'])
        guest.execute(n, ['/usr/bin/udevadm', 'settle'])
        for wwid in [value['data_wwid']] + value['voting_wwids']:
            guest.execute(n, ['/usr/bin/test', '-b', '/dev/disk/by-id/scsi-' + wwid])
    all_nodes(connect)


def services():
    credentials()
    connect_storage()
    guest.command(['systemctl', 'start', 'corosync-qnetd'])
    all_nodes(lambda n: guest.execute(n, ['/usr/bin/systemctl', 'start', 'corosync']))
    all_nodes(lambda n: guest.execute(n, ['/usr/bin/systemctl', 'start', 'corosync-qdevice']))
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        states = all_nodes(lambda n: guest.execute(n, ['/usr/sbin/corosync-quorumtool', '-s'], check=False))
        if all(r['rc'] == 0 and 'Nodes:            4' in r['stdout'] and
               'Total votes:      7' in r['stdout'] and 'Quorate:          Yes' in r['stdout'] for r in states):
            break
        time.sleep(2)
    else:
        raise RuntimeError('four members / seven votes not established: ' + repr(states))
    all_nodes(lambda n: guest.execute(n, ['/usr/bin/systemctl', 'start', 'pacemaker']))


def wait_storage(seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = all_nodes(lambda n: guest.execute(n, ['/usr/bin/findmnt', '-rn', '-t', 'gfs2', '/srv/pgrac/shared'], check=False))
        if all(r['rc'] == 0 for r in result):
            return
        time.sleep(2)
    raise RuntimeError('shared GFS2 is not mounted on all four guests')
