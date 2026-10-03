#!/usr/bin/env python3
"""Disposable Linux namespaces with real Corosync and TLS net/lms qdevice.

Run only in a development VM. Requires root for private namespaces and loop
devices. This is not a fencing provider or a deployment certification.
Distribution packages are unpacked in --vendor; no system service is started.
All logs and storage are retained. stop() removes only this run's live handles.
Author: SqlRush <sqlrush@gmail.com>
"""
import hashlib
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


def qualified_status(quorum, device, count):
    """A Corosync-only majority is insufficient for the TLS qdevice fixture."""
    return bool(re.search(r'^Quorate:\s+Yes$', quorum, re.M)
                and len(re.findall(r'\sA,V,NMW\s', quorum)) == count
                and re.search(r'^\s*State:\s+Connected\s*$', device, re.M)
                and re.search(r'^\s*Algorithm:\s+LMS\s*$', device, re.M))


class LocalQuorum:
    def __init__(self, root, vendor, count, uid, gid):
        if count not in (2, 4) or os.geteuid() != 0:
            raise ValueError('local quorum requires Linux root and two or four nodes')
        self.root, self.vendor = Path(root).resolve(), Path(vendor).resolve(strict=True)
        self.count, self.uid, self.gid = count, uid, gid
        self.tag = 'bq' + hashlib.sha256(str(self.root).encode()).hexdigest()[:8]
        self.name = 'p3b_' + self.tag
        self.nodes, self.processes, self.loops = [], [], []
        self.bridge_created = False
        self.owned_root = False
        self.lock = None
        self.env = dict(os.environ, LD_LIBRARY_PATH=str(self.vendor/'usr/lib/aarch64-linux-gnu'),
                        PATH=f'{self.vendor}/usr/sbin:{self.vendor}/usr/bin:' + os.environ['PATH'])

    def run(self, argv, check=True, **kwargs):
        result = subprocess.run([str(a) for a in argv], text=True, capture_output=True,
                                timeout=kwargs.pop('timeout', 30), env=self.env, **kwargs)
        with (self.root/'commands.jsonl').open('a') as log:
            log.write(json.dumps(dict(argv=[str(a) for a in argv], rc=result.returncode,
                                      stdout=result.stdout, stderr=result.stderr)) + '\n')
        if check and result.returncode:
            raise RuntimeError(f'{argv[0]} rc={result.returncode}: {result.stderr or result.stdout}')
        return result

    def ns(self, index, argv, *, database=False):
        node = self.nodes[index]
        if Path(f'/proc/{node["pid"]}/ns/net').stat().st_ino != node['net_inode']:
            raise RuntimeError('namespace owner changed')
        command = ['nsenter', '-t', str(node['pid']), '-n', '-m', '-i']
        if database:
            command += ['setpriv', '--reuid', str(self.uid), '--regid', str(self.gid), '--init-groups']
        return command + ['env', 'LD_LIBRARY_PATH='+self.env['LD_LIBRARY_PATH'],
                          'PATH='+self.env['PATH'], *map(str, argv)]

    def spawn(self, argv, logfile):
        with Path(logfile).open('a') as log:
            process = subprocess.Popen(list(map(str, argv)), stdout=log, stderr=log,
                                       env=self.env, start_new_session=True)
        self.processes.append(process)
        return process

    def start(self):
        self.root.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.owned_root = True
        os.chown(self.root, self.uid, self.gid)
        self.lock = open('/run/lock/pgrac-local-quorum.lock', 'a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.run(['ip', 'link', 'add', self.tag, 'type', 'bridge'])
        self.bridge_created = True
        self.run(['ip', 'addr', 'add', '10.231.109.254/24', 'dev', self.tag])
        self.run(['ip', 'link', 'set', self.tag, 'up'])
        # Empty mount points in this development VM, never system configuration.
        for path in ('/etc/corosync', '/var/lib/corosync', '/usr/lib/aarch64-linux-gnu/kronosnet'):
            Path(path).mkdir(exist_ok=True)
        for index in range(self.count + 1):
            node = self.root/f'ns{index}'
            node.mkdir()
            for path in ('etc/qnetd', 'etc/qdevice/net', 'run', 'state', 'shm'):
                (node/path).mkdir(parents=True)
            self.spawn(['unshare', '--net', '--mount', '--ipc', '--fork',
                        sys.executable, __file__, 'hold', str(node), str(self.vendor)], node/'hold.log')
            deadline = time.monotonic() + 10
            while not (node/'pid').exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError('namespace setup failed: '+str(node/'hold.log'))
                time.sleep(.05)
            pid = int((node/'pid').read_text())
            entry = dict(pid=pid, net_inode=Path(f'/proc/{pid}/ns/net').stat().st_ino,
                         address=f'10.231.109.{index+1}', root=str(node))
            self.nodes.append(entry)
            link, peer = f'{self.tag}{index}', f'{self.tag}p{index}'
            self.run(['ip', 'link', 'add', link, 'type', 'veth', 'peer', 'name', peer])
            self.run(['ip', 'link', 'set', peer, 'netns', str(pid)])
            self.run(['ip', 'link', 'set', link, 'master', self.tag])
            self.run(['ip', 'link', 'set', link, 'up'])
            self.run(self.ns(index, ['ip', 'addr', 'add', entry['address']+'/24', 'dev', peer]))
            self.run(self.ns(index, ['ip', 'link', 'set', peer, 'up']))
            self.run(self.ns(index, ['ip', 'link', 'set', 'lo', 'up']))
        witness = self.count
        self.run(self.ns(witness, [self.vendor/'usr/bin/corosync-qnetd-certutil', '-i']))
        ca = Path(self.nodes[witness]['root'])/'etc/qnetd/nssdb/qnetd-cacert.crt'
        for n in range(self.count):
            self.run(self.ns(n, [self.vendor/'usr/sbin/corosync-qdevice-net-certutil', '-i', '-c', ca]))
        self.run(self.ns(0, [self.vendor/'usr/sbin/corosync-qdevice-net-certutil', '-r', '-n', self.name]))
        request = Path(self.nodes[0]['root'])/'etc/qdevice/net/nssdb/qdevice-net-node.crq'
        self.run(self.ns(witness, [self.vendor/'usr/bin/corosync-qnetd-certutil',
                                   '-s', '-c', request, '-n', self.name]))
        signed = Path(self.nodes[witness]['root'])/f'etc/qnetd/nssdb/cluster-{self.name}.crt'
        self.run(self.ns(0, [self.vendor/'usr/sbin/corosync-qdevice-net-certutil', '-M', '-c', signed]))
        bundle = Path(self.nodes[0]['root'])/'etc/qdevice/net/nssdb/qdevice-net-node.p12'
        for n in range(1, self.count):
            self.run(self.ns(n, [self.vendor/'usr/sbin/corosync-qdevice-net-certutil', '-m', '-c', bundle]))
        self.spawn(self.ns(witness, [self.vendor/'usr/bin/corosync-qnetd', '-f', '-s', 'req',
                                     '-l', self.nodes[witness]['address']]), self.root/'qnetd.log')
        for n in range(self.count):
            path = Path(self.nodes[n]['root'])/'etc/corosync.conf'
            path.write_text(self.configuration())
            self.spawn(self.ns(n, [self.vendor/'usr/sbin/corosync', '-f', '-c', path]),
                       self.root/f'corosync{n}.log')
        deadline = time.monotonic() + 60
        while True:
            ready = [self.run(self.ns(n, [self.vendor/'usr/sbin/corosync-cmapctl',
                                         'runtime.votequorum.this_node_id']), check=False).returncode == 0
                     for n in range(self.count)]
            if all(ready):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('Corosync did not become ready')
            time.sleep(.2)
        for n in range(self.count):
            self.spawn(self.ns(n, [self.vendor/'usr/sbin/corosync-qdevice', '-f']),
                       self.root/f'qdevice{n}.log')
        self.wait_quorum()
        self.save()

    def configuration(self):
        nodes = '\n'.join(f'  node {{\n    ring0_addr: {self.nodes[n]["address"]}\n'
                          f'    nodeid: {n+1}\n  }}' for n in range(self.count))
        return (f'totem {{\n version: 2\n cluster_name: {self.name}\n transport: knet\n}}\n'
                f'nodelist {{\n{nodes}\n}}\nquorum {{\n provider: corosync_votequorum\n'
                f' device {{\n  model: net\n  net {{\n   host: {self.nodes[self.count]["address"]}\n'
                '   algorithm: lms\n   tls: required\n  }\n }\n}\n'
                'logging {\n to_stderr: yes\n to_syslog: no\n}\n'
                f'uidgid {{\n uid: {self.uid}\n gid: {self.gid}\n}}\n')

    def wait_quorum(self):
        deadline = time.monotonic() + 60
        while True:
            ready = []
            for n in range(self.count):
                result = self.run(self.ns(n, [self.vendor/'usr/sbin/corosync-quorumtool', '-s']), check=False)
                (self.root/f'quorum{n}.txt').write_text(result.stdout+result.stderr)
                device = self.run(self.ns(n, [self.vendor/'usr/sbin/corosync-qdevice-tool', '-s']), check=False)
                (self.root/f'qdevice{n}.txt').write_text(device.stdout+device.stderr)
                ready.append(result.returncode == 0 and device.returncode == 0
                             and qualified_status(result.stdout, device.stdout, self.count))
            if all(ready):
                for n in range(self.count):
                    result = self.run(self.ns(n, [self.vendor/'usr/sbin/corosync-cmapctl']))
                    (self.root/f'cmap{n}.txt').write_text(result.stdout)
                return
            if time.monotonic() >= deadline:
                raise RuntimeError('real TLS qdevice quorum did not become ready')
            time.sleep(.25)

    def attach_vote(self, image):
        # Caller just created and verified the image using the native formatter.
        # losetup selects an unused handle; never operate on a supplied old device.
        loop = self.run(['losetup', '--find', '--show', '--direct-io=on', str(image)]).stdout.strip()
        self.loops.append(loop)
        os.chown(loop, self.uid, self.gid)
        os.chmod(loop, 0o600)
        self.save()
        return loop

    def save(self):
        (self.root/'handles.json').write_text(json.dumps(dict(
            bridge=self.tag, name=self.name, nodes=self.nodes, loops=self.loops,
            vendor=str(self.vendor), count=self.count, uid=self.uid, gid=self.gid,
            processes=[p.pid for p in self.processes]), indent=2)+'\n')

    def stop(self):
        if not self.owned_root:
            return
        # Databases must have been stopped by the caller before these services.
        errors = []
        for process in reversed(self.processes):
            try:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
            except ProcessLookupError:
                process.wait(timeout=5)
            except Exception as error:
                errors.append(f'process {process.pid}: {error}')
        for loop in reversed(self.loops):
            try:
                self.run(['losetup', '-d', loop])
            except Exception as error:
                errors.append(f'{loop}: {error}')
        if self.bridge_created:
            try:
                self.run(['ip', 'link', 'del', self.tag])
            except Exception as error:
                errors.append(f'{self.tag}: {error}')
        self.save()
        if self.lock:
            self.lock.close()
        if errors:
            raise RuntimeError('; '.join(errors))


def hold(directory, vendor):
    def run(*args):
        subprocess.run(args, check=True)
    run('mount', '--make-rprivate', '/')
    for child, target in [('etc', '/etc/corosync'), ('run', '/run'),
                          ('state', '/var/lib/corosync'), ('shm', '/dev/shm')]:
        run('mount', '--bind', str(directory/child), target)
    os.chmod('/dev/shm', 0o1777)
    run('mount', '--bind', str(vendor/'usr/lib/aarch64-linux-gnu/kronosnet'),
        '/usr/lib/aarch64-linux-gnu/kronosnet')
    (directory/'pid').write_text(str(os.getpid()))
    signal.signal(signal.SIGTERM, lambda *args: sys.exit(0))
    while True:
        signal.pause()


if __name__ == '__main__':
    if len(sys.argv) == 4 and sys.argv[1] == 'hold':
        hold(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        raise SystemExit('Use LocalQuorum from a disposable test controller')
