"""Native cohort specialization of the PRE2 black-box command adapter.

A root-owned test controller supplies live LocalQuorum handles and fresh loop
devices. No product observation is invented by this configuration adapter.
Author: SqlRush <sqlrush@gmail.com>
"""
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import time
import uuid

from blackbox import BlackBox
from cohort import request, discovery, topology
from local_quorum import qualified_status


def entry_profile(handles, formatter, votes):
    return dict(version=1, initialize=[dict(tool='initdb', argv=[
        '-D', '${root}/cohort', '-k', '-A', 'trust', '--no-locale',
        '--pgrac-initdb-cohort', '--pgrac-initdb-shared-config=${root}/request.conf'])],
        operations={}, fixture=dict(kind='local-cohort-v1', handles=str(handles),
                                    vote_formatter=str(formatter), votes=list(votes)))


class CohortEntry(BlackBox):
    def __init__(self, layout, bindir, profile, runner=subprocess.run):
        super().__init__(layout, bindir, profile, runner=self.run_product)
        self.transport = runner
        self.fixture = profile['fixture']
        self.handles = json.loads(Path(self.fixture['handles']).read_text())
        self.vendor = Path(self.handles['vendor'])
        self.addresses = [n['address'] for n in self.handles['nodes'][:self.handles['count']]]

    def validate(self):
        super().validate()
        wanted = entry_profile(self.fixture['handles'], self.fixture['vote_formatter'], self.fixture['votes'])
        if self.profile['initialize'] != wanted['initialize']:
            raise ValueError('local cohort requires the single canonical initializer')
        if self.handles['count'] not in (2, 4) or (self.layout.get('nodes')
                and len(self.layout['nodes']) != self.handles['count']):
            raise ValueError('local quorum and cohort member counts differ')
        if len(self.fixture['votes']) != 3 or len(set(self.fixture['votes'])) != 3:
            raise ValueError('three distinct fresh voting devices required')

    def command_node(self, tool, argv):
        if tool == 'initdb':
            return 0
        if tool != 'pg_ctl':
            return None
        if '-D' in argv and argv.index('-D') + 1 < len(argv):
            data = argv[argv.index('-D') + 1]
            for node in self.layout.get('nodes', []):
                if data == node['data_dir']:
                    return node['id']
        raise ValueError('pg_ctl DATA does not belong to this cohort')

    def namespace_command(self, node, argv, database=True):
        handle = self.handles['nodes'][node]
        # nsenter is privileged; the controller must still own this namespace.
        prefix = [] if os.geteuid() == 0 else ['sudo', '-n']
        check = self.transport(prefix + ['stat', '-Lc', '%i', f'/proc/{handle["pid"]}/ns/net'],
                               text=True, capture_output=True, timeout=min(5, self.remaining()))
        if check.returncode or check.stdout.strip() != str(handle['net_inode']):
            raise RuntimeError('local quorum namespace owner changed')
        command = prefix + ['nsenter', '-t', str(handle['pid']), '-n', '-m', '-i']
        if database:
            command += ['setpriv', '--reuid', str(self.handles['uid']), '--regid',
                        str(self.handles['gid']), '--init-groups']
        return command + ['env', 'LD_LIBRARY_PATH='+str(self.vendor/'usr/lib/aarch64-linux-gnu')+':'+str(self.bindir.parent/'lib'),
                          *argv]

    def run_product(self, argv, **kwargs):
        tool = Path(argv[0]).name
        node = self.command_node(tool, argv[1:])
        env = kwargs.setdefault('env', dict(os.environ))
        env['PGUSER'] = pwd.getpwuid(self.handles['uid']).pw_name
        env['LD_LIBRARY_PATH'] = str(self.vendor/'usr/lib/aarch64-linux-gnu')+':'+str(self.bindir.parent/'lib')
        if node is not None:
            argv = self.namespace_command(node, argv)
        return self.transport(argv, **kwargs)

    def verify_fixture(self):
        for n in range(self.handles['count']):
            observations = []
            for tool in ('corosync-quorumtool', 'corosync-qdevice-tool'):
                argv = self.namespace_command(n, [str(self.vendor/'usr/sbin'/tool), '-s'], database=False)
                result = self.transport(argv, text=True, capture_output=True,
                                        timeout=min(5, self.remaining()))
                self.record(dict(prerequisite=tool, node=n, rc=result.returncode,
                                 stdout=result.stdout, stderr=result.stderr))
                if result.returncode:
                    raise RuntimeError('cannot observe current storage quorum')
                observations.append(result.stdout)
            if not qualified_status(*observations, self.handles['count']):
                raise RuntimeError('storage qdevice quorum is not qualified')
        for n, device in enumerate(self.fixture['votes']):
            result = self.transport([self.fixture['vote_formatter'], '--attest', device, str(n)],
                text=True, capture_output=True, timeout=min(5, self.remaining()))
            self.record(dict(prerequisite='fresh native voting device', device=device,
                             rc=result.returncode, stderr=result.stderr))
            if result.returncode:
                raise RuntimeError('fresh voting device native attestation refused')

    def prepare_initialize(self):
        if any(os.path.lexists(self.root/name) for name in ('cohort', 'request.conf')):
            raise ValueError('cohort creation output already exists')
        self.verify_fixture()
        for node in self.layout['nodes']:
            socket = Path(node['host'])
            if not socket.is_absolute():
                raise ValueError('local cohort requires a Unix socket directory')
            socket.mkdir(parents=True, exist_ok=True)
            if os.geteuid() == 0:
                os.chown(socket, self.handles['uid'], self.handles['gid'])
        text = request(self.layout, self.handles['name'], self.addresses,
                       (int(time.time()) << 32) | 1, uuid.uuid4().hex, uuid.uuid4().hex)
        path = self.root/'request.conf'
        with path.open('x') as output:
            output.write(text)
        path.chmod(0o600)
        if os.geteuid() == 0:
            os.chown(path, self.handles['uid'], self.handles['gid'])

    def finish_initialize(self):
        # The native creator owns every persistent byte; relocation preserves
        # symlinks. It does not merge with an existing DATA or rewrite authority.
        for node in self.layout['nodes']:
            source = self.root/'cohort'/f'node_{node["id"]}'
            target = Path(node['data_dir'])
            if source != target:
                if target.exists():
                    target.rmdir()  # only the previously checked empty directory
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source), str(target))
            (target/'pgrac.conf').write_text(topology(self.handles['name'], self.layout['nodes'], self.addresses))
            with (target/'postgresql.conf').open('a') as output:
                output.write(discovery(self.layout, node, self.fixture['votes']))
        # Keep regular TAP disk handles meaningful without copying authority
        # bytes or replacing the controller's disposable loop devices.
        for alias, device in zip(self.layout.get('voting_disks', []), self.fixture['votes']):
            Path(alias).symlink_to(device)
