"""Native cohort adapter tests; command mocks are not startup acceptance."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from cohort_entry import CohortEntry, entry_profile


class CohortEntryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.layout = dict(root=str(self.root/'case'), name='cohort',
                           shared_data_dir=str(self.root/'case/shared'),
                           wal_root=str(self.root/'case/wal'), extra_conf=['autovacuum = off'],
                           voting_disks=[str(self.root/f'case/vote{n}') for n in range(3)],
                           nodes=[dict(id=n, data_dir=str(self.root/f'pg{n}/pgdata'),
                                       host=str(self.root/'sockets'), port=22000+n,
                                       ic_port=22100+n, data_port=22200+n*2) for n in range(2)])
        handles = self.root/'handles.json'
        handles.write_text(json.dumps(dict(name='p3b_unit', count=2, uid=os.getuid(), gid=os.getgid(),
            vendor=str(self.root/'vendor'), nodes=[dict(pid=100+n, net_inode=10+n,
            address=f'10.231.109.{n+1}') for n in range(3)])))
        self.profile = entry_profile(handles, self.root/'formatter', ['/dev/loop90', '/dev/loop91', '/dev/loop92'])
        self.calls = []

    def runner(self, argv, **kwargs):
        self.calls.append(argv)
        if Path(argv[0]).name == 'initdb':
            parent = Path(argv[argv.index('-D')+1])
            for n in range(2):
                data = parent/f'node_{n}'
                (data/'global').mkdir(parents=True)
                # Opaque unit payload: adapter must move, never interpret it.
                (data/'global/pg_control').write_bytes(b'opaque unit control')
                (data/'postgresql.conf').write_text('# initdb output\n')
                (data/'pg_wal').symlink_to(self.root/'case/wal'/f'thread{n}')
        return subprocess.CompletedProcess(argv, 0, '', '')

    def driver(self):
        return CohortEntry(self.layout, self.root/'bin', self.profile, runner=self.runner)

    def test_native_cohort_preserves_creator_bytes_and_uses_exact_node(self):
        driver = self.driver()
        with patch.object(driver, 'verify_fixture'), patch.object(driver, 'namespace_command',
                side_effect=lambda node, argv: argv):
            driver.fresh_init()
        self.assertEqual(len([a for a in self.calls if Path(a[0]).name == 'initdb']), 1)
        for node in self.layout['nodes']:
            data = Path(node['data_dir'])
            self.assertEqual((data/'global/pg_control').read_bytes(), b'opaque unit control')
            self.assertTrue((data/'pg_wal').is_symlink())
            self.assertIn('cluster.voting_disks=', (data/'postgresql.conf').read_text())
            self.assertIn('autovacuum = off', (data/'postgresql.conf').read_text())
            self.assertEqual(driver.command_node('pg_ctl', ['start', '-D', node['data_dir']]), node['id'])
        self.assertEqual(driver.command_node('initdb', ['--pgrac-initdb-cohort']), 0)
        with self.assertRaisesRegex(ValueError, 'DATA'):
            driver.command_node('pg_ctl', ['-D', '/unowned/pgdata', 'start'])

    def test_existing_creator_output_never_overwritten(self):
        parent = self.root/'case/cohort'
        parent.mkdir(parents=True)
        (parent/'sentinel').write_text('keep')
        driver = self.driver()
        with self.assertRaisesRegex(ValueError, 'cohort'):
            driver.fresh_init()
        self.assertEqual((parent/'sentinel').read_text(), 'keep')
        self.assertEqual(self.calls, [])

    def test_initializer_does_not_claim_missing_observation_capabilities(self):
        driver = self.driver()
        with patch.object(driver, 'namespace_command', side_effect=lambda node, argv: argv):
            caps = driver.describe()['capabilities']
        self.assertIn('fresh_init', caps)
        self.assertNotIn('root_observation', caps)
        self.assertNotIn('open_observation', caps)
        self.assertNotIn('shutdown_observation', caps)


if __name__ == '__main__':
    unittest.main()
