"""Configuration and prerequisite negatives, never counted as live acceptance."""
from pathlib import Path
import unittest

from cohort import request, quote
from local_quorum import qualified_status


class CohortTest(unittest.TestCase):
    def layout(self, count=4):
        return dict(root='/new/test', shared_data_dir='/new/test/shared', wal_root='/new/test/wal',
                    nodes=[dict(id=n, port=19000+n, host='/new/test/sockets') for n in range(count)])

    def render(self, layout):
        return request(layout, 'p3b_test', [f'10.0.0.{n+1}' for n in range(len(layout['nodes']))],
                       '7584383251700000001', 'a'*32, 'b'*32)

    def test_creation_identity_and_shared_quorum_mapping(self):
        for count in (2, 4):
            text = self.render(self.layout(count))
            self.assertIn(f'@configured_0={(1<<count)-1:016x}\n', text)
            self.assertIn("common.cluster.storage_quorum_cluster='p3b_test'\n", text)
            self.assertIn("common.cluster.storage_quorum_nodes='"+
                          ','.join(f'{n}:{n+1}' for n in range(count))+"'\n", text)
            self.assertNotIn('node000.cluster.storage_quorum', text)
            self.assertIn("common.cluster.xid_striping='on'", text)
            self.assertNotIn('timeout=120', text)

    def test_missing_or_duplicate_member_refused(self):
        for ids in ((0,), (0, 0), (0, 2), (0, 1, 2, 4)):
            layout = self.layout(len(ids))
            for node, n in zip(layout['nodes'], ids):
                node['id'] = n
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                self.render(layout)

    def test_canonical_values_cannot_inject_another_assignment(self):
        for value in ("hello\ncommon.cluster.xid_striping='off'", 'x\r', 'x\0'):
            with self.assertRaises(ValueError):
                quote(value)
        self.assertEqual(quote("a'b"), "'a''b'")

    def test_test_settings_are_in_original_common_or_instance_input(self):
        layout = self.layout(2)
        layout['extra_conf'] = ['autovacuum = off', 'cluster.read_scache = on',
                                "shared_buffers = '32MB'", 'cluster.xnode_profile = on']
        text = self.render(layout)
        self.assertIn("common.autovacuum='off'\n", text)
        self.assertIn("common.cluster.read_scache='on'\n", text)
        self.assertIn("common.cluster.xnode_profile='on'\n", text)
        self.assertIn("node001.shared_buffers='32MB'\n", text)
        for bad in ("cluster.storage_quorum_nodes='0:10,1:11'", 'port=22222',
                    'autovacuum=off\ncluster.enabled=off'):
            layout['extra_conf'] = [bad]
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.render(layout)

    def test_connected_tls_profile_requires_actual_qdevice_votes(self):
        device = 'Algorithm:\t\tLMS\nState:\t\t\tConnected\n'
        good = 'Quorate:          Yes\n  1  1 A,V,NMW (local)\n  2  1 A,V,NMW \n'
        self.assertTrue(qualified_status(good, device, 2))
        self.assertFalse(qualified_status(good.replace('A,V,NMW', 'NA,NV,NMW'), device, 2))
        self.assertFalse(qualified_status(good, device.replace('Connected', 'Waiting'), 2))
        self.assertFalse(qualified_status(good, device.replace('LMS', 'FFSplit'), 2))
        self.assertFalse(qualified_status(good, device, 4))


if __name__ == '__main__':
    unittest.main()
