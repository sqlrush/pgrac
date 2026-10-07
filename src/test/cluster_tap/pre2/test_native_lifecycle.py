"""Only matching native ROOT, runtime and membership may prove OPEN.

Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import json
from pathlib import Path
import subprocess
import unittest

from native_lifecycle import observe


class NativeLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.layout = dict(shared_data_dir='/case/shared', wal_root='/case/wal',
                           nodes=[dict(id=n, data_dir=f'/case/node{n}') for n in range(2)])
        self.writers = [dict(node=n, thread=n+1, incarnation=70+n,
                             wal_generation=4, boot=f'{70+n:016x}') for n in range(2)]
        self.cuts = [dict(system_identifier='1234', root_digest='a'*64,
                         generation=9, root_phase='OPEN', members=[0, 1],
                         writer=w, final_checkpoint=False) for w in self.writers]
        self.runtime = dict(epoch=3, resource_x_formation=8, r4_generation=6,
                            writer=dict(self.writers[1], predecessor=dict(self.writers[1],
                                        incarnation=60, boot=f'{60:016x}', wal_generation=2)))
        # cluster_get_membership() exposes lowercase SQL labels, not C enums.
        self.members = [dict(node_id=n, declared=True, state='member',
                             presented_incarnation=70+n, last_admitted_incarnation=70+n,
                             admitted_epoch=3, removed=False) for n in range(2)]
        self.calls = []
        self.after = None

    def command(self, tool, argv, **kwargs):
        self.calls.append((tool, argv))
        self.assertEqual(tool, 'postgres')
        self.assertEqual(argv[:1], ['--pgrac-observe-writer'])
        self.assertEqual(argv[2:4], ['/case/shared', '/case/wal'])
        cut = copy.deepcopy(self.cuts[int(argv[-1])])
        if self.after and len(self.calls) > 2:
            self.after(cut)
        return subprocess.CompletedProcess(argv, 0, json.dumps(cut), '')

    def sql(self, node, query):
        self.assertEqual(node['id'], 1)
        self.assertTrue(query.lstrip().startswith('SELECT'))
        return subprocess.CompletedProcess([], 0,
            json.dumps(dict(runtime=self.runtime, membership=self.members)), '')

    def test_open_needs_native_and_runtime_and_complete_membership(self):
        value = observe(self, 'open_observation', dict(node=1))
        self.assertEqual(value['phase'], 'OPEN')
        self.assertEqual(value['writer'], self.runtime['writer'])
        self.assertEqual(value['root_digest'], self.cuts[1]['root_digest'])
        self.assertEqual(len(self.calls), 3)

    def test_root_open_without_a_runtime_proof_is_not_open(self):
        for bad in (None, 'unavailable', {}):
            with self.subTest(bad=bad):
                self.runtime = bad
                self.assertNotEqual(observe(self, 'open_observation', dict(node=1))['phase'], 'OPEN')

    def test_wrong_runtime_writer_or_zero_gate_is_not_open(self):
        original = copy.deepcopy(self.runtime)
        for field in ('epoch', 'resource_x_formation', 'r4_generation'):
            self.runtime = copy.deepcopy(original)
            self.runtime[field] = 0
            self.assertNotEqual(observe(self, 'open_observation', dict(node=1))['phase'], 'OPEN')
        self.runtime = copy.deepcopy(original)
        self.runtime['writer']['boot'] = 'old-boot'
        self.assertNotEqual(observe(self, 'open_observation', dict(node=1))['phase'], 'OPEN')

    def test_incomplete_old_or_nonmember_cannot_prove_open(self):
        original = copy.deepcopy(self.members)
        variants = [[original[1]], original + [original[1]]]
        for key, value in [('state', 'dead'), ('state', 'joining'),
                           ('state', 'MEMBER'), ('state', None),
                           ('removed', True), ('admitted_epoch', 2),
                           ('presented_incarnation', 1), ('last_admitted_incarnation', 1)]:
            rows = copy.deepcopy(original)
            rows[0][key] = value
            variants.append(rows)
        for rows in variants:
            self.members = rows
            self.assertNotEqual(observe(self, 'open_observation', dict(node=1))['phase'], 'OPEN')

    def test_root_drift_and_different_peer_cut_cannot_prove_open(self):
        self.after = lambda cut: cut.update(root_digest='b'*64)
        self.assertNotEqual(observe(self, 'open_observation', dict(node=1))['phase'], 'OPEN')
        self.after = None
        self.cuts[0]['generation'] += 1
        self.assertNotEqual(observe(self, 'open_observation', dict(node=1))['phase'], 'OPEN')

    def test_shutdown_uses_observed_writers_not_the_request(self):
        for cut in self.cuts:
            cut.update(root_phase='CLOSED', final_checkpoint=True)
        value = observe(self, 'shutdown_observation', dict(nodes=[0, 1], writers=['invented']))
        self.assertEqual(value['writers'], [dict(w, state='CLOSED') for w in self.writers])
        self.assertNotIn('all_processes_exited', value)
        self.cuts[1]['final_checkpoint'] = False
        with self.assertRaisesRegex(ValueError, 'CLOSED'):
            observe(self, 'shutdown_observation', dict(nodes=[0, 1]))

    def test_root_comes_from_the_product_and_bad_node_is_rejected(self):
        self.assertEqual(observe(self, 'root_observation', {})['generation'], 9)
        for node in (-1, 2, '1', True):
            with self.assertRaises(ValueError):
                observe(self, 'open_observation', dict(node=node))


if __name__ == '__main__':
    unittest.main()
