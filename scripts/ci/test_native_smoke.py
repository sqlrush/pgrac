#!/usr/bin/env python3
"""Native smoke setup must preserve the complete release test selection.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import mvp_ci

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/test/cluster_tap/pre2'))
import local_quorum


class NativeSmokeTest(unittest.TestCase):
    def test_platform_partition_preserves_every_original_case(self):
        policy = mvp_ci.load_policy()
        local = mvp_ci.smoke_selection(policy, native=False)
        native = mvp_ci.smoke_selection(policy, native=True)
        self.assertEqual(native, ['t/031_undo_publication_refusal.pl'])
        self.assertEqual(len(local), 5)
        self.assertFalse(set(local) & set(native))
        self.assertEqual(set(local) | set(native), set(policy['smoke_tests']))

    def test_native_selection_cannot_add_remove_or_repeat_a_case(self):
        for invalid in ([], ['t/001_unselected.pl'],
                        ['t/031_undo_publication_refusal.pl'] * 2):
            policy = dict(mvp_ci.load_policy(), native_smoke_tests=invalid)
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                mvp_ci.smoke_selection(policy, native=True)

    def test_vendor_library_path_matches_host_and_never_guesses_unknown_cpu(self):
        for machine, triplet in [('aarch64', 'aarch64-linux-gnu'),
                                 ('arm64', 'aarch64-linux-gnu'),
                                 ('x86_64', 'x86_64-linux-gnu')]:
            with patch('platform.machine', return_value=machine):
                self.assertEqual(local_quorum.native_library_dir(Path('/vendor')),
                                 Path('/vendor/usr/lib') / triplet)
        with patch('platform.machine', return_value='unqualified'):
            with self.assertRaises(ValueError):
                local_quorum.native_library_dir(Path('/vendor'))

    def test_tap_requires_success_complete_plan_and_no_skip(self):
        spec = importlib.util.spec_from_file_location('native_smoke',
            ROOT / 'scripts/ci/run_native_smoke.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertTrue(module.tap_result('ok 1 - real\n1..1\n', 0)['passed'])
        for text, rc in [('ok 1 - real\n', 0), ('ok 1 - real\n1..2\n', 0),
                         ('not ok 1 - real\n1..1\n', 0),
                         ('ok 1 - fake # SKIP missing environment\n1..1\n', 0),
                         ('ok 1 - real\n1..1\n', 1), ('1..0 # SKIP\n', 0)]:
            with self.subTest(text=text, rc=rc):
                self.assertFalse(module.tap_result(text, rc)['passed'])

    def test_cleanup_refuses_another_cohort_path_identity_or_namespace(self):
        spec = importlib.util.spec_from_file_location('native_smoke_cleanup',
            ROOT / 'scripts/ci/run_native_smoke.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case = root/'owned'
            (case/'data').mkdir(parents=True)
            proc = root/'proc'
            handles, nodes = [], []
            for n in range(4):
                data = case/'data'/f'node{n}'
                data.mkdir()
                (data/'postmaster.pid').write_text(f'{200+n}\n{data}\n')
                net = proc/str(200+n)/'ns/net'
                net.parent.mkdir(parents=True)
                net.touch()
                handles.append(dict(net_inode=net.stat().st_ino))
                nodes.append(dict(id=n, data_dir=str(data)))
            layout = case/'data/blackbox-layout.json'
            layout.write_text(json.dumps(dict(nodes=nodes)))
            self.assertEqual(module.cleanup_targets(layout, case, handles, os.getuid(), proc), nodes)
            original = layout.read_text()
            for fault in ('outside', 'node', 'namespace', 'uid'):
                changed = json.loads(original)
                expected = [dict(h) for h in handles]
                uid = os.getuid()
                if fault == 'outside':
                    outside = root/'other-cohort'
                    outside.mkdir()
                    changed['nodes'][0]['data_dir'] = str(outside)
                elif fault == 'node':
                    changed['nodes'][0]['id'] = 2
                elif fault == 'namespace':
                    expected[0]['net_inode'] += 1
                else:
                    uid += 1
                layout.write_text(json.dumps(changed))
                with self.subTest(fault=fault), self.assertRaises(ValueError):
                    module.cleanup_targets(layout, case, expected, uid, proc)
            layout.write_text(original)
            # A symlink under the run root cannot make outside DATA owned.
            link = case/'data/aliased'
            link.symlink_to(root/'other-cohort', target_is_directory=True)
            changed = json.loads(original)
            changed['nodes'][0]['data_dir'] = str(link)
            layout.write_text(json.dumps(changed))
            with self.assertRaises(ValueError):
                module.cleanup_targets(layout, case, handles, os.getuid(), proc)


if __name__ == '__main__':
    unittest.main()
