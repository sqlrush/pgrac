"""Cold example-data restore checks; no database or VM is started."""
from pathlib import Path
import hashlib
import io
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import appliance


class FactoryRestore(unittest.TestCase):
    def archive(self, parent, unsafe=False):
        path = parent / 'factory.tar.gz'
        with tarfile.open(path, 'w:gz') as archive:
            files = {f'demo/node{n}/data/PG_VERSION': b'16\n' for n in range(4)}
            files.update({f'demo/vote{n}.img': b'factory vote' for n in range(3)})
            files.update({'demo/data/factory': b'clean', 'demo/wal/factory': b'clean'})
            if unsafe:
                files['../outside'] = b'must not be written'
            for name, data in files.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def test_bad_hash_leaves_existing_data_untouched(self):
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            root = parent / 'demo'
            root.mkdir()
            (root / 'existing').write_text('keep')
            archive, _ = self.archive(parent)
            self.assertTrue(hasattr(appliance, 'replace_factory_data'), 'cold restore entry is missing')
            with self.assertRaises(RuntimeError):
                appliance.replace_factory_data(archive, '0' * 64, root)
            self.assertEqual((root / 'existing').read_text(), 'keep')

    def test_unsafe_archive_leaves_existing_data_untouched(self):
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            root = parent / 'demo'
            root.mkdir()
            (root / 'existing').write_text('keep')
            archive, sha = self.archive(parent, unsafe=True)
            self.assertTrue(hasattr(appliance, 'replace_factory_data'), 'cold restore entry is missing')
            with self.assertRaises(RuntimeError):
                appliance.replace_factory_data(archive, sha, root)
            self.assertEqual((root / 'existing').read_text(), 'keep')
            self.assertFalse((parent / 'outside').exists())

    def test_restore_replaces_entire_example_cohort(self):
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            root = parent / 'demo'
            root.mkdir()
            (root / 'old-undo').write_text('old')
            archive, sha = self.archive(parent)
            self.assertTrue(hasattr(appliance, 'replace_factory_data'), 'cold restore entry is missing')
            appliance.replace_factory_data(archive, sha, root)
            self.assertFalse((root / 'old-undo').exists())
            self.assertEqual((root / 'data/factory').read_bytes(), b'clean')
            self.assertEqual((root / 'wal/factory').read_bytes(), b'clean')
            self.assertTrue(all((root / f'node{n}/data/PG_VERSION').is_file() for n in range(4)))

    def test_pidfile_refuses_cold_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            root = parent / 'demo'
            (root / 'node0/data').mkdir(parents=True)
            (root / 'node0/data/postmaster.pid').write_text('123\n')
            archive, sha = self.archive(parent)
            self.assertTrue(hasattr(appliance, 'replace_factory_data'), 'cold restore entry is missing')
            with self.assertRaises(RuntimeError):
                appliance.replace_factory_data(archive, sha, root)
            self.assertTrue((root / 'node0/data/postmaster.pid').exists())

    def test_snapshot_cannot_borrow_old_data_directory_via_link(self):
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            root = parent / 'demo'
            (root / 'data').mkdir(parents=True)
            (root / 'wal').mkdir()
            (root / 'data/keep').write_text('original')
            archive = parent / 'linked-factory.tar.gz'
            with tarfile.open(archive, 'w:gz') as source:
                names = [f'demo/node{n}/data/PG_VERSION' for n in range(4)]
                names += [f'demo/vote{n}.img' for n in range(3)]
                for name in names:
                    item = tarfile.TarInfo(name)
                    item.size = 3
                    source.addfile(item, io.BytesIO(b'16\n'))
                for name in ('data', 'wal'):
                    item = tarfile.TarInfo('demo/' + name)
                    item.type = tarfile.SYMTYPE
                    item.linkname = str(root / name)
                    source.addfile(item)
            sha = hashlib.sha256(archive.read_bytes()).hexdigest()
            with self.assertRaises(RuntimeError):
                appliance.replace_factory_data(archive, sha, root)
            self.assertEqual((root / 'data/keep').read_text(), 'original')

    def test_each_sweep_point_uses_fresh_data(self):
        calls = []
        def fresh(clients, seconds):
            calls.append((clients, seconds))
            return {'valid': True, 'clients_per_instance': clients, 'common_window_tps': 1}
        with patch.object(appliance, 'fresh_bench', side_effect=fresh, create=True), \
             patch.object(appliance, 'bench', side_effect=RuntimeError('bypassed cold reset')):
            result = appliance.sweep(60)
        self.assertEqual(calls, [(16, 60), (32, 60), (48, 60), (64, 60)])
        self.assertEqual(result['status'], 'SWEEP_PASS')

    def test_exited_clients_after_first_error_keep_all_raw_results(self):
        class Client:
            def __init__(self):
                self.returncode = None
                self.calls = 0
            def poll(self):
                self.calls += 1
                if self.calls > 1:
                    self.returncode = 1
                return self.returncode
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            logs = [parent / f'node{n}.log' for n in range(4)]
            for path in logs:
                path.write_text('')
            def spawn(*args, **kwargs):
                logs[0].write_text('ERROR: rejected operation\n')
                return Client()
            with patch.object(appliance, 'OUT', parent), \
                 patch.object(appliance, 'logfile', side_effect=lambda n: logs[n]), \
                 patch.object(appliance, 'sql', return_value={'stdout': 'on'}), \
                 patch.object(appliance, 'node_command', return_value=['unused']), \
                 patch.object(appliance.subprocess, 'Popen', side_effect=spawn):
                result = appliance.bench(16, 60)
            self.assertFalse(result['valid'])
            self.assertEqual(len(result['nodes']), 4)
            self.assertTrue(all(n['ended'] >= n['started'] for n in result['nodes']))


if __name__ == '__main__':
    unittest.main()
