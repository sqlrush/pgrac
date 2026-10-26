"""Eligibility tests for passive startup samples, not a native startup verdict."""
import hashlib
import importlib.util
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch


class StartupWaitProbeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.exe = self.root / 'postgres'
        self.pid = 1234
        self.birth = 91
        self.bias = 0x10000
        self.proc = self.root / str(self.pid)
        self.proc.mkdir()
        (self.proc / 'exe').symlink_to(self.exe)
        (self.proc / 'root').symlink_to('/')
        self.write_elf()
        self.set_birth(self.birth)
        with (self.proc / 'mem').open('wb') as f:
            f.truncate(self.bias + 0x5000)
        self.put(0x500, '<i', self.pid)
        self.put(0x508, '<Q', self.bias + 0x3000)
        self.put(0x510, '<Q', self.bias + 0x4000)
        self.put(0x4000, '<I', 0x16000007)
        self.maps = [self.mapping(self.exe, self.bias, self.bias + 0x1000, 'r-xp'),
                     '%x-%x rw-p 00000000 00:00 0\n' %
                     (self.bias + 0x3000, self.bias + 0x5000)]
        self.set_maps()

    def module(self):
        spec = importlib.util.find_spec('startup_wait_probe')
        self.assertIsNotNone(spec, 'passive startup wait probe is missing')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def write_elf(self):
        data = bytearray(0xc00)
        ident = b'\x7fELF\x02\x01\x01' + bytes(9)
        struct.pack_into('<16sHHIQQQIHHHHHH', data, 0, ident, 3, 183, 1,
                         0, 64, 0xa00, 0, 64, 56, 1, 64, 3, 0)
        struct.pack_into('<IIQQQQQQ', data, 64, 1, 7, 0, 0, 0,
                         len(data), 0x1000, 0x1000)
        strings = b'\0MyProcPid\0MyProc\0my_wait_event_info\0'
        data[0x900:0x900 + len(strings)] = strings
        for i, (name, value, size) in enumerate((('MyProcPid', 0x500, 4),
                                                ('MyProc', 0x508, 8),
                                                ('my_wait_event_info', 0x510, 8)), 1):
            struct.pack_into('<IBBHQQ', data, 0x800 + i * 24,
                             strings.index(b'\0' + name.encode() + b'\0') + 1, 0x11, 0, 1, value, size)
        struct.pack_into('<IIQQQQIIQQ', data, 0xa40, 0, 3, 0, 0,
                         0x900, len(strings), 0, 0, 1, 0)
        struct.pack_into('<IIQQQQIIQQ', data, 0xa80, 0, 2, 0, 0,
                         0x800, 96, 1, 0, 8, 24)
        self.exe.write_bytes(data)
        self.sha = hashlib.sha256(data).hexdigest()

    def set_birth(self, birth):
        tail = ['S'] + ['0'] * 18 + [str(birth)]
        (self.proc / 'stat').write_text('%d (postgres (startup)) %s\n' %
                                      (self.pid, ' '.join(tail)))

    def put(self, offset, fmt, value):
        with (self.proc / 'mem').open('r+b') as f:
            f.seek(self.bias + offset)
            f.write(struct.pack(fmt, value))

    def mapping(self, path, start, end, mode):
        st = path.stat()
        return '%x-%x %s 00000000 %x:%x %d %s\n' % (
            start, end, mode, os.major(st.st_dev), os.minor(st.st_dev),
            st.st_ino, path)

    def set_maps(self):
        (self.proc / 'maps').write_text(''.join(self.maps))

    def probe(self, module):
        return module.StartupWaitProbe(self.exe, self.sha, os.getuid(), self.root)

    def test_original_wait_is_read_without_writing_sampled_files(self):
        m = self.module()
        before = {p.name: p.read_bytes() for p in self.proc.iterdir() if p.is_file()}
        row = self.probe(m).sample(self.pid, self.birth)
        self.assertEqual(row['status'], 'OBSERVED')
        self.assertEqual(row['wait_event_info'], 0x16000007)
        self.assertFalse(row['atomic_graph'])
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.proc.iterdir()
                                  if p.is_file()})

    def test_wrong_frozen_image_hash_is_rejected(self):
        m = self.module()
        with self.assertRaises(ValueError):
            m.StartupWaitProbe(self.exe, '0' * 64, os.getuid(), self.root)

    def test_pid_reuse_is_unknown_instead_of_wait_zero(self):
        m = self.module()
        self.set_birth(self.birth + 1)
        row = self.probe(m).sample(self.pid, self.birth)
        self.assertEqual(row['status'], 'UNKNOWN')
        self.assertIsNone(row['wait_event_info'])

    def test_other_executable_or_uid_cannot_qualify(self):
        m = self.module()
        for change in ('exe', 'uid'):
            with self.subTest(change=change):
                p = self.probe(m)
                if change == 'exe':
                    other = self.root / 'other'; other.write_bytes(self.exe.read_bytes())
                    (self.proc / 'exe').unlink(); (self.proc / 'exe').symlink_to(other)
                else:
                    p.uid += 1
                self.assertEqual(p.sample(self.pid, self.birth)['status'], 'UNKNOWN')
                (self.proc / 'exe').unlink(); (self.proc / 'exe').symlink_to(self.exe)

    def test_null_or_unmapped_wait_storage_is_unknown(self):
        m = self.module()
        for address in (0, 0xdeadbeef):
            with self.subTest(address=address):
                self.put(0x510, '<Q', address)
                self.assertEqual(self.probe(m).sample(self.pid, self.birth)['status'], 'UNKNOWN')

    def test_changed_wait_between_reads_is_unknown(self):
        m = self.module()
        p = self.probe(m)
        original = os.pread
        changed = False
        def racing(fd, size, offset):
            nonlocal changed
            data = original(fd, size, offset)
            if offset == self.bias + 0x4000 and not changed:
                changed = True
                self.put(0x4000, '<I', 1)
            return data
        with patch.object(m.os, 'pread', side_effect=racing):
            row = p.sample(self.pid, self.birth)
        self.assertEqual(row['status'], 'UNKNOWN')
        self.assertIsNone(row['wait_event_info'])

    def test_short_memory_read_or_process_exit_is_unknown(self):
        m = self.module()
        p = self.probe(m)
        (self.proc / 'mem').write_bytes(b'')
        self.assertEqual(p.sample(self.pid, self.birth)['status'], 'UNKNOWN')
        (self.proc / 'stat').unlink()
        self.assertEqual(p.sample(self.pid, self.birth)['status'], 'UNKNOWN')

    def test_libpq_hash_belongs_to_actual_mapped_inode(self):
        m = self.module()
        lib = self.root / 'libpq.so.5.16'; lib.write_bytes(b'actual mapped test library')
        self.maps.append(self.mapping(lib, 0x50000, 0x51000, 'r-xp'))
        self.set_maps()
        row = m.mapped_libpq(self.pid, self.exe, self.birth, os.getuid(), self.root)
        self.assertEqual(row['status'], 'OBSERVED')
        self.assertEqual(row['images'][0]['sha256'], hashlib.sha256(lib.read_bytes()).hexdigest())

    def test_missing_or_replaced_libpq_is_unknown(self):
        m = self.module()
        for mapped in (False, True):
            if mapped:
                lib = self.root / 'libpq.so.5'; lib.write_bytes(b'old')
                self.maps.append(self.mapping(lib, 0x50000, 0x51000, 'r-xp'))
                replacement = self.root / 'replacement'; replacement.write_bytes(b'new')
                replacement.replace(lib)
                self.set_maps()
            row = m.mapped_libpq(self.pid, self.exe, self.birth, os.getuid(), self.root)
            self.assertEqual(row['status'], 'UNKNOWN')
            self.assertEqual(row['images'], [])


if __name__ == '__main__':
    unittest.main()
