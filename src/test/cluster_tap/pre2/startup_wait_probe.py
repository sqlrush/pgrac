"""Passive Linux startup observations. No attach, signals, SQL, or target writes.

Equal bracketing reads qualify only the copied scalar, never an atomic wait
graph, duration, blocker, or proof that no intervening wait occurred.
"""
import hashlib
import os
from pathlib import Path
import re
import struct
import time


SYMBOLS = {'MyProcPid': 4, 'MyProc': 8, 'my_wait_event_info': 8}


def _file_key(st):
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns


def _digest(path):
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        digest = hashlib.sha256()
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if _file_key(before) != _file_key(after):
        raise ValueError('image changed while hashing')
    return digest.hexdigest(), after


def _identity(proc, pid, birth, uid, executable):
    if (type(pid) is not int or not 0 < pid < 2**31 or
            type(birth) is not int or birth <= 0 or proc.stat().st_uid != uid):
        raise ValueError('missing birth identity or wrong UID')
    text = (proc / 'stat').read_text()
    if int(text.split(' ', 1)[0]) != pid:
        raise ValueError('stat PID differs')
    actual = int(text.rsplit(')', 1)[1].split()[19])
    expected = executable.stat()
    actual_exe = (proc / 'exe').stat()
    if (actual != birth or (proc / 'exe').resolve(strict=True) != executable or
            (actual_exe.st_dev, actual_exe.st_ino) != (expected.st_dev, expected.st_ino)):
        raise ValueError('process birth or executable differs')
    return actual, actual_exe.st_dev, actual_exe.st_ino


def process_birth(pid, proc_root=Path('/proc')):
    """Kernel start ticks for a separately observed PID; no identity inference."""
    text = (Path(proc_root) / str(pid) / 'stat').read_text()
    if int(text.split(' ', 1)[0]) != pid:
        raise ValueError('stat PID differs')
    return int(text.rsplit(')', 1)[1].split()[19])


def _maps(proc):
    text = (proc / 'maps').read_text()
    if len(text) > 2 * 1024 * 1024:
        raise ValueError('maps exceeds diagnostic bound')
    result = []
    for line in text.splitlines():
        fields = line.split(None, 5)
        lo, hi = (int(x, 16) for x in fields[0].split('-'))
        result.append(dict(start=lo, end=hi, perms=fields[1],
                           offset=int(fields[2], 16),
                           device=tuple(int(x, 16) for x in fields[3].split(':')),
                           inode=int(fields[4]), path=fields[5] if len(fields) == 6 else ''))
    return result


def _mapped_file(row, st):
    return (row['inode'] == st.st_ino and
            row['device'] == (os.major(st.st_dev), os.minor(st.st_dev)))


class StartupWaitProbe:
    """Read only the original published wait scalar from a SHA-bound ELF64."""

    def __init__(self, executable, sha256, uid, proc_root=Path('/proc')):
        self.executable = Path(executable).resolve(strict=True)
        self.uid, self.proc_root = uid, Path(proc_root)
        before = self.executable.stat()
        if before.st_size > 128 * 1024 * 1024:
            raise ValueError('ELF exceeds diagnostic bound')
        data = self.executable.read_bytes()
        if (_file_key(before) != _file_key(self.executable.stat()) or
                hashlib.sha256(data).hexdigest() != sha256):
            raise ValueError('frozen executable hash differs')
        self.file_key, self.sha256 = _file_key(before), sha256

        def unpack(fmt, offset):
            if offset < 0 or offset + struct.calcsize(fmt) > len(data):
                raise ValueError('ELF range outside frozen file')
            return struct.unpack_from(fmt, data, offset)

        header = unpack('<16sHHIQQQIHHHHHH', 0)
        if (header[0][:7] != b'\x7fELF\x02\x01\x01' or header[1] not in (2, 3) or
                header[2] not in (62, 183) or header[3] != 1 or header[8] != 64 or
                header[9] != 56 or not 0 < header[10] <= 4096 or
                header[11] != 64 or not 0 < header[12] <= 8192):
            raise ValueError('unsupported ELF identity or tables')
        self.elf_type, self.machine = header[1:3]
        self.loads = []
        for i in range(header[10]):
            row = unpack('<IIQQQQQQ', header[5] + 56 * i)
            if row[0] == 1:
                if row[5] > row[6] or row[2] + row[5] > len(data):
                    raise ValueError('invalid ELF load range')
                self.loads.append(dict(offset=row[2], address=row[3],
                                       filesz=row[5], memsz=row[6], flags=row[1]))
        sections = [unpack('<IIQQQQIIQQ', header[6] + 64 * i)
                    for i in range(header[12])]
        self.symbols = {}
        for section in sections:
            if section[1] != 2:
                continue
            if section[9] != 24 or section[5] % 24 or section[6] >= len(sections):
                raise ValueError('invalid ELF symbol table')
            strings = sections[section[6]]
            if (strings[1] != 3 or strings[4] + strings[5] > len(data) or
                    section[4] + section[5] > len(data)):
                raise ValueError('ELF symbols or strings outside file')
            for i in range(section[5] // 24):
                name, info, _, index, address, size = unpack('<IBBHQQ', section[4] + i * 24)
                if name >= strings[5]:
                    raise ValueError('ELF symbol name outside table')
                start = strings[4] + name
                end = data.find(b'\0', start, strings[4] + strings[5])
                if end < 0:
                    raise ValueError('unterminated ELF symbol')
                label = data[start:end].decode('ascii', errors='replace')
                if label not in SYMBOLS:
                    continue
                if (label in self.symbols or info != 0x11 or index == 0 or
                        size != SYMBOLS[label] or not any(
                            s['flags'] & 2 and s['address'] <= address and
                            address + size <= s['address'] + s['memsz'] for s in self.loads)):
                    raise ValueError('ambiguous or invalid native wait symbol')
                self.symbols[label] = address
        if self.symbols.keys() != SYMBOLS.keys():
            raise ValueError('original native wait symbols unavailable')

    def _bias(self, maps):
        st = self.executable.stat()
        if _file_key(st) != self.file_key:
            raise ValueError('frozen executable changed')
        page = os.sysconf('SC_PAGE_SIZE')
        image_maps = [m for m in maps if _mapped_file(m, st)]
        if not any('x' in m['perms'] for m in image_maps):
            raise ValueError('native executable mapping absent')
        common = None
        for m in image_maps:
            candidates = set()
            for s in self.loads:
                low = s['offset'] // page * page
                high = (s['offset'] + s['filesz'] + page - 1) // page * page
                if low <= m['offset'] < high:
                    candidates.add(m['start'] - (s['address'] // page * page +
                                                 m['offset'] - low))
            common = candidates if common is None else common & candidates
        if not common or len(common) != 1:
            raise ValueError('ELF load bias is missing or ambiguous')
        bias = common.pop()
        if bias < 0 or self.elf_type == 2 and bias != 0:
            raise ValueError('invalid ELF relocation')
        return bias

    def sample(self, pid, birth):
        result = dict(status='UNKNOWN', pid=pid, start_ticks=birth,
                      wait_event_info=None, atomic_graph=False,
                      binary_sha256=self.sha256, wall_ns=time.time_ns(),
                      monotonic_ns=time.monotonic_ns())
        try:
            proc = self.proc_root / str(pid)
            first = _identity(proc, pid, birth, self.uid, self.executable)
            maps = _maps(proc)
            bias = self._bias(maps)
            fd = os.open(proc / 'mem', os.O_RDONLY | os.O_CLOEXEC)
            try:
                def read(address, fmt):
                    size = struct.calcsize(fmt)
                    if not any('r' in m['perms'] and m['start'] <= address and
                               address + size <= m['end'] for m in maps):
                        raise ValueError('published storage is not mapped readable')
                    data = os.pread(fd, size, address)
                    if len(data) != size:
                        raise ValueError('short native memory read')
                    return struct.unpack(fmt, data)[0]

                def cut():
                    native_pid = read(bias + self.symbols['MyProcPid'], '<i')
                    my_proc = read(bias + self.symbols['MyProc'], '<Q')
                    storage = read(bias + self.symbols['my_wait_event_info'], '<Q')
                    if native_pid != pid or my_proc == 0 or storage == 0:
                        raise ValueError('native process or wait storage unavailable')
                    return native_pid, my_proc, storage, read(storage, '<I')

                a, b = cut(), cut()
            finally:
                os.close(fd)
            if (a != b or first != _identity(proc, pid, birth, self.uid, self.executable) or
                    _file_key(self.executable.stat()) != self.file_key):
                raise ValueError('process or published wait changed during sample')
            result.update(status='OBSERVED', wait_event_info=a[3],
                          storage='original_my_wait_event_info', load_bias=bias)
        except (OSError, ValueError, IndexError, struct.error) as error:
            result['error'] = str(error)
        result.update(finished_wall_ns=time.time_ns(), finished_monotonic_ns=time.monotonic_ns())
        return result


def mapped_libpq(pid, executable, birth, uid, proc_root=Path('/proc')):
    """Hash only libpq files matching their actual mapped device/inode."""
    result = dict(status='UNKNOWN', pid=pid, start_ticks=birth, images=[],
                  wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    try:
        proc, executable = Path(proc_root) / str(pid), Path(executable).resolve(strict=True)
        first = _identity(proc, pid, birth, uid, executable)
        maps = _maps(proc)
        images, seen = [], set()
        for row in maps:
            path = Path(row['path'])
            if not path.is_absolute() or not re.fullmatch(r'libpq\.so(?:\.\d+)*', path.name):
                continue
            key = row['device'], row['inode'], row['path']
            if key in seen:
                continue
            seen.add(key)
            digest, st = _digest(proc / 'root' / str(path).lstrip('/'))
            if not _mapped_file(row, st):
                raise ValueError('mapped libpq was replaced or cannot be bound')
            images.append(dict(path=str(path), device=list(row['device']),
                               inode=row['inode'], bytes=st.st_size, sha256=digest))
        if not images or first != _identity(proc, pid, birth, uid, executable):
            raise ValueError('libpq mapping absent or client identity changed')
        result.update(status='OBSERVED', images=images)
    except (OSError, ValueError, IndexError) as error:
        result['error'] = str(error)
    result.update(finished_wall_ns=time.time_ns(), finished_monotonic_ns=time.monotonic_ns())
    return result
