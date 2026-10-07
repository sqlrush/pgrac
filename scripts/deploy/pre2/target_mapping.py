"""Consume maps using the pinned installed C verifier; no target action.

Author: SqlRush <sqlrush@gmail.com>

Verifier identity and map expectations are independently trusted deployment
inputs. They must never be obtained from the map or an unauthenticated request.
This loader neither certifies native inventory nor produces isolation evidence.
"""

from dataclasses import dataclass
import hashlib
import hmac
import os
import re
import stat
import struct
import subprocess
import sys
import time

from target_inventory import _open_path
from target_journal import TargetJournalError

MAX_PAYLOAD = 65536
MAX_PACKET = MAX_PAYLOAD + 96


@dataclass(frozen=True)
class VerifierPin:
    path: str
    sha256: str
    owner_uid: int = 0


@dataclass(frozen=True)
class MapExpected:
    system_identifier: int
    node_id: int
    mapping_generation: int
    protected_set_digest: str
    public_key: bytes


@dataclass(frozen=True)
class ProtectedRoute:
    ordinal: int
    digest: str
    kind: int
    initiator: str
    credential_ref: str
    target: str
    endpoint: str
    tpg: int
    lun_wwid: str
    lun_serial: str
    backstore_uuid: str
    media_kind: int
    filesystem_uuid: str
    roles: int


@dataclass(frozen=True)
class ProtectedMap:
    system_identifier: int
    node_id: int
    mapping_generation: int
    protected_set_digest: str
    database_uuid: str
    storage_uuid: str
    authority_uuid: str
    profile: str
    hypervisor_uuid: str
    guest_uuid: str
    routes: tuple


def _u64(value, minimum=1):
    return type(value) is int and minimum <= value < 1 << 64


def _hex_digest(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) and value != "0" * 64


def _arguments(verifier, expected, packet, deadline):
    if (type(verifier) is not VerifierPin or not _hex_digest(verifier.sha256)
            or type(verifier.owner_uid) is not int or verifier.owner_uid < 0
            or type(expected) is not MapExpected or not _u64(expected.system_identifier)
            or not _u64(expected.mapping_generation) or not _u64(expected.node_id, 0)
            or expected.node_id >= 128 or not _hex_digest(expected.protected_set_digest)
            or type(expected.public_key) is not bytes or len(expected.public_key) != 32
            or type(packet) is not bytes or not 96 < len(packet) <= MAX_PACKET
            or not _u64(deadline) or deadline <= time.monotonic_ns()):
        raise TargetJournalError("TARGET_MAP_ARGUMENT")
    if sys.platform != "linux":
        raise TargetJournalError("TARGET_MAP_PLATFORM_UNSUPPORTED")


def _pin_verifier(verifier):
    fd = _open_path(verifier.path, os.O_RDONLY)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != verifier.owner_uid
                or before.st_mode & 0o022 or not before.st_mode & 0o111 or before.st_nlink != 1
                or not 0 < before.st_size <= 16 * 1024 * 1024):
            raise TargetJournalError("TARGET_MAP_EXECUTABLE")
        digest, read_bytes = hashlib.sha256(), 0
        while True:
            data = os.read(fd, 65536)
            if not data:
                break
            read_bytes += len(data)
            if read_bytes > before.st_size:
                raise TargetJournalError("TARGET_MAP_EXECUTABLE")
            digest.update(data)
        after = os.fstat(fd)
        # Reading for the digest may itself update atime. Only stable identity,
        # permissions and content-change metadata belong in this comparison.
        stable = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink",
                  "st_size", "st_mtime_ns", "st_ctime_ns")
        if (read_bytes != before.st_size
                or any(getattr(before, field) != getattr(after, field) for field in stable)
                or not hmac.compare_digest(digest.hexdigest(), verifier.sha256)):
            raise TargetJournalError("TARGET_MAP_EXECUTABLE")
        return fd
    except BaseException:
        os.close(fd)
        raise


class _Fields:
    """Structural extraction only, after common C verification has succeeded."""

    def __init__(self, payload):
        self.payload = payload
        self.offset = 0

    def field(self, size=None):
        if self.offset + 4 > len(self.payload):
            raise TargetJournalError("TARGET_MAP_PAYLOAD")
        length = struct.unpack_from("<I", self.payload, self.offset)[0]
        self.offset += 4
        end = self.offset + length
        if length > 1024 or end > len(self.payload) or (size is not None and size != length):
            raise TargetJournalError("TARGET_MAP_PAYLOAD")
        value = self.payload[self.offset:end]
        self.offset = end
        return value

    def u32(self):
        return struct.unpack("<I", self.field(4))[0]

    def u64(self):
        return struct.unpack("<Q", self.field(8))[0]

    def uuid(self):
        return self.field(16).hex()

    def text(self):
        value = self.field()
        if not value or any(byte < 0x21 or byte > 0x7e for byte in value):
            raise TargetJournalError("TARGET_MAP_PAYLOAD")
        return value.decode("ascii")


def _payload_map(expected, payload):
    if (not 0 < len(payload) <= MAX_PAYLOAD
            or not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected.protected_set_digest)):
        raise TargetJournalError("TARGET_MAP_PAYLOAD")
    fields = _Fields(payload)
    if fields.text() != "PGRAC-PROTECTED-SET-V2" or fields.u32() != 3:
        raise TargetJournalError("TARGET_MAP_PAYLOAD")
    database, storage, authority = fields.uuid(), fields.uuid(), fields.uuid()
    profile, generation = fields.text(), fields.u64()
    hypervisor, guest, count = fields.uuid(), fields.uuid(), fields.u32()
    if profile != "pre2-kvm-gfs2-v1" or generation != expected.mapping_generation or not 0 < count <= 128:
        raise TargetJournalError("TARGET_MAP_PROFILE")
    routes = []
    for ordinal in range(count):
        start = fields.offset
        kind = fields.u32()
        initiator, credential, target, endpoint = fields.text(), fields.text(), fields.text(), fields.text()
        tpg, wwid, serial = fields.u32(), fields.text(), fields.text()
        backstore, media, filesystem, roles = fields.uuid(), fields.u32(), fields.uuid(), fields.u32()
        digest = hashlib.sha256(b"PGRAC-TARGET-ROUTE-V1\0" + struct.pack("<I", ordinal)
                                + payload[start:fields.offset]).hexdigest()
        routes.append(ProtectedRoute(ordinal, digest, kind, initiator, credential, target, endpoint,
                                     tpg, wwid, serial, backstore, media, filesystem, roles))
    if fields.offset != len(payload):
        raise TargetJournalError("TARGET_MAP_PAYLOAD")
    return ProtectedMap(expected.system_identifier, expected.node_id, generation,
                        expected.protected_set_digest, database, storage, authority, profile,
                        hypervisor, guest, tuple(routes))


def load_verified_map(verifier, expected, packet, deadline_mono_ns):
    """Verify one map under the existing caller envelope; no shell/path fallback."""
    fd = None
    try:
        _arguments(verifier, expected, packet, deadline_mono_ns)
        fd = _pin_verifier(verifier)
        remaining = (deadline_mono_ns - time.monotonic_ns()) / 1_000_000_000
        if remaining <= 0:
            raise TargetJournalError("TARGET_MAP_ENVELOPE_EXPIRED")
        result = subprocess.run(
            [f"/proc/self/fd/{fd}", str(expected.system_identifier), str(expected.node_id),
             str(expected.mapping_generation), expected.protected_set_digest, expected.public_key.hex()],
            input=packet, stdout=subprocess.PIPE, stderr=subprocess.PIPE, pass_fds=(fd,),
            env={"LC_ALL": "C"}, timeout=remaining, check=False)
        if result.returncode != 0 or result.stderr or time.monotonic_ns() >= deadline_mono_ns:
            raise TargetJournalError("TARGET_MAP_UNVERIFIED")
        return _payload_map(expected, result.stdout)
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_MAP_UNPROVEN") from None
    finally:
        if fd is not None:
            os.close(fd)
