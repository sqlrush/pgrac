"""Persistent target-side deny obligations; never an isolation certificate.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import asdict, dataclass
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import PurePath
import re
import stat

MAX_JOURNAL_BYTES = 4 * 1024 * 1024
MAX_RECORD_BYTES = 16 * 1024
ZERO_DIGEST = "0" * 64


class TargetJournalError(RuntimeError):
    """Value-free failure: target admission must remain closed."""


@dataclass(frozen=True)
class DrainIdentity:
    operation_id: str
    attempt: int
    daemon_boot_id: str
    target_boot_id: str
    guest_uuid: str
    mapping_generation: int
    protected_set_digest: str
    route_digests: tuple


@dataclass(frozen=True)
class DenyState:
    identity: DrainIdentity
    phases: tuple


def _hex(value, size):
    return (type(value) is str and re.fullmatch(r"[0-9a-f]{%d}" % size, value)
            and value != "0" * size)


def _uint(value):
    return type(value) is int and 0 < value < 1 << 64


def _identity(value):
    if (type(value) is not DrainIdentity or not _uint(value.attempt)
            or not _uint(value.mapping_generation)
            or any(not _hex(getattr(value, name), 32) for name in
                   ("operation_id", "daemon_boot_id", "target_boot_id", "guest_uuid"))
            or not _hex(value.protected_set_digest, 64)
            or type(value.route_digests) is not tuple
            or not 0 < len(value.route_digests) <= 128
            or any(not _hex(item, 64) for item in value.route_digests)
            or len(set(value.route_digests)) != len(value.route_digests)):
        raise TargetJournalError("TARGET_IDENTITY_INVALID")
    return value


def _canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise TargetJournalError("TARGET_JOURNAL_DUPLICATE_KEY")
        result[key] = value
    return result


def _directory_fd(directory, owner_uid):
    """Open every path component without following a symbolic link."""
    path = PurePath(directory)
    if not path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
        raise TargetJournalError("TARGET_JOURNAL_DIRECTORY")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open(path.anchor, flags)
    try:
        for component in path.parts[1:]:
            status = os.fstat(fd)
            sticky_root = status.st_uid == 0 and status.st_mode & stat.S_ISVTX
            if (status.st_uid not in (0, owner_uid)
                    or (status.st_mode & 0o022 and not sticky_root)):
                raise TargetJournalError("TARGET_JOURNAL_DIRECTORY")
            child = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        status = os.fstat(fd)
        if status.st_uid != owner_uid or stat.S_IMODE(status.st_mode) != 0o700:
            raise TargetJournalError("TARGET_JOURNAL_DIRECTORY")
        return fd
    except BaseException:
        os.close(fd)
        raise


class TargetJournal:
    """Exclusive journal owner; no physical action is implemented here."""

    def __init__(self, directory, inventory_digest, *, initialize=False, owner_uid=0):
        self.fd = self.directory_fd = None
        self.poisoned = False
        self.inventory = inventory_digest
        self.sequence = self.size = 0
        self.digest = ZERO_DIGEST
        self.states = {}
        if (not _hex(inventory_digest, 64) or type(initialize) is not bool
                or type(owner_uid) is not int or owner_uid < 0):
            raise TargetJournalError("TARGET_JOURNAL_ARGUMENT")
        try:
            self.directory_fd = _directory_fd(directory, owner_uid)
            flags = os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC
            if initialize:
                flags |= os.O_CREAT | os.O_EXCL
            self.fd = os.open("deny.journal", flags, 0o600, dir_fd=self.directory_fd)
            status = os.fstat(self.fd)
            if (not stat.S_ISREG(status.st_mode) or status.st_uid != owner_uid
                    or stat.S_IMODE(status.st_mode) != 0o600 or status.st_nlink != 1):
                raise TargetJournalError("TARGET_JOURNAL_FILE")
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TargetJournalError("TARGET_JOURNAL_BUSY") from None
            if initialize:
                self._append("GENESIS", {"inventory_digest": inventory_digest})
                os.fsync(self.directory_fd)
            else:
                self._replay()
        except BaseException as error:
            self.close()
            if isinstance(error, OSError):
                raise TargetJournalError("TARGET_JOURNAL_IO") from None
            raise

    def __enter__(self):
        self._usable()
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        for name in ("fd", "directory_fd"):
            fd = getattr(self, name, None)
            if fd is not None:
                setattr(self, name, None)
                os.close(fd)

    def _usable(self):
        if self.fd is None or self.poisoned:
            raise TargetJournalError("TARGET_JOURNAL_UNUSABLE")

    def _transition(self, kind, payload):
        if type(payload) is not dict:
            raise TargetJournalError("TARGET_JOURNAL_RECORD")
        if self.sequence == 0:
            if kind != "GENESIS" or payload != {"inventory_digest": self.inventory}:
                raise TargetJournalError("TARGET_JOURNAL_GENESIS")
            return {}
        if kind == "DENY":
            return self._deny_transition(payload)
        if kind == "ROUTE":
            return self._route_transition(payload)
        raise TargetJournalError("TARGET_JOURNAL_TRANSITION")

    def _deny_transition(self, payload):
        try:
            fields = dict(payload)
            if type(fields["route_digests"]) is not list:
                raise ValueError
            fields["route_digests"] = tuple(fields["route_digests"])
            requested = _identity(DrainIdentity(**fields))
        except (KeyError, TypeError, ValueError):
            raise TargetJournalError("TARGET_JOURNAL_IDENTITY") from None
        previous = self.states.get(requested.operation_id)
        phases = (0,) * len(requested.route_digests)
        if previous is not None:
            old = previous.identity
            immutable = ("guest_uuid", "mapping_generation", "protected_set_digest", "route_digests")
            if (requested.attempt <= old.attempt
                    or any(getattr(requested, field) != getattr(old, field) for field in immutable)):
                raise TargetJournalError("TARGET_JOURNAL_IDENTITY_DRIFT")
            if old.target_boot_id == requested.target_boot_id:
                phases = previous.phases
        elif (len(self.states) >= 128 or any(state.identity.guest_uuid == requested.guest_uuid
                                            for state in self.states.values())):
            raise TargetJournalError("TARGET_JOURNAL_OWNER_CONFLICT")
        return {**self.states, requested.operation_id: DenyState(requested, phases)}

    def _route_transition(self, payload):
        if set(payload) != {"operation_id", "target_boot_id", "ordinal", "phase"}:
            raise TargetJournalError("TARGET_JOURNAL_ROUTE")
        operation, boot = payload["operation_id"], payload["target_boot_id"]
        ordinal, phase = payload["ordinal"], payload["phase"]
        if not _hex(operation, 32) or not _hex(boot, 32):
            raise TargetJournalError("TARGET_JOURNAL_ROUTE")
        state = self.states.get(operation)
        if (state is None or state.identity.target_boot_id != boot
                or type(ordinal) is not int or not 0 <= ordinal < len(state.phases)
                or type(phase) is not int or not 1 <= phase <= 3
                or state.phases[ordinal] + 1 != phase):
            raise TargetJournalError("TARGET_JOURNAL_TRANSITION")
        phases = list(state.phases)
        phases[ordinal] = phase
        return {**self.states, operation: DenyState(state.identity, tuple(phases))}

    def _replay(self):
        chunks, size = [], 0
        while True:
            part = os.read(self.fd, min(65536, MAX_JOURNAL_BYTES + 1 - size))
            if not part:
                break
            chunks.append(part)
            size += len(part)
            if size > MAX_JOURNAL_BYTES:
                raise TargetJournalError("TARGET_JOURNAL_TOO_LARGE")
        raw = b"".join(chunks)
        if not raw or not raw.endswith(b"\n"):
            raise TargetJournalError("TARGET_JOURNAL_INCOMPLETE")
        for line in raw.splitlines(keepends=True):
            self._replay_record(line)
        self.size = size

    def _replay_record(self, line):
        if len(line) > MAX_RECORD_BYTES:
            raise TargetJournalError("TARGET_JOURNAL_TOO_LARGE")
        try:
            record = json.loads(line, object_pairs_hook=_pairs)
            if (type(record) is not dict or _canonical(record) != line
                    or set(record) != {"version", "sequence", "previous", "kind", "payload", "digest"}
                    or type(record["version"]) is not int or record["version"] != 1
                    or type(record["sequence"]) is not int
                    or record["sequence"] != self.sequence + 1
                    or record["previous"] != self.digest or not _hex(record["digest"], 64)):
                raise TargetJournalError("TARGET_JOURNAL_RECORD")
            digest = record.pop("digest")
            if not hmac.compare_digest(digest, hashlib.sha256(_canonical(record)).hexdigest()):
                raise TargetJournalError("TARGET_JOURNAL_CHECKSUM")
            states = self._transition(record["kind"], record["payload"])
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise TargetJournalError("TARGET_JOURNAL_RECORD") from None
        self.states, self.digest, self.sequence = states, digest, record["sequence"]

    def _append(self, kind, payload):
        self._usable()
        states = self._transition(kind, payload)
        record = {"version": 1, "sequence": self.sequence + 1,
                  "previous": self.digest, "kind": kind, "payload": payload}
        digest = hashlib.sha256(_canonical(record)).hexdigest()
        raw = _canonical({**record, "digest": digest})
        if len(raw) > MAX_RECORD_BYTES or self.size + len(raw) > MAX_JOURNAL_BYTES:
            raise TargetJournalError("TARGET_JOURNAL_TOO_LARGE")
        try:
            if os.fstat(self.fd).st_size != self.size:
                raise OSError("changed journal")
            position = 0
            while position < len(raw):
                count = os.write(self.fd, raw[position:])
                if count <= 0:
                    raise OSError("short write")
                position += count
            os.fsync(self.fd)
        except BaseException as error:
            self.poisoned = True
            if isinstance(error, OSError):
                raise TargetJournalError("TARGET_JOURNAL_IO") from None
            raise
        # Publish only after fsync. A caller must not act from a failed append.
        self.states, self.digest = states, digest
        self.sequence += 1
        self.size += len(raw)

    def deny(self, requested):
        self._usable()
        requested = _identity(requested)
        previous = self.states.get(requested.operation_id)
        if previous is not None and previous.identity == requested:
            return
        payload = asdict(requested)
        payload["route_digests"] = list(requested.route_digests)
        self._append("DENY", payload)

    def advance(self, operation_id, target_boot_id, ordinal, phase):
        """Record an actual adapter edge, never an observation of absent state."""
        self._append("ROUTE", {"operation_id": operation_id, "target_boot_id": target_boot_id,
                               "ordinal": ordinal, "phase": phase})

    def denied(self):
        self._usable()
        return tuple(self.states.values())

    def completion_recorded(self, operation_id, target_boot_id):
        """Past completion facts only; OFF/inventory/deny still need fresh checks."""
        self._usable()
        state = self.states.get(operation_id)
        return (state is not None and state.identity.target_boot_id == target_boot_id
                and all(phase == 3 for phase in state.phases))
