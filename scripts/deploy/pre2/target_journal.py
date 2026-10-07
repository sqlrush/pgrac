"""Persistent target-side deny obligations; never an isolation certificate.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import asdict, dataclass, replace
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
class RejoinIntent:
    system_identifier: int
    old_node_id: int
    old_incarnation: int
    candidate_incarnation: int
    rejoin_gate_digest: str


@dataclass(frozen=True)
class RejoinState:
    intent: RejoinIntent
    phase: int  # 1 ARMED, 2 RESTORING, 3 ACCESS_READY, 4 REVOKED
    refresh_started: bool = False  # Provider readback, never DB OPEN.


@dataclass(frozen=True)
class RejoinCommit:
    database_incarnation: int
    formation_epoch: int
    root_generation: int
    decision_digest: str


@dataclass(frozen=True)
class DenyState:
    identity: DrainIdentity
    phases: tuple
    successor_operation_id: str = ""
    rejoin: RejoinState = None
    completion: RejoinCommit = None


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


def _rejoin_intent(value):
    if (type(value) is not RejoinIntent or not _uint(value.system_identifier)
            or type(value.old_node_id) is not int or not 0 <= value.old_node_id < 128
            or not _uint(value.old_incarnation) or not _uint(value.candidate_incarnation)
            or value.candidate_incarnation <= value.old_incarnation
            or not _hex(value.rejoin_gate_digest, 64)):
        raise TargetJournalError("TARGET_REJOIN_INTENT")
    return value


def _rejoin_payload(value):
    try:
        if type(value) is not dict:
            raise ValueError
        return _rejoin_intent(RejoinIntent(**value))
    except (TypeError, ValueError):
        raise TargetJournalError("TARGET_REJOIN_INTENT") from None


def _rejoin_commit(value):
    if (type(value) is not RejoinCommit
            or any(not _uint(getattr(value, field)) for field in
                   ("database_incarnation", "formation_epoch", "root_generation"))
            or not _hex(value.decision_digest, 64)):
        raise TargetJournalError("TARGET_REJOIN_COMMIT")
    return value


def _commit_payload(value):
    try:
        if type(value) is not dict:
            raise ValueError
        return _rejoin_commit(RejoinCommit(**value))
    except (TypeError, ValueError):
        raise TargetJournalError("TARGET_REJOIN_COMMIT") from None


def _identity_reference(identity):
    return {"operation_id": identity.operation_id,
            "identity_digest": hashlib.sha256(_canonical(asdict(identity))).hexdigest()}


def _identity_payload(value):
    try:
        if type(value) is not dict or type(value["route_digests"]) is not list:
            raise ValueError
        fields = {**value, "route_digests": tuple(value["route_digests"])}
        return _identity(DrainIdentity(**fields))
    except (KeyError, TypeError, ValueError):
        raise TargetJournalError("TARGET_JOURNAL_IDENTITY") from None


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
                # A valid tail may still be in page cache after an interrupted
                # append. Establish durability before exposing replayed state
                # or allowing an idempotent acknowledgement without a write.
                os.fsync(self.fd)
                os.fsync(self.directory_fd)
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
        if kind == "HANDOFF":
            return self._handoff_transition(payload)
        if kind in ("REJOIN_ARM", "REJOIN_PHASE"):
            return self._rejoin_transition(payload, arm=kind == "REJOIN_ARM")
        if kind == "REJOIN_OWNER":
            return self._owner_transition(payload)
        if kind == "BOOT_RECONCILE":
            return self._boot_transition(payload)
        if kind == "REJOIN_COMMIT":
            return self._commit_transition(payload)
        raise TargetJournalError("TARGET_JOURNAL_TRANSITION")

    def _deny_transition(self, payload):
        requested = _identity_payload(payload)
        previous = self.states.get(requested.operation_id)
        phases = (0,) * len(requested.route_digests)
        if previous is not None:
            old = previous.identity
            immutable = ("guest_uuid", "mapping_generation", "protected_set_digest", "route_digests")
            if (previous.successor_operation_id or previous.completion is not None
                    or requested.attempt <= old.attempt
                    or (previous.rejoin is not None and previous.rejoin.phase != 4)
                    or (previous.rejoin is not None and old.target_boot_id != requested.target_boot_id)
                    or any(getattr(requested, field) != getattr(old, field) for field in immutable)):
                raise TargetJournalError("TARGET_JOURNAL_IDENTITY_DRIFT")
            if old.target_boot_id == requested.target_boot_id:
                phases = previous.phases
        elif (len(self.states) >= 128 or any(not state.successor_operation_id and state.completion is None
                                            and state.identity.guest_uuid == requested.guest_uuid
                                            for state in self.states.values())):
            raise TargetJournalError("TARGET_JOURNAL_OWNER_CONFLICT")
        states = dict(self.states)
        if previous is None:
            # A fresh fence closes the prior terminal lineage atomically. An
            # old successful completion can no longer be replied to as current.
            for key, state in self.states.items():
                if (not state.successor_operation_id and state.completion is not None
                        and state.identity.guest_uuid == requested.guest_uuid):
                    states[key] = replace(state, successor_operation_id=requested.operation_id)
        return {**states, requested.operation_id:
                DenyState(requested, phases, rejoin=previous.rejoin if previous else None)}

    def _route_transition(self, payload):
        if set(payload) != {"operation_id", "target_boot_id", "ordinal", "phase"}:
            raise TargetJournalError("TARGET_JOURNAL_ROUTE")
        operation, boot = payload["operation_id"], payload["target_boot_id"]
        ordinal, phase = payload["ordinal"], payload["phase"]
        if not _hex(operation, 32) or not _hex(boot, 32):
            raise TargetJournalError("TARGET_JOURNAL_ROUTE")
        state = self.states.get(operation)
        if (state is None or state.successor_operation_id or state.identity.target_boot_id != boot
                or (state.rejoin is not None and state.rejoin.phase != 4)
                or type(ordinal) is not int or not 0 <= ordinal < len(state.phases)
                or type(phase) is not int or not 1 <= phase <= 3
                or state.phases[ordinal] + 1 != phase):
            raise TargetJournalError("TARGET_JOURNAL_TRANSITION")
        phases = list(state.phases)
        phases[ordinal] = phase
        return {**self.states, operation: replace(state, phases=tuple(phases))}

    def _handoff_transition(self, payload):
        if set(payload) != {"previous", "successor"}:
            raise TargetJournalError("TARGET_JOURNAL_HANDOFF")
        reference = payload["previous"]
        if (type(reference) is not dict or set(reference) != {"operation_id", "identity_digest"}
                or not _hex(reference["operation_id"], 32) or not _hex(reference["identity_digest"], 64)):
            raise TargetJournalError("TARGET_JOURNAL_HANDOFF")
        old = self.states.get(reference["operation_id"])
        if (old is None or not hmac.compare_digest(reference["identity_digest"],
                                                  hashlib.sha256(_canonical(asdict(old.identity))).hexdigest())):
            raise TargetJournalError("TARGET_JOURNAL_HANDOFF")
        previous = old.identity
        successor = _identity_payload(payload["successor"])
        immutable = ("guest_uuid", "target_boot_id", "mapping_generation",
                     "protected_set_digest", "route_digests")
        if (old.successor_operation_id
                or (old.rejoin is not None and old.rejoin.phase != 4)
                or any(phase != 3 for phase in old.phases)
                or successor.operation_id in self.states or len(self.states) >= 128
                or any(getattr(previous, field) != getattr(successor, field) for field in immutable)):
            raise TargetJournalError("TARGET_JOURNAL_HANDOFF")
        return {**self.states,
                previous.operation_id: replace(old, successor_operation_id=successor.operation_id),
                successor.operation_id: DenyState(successor, old.phases)}

    def _rejoin_transition(self, payload, *, arm):
        if set(payload) != {"identity", "intent", "phase"}:
            raise TargetJournalError("TARGET_REJOIN_RECORD")
        reference = payload["identity"]
        if (type(reference) is not dict or set(reference) != {"operation_id", "identity_digest"}
                or not _hex(reference["operation_id"], 32) or not _hex(reference["identity_digest"], 64)):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        state = self.states.get(reference["operation_id"])
        if (state is None or state.successor_operation_id or state.completion is not None
                or not hmac.compare_digest(reference["identity_digest"],
                                            _identity_reference(state.identity)["identity_digest"])):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        intent, phase = _rejoin_payload(payload["intent"]), payload["phase"]
        if type(phase) is not int:
            raise TargetJournalError("TARGET_REJOIN_PHASE")
        if arm:
            if phase != 1:
                raise TargetJournalError("TARGET_REJOIN_PHASE")
            self._new_candidate(state, intent)
        elif (state.rejoin is None or state.rejoin.intent != intent
              or not ((state.rejoin.phase, phase) in ((1, 2), (2, 3))
                      or (state.rejoin.phase in (1, 2, 3) and phase == 4))):
            raise TargetJournalError("TARGET_REJOIN_PHASE")
        phases = (0,) * len(state.phases) if phase == 4 else state.phases
        rejoin = RejoinState(intent, phase) if arm else replace(state.rejoin, phase=phase)
        return {**self.states, state.identity.operation_id:
                replace(state, phases=phases, rejoin=rejoin)}

    def _new_candidate(self, state, intent):
        if state.rejoin is not None or any(value != 3 for value in state.phases):
            raise TargetJournalError("TARGET_REJOIN_NOT_DRAINED")
        for old in self.states.values():
            if old.identity.guest_uuid == state.identity.guest_uuid and old.rejoin is not None:
                prior = old.rejoin.intent
                if (intent.system_identifier != prior.system_identifier
                        or intent.old_node_id != prior.old_node_id
                        or intent.candidate_incarnation <= prior.candidate_incarnation):
                    raise TargetJournalError("TARGET_REJOIN_CANDIDATE_REUSED")

    def _owner_predecessor(self, reference):
        if (type(reference) is not dict or set(reference) != {"operation_id", "identity_digest"}
                or not _hex(reference["operation_id"], 32) or not _hex(reference["identity_digest"], 64)):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        state = self.states.get(reference["operation_id"])
        if (state is None or state.successor_operation_id or state.completion is not None
                or not hmac.compare_digest(reference["identity_digest"],
                                            _identity_reference(state.identity)["identity_digest"])):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        return state

    def _owner_transition(self, payload):
        if set(payload) != {"previous", "successor", "intent", "action"}:
            raise TargetJournalError("TARGET_REJOIN_OWNER_RECORD")
        state = self._owner_predecessor(payload["previous"])
        old, new = state.identity, _identity_payload(payload["successor"])
        intent, action = _rejoin_payload(payload["intent"]), payload["action"]
        if type(action) is not str or action not in ("authorize", "refresh", "revoke", "fence"):
            raise TargetJournalError("TARGET_REJOIN_OWNER_ACTION")
        immutable = ("guest_uuid", "target_boot_id", "mapping_generation",
                     "protected_set_digest", "route_digests")
        if any(getattr(old, name) != getattr(new, name) for name in immutable):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        if state.rejoin is not None and state.rejoin.intent != intent:
            raise TargetJournalError("TARGET_REJOIN_INTENT")
        if action == "fence":
            if state.rejoin is None or new.operation_id in self.states or len(self.states) >= 128:
                raise TargetJournalError("TARGET_REJOIN_OWNER_CONFLICT")
            return {**self.states,
                    old.operation_id: replace(state, successor_operation_id=new.operation_id),
                    new.operation_id: DenyState(new, (0,) * len(state.phases),
                                               rejoin=RejoinState(intent, 4))}
        if (old.operation_id != new.operation_id or new.attempt < old.attempt
                or (new.attempt == old.attempt and (old != new or action != "revoke"))
                or (action != "revoke" and old.daemon_boot_id != new.daemon_boot_id)):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        if action == "authorize":
            self._new_candidate(state, intent)
            rejoin, phases = RejoinState(intent, 1), state.phases
        elif action == "refresh":
            if state.rejoin is None or state.rejoin.phase != 3 or state.rejoin.refresh_started:
                raise TargetJournalError("TARGET_REJOIN_PHASE")
            rejoin, phases = replace(state.rejoin, refresh_started=True), state.phases
        else:
            if state.rejoin is None:
                if new.attempt == old.attempt:
                    raise TargetJournalError("TARGET_REJOIN_IDENTITY")
                self._new_candidate(state, intent)
            rejoin = RejoinState(intent, 4) if state.rejoin is None else replace(state.rejoin, phase=4)
            phases = state.phases if state.rejoin is not None and state.rejoin.phase == 4 else (0,) * len(state.phases)
        return {**self.states, old.operation_id: replace(state, identity=new, phases=phases, rejoin=rejoin)}

    def _boot_transition(self, payload):
        if set(payload) != {"previous", "successor", "intent"}:
            raise TargetJournalError("TARGET_BOOT_RECORD")
        state = self._owner_predecessor(payload["previous"])
        old, new = state.identity, _identity_payload(payload["successor"])
        intent = None if payload["intent"] is None else _rejoin_payload(payload["intent"])
        immutable = ("operation_id", "guest_uuid", "mapping_generation", "protected_set_digest", "route_digests")
        if (new.attempt <= old.attempt or new.target_boot_id == old.target_boot_id
                or any(getattr(old, key) != getattr(new, key) for key in immutable)
                or (state.rejoin is None and intent is not None)
                or (state.rejoin is not None and state.rejoin.intent != intent)):
            raise TargetJournalError("TARGET_BOOT_IDENTITY")
        rejoin = replace(state.rejoin, phase=4) if state.rejoin is not None else None
        return {**self.states, old.operation_id:
                replace(state, identity=new, phases=(0,) * len(state.phases), rejoin=rejoin)}

    def _commit_transition(self, payload):
        if set(payload) != {"identity", "intent", "completion"}:
            raise TargetJournalError("TARGET_REJOIN_COMMIT_RECORD")
        state = self._owner_predecessor(payload["identity"])
        intent, completion = _rejoin_payload(payload["intent"]), _commit_payload(payload["completion"])
        if (state.rejoin is None or state.rejoin.intent != intent
                or state.rejoin.phase != 3 or not state.rejoin.refresh_started
                or any(phase != 3 for phase in state.phases)):
            raise TargetJournalError("TARGET_REJOIN_COMMIT_UNPREPARED")
        return {**self.states, state.identity.operation_id: replace(state, completion=completion)}

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
        if previous is not None and (previous.successor_operation_id
                                     or (previous.rejoin is not None and previous.rejoin.phase != 4)):
            raise TargetJournalError("TARGET_JOURNAL_RETIRED")
        if previous is not None and previous.identity == requested:
            return
        payload = asdict(requested)
        payload["route_digests"] = list(requested.route_digests)
        self._append("DENY", payload)

    def reconcile_boot(self, requested, *, intent=None):
        """Adopt the authenticated owner's later attempt while exports are closed.

        Caller must validate current kernel boot and complete closed namespace.
        This never invents an attempt, forgets revocation or proves native drain.
        An exact retry cannot rewind this boot's already recorded progress.
        """
        self._usable()
        requested = _identity(requested)
        if intent is not None:
            intent = _rejoin_intent(intent)
        state = self.states.get(requested.operation_id)
        if state is None or state.successor_operation_id or state.completion is not None:
            raise TargetJournalError("TARGET_BOOT_IDENTITY")
        if state.identity == requested:
            if ((state.rejoin is None and intent is None)
                    or (state.rejoin is not None and state.rejoin.phase == 4 and state.rejoin.intent == intent)):
                return
            raise TargetJournalError("TARGET_BOOT_IDENTITY")
        self._append("BOOT_RECONCILE", {"previous": _identity_reference(state.identity),
                     "successor": {**asdict(requested), "route_digests": list(requested.route_digests)},
                     "intent": asdict(intent) if intent is not None else None})

    def advance(self, operation_id, target_boot_id, ordinal, phase):
        """Record an actual adapter edge, never an observation of absent state."""
        self._append("ROUTE", {"operation_id": operation_id, "target_boot_id": target_boot_id,
                               "ordinal": ordinal, "phase": phase})

    def handoff(self, previous, successor):
        """Transfer a completed deny without opening access or dropping history."""
        previous, successor = _identity(previous), _identity(successor)
        if self.handoff_recorded(previous, successor):
            return
        # The predecessor is already durable. Refer to its exact identity rather
        # than duplicating 128 routes and exceeding the existing record bound.
        first = {"operation_id": previous.operation_id,
                 "identity_digest": hashlib.sha256(_canonical(asdict(previous))).hexdigest()}
        second = asdict(successor)
        second["route_digests"] = list(successor.route_digests)
        self._append("HANDOFF", {"previous": first, "successor": second})

    def handoff_recorded(self, previous, successor):
        """Recorded exact ownership only, not current native OFF/drain proof."""
        self._usable()
        previous, successor = _identity(previous), _identity(successor)
        old, current = self.states.get(previous.operation_id), self.states.get(successor.operation_id)
        return (old is not None and old.identity == previous
                and old.successor_operation_id == successor.operation_id
                and current is not None and current.identity == successor
                and not current.successor_operation_id)

    def denied(self):
        """Unresolved/restart-denial obligations, not proof of physical absence."""
        self._usable()
        return tuple(state for state in self.states.values()
                     if not state.successor_operation_id and state.completion is None)

    def _current_rejoin_owner(self, identity):
        self._usable()
        identity = _identity(identity)
        state = self.states.get(identity.operation_id)
        if (state is None or state.successor_operation_id or state.completion is not None
                or state.identity != identity):
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        return state

    def arm_rejoin(self, identity, intent):
        """Persist an already authorized C-owner intent; never opens access.

        Caller must first bind registry/current OFF, invalidate old admissions
        and reserve the new incarnation. Exact retries do not rewind a phase.
        """
        state, intent = self._current_rejoin_owner(identity), _rejoin_intent(intent)
        if (state.rejoin is not None and not state.rejoin.refresh_started
                and state.rejoin.intent == intent and state.rejoin.phase in (1, 2, 3)):
            return
        self._append("REJOIN_ARM", {"identity": _identity_reference(identity),
                                   "intent": asdict(intent), "phase": 1})

    def advance_rejoin(self, identity, intent, phase):
        """Record a native owner's ordered edge; phase 4 requires fresh drain.

        Persist RESTORING before mutation. Persist ACCESS_READY only after native
        exact readback. REVOKED forbids late completion; none imply power or OPEN.
        """
        state, intent = self._current_rejoin_owner(identity), _rejoin_intent(intent)
        if type(phase) is not int or phase not in (2, 3, 4):
            raise TargetJournalError("TARGET_REJOIN_PHASE")
        if (state.rejoin is not None and state.rejoin.intent == intent
                and state.rejoin.phase == phase):
            return
        self._append("REJOIN_PHASE", {"identity": _identity_reference(identity),
                                     "intent": asdict(intent), "phase": phase})

    def _adopt_owner(self, identity, intent, action):
        self._usable()
        identity, intent = _identity(identity), _rejoin_intent(intent)
        state = self.states.get(identity.operation_id)
        if state is None or state.successor_operation_id or state.completion is not None:
            raise TargetJournalError("TARGET_REJOIN_IDENTITY")
        if state.identity == identity and state.rejoin is not None and state.rejoin.intent == intent:
            rejoin = state.rejoin
            if ((action == "authorize" and rejoin.phase in (1, 2, 3) and not rejoin.refresh_started)
                    or (action == "refresh" and rejoin.phase == 3 and rejoin.refresh_started)
                    or (action == "revoke" and rejoin.phase == 4)):
                return
        successor = {**asdict(identity), "route_digests": list(identity.route_digests)}
        self._append("REJOIN_OWNER", {"previous": _identity_reference(state.identity),
                                      "successor": successor, "intent": asdict(intent),
                                      "action": action})

    def authorize_rejoin(self, identity, intent):
        """Adopt the C AUTHORIZE_ON attempt and arm, in one durable append.

        Caller already invalidated old admissions and reserved the incarnation;
        native OFF/route validation must still precede restoring any permission.
        """
        self._adopt_owner(identity, intent, "authorize")

    def refresh_rejoin(self, identity, intent):
        """Adopt the C REFRESH_ON attempt without replaying permission or ON.

        Actual ON/current route readback is separate. This is never DB OPEN.
        """
        self._adopt_owner(identity, intent, "refresh")

    def revoke_rejoin(self, identity, intent):
        """Retire late grants before compensating OFF/drain under current owner."""
        self._adopt_owner(identity, intent, "revoke")

    def commit_rejoin(self, identity, intent, completion):
        """Consume the trusted membership owner's durable completion decision.

        Internal journal API only: caller must verify actual committed root
        evidence before entering. No provider observation or digest alone is
        authority. This neither changes native permissions nor grants DB OPEN.
        The same historical REFRESH identity survives a lost ACK/target reboot;
        it is not reinterpreted as a current-boot isolation proof.
        """
        self._usable()
        identity, intent, completion = _identity(identity), _rejoin_intent(intent), _rejoin_commit(completion)
        state = self.states.get(identity.operation_id)
        if (state is None or state.successor_operation_id or state.identity != identity
                or state.rejoin is None or state.rejoin.intent != intent):
            raise TargetJournalError("TARGET_REJOIN_COMMIT_IDENTITY")
        if state.completion is not None:
            if state.completion != completion:
                raise TargetJournalError("TARGET_REJOIN_COMMIT_CONFLICT")
            return
        self._append("REJOIN_COMMIT", {"identity": _identity_reference(identity),
                                      "intent": asdict(intent), "completion": asdict(completion)})

    def supersede_rejoin(self, identity, victim_incarnation):
        """New authorized fence of the candidate; retain exact revoked lineage.

        This changes only the obligation owner. It proves neither OFF nor drain.
        The authenticated C request must target this candidate, not the old one.
        """
        self._usable()
        identity = _identity(identity)
        if not _uint(victim_incarnation):
            raise TargetJournalError("TARGET_REJOIN_VICTIM")
        states = [s for s in self.denied() if s.identity.guest_uuid == identity.guest_uuid]
        if len(states) != 1:
            raise TargetJournalError("TARGET_REJOIN_OWNER_CONFLICT")
        state = states[0]
        if state.rejoin is None or state.rejoin.intent.candidate_incarnation != victim_incarnation:
            raise TargetJournalError("TARGET_REJOIN_VICTIM")
        if (state.identity == identity and state.rejoin.phase == 4
                and any(s.successor_operation_id == identity.operation_id and s.rejoin is not None
                        and s.rejoin.intent == state.rejoin.intent for s in self.states.values())):
            return
        successor = {**asdict(identity), "route_digests": list(identity.route_digests)}
        self._append("REJOIN_OWNER", {"previous": _identity_reference(state.identity),
                                      "successor": successor, "intent": asdict(state.rejoin.intent),
                                      "action": "fence"})

    def completion_recorded(self, operation_id, target_boot_id):
        """Past completion facts only; OFF/inventory/deny still need fresh checks."""
        self._usable()
        state = self.states.get(operation_id)
        return (state is not None and not state.successor_operation_id
                and state.completion is None
                and (state.rejoin is None or state.rejoin.phase == 4)
                and state.identity.target_boot_id == target_boot_id
                and all(phase == 3 for phase in state.phases))
