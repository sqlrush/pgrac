"""Owner-only local locators bound to authenticated protected-set identities.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import dataclass, fields
import hashlib
import json
import os
import re
import stat
import time

from target_inventory import IQN, RouteBinding, _path, _portal, _uint
from target_journal import DrainIdentity, TargetJournalError, _canonical, _identity, _pairs
from target_mapping import (MAX_PACKET, MapExpected, ProtectedMap, ProtectedRoute,
                            VerifierPin, _hex_digest, load_verified_map)

MAX_REGISTRY_BYTES = 4 * 1024 * 1024
LOGICAL_FIELDS = frozenset(field.name for field in fields(ProtectedRoute))
NATIVE_FIELDS = frozenset(("mapped_lun", "tpg_lun", "storage_path", "file_path", "device", "inode", "size"))


@dataclass(frozen=True)
class NodeRegistry:
    mapping: ProtectedMap
    bindings: tuple


@dataclass(frozen=True)
class TargetRegistry:
    inventory_digest: str
    nodes: tuple

    def node(self, node_id):
        if type(node_id) is int:
            for node in self.nodes:
                if node.mapping.node_id == node_id:
                    return node
        raise TargetJournalError("TARGET_REGISTRY_NODE")

    def drain_identity(self, node_id, operation_id, attempt, daemon_boot_id, target_boot_id):
        mapping = self.node(node_id).mapping
        return _identity(DrainIdentity(operation_id, attempt, daemon_boot_id, target_boot_id,
                                       mapping.guest_uuid, mapping.mapping_generation,
                                       mapping.protected_set_digest,
                                       tuple(route.digest for route in mapping.routes)))


def _keys(value, expected):
    if type(value) is not dict or value.keys() != expected:
        raise TargetJournalError("TARGET_REGISTRY_SCHEMA")


def _constant(_value):
    raise TargetJournalError("TARGET_REGISTRY_JSON")


def _open_registry(path, owner_uid):
    parts = _path(path)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory = os.open("/", flags)
    try:
        # Root-owned sticky ancestors (e.g. /tmp) protect each trusted-owned
        # child against rename by other users. Every child is then checked too;
        # an attacker-owned intermediate directory is never trusted.
        for index, component in enumerate(parts[1:], 1):
            info = os.fstat(directory)
            sticky_root = info.st_uid == 0 and info.st_mode & stat.S_ISVTX
            if (info.st_uid not in (0, owner_uid)
                    or (info.st_mode & 0o022 and not sticky_root)):
                raise TargetJournalError("TARGET_REGISTRY_DIRECTORY")
            if index == len(parts) - 1:
                return os.open(component, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                               dir_fd=directory)
            child = os.open(component, flags, dir_fd=directory)
            os.close(directory)
            directory = child
    finally:
        os.close(directory)


def _read_owned(path, owner_uid):
    fd = _open_registry(path, owner_uid)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != owner_uid
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or not 0 < before.st_size <= MAX_REGISTRY_BYTES):
            raise TargetJournalError("TARGET_REGISTRY_FILE")
        data = bytearray()
        while len(data) <= MAX_REGISTRY_BYTES:
            block = os.read(fd, min(65536, MAX_REGISTRY_BYTES + 1 - len(data)))
            if not block:
                break
            data.extend(block)
        after = os.fstat(fd)
        stable = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink",
                  "st_size", "st_mtime_ns", "st_ctime_ns")
        if (len(data) != before.st_size
                or any(getattr(before, key) != getattr(after, key) for key in stable)):
            raise TargetJournalError("TARGET_REGISTRY_CHANGED")
        return json.loads(data.decode("utf-8", errors="strict"), object_pairs_hook=_pairs,
                          parse_constant=_constant)
    finally:
        os.close(fd)


def _route_binding(route, local):
    _keys(local, LOGICAL_FIELDS | NATIVE_FIELDS)
    for key in LOGICAL_FIELDS:
        signed = getattr(route, key)
        if type(local[key]) is not type(signed) or local[key] != signed:
            raise TargetJournalError("TARGET_REGISTRY_ROUTE_IDENTITY")
    if (route.kind != 1 or not re.fullmatch(IQN, route.target)
            or not re.fullmatch(IQN, route.initiator) or not _uint(route.tpg, 65535, 1)
            or not re.fullmatch(r"[!-~]{1,255}", route.lun_serial)
            or not _uint(local["mapped_lun"], 255) or not _uint(local["tpg_lun"], 65535)
            or not _uint(local["device"], (1 << 64) - 1)
            or not _uint(local["inode"], (1 << 64) - 1, 1)
            or not _uint(local["size"], (1 << 63) - 1, 1)
            or type(local["storage_path"]) is not str
            or not re.fullmatch(r"/sys/kernel/config/target/core/fileio_[0-9]+/[A-Za-z0-9_-][A-Za-z0-9_.-]{0,127}",
                                local["storage_path"])):
        raise TargetJournalError("TARGET_REGISTRY_NATIVE_BINDING")
    _path(local["file_path"])
    address, port_text = route.endpoint.split(":")
    if not re.fullmatch(r"[1-9][0-9]{0,4}", port_text):
        raise TargetJournalError("TARGET_REGISTRY_NATIVE_BINDING")
    portal, port = _portal(address, int(port_text))
    return RouteBinding(route.ordinal, route.digest, route.target, route.tpg, portal, port,
                        route.initiator, local["mapped_lun"], local["tpg_lun"], local["storage_path"],
                        route.lun_serial, local["file_path"], local["device"], local["inode"], local["size"])


def load_registry(path, deadline_mono_ns, *, owner_uid=0):
    """Authenticate the complete local inventory without opening target exports.

    The file owner, verifier/key and local locators are deployment trust, not
    request fields. The returned immutable inventory is not drain evidence.
    """
    try:
        if (type(owner_uid) is not int or owner_uid < 0
                or not _uint(deadline_mono_ns, (1 << 64) - 1, 1)
                or deadline_mono_ns <= time.monotonic_ns()):
            raise TargetJournalError("TARGET_REGISTRY_ARGUMENT")
        document = _read_owned(path, owner_uid)
        _keys(document, {"version", "system_identifier", "mapping_generation", "map_public_key", "verifier", "nodes"})
        if (type(document["version"]) is not int or document["version"] != 1
                or not _uint(document["system_identifier"], (1 << 64) - 1, 1)
                or not _uint(document["mapping_generation"], (1 << 64) - 1, 1)
                or not _hex_digest(document["map_public_key"])
                or type(document["nodes"]) is not list or not 1 <= len(document["nodes"]) <= 4):
            raise TargetJournalError("TARGET_REGISTRY_SCHEMA")
        _keys(document["verifier"], {"path", "sha256"})
        verifier = VerifierPin(document["verifier"]["path"], document["verifier"]["sha256"], owner_uid)
        key = bytes.fromhex(document["map_public_key"])
        nodes, node_ids, guest_ids, shared = [], set(), set(), None
        for item in document["nodes"]:
            _keys(item, {"node_id", "protected_set_digest", "signed_map", "routes"})
            if (not _uint(item["node_id"], 127) or item["node_id"] in node_ids
                    or type(item["signed_map"]) is not str
                    or not 192 < len(item["signed_map"]) <= 2 * MAX_PACKET
                    or not re.fullmatch(r"(?:[0-9a-f]{2})+", item["signed_map"])):
                raise TargetJournalError("TARGET_REGISTRY_NODE")
            expected = MapExpected(document["system_identifier"], item["node_id"], document["mapping_generation"],
                                   item["protected_set_digest"], key)
            mapping = load_verified_map(verifier, expected, bytes.fromhex(item["signed_map"]), deadline_mono_ns)
            common = (mapping.database_uuid, mapping.storage_uuid, mapping.authority_uuid,
                      mapping.hypervisor_uuid, mapping.profile)
            if (shared is not None and shared != common) or mapping.guest_uuid in guest_ids:
                raise TargetJournalError("TARGET_REGISTRY_SHARED_IDENTITY")
            shared = common
            if type(item["routes"]) is not list or len(item["routes"]) != len(mapping.routes):
                raise TargetJournalError("TARGET_REGISTRY_ROUTE_COVERAGE")
            bindings = tuple(_route_binding(route, local) for route, local in zip(mapping.routes, item["routes"]))
            native_keys = {(b.target, b.tpg, b.portal, b.port, b.initiator, b.mapped_lun) for b in bindings}
            if len(native_keys) != len(bindings):
                raise TargetJournalError("TARGET_REGISTRY_ROUTE_COVERAGE")
            nodes.append(NodeRegistry(mapping, bindings))
            node_ids.add(mapping.node_id)
            guest_ids.add(mapping.guest_uuid)
        if time.monotonic_ns() >= deadline_mono_ns:
            raise TargetJournalError("TARGET_REGISTRY_ENVELOPE_EXPIRED")
        return TargetRegistry(hashlib.sha256(_canonical(document)).hexdigest(), tuple(nodes))
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_REGISTRY_UNPROVEN") from None
