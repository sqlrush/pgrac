"""Resolve authenticated route bindings without changing target configuration.

Author: SqlRush <sqlrush@gmail.com>

The caller owns configuration exclusively throughout resolve, drain and final
readback. It supplies an already initialized native RTSRoot: its constructor
can mount configfs or load modules and is deliberately not called here. Local
bindings must already be bound to the authenticated protected set. These handles
are neither authentication nor an OFF/isolation certificate.
"""

from dataclasses import dataclass
import ipaddress
import os
from pathlib import PurePosixPath
import re
import stat

from target_drain import AclGroup, FlushGroup, _require_absent
from target_journal import TargetJournalError, _identity

MAX_NATIVE_ITEMS = 4096
IQN = r"iqn\.[a-z0-9][a-z0-9.:-]{1,218}"
ISCSI = "/sys/kernel/config/target/iscsi/"


@dataclass(frozen=True)
class RouteBinding:
    """Trusted local binding; serial is LIO's unit serial, not a SCSI WWID."""
    ordinal: int
    digest: str
    target: str
    tpg: int
    portal: str
    port: int
    initiator: str
    mapped_lun: int
    tpg_lun: int
    storage_path: str
    serial: str
    file_path: str
    device: int
    inode: int
    size: int


class ResolvedRoutes:
    """Own the open files for one synchronous inventory/drain/readback scope."""

    def __init__(self, acls, flushes):
        self.acl_groups = tuple(acls)
        self.flush_groups = tuple(flushes)
        self.closed = False

    def close(self):
        if not self.closed:
            self.closed = True
            for group in self.flush_groups:
                os.close(group.fd)

    def __enter__(self):
        if self.closed:
            raise TargetJournalError("TARGET_INVENTORY_CLOSED")
        return self

    def __exit__(self, *_args):
        self.close()


@dataclass(frozen=True)
class _AbsentAcl:
    path: str
    mapped_luns: tuple = ()


def _uint(value, maximum, minimum=0):
    return type(value) is int and minimum <= value <= maximum


def _path(path):
    if (type(path) is not str or not 1 < len(path) < 4096 or "\x00" in path
            or not path.startswith("/") or path.startswith("//")
            or ".." in PurePosixPath(path).parts or str(PurePosixPath(path)) != path):
        raise TargetJournalError("TARGET_PATH_INVALID")
    return PurePosixPath(path).parts


def _open_path(path, flags):
    parts = _path(path)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory = os.open("/", directory_flags)
    try:
        for component in parts[1:-1]:
            child = os.open(component, directory_flags, dir_fd=directory)
            os.close(directory)
            directory = child
        return os.open(parts[-1], flags | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                       dir_fd=directory)
    finally:
        os.close(directory)


def _read_config(path):
    fd = _open_path(path, os.O_RDONLY)
    try:
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(4097)
        if len(data) > 4096:
            raise TargetJournalError("TARGET_CONFIG_TOO_LARGE")
        return data.decode("ascii", errors="strict")
    finally:
        os.close(fd)


def _portal(address, port):
    # This certified fileio profile uses explicit IPv4/TCP portals. A wildcard,
    # offload or an additional transport is not silently covered by that proof.
    if type(address) is not str or not _uint(port, 65535, 1):
        raise TargetJournalError("TARGET_PORTAL_INVALID")
    parsed = ipaddress.IPv4Address(address)
    if str(parsed) != address or parsed.is_unspecified or parsed.is_multicast:
        raise TargetJournalError("TARGET_PORTAL_INVALID")
    return address, port


def _bindings(identity, bindings):
    _identity(identity)
    if type(bindings) is not tuple or len(bindings) != len(identity.route_digests):
        raise TargetJournalError("TARGET_ROUTE_BINDING")
    groups, keys = {}, set()
    for ordinal, item in enumerate(bindings):
        if (type(item) is not RouteBinding or type(item.ordinal) is not int
                or item.ordinal != ordinal or item.digest != identity.route_digests[ordinal]
                or type(item.target) is not str or not re.fullmatch(IQN, item.target)
                or type(item.initiator) is not str or not re.fullmatch(IQN, item.initiator)
                or not _uint(item.tpg, 65535, 1) or not _uint(item.mapped_lun, 255)
                or not _uint(item.tpg_lun, 65535) or not _uint(item.device, (1 << 64) - 1)
                or not _uint(item.inode, (1 << 64) - 1, 1) or not _uint(item.size, (1 << 63) - 1, 1)
                or type(item.serial) is not str or not re.fullmatch(r"[!-~]{1,255}", item.serial)
                or type(item.storage_path) is not str
                or not re.fullmatch(r"/sys/kernel/config/target/core/fileio_[0-9]+/[A-Za-z0-9_-][A-Za-z0-9_.-]{0,127}",
                                    item.storage_path)):
            raise TargetJournalError("TARGET_ROUTE_BINDING")
        _path(item.file_path)
        portal = _portal(item.portal, item.port)
        group = item.target, item.tpg, item.initiator
        key = group + portal + (item.mapped_lun,)
        if key in keys:
            raise TargetJournalError("TARGET_ROUTE_BINDING")
        keys.add(key)
        groups.setdefault(group, []).append(item)
    return groups


def _history(journal, identity):
    for state in journal.denied():
        old = state.identity
        if old.operation_id == identity.operation_id:
            immutable = ("guest_uuid", "mapping_generation", "protected_set_digest", "route_digests")
            if (identity.attempt < old.attempt
                    or (identity.attempt == old.attempt and identity != old)
                    or any(getattr(identity, key) != getattr(old, key) for key in immutable)):
                raise TargetJournalError("TARGET_JOURNAL_IDENTITY_DRIFT")
            if identity.target_boot_id == old.target_boot_id:
                return state.phases
        elif old.guest_uuid == identity.guest_uuid:
            raise TargetJournalError("TARGET_JOURNAL_OWNER_CONFLICT")
    return (0,) * len(identity.route_digests)


class _Census:
    def __init__(self):
        self.remaining = MAX_NATIVE_ITEMS

    def items(self, values):
        for value in values:
            self.remaining -= 1
            if self.remaining < 0:
                raise TargetJournalError("TARGET_INVENTORY_TOO_LARGE")
            yield value


def _tpg_ports(tpg, census):
    for field, expected in (("generate_node_acls", "0"), ("cache_dynamic_acls", "0"),
                            ("demo_mode_write_protect", "1")):
        if tpg.get_attribute(field).strip() != expected:
            raise TargetJournalError("TARGET_AUTOMATIC_ACCESS")
    if tpg.get_parameter("ErrorRecoveryLevel").strip() != "0":
        raise TargetJournalError("TARGET_TRANSPORT_UNSUPPORTED")
    ports = set()
    for portal in census.items(tpg.network_portals):
        key = _portal(portal.ip_address, portal.port)
        if (key in ports or portal.path != f"{tpg.path}/np/{key[0]}:{key[1]}"
                or portal.parent_tpg.path != tpg.path):
            raise TargetJournalError("TARGET_PORTAL_INVALID")
        # Do not use iser/offload properties: rtslib treats I/O errors as false.
        if any(_read_config(portal.path + "/" + field).strip() != "0"
               for field in ("iser", "cxgbit")):
            raise TargetJournalError("TARGET_TRANSPORT_UNSUPPORTED")
        ports.add(key)
    return ports


def _storage(item, storage):
    if (storage.plugin != "fileio" or storage.path != item.storage_path
            or storage.wwn != item.serial or storage.udev_path != item.file_path
            or storage.size != item.size):
        raise TargetJournalError("TARGET_BACKSTORE_IDENTITY")
    info = _read_config(storage.path + "/info")
    lines = re.findall(r"(?m)^[ \t]*(?:TCM FILEIO ID: [0-9]+[ \t]+)?File: (.+?) +Size: ([0-9]+)"
                       r" +Mode: (\S+) +Async: ([0-9]+)[ \t]*$", info)
    if (len(lines) != 1 or lines[0] != (item.file_path, str(item.size), "O_DSYNC", "0")
            or _read_config(storage.path + "/attrib/emulate_write_cache").strip() != "0"):
        raise TargetJournalError("TARGET_BACKSTORE_MODE")


def _mapped_link(path, expected):
    """A partially created mapped LUN may have no link, never a different one."""
    with os.scandir(path) as entries:
        links = [entry.name for entry in _Census().items(entries) if entry.is_symlink()]
    if not links:
        return None
    if len(links) != 1:
        raise TargetJournalError("TARGET_LUN_IDENTITY")
    target = os.readlink(path + "/" + links[0])
    if len(target) >= 4096 or os.path.normpath(os.path.join(path, target)) != expected:
        raise TargetJournalError("TARGET_LUN_IDENTITY")
    return links[0]


def _resolve_acl(tpg, ports, acl, items, phases, luns, census, *, revoked=False):
    first = items[0]
    path = tpg.path + "/acls/" + first.initiator
    ordinals = tuple(item.ordinal for item in items)
    completed = all(phases[n] >= 2 for n in ordinals)
    if completed:
        if acl is not None:
            raise TargetJournalError("TARGET_ACL_RECREATED")
        _require_absent(path)
        acl = _AbsentAcl(path)
    elif acl is None and not revoked:
        raise TargetJournalError("TARGET_DRAIN_UNPROVEN")
    elif acl is None:
        acl = _AbsentAcl(path)
    expected = {(item.portal, item.port, item.mapped_lun) for item in items}
    desired = {}
    for item in items:
        prior = desired.setdefault(item.mapped_lun, item.tpg_lun)
        if prior != item.tpg_lun:
            raise TargetJournalError("TARGET_LUN_IDENTITY")
    mapped = dict(desired) if completed else {}
    if not completed:
        for lun in census.items(acl.mapped_luns):
            if (not _uint(lun.mapped_lun, 255) or lun.mapped_lun in mapped
                    or lun.path != path + f"/lun_{lun.mapped_lun}"):
                raise TargetJournalError("TARGET_LUN_IDENTITY")
            if revoked:
                if lun.mapped_lun not in desired:
                    raise TargetJournalError("TARGET_LUN_IDENTITY")
                _mapped_link(lun.path, tpg.path + f"/lun/lun_{desired[lun.mapped_lun]}")
                mapped[lun.mapped_lun] = desired[lun.mapped_lun]
            else:
                if (not _uint(lun.tpg_lun.lun, 65535)
                        or lun.tpg_lun.path != tpg.path + f"/lun/lun_{lun.tpg_lun.lun}"):
                    raise TargetJournalError("TARGET_LUN_IDENTITY")
                mapped[lun.mapped_lun] = lun.tpg_lun.lun
    observed = {(ip, port, lun) for ip, port in ports for lun in (desired if revoked else mapped)}
    if observed != expected:
        raise TargetJournalError("TARGET_ROUTE_COVERAGE")
    for item in items:
        if ((desired if revoked else mapped).get(item.mapped_lun) != item.tpg_lun or item.tpg_lun not in luns):
            raise TargetJournalError("TARGET_LUN_IDENTITY")
        _storage(item, luns[item.tpg_lun].storage_object)
    return AclGroup(acl, path, tuple(path + f"/lun_{lun}" for lun in sorted(mapped)), ordinals)


def _native_inventory(root, groups, phases, *, revoked=False):
    census, found, targets = _Census(), {}, set()
    victims = {key[2] for key in groups}
    for target in census.items(root.targets):
        if (target.fabric_module.name != "iscsi" or type(target.wwn) is not str
                or not re.fullmatch(IQN, target.wwn) or target.wwn in targets
                or target.path != ISCSI + target.wwn):
            raise TargetJournalError("TARGET_NATIVE_TARGET")
        targets.add(target.wwn)
        tags = set()
        for tpg in census.items(target.tpgs):
            if (not _uint(tpg.tag, 65535, 1) or tpg.tag in tags
                    or tpg.path != target.path + f"/tpgt_{tpg.tag}"
                    or tpg.parent_target.path != target.path):
                raise TargetJournalError("TARGET_NATIVE_TPG")
            tags.add(tpg.tag)
            ports = _tpg_ports(tpg, census)
            acls = {}
            for acl in census.items(tpg.node_acls):
                if (type(acl.node_wwn) is not str or not re.fullmatch(IQN, acl.node_wwn)
                        or acl.node_wwn in acls or acl.parent_tpg.path != tpg.path
                        or acl.path != tpg.path + "/acls/" + acl.node_wwn):
                    raise TargetJournalError("TARGET_ACL_IDENTITY")
                acls[acl.node_wwn] = acl
                if acl.node_wwn in victims and (target.wwn, tpg.tag, acl.node_wwn) not in groups:
                    raise TargetJournalError("TARGET_ROUTE_COVERAGE")
            luns = {}
            for lun in census.items(tpg.luns):
                if (not _uint(lun.lun, 65535) or lun.lun in luns
                        or lun.path != tpg.path + f"/lun/lun_{lun.lun}"):
                    raise TargetJournalError("TARGET_LUN_IDENTITY")
                luns[lun.lun] = lun
            for key, items in groups.items():
                if key[:2] == (target.wwn, tpg.tag):
                    found[key] = _resolve_acl(tpg, ports, acls.get(key[2]), items, phases, luns, census,
                                              revoked=revoked)
    if found.keys() != groups.keys():
        raise TargetJournalError("TARGET_ROUTE_COVERAGE")
    return tuple(found[key] for key in groups)


def _pin_files(bindings, descriptors):
    files, identities = {}, {}
    for item in bindings:
        expected = item.device, item.inode, item.size
        if item.file_path not in files:
            fd = _open_path(item.file_path, os.O_RDWR)
            descriptors.append(fd)
            status = os.fstat(fd)
            if (not stat.S_ISREG(status.st_mode)
                    or (status.st_dev, status.st_ino, status.st_size) != expected):
                raise TargetJournalError("TARGET_BACKSTORE_IDENTITY")
            files[item.file_path] = expected
            key = item.device, item.inode
            if key in identities:
                os.close(fd)
                descriptors.remove(fd)
            else:
                identities[key] = fd, []
        elif files[item.file_path] != expected:
            raise TargetJournalError("TARGET_BACKSTORE_IDENTITY")
        identities[(item.device, item.inode)][1].append(item.ordinal)
    return tuple(FlushGroup(fd, key[0], key[1], tuple(ordinals))
                 for key, (fd, ordinals) in identities.items())


def resolve_routes(root, journal, identity, bindings):
    """Read-only census; close all pins on refusal. No RTSRoot construction."""
    descriptors = []
    try:
        groups = _bindings(identity, bindings)
        phases = _history(journal, identity)
        acls = _native_inventory(root, groups, phases)
        flushes = _pin_files(bindings, descriptors)
        return ResolvedRoutes(acls, flushes)
    except BaseException as error:
        for fd in descriptors:
            os.close(fd)
        if isinstance(error, TargetJournalError):
            raise
        if isinstance(error, Exception):
            # Native libraries can embed target/CHAP configuration in errors.
            raise TargetJournalError("TARGET_INVENTORY_UNPROVEN") from None
        raise


def resolve_rejoin_routes(root, journal, identity, bindings, *, present):
    """Exact active restore or revoked subset census, never isolation proof.

    Partial mappings are allowed only for an exact revoked rejoin lineage. The
    caller still needs its fresh native teardown boundary and backing flush.
    """
    descriptors = []
    try:
        state = journal._current_rejoin_owner(identity)
        if (type(present) is not bool or state.rejoin is None
                or state.rejoin.phase not in ((2, 3) if present else (4,))):
            raise TargetJournalError("TARGET_REJOIN_PHASE")
        groups = _bindings(identity, bindings)
        phases = (0,) * len(bindings) if present else state.phases
        acls = _native_inventory(root, groups, phases, revoked=not present)
        flushes = _pin_files(bindings, descriptors)
        return ResolvedRoutes(acls, flushes)
    except BaseException as error:
        for fd in descriptors:
            os.close(fd)
        if isinstance(error, TargetJournalError):
            raise
        if isinstance(error, Exception):
            raise TargetJournalError("TARGET_INVENTORY_UNPROVEN") from None
        raise
