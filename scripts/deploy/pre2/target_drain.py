"""Ordered LIO teardown and flush using already resolved, pinned route handles.

Author: SqlRush <sqlrush@gmail.com>

This internal executor is not a CLI or a certificate producer. Its caller owns
the authenticated mapping, exclusive target configuration and OFF/rejoin gates.
"""

from dataclasses import dataclass
import os
import re
import stat

from target_journal import DrainIdentity, TargetJournalError


@dataclass(frozen=True)
class AclGroup:
    acl: object
    path: str
    mapped_lun_paths: tuple
    ordinals: tuple


@dataclass(frozen=True)
class FlushGroup:
    fd: int
    device: int
    inode: int
    ordinals: tuple


def _coverage(groups, group_type, count):
    if type(groups) is not tuple or not 0 < len(groups) <= count:
        raise TargetJournalError("TARGET_ROUTE_COVERAGE")
    seen = set()
    for group in groups:
        if (type(group) is not group_type or type(group.ordinals) is not tuple
                or not 0 < len(group.ordinals) <= count):
            raise TargetJournalError("TARGET_ROUTE_COVERAGE")
        for ordinal in group.ordinals:
            if type(ordinal) is not int or not 0 <= ordinal < count or ordinal in seen:
                raise TargetJournalError("TARGET_ROUTE_COVERAGE")
            seen.add(ordinal)
    if seen != set(range(count)):
        raise TargetJournalError("TARGET_ROUTE_COVERAGE")


def _phases(journal, identity):
    for state in journal.denied():
        if (state.identity.operation_id == identity.operation_id
                and state.identity.target_boot_id == identity.target_boot_id):
            return state.phases
    return (0,) * len(identity.route_digests)


def _require_absent(path):
    try:
        os.lstat(path)
    except FileNotFoundError:
        return
    raise TargetJournalError("TARGET_ACL_RECREATED")


def _pinned_file(group):
    if (type(group.fd) is not int or group.fd < 0
            or type(group.device) is not int or type(group.inode) is not int):
        raise TargetJournalError("TARGET_BACKSTORE_IDENTITY")
    info = os.fstat(group.fd)
    if (not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != (group.device, group.inode)):
        raise TargetJournalError("TARGET_BACKSTORE_IDENTITY")


def _acl_snapshot(group, phases):
    # No caller-supplied path outside the selected native iSCSI ACL namespace.
    iqn = r"iqn\.[a-z0-9][a-z0-9.:-]{1,218}"
    pattern = rf"/sys/kernel/config/target/iscsi/{iqn}/tpgt_([1-9][0-9]{{0,4}})/acls/{iqn}"
    match = re.fullmatch(pattern, group.path) if type(group.path) is str else None
    if (not match or int(match.group(1)) > 65535 or group.acl.path != group.path
            or type(group.mapped_lun_paths) is not tuple
            or not 0 < len(group.mapped_lun_paths) <= 128
            or len(set(group.mapped_lun_paths)) != len(group.mapped_lun_paths)):
        raise TargetJournalError("TARGET_ACL_IDENTITY")
    for path in group.mapped_lun_paths:
        suffix = path.removeprefix(group.path + "/") if type(path) is str else ""
        if not re.fullmatch(r"lun_(0|[1-9][0-9]{0,4})", suffix) or int(suffix[4:]) > 16383:
            raise TargetJournalError("TARGET_LUN_IDENTITY")
    if all(phases[n] >= 2 for n in group.ordinals):
        _require_absent(group.path)
        return ()
    luns = tuple(group.acl.mapped_luns)
    if (len(luns) != len(group.mapped_lun_paths)
            or sorted(lun.path for lun in luns) != sorted(group.mapped_lun_paths)):
        raise TargetJournalError("TARGET_LUN_IDENTITY")
    return luns


def _teardown(journal, identity, group, luns):
    phases = _phases(journal, identity)
    if all(phases[n] >= 2 for n in group.ordinals):
        _require_absent(group.path)
        return
    for ordinal in group.ordinals:
        if phases[ordinal] == 0:
            journal.advance(identity.operation_id, identity.target_boot_id, ordinal, 1)
    # Revalidate before the first mutation; no ignored .exists/no-op completion.
    if group.acl.path != group.path or sorted(lun.path for lun in luns) != sorted(
            lun.path for lun in group.acl.mapped_luns):
        raise TargetJournalError("TARGET_LUN_IDENTITY")
    for lun in luns:
        lun.delete()
    # Unlike CFSNode.delete(), this cannot silently succeed when the ACL is absent.
    # Selected-kernel configfs release synchronously waits for accepted commands.
    os.rmdir(group.path)
    for ordinal in group.ordinals:
        if phases[ordinal] < 2:
            journal.advance(identity.operation_id, identity.target_boot_id, ordinal, 2)


def drain_routes(journal, identity, acl_groups, flush_groups):
    """Return recorded phases, not OFF/proof. No cleanup or re-enable on failure.

    The authenticated resolver must supply complete groups from the selected
    synchronous fileio profile, pinned open backstore files and native NodeACLs.
    Missing ACLs are acceptable only with this boot's durable completed teardown.
    """
    if type(identity) is not DrainIdentity or not 0 < len(identity.route_digests) <= 128:
        raise TargetJournalError("TARGET_IDENTITY_INVALID")
    count = len(identity.route_digests)
    _coverage(acl_groups, AclGroup, count)
    _coverage(flush_groups, FlushGroup, count)
    if (any(type(g.path) is not str for g in acl_groups)
            or len({g.path for g in acl_groups}) != len(acl_groups)):
        raise TargetJournalError("TARGET_ACL_IDENTITY")
    try:
        phases = _phases(journal, identity)
        snapshots = [_acl_snapshot(group, phases) for group in acl_groups]
        for group in flush_groups:
            _pinned_file(group)
        journal.deny(identity)
        for group, luns in zip(acl_groups, snapshots):
            _teardown(journal, identity, group, luns)
        for group in flush_groups:
            _pinned_file(group)
            phases = _phases(journal, identity)
            if any(phases[n] < 2 for n in group.ordinals):
                raise TargetJournalError("TARGET_DRAIN_INCOMPLETE")
            if any(phases[n] < 3 for n in group.ordinals):
                os.fsync(group.fd)
                for ordinal in group.ordinals:
                    if phases[ordinal] == 2:
                        journal.advance(identity.operation_id, identity.target_boot_id, ordinal, 3)
        for group in acl_groups:
            _require_absent(group.path)
        return _phases(journal, identity)
    except TargetJournalError:
        raise
    except Exception:
        # Native APIs may put paths/credentials into exceptions. Do not expose them.
        raise TargetJournalError("TARGET_DRAIN_UNPROVEN") from None
