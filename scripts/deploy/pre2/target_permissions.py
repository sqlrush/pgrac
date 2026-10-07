"""Exact native storage permissions for an already authorized rejoin owner.

Author: SqlRush <sqlrush@gmail.com>
No power action, admission certificate or membership decision is made here.
Caller exclusively owns target configuration and prevents unsolicited guest ON.
"""

from dataclasses import dataclass
import os
import stat
import time

from target_drain import _pinned_file, _require_absent
from target_guest import GuestOffObservation, observe_off
from target_inventory import (_Census, _mapped_link, _open_path,
                              resolve_rejoin_routes, resolve_routes)
from target_journal import (DrainIdentity, RejoinIntent, TargetJournal,
                            TargetJournalError, _rejoin_intent)
from target_operation import _kernel_boot_id
from target_registry import TargetRegistry
from target_startup import _closed_template, _layout

AUTH_FIELDS = ("chap_userid", "chap_password", "chap_mutual_userid", "chap_mutual_password")


@dataclass(frozen=True)
class RejoinPermissionObservation:
    identity: DrainIdentity
    intent: RejoinIntent
    guest: GuestOffObservation
    phase: int


def _guard(registry, journal, identity, intent, deadline):
    if (type(registry) is not TargetRegistry or type(journal) is not TargetJournal
            or registry.inventory_digest != journal.inventory
            or type(deadline) is not int or not 0 < deadline < 1 << 64
            or time.monotonic_ns() >= deadline):
        raise TargetJournalError("TARGET_REJOIN_ENVELOPE")
    _rejoin_intent(intent)
    state = journal._current_rejoin_owner(identity)
    node = registry.node(intent.old_node_id)
    expected = registry.drain_identity(intent.old_node_id, identity.operation_id, identity.attempt,
                                       identity.daemon_boot_id, identity.target_boot_id)
    if (state.rejoin is None or state.rejoin.intent != intent or expected != identity
            or node.mapping.system_identifier != intent.system_identifier
            or _kernel_boot_id() != identity.target_boot_id):
        raise TargetJournalError("TARGET_REJOIN_IDENTITY")
    return node, state


def _access_template(registry, journal, root, node, config):
    tpgs, stores = _layout(registry, journal)
    template = _closed_template(config, tpgs, stores)
    wanted = {(b.target, b.tpg, b.initiator) for b in node.bindings}
    configs = {(target["wwn"], tpg["tag"]): tpg for target in template["targets"] for tpg in target["tpgs"]}
    result, census = [], _Census()
    for target in census.items(root.targets):
        for tpg in census.items(target.tpgs):
            for key in sorted(wanted):
                if key[:2] != (target.wwn, tpg.tag):
                    continue
                expected = configs[key[:2]]
                if tpg.get_attribute("authentication").strip() != str(expected["attributes"]["authentication"]):
                    raise TargetJournalError("TARGET_REJOIN_AUTH")
                acl = next(a for a in expected["node_acls"] if a["node_wwn"] == key[2])
                result.append((tpg, acl))
    if len(result) != len(wanted):
        raise TargetJournalError("TARGET_ROUTE_COVERAGE")
    return result


def _create_acl(tpg, initiator):
    from rtslib_fb import NodeACL
    return NodeACL(tpg, initiator, mode="create")


def _create_lun(acl, index, tpg_lun):
    from rtslib_fb import MappedLUN
    return MappedLUN(acl, index, tpg_lun, write_protect=False)


def _auth(acl, expected, *, configure):
    for name in AUTH_FIELDS:
        value = expected.get(name, "")
        if configure and getattr(acl, name) != value:
            setattr(acl, name, value)
        if getattr(acl, name) != value:
            raise TargetJournalError("TARGET_REJOIN_AUTH")


def _access_readback(root, journal, identity, node, template):
    with resolve_rejoin_routes(root, journal, identity, node.bindings, present=True):
        for tpg, expected in template:
            acl = next(a for a in tpg.node_acls if a.node_wwn == expected["node_wwn"])
            _auth(acl, expected, configure=False)
            for lun in acl.mapped_luns:
                if lun.write_protect is not False:
                    raise TargetJournalError("TARGET_REJOIN_PERMISSION")


def restore_rejoin_access(registry, journal, root, connection, identity, intent, config, deadline):
    """OFF-only permission restore. An uncertain RESTORING must be revoked.

    The C coordinator must have invalidated old admissions and reserved the new
    incarnation before arming this intent. This function cannot authorize ON.
    """
    mutated = False
    try:
        node, state = _guard(registry, journal, identity, intent, deadline)
        if state.rejoin.phase == 2:
            journal.advance_rejoin(identity, intent, 4)
            raise TargetJournalError("TARGET_REJOIN_REVOKE_REQUIRED")
        if state.rejoin.phase not in (1, 3) or state.rejoin.refresh_started:
            raise TargetJournalError("TARGET_REJOIN_PHASE")
        template = _access_template(registry, journal, root, node, config)
        observe_off(connection, node.mapping, deadline)
        if state.rejoin.phase == 1:
            with resolve_routes(root, journal, identity, node.bindings):
                _guard(registry, journal, identity, intent, deadline)
                journal.advance_rejoin(identity, intent, 2)
                mutated = True
                def check_restoring():
                    _node, current = _guard(registry, journal, identity, intent, deadline)
                    if current.rejoin.phase != 2:
                        raise TargetJournalError("TARGET_REJOIN_PHASE")
                for tpg, expected in template:
                    check_restoring()
                    acl = _create_acl(tpg, expected["node_wwn"])
                    check_restoring()
                    if acl.path != tpg.path + "/acls/" + expected["node_wwn"]:
                        raise TargetJournalError("TARGET_ACL_IDENTITY")
                    _auth(acl, expected, configure=True)
                    for lun in expected["mapped_luns"]:
                        check_restoring()
                        _create_lun(acl, lun["index"], lun["tpg_lun"])
                        check_restoring()
        _access_readback(root, journal, identity, node, template)
        guest = observe_off(connection, node.mapping, deadline)
        _guard(registry, journal, identity, intent, deadline)
        journal.advance_rejoin(identity, intent, 3)
        _guard(registry, journal, identity, intent, deadline)
        return RejoinPermissionObservation(identity, intent, guest, 3)
    except Exception as error:
        if mutated:
            try:
                journal.advance_rejoin(identity, intent, 4)
            except Exception:
                # A failed/poisoned append must not permit any later action.
                raise TargetJournalError("TARGET_REJOIN_RECONCILE_REQUIRED") from None
        if isinstance(error, TargetJournalError):
            raise error from None
        raise TargetJournalError("TARGET_REJOIN_UNPROVEN") from None


def _open_acl_parent(path):
    fd = _open_path(path.rsplit("/", 1)[0], os.O_RDONLY | os.O_DIRECTORY)
    try:
        initial = os.fstat(fd)
        if not stat.S_ISDIR(initial.st_mode):
            raise TargetJournalError("TARGET_ACL_IDENTITY")
        return fd, initial
    except BaseException:
        os.close(fd)
        raise


def _same_parent(path, fd, initial):
    current = os.fstat(fd)
    fresh = _open_path(path.rsplit("/", 1)[0], os.O_RDONLY | os.O_DIRECTORY)
    try:
        linked = os.fstat(fresh)
        expected = initial.st_dev, initial.st_ino
        if (current.st_dev, current.st_ino) != expected or (linked.st_dev, linked.st_ino) != expected:
            raise TargetJournalError("TARGET_ACL_IDENTITY")
    finally:
        os.close(fresh)


def _remove_mapped_lun(path, expected):
    link = _mapped_link(path, expected)
    if link is not None:
        os.unlink(path + "/" + link)
    # Includes an interrupted native constructor with no link yet installed.
    os.rmdir(path)


def _rmdir_acl(name, parent_fd):
    # The selected Linux do_rmdir holds this parent inode lock through configfs
    # ACL release. Even ENOENT is serialized behind an earlier dropping ACL;
    # directory enumeration/NodeACL.delete's absent no-op has no such boundary.
    os.rmdir(name, dir_fd=parent_fd)


def _revoke_group(journal, identity, group, bindings, check):
    phases = journal._current_rejoin_owner(identity).phases
    if all(phases[n] >= 2 for n in group.ordinals):
        _require_absent(group.path)
        return
    expected = {f"lun_{b.mapped_lun}": f"{group.path.rsplit('/acls/', 1)[0]}/lun/lun_{b.tpg_lun}"
                for b in bindings if b.ordinal in group.ordinals}
    fd, initial = _open_acl_parent(group.path)
    try:
        for n in group.ordinals:
            if phases[n] == 0:
                journal.advance(identity.operation_id, identity.target_boot_id, n, 1)
        check()
        _same_parent(group.path, fd, initial)
        for path in group.mapped_lun_paths:
            _remove_mapped_lun(path, expected[path.rsplit("/", 1)[1]])
        check()
        try:
            _rmdir_acl(group.path.rsplit("/", 1)[1], fd)
        except FileNotFoundError:
            # Only exact current-boot REVOKED lineage reaches here. Never used
            # by generic drain, and never inferred from .exists/session count.
            pass
        _same_parent(group.path, fd, initial)
        _require_absent(group.path)
        check()
        for n in group.ordinals:
            if phases[n] < 2:
                journal.advance(identity.operation_id, identity.target_boot_id, n, 2)
    finally:
        os.close(fd)


def revoke_rejoin_access(registry, journal, root, connection, identity, intent, deadline):
    """Durable revocation, native OFF, exact teardown, flush and fresh readback.

    No compensation power command here: ON leaves REVOKED and unproven so the
    existing coordinator can issue OFF before retrying this same obligation.
    """
    try:
        node, _state = _guard(registry, journal, identity, intent, deadline)
        journal.advance_rejoin(identity, intent, 4)
        observe_off(connection, node.mapping, deadline)
        def check():
            _node, state = _guard(registry, journal, identity, intent, deadline)
            if state.rejoin.phase != 4:
                raise TargetJournalError("TARGET_REJOIN_PHASE")
        with resolve_rejoin_routes(root, journal, identity, node.bindings, present=False) as resolved:
            for group in resolved.acl_groups:
                _revoke_group(journal, identity, group, node.bindings, check)
            for group in resolved.flush_groups:
                check()
                _pinned_file(group)
                phases = journal._current_rejoin_owner(identity).phases
                if any(phases[n] < 2 for n in group.ordinals):
                    raise TargetJournalError("TARGET_DRAIN_INCOMPLETE")
                if any(phases[n] < 3 for n in group.ordinals):
                    os.fsync(group.fd)
                    check()
                    for n in group.ordinals:
                        if phases[n] == 2:
                            journal.advance(identity.operation_id, identity.target_boot_id, n, 3)
            with resolve_rejoin_routes(root, journal, identity, node.bindings, present=False):
                guest = observe_off(connection, node.mapping, deadline)
                check()
                if not journal.completion_recorded(identity.operation_id, identity.target_boot_id):
                    raise TargetJournalError("TARGET_DRAIN_INCOMPLETE")
                return RejoinPermissionObservation(identity, intent, guest, 4)
    except TargetJournalError as error:
        raise error from None
    except Exception:
        raise TargetJournalError("TARGET_REJOIN_UNPROVEN") from None
