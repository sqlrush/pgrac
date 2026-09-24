"""Reconcile existing operations while every registered target export is closed.

Author: SqlRush <sqlrush@gmail.com>
No export restore/enable, power, signing, admission or invented owner attempt.
"""

from contextlib import contextmanager
import os
import time

from target_admission import _enable_value
from target_drain import _pinned_file
from target_guest import observe_off
import target_inventory as native
from target_journal import TargetJournalError, _hex, _rejoin_intent, _uint
from target_operation import _kernel_boot_id
from target_permissions import _revoke_group
from target_startup import _closed_template, _layout


def _fresh(boot, deadline):
    if (not _hex(boot, 32) or not _uint(deadline) or time.monotonic_ns() >= deadline
            or _kernel_boot_id() != boot):
        raise TargetJournalError("TARGET_CLOSED_ENVELOPE")


@contextmanager
def closed_inventory(registry, journal, root, config, boot, deadline):
    """Whole closed namespace and pinned files; missing survivors still refuse.

    Only durable denied guests may have absent/partial mappings. Even for them,
    enumeration is not drain proof; the exact serialized syscall must follow.
    The caller exclusively owns all target configuration across this scope.
    """
    descriptors = []
    try:
        _fresh(boot, deadline)
        tpgs, stores = _layout(registry, journal)
        _closed_template(config, tpgs, stores)
        snapshot = journal.sequence, journal.digest
        denied = {s.identity.guest_uuid: s for s in journal.denied()}
        census, targets, found, storage_paths = native._Census(), set(), set(), set()
        for storage in census.items(root.storage_objects):
            if storage.path not in stores or storage.path in storage_paths:
                raise TargetJournalError("TARGET_CLOSED_STORAGE")
            storage_paths.add(storage.path)
        if storage_paths != stores.keys():
            raise TargetJournalError("TARGET_CLOSED_STORAGE")
        for target in census.items(root.targets):
            if target.wwn in targets or target.wwn not in {key[0] for key in tpgs}:
                raise TargetJournalError("TARGET_CLOSED_TARGET")
            targets.add(target.wwn)
            for tpg in census.items(target.tpgs):
                key = target.wwn, tpg.tag
                if key not in tpgs or key in found:
                    raise TargetJournalError("TARGET_CLOSED_TPG")
                found.add(key)
                _enable_value(tpg, False)
                acls = [acl.node_wwn for acl in census.items(tpg.node_acls)]
                allowed = {b.initiator for b in tpgs[key]}
                required = {b.initiator for n in registry.nodes if n.mapping.guest_uuid not in denied
                            for b in n.bindings if (b.target, b.tpg) == key}
                if len(acls) != len(set(acls)) or not required <= set(acls) <= allowed:
                    raise TargetJournalError("TARGET_CLOSED_ACL")
                luns = [lun.lun for lun in census.items(tpg.luns)]
                if len(luns) != len(set(luns)) or set(luns) != {b.tpg_lun for b in tpgs[key]}:
                    raise TargetJournalError("TARGET_CLOSED_LUN")
        if found != tpgs.keys():
            raise TargetJournalError("TARGET_CLOSED_TPG")
        resolved = {}
        for node in registry.nodes:
            groups = {}
            for binding in node.bindings:
                groups.setdefault((binding.target, binding.tpg, binding.initiator), []).append(binding)
            state = denied.get(node.mapping.guest_uuid)
            phases = (0,) * len(node.bindings)
            if (state is not None and state.identity.target_boot_id == boot
                    and (state.rejoin is None or state.rejoin.phase == 4)):
                phases = state.phases
            acls = native._native_inventory(root, groups, phases, revoked=state is not None)
            flushes = native._pin_files(node.bindings, descriptors)
            resolved[node.mapping.node_id] = acls, flushes
        _fresh(boot, deadline)
        if (journal.sequence, journal.digest) != snapshot:
            raise TargetJournalError("TARGET_CLOSED_CHANGED")
        yield resolved
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_CLOSED_UNPROVEN") from None
    finally:
        for fd in descriptors:
            os.close(fd)


def _binding(registry, identity, node_id, intent):
    node = registry.node(node_id)
    expected = registry.drain_identity(node_id, identity.operation_id, identity.attempt,
                                       identity.daemon_boot_id, identity.target_boot_id)
    if identity != expected:
        raise TargetJournalError("TARGET_CLOSED_IDENTITY")
    if intent is not None:
        _rejoin_intent(intent)
        if intent.system_identifier != node.mapping.system_identifier or intent.old_node_id != node_id:
            raise TargetJournalError("TARGET_CLOSED_IDENTITY")
    return node


def _prepared(journal, identity, intent):
    state = journal._current_rejoin_owner(identity)
    if ((state.rejoin is None and intent is not None)
            or (state.rejoin is not None and (state.rejoin.phase != 4 or state.rejoin.intent != intent))):
        raise TargetJournalError("TARGET_CLOSED_REVOKE_REQUIRED")
    return state


def prepare_closed_deny(registry, journal, root, identity, node_id, config, deadline, *, intent=None):
    """Exact authenticated C owner prepares before its Pacemaker OFF command."""
    _binding(registry, identity, node_id, intent)
    with closed_inventory(registry, journal, root, config, identity.target_boot_id, deadline):
        state = journal.states.get(identity.operation_id)
        if state is not None and state.rejoin is not None and state.rejoin.intent != intent:
            raise TargetJournalError("TARGET_CLOSED_REVOKE_REQUIRED")
        if state is not None and state.identity.target_boot_id != identity.target_boot_id:
            journal.reconcile_boot(identity, intent=intent)
        elif intent is not None:
            journal.revoke_rejoin(identity, intent)
        else:
            journal.deny(identity)
        _prepared(journal, identity, intent)
        _fresh(identity.target_boot_id, deadline)


def complete_closed_drain(registry, journal, root, connection, identity, node_id, config, deadline, *, intent=None):
    """Fresh OFF/serialized teardown/flush; returned phases are uncertified.

    Restart or interrupted partial removal never creates an ACL to manufacture
    a teardown event. All exports remain closed through the real syscall edge.
    """
    try:
        node = _binding(registry, identity, node_id, intent)
        _fresh(identity.target_boot_id, deadline)
        _prepared(journal, identity, intent)
        observe_off(connection, node.mapping, deadline)
        with closed_inventory(registry, journal, root, config, identity.target_boot_id, deadline) as routes:
            acls, flushes = routes[node_id]
            def check():
                _fresh(identity.target_boot_id, deadline)
                _prepared(journal, identity, intent)
                for target in native._Census().items(root.targets):
                    for tpg in native._Census().items(target.tpgs):
                        _enable_value(tpg, False)
            for group in acls:
                check()
                _revoke_group(journal, identity, group, node.bindings, check)
            for group in flushes:
                check()
                _pinned_file(group)
                phases = _prepared(journal, identity, intent).phases
                if any(phases[n] < 2 for n in group.ordinals):
                    raise TargetJournalError("TARGET_DRAIN_INCOMPLETE")
                if any(phases[n] < 3 for n in group.ordinals):
                    os.fsync(group.fd)
                    check()
                    for n in group.ordinals:
                        if phases[n] == 2:
                            journal.advance(identity.operation_id, identity.target_boot_id, n, 3)
            with closed_inventory(registry, journal, root, config, identity.target_boot_id, deadline):
                observe_off(connection, node.mapping, deadline)
                check()
                if not journal.completion_recorded(identity.operation_id, identity.target_boot_id):
                    raise TargetJournalError("TARGET_DRAIN_INCOMPLETE")
                return _prepared(journal, identity, intent).phases
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_CLOSED_UNPROVEN") from None
