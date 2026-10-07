"""Open staged target exports without clearing any denied guest.

Author: SqlRush <sqlrush@gmail.com>
This internal startup boundary is not REJOIN, a power action or a certificate.
"""

from dataclasses import dataclass
import os
import time

import target_inventory as native
from target_journal import TargetJournalError
from target_operation import _kernel_boot_id
from target_startup import ClosedTargetStage, _layout


@dataclass(frozen=True)
class TargetStartupObservation:
    target_boot_id: str
    inventory_digest: str
    journal_sequence: int
    journal_digest: str
    tpg_keys: tuple
    excluded_guests: tuple
    observed_mono_ns: int


def _enable_value(tpg, expected):
    # rtslib treats missing enable as true; the selected iSCSI profile must
    # instead read the actual attribute and propagate any missing/I/O failure.
    if native._read_config(tpg.path + "/enable").strip() != str(int(expected)):
        raise TargetJournalError("TARGET_ADMISSION_ENABLE_UNPROVEN")


def _denied(journal, boot):
    result = {}
    for state in journal.denied():
        if (state.identity.target_boot_id != boot
                or not journal.completion_recorded(state.identity.operation_id, boot)
                or state.identity.guest_uuid in result):
            raise TargetJournalError("TARGET_ADMISSION_DRAIN_UNPROVEN")
        result[state.identity.guest_uuid] = state
    return result


def _inventory(registry, root, tpgs, stores, denied, enabled):
    census, targets, found, storage_paths = native._Census(), set(), {}, set()
    for storage in census.items(root.storage_objects):
        if storage.path not in stores or storage.path in storage_paths:
            raise TargetJournalError("TARGET_ADMISSION_STORAGE")
        storage_paths.add(storage.path)
    if storage_paths != stores.keys():
        raise TargetJournalError("TARGET_ADMISSION_STORAGE")
    for target in census.items(root.targets):
        if target.wwn in targets or target.wwn not in {key[0] for key in tpgs}:
            raise TargetJournalError("TARGET_ADMISSION_TARGET")
        targets.add(target.wwn)
        for tpg in census.items(target.tpgs):
            key = target.wwn, tpg.tag
            if key not in tpgs or key in found:
                raise TargetJournalError("TARGET_ADMISSION_TPG")
            _enable_value(tpg, enabled)
            found[key] = tpg
            actual = [acl.node_wwn for acl in census.items(tpg.node_acls)]
            expected = {b.initiator for node in registry.nodes if node.mapping.guest_uuid not in denied
                        for b in node.bindings if (b.target, b.tpg) == key}
            if len(actual) != len(expected) or set(actual) != expected:
                raise TargetJournalError("TARGET_ADMISSION_ACL")
            luns = [lun.lun for lun in census.items(tpg.luns)]
            expected_luns = {b.tpg_lun for b in tpgs[key]}
            if len(luns) != len(expected_luns) or set(luns) != expected_luns:
                raise TargetJournalError("TARGET_ADMISSION_LUN")
    if found.keys() != tpgs.keys():
        raise TargetJournalError("TARGET_ADMISSION_TPG")
    for node in registry.nodes:
        groups = {}
        for b in node.bindings:
            groups.setdefault((b.target, b.tpg, b.initiator), []).append(b)
        state = denied.get(node.mapping.guest_uuid)
        native._native_inventory(root, groups, state.phases if state else (0,) * len(node.bindings))
    return tuple(found[key] for key in sorted(found))


def _reclose(tpgs):
    complete = True
    for tpg in tpgs:
        try:
            tpg.enable = False
        except Exception:
            complete = False
    for tpg in tpgs:
        try:
            _enable_value(tpg, False)
        except Exception:
            complete = False
    return complete


def _unchanged(journal, snapshot, boot, deadline):
    journal.denied()
    if ((journal.sequence, journal.digest) != snapshot or _kernel_boot_id() != boot
            or time.monotonic_ns() >= deadline):
        raise TargetJournalError("TARGET_ADMISSION_CHANGED")


def open_staged_exports(registry, journal, root, stage, deadline_mono_ns):
    """Enable exact staged TPGs only after every old denied route is drained.

    Caller owns target configuration throughout. This does not remove DENY,
    authorize ON, recreate ACLs or substitute for a fresh isolation proof.
    """
    descriptors, tpg_handles, changed = [], (), False
    try:
        if (type(stage) is not ClosedTargetStage or type(deadline_mono_ns) is not int
                or not 0 < deadline_mono_ns < 1 << 64 or time.monotonic_ns() >= deadline_mono_ns):
            raise TargetJournalError("TARGET_ADMISSION_ARGUMENT")
        tpgs, stores = _layout(registry, journal)
        boot = _kernel_boot_id()
        if (stage.target_boot_id != boot or stage.inventory_digest != registry.inventory_digest
                or stage.tpg_keys != tuple(sorted(tpgs)) or stage.journal_sequence > journal.sequence
                or (stage.journal_sequence == journal.sequence and stage.journal_digest != journal.digest)):
            raise TargetJournalError("TARGET_ADMISSION_STAGE")
        denied = _denied(journal, boot)
        snapshot = journal.sequence, journal.digest
        tpg_handles = _inventory(registry, root, tpgs, stores, denied, False)
        for node in registry.nodes:
            native._pin_files(node.bindings, descriptors)
        _unchanged(journal, snapshot, boot, deadline_mono_ns)
        changed = True
        for tpg in tpg_handles:
            tpg.enable = True
            _enable_value(tpg, True)
            _unchanged(journal, snapshot, boot, deadline_mono_ns)
        _inventory(registry, root, tpgs, stores, denied, True)
        for node in registry.nodes:
            native._pin_files(node.bindings, descriptors)
        _unchanged(journal, snapshot, boot, deadline_mono_ns)
        return TargetStartupObservation(boot, registry.inventory_digest, snapshot[0], snapshot[1],
                                        stage.tpg_keys, tuple(sorted(denied)), time.monotonic_ns())
    except BaseException as error:
        if changed and not _reclose(tpg_handles):
            raise TargetJournalError("TARGET_ADMISSION_ROLLBACK_UNPROVEN") from None
        if isinstance(error, TargetJournalError):
            raise error from None
        if isinstance(error, Exception):
            raise TargetJournalError("TARGET_ADMISSION_UNPROVEN") from None
        raise
    finally:
        for fd in descriptors:
            os.close(fd)
