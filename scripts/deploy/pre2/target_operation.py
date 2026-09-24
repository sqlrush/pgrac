"""Join authenticated identity, native OFF, exact route drain and final readback.

Author: SqlRush <sqlrush@gmail.com>
No management transport, power action, export enable or certificate is provided.
"""

from dataclasses import dataclass
import os
import re
import time
import uuid

from target_drain import drain_routes
from target_guest import GuestOffObservation, observe_off
from target_inventory import _open_path, resolve_routes
from target_journal import DrainIdentity, TargetJournal, TargetJournalError, _hex
from target_registry import TargetRegistry


@dataclass(frozen=True)
class OffDrainCompletion:
    identity: DrainIdentity
    challenge: str
    guest: GuestOffObservation
    phases: tuple


def complete_off_drain(registry, journal, root, connection, node_id, operation_id,
                       attempt, daemon_boot_id, target_boot_id, challenge, deadline_mono_ns):
    """Run in the authenticated owner's bounded worker with exclusive config.

    This does not establish management policy/unsolicited-ON exclusion. A signer
    must additionally verify that policy and export-start deny enforcement.
    A failed call never clears obligations or re-creates an ACL.
    """
    try:
        if (type(registry) is not TargetRegistry or type(journal) is not TargetJournal
                or journal.inventory != registry.inventory_digest
                or not _hex(challenge, 32) or not _hex(target_boot_id, 32)
                or type(deadline_mono_ns) is not int or not 0 < deadline_mono_ns < 1 << 64
                or time.monotonic_ns() >= deadline_mono_ns
                or _kernel_boot_id() != target_boot_id):
            raise TargetJournalError("TARGET_OPERATION_IDENTITY")
        node = registry.node(node_id)
        identity = registry.drain_identity(node_id, operation_id, attempt, daemon_boot_id, target_boot_id)
        with resolve_routes(root, journal, identity, node.bindings) as resolved:
            # Durable obligation precedes all physical action. Native OFF is a
            # separate prerequisite, not inferred from this durable record.
            journal.deny(identity)
            observe_off(connection, node.mapping, deadline_mono_ns)
            phases = drain_routes(journal, identity, resolved.acl_groups, resolved.flush_groups)
            guest = observe_off(connection, node.mapping, deadline_mono_ns)
            # Re-enumerate after drain: a new route or automatic ACL cannot be
            # covered by the old census, even if all its old phases are FLUSHED.
            with resolve_routes(root, journal, identity, node.bindings):
                if (phases != (3,) * len(identity.route_digests)
                        or not journal.completion_recorded(operation_id, target_boot_id)
                        or _kernel_boot_id() != target_boot_id
                        or time.monotonic_ns() >= deadline_mono_ns):
                    raise TargetJournalError("TARGET_OPERATION_INCOMPLETE")
                return OffDrainCompletion(identity, challenge, guest, phases)
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_OPERATION_UNPROVEN") from None


def _kernel_boot_id():
    fd = _open_path("/proc/sys/kernel/random/boot_id", os.O_RDONLY)
    try:
        raw = os.read(fd, 38)
        if not re.fullmatch(rb"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\n", raw):
            raise TargetJournalError("TARGET_OPERATION_BOOT")
        value = uuid.UUID(raw[:-1].decode("ascii")).hex
        if not _hex(value, 32):
            raise TargetJournalError("TARGET_OPERATION_BOOT")
        return value
    finally:
        os.close(fd)
