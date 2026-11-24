"""Storage-only LIO denial for an authenticated Lima target owner.

Author: SqlRush <sqlrush@gmail.com>
No management transport, power action, export enable or certificate is provided.
"""

from dataclasses import dataclass
import time

from target_drain import drain_routes
from target_inventory import resolve_routes
from target_journal import DrainIdentity, TargetJournal, TargetJournalError, _hex, _identity
from target_native import lio_native_handles


@dataclass(frozen=True)
class StorageDenyObservation:
    identity: DrainIdentity
    phases: tuple


def deny_storage(journal, inventory_digest, identity, bindings, deadline_mono_ns):
    """Drain the exact authenticated routes under exclusive configuration.

    The caller owns mapping authentication and the bounded management worker.
    This observation covers neither guest OFF nor old-writer I/O rejection.
    All failure paths retain denial obligations and never restore access.
    """
    try:
        _identity(identity)
        if (type(journal) is not TargetJournal or not _hex(inventory_digest, 64)
                or journal.inventory != inventory_digest
                or type(deadline_mono_ns) is not int or not 0 < deadline_mono_ns < 1 << 64
                or time.monotonic_ns() >= deadline_mono_ns):
            raise TargetJournalError("TARGET_STORAGE_IDENTITY")
        with lio_native_handles(identity.target_boot_id) as root:
            with resolve_routes(root, journal, identity, bindings) as resolved:
                phases = drain_routes(journal, identity, resolved.acl_groups, resolved.flush_groups)
            # The previous census cannot account for a newly exposed route or
            # automatic ACL. Read the full native inventory again after flush.
            with resolve_routes(root, journal, identity, bindings):
                if (phases != (3,) * len(identity.route_digests)
                        or not journal.completion_recorded(identity.operation_id, identity.target_boot_id)
                        or time.monotonic_ns() >= deadline_mono_ns):
                    raise TargetJournalError("TARGET_STORAGE_INCOMPLETE")
                observation = StorageDenyObservation(identity, phases)
        # Native handle exit revalidates the same configfs and kernel boot.
        if time.monotonic_ns() >= deadline_mono_ns:
            raise TargetJournalError("TARGET_STORAGE_INCOMPLETE")
        return observation
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_STORAGE_UNPROVEN") from None
