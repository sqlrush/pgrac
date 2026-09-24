"""Exact-guest readback only, never a power action or isolation certificate.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import dataclass
import re
import time
import uuid
import xml.etree.ElementTree as ET

from target_journal import TargetJournalError, _hex
from target_mapping import ProtectedMap

LIBVIRT_VERSION = 10000000
DOMAIN_SHUTOFF = 5


@dataclass(frozen=True)
class GuestOffObservation:
    hypervisor_uuid: str
    guest_uuid: str
    libvirt_version: int
    state: int
    reason: int
    observed_mono_ns: int


def _native_uuid(value):
    if (type(value) is not str
            or not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value)):
        raise TargetJournalError("TARGET_GUEST_IDENTITY")
    result = uuid.UUID(value).hex
    if not _hex(result, 32):
        raise TargetJournalError("TARGET_GUEST_IDENTITY")
    return result


def _integer(value, expected):
    return type(value) is int and value == expected


def _host(connection, mapping):
    if (connection.getURI() != "qemu:///system" or connection.getType() != "QEMU"
            or not _integer(connection.isAlive(), 1)
            or not _integer(connection.getLibVersion(), LIBVIRT_VERSION)):
        raise TargetJournalError("TARGET_GUEST_PROFILE")
    document = connection.getCapabilities()
    if type(document) is not str or not 0 < len(document) <= 1048576 or "<!" in document:
        raise TargetJournalError("TARGET_GUEST_CAPABILITIES")
    root = ET.fromstring(document)
    hosts, identifiers = root.findall("./host"), root.findall("./host/uuid")
    if (root.tag != "capabilities" or len(hosts) != 1 or len(identifiers) != 1
            or len(identifiers[0]) or _native_uuid(identifiers[0].text) != mapping.hypervisor_uuid):
        raise TargetJournalError("TARGET_GUEST_IDENTITY")


def _guest(connection, mapping):
    domain = connection.lookupByUUIDString(str(uuid.UUID(hex=mapping.guest_uuid)))
    if (_native_uuid(domain.UUIDString()) != mapping.guest_uuid
            or not _integer(domain.isPersistent(), 1) or not _integer(domain.isActive(), 0)
            or not _integer(domain.autostart(), 0) or not _integer(domain.hasManagedSaveImage(0), 0)):
        raise TargetJournalError("TARGET_GUEST_NOT_OFF")
    state = domain.state(0)
    if (type(state) not in (tuple, list) or len(state) != 2
            or not _integer(state[0], DOMAIN_SHUTOFF)
            or type(state[1]) is not int or not 0 <= state[1] <= 0x7fffffff):
        raise TargetJournalError("TARGET_GUEST_NOT_OFF")
    return tuple(state)


def observe_off(connection, mapping, deadline_mono_ns):
    """Use a borrowed read-only connection under the parent's bounded worker.

    Exclusive management ownership and persistent storage denial must outlive
    this readback. Two reads are not an atomic snapshot or future-ON exclusion.
    """
    try:
        if (type(mapping) is not ProtectedMap or mapping.profile != "pre2-kvm-gfs2-v1"
                or not _hex(mapping.hypervisor_uuid, 32) or not _hex(mapping.guest_uuid, 32)
                or type(deadline_mono_ns) is not int or not 0 < deadline_mono_ns < 1 << 64
                or time.monotonic_ns() >= deadline_mono_ns):
            raise TargetJournalError("TARGET_GUEST_ARGUMENT")
        _host(connection, mapping)
        before = _guest(connection, mapping)
        _host(connection, mapping)
        after = _guest(connection, mapping)
        now = time.monotonic_ns()
        if before != after or now >= deadline_mono_ns:
            raise TargetJournalError("TARGET_GUEST_CHANGED")
        return GuestOffObservation(mapping.hypervisor_uuid, mapping.guest_uuid,
                                   LIBVIRT_VERSION, after[0], after[1], now)
    except TargetJournalError:
        raise
    except Exception:
        # Native libvirt exceptions/XML may contain management endpoints or paths.
        raise TargetJournalError("TARGET_GUEST_UNPROVEN") from None
