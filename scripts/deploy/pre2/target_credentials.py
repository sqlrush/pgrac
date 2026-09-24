"""Protected TLS loading in the dedicated Linux target owner.

Author: SqlRush <sqlrush@gmail.com>
Disables core dumps permanently before opening credentials. No key bytes are
copied into Python. The owner must not change credentials/capabilities later.
"""

import ctypes
from dataclasses import dataclass
import os
import resource
import ssl
import stat
import sys
import time

from target_journal import TargetJournalError, _uint
from target_registry import _open_registry

MAX_CREDENTIAL_BYTES = 65536


@dataclass(frozen=True)
class TlsFiles:
    ca_certificate: str
    certificate: str
    private_key: str
    owner_uid: int = 0


def _lock_secrets():
    # RLIMIT alone does not stop Linux pipe core collectors.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
    libc.prctl.restype = ctypes.c_int
    if (libc.prctl(4, 0, 0, 0, 0) != 0 or libc.prctl(3, 0, 0, 0, 0) != 0
            or resource.getrlimit(resource.RLIMIT_CORE) != (0, 0)):
        raise TargetJournalError("TARGET_TLS_DUMP_PROTECTION")


def _credential_info(fd, owner):
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != owner
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
            or not 0 < info.st_size <= MAX_CREDENTIAL_BYTES):
        raise TargetJournalError("TARGET_TLS_FILE")
    return tuple(getattr(info, field) for field in ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid",
                                                   "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns"))


def _no_password():
    # A supplied callback prevents OpenSSL from falling back to a terminal prompt.
    raise TargetJournalError("TARGET_TLS_ENCRYPTED_KEY")


def load_server_context(files, deadline_mono_ns):
    """Load explicit trust only; does not listen or grant operation permission.

    Run in the dedicated owner process. Effective identity and configuration
    remain frozen afterwards. A worker supervisor bounds blocking native I/O;
    this deadline never provides isolation evidence or changes DB timeouts.
    """
    descriptors, identities = [], []
    try:
        if (sys.platform != "linux" or type(files) is not TlsFiles
                or type(files.owner_uid) is not int or files.owner_uid != os.geteuid()
                or not _uint(deadline_mono_ns) or time.monotonic_ns() >= deadline_mono_ns):
            raise TargetJournalError("TARGET_TLS_ARGUMENT")
        _lock_secrets()
        for path in (files.ca_certificate, files.certificate, files.private_key):
            fd = _open_registry(path, files.owner_uid)
            descriptors.append(fd)
            identities.append(_credential_info(fd, files.owner_uid))
        if time.monotonic_ns() >= deadline_mono_ns:
            raise TargetJournalError("TARGET_TLS_EXPIRED")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        context.options |= ssl.OP_NO_TICKET
        context.num_tickets = 0
        ca, certificate, key = (f"/proc/self/fd/{fd}" for fd in descriptors)
        context.load_verify_locations(cafile=ca)
        context.load_cert_chain(certfile=certificate, keyfile=key, password=_no_password)
        for fd, previous in zip(descriptors, identities):
            if previous != _credential_info(fd, files.owner_uid):
                raise TargetJournalError("TARGET_TLS_CHANGED")
        if time.monotonic_ns() >= deadline_mono_ns or context.keylog_filename is not None:
            raise TargetJournalError("TARGET_TLS_UNPROVEN")
        return context
    except Exception:
        # Also suppress protected-file errors carrying path/native details.
        raise TargetJournalError("TARGET_TLS_UNPROVEN") from None
    finally:
        for fd in descriptors:
            os.close(fd)
