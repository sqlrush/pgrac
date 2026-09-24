"""Initialized-only handles for the fixed Linux target-management profile.

Author: SqlRush <sqlrush@gmail.com>
Called only inside the authenticated, exclusively owned, bounded worker.
Handle construction never mounts configfs, loads modules, creates exports or
changes guest power. Explicit authorized operations remain separate consumers.
"""

from contextlib import contextmanager
import ctypes
import os
import stat
import sys

from target_guest import LIBVIRT_VERSION
from target_inventory import _open_path, _read_config
from target_journal import TargetJournalError

CONFIGFS_ROOT = "/sys/kernel/config/target"
CONFIGFS_MAGIC = 0x62656570
RTSLIB_VERSION = "2.1.74"
DBROOT = "/etc/rtslib-fb-target"


def _configfs_identity():
    """No-symlink native configfs, not a lookalike ordinary directory."""
    if sys.platform != "linux" or os.uname().machine not in ("aarch64", "x86_64"):
        raise TargetJournalError("TARGET_NATIVE_PLATFORM")
    descriptors = []
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.fstatfs.argtypes = (ctypes.c_int, ctypes.c_void_p)
        libc.fstatfs.restype = ctypes.c_int
        identity = None
        for suffix in ("", "/core", "/iscsi"):
            fd = _open_path(CONFIGFS_ROOT + suffix, os.O_RDONLY | os.O_DIRECTORY)
            descriptors.append(fd)
            info = os.fstat(fd)
            # The supported Linux 64-bit statfs layout starts with a long
            # f_type. The remaining native structure fits this aligned buffer.
            buffer = (ctypes.c_long * 32)()
            if (libc.fstatfs(fd, ctypes.byref(buffer)) != 0 or buffer[0] != CONFIGFS_MAGIC
                    or not stat.S_ISDIR(info.st_mode) or info.st_uid != 0
                    or info.st_mode & 0o022):
                raise TargetJournalError("TARGET_NATIVE_CONFIGFS")
            if identity is None:
                identity = (info.st_dev, info.st_ino)
            elif info.st_dev != identity[0]:
                raise TargetJournalError("TARGET_NATIVE_CONFIGFS")
        return identity
    finally:
        for fd in descriptors:
            os.close(fd)


def _lookup_root():
    from rtslib_fb.node import CFSNode
    from rtslib_fb.root import RTSRoot
    # The selected RTSRoot constructor is an initializer, not a read-only
    # lookup: it may mount configfs, modprobe and rewrite dbroot. Its base
    # initializer only assigns the fixed path. Preserve the actual installed
    # lookup/operation methods without invoking implicit host setup.
    root = object.__new__(RTSRoot)
    CFSNode.__init__(root)
    if root.path != CONFIGFS_ROOT:
        raise TargetJournalError("TARGET_NATIVE_ROOT")
    root._check_self()
    root._dbroot = _read_config(CONFIGFS_ROOT + "/dbroot").strip()
    if root._dbroot != DBROOT:
        raise TargetJournalError("TARGET_NATIVE_DBROOT")
    root.invalidate_caches()
    return root


@contextmanager
def native_handles():
    """Fresh read-only libvirt connection and existing LIO lookup, per worker.

    These are neither authenticated map bindings nor OFF/drain certificates.
    The caller still performs full route/guest readback under the owner lock.
    """
    connection = None
    try:
        try:
            if sys.platform != "linux" or os.geteuid() != 0:
                raise TargetJournalError("TARGET_NATIVE_PRIVILEGE")
            import libvirt
            import rtslib_fb
            if (rtslib_fb.__version__ != RTSLIB_VERSION
                    or type(libvirt.getVersion()) is not int
                    or libvirt.getVersion() != LIBVIRT_VERSION):
                raise TargetJournalError("TARGET_NATIVE_VERSION")
            identity = _configfs_identity()
            root = _lookup_root()
            # Libvirt's default error hook writes native details to stderr.
            # This process is dedicated to one target operation; no other
            # library user shares the hook or this fresh connection.
            libvirt.registerErrorHandler(lambda _context, _error: None, None)
            connection = libvirt.openReadOnly("qemu:///system")
            if (connection is None or connection.getURI() != "qemu:///system"
                    or connection.getType() != "QEMU" or connection.isAlive() != 1
                    or connection.getLibVersion() != LIBVIRT_VERSION
                    or _configfs_identity() != identity):
                raise TargetJournalError("TARGET_NATIVE_CONNECTION")
            yield root, connection
        finally:
            if connection is not None and connection.close() != 0:
                raise TargetJournalError("TARGET_NATIVE_CLOSE")
    except Exception:
        raise TargetJournalError("TARGET_NATIVE_UNPROVEN") from None
