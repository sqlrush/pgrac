"""Shared constants and helpers for the PRE2 four-VM deployment tools.

Author: SqlRush <sqlrush@gmail.com>

The PRE1 deployment helpers are imported read-only from ../../pre1. These
tools never modify them, and never write outside the paths named in a
validated request.
"""

import hashlib
import os
from pathlib import Path
import stat
import sys

PRE1_DIR = Path(__file__).resolve().parents[2] / "pre1"
if str(PRE1_DIR) not in sys.path:
    sys.path.insert(0, str(PRE1_DIR))

from common import (PreflightError, _schema_check, canonical_bytes,  # noqa: E402,F401
                    document_sha, endpoint, ipv4, load_json, paths_disjoint,
                    publish_artifact, safe_remote_path, unique)

PROFILE_ID = "pre2-gfs2-arm64-lab-v1"
NODE_COUNT = 4
FOUNDER_NODE = 0
FOUNDER_THREAD = 1
SCHEMA_VERSION = 1

# Status words shared by every result document.
READY = "READY"
BLOCKED = "BLOCKED"
PASS = "PASS"
ERROR = "ERROR"


def file_sha256(path):
    """SHA-256 of one regular, single-link file that does not change while read."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        raise PreflightError("FILE_UNAVAILABLE", str(path)) from None
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise PreflightError("FILE_INVALID", str(path))
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise PreflightError("FILE_CHANGED", str(path))
    return digest.hexdigest()


def dashed_uuid(hex32):
    """Render 32 lowercase hex digits in the 8-4-4-4-12 configuration form."""
    return "-".join((hex32[0:8], hex32[8:12], hex32[12:16], hex32[16:20], hex32[20:32]))
