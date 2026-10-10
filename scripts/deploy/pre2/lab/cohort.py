"""Original cohort creation: one initdb for all members, then exact PGDATA distribution.

Author: SqlRush <sqlrush@gmail.com>

When the release initdb accepts --pgrac-initdb-cohort, one original creator
runs initdb once with the canonical configuration request. -D names a new
local cache parent; the creator writes node_0..node_N there and the shared
DATA, WAL and UNDO roots named in the request. Each node_N is then copied,
unchanged, to its member's new local PGDATA: regular files, modes and symlink
targets are preserved, hard links are refused, and shared paths are not moved.
"""

import hashlib
import os
from pathlib import Path, PurePosixPath
import stat

from lab_common import FOUNDER_NODE, PreflightError

COHORT_CREATED = "Shared cohort created; shared startup remains closed."
COHORT_LOCALE_REFUSAL = "original cohort currently requires libc C locale"
# The local bootstrap file names only how to find startup authority (plus strings the
# shared configuration policy never accepts). Every other setting comes from ROOT.
BOOTSTRAP_DISCOVERY = ("cluster.enabled", "cluster.shared_config", "cluster.controlfile_shared_authority",
                       "cluster.shared_data_dir", "cluster.wal_threads_dir", "cluster.undo_tablespace_path")


def layout(request, derived):
    """Cache root and request file beside the members' local roots (parent must exist)."""
    founder = derived["nodes"][FOUNDER_NODE]["pgdata"]
    local_root = PurePosixPath(founder).parent.parent
    return {"cache_root": str(local_root / "cohort"), "local_root": str(local_root)}


def initdb_argv(install_root, cache_root, config_path):
    """The release's documented cohort command; nothing else is added."""
    return [install_root + "/bin/initdb", "-D", cache_root, "-k", "-A", "trust", "--no-locale",
            "--pgrac-initdb-cohort", "--pgrac-initdb-shared-config=" + config_path]


def manifest(root):
    """Exact tree listing without following symlinks: type, mode, size, sha256 or link target."""
    root = Path(root)
    entries = []
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs.sort()
        for name in sorted(dirs + files):
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            st = os.lstat(path)
            mode = stat.S_IMODE(st.st_mode)
            if stat.S_ISLNK(st.st_mode):
                entries.append(["l", relative, oct(mode), os.readlink(path)])
            elif stat.S_ISDIR(st.st_mode):
                entries.append(["d", relative, oct(mode)])
            elif stat.S_ISREG(st.st_mode):
                if st.st_nlink != 1:
                    raise PreflightError("PGDATA_HARD_LINK", relative)
                digest = hashlib.sha256()
                with open(path, "rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                entries.append(["f", relative, oct(mode), st.st_size, digest.hexdigest()])
            else:
                raise PreflightError("PGDATA_SPECIAL_FILE", relative)
        # Symlinked directories are listed above as links and never descended.
        dirs[:] = [d for d in dirs if not os.path.islink(Path(directory) / d)]
    top = os.lstat(root)
    body = {"root_mode": oct(stat.S_IMODE(top.st_mode)), "entries": entries}
    return dict(body, sha256=hashlib.sha256(repr(body).encode()).hexdigest(), count=len(entries))


def plan(request, derived, contract, rendered, install_root):
    paths = layout(request, derived)
    config_path = "%s/pre2-initial-config-%s.conf" % (paths["local_root"],
                                                     rendered["config_request"]["sha256"][:16])
    shared = derived["shared"]
    return {"creation": "cohort", "founder_node": FOUNDER_NODE, "cache_root": paths["cache_root"],
            "config_request_path": config_path,
            "shared_roots": {"data": shared["shared_data_dir"], "wal": shared["wal_threads_dir"],
                             "undo": shared["undo_tablespace_path"]},
            "initdb_argv": initdb_argv(install_root, paths["cache_root"], config_path),
            "members": [{"node_id": node["node_id"], "source": "%s/node_%d" % (paths["cache_root"], node["node_id"]),
                         "pgdata": node["pgdata"]} for node in request["nodes"]]}
