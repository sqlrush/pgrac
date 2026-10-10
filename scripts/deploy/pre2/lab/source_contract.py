#!/usr/bin/env python3
"""Read the release source tree and report what its entries actually support.

Author: SqlRush <sqlrush@gmail.com>

Every capability here is derived from the release's own source files: the
initdb option table and messages, the shared configuration policy table, the
WAL thread layout of pgrac-init, the catalog version and the shared-filesystem
I/O code. Nothing is assumed from a document. A capability that cannot be
proven from source is reported as absent, which makes the dependent stage
BLOCKED instead of being emulated by this tool.

    source_contract.py --source /path/to/source-tree [--out contract.json]
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

from lab_common import PreflightError, file_sha256

INITDB = "src/bin/initdb/initdb.c"
PGRAC_INIT = "src/bin/pgrac/pgrac-init"
CONFIG_POLICY = "src/backend/cluster/cluster_shared_config_guc.c"
CLUSTER_GUC = "src/backend/cluster/cluster_guc.c"
GUC_TABLES = "src/backend/utils/misc/guc_tables.c"
CATVERSION = "src/include/catalog/catversion.h"
SHAREDFS = "src/backend/cluster/storage/cluster_shared_fs_sharedfs.c"
PRE2_FIXTURE = "src/test/perl/PostgreSQL/Test/ClusterPRE2.pm"

NATIVE_OPTIONS = ("pgrac-initdb-thread", "pgrac-initdb-system-identifier",
                  "pgrac-initdb-shared-base", "pgrac-initdb-storage-uuid",
                  "pgrac-initdb-database-incarnation")
CONFIG_OPTION = "pgrac-initdb-shared-config"
INITDB_CONFIG = "src/backend/cluster/cluster_initdb_config.c"
COHORT_OWNER = "src/backend/cluster/cluster_initdb_cohort.c"
CATALOG_READ = "src/backend/cluster/cluster_control_catalog_read.c"
XLOG = "src/backend/access/transam/xlog.c"
ROOT_NOT_PUBLISHED = "shared startup authority is not published"
POLICY_BITS = {"POLICY_COMMON": "COMMON", "POLICY_INSTANCE": "INSTANCE", "POLICY_COLD": "COLD",
               "POLICY_STRING": "STRING", "POLICY_PATH": "PATH", "POLICY_UUID": "UUID"}


def read_text(root, relative):
    path = root / relative
    try:
        return path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeDecodeError):
        raise PreflightError("SOURCE_FILE_UNAVAILABLE", relative) from None


def parse_policy_table(text):
    """Return {name: sorted flag words} from the config_policies[] table."""
    start = text.find("config_policies[] = {")
    end = text.find("};", start)
    if start < 0 or end < 0:
        raise PreflightError("SOURCE_POLICY_TABLE_UNRECOGNIZED", CONFIG_POLICY)
    table = {}
    for name, flags in re.findall(r'\{\s*"([a-z0-9_.]+)",\s*([A-Z_| ]+?)\s*\}', text[start:end]):
        words = [w.strip() for w in flags.split("|")]
        if name in table or any(w not in POLICY_BITS for w in words):
            raise PreflightError("SOURCE_POLICY_TABLE_UNRECOGNIZED", name)
        table[name] = sorted(POLICY_BITS[w] for w in words)
    if not table:
        raise PreflightError("SOURCE_POLICY_TABLE_UNRECOGNIZED", CONFIG_POLICY)
    return table


def parse_bootstrap_required(text):
    """Return {name: kind} from bootstrap_required[]; kinds are CONFIG_BOOTSTRAP_*."""
    start = text.find("bootstrap_required[] = {")
    end = text.find("};", start)
    if start < 0 or end < 0:
        raise PreflightError("SOURCE_BOOTSTRAP_TABLE_UNRECOGNIZED", CONFIG_POLICY)
    found = re.findall(r'\{\s*"([a-z0-9_.]+)",\s*CONFIG_BOOTSTRAP_([A-Z]+)\s*\}', text[start:end])
    if not found:
        raise PreflightError("SOURCE_BOOTSTRAP_TABLE_UNRECOGNIZED", CONFIG_POLICY)
    return dict(found)


def parse_string_gucs(guc_tables, cluster_guc):
    """Names of string-typed GUCs: native ConfigureNamesString plus cluster strings."""
    start = guc_tables.find("struct config_string ConfigureNamesString[] =")
    end = guc_tables.find("struct config_enum ConfigureNamesEnum[] =", start)
    if start < 0 or end < 0:
        raise PreflightError("SOURCE_GUC_TABLE_UNRECOGNIZED", GUC_TABLES)
    names = set(re.findall(r'\{\s*\{\s*"([a-z0-9_]+)",\s*PGC_', guc_tables[start:end]))
    names |= set(re.findall(r'DefineCustomStringVariable\(\s*"([a-z0-9_.]+)"', cluster_guc))
    if "hba_file" not in names or "cluster.shared_data_dir" not in names:
        raise PreflightError("SOURCE_GUC_TABLE_UNRECOGNIZED", GUC_TABLES)
    return sorted(names)


def parse_catalog_version(text):
    found = re.findall(r"^#define CATALOG_VERSION_NO\s+(\d+)\s*$", text, re.M)
    if len(found) != 1:
        raise PreflightError("SOURCE_CATVERSION_UNRECOGNIZED", CATVERSION)
    return int(found[0])


def parse_required_catalog(text):
    found = set(re.findall(r"eq '(\d{9})'", text))
    if len(found) != 1:
        raise PreflightError("SOURCE_FORMAT_REQUIREMENT_UNRECOGNIZED", PRE2_FIXTURE)
    return int(found.pop())


def initdb_capabilities(text, creator_text):
    """text is initdb.c (option table); creator_text is every initdb C source."""
    options = {name: bool(re.search(r'\{\s*"%s",\s*required_argument' % re.escape(name), text))
               for name in NATIVE_OPTIONS}
    return {
        "native_options": options,
        "native_writer": all(options.values()),
        # The release's own creator says whether it publishes startup authority.
        "root_not_published_message": ROOT_NOT_PUBLISHED in text,
        "founder_thread_only_base": "founder thread 1" in text,
        "native_requires_checksums": "requires checksums and full initdb sync" in text,
        # The creator names only the founder origin; other origins have no creator.
        "founder_side_origin": "origin_0" if '"native_side"' in creator_text
                               and '"origin_0"' in creator_text else None,
        "other_origin_side_creator": bool(re.search(r'"origin_%', creator_text)),
        "relmap_authority_initializer": "relmap" in creator_text.lower(),
        "shared_config_initializer": "cluster.shared_config" in creator_text,
        "cohort_option": bool(re.search(r'\{\s*"pgrac-initdb-cohort",\s*no_argument', text)),
        # Founder-only creation of the generation-1 common configuration object.
        "initial_config_option": bool(re.search(r'\{\s*"%s",\s*required_argument' % CONFIG_OPTION, text)),
        "initial_config_requires_explicit_sysid": "explicit common system identity" in text,
    }


def cohort_capabilities(texts, xlog):
    """Complete original cohort owner and the read-only startup routes that consume it."""
    owner = texts.get(COHORT_OWNER, "")
    return {"owner": "ClusterInitdbCohortMain" in owner,
            "creation_message": "Shared cohort created; shared startup remains closed." in owner,
            "root_last": "create_root_objects" in owner,
            "native_side_routing": "route_original_side" in owner,
            "catalog_objects": "create_catalog_objects" in owner,
            "catalog_read_registration": "catalog_read_dirs" in texts.get(CATALOG_READ, ""),
            "startup_bootstrap_route": "cluster_control_bootstrap_prepare(DataDir" in xlog,
            "requires_c_locale": "original cohort currently requires libc C locale" in texts.get(INITDB, "")}


def pgrac_init_layout(text):
    convention = ("thread_id = node_id + 1" in text
                  and 'THREAD_DIR="$WAL_THREADS_DIR/thread_$THREAD_ID"' in text)
    return {"thread_dir_pattern": "thread_{node_id_plus_1}" if convention else None,
            "shared_config_initializer": "cluster.shared_config" in text or "shared-config" in text}


def sharedfs_direct_io(text):
    return "io_direct_flags & IO_DIRECT_DATA" in text and "PG_O_DIRECT" in text


def git_identity(root):
    def git(*argv):
        completed = subprocess.run(["git", "-C", str(root)] + list(argv), capture_output=True,
                                   text=True, timeout=30, check=False)
        return completed.stdout.strip() if completed.returncode == 0 else None
    commit = git("rev-parse", "HEAD")
    dirty = git("status", "--porcelain", "--untracked-files=no")
    return {"commit": commit, "tracked_tree_clean": dirty == "" if dirty is not None else None}


def collect(source):
    root = Path(source).resolve(strict=True)
    texts = {name: read_text(root, name) for name in
             (INITDB, PGRAC_INIT, CONFIG_POLICY, CLUSTER_GUC, GUC_TABLES, CATVERSION, SHAREDFS)}
    creators = sorted(str(p.relative_to(root)) for p in (root / "src/bin/initdb").glob("*.c"))
    for name in creators:
        texts.setdefault(name, read_text(root, name))
    creator_text = "\n".join(texts[name] for name in creators)
    for optional in (INITDB_CONFIG, COHORT_OWNER, CATALOG_READ):
        try:
            texts[optional] = read_text(root, optional)
        except PreflightError:
            pass                  # older releases lack these owners
    xlog = read_text(root, XLOG)
    policy = parse_policy_table(texts[CONFIG_POLICY])
    try:
        required_catalog = parse_required_catalog(read_text(root, PRE2_FIXTURE))
    except PreflightError:
        required_catalog = None
    contract = {
        "schema_version": 1,
        "kind": "pre2-lab-source-contract",
        "source": dict(git_identity(root), root=str(root),
                       file_sha256={name: file_sha256(root / name) for name in texts}),
        "initdb": dict(initdb_capabilities(texts[INITDB], creator_text),
                       initial_config_object_dir="global/config_images"
                       if "config_images" in texts.get(INITDB_CONFIG, "") else None),
        "cohort": cohort_capabilities(texts, xlog),
        "pgrac_init": pgrac_init_layout(texts[PGRAC_INIT]),
        "config_policy": policy,
        "bootstrap_required": parse_bootstrap_required(texts[CONFIG_POLICY]),
        "string_gucs": parse_string_gucs(texts[GUC_TABLES], texts[CLUSTER_GUC]),
        "catalog_version": parse_catalog_version(texts[CATVERSION]),
        "required_catalog_version": required_catalog,
        "direct_io": {"sharedfs_consumes_io_direct_data": sharedfs_direct_io(texts[SHAREDFS]),
                      "policy": policy.get("debug_io_direct")},
    }
    return contract


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", required=True)
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    try:
        contract = collect(args.source)
    except PreflightError as exc:
        print(json.dumps({"status": exc.status, "reason": exc.reason, "field": exc.field}))
        return 2
    text = json.dumps(contract, indent=1, sort_keys=True) + "\n"
    if args.out:
        Path(args.out).write_text(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
