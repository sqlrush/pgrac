"""Render PRE2 laboratory configuration, checked against the candidate's policy.

Author: SqlRush <sqlrush@gmail.com>

Two products per node:

* a local bootstrap file: the entries the server needs before it can select
  shared startup authority, plus local strings that the shared configuration
  policy does not accept (hba_file, cluster_name, cluster.voting_disks);
* the shared configuration entry list (COMMON plus per-node INSTANCE). This
  tool only lists and checks the entries; the candidate's own publisher writes
  the shared object. An entry the policy would refuse is an error here.

The values start from the PRE1 laboratory profile. The overrides below are
the shared-mode settings; nothing else differs from PRE1.
"""

import hashlib
import re

from lab_common import PreflightError

CONFIG_MAX_BYTES = 1024 * 1024
CONFIG_MAX_VALUE = 8192
CONFIG_MAX_NAME = 127

# PRE1 bootstrap_config.CANONICAL values, as (name, value) pairs.
PRE1_VALUES = [
    ("autovacuum", "off"), ("shared_buffers", "1GB"), ("max_connections", "512"),
    ("reserved_connections", "0"), ("superuser_reserved_connections", "3"),
    ("work_mem", "4MB"), ("maintenance_work_mem", "64MB"), ("fsync", "on"),
    ("synchronous_commit", "on"), ("full_page_writes", "on"), ("wal_buffers", "64MB"),
    ("max_wal_size", "4GB"), ("min_wal_size", "1GB"), ("checkpoint_timeout", "5min"),
    ("checkpoint_completion_target", "0.9"), ("max_parallel_workers_per_gather", "0"),
    ("restart_after_crash", "off"), ("log_statement", "none"),
    ("log_min_error_statement", "error"), ("log_line_prefix", "%m [%p] "), ("log_timezone", "UTC"),
    ("cluster.enabled", "on"), ("cluster.interconnect_tier", "tier1"),
    ("cluster.allow_single_node", "off"), ("cluster.shared_storage_backend", "cluster_fs"),
    ("cluster.smgr_user_relations", "on"), ("cluster.relation_extend_lock_enabled", "on"),
    ("cluster.controlfile_shared_authority", "off"), ("cluster.shared_catalog", "off"),
    ("cluster.merged_recovery", "off"), ("cluster.wal_threads_dir", ""),
    ("cluster.clean_leave_enabled", "on"), ("cluster.pcm_grd_max_entries", "131072"),
    ("cluster.undo_buffers", "65536"), ("cluster.ges_dedup_max_entries", "65536"),
    ("cluster.gcs_block_dedup_max_entries", "32768"), ("cluster.lms_enabled", "on"),
    ("cluster.lms_workers", "2"), ("cluster.read_scache", "on"), ("cluster.online_join", "on"),
    ("cluster.quorum_poll_interval_ms", "2000"), ("cluster.write_fence_lease_ms", "60000"),
    ("cluster.join_convergence_timeout_ms", "30000"), ("cluster.xid_striping", "on"),
    ("cluster.page_scn_shortcut", "on"),
    ("cluster.past_image", "on"), ("cluster.crossnode_write_write", "on"),
    ("cluster.crossnode_cr_data_plane", "on"),
    ("cluster.gcs_reply_timeout_ms", "3000"), ("cluster.gcs_block_retransmit_max_retries", "8"),
    ("cluster.cssd_heartbeat_interval_ms", "2000"), ("cluster.cssd_dead_deadband_factor", "10"),
    ("cluster.voting_disk_size_bytes", "525824"),
]

# Smaller guests: shared memory scales with shared_buffers (cluster.pcm_grd_max_entries must
# cover NBuffers), so this profile shrinks buffers and the largest cluster tables together.
MEMORY_PROFILES = {
    "pre1": {},
    "lab-8g": {"shared_buffers": "512MB", "cluster.pcm_grd_max_entries": "65536",
               "cluster.undo_buffers": "16384", "cluster.gcs_block_dedup_max_entries": "8192",
               # Keep the initial configuration aligned with the cohort creation value.
               "max_connections": "100"},
}
# Shared memory may use at most this fraction of guest memory (rest: OS, backends, page cache).
SHMEM_FRACTION_OF_GUEST = 0.6

# Omitted on purpose: log_min_messages ('warning' in PRE1, which is the PostgreSQL default).
# `postgres -C <runtime-computed>` raises it internally at the highest priority, so a shared
# entry for it makes the candidate refuse the read-only shared-memory check.

# Remote-transaction visibility: both switches must be on for cross-node reads of another
# instance's transactions (otherwise the TT authority is unavailable). They are pinned here,
# after the memory profile, so no profile can drop them; they reach the members only through
# the canonical COMMON request, never through the local bootstrap file or -c.
REMOTE_VISIBILITY = {"cluster.crossnode_runtime_visibility": "on", "cluster.undo_gcs_coherence": "on"}

# Shared database mode: control file, catalog and configuration on shared storage.
SHARED_MODE = {"cluster.controlfile_shared_authority": "on", "cluster.shared_catalog": "on",
               "cluster.shared_config": "on", "cluster.merged_recovery": "on"}
LOCAL_ONLY_STRINGS = ("hba_file", "cluster_name", "cluster.voting_disks")


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def policy_flags(contract, name):
    """Mirror config_policy_flags(): unlisted parameters default to COMMON."""
    return set(contract["config_policy"].get(name, ["COMMON"]))


def check_entry(contract, name, value, node_id):
    """Static subset of the candidate's policy_check(); native value hooks still run later."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*", name):
        raise PreflightError("POLICY_FORMAT", name)
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise PreflightError("POLICY_FORMAT", name)
    if "." in name and not name.startswith("cluster."):
        raise PreflightError("POLICY_UNSUPPORTED", name)
    if name == "cluster.injection_points" or name.startswith(("cluster.test_", "cluster.gcs_block_drop_")):
        raise PreflightError("POLICY_UNSUPPORTED", name)
    flags = policy_flags(contract, name)
    if name in contract["string_gucs"] and "STRING" not in flags:
        raise PreflightError("POLICY_UNSUPPORTED", name)
    if (node_id is None and "COMMON" not in flags) or (node_id is not None and "INSTANCE" not in flags):
        raise PreflightError("POLICY_SCOPE", name)
    if "PATH" in flags and (not value.startswith("/") or any(
            part in (".", "..") for part in value.split("/"))):
        raise PreflightError("POLICY_REFERENCE", name)
    if "UUID" in flags and not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value):
        raise PreflightError("POLICY_REFERENCE", name)
    if name == "track_commit_timestamp" and value not in ("off", "false", "0"):
        raise PreflightError("POLICY_UNSUPPORTED", name)
    if name == "wal_level" and value.lower() == "logical":
        raise PreflightError("POLICY_UNSUPPORTED", name)
    return {"name": name, "value": value, "node_id": node_id, "flags": sorted(flags)}


def common_values(request, derived, contract):
    values = dict(PRE1_VALUES)
    values.update(MEMORY_PROFILES[request["memory_profile"]])
    values.update(REMOTE_VISIBILITY)
    values.update(SHARED_MODE)
    shared = derived["shared"]
    values.update({"cluster.shared_data_dir": shared["shared_data_dir"],
                   "cluster.wal_threads_dir": shared["wal_threads_dir"],
                   "cluster.undo_tablespace_path": shared["undo_tablespace_path"],
                   "cluster.shared_storage_uuid": derived["storage_uuid_dashed"],
                   "cluster.config_file": derived["nodes"][0]["peers_file"]})
    # Instance-only parameters move to the per-node stanza.
    instance = {name: values.pop(name) for name in list(values)
                if "INSTANCE" in policy_flags(contract, name) and "COMMON" not in policy_flags(contract, name)}
    if request["direct_io"] == "data":
        values["debug_io_direct"] = "data"
    # Storage eligibility comes only from the common shared configuration (candidates that know it).
    if "cluster.storage_quorum_nodes" in contract["string_gucs"]:
        quorum = request["storage_quorum"]
        values["cluster.storage_quorum_cluster"] = quorum["cluster"]
        values["cluster.storage_quorum_nodes"] = ",".join(
            "%d:%d" % (n, quorum["nodes"][str(n)]) for n in sorted(int(k) for k in quorum["nodes"]))
    return values, instance


def instance_values(node, derived, carried):
    host, port = node["sql_addr"].split(":")
    values = dict(carried)
    values.update({"cluster.node_id": str(node["node_id"]), "port": port,
                   "listen_addresses": host,
                   "unix_socket_directories": derived["nodes"][node["node_id"]]["socket_dir"],
                   "unix_socket_permissions": "0700"})
    return values


def shared_entries(request, derived, contract):
    """Return (entries, refusals). A refusal is a candidate policy that blocks publication."""
    common, carried = common_values(request, derived, contract)
    entries, refusals = [], []
    for name in sorted(common):
        try:
            entries.append(check_entry(contract, name, common[name], None))
        except PreflightError as exc:
            refusals.append({"name": name, "node_id": None, "reason": exc.reason})
    for node in request["nodes"]:
        values = instance_values(node, derived, carried)
        for name in sorted(values):
            try:
                entries.append(check_entry(contract, name, values[name], node["node_id"]))
            except PreflightError as exc:
                refusals.append({"name": name, "node_id": node["node_id"], "reason": exc.reason})
    return entries, refusals


# How a member finds its startup authority. The values equal the shared image; a
# configuration FILE has the same priority as the shared image, while command-line
# (-c) copies of shared settings are refused by the candidate.
BOOTSTRAP_DISCOVERY = ("cluster.enabled", "cluster.shared_config", "cluster.controlfile_shared_authority",
                       "cluster.shared_data_dir", "cluster.wal_threads_dir", "cluster.undo_tablespace_path")


def bootstrap_file(request, derived, contract, node):
    """Local file: discovery entries plus local-only strings the shared policy never accepts."""
    common, _ = common_values(request, derived, contract)
    missing = [name for name in BOOTSTRAP_DISCOVERY if name not in common]
    if missing:
        raise PreflightError("BOOTSTRAP_ENTRY_UNRENDERED", missing[0])
    paths = derived["nodes"][node["node_id"]]
    lines = ["# PGRAC: PRE2 laboratory bootstrap discovery for node %d." % node["node_id"],
             "# Shared startup authority supplies every other setting."]
    for name in BOOTSTRAP_DISCOVERY:
        lines.append("%s = %s" % (name, quote(common[name])))
    lines.append("cluster.node_id = %d" % node["node_id"])
    lines.append("hba_file = %s" % quote(paths["hba_file"]))
    lines.append("cluster_name = %s" % quote("%s_node%d" % (request["cluster_name"], node["node_id"])))
    lines.append("cluster.voting_disks = %s" % quote(
        ",".join("/dev/disk/by-id/scsi-" + v for v in request["voting_wwids"])))
    return "\n".join(lines) + "\n"


def peers_file(request):
    text = "[cluster]\nname = " + request["cluster_name"] + "\n\n"
    for node in request["nodes"]:
        text += ("[node.%d]\ninterconnect_addr = %s\ndata_addr = %s\n\n"
                 % (node["node_id"], node["control_addr"], node["data_base_addr"]))
    return text


def hba_file(request):
    return ("# Isolated trusted laboratory controller only; not a production policy.\n"
            "local all all peer\n"
            "host postgres pgrac %s/32 trust\n"
            "host postgres racbench %s/32 scram-sha-256\n"
            % (request["controller_addr"], request["controller_addr"]))


def config_request_bytes(request, entries):
    """Canonical generation-1 common configuration request (candidate codec rules).

    Fixed metadata header, then every entry as key='value' with keys strictly
    increasing bytewise: common.<name> before nodeNNN.<name>. The only escape is
    a doubled single quote; control characters are refused.
    """
    configured = 0
    for node in request["nodes"]:
        configured |= 1 << node["node_id"]
    header = ("@authority_uuid=%s\n@configured_0=%016x\n@configured_1=%016x\n"
              "@database_incarnation=%d\n@format=1\n@generation=1\n@storage_uuid=%s\n"
              "@system_identifier=%s\n" % (request["authority_uuid"], configured, 0,
                                            request["database_incarnation"], request["storage_uuid"],
                                            request["system_identifier"]))
    lines = []
    for entry in entries:
        name, value = entry["name"], entry["value"]
        if len(name) > CONFIG_MAX_NAME or len(value.encode()) > CONFIG_MAX_VALUE or any(
                ord(c) < 32 or ord(c) == 127 for c in value):
            raise PreflightError("CONFIG_REQUEST_ENTRY_INVALID", name)
        key = "common." + name if entry["node_id"] is None else "node%03d.%s" % (entry["node_id"], name)
        lines.append((key.encode(), "%s='%s'\n" % (key, value.replace("'", "''"))))
    lines.sort(key=lambda item: item[0])
    keys = [key for key, _ in lines]
    if len(set(keys)) != len(keys):
        raise PreflightError("CONFIG_REQUEST_DUPLICATE_KEY")
    data = (header + "".join(text for _, text in lines)).encode("utf-8")
    if len(data) > CONFIG_MAX_BYTES:
        raise PreflightError("CONFIG_REQUEST_TOO_LARGE")
    return data


def render(request, derived, contract):
    entries, refusals = shared_entries(request, derived, contract)
    files = {}
    for node in request["nodes"]:
        files[node["node_id"]] = {"pre2-bootstrap.conf": bootstrap_file(request, derived, contract, node),
                                  "pgrac.conf": peers_file(request),
                                  "pre2-hba.conf": hba_file(request)}
    config = None
    if not refusals:
        data = config_request_bytes(request, entries)
        config = {"text": data.decode("utf-8"), "sha256": hashlib.sha256(data).hexdigest()}
    return {"shared_entries": entries, "policy_refusals": refusals, "files": files, "config_request": config}
