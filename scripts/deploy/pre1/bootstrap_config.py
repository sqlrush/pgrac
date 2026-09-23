"""Render the fixed isolated-lab PRE1 bootstrap configuration, without writes.

Author: SqlRush <sqlrush@gmail.com>

This is not storage, authentication or startup qualification. The trusted lab
controller alone receives SQL superuser trust; this profile is not an Internet
or customer-production authentication policy. No user-supplied GUC is accepted.
"""

import ipaddress
from pathlib import Path
import re

from common import (PreflightError, _schema_check, endpoint, ipv4, load_json,
                    paths_disjoint, safe_remote_path, unique)


CANONICAL = """# PGRAC: fixed PRE1 laboratory candidate, no crash recovery/autostart.
autovacuum = off
shared_buffers = 1GB
max_connections = 512
reserved_connections = 0
superuser_reserved_connections = 3
work_mem = 4MB
maintenance_work_mem = 64MB
fsync = on
synchronous_commit = on
full_page_writes = on
wal_buffers = 64MB
max_wal_size = 4GB
min_wal_size = 1GB
checkpoint_timeout = 5min
checkpoint_completion_target = 0.9
max_parallel_workers_per_gather = 0
restart_after_crash = off
log_statement = none
log_min_messages = warning
log_min_error_statement = error
log_line_prefix = '%m [%p] '
log_timezone = 'UTC'
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.shared_storage_backend = cluster_fs
cluster.smgr_user_relations = on
cluster.relation_extend_lock_enabled = on
cluster.controlfile_shared_authority = off
cluster.shared_catalog = off
cluster.merged_recovery = off
cluster.wal_threads_dir = ''
cluster.clean_leave_enabled = on
cluster.pcm_grd_max_entries = 131072
cluster.undo_buffers = 65536
cluster.ges_dedup_max_entries = 65536
cluster.gcs_block_dedup_max_entries = 32768
cluster.lms_enabled = on
cluster.lms_workers = 2
cluster.read_scache = on
cluster.online_join = on
cluster.quorum_poll_interval_ms = 2000
cluster.write_fence_lease_ms = 60000
cluster.join_convergence_timeout_ms = 30000
cluster.xid_striping = on
cluster.crossnode_runtime_visibility = on
cluster.page_scn_shortcut = on
cluster.past_image = on
cluster.crossnode_write_write = on
cluster.undo_gcs_coherence = on
cluster.crossnode_cr_data_plane = on
cluster.gcs_reply_timeout_ms = 3000
cluster.gcs_block_retransmit_max_retries = 8
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.voting_disk_size_bytes = 525824
"""


def literal_path(value, field):
    if type(value) is not str or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value):
        raise PreflightError("CONFIG_PATH_INVALID", field)
    return safe_remote_path(value, field)


def render(request):
    keys = {"schema_version", "profile_id", "nodes", "cluster_name", "shared_root",
            "controller_addr", "voting_wwids"}
    if (type(request) is not dict or set(request) != keys
            or type(request["schema_version"]) is not int or request["schema_version"] != 1
            or request["profile_id"] != "pre1-gfs2-arm64-lab-v1"
            or type(request["cluster_name"]) is not str
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", request["cluster_name"])):
        raise PreflightError("CONFIG_REQUEST_INVALID")
    controller = ipv4(request["controller_addr"], "controller_addr")
    if not ipaddress.IPv4Address(controller).is_private:
        raise PreflightError("CONFIG_ISOLATED_CONTROLLER_REQUIRED")
    shared = literal_path(request["shared_root"], "shared_root")
    nodes, votes = request["nodes"], request["voting_wwids"]
    if type(nodes) is not list or len(nodes) != 4:
        raise PreflightError("FOUR_NODES_REQUIRED")
    schema = load_json(Path(__file__).with_name("profile.schema.json"))
    occupied = []
    for node in nodes:
        _schema_check(node, schema["$defs"]["node"], schema["$defs"], "node")
        if node["data_workers"] != 2:
            raise PreflightError("CONFIG_WORKER_COUNT_FROZEN")
        paths_disjoint([shared] + [literal_path(node[key], key)
                        for key in ("pgdata", "install_root", "log_root")], "node")
        sql, control, data = [endpoint(node[key], key)
                              for key in ("sql_addr", "control_addr", "data_base_addr")]
        if data[1] == 65535:
            raise PreflightError("CONFIG_DATA_PORT_RANGE")
        ports = [sql, control, data, (data[0], data[1] + 1)]
        if any(not ipaddress.IPv4Address(host).is_private for host, _ in ports):
            raise PreflightError("CONFIG_ISOLATED_ADDRESS_REQUIRED")
        occupied.extend(ports)
    for key in ("node_id", "vm_uuid", "machine_id", "boot_id"):
        unique([node[key] for node in nodes], "nodes." + key)
    if {node["node_id"] for node in nodes} != set(range(4)):
        raise PreflightError("FOUR_NODES_REQUIRED")
    unique(occupied, "ports")
    if (type(votes) is not list or len(votes) != 3
            or any(type(v) is not str or not re.fullmatch(r"3[0-9a-f]{16}(?:[0-9a-f]{16})?", v) for v in votes)):
        # Whole SCSI NAA devices only; byte identity is independently checked live.
        raise PreflightError("CONFIG_VOTING_IDENTITY_INVALID")
    unique(votes, "votes")
    nodes = sorted(nodes, key=lambda n: n["node_id"])
    peers = "[cluster]\nname = " + request["cluster_name"] + "\n\n"
    for node in nodes:
        peers += (f"[node.{node['node_id']}]\ninterconnect_addr = {node['control_addr']}\n"
                  f"data_addr = {node['data_base_addr']}\n\n")
    results = []
    for node in nodes:
        host, port = endpoint(node["sql_addr"], "sql_addr")
        conf = CANONICAL + (
            f"port = {port}\nlisten_addresses = '{host}'\n"
            f"cluster_name = '{request['cluster_name']}_node{node['node_id']}'\n"
            f"cluster.node_id = {node['node_id']}\ncluster.shared_data_dir = '{shared}'\n"
            "cluster.voting_disks = '" + ",".join("/dev/disk/by-id/scsi-" + v for v in votes) + "'\n"
            f"unix_socket_directories = '{node['log_root']}/socket'\nunix_socket_permissions = 0700\n"
            f"hba_file = '{node['pgdata']}/pre1-hba.conf'\n")
        hba = ("# Isolated trusted PRE1 controller only; not a production policy.\n"
               "local all all peer\n"
               f"host postgres pgrac {controller}/32 trust\n"
               f"host postgres racbench {controller}/32 scram-sha-256\n")
        results.append({"pre1-runtime.conf": conf, "pgrac.conf": peers, "pre1-hba.conf": hba})
    return results
