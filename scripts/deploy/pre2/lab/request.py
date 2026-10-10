"""Validate a PRE2 laboratory request and derive its fixed path layout.

Author: SqlRush <sqlrush@gmail.com>

Node records use the PRE1 node schema unchanged. The PRE2 request adds the
fresh database namespace (storage identity, database incarnation), a dataset
directory on the shared GFS2 mount, a local configuration directory that has
the same absolute path on every guest, and the data I/O mode.
"""

import ipaddress
from pathlib import Path, PurePosixPath
import re

from lab_common import (FOUNDER_NODE, NODE_COUNT, PROFILE_ID, SCHEMA_VERSION, PreflightError,
                        _schema_check, dashed_uuid, endpoint, ipv4, load_json, paths_disjoint,
                        safe_remote_path, unique)

KEYS = {"schema_version", "profile_id", "cluster_name", "dataset_id", "controller_addr",
        "shared_mount", "fs_uuid", "storage_uuid", "authority_uuid", "system_identifier",
        "database_incarnation", "config_dir",
        "direct_io", "storage_write_cache", "memory_profile", "guest_memory_gib", "voting_wwids",
        "storage_quorum", "nodes"}
DIRECT_IO_MODES = ("off", "data")
WRITE_CACHE_MODES = ("write-through", "write-back")
MEMORY_PROFILES = ("pre1", "lab-8g")


def literal_path(value, field):
    if type(value) is not str or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value):
        raise PreflightError("REQUEST_PATH_INVALID", field)
    return safe_remote_path(value, field)


def validate_write_cache(value):
    """Shared data LUN cache mode. Write-back is accepted only with flush evidence.

    The evidence is a record that cache flushes (SYNCHRONIZE CACHE / FUA) issued
    by the guests reached durable media on the storage target. This tool does
    not configure the target; it records the declared mode with the evidence.
    """
    if type(value) is not dict or set(value) != {"mode", "flush_verification"} \
            or value["mode"] not in WRITE_CACHE_MODES:
        raise PreflightError("REQUEST_INVALID", "storage_write_cache")
    evidence = value["flush_verification"]
    if value["mode"] == "write-through":
        if evidence is not None:
            raise PreflightError("REQUEST_INVALID", "storage_write_cache.flush_verification")
        return
    if (type(evidence) is not dict or set(evidence) != {"path", "sha256"}
            or type(evidence["path"]) is not str or not evidence["path"].startswith("/")
            or type(evidence["sha256"]) is not str or not re.fullmatch(r"[0-9a-f]{64}", evidence["sha256"])):
        raise PreflightError("WRITE_CACHE_FLUSH_UNVERIFIED", "storage_write_cache")


def node_schema():
    schema = load_json(Path(__file__).resolve().parents[2] / "pre1" / "profile.schema.json")
    return schema["$defs"]["node"], schema["$defs"]


def layout(request):
    """Derived, non-overridable paths. thread_<node+1> follows pgrac-init."""
    root = PurePosixPath(request["shared_mount"]) / request["dataset_id"]
    shared = {"dataset_root": str(root), "shared_data_dir": str(root / "data"),
              "wal_threads_dir": str(root / "wal"), "undo_tablespace_path": str(root / "undo")}
    config_dir = PurePosixPath(request["config_dir"])
    nodes = {}
    for node in request["nodes"]:
        n = node["node_id"]
        nodes[n] = {"pgdata": node["pgdata"], "thread": n + 1,
                    "wal_dir": str(root / "wal" / ("thread_%d" % (n + 1))),
                    "local_config": str(config_dir / "pre2-bootstrap.conf"),
                    "hba_file": str(config_dir / "pre2-hba.conf"),
                    "peers_file": str(config_dir / "pgrac.conf"),
                    "socket_dir": str(PurePosixPath(node["log_root"]) / "socket")}
    return {"shared": shared, "config_dir": str(config_dir), "nodes": nodes,
            "storage_uuid_dashed": dashed_uuid(request["storage_uuid"])}


def validate_storage_quorum(value):
    """Corosync storage component the candidate admits against: totem.cluster_name and slot->nodeid."""
    if (type(value) is not dict or set(value) != {"cluster", "nodes"} or type(value["cluster"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value["cluster"])):
        raise PreflightError("REQUEST_INVALID", "storage_quorum")
    ids = value["nodes"]
    if (type(ids) is not dict or set(ids) != {str(n) for n in range(NODE_COUNT)}
            or any(type(v) is not int or not 1 <= v <= 2 ** 32 - 1 for v in ids.values())
            or len(set(ids.values())) != NODE_COUNT):
        raise PreflightError("REQUEST_INVALID", "storage_quorum.nodes")


def validate(request):
    if (type(request) is not dict or set(request) != KEYS
            or type(request["schema_version"]) is not int or request["schema_version"] != SCHEMA_VERSION
            or request["profile_id"] != PROFILE_ID):
        raise PreflightError("REQUEST_INVALID")
    if type(request["cluster_name"]) is not str or not re.fullmatch(r"[a-z][a-z0-9_]{0,47}",
                                                                    request["cluster_name"]):
        raise PreflightError("REQUEST_INVALID", "cluster_name")
    if (type(request["dataset_id"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", request["dataset_id"])):
        raise PreflightError("REQUEST_INVALID", "dataset_id")
    if (type(request["fs_uuid"]) is not str
            or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", request["fs_uuid"])):
        raise PreflightError("REQUEST_INVALID", "fs_uuid")
    if (type(request["storage_uuid"]) is not str
            or not re.fullmatch(r"[0-9a-f]{32}", request["storage_uuid"])
            or set(request["storage_uuid"]) == {"0"}):
        raise PreflightError("REQUEST_INVALID", "storage_uuid")
    # The startup authority identity is an RFC 4122 version-4 UUID (the candidate's ROOT
    # encoder refuses anything else): version nibble 4, variant bits 10.
    if (type(request["authority_uuid"]) is not str
            or not re.fullmatch(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}", request["authority_uuid"])
            or request["authority_uuid"] == request["storage_uuid"]):
        raise PreflightError("REQUEST_INVALID", "authority_uuid")
    sysid = request["system_identifier"]
    if (type(sysid) is not str or not re.fullmatch(r"[1-9][0-9]{0,19}", sysid)
            or int(sysid) > 2 ** 64 - 1):
        raise PreflightError("REQUEST_INVALID", "system_identifier")
    incarnation = request["database_incarnation"]
    if type(incarnation) is not int or not 1 <= incarnation <= 2 ** 63 - 1:
        raise PreflightError("REQUEST_INVALID", "database_incarnation")
    if request["direct_io"] not in DIRECT_IO_MODES:
        raise PreflightError("REQUEST_INVALID", "direct_io")
    validate_write_cache(request["storage_write_cache"])
    validate_storage_quorum(request["storage_quorum"])
    if request["memory_profile"] not in MEMORY_PROFILES:
        raise PreflightError("REQUEST_INVALID", "memory_profile")
    if type(request["guest_memory_gib"]) is not int or not 4 <= request["guest_memory_gib"] <= 64:
        raise PreflightError("REQUEST_INVALID", "guest_memory_gib")
    controller = ipv4(request["controller_addr"], "controller_addr")
    if not ipaddress.IPv4Address(controller).is_private:
        raise PreflightError("CONFIG_ISOLATED_CONTROLLER_REQUIRED")
    mount = literal_path(request["shared_mount"], "shared_mount")
    config_dir = literal_path(request["config_dir"], "config_dir")
    votes = request["voting_wwids"]
    if (type(votes) is not list or len(votes) != 3
            or any(type(v) is not str or not re.fullmatch(r"3[0-9a-f]{16}(?:[0-9a-f]{16})?", v)
                   for v in votes)):
        raise PreflightError("CONFIG_VOTING_IDENTITY_INVALID")
    unique(votes, "voting_wwids")
    nodes = request["nodes"]
    if type(nodes) is not list or len(nodes) != NODE_COUNT:
        raise PreflightError("FOUR_NODES_REQUIRED")
    rule, definitions = node_schema()
    occupied = []
    for node in nodes:
        _schema_check(node, rule, definitions, "node")
        local = [literal_path(node[key], key) for key in ("pgdata", "install_root", "log_root")]
        paths_disjoint([mount, config_dir] + local, "node")
        sql, control, data = [endpoint(node[key], key)
                              for key in ("sql_addr", "control_addr", "data_base_addr")]
        if data[1] + node["data_workers"] > 65536:
            raise PreflightError("CONFIG_DATA_PORT_RANGE")
        ports = [sql, control] + [(data[0], p) for p in range(data[1], data[1] + node["data_workers"])]
        if any(not ipaddress.IPv4Address(host).is_private for host, _ in ports):
            raise PreflightError("CONFIG_ISOLATED_ADDRESS_REQUIRED")
        occupied.extend(ports)
    unique(occupied, "ports")
    for key in ("node_id", "vm_uuid", "machine_id", "boot_id"):
        unique([node[key] for node in nodes], "nodes." + key)
    if {node["node_id"] for node in nodes} != set(range(NODE_COUNT)):
        raise PreflightError("FOUR_NODES_REQUIRED")
    if not any(node["node_id"] == FOUNDER_NODE for node in nodes):
        raise PreflightError("FOUNDER_NODE_REQUIRED")
    result = dict(request, nodes=sorted(nodes, key=lambda n: n["node_id"]))
    derived = layout(result)
    paths_disjoint([PurePosixPath(derived["shared"][k]) for k in
                    ("shared_data_dir", "wal_threads_dir", "undo_tablespace_path")], "shared")
    return result, derived
