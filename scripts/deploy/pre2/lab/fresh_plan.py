"""Build the fresh PRE2 deployment plan and its explicit wait points.

Author: SqlRush <sqlrush@gmail.com>

The plan records configuration, source capabilities and the native creation
commands for a new shared database. PRE2 uses one cohort initialization, then
distributes each member's directory and checks its bootstrap configuration.
Compatibility commands remain available for older source versions.

A required capability that is missing from the supplied source is reported as
BLOCKED. The tool does not create replacement metadata or bypass that check.
"""

import cohort
from lab_common import BLOCKED, FOUNDER_NODE, FOUNDER_THREAD, READY, document_sha

INITDB_COMMON = ("--auth-local=peer", "--auth-host=reject", "--no-locale", "-E", "UTF8")


def initdb_argv(install_root, username, node_layout, request, system_identifier=None, config_path=None):
    """Exact native writer creation command for one node.

    The founder (thread 1) also creates the shared DATA base and, when the
    release supports it, the generation-1 common configuration object from
    config_path. Every thread uses the request's common system identifier.
    -k enables data checksums; native creation refuses to run without them and
    without full sync, so -N/-S are never used.
    """
    argv = [install_root + "/bin/initdb", "-D", node_layout["pgdata"], "-X", node_layout["wal_dir"],
            "-k", "-U", username] + list(INITDB_COMMON)
    argv.append("--pgrac-initdb-thread=%d" % node_layout["thread"])
    if system_identifier is None:
        raise ValueError("native writer requires the common system identifier")
    argv.append("--pgrac-initdb-system-identifier=%s" % system_identifier)
    if node_layout["thread"] == FOUNDER_THREAD:
        argv += ["--pgrac-initdb-shared-base=" + node_layout["shared_base"],
                 "--pgrac-initdb-storage-uuid=" + request["storage_uuid"],
                 "--pgrac-initdb-database-incarnation=%d" % request["database_incarnation"]]
        if config_path is not None:
            argv.append("--pgrac-initdb-shared-config=" + config_path)
    elif config_path is not None:
        raise ValueError("only the founder creates the initial configuration")
    return argv


def config_request_path(founder_pgdata, sha256):
    """Next to the founder's PGDATA (whose parent must already exist), never inside a target."""
    return "%s/pre2-initial-config-%s.conf" % (founder_pgdata.rsplit("/", 1)[0], sha256[:16])


def cohort_mode(contract):
    """The release creates every member through one original cohort initdb."""
    cohort = contract.get("cohort") or {}
    return bool(contract["initdb"].get("cohort_option") and cohort.get("owner")
                and cohort.get("creation_message"))


def cohort_wait_points(request, contract, add):
    """Cohort releases: each step is performed by the release's own creator or startup route."""
    c = contract["cohort"]
    add("COHORT_ENTRY", "native_writers", not cohort_mode(contract),
        {"initdb_cohort_option": contract["initdb"]["cohort_option"], "owner": c["owner"],
         "requires_c_locale": c["requires_c_locale"]})
    add("FRESH_ROOT_PUBLICATION", "publish_root", not c["root_last"], {"cohort_root_last": c["root_last"]})
    add("RELMAP_AUTHORITY_INIT", "relmap", not c["catalog_objects"],
        {"cohort_catalog_objects": c["catalog_objects"]})
    add("COHORT_NATIVE_SIDE", "cohort_side", not c["native_side_routing"],
        {"cohort_native_side_routing": c["native_side_routing"]})
    add("SHARED_CONFIG_PUBLICATION", "publish_config", not contract["initdb"].get("initial_config_option"),
        {"cohort_consumes_config_request": contract["initdb"].get("initial_config_option", False)})
    required = contract["required_catalog_version"]
    add("FORMAT_ACTIVATION", "format", required is None or contract["catalog_version"] != required,
        {"catalog_version": contract["catalog_version"], "required": required})
    if request["direct_io"] == "data":
        policy = contract["direct_io"]["policy"] or []
        add("DIRECT_IO_DATA", "publish_config",
            not (contract["direct_io"]["sharedfs_consumes_io_direct_data"]
                 and {"COMMON", "COLD", "STRING"} <= set(policy)),
            dict(contract["direct_io"], setting="debug_io_direct=data"))
    route = c["startup_bootstrap_route"] and c["catalog_read_registration"]
    add("SHARED_OPEN_ENTRY", "open", not route,
        {"startup_bootstrap_route": c["startup_bootstrap_route"],
         "catalog_read_registration": c["catalog_read_registration"],
         "note": "an entry exists; whether the cluster opens is observed live, never inferred"})


def wait_points(request, contract):
    """Ordered wait points. Each names the stage it gates and its source evidence."""
    initdb, init = contract["initdb"], contract["pgrac_init"]
    points = []

    def add(point_id, stage, blocked, evidence):
        points.append({"id": point_id, "stage": stage, "status": BLOCKED if blocked else READY,
                       "evidence": evidence})

    if cohort_mode(contract):
        cohort_wait_points(request, contract, add)
        return points

    add("NATIVE_WRITER_ENTRY", "native_writers", not initdb["native_writer"],
        {"initdb_native_options": initdb["native_options"],
         "checksums_required_by_creator": initdb["native_requires_checksums"]})
    add("WAL_THREAD_LAYOUT", "native_writers", init["thread_dir_pattern"] is None,
        {"pgrac_init_thread_dir": init["thread_dir_pattern"]})
    if initdb["root_not_published_message"]:
        add("FRESH_ROOT_PUBLICATION", "publish_root", True,
            {"creator_output": "shared startup authority is not published"})
    else:
        add("FRESH_ROOT_PUBLICATION", "publish_root", True,
            {"creator_output": None, "reason": "TOOL_UPDATE_REQUIRED_FOR_NEW_ENTRY"})
    add("RELMAP_AUTHORITY_INIT", "relmap", not initdb["relmap_authority_initializer"],
        {"initdb_relmap_initializer": initdb["relmap_authority_initializer"]})
    add("COHORT_NATIVE_SIDE", "cohort_side", not initdb["other_origin_side_creator"],
        {"founder_origin": initdb["founder_side_origin"],
         "other_origin_creator": initdb["other_origin_side_creator"]})
    add("SHARED_CONFIG_PUBLICATION", "publish_config", not initdb.get("initial_config_option"),
        {"founder_initial_config_option": initdb.get("initial_config_option", False),
         "object_dir": initdb.get("initial_config_object_dir"),
         "note": "the founder creates the generation-1 object; ROOT selection is FRESH_ROOT_PUBLICATION"})
    required = contract["required_catalog_version"]
    add("FORMAT_ACTIVATION", "format",
        required is None or contract["catalog_version"] != required,
        {"catalog_version": contract["catalog_version"], "required": required})
    if request["direct_io"] == "data":
        policy = contract["direct_io"]["policy"] or []
        add("DIRECT_IO_DATA", "publish_config",
            not (contract["direct_io"]["sharedfs_consumes_io_direct_data"]
                 and {"COMMON", "COLD", "STRING"} <= set(policy)),
            dict(contract["direct_io"], setting="debug_io_direct=data"))
    others = [p for p in points if p["status"] == BLOCKED]
    add("SHARED_OPEN_ENTRY", "open", True,
        {"blocked_before": [p["id"] for p in others],
         "reason": "PRIOR_WAIT_POINTS" if others else "TOOL_UPDATE_REQUIRED_FOR_NEW_ENTRY"})
    return points


STAGES = ("render", "native_writers", "publish_root", "relmap", "cohort_side",
          "publish_config", "format", "open")


def stage_status(points):
    status = {}
    blocked_earlier = False
    for stage in STAGES:
        mine = [p["id"] for p in points if p["stage"] == stage and p["status"] == BLOCKED]
        if mine:
            status[stage] = {"status": BLOCKED, "wait_points": mine}
            if stage != "render":
                blocked_earlier = True
        elif blocked_earlier and stage not in ("render", "native_writers"):
            status[stage] = {"status": BLOCKED, "wait_points": ["PRIOR_STAGE_BLOCKED"]}
        else:
            status[stage] = {"status": READY, "wait_points": []}
    return status


def build(request, derived, contract, rendered, usernames):
    if cohort_mode(contract) and rendered["config_request"] is not None:
        return build_cohort(request, derived, contract, rendered)
    creates_config = bool(contract["initdb"].get("initial_config_option")) and rendered["config_request"] is not None
    nodes = []
    for node in request["nodes"]:
        n = node["node_id"]
        node_layout = dict(derived["nodes"][n])
        config_path = None
        if n == FOUNDER_NODE:
            node_layout["shared_base"] = derived["shared"]["shared_data_dir"]
            if creates_config:
                config_path = config_request_path(node_layout["pgdata"], rendered["config_request"]["sha256"])
        argv = initdb_argv(node["install_root"], usernames[n], node_layout, request,
                           request["system_identifier"], config_path)
        nodes.append({"node_id": n, "thread": node_layout["thread"], "pgdata": node_layout["pgdata"],
                      "wal_dir": node_layout["wal_dir"], "shared_base": node_layout.get("shared_base"),
                      "config_request_path": config_path, "initdb_argv": argv})
    points = wait_points(request, contract)
    plan = {"schema_version": 1, "kind": "pre2-lab-fresh-plan",
            "request_sha256": document_sha(request), "contract_sha256": document_sha(contract),
            "source": contract["source"], "layout": derived, "nodes": nodes,
            "storage_write_cache": request["storage_write_cache"],
            "shared_entries": rendered["shared_entries"], "policy_refusals": rendered["policy_refusals"],
            "config_request": rendered["config_request"], "system_identifier": request["system_identifier"],
            "wait_points": points, "stages": stage_status(points)}
    if rendered["policy_refusals"]:
        plan["stages"]["publish_config"] = {"status": BLOCKED, "wait_points": ["POLICY_REFUSAL"]}
        if contract["initdb"].get("initial_config_option") or cohort_mode(contract):
            # The founder creates the configuration exactly once; never create a base without it.
            plan["stages"]["native_writers"] = {"status": BLOCKED, "wait_points": ["POLICY_REFUSAL"]}
    return plan


def build_cohort(request, derived, contract, rendered):
    install = request["nodes"][FOUNDER_NODE]["install_root"]
    points = wait_points(request, contract)
    plan = {"schema_version": 1, "kind": "pre2-lab-fresh-plan", "creation": "cohort",
            "request_sha256": document_sha(request), "contract_sha256": document_sha(contract),
            "source": contract["source"], "layout": derived,
            "nodes": [{"node_id": n["node_id"], "thread": derived["nodes"][n["node_id"]]["thread"],
                       "pgdata": derived["nodes"][n["node_id"]]["pgdata"]} for n in request["nodes"]],
            "cohort": cohort.plan(request, derived, contract, rendered, install),
            "storage_write_cache": request["storage_write_cache"],
            "shared_entries": rendered["shared_entries"], "policy_refusals": rendered["policy_refusals"],
            "config_request": rendered["config_request"], "system_identifier": request["system_identifier"],
            "wait_points": points, "stages": stage_status(points)}
    return plan
