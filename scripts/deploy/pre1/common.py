"""Read-only deployment validation and evidence helpers.

Author: SqlRush <sqlrush@gmail.com>
"""

import fcntl
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import tempfile


class PreflightError(Exception):
    """Safe, value-free error suitable for a machine-readable result."""

    def __init__(self, reason, field="profile", status="BLOCKED"):
        self.reason = reason
        self.field = field
        self.status = status
        super().__init__(f"{reason}: {field}")


def load_json(path):
    """Read bounded JSON, rejecting duplicate keys and non-finite numbers."""
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise PreflightError("JSON_DUPLICATE_KEY")
            value[key] = item
        return value

    def nonfinite(_value):
        raise PreflightError("JSON_INVALID")

    try:
        with open(path, "rb") as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise PreflightError("JSON_TOO_LARGE")
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    except (ValueError, RecursionError) as exc:
        raise PreflightError("JSON_INVALID") from exc
    except OSError as exc:
        raise PreflightError("INPUT_IO", status="ERROR") from exc


def canonical_bytes(document):
    return (json.dumps(document, sort_keys=True, ensure_ascii=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode()


def document_sha(document):
    return hashlib.sha256(canonical_bytes(document)).hexdigest()


def _schema_check(value, rule, definitions, field="profile"):
    """Validate only the closed subset used by the bundled schema.

    Unknown schema keywords are errors, not silently unsupported validation.
    This is not a general-purpose JSON Schema implementation.
    """
    supported = {"$schema", "title", "description", "$defs", "$ref", "type",
                 "properties", "required", "additionalProperties", "items",
                 "minItems", "maxItems", "minLength", "maxLength", "pattern",
                 "minimum", "maximum", "enum", "const"}
    if set(rule) - supported:
        raise PreflightError("SCHEMA_UNSUPPORTED", status="ERROR")
    if "$ref" in rule:
        name = rule["$ref"].removeprefix("#/$defs/")
        if name not in definitions:
            raise PreflightError("SCHEMA_UNSUPPORTED", status="ERROR")
        return _schema_check(value, definitions[name], definitions, field)
    types = {"object": (dict,), "array": (list,), "string": (str,),
             "integer": (int,), "number": (int, float), "boolean": (bool,)}
    if type(value) not in types[rule["type"]]:
        raise PreflightError("FIELD_TYPE", field)
    if isinstance(value, float) and not math.isfinite(value):
        raise PreflightError("FIELD_VALUE", field)
    if "const" in rule and value != rule["const"]:
        raise PreflightError("FIELD_VALUE", field)
    if "enum" in rule and value not in rule["enum"]:
        raise PreflightError("FIELD_VALUE", field)
    if type(value) is dict:
        properties = rule["properties"]
        if rule.get("additionalProperties") is not False:
            raise PreflightError("SCHEMA_UNSUPPORTED", status="ERROR")
        if set(value) - set(properties):
            # Neither unrecognized keys nor values belong in error output.
            raise PreflightError("UNKNOWN_FIELD", field)
        for key in rule["required"]:
            if key not in value:
                raise PreflightError("REQUIRED_FIELD", f"{field}.{key}")
        for key, item in value.items():
            _schema_check(item, properties[key], definitions, f"{field}.{key}")
    elif type(value) is list:
        if not rule["minItems"] <= len(value) <= rule["maxItems"]:
            raise PreflightError("FIELD_RANGE", field)
        for n, item in enumerate(value):
            _schema_check(item, rule["items"], definitions, f"{field}[{n}]")
    elif type(value) is str:
        if (len(value) < rule.get("minLength", 0) or
                len(value) > rule.get("maxLength", 4096)):
            raise PreflightError("FIELD_RANGE", field)
        if any(ord(c) < 32 for c in value):
            raise PreflightError("FIELD_VALUE", field)
        if "pattern" in rule and not re.fullmatch(rule["pattern"], value):
            raise PreflightError("FIELD_VALUE", field)
    elif type(value) in (int, float):
        if value < rule.get("minimum", -math.inf) or value > rule.get("maximum", math.inf):
            raise PreflightError("FIELD_RANGE", field)


def unique(values, field):
    if len(values) != len(set(values)):
        raise PreflightError("IDENTITY_DUPLICATE", field)


def safe_remote_path(value, field):
    """Lexical guard only: guest-side realpath/ownership is still mandatory."""
    path = PurePosixPath(value)
    if (path.anchor != "/" or ".." in path.parts or str(path) != value or
            len(path.parts) < 3 or path.parts[1] in ("root", "home", "Users", "proc", "sys", "dev")):
        raise PreflightError("UNSAFE_PATH", field)
    return path


def paths_disjoint(values, field):
    for n, left in enumerate(values):
        for right in values[n + 1:]:
            if left == right or left in right.parents or right in left.parents:
                raise PreflightError("PATH_OVERLAP", field)


def ipv4(value, field):
    try:
        address = ipaddress.IPv4Address(value)
        if address.is_loopback or address.is_unspecified or address.is_multicast or address.is_link_local:
            raise ValueError
        return str(address)
    except ValueError as exc:
        raise PreflightError("ENDPOINT_INVALID", field) from exc


def endpoint(value, field):
    try:
        host, port = value.split(":")
        number = int(port)
        if str(number) != port or not 1 <= number <= 65535:
            raise ValueError
        return ipv4(host, field), number
    except ValueError as exc:
        raise PreflightError("ENDPOINT_INVALID", field) from exc


def validate_profile(profile):
    schema = load_json(Path(__file__).with_name("profile.schema.json"))
    _schema_check(profile, schema, schema["$defs"])
    nodes = profile["nodes"]
    for key in ("node_id", "vm_uuid", "machine_id", "boot_id"):
        unique([n[key] for n in nodes], f"nodes.{key}")
    shared = safe_remote_path(profile["mountpoint"], "mountpoint")
    occupied = []
    for n in nodes:
        prefix = f"nodes[{n['node_id']}]"
        paths = [safe_remote_path(n[k], f"{prefix}.{k}")
                 for k in ("pgdata", "install_root", "log_root")]
        paths_disjoint(paths + [shared], prefix)
        admin = n["admin_endpoint"]
        ipv4(admin["host"], f"{prefix}.admin_endpoint.host")
        safe_remote_path(admin["identity_file"], f"{prefix}.admin_endpoint.identity_file")
        sql = endpoint(n["sql_addr"], f"{prefix}.sql_addr")
        control = endpoint(n["control_addr"], f"{prefix}.control_addr")
        host, port = endpoint(n["data_base_addr"], f"{prefix}.data_base_addr")
        if port + n["data_workers"] > 65536:
            raise PreflightError("ENDPOINT_INVALID", f"{prefix}.data_workers")
        ports = [sql, control] + [(host, p) for p in range(port, port + n["data_workers"])]
        if len(set(occupied + ports)) != len(occupied + ports):
            raise PreflightError("PORT_OVERLAP", prefix)
        occupied += ports
    mapping = profile["fencing"]["vm_map"]
    unique([n["node_id"] for n in mapping], "fencing.vm_map.node_id")
    if {n["node_id"]: n["vm_uuid"] for n in mapping} != {n["node_id"]: n["vm_uuid"] for n in nodes}:
        raise PreflightError("FENCE_MAPPING_MISMATCH", "fencing.vm_map")
    safe_remote_path(profile["fencing"]["credential_ref"], "fencing.credential_ref")
    unique([n["node_id"] for n in profile["clock_offset"]], "clock_offset.node_id")
    votes = profile["votes"]
    unique([v["index"] for v in votes], "votes.index")
    main = [v["wwid"] for v in votes]
    negative = profile["fixture_inventory"]["NEGATIVE"]["vote_wwids"]
    all_devices = [profile["data_lun_wwid"]] + main + negative
    if len(set(all_devices)) != len(all_devices):
        raise PreflightError("DEVICE_OVERLAP", "votes")
    if set(profile["fixture_inventory"]["MAIN"]["vote_wwids"]) != set(main):
        raise PreflightError("DEVICE_AUTHORIZATION_MISMATCH", "fixture_inventory.MAIN")
    paths_disjoint([safe_remote_path(profile["fixture_inventory"][k]["root"], f"fixture_inventory.{k}")
                    for k in ("MAIN", "NEGATIVE")], "fixture_inventory")
    allow = profile["authorization"]["device_allowlist"]
    unique([v["wwid"] for v in allow], "authorization.device_allowlist.wwid")
    by_wwid = {v["wwid"]: v for v in allow}
    needed = [(profile["data_lun_wwid"], "data", None)]
    needed += [(v["wwid"], "voting", v["size"]) for v in votes]
    needed += [(v, "negative-voting", None) for v in negative]
    for wwid, purpose, size in needed:
        record = by_wwid.get(wwid)
        if record is None or record["purpose"] != purpose or (size is not None and record["size"] != size):
            raise PreflightError("DEVICE_AUTHORIZATION_MISMATCH", "authorization.device_allowlist")
    return profile


def publish_artifact(path, document):
    """Publish one canonical JSON document without replacing prior evidence."""
    return publish_bytes(path, canonical_bytes(document))


def publish_bytes(path, payload):
    """Publish once, under a local controller lock; never replace old evidence.

    An fsync failure after publication leaves an unqualified orphan, not permission to
    overwrite it. The caller reports ERROR and must reconcile that artifact.
    """
    destination = Path(path)
    temporary = None
    lock_fd = None
    try:
        parent = destination.parent.resolve(strict=True)
        destination = parent / destination.name
        lock_fd = os.open(parent / ".pre1-evidence.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PreflightError("CONTROLLER_BUSY", "out", "ERROR") from exc
        if os.path.lexists(destination):
            raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
        fd, temporary = tempfile.mkstemp(prefix=".pre1-", dir=parent)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        # A replacing rename can clobber a noncooperating late writer despite
        # our controller lock. Same-directory link publishes atomically with
        # kernel-enforced no-replace semantics on both Linux and macOS.
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as exc:
            raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR") from exc
        os.unlink(temporary)
        temporary = None
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return hashlib.sha256(payload).hexdigest()
    except OSError as exc:
        raise PreflightError("ARTIFACT_IO", "out", "ERROR") from exc
    finally:
        if temporary is not None:
            os.unlink(temporary)
        if lock_fd is not None:
            os.close(lock_fd)
