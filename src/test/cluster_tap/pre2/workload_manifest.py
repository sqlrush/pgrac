"""Verify retained workload files; declarations are not runtime attestation.

No server is contacted. File digests bind the bytes read here, not their origin
or capture time. Inventory PID matches remain candidates, never sealed process
identities or normalized contract events.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from workload_log import read_trace


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _hex(value, length):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{%d}" % length, value) is not None


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def _json(data):
    def invalid_constant(value):
        raise ValueError("invalid JSON constant: " + value)

    value = json.loads(data.decode("utf-8"), object_pairs_hook=_object,
                       parse_constant=invalid_constant)
    _require(isinstance(value, dict), "JSON document is not an object")
    return value


def _file(root, descriptor, retain):
    _require(isinstance(descriptor, dict), "file descriptor is not an object")
    name = descriptor.get("path")
    _require(_text(name) and not Path(name).is_absolute(), "file path must be relative")
    path = (root / name).resolve(strict=True)
    _require(path.is_relative_to(root) and path.is_file(), "file escapes capture or is not regular")
    _require(_hex(descriptor.get("sha256"), 64), "invalid file SHA256")
    size = descriptor.get("size_bytes")
    _require(type(size) is int and size >= 0, "invalid file size")
    digest = hashlib.sha256()
    length = 0
    chunks = []
    with path.open("rb") as stream:
        info = os.fstat(stream.fileno())
        _require(stat.S_ISREG(info.st_mode), "file is not regular")
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
            if retain:
                chunks.append(chunk)
    _require(length == size and digest.hexdigest() == descriptor["sha256"],
             "file digest or size mismatch: " + name)
    return (dict(path=name, sha256=digest.hexdigest(), size_bytes=length),
            (info.st_dev, info.st_ino), b"".join(chunks))


def read_capture(manifest_path):
    """Check a version-1 four-node capture and parse its verified stderr bytes.

    All file IDs must have one role and distinct physical files. Source hashes,
    node boot/epoch and inventory/build declarations must be internally
    consistent; this does not establish built-from-source or running-binary
    identity. No PID candidate can supply missing producer incarnation fields.
    """
    try:
        return _read_capture(Path(manifest_path).resolve(strict=True))
    except (OSError, RuntimeError) as error:
        raise ValueError(str(error)) from error


def _read_capture(path):
    raw_manifest = path.read_bytes()
    manifest = _json(raw_manifest)
    _require(type(manifest.get("version")) is int and manifest["version"] == 1,
             "unsupported manifest version")
    _require(_text(manifest.get("run_id")), "missing run ID")
    source = manifest.get("source")
    _require(isinstance(source, dict) and _hex(source.get("commit"), 40) and
             _hex(source.get("tree"), 40), "invalid source declaration")
    descriptors = manifest.get("files")
    _require(isinstance(descriptors, dict) and descriptors, "missing files")
    _require(all(_text(name) for name in descriptors), "empty file ID")
    nodes = manifest.get("nodes")
    _require(isinstance(nodes, list) and len(nodes) == 4, "four nodes required")
    references = [source.get("file"), manifest.get("harness")]
    retained = set()
    node_ids = set()
    for node in nodes:
        _require(isinstance(node, dict), "node is not an object")
        node_id = node.get("node_id")
        _require(type(node_id) is int and node_id in range(4) and node_id not in node_ids,
                 "duplicate or invalid node ID")
        node_ids.add(node_id)
        _require(_text(node.get("boot_id")), "missing node boot ID")
        epoch = node.get("cluster_epoch")
        _require(isinstance(epoch, str) and re.fullmatch(r"[0-9]+", epoch) is not None,
                 "invalid cluster epoch")
        for role in ("postgres", "config", "stderr", "inventory"):
            reference = node.get(role)
            _require(_text(reference), "missing node file: " + role)
            references.append(reference)
            if role in ("stderr", "inventory"):
                retained.add(reference)
    _require(len({node["cluster_epoch"] for node in nodes}) == 1,
             "node cluster epochs differ")
    _require(all(_text(reference) for reference in references), "missing source/harness file")
    _require(len(set(references)) == len(references) and set(references) == set(descriptors),
             "file IDs must have exactly one role, without missing or unused files")

    files = {}
    contents = {}
    physical_files = set()
    for name, descriptor in descriptors.items():
        metadata, identity, data = _file(path.parent, descriptor, name in retained)
        _require(identity not in physical_files, "file aliases another capture role")
        physical_files.add(identity)
        files[name] = metadata
        if name in retained:
            contents[name] = data
    binaries = {files[node["postgres"]]["sha256"] for node in nodes}
    _require(len(binaries) == 1, "node postgres bytes differ")

    result_nodes = []
    statuses = set()
    for node in nodes:
        inventory = _json(contents[node["inventory"]])
        expected = dict(run_id=manifest["run_id"], node_id=node["node_id"],
                        boot_id=node["boot_id"], cluster_epoch=node["cluster_epoch"],
                        source_commit=source["commit"], source_tree=source["tree"],
                        postgres_sha256=files[node["postgres"]]["sha256"],
                        config_sha256=files[node["config"]]["sha256"],
                        harness_sha256=files[manifest["harness"]]["sha256"])
        _require(all(type(inventory.get(key)) is type(value) and inventory[key] == value
                     for key, value in expected.items()), "inventory capture declaration mismatch")
        processes = inventory.get("processes")
        _require(isinstance(processes, list), "missing inventory processes")
        identities = set()
        for process in processes:
            _require(isinstance(process, dict), "inventory process is not an object")
            _require(type(process.get("pid")) is int and process["pid"] > 0 and
                     type(process.get("procno")) is int and process["procno"] >= 0,
                     "invalid inventory PID/procno")
            _require(all(_text(process.get(key)) for key in
                         ("backend_start", "process_incarnation", "role")),
                     "incomplete inventory process declaration")
            identity = tuple(process[key] for key in
                             ("pid", "procno", "backend_start", "process_incarnation"))
            _require(identity not in identities, "duplicate inventory process identity")
            identities.add(identity)
        trace = read_trace(contents[node["stderr"]])
        statuses.add(trace["parse_status"])
        candidates = []
        for observation in trace["observations"]:
            fields = observation["fields"]
            _require(int(fields["node_id"]) == node["node_id"], "LOG node contradicts file role")
            candidates.append(dict(inventory_indices=[index for index, process in
                                                      enumerate(processes)
                                                      if process["pid"] == int(fields["pid"])]))
        result_nodes.append(dict(node_id=node["node_id"], declared_inventory=inventory,
                                 file_ids={role: node[role] for role in
                                           ("postgres", "config", "stderr", "inventory")},
                                 trace=trace, process_candidates=candidates))
    parse_status = ("REJECTED" if "REJECTED" in statuses else
                    "PARSED" if "PARSED" in statuses else "NOT_OBSERVED")
    return dict(manifest=dict(sha256=hashlib.sha256(raw_manifest).hexdigest(),
                              size_bytes=len(raw_manifest)),
                run_id=manifest["run_id"], declared_source=source, files=files,
                nodes=result_nodes, file_binding="VERIFIED", parse_status=parse_status,
                build_binding="DECLARED_ONLY", identity_bound=False,
                field_root="OPEN", ready_to_run=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="one retained version-1 capture manifest")
    args = parser.parse_args()
    try:
        result = read_capture(args.manifest)
    except ValueError as error:
        result = dict(file_binding="REJECTED", reason=str(error), build_binding="DECLARED_ONLY",
                      identity_bound=False, field_root="OPEN", ready_to_run=False)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["file_binding"] == "REJECTED":
        return 1
    return {"PARSED": 0, "REJECTED": 1, "NOT_OBSERVED": 2}[result["parse_status"]]


if __name__ == "__main__":
    raise SystemExit(main())
