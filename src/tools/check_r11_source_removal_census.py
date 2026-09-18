#!/usr/bin/env python3
# PGRAC: cluster-specific implementation and build integration.
"""Verify the exact R11 writer/wire/worker source-removal census."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
from typing import Any


SCHEMA = "R11SourceRemovalCensusV3"
SOURCE_REMOVAL_COMMIT = "cb7c7b585cb63ea4906d49fe01352b005a89b8e8"
SOURCE_REMOVAL_TREE = "e80f7dc31d269b5927a36cf393d9b2cccf70c02e"
PRODUCT_COMMIT = "fe68024a0f79d5a1d187e9f1885cbe9ac441153b"
PRODUCT_TREE = "dd51f830614f1f3ec82658f8806ac3eaf1275a8a"
L3_COMMIT = "cc1c5a554276542a05c15f5f1e0e0c7317fba66e"
L3_TREE = "be71cb8fa6bba4164f8f9b57e54adcc6ef2a34b5"
CURRENT_PRODUCT_SNAPSHOT = {
    "algorithm": "sha256-canonical-path-blob-v1",
    "path_count": 2225,
    "sha256": "c3f2ab07f937e7b71c8c4791d3ff1a04f99f9eacc6a989b67c65e2a57fccee4d",
}

LAYERS = {
    "wire_authority": {
        "forbidden_patterns": [
            r"\bPcmX(?:EnqueuePayload|PrehandleCancelPayload|AdmitAckPayload|PhasePayload)\b",
            r"\bPcmX(?:RevokePayload|GrantPayload|InstallReadyPayload|FinalAckPayload)\b",
            r"\bPcmX(?:BlockerSetHeaderPayload|BlockerChunkPayload|DrainPollPayload|RetirePayload)\b",
        ],
        "positive_anchors": [
            {
                "path": "src/backend/cluster/cluster_gcs_block.c",
                "symbol": "gcs_block_legacy_pcm_x_stale_ingress",
            },
            {
                "path": "src/backend/cluster/cluster_gcs_block.c",
                "symbol": "gcs_block_resource_x_type17_ingress",
            },
            {
                "path": "src/backend/cluster/cluster_lms_outbound.c",
                "symbol": "cluster_lms_outbound_resource_x_intent_pump",
            },
        ],
    },
    "worker_authority": {
        "forbidden_patterns": [
            r"\bcluster_gcs_block_pcm_x_formation_tick\b",
            r"\bcluster_gcs_block_pcm_x_image_pump_tick\b",
            r"\bcluster_gcs_pcm_x_terminal_kick\b",
            r"\bcluster_gcs_pcm_x_blocker_probe_kick\b",
            r"\bcluster_pcm_x_retry_work_next\b",
        ],
        "positive_anchors": [
            {
                "path": "src/backend/cluster/cluster_lms.c",
                "symbol": "cluster_lms_outbound_resource_x_intent_pump",
            },
            {
                "path": "src/backend/cluster/cluster_lms_outbound.c",
                "symbol": "cluster_pcm_lock_resource_x_outbound_work_probe_exact",
            },
        ],
    },
    "writer_root": {
        "forbidden_patterns": [
            r"\bcluster_gcs_pcm_x_acquire_writer\b",
            r"\bgcs_block_pcm_x_acquire_writer_impl\b",
            r"\bPcmXTicket",
            r"\bPcmXLocal(?:Handle|Holder|WriterClaim|Progress|Cutoff)\b",
            r"\bcluster_pcm_x_local_",
            r"\bcluster_pcm_x_master_",
        ],
        "positive_anchors": [
            {
                "path": "src/backend/storage/buffer/bufmgr.c",
                "symbol": "cluster_resource_x_writer_path_snapshot",
            },
            {
                "path": "src/backend/storage/buffer/bufmgr.c",
                "symbol": "cluster_gcs_resource_x_target_acquire_until_exact",
            },
            {
                "path": "src/backend/cluster/cluster_semantic_activation.c",
                "symbol": "cluster_resource_x_writer_path_snapshot",
            },
        ],
    },
}

L1_PATTERNS = [
    r"\bPcmX(?:Allocator|MasterTicket|LocalTag|LocalMembership)\b",
    r"\bPcmX(?:PeerFrontier|OutboundTargetFrontier|RuntimeSnapshot|LegacyL3TerminalProof)\b",
    r"\bcluster_pcm_x_(?:allocator|master|local|convert|retry_work|nested_wait)_",
]
L2_PATTERNS = [
    r"\bPCM_X_RUNTIME_",
    r"\bPcmXRuntimeSnapshot\b",
    r"\bPcmXPeerFrontier\b",
    r"\bPcmXOutboundTargetFrontier\b",
    *LAYERS["worker_authority"]["forbidden_patterns"],
]
L3_ANCHORS = [
    {
        "blob_sha256": "469457ca2f55ef3e6f6108e5172e4245909d8d683a2e1382540884f3111631c5",
        "path": "src/backend/cluster/cluster_pcm_x_convert.c",
        "required_symbols": [
            "pcm_x_legacy_l3_source_digest_locked",
            "pcm_x_legacy_l3_proof_matches",
            "pcm_x_legacy_l3_publish_exact",
        ],
    },
    {
        "blob_sha256": "83d1abbda115d6d892bb3d77a217ef53512430c2f043aa520645cffd5a81909b",
        "path": "src/test/cluster_unit/test_cluster_pcm_x_convert.c",
        "required_symbols": [
            "test_resource_x_activation_callbacks_reject_missing_exact_predecessor_pair",
            "test_resource_x_activation_refuses_open_local_round",
            "test_resource_x_activation_refuses_staged_local_frame",
        ],
    },
    {
        "blob_sha256": "e975084d7a9153d4efa5b2b8bfb2c5fa889db43b3a82c4d0a45d8aecce331103",
        "path": "src/test/cluster_unit/test_cluster_r4_activation_fsm.c",
        "required_symbols": [
            "test_93dc_r11_round_reuses_exact_r4_four_node_carrier",
            "test_93dca_member_accepts_exact_r11_sample_and_barrier_on_r4_source",
        ],
    },
]
L3_COMMANDS = [
    {
        "command": "src/test/cluster_unit/test_cluster_pcm_x_convert",
        "plan": 325,
        "required_tests": L3_ANCHORS[1]["required_symbols"],
    },
    {
        "command": "src/test/cluster_unit/test_cluster_r4_activation_fsm",
        "plan": 207,
        "required_tests": L3_ANCHORS[2]["required_symbols"],
    },
]
BUILD_LINK = {
    "command": "make -C src/backend -j4",
    "exit_status": 0,
    "forbidden_objects": [
        "cluster_pcm_x_convert.o",
        "cluster_pcm_x_image_fetch.o",
    ],
    "manifest_path": "src/backend/cluster/Makefile",
    "required_objects": [
        "cluster_pcm_lock.o",
        "cluster_resource_x_identity.o",
        "cluster_resource_x_retry.o",
    ],
}
REMOVED_PATHS = [
    "src/backend/cluster/cluster_pcm_x_convert.c",
    "src/backend/cluster/cluster_pcm_x_image_fetch.c",
    "src/include/cluster/cluster_pcm_x_convert.h",
    "src/include/cluster/cluster_pcm_x_image_fetch.h",
    "src/test/cluster_unit/test_cluster_pcm_x_convert.c",
    "src/test/cluster_unit/test_cluster_pcm_x_image_fetch.c",
]


class CensusError(RuntimeError):
    """The census is stale, weakened, or contradicts the exact git trees."""


def canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def git(source_root: pathlib.Path, *args: str, binary: bool = False) -> str | bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(source_root), *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", b"")
        if isinstance(detail, bytes):
            detail = detail.decode("utf-8", errors="replace")
        raise CensusError(f"git census lookup failed: {detail.strip()}") from error
    if binary:
        return result.stdout
    return result.stdout.decode("utf-8").strip()


def load_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise CensusError(f"cannot read census manifest: {error}") from error
    if not isinstance(value, dict) or raw != canonical_json(value):
        raise CensusError("census manifest must be one canonical JSON object")
    return value


def tree_text(source_root: pathlib.Path, commit: str) -> tuple[list[str], str]:
    names = git(
        source_root,
        "ls-tree",
        "-r",
        "--name-only",
        commit,
        "--",
        "src/backend",
        "src/include",
    ).splitlines()
    paths = [
        name
        for name in names
        if name.endswith((".c", ".h")) or pathlib.PurePosixPath(name).name == "Makefile"
    ]
    chunks = []
    for path in paths:
        chunks.append(git(source_root, "show", f"{commit}:{path}"))
    return paths, "\n".join(chunks)


def current_product_text(
    source_root: pathlib.Path,
) -> tuple[list[str], str, dict[str, Any]]:
    names = git(
        source_root,
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
        "--",
        "src/backend",
        "src/include",
    ).splitlines()
    paths = []
    entries = []
    chunks = []
    for path in sorted(set(names)):
        if not (
            path.endswith((".c", ".h"))
            or pathlib.PurePosixPath(path).name == "Makefile"
        ):
            continue
        absolute = source_root / path
        if not absolute.exists():
            continue
        if not absolute.is_file():
            raise CensusError(f"current product path is not a regular file: {path}")
        blob = absolute.read_bytes()
        paths.append(path)
        entries.append({"path": path, "sha256": hashlib.sha256(blob).hexdigest()})
        try:
            chunks.append(blob.decode("utf-8"))
        except UnicodeDecodeError as error:
            raise CensusError(f"current product path is not UTF-8 text: {path}") from error
    snapshot = {
        "algorithm": "sha256-canonical-path-blob-v1",
        "path_count": len(entries),
        "sha256": hashlib.sha256(
            canonical_json(entries).encode("utf-8")
        ).hexdigest(),
    }
    return paths, "\n".join(chunks), snapshot


def require_exact_manifest(manifest: dict[str, Any]) -> None:
    if set(manifest) != {
        "build_link",
        "current_product_snapshot",
        "gates",
        "layers",
        "schema_version",
        "source_removed_subject",
    }:
        raise CensusError("census top-level fields are stale")
    if manifest["schema_version"] != SCHEMA:
        raise CensusError("census schema is stale")
    if manifest["current_product_snapshot"] != CURRENT_PRODUCT_SNAPSHOT:
        raise CensusError("current product snapshot identity is not exact")
    if manifest["source_removed_subject"] != {
        "commit": PRODUCT_COMMIT,
        "pre_removal_commit": L3_COMMIT,
        "pre_removal_tree": L3_TREE,
        "source_removal_commit": SOURCE_REMOVAL_COMMIT,
        "source_removal_tree": SOURCE_REMOVAL_TREE,
        "tree": PRODUCT_TREE,
    }:
        raise CensusError("source-removed subject is not exact")
    if manifest["layers"] != LAYERS:
        raise CensusError("three-layer graph rules were changed")
    if manifest["build_link"] != BUILD_LINK:
        raise CensusError("build/link evidence contract was changed")
    gates = manifest["gates"]
    expected_gates = {
        "L1": {"forbidden_patterns": L1_PATTERNS, "status": "GREEN"},
        "L2": {"forbidden_patterns": L2_PATTERNS, "status": "GREEN"},
        "L3": {
            "anchors": L3_ANCHORS,
            "checkpoint_commit": L3_COMMIT,
            "checkpoint_tree": L3_TREE,
            "status": "GREEN",
            "test_commands": L3_COMMANDS,
        },
    }
    if gates != expected_gates:
        raise CensusError("L1/L2/L3 gate contract was changed")


def validate(source_root: pathlib.Path, manifest_path: pathlib.Path) -> str:
    manifest = load_manifest(manifest_path)
    require_exact_manifest(manifest)
    if git(source_root, "rev-parse", f"{PRODUCT_COMMIT}^{{tree}}") != PRODUCT_TREE:
        raise CensusError("final source-removed product tree identity mismatch")
    if git(source_root, "rev-parse", f"{SOURCE_REMOVAL_COMMIT}^{{tree}}") != SOURCE_REMOVAL_TREE:
        raise CensusError("source-removed tree identity mismatch")
    if git(source_root, "rev-parse", f"{SOURCE_REMOVAL_COMMIT}^") != L3_COMMIT:
        raise CensusError("pre-removal L3 is not the direct source-removal parent")
    if git(source_root, "rev-parse", f"{L3_COMMIT}^{{tree}}") != L3_TREE:
        raise CensusError("pre-removal L3 tree identity mismatch")
    try:
        git(source_root, "merge-base", "--is-ancestor", SOURCE_REMOVAL_COMMIT, PRODUCT_COMMIT)
    except CensusError as error:
        raise CensusError("source-removal commit is not an ancestor of the final product") from error
    try:
        git(source_root, "merge-base", "--is-ancestor", PRODUCT_COMMIT, "HEAD")
    except CensusError as error:
        raise CensusError("final source-removed product is not an ancestor of HEAD") from error

    paths, production, current_snapshot = current_product_text(source_root)
    if current_snapshot != CURRENT_PRODUCT_SNAPSHOT:
        raise CensusError("current product bytes do not match the census snapshot")
    for path in REMOVED_PATHS:
        if (source_root / path).exists():
            raise CensusError(f"retired source path remains: {path}")
    for layer, contract in LAYERS.items():
        for pattern in contract["forbidden_patterns"]:
            if re.search(pattern, production):
                raise CensusError(f"{layer} legacy match remains: {pattern}")
        for anchor in contract["positive_anchors"]:
            anchor_path = source_root / anchor["path"]
            if not anchor_path.is_file():
                raise CensusError(f"{layer} positive anchor path is missing: {anchor['path']}")
            body = anchor_path.read_text(encoding="utf-8")
            if anchor["symbol"] not in body:
                raise CensusError(f"{layer} positive anchor is missing: {anchor['symbol']}")
    for gate_name, patterns in (("L1", L1_PATTERNS), ("L2", L2_PATTERNS)):
        for pattern in patterns:
            if re.search(pattern, production):
                raise CensusError(f"{gate_name} is nonzero: {pattern}")

    stale_body = (source_root / "src/backend/cluster/cluster_gcs_block.c").read_text(
        encoding="utf-8"
    )
    if len(re.findall(r"(?m)^gcs_block_legacy_pcm_x_stale_ingress\s*\(", stale_body)) != 1:
        raise CensusError("bounded stale-family ingress cardinality is not one")
    build_body = (source_root / BUILD_LINK["manifest_path"]).read_text(encoding="utf-8")
    for name in BUILD_LINK["forbidden_objects"]:
        if name in build_body:
            raise CensusError(f"retired object remains in build manifest: {name}")
    for name in BUILD_LINK["required_objects"]:
        if name not in build_body:
            raise CensusError(f"target object is missing from build manifest: {name}")

    for anchor in L3_ANCHORS:
        blob = git(source_root, "show", f"{L3_COMMIT}:{anchor['path']}", binary=True)
        if hashlib.sha256(blob).hexdigest() != anchor["blob_sha256"]:
            raise CensusError(f"pre-removal L3 blob mismatch: {anchor['path']}")
        text = blob.decode("utf-8")
        for symbol in anchor["required_symbols"]:
            if symbol not in text:
                raise CensusError(f"pre-removal L3 anchor missing: {symbol}")

    return hashlib.sha256(canonical_json(manifest).encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    args = parser.parse_args()
    try:
        digest = validate(args.source_root.resolve(), args.manifest.resolve())
    except CensusError as error:
        print(f"R11 source-removal census: RED: {error}", file=sys.stderr)
        return 1
    print(f"R11 source-removal census: GREEN: manifest_sha256={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
