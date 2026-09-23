#!/usr/bin/env python3
"""Offline fresh voting images and read-only deployment plans.

No command in this tool opens a block device for writing. An image is not a
formatted-device certificate. Runtime records must never pass the fresh check.

Author: SqlRush <sqlrush@gmail.com>
"""

import argparse
from functools import lru_cache
import hashlib
import json
import os
import stat
import struct

from common import (PreflightError, document_sha, load_json, publish_artifact,
                    publish_bytes, validate_profile)


MAX_NODES = 128
SLOT_BYTES = 512
CRC_OFFSET = 508
MAGIC = 0x51564F54
VERSION = 1
IMAGE_BYTES = (8 * MAX_NODES + 3) * SLOT_BYTES


def crc32c(data):
    """Frozen member-record Castagnoli CRC, not the IEEE CRC32 variant."""
    crc = 0xFFFFFFFF
    for value in data:
        crc ^= value
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def check_index(index):
    if type(index) is not int or not 0 <= index < 3:
        raise PreflightError("VOTING_INDEX_INVALID", "index")


@lru_cache(maxsize=3)
def _canonical_image(index):
    image = bytearray(IMAGE_BYTES)
    for node in range(MAX_NODES):
        offset = node * SLOT_BYTES
        struct.pack_into("<III", image, offset, MAGIC, VERSION, node)
        struct.pack_into("<I", image, offset + 48, index)
        struct.pack_into("<I", image, offset + CRC_OFFSET,
                         crc32c(image[offset:offset + CRC_OFFSET]))
    return bytes(image)


def canonical_image(index):
    check_index(index)  # Validate before caching: bool/float must not alias int.
    return _canonical_image(index)


def check_image(image, index):
    """Independently parse identity/CRC, then require every initial byte."""
    check_index(index)
    if len(image) != IMAGE_BYTES:
        raise PreflightError("VOTING_IMAGE_LENGTH", "image")
    for node in range(MAX_NODES):
        offset = node * SLOT_BYTES
        magic, version, stored_node = struct.unpack_from("<III", image, offset)
        stored_index = struct.unpack_from("<I", image, offset + 48)[0]
        if (magic, version, stored_node, stored_index) != (MAGIC, VERSION, node, index):
            raise PreflightError("VOTING_MEMBER_IDENTITY", "image")
        expected_crc = struct.unpack_from("<I", image, offset + CRC_OFFSET)[0]
        if crc32c(image[offset:offset + CRC_OFFSET]) != expected_crc:
            raise PreflightError("VOTING_MEMBER_CRC", "image")
        # generation/flags/SCN/marker/reserved bytes must all be initial zero.
        if any(image[offset + 12:offset + 48]) or any(image[offset + 52:offset + CRC_OFFSET]):
            raise PreflightError("VOTING_NOT_FRESH", "image")
    if any(image[MAX_NODES * SLOT_BYTES:]):
        raise PreflightError("VOTING_NOT_FRESH", "image")
    return {"state": "FRESH_INITIAL_IMAGE", "index": index, "bytes": IMAGE_BYTES,
            "sha256": hashlib.sha256(image).hexdigest(), "members": MAX_NODES}


def publish_image(path, index):
    image = canonical_image(index)
    check_image(image, index)
    return publish_bytes(path, image)


def read_image(path):
    """Image input is a bounded regular file, never a device or FIFO."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size != IMAGE_BYTES:
                raise PreflightError("VOTING_IMAGE_FILE", "image")
            return stream.read(IMAGE_BYTES + 1)
    except OSError as exc:
        raise PreflightError("VOTING_IMAGE_IO", "image", "ERROR") from exc


def make_plan(profile):
    validate_profile(profile)
    allowed = {entry["wwid"]: entry for entry in profile["authorization"]["device_allowlist"]}
    votes = []
    for vote in sorted(profile["votes"], key=lambda value: value["index"]):
        if not allowed[vote["wwid"]]["fresh"]:
            raise PreflightError("VOTING_FRESH_AUTHORIZATION_REQUIRED", "votes")
        if vote["size"] < IMAGE_BYTES or vote["size"] % SLOT_BYTES:
            raise PreflightError("VOTING_CAPACITY_INVALID", "votes")
        votes.append({**vote, "image_sha256": hashlib.sha256(canonical_image(vote["index"])).hexdigest()})
    return {"schema_version": 1, "status": "PLANNED_NOT_EXECUTED",
            "campaign_id": profile["campaign_id"], "profile_sha256": document_sha(profile),
            "deployment_qualified": False, "device_writes_enabled": False,
            "image_bytes": IMAGE_BYTES, "votes": votes,
            "pending": ["EXACT_DEVICE_IDENTITY", "NO_MOUNTS_OR_HOLDERS",
                        "ALL_DATABASES_STOPPED", "CAMPAIGN_NOT_STARTED",
                        "COMPLETE_RANGE_BLANK", "DIRECT_WRITE_FLUSH_READBACK",
                        "FOUR_GUEST_DIRECT_READBACK"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    image = commands.add_parser("image", help="create a new local canonical file; no device writes")
    image.add_argument("--index", type=int, choices=range(3), required=True)
    image.add_argument("--out", required=True)
    check = commands.add_parser("check-image", help="validate a local initial image, not runtime state")
    check.add_argument("--index", type=int, choices=range(3), required=True)
    check.add_argument("--image", required=True)
    plan = commands.add_parser("plan", help="publish an immutable, non-executed three-LUN plan")
    plan.add_argument("--profile", required=True)
    plan.add_argument("--out", required=True)
    args = parser.parse_args()
    try:
        if args.command == "image":
            digest = publish_image(args.out, args.index)
            result = {"status": "IMAGE_CREATED", "index": args.index,
                      "bytes": IMAGE_BYTES, "sha256": digest, "deployment_qualified": False}
        elif args.command == "check-image":
            result = {"status": "IMAGE_VALID", **check_image(read_image(args.image), args.index),
                      "deployment_qualified": False}
        else:
            result = make_plan(load_json(args.profile))
            digest = publish_artifact(args.out, result)
            result = {**result, "artifact_sha256": digest}
        print(json.dumps(result, sort_keys=True))
        return 0
    except PreflightError as exc:
        print(json.dumps({"status": exc.status, "reason": exc.reason,
                          "field": exc.field, "deployment_qualified": False}, sort_keys=True))
        return 3 if exc.status == "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
