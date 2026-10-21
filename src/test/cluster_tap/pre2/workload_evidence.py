"""Project producer fields from verified retained files, without joining events.

OBSERVED describes scalar values in one diagnostic projection. It establishes
neither a complete identity nor transport, authority, durability or cleanup.
The original trace, unknown flags and sequence order remain in the output.
"""
import argparse
import json
from pathlib import Path
import re

from workload_log import _fields
from workload_manifest import read_capture


def _same(names, kind="u64"):
    return {name: (name, kind) for name in names.split()}


TAG = dict(spcOid=("spc", "u32"), dbOid=("db", "u32"),
           relNumber=("rel", "u32"), forkNum=("fork", "u32"), blockNum=("block", "u32"))
KEY = _same("system_identifier origin_boot_incarnation formation_epoch "
            "admission_record_generation root_descriptor_incarnation root_id root_generation")
KEY.update(_same("origin_node_id owner_instance cluster_epoch segment_id "
                 "segment_generation slot_offset slot_wrap canonical_xid", "u32"))
RECEIPT = dict(_same("reservation_seq"), **_same("attempt target_ordinal", "u32"))
HANDLE = _same("participant_index receipt_index journal_slot_generation")
TARGET = dict(TAG, **_same("page_lsn page_scn"),
              **_same("itl_slot itl_wrap itl_xid offset itemid_flags", "u32"), uba=("uba", "hex16"))
PUBLICATION = _same("wire_request_id operation_id publication_boot "
                    "descriptor_hash publication_journal_sequence publication_key_sequence "
                    "publication_slot_generation")
PUBLICATION.update(_same("publication_attempt publication_grant publication_requester "
                        "publication_capability_generation member_ordinal member_role "
                        "reference_kind target_kind", "u32"))
PUBLICATION["publication_backend"] = ("publication_backend", "i32")

REPLY = {"request_id": ("reply_request_id", "u64"), "epoch": ("reply_epoch", "u64"),
         "sender": ("reply_sender", "i32"), "backend": ("reply_backend", "i32"),
         "transition": ("reply_transition", "u32"), **_same("forwarding_master status", "i32"),
         "page_lsn": ("page_lsn", "u64")}
CONSUME_KEY = dict(KEY, slot_offset=("tt_slot_offset", "u32"),
                   slot_wrap=("tt_wrap", "u32"), canonical_xid=("raw_xid", "u32"))
FINAL_TARGET = {name: ("final_" + source, kind) for name, (source, kind) in
                dict(TAG, **_same("page_lsn page_scn"),
                     **_same("itl_slot itl_wrap itl_xid", "u32")).items()}
DATA_TARGET = {name: ("data_" + source, kind) for name, (source, kind) in TAG.items()}
DATA_TARGET.update(offset=("data_offset", "u32"), uba=("consumed_uba_raw", "hex16"))
SIGNED_TAG = dict(TAG, forkNum=("fork", "i32"))

PACKED = {
    "tag": (("spcOid", "u32"), ("dbOid", "u32"), ("relNumber", "u32"),
            ("forkNum", "i32"), ("blockNum", "u32")),
    "tid": (("blockNum", "u32"), ("offsetNumber", "u32")),
    "uba_words": (("word0", "u64"), ("word1", "u64")),
    "mask": (("infomask", "u32"), ("infomask2", "u32")),
}
ITEM_SCHEMAS = {
    "item": (_same("lp", "u32"), ("item_known",)),
    "storage": (_same("off len"), ("item_known",)),
    "tuple": (dict(_same("xmin xmax raw_cid itl", "u32"),
                   ctid=("ctid", "tid"), mask=("mask", "mask"), combo=("combo", "bool")),
              ("item_known", "tuple_known")),
    "slot": (dict(_same("slot_xid wrap flags", "u32"),
                  **_same("write_scn commit_scn first_change_lsn"), uba_raw=("uba_raw", "uba_words")),
             ("item_known", "tuple_known", "slot_known")),
}
ITEM_IMAGES = {
    "HOT_FULL": {name: guard for name, guard in
                 (("before_root", "before_known"), ("before_queried", "before_known"),
                  ("full_root", "full_known"), ("full_queried", "full_known"))},
    "R4_HEAP_UPDATE": {name: name + "_page_known" for name in
                       ("before_old", "before_new", "after_old", "after_new")},
}

# These names and guards match the existing formatter projections. They do not
# describe a normalized phase chain. The legacy receipt has no sequence domain.
SCHEMAS = {
    "READ_IMAGE": {
        "request": (dict(_same("operation_request_id request_id request_epoch"),
                         attempt=("attempt", "i32"), transition_id=("transition_id", "u32")),),
        "tag": (TAG,), "reply": (REPLY, ("reply_known",)),
    },
    "READ_IMAGE_HOP": {
        "request": (dict(_same("request_id request_epoch"),
                         **_same("requester_node requester_backend master_node", "i32")),
                    ("forward_known", "reply_known"), True),
        "tag": (TAG, ("tag_known",)),
        "reply": (dict(_same("page_lsn"), **_same("reply_sender forwarding_master reply_status", "i32")),
                  ("reply_known",)),
    },
    "CTRC_RECEIPT": {
        "transaction_key": (KEY, ("canonical_key_known",)),
        "receipt": (RECEIPT, ("receipt_known",)),
        "target": (dict(TARGET, spcOid=("spc_oid", "u32"), dbOid=("db_oid", "u32"),
                        relNumber=("rel_number", "u32")), ("target_known",)),
    },
    "CTRC_RECEIPT_LIFECYCLE": {
        "transaction_key": (KEY, ("key_known",)),
        "receipt": (RECEIPT, ("receipt_known",)),
        "target": (TARGET, ("target_known",)),
        "handle": (HANDLE, ("handle_known", "handle_valid")),
        "publication": (PUBLICATION, ("publication_known",)),
    },
    "CTRC_CONSUME": {
        "transaction_key": (CONSUME_KEY, ("key_known",)),
        "handle": (HANDLE, ("key_known",)),
        "consume": (dict(_same("consumer_sequence"), **_same("attempt target_ordinal", "u32"),
                         route_retarget=("route_retarget", "bool")),),
        "final_target": (FINAL_TARGET,),
        "data_target": (DATA_TARGET, ("data_target_known",)),
    },
    "HOT_FULL": {
        "tag": (SIGNED_TAG,),
        "snapshot": (dict(_same("cycle read_scn read_epoch"),
                          **_same("root_block root_offset queried_offset snapshot_curcid", "u32")),),
        "build": (_same("build_result build_reason", "i32"), ("build_known",)),
        "search": (_same("candidate_kind", "i32"), ("search_known",)),
        **{name + "_page": ({"page_lsn": (name + "_lsn", "u64"),
                             "page_scn": (name + "_scn", "u64"),
                             "watermark": (name + "_watermark", "u64")}, (name + "_known",))
           for name in ("before", "full")},
    },
    "R4_HEAP_UPDATE": {
        "update": (dict(_same("raw_xid canonical_xid input_command_id", "u32"),
                        **_same("hot cross_page", "bool")),),
        "tids": (_same("requested_tid old_tid new_tid", "tid"),),
        "undo": (dict(uba_raw=("undo_uba_raw", "uba_words")),),
        "wal": (_same("wal_lsn"), ("wal_inserted",)),
        **{name + "_page": ({"tag": (tag, "tag"), "offset": (name + "_offset", "u32"),
                             "page_lsn": (name + "_lsn", "u64"),
                             "page_scn": (name + "_scn", "u64")}, (name + "_page_known",))
           for name, tag in (("before_old", "old_tag"), ("before_new", "new_tag"),
                             ("after_old", "after_old_tag"), ("after_new", "after_new_tag"))},
    },
    "R4_UNDO": {
        "tag": (SIGNED_TAG,),
        "builder": (dict(_same("slot_generation builder_incarnation edge_incarnation"),
                         **_same("slot builder_worker edge_worker edge_role", "u32"),
                         **_same("builder_pid edge_pid", "i32")),),
        "route": (dict(_same("read_scn formation_epoch activation_generation "
                             "master_authority_generation master_transition expected_page_scn "
                             "copied_page_lsn copied_page_scn foreign_request_id "
                             "origin_formation_epoch origin_tt_generation origin_authority_scn"),
                       **_same("real_master selected_holder", "i32")),),
        "walk": (dict(_same("candidate_cursor candidate_count covered_mask record_format "
                            "build_steps max_steps visited", "u32"),
                      have_previous=("have_previous", "bool")),),
        "locator": (dict(_same("locator_xid locator_tt_wrap locator_itl_kind locator_itl_slot", "u32"),
                         uba_raw=("locator_uba", "uba_words")), ("locator_known",)),
        "undo": (dict(uba_raw=("uba", "uba_words")), ("uba_known",)),
        "decoded_uba": ({name: ("uba_" + name, "u32") for name in
                         ("segment", "block", "tt_offset", "row_offset")}, ("uba_known", "uba_decoded")),
        "read_return": (_same("record_length"), ("read_return_known",)),
        "record": (dict(_same("record_type record_flags record_payload_length record_xid record_origin "
                              "record_tt_segment record_tt_slot record_tt_wrap_plus1", "u32"),
                        **_same("record_write_scn"), prev_uba_raw=("record_prev", "uba_words")),
                   ("record_header_known",)),
        "record_target": (dict({name: ("target_" + source, kind) for name, (source, kind) in
                                SIGNED_TAG.items()}, offset=("target_offset", "u32")),
                          ("record_header_known",)),
        "page": (_same("page_lsn page_scn page_watermark"), ("page_known",)),
        "page_itl": (dict(_same("page_itl_xid page_itl_wrap page_itl_flags", "u32"),
                          **_same("page_itl_write_scn"), uba_raw=("page_itl_uba", "uba_words")),
                     ("page_known", "page_itl_known")),
        "target_item": (dict(flags=("target_item_flags", "u32"),
                             offset=("target_item_offset", "u64"), length=("target_item_length", "u64")),
                        ("page_known", "target_item_known")),
        "target_tuple": (dict(xmin=("tuple_xmin", "u32"), xmax=("tuple_xmax", "u32"),
                              ctid=("tuple_ctid", "tid"), mask=("tuple_mask", "mask")),
                         ("page_known", "target_item_known", "target_tuple_known")),
        "locator_reason": (_same("locator_reason", "i32"), ("locator_reason_known",)),
    },
}


def _scalar(raw, kind):
    if kind in PACKED:
        parts = raw.split("/")
        if len(parts) != len(PACKED[kind]):
            raise ValueError("unexpected slash component count")
        return {name: _scalar(part, component_kind) for part, (name, component_kind) in
                zip(parts, PACKED[kind])}
    if kind == "hex16":
        if re.fullmatch(r"[0-9a-f]{32}", raw) is None:
            raise ValueError("not a 16-byte hex projection")
        return raw
    signed = kind == "i32"
    if re.fullmatch(r"-?[0-9]+" if signed else r"[0-9]+", raw) is None:
        raise ValueError("not a decimal projection")
    value = int(raw)
    low, high = ((-(1 << 31), (1 << 31) - 1) if signed else
                 (0, 1) if kind == "bool" else
                 (0, (1 << (32 if kind == "u32" else 64)) - 1))
    if not low <= value <= high:
        raise ValueError("projection outside formatter range")
    return value


def _group(fields, schema, guards=(), any_guard=False):
    sources = {name: source for name, (source, _) in schema.items()}
    raw = {source: fields[source] for source in sources.values() if source in fields}
    guard_values = {name: fields.get(name) for name in guards}
    missing = [name for name in (*guards, *sources.values()) if name not in fields]
    invalid = [name for name in guards if name in fields and fields[name] not in ("0", "1")]
    active = (not any(name in missing or name in invalid for name in guards) and
              ((any if any_guard else all)(fields[name] == "1" for name in guards)))
    values = {}
    if active:
        for name, (source, kind) in schema.items():
            if source not in fields:
                continue
            try:
                values[name] = _scalar(fields[source], kind)
            except ValueError:
                invalid.append(source)
    status = "INVALID" if invalid else "OBSERVED" if active and not missing else "UNKNOWN"
    return dict(status=status, values=values if status == "OBSERVED" else {},
                source_fields=sources, raw_values=raw, guards=guard_values,
                missing=missing, invalid=invalid)


def _item_groups(fields, name, page_guard):
    """Parse one owned ItemId snapshot, without borrowing from other images."""
    raw = fields.get(name)
    nested = {}
    malformed = False
    if raw is not None:
        try:
            if not (raw.startswith("{") and raw.endswith("}")):
                raise ValueError("expected ItemId brace projection")
            nested = {name + "." + key: value for key, value in _fields(raw[1:-1]).items()}
        except ValueError:
            malformed = True
    local = dict(nested, **{page_guard: fields[page_guard]} if page_guard in fields else {})
    groups = {}
    for suffix, (schema, guards) in ITEM_SCHEMAS.items():
        schema = {key: (name + "." + source, kind) for key, (source, kind) in schema.items()}
        group = _group(local, schema, (page_guard, *(name + "." + guard for guard in guards)))
        if raw is not None:
            group["raw_values"][name] = raw
        else:
            group["missing"].append(name)
        if malformed:
            group.update(status="INVALID", values={}, invalid=[name])
        groups[name + "_" + suffix] = group
    return groups


def read_evidence(manifest_path):
    """Read verified capture bytes once, retaining each producer's local view.

    No inventory candidate supplies a missing field. No cross-line comparison,
    sorting, deduplication, normalized contract event or acceptance verdict is
    produced. Numeric ranges validate formatter representations, not protocol
    validity. A key projection does not establish a FullXid or process identity.
    """
    result = read_capture(manifest_path)
    statuses = []
    for node in result["nodes"]:
        file_id = node["file_ids"]["stderr"]
        projected = []
        for index, observation in enumerate(node["trace"]["observations"]):
            fields = observation["fields"]
            family = fields["PGRAC_FAMILY"]
            schema = SCHEMAS.get(family)
            groups = {} if schema is None else {
                name: _group(fields, *description) for name, description in schema.items()}
            for name, guard in ITEM_IMAGES.get(family, {}).items():
                groups.update(_item_groups(fields, name, guard))
            status = ("UNSUPPORTED" if schema is None else
                      "INVALID" if any(group["status"] == "INVALID" for group in groups.values()) else
                      "PARTIAL" if any(group["status"] == "UNKNOWN" for group in groups.values()) else
                      "PROJECTED")
            statuses.append(status)
            source = dict(manifest_sha256=result["manifest"]["sha256"], stderr_file_id=file_id,
                          stderr_sha256=result["files"][file_id]["sha256"], observation_index=index)
            source.update({name: observation[name] for name in
                           ("line", "byte_offset", "byte_length", "sha256")})
            projected.append(dict(source=source, family=family, event=fields["event"],
                                  event_seq=int(fields["event_seq"]),
                                  sequence_domain=fields.get("sequence_domain"),
                                  identity_bound=False, status=status, groups=groups))
        node["producer_fields"] = projected
    result["projection_status"] = (
        "INVALID" if "INVALID" in statuses else
        "NOT_OBSERVED" if not any(status != "UNSUPPORTED" for status in statuses) else
        "PARTIAL" if any(status in ("PARTIAL", "UNSUPPORTED") for status in statuses) else "PROJECTED")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="one retained version-1 capture manifest")
    args = parser.parse_args()
    try:
        result = read_evidence(args.manifest)
    except ValueError as error:
        result = dict(file_binding="REJECTED", reason=str(error), identity_bound=False,
                      field_root="OPEN", ready_to_run=False)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if (result["file_binding"] == "REJECTED" or result["parse_status"] == "REJECTED" or
            result["projection_status"] == "INVALID"):
        return 1
    return 2 if result["projection_status"] == "NOT_OBSERVED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
