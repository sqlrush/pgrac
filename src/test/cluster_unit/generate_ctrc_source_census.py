#!/usr/bin/env python3
"""Generate the closed CTRC source census from semantic source anchors.

The scanner removes comments and literals, attributes every anchor occurrence
to its enclosing C function, and refuses any owner that lacks an explicit
semantic classification.  It intentionally does not infer a class from a
directory, filename, or comment.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class ScanRule:
    category: str
    paths: Tuple[str, ...]
    pattern: str


@dataclass(frozen=True)
class Hit:
    category: str
    path: str
    owner: str


SCAN_RULES: Tuple[ScanRule, ...] = (
    ScanRule(
        "HEAP_PRODUCER_ENTRYPOINT",
        ("src/backend/access/heap/heapam.c",),
        r"\bheap_(?:insert|multi_insert|delete|update|lock_tuple|lock_updated_tuple_rec)\s*\(",
    ),
    ScanRule(
        "HEAP_RECEIPT_BOUNDARY",
        ("src/backend/access/heap/heapam.c",),
        r"\bcluster_heap_no_retry_boundary_apply\s*\(",
    ),
    ScanRule(
        "HEAP_ITL_ALLOC_REUSE",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/cluster/cluster_itl.c",
        ),
        r"\bcluster_(?:heap_itl_alloc_once|itl_alloc_or_reuse_(?:lock_)?slot)\s*\(",
    ),
    ScanRule(
        "HEAP_ITL_PUBLISH",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/cluster/cluster_itl.c",
        ),
        r"\bcluster_itl_stamp_active\s*\(",
    ),
    ScanRule(
        "HEAP_ITL_REGISTER",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/cluster/cluster_itl_touch.c",
        ),
        r"\bcluster_itl_touch_register_exact_ctrc\s*\(",
    ),
    ScanRule(
        "CURRENT_MX_HEAP_PREPARE",
        ("src/backend/access/heap/heapam.c",),
        r"\bcluster_current_mx_stamp_prepare_(?:plan|exact_publication)\s*\(",
    ),
    ScanRule(
        "CURRENT_MX_HEAP_PUBLISH",
        ("src/backend/access/heap/heapam.c",),
        r"\bcluster_current_mx_stamp_published\s*\(",
    ),
    ScanRule(
        "CURRENT_MX_RECOMPOSE",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/cluster/cluster_multixact_current.c",
        ),
        r"\bcluster_multixact_current_recompose\s*\(",
    ),
    ScanRule(
        "MULTIXACT_PUBLISHER",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/access/transam/multixact.c",
            "src/backend/cluster/cluster_multixact_current.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
        ),
        r"\bMultiXactId(?:Create|CreateFromMembers|CreateFromCurrentMembers|CreateLocalCurrentMembers|Expand)\s*\(",
    ),
    ScanRule(
        "HOT_PRUNE_FREEZE_REWRITE",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/access/heap/pruneheap.c",
            "src/backend/access/heap/vacuumlazy.c",
            "src/backend/access/heap/rewriteheap.c",
        ),
        r"\b(?:HeapTuple(?:Set|Clear)(?:HotUpdated|HeapOnly)|heap_page_prune(?:_opt|_execute)?|heap_(?:prepare_|execute_)?freeze_tuple|raw_heap_insert|rewrite_heap_tuple|heap_lock_updated_tuple_rec)\s*\(",
    ),
    ScanRule(
        "KO_PHYSICAL_REMOVAL",
        (
            "src/backend/catalog/storage.c",
            "src/backend/cluster/cluster_ko_lock.c",
            "src/backend/storage/smgr/smgr.c",
        ),
        r"\b(?:cluster_ko_flush_and_wait_ack|RelationDropStorage|RelationTruncate|smgrdounlinkall|smgrtruncate2|KO_FLUSH_ACK_DONE)\b",
    ),
    ScanRule(
        "ITL_TERMINAL_DISCHARGE",
        (
            "src/backend/cluster/cluster_itl_cleanout.c",
            "src/backend/cluster/cluster_itl_touch.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
        ),
        r"\b(?:cluster_itl_cleanout_lazy|cluster_itl_xact_(?:precommit|abort|subabort)_finish|cluster_ctrc_receipt_discharge_itl(?:_shared)?)\s*\(",
    ),
    ScanRule(
        "ITL_STATUS_WRITER",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/cluster/cluster_itl.c",
            "src/backend/cluster/cluster_itl_cleanout.c",
            "src/backend/cluster/cluster_itl_touch.c",
        ),
        r"(?:\b(?:slot|cslot)->flags|\bslot\.flags)\s*=\s*(?:terminal_flags\s*\[[^\]]+\]|is_lock_only\s*\?|ITL_FLAG_(?:FREE|ACTIVE|COMMITTED|ABORTED|NEEDS_CLEANOUT|LOCK_ONLY_ACTIVE|LOCK_ONLY_COMMITTED|LOCK_ONLY_ABORTED|LOCK_ONLY_XMAX_IS_MULTI))",
    ),
    ScanRule(
        "TT_STATUS_WRITER",
        (
            "src/backend/cluster/cluster_tt_durable.c",
            "src/backend/cluster/cluster_tt_slot.c",
            "src/backend/cluster/storage/cluster_undo_xlog.c",
        ),
        r"(?:\b(?:slot|successor|fresh|entry|s)->status|\b(?:slot|successor|fresh|entry)\.status)\s*=\s*(?:\(uint8\)\s*)?TT_SLOT_(?:ACTIVE|COMMITTED|ABORTED|RECYCLABLE|UNUSED)",
    ),
    ScanRule(
        "TT_RELEASE_FLAG_WRITER",
        (
            "src/backend/cluster/cluster_tt_durable.c",
            "src/backend/cluster/cluster_tt_slot.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
            "src/backend/cluster/storage/cluster_undo_xlog.c",
        ),
        r"\bTT_SLOT_FLAG_CTRC_RELEASE_PROVEN\b",
    ),
    ScanRule(
        "CURRENT_SLOT_GC",
        (
            "src/backend/cluster/cluster_tt_slot.c",
            "src/backend/cluster/cluster_undo_cleaner.c",
        ),
        r"\bcluster_tt_slot_gc_current_pass\s*\(",
    ),
    ScanRule(
        "ROLLED_SEGMENT_RECYCLE",
        (
            "src/backend/cluster/cluster_undo_cleaner.c",
            "src/backend/cluster/cluster_undo_record.c",
            "src/backend/cluster/storage/cluster_undo_alloc.c",
            "src/backend/cluster/storage/cluster_undo_block0_current.c",
            "src/backend/cluster/storage/cluster_undo_xlog.c",
        ),
        r"\bcluster_undo_(?:segment_advance_recyclable|segment_try_mark_recyclable|segment_reuse_in_place|segment_reuse_first_recyclable|emit_segment_recycle|redo_segment_recycle|redo_segment_reuse)\s*\(",
    ),
    ScanRule(
        "CURRENT_MX_PROOF_SENDER",
        (
            "src/backend/cluster/cluster_cr_server.c",
            "src/backend/cluster/cluster_gcs_block.c",
            "src/backend/cluster/cluster_multixact_current.c",
        ),
        r"\b(?:cluster_gcs_current_mx_member_proof_(?:serve_inline|fetch_and_wait)|cluster_multixact_current_resolve_origin_member_proof|gcs_block_current_mx_origin_advance)\s*\(",
    ),
    ScanRule(
        "CURRENT_MX_WIRE_DECODER",
        (
            "src/backend/cluster/cluster_cr_server.c",
            "src/backend/cluster/cluster_gcs_block.c",
            "src/backend/cluster/cluster_multixact_current_wire.c",
        ),
        r"\bcluster_multixact_current_wire_validate_(?:describe_(?:forward|reply)|proof_(?:forward|reply|reply_frame))\s*\(",
    ),
    ScanRule(
        "CTRC_TOUCH_BEFORE_PROOF",
        (
            "src/backend/cluster/cluster_gcs_block.c",
            "src/backend/cluster/cluster_runtime_visibility.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
        ),
        r"\bcluster_ctrc_origin_touch_exact\s*\(",
    ),
    ScanRule(
        "CTRC_RECEIPT_LIFECYCLE",
        (
            "src/backend/access/heap/heapam.c",
            "src/backend/cluster/cluster_itl_touch.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
            "src/backend/cluster/cluster_undo_record.c",
        ),
        r"\bcluster_ctrc_receipt_(?:prepare_shared|apply_shared|retarget_itl_shared|cancel_shared|discharge_itl_shared)\s*\(",
    ),
    ScanRule(
        "CTRC_CLOSE_WIRE",
        (
            "src/backend/cluster/cluster_gcs_block.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
        ),
        r"\bcluster_ctrc_(?:seal_request_(?:encode|decode)|seal_reply_(?:encode|decode)|participant_request_shared|origin_ack_land_shared)\s*\(",
    ),
    ScanRule(
        "CTRC_RELATION_GATE",
        (
            "src/backend/access/heap/rewriteheap.c",
            "src/backend/cluster/cluster_ko_lock.c",
            "src/backend/cluster/cluster_terminal_ref_census.c",
        ),
        r"\bcluster_ctrc_relation_removal_ready_(?:from_snapshot|shared)\s*\(",
    ),
    ScanRule(
        "CURRENT_MX_TERMINAL_CLEANOUT",
        ("src/backend/cluster/cluster_terminal_ref_census.c",),
        r"\b(?:cluster_ctrc_current_mx_rewrite_plan|cluster_ctrc_receipt_discharge_current_mx(?:_shared)?|cluster_ctrc_transfer_(?:note_successor_receipt|note_descriptor_durable|remove_predecessor)|ctrc_cleaner_prepare_current_mx_successor|ctrc_cleaner_clean_current_mx_receipt|HeapTupleHeaderSetXmax)\s*\(",
    ),
)


# Filled only with explicit owner-level decisions.  Each value is:
# class, reference kind, target kind, successor owner, discharge owner, test.
CLASSIFICATIONS: Dict[Tuple[str, str, str], Tuple[str, str, str, str, str, str]] = {}


def _classify_owners(
    category: str,
    path: str,
    owners: Sequence[str],
    source_class: str,
    reference_kind: str,
    target_kind: str,
    successor_owner: str,
    discharge_owner: str,
    focused_test: str,
) -> None:
    """Install fixed owner decisions; there are no wildcard classifications."""
    for owner in owners:
        key = (category, path, owner)
        if key in CLASSIFICATIONS:
            raise RuntimeError(f"duplicate CTRC classification: {key!r}")
        CLASSIFICATIONS[key] = (
            source_class,
            reference_kind,
            target_kind,
            successor_owner,
            discharge_owner,
            focused_test,
        )


_classify_owners(
    "CTRC_CLOSE_WIRE",
    "src/backend/cluster/cluster_gcs_block.c",
    (
        "cluster_gcs_ctrc_dispatch_close",
        "gcs_ctrc_dispatch_prepare",
        "gcs_block_try_ctrc_forward136",
        "gcs_block_try_land_ctrc_reply",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_CTRC_TARGET_KINDS",
    "NO_SUCCESSOR_ON_CLOSE",
    "cluster_ctrc_participant_request_apply",
    "MXA-T24",
)
_classify_owners(
    "CTRC_CLOSE_WIRE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    (
        "cluster_ctrc_origin_ack_land_shared",
        "cluster_ctrc_participant_request_shared",
        "cluster_ctrc_seal_reply_decode",
        "cluster_ctrc_seal_reply_encode",
        "cluster_ctrc_seal_request_decode",
        "cluster_ctrc_seal_request_encode",
        "ctrc_ack_matches_request",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_CTRC_TARGET_KINDS",
    "NO_SUCCESSOR_ON_CLOSE",
    "cluster_ctrc_participant_request_apply",
    "MXA-T24",
)

_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_stamp_apply_exact_publication",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_apply_exact_publication",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T22",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_stamp_prepare_slow",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_PAGE_PENDING_OFFNUM",
    "cluster_current_mx_stamp_apply_exact_publication",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T21",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_stamp_cancel",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_PAGE_PENDING_OFFNUM|CTRC_TARGET_EXACT_TID",
    "NO_SUCCESSOR_PREMUTATION_CANCEL",
    "cluster_ctrc_receipt_cancel_shared",
    "MXA-T24",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_itl_touch.c",
    ("itl_finish_flush_batch",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("cluster_ctrc_receipt_apply_shared", "cluster_ctrc_receipt_prepare_shared"),
    "REGISTERED_REFERENCE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_CTRC_TARGET_KINDS",
    "cluster_ctrc_receipt_apply_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared|cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T21",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("cluster_ctrc_receipt_retarget_itl_shared",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_PAGE_PENDING_ITL_SLOT|CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_ctrc_receipt_retarget_itl_shared",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T37",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    (
        "cluster_ctrc_receipt_cancel_shared",
        "cluster_ctrc_receipt_discharge_itl_shared",
        "ctrc_cleaner_cancel_successor_receipts",
        "ctrc_cleaner_clean_itl_receipt",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_CTRC_TARGET_KINDS",
    "NO_LIVE_SUCCESSOR_REMOVED",
    "cluster_ctrc_receipt_cancel_shared|cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("ctrc_cleaner_prepare_current_mx_successor",),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_RECOMPOSED_SURVIVOR",
    "CTRC_TARGET_EXACT_TID",
    "ctrc_cleaner_prepare_current_mx_successor",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_undo_record.c",
    ("cluster_undo_record_ctrc_apply_prepared", "cluster_undo_record_ctrc_prepare_pending"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_PAGE_PENDING_ITL_SLOT|CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "CTRC_RECEIPT_LIFECYCLE",
    "src/backend/cluster/cluster_undo_record.c",
    ("cluster_undo_record_cancel_prepared",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_PAGE_PENDING_ITL_SLOT|CTRC_TARGET_EXACT_ITL_SLOT",
    "NO_SUCCESSOR_PREMUTATION_CANCEL",
    "cluster_ctrc_receipt_cancel_shared",
    "MXA-T23",
)

_classify_owners(
    "CTRC_RELATION_GATE",
    "src/backend/access/heap/rewriteheap.c",
    ("begin_heap_rewrite",),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "NO_REFERENCE_TRANSFER_ALLOWED",
    "cluster_ctrc_relation_removal_ready_shared",
    "MXA-T27",
)
_classify_owners(
    "CTRC_RELATION_GATE",
    "src/backend/cluster/cluster_ko_lock.c",
    ("cluster_ko_drain_inbound_and_apply", "cluster_ko_flush_and_wait_ack"),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "NO_REFERENCE_TRANSFER_ALLOWED",
    "cluster_ctrc_relation_removal_ready_shared",
    "MXA-T27",
)
_classify_owners(
    "CTRC_RELATION_GATE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    (
        "cluster_ctrc_relation_removal_ready_from_snapshot",
        "cluster_ctrc_relation_removal_ready_shared",
    ),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "NO_REFERENCE_TRANSFER_ALLOWED",
    "cluster_ctrc_relation_removal_ready_shared",
    "MXA-T27",
)

_classify_owners(
    "CTRC_TOUCH_BEFORE_PROOF",
    "src/backend/cluster/cluster_gcs_block.c",
    ("gcs_block_current_mx_origin_sample_held",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)
_classify_owners(
    "CTRC_TOUCH_BEFORE_PROOF",
    "src/backend/cluster/cluster_runtime_visibility.c",
    ("cluster_runtime_visibility_current_owner_lookup_internal",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)
_classify_owners(
    "CTRC_TOUCH_BEFORE_PROOF",
    "src/backend/cluster/cluster_runtime_visibility.c",
    ("cluster_runtime_visibility_current_mx_updater_provenance_exact",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T40",
)
_classify_owners(
    "CTRC_TOUCH_BEFORE_PROOF",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("cluster_ctrc_origin_touch_exact",),
    "REGISTERED_REFERENCE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_CTRC_TARGET_KINDS",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared|cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T20",
)

_classify_owners(
    "CURRENT_MX_HEAP_PREPARE",
    "src/backend/access/heap/heapam.c",
    (
        "cluster_current_mx_stamp_prepare_exact_publication",
        "cluster_current_mx_stamp_prepare_existing",
        "cluster_current_mx_stamp_prepare_plan",
        "cluster_current_mx_stamp_prepare_update_pair",
        "heap_delete",
        "heap_lock_tuple",
        "heap_update",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_PAGE_PENDING_OFFNUM|CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_apply_exact_publication",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T21",
)
_classify_owners(
    "CURRENT_MX_HEAP_PUBLISH",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_stamp_published", "heap_delete", "heap_lock_tuple", "heap_update"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_apply_exact_publication",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T22",
)
_classify_owners(
    "CURRENT_MX_RECOMPOSE",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_authorize", "cluster_current_mx_compose_remote_single"),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_RECOMPOSED_SURVIVOR",
    "CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_prepare_plan",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_compose_remote_single",),
    "PROVEN_LOCAL_NONCLUSTER",
    "CURRENT_MX_PROOF_ONLY_DESCRIPTOR",
    "NO_HEAP_TARGET_IN_CONSTRUCTOR",
    "CALLER_BUILDS_REGISTERED_DESCRIPTOR",
    "NO_REFERENCE_TO_DISCHARGE",
    "MXA-T41",
)
_classify_owners(
    "CURRENT_MX_RECOMPOSE",
    "src/backend/cluster/cluster_multixact_current.c",
    ("cluster_multixact_current_recompose",),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_RECOMPOSED_SURVIVOR",
    "CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_prepare_plan",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)

_classify_owners(
    "CURRENT_MX_PROOF_SENDER",
    "src/backend/cluster/cluster_cr_server.c",
    ("cluster_gcs_current_mx_member_proof_serve_inline",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)
_classify_owners(
    "CURRENT_MX_PROOF_SENDER",
    "src/backend/cluster/cluster_gcs_block.c",
    (
        "cluster_gcs_current_mx_member_proof_fetch_and_wait",
        "gcs_block_current_mx_origin_advance",
        "gcs_block_current_mx_origin_after_release",
        "gcs_block_current_mx_origin_sample_held",
        "gcs_block_try_current_mx_forward128",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)
_classify_owners(
    "CURRENT_MX_PROOF_SENDER",
    "src/backend/cluster/cluster_multixact_current.c",
    (
        "cluster_multixact_current_members_resolve_internal",
        "cluster_multixact_current_resolve_origin_member_proof",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)

_classify_owners(
    "CURRENT_MX_WIRE_DECODER",
    "src/backend/cluster/cluster_cr_server.c",
    (
        "cluster_gcs_current_mx_describe_serve_inline",
        "cluster_gcs_current_mx_member_proof_serve_inline",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)
_classify_owners(
    "CURRENT_MX_WIRE_DECODER",
    "src/backend/cluster/cluster_gcs_block.c",
    (
        "cluster_gcs_current_mx_describe_fetch_and_wait",
        "cluster_gcs_current_mx_member_proof_fetch_and_wait",
        "gcs_block_current_mx_origin_try_accept",
        "gcs_block_try_land_current_mx_reply",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)
_classify_owners(
    "CURRENT_MX_WIRE_DECODER",
    "src/backend/cluster/cluster_multixact_current_wire.c",
    (
        "cluster_multixact_current_wire_validate_describe_forward",
        "cluster_multixact_current_wire_validate_describe_reply",
        "cluster_multixact_current_wire_validate_proof_forward",
        "cluster_multixact_current_wire_validate_proof_reply",
        "cluster_multixact_current_wire_validate_proof_reply_frame",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "cluster_ctrc_receipt_prepare_shared",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T20",
)

_classify_owners(
    "CURRENT_MX_TERMINAL_CLEANOUT",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    (
        "cluster_ctrc_current_mx_rewrite_plan",
        "cluster_ctrc_transfer_note_descriptor_durable",
        "cluster_ctrc_transfer_note_successor_receipt",
        "cluster_ctrc_transfer_remove_predecessor",
        "ctrc_cleaner_clean_current_mx_receipt",
        "ctrc_cleaner_clean_next_receipt",
        "ctrc_cleaner_current_mx_plan_header",
        "ctrc_cleaner_prepare_current_mx_successor",
    ),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_RECOMPOSED_SURVIVOR|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "ctrc_cleaner_prepare_current_mx_successor",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)
_classify_owners(
    "CURRENT_MX_TERMINAL_CLEANOUT",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    (
        "cluster_ctrc_receipt_discharge_current_mx",
        "cluster_ctrc_receipt_discharge_current_mx_shared",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_RECOMPOSED_SURVIVOR|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "TERMINAL_REWRITE_OR_PROVED_ABSENCE",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T25",
)

_classify_owners(
    "CURRENT_SLOT_GC",
    "src/backend/cluster/cluster_tt_slot.c",
    ("cluster_tt_slot_gc_current_pass",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T30",
)
_classify_owners(
    "CURRENT_SLOT_GC",
    "src/backend/cluster/cluster_undo_cleaner.c",
    ("undo_cleaner_run_pass",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T30",
)

_classify_owners(
    "HEAP_PRODUCER_ENTRYPOINT",
    "src/backend/access/heap/heapam.c",
    (
        "heap_delete",
        "heap_insert",
        "heap_lock_tuple",
        "heap_lock_updated_tuple",
        "heap_lock_updated_tuple_authoritative",
        "heap_lock_updated_tuple_rec",
        "heap_multi_insert",
        "heap_update",
        "simple_heap_delete",
        "simple_heap_insert",
        "simple_heap_update",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_PAGE_PENDING_ITL_SLOT|CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "HEAP_RECEIPT_BOUNDARY",
    "src/backend/access/heap/heapam.c",
    (
        "cluster_heap_no_retry_boundary_apply",
        "cluster_heap_test_no_retry_boundary",
        "heap_delete",
        "heap_insert",
        "heap_lock_tuple",
        "heap_lock_updated_tuple_rec",
        "heap_update",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA|CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_ITL_SLOT|CTRC_TARGET_EXACT_TID",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared|cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T22",
)
_classify_owners(
    "HEAP_ITL_ALLOC_REUSE",
    "src/backend/access/heap/heapam.c",
    (
        "cluster_heap_itl_alloc_once",
        "cluster_heap_itl_alloc_with_terminal_census",
        "cluster_heap_itl_plan_prepared_undo_target",
    ),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_PAGE_PENDING_ITL_SLOT|CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_itl_plan_prepared_undo_target",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "HEAP_ITL_ALLOC_REUSE",
    "src/backend/access/heap/heapam.c",
    ("cluster_heap_ctrc_stage_reusable_itl_receipt",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_PAGE_PENDING_ITL_SLOT|CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_ctrc_receipt_retarget_itl_shared",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T37",
)
_classify_owners(
    "HEAP_ITL_ALLOC_REUSE",
    "src/backend/cluster/cluster_itl.c",
    ("cluster_itl_alloc_or_reuse_lock_slot", "cluster_itl_alloc_or_reuse_slot"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_itl_plan_prepared_undo_target",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "HEAP_ITL_PUBLISH",
    "src/backend/access/heap/heapam.c",
    ("heap_delete", "heap_insert", "heap_update"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "HEAP_ITL_PUBLISH",
    "src/backend/cluster/cluster_itl.c",
    ("cluster_itl_stamp_active",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "HEAP_ITL_REGISTER",
    "src/backend/access/heap/heapam.c",
    ("heap_delete", "heap_insert", "heap_lock_tuple", "heap_lock_updated_tuple_rec", "heap_update"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "HEAP_ITL_REGISTER",
    "src/backend/cluster/cluster_itl_touch.c",
    ("cluster_itl_touch_register_exact_ctrc",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)

_classify_owners(
    "HOT_PRUNE_FREEZE_REWRITE",
    "src/backend/access/heap/heapam.c",
    (
        "heap_execute_freeze_tuple",
        "heap_freeze_execute_prepared",
        "heap_freeze_tuple",
        "heap_lock_updated_tuple",
        "heap_lock_updated_tuple_authoritative",
        "heap_lock_updated_tuple_rec",
        "heap_prepare_freeze_tuple",
        "heap_update",
        "heapgetpage",
    ),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_RECOMPOSED_SURVIVOR|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "REGISTERED_SUCCESSOR_OR_REFUSE_BEFORE_MUTATION",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)
_classify_owners(
    "HOT_PRUNE_FREEZE_REWRITE",
    "src/backend/access/heap/heapam.c",
    ("heap_xlog_freeze_page", "heap_xlog_prune"),
    "PROVEN_LOCAL_NONCLUSTER",
    "WAL_REPLAY_OF_PRECLASSIFIED_MUTATION",
    "RECOVERY_BUFFER_TARGET",
    "ORIGINATING_WAL_RECORD",
    "ORIGINATING_PRIMARY_CTRC_PATH",
    "MXA-T35",
)
_classify_owners(
    "HOT_PRUNE_FREEZE_REWRITE",
    "src/backend/access/heap/pruneheap.c",
    ("heap_page_prune", "heap_page_prune_internal", "heap_page_prune_execute", "heap_page_prune_opt"),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_RECOMPOSED_SURVIVOR|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "REGISTERED_UPDATE_SUCCESSOR_OR_REFUSE_BEFORE_MUTATION",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)
_classify_owners(
    "HOT_PRUNE_FREEZE_REWRITE",
    "src/backend/access/heap/rewriteheap.c",
    ("end_heap_rewrite", "raw_heap_insert", "rewrite_heap_tuple"),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "cluster_ctrc_relation_removal_ready_shared",
    "cluster_ctrc_relation_removal_ready_shared",
    "MXA-T27",
)
_classify_owners(
    "HOT_PRUNE_FREEZE_REWRITE",
    "src/backend/access/heap/vacuumlazy.c",
    ("lazy_scan_prune",),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_RECOMPOSED_SURVIVOR|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_EXACT_TID",
    "REGISTERED_UPDATE_SUCCESSOR_OR_REFUSE_BEFORE_MUTATION",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)

_classify_owners(
    "ITL_STATUS_WRITER",
    "src/backend/access/heap/heapam.c",
    ("heap_lock_tuple", "heap_lock_updated_tuple_rec"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_STATUS_WRITER",
    "src/backend/access/heap/heapam.c",
    ("cluster_heap_itl_apply_terminal_census",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_STATUS_WRITER",
    "src/backend/cluster/cluster_itl.c",
    ("cluster_itl_stamp_active", "cluster_itl_stamp_multixact_marker"),
    "REGISTERED_REFERENCE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "cluster_heap_no_retry_boundary_apply",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_STATUS_WRITER",
    "src/backend/cluster/cluster_itl.c",
    ("cluster_itl_stamp_aborted", "cluster_itl_stamp_committed"),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_STATUS_WRITER",
    "src/backend/cluster/cluster_itl_cleanout.c",
    ("cluster_itl_cleanout_lazy",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_STATUS_WRITER",
    "src/backend/cluster/cluster_itl_touch.c",
    ("itl_finish_stamp_page",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)

_classify_owners(
    "ITL_TERMINAL_DISCHARGE",
    "src/backend/cluster/cluster_itl_cleanout.c",
    ("cluster_itl_cleanout_lazy",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_TERMINAL_DISCHARGE",
    "src/backend/cluster/cluster_itl_touch.c",
    (
        "cluster_itl_xact_abort_finish",
        "cluster_itl_xact_precommit_finish",
        "cluster_itl_xact_subabort_finish",
        "itl_finish_flush_batch",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)
_classify_owners(
    "ITL_TERMINAL_DISCHARGE",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    (
        "cluster_ctrc_receipt_discharge_itl",
        "cluster_ctrc_receipt_discharge_itl_shared",
        "ctrc_cleaner_clean_itl_receipt",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "CTRC_REF_HEAP_ITL_UBA",
    "CTRC_TARGET_EXACT_ITL_SLOT",
    "TERMINAL_INDEPENDENT_ITL_PROJECTION",
    "cluster_ctrc_receipt_discharge_itl_shared",
    "MXA-T23",
)

_classify_owners(
    "KO_PHYSICAL_REMOVAL",
    "src/backend/catalog/storage.c",
    ("RelationDropStorage", "RelationTruncate", "smgrDoPendingDeletes"),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "NO_REFERENCE_TRANSFER_ALLOWED",
    "cluster_ko_flush_and_wait_ack",
    "MXA-T27",
)
_classify_owners(
    "KO_PHYSICAL_REMOVAL",
    "src/backend/catalog/storage.c",
    ("smgr_redo",),
    "PROVEN_LOCAL_NONCLUSTER",
    "WAL_REPLAY_OF_PRECLASSIFIED_MUTATION",
    "RECOVERY_STORAGE_TARGET",
    "ORIGINATING_WAL_RECORD",
    "ORIGINATING_PRIMARY_KO_GATE",
    "MXA-T35",
)
_classify_owners(
    "KO_PHYSICAL_REMOVAL",
    "src/backend/cluster/cluster_ko_lock.c",
    (
        "cluster_ko_drain_inbound_and_apply",
        "cluster_ko_flush_ack_handler",
        "cluster_ko_flush_and_wait_ack",
    ),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "NO_REFERENCE_TRANSFER_ALLOWED",
    "cluster_ctrc_relation_removal_ready_shared",
    "MXA-T27",
)
_classify_owners(
    "KO_PHYSICAL_REMOVAL",
    "src/backend/storage/smgr/smgr.c",
    ("smgrdounlinkall", "smgrtruncate2"),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "ALL_CTRC_REFERENCE_KINDS",
    "ALL_RELATION_TARGETS",
    "NO_REFERENCE_TRANSFER_ALLOWED",
    "cluster_ko_flush_and_wait_ack",
    "MXA-T27",
)
_classify_owners(
    "KO_PHYSICAL_REMOVAL",
    "src/backend/storage/smgr/smgr.c",
    ("smgrtruncate",),
    "PROVEN_LOCAL_NONCLUSTER",
    "NO_HEAP_TUPLE_REFERENCE",
    "FSM_OR_VISIBILITYMAP_FORK_ONLY",
    "NO_REFERENCE_TRANSFER_REQUIRED",
    "CALLER_OWNS_MAIN_FORK_KO_GATE",
    "MXA-T35",
)

_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/heap/heapam.c",
    ("FreezeMultiXactId",),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_RECOMPOSED_SURVIVOR",
    "CTRC_TARGET_EXACT_TID",
    "CTRC_CLEANER_SUCCESSOR_OR_NATIVE_LOCAL_ONLY",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/heap/heapam.c",
    ("cluster_current_mx_stamp_prepare_plan",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_PAGE_PENDING_OFFNUM|CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_apply_exact_publication",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T22",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/heap/heapam.c",
    ("compute_new_xmax_infomask",),
    "PROVEN_LOCAL_NONCLUSTER",
    "PG_NATIVE_LOCAL_MEMBER_SET",
    "CALLER_OWNED_HEAP_TARGET",
    "CURRENT_MX_BRANCH_BYPASSES_NATIVE_COMPOSER",
    "NATIVE_TRANSACTION_STATUS",
    "MXA-T35",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/transam/multixact.c",
    ("MultiXactIdCreate", "MultiXactIdCreateFromMembers", "MultiXactIdExpand"),
    "PROVEN_LOCAL_NONCLUSTER",
    "PG_NATIVE_DESCRIPTOR_ONLY",
    "NO_HEAP_TARGET_IN_CONSTRUCTOR",
    "CALLER_OWNS_REFERENCE_PUBLICATION",
    "CALLER_OWNS_REFERENCE_DISCHARGE",
    "MXA-T35",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/transam/multixact.c",
    ("MultiXactIdCreateLocalCurrentMembers",),
    "REGISTERED_REFERENCE",
    "CTRC_REF_CURRENT_MX_LOCKER|CTRC_REF_CURRENT_MX_UPDATER|CTRC_REF_RECOMPOSED_SURVIVOR|CTRC_REF_HOT_FOLLOW_EDGE",
    "CTRC_TARGET_PAGE_PENDING_OFFNUM|CTRC_TARGET_EXACT_TID",
    "cluster_current_mx_stamp_prepare_plan|ctrc_cleaner_prepare_current_mx_successor",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T22",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/access/transam/multixact.c",
    ("MultiXactIdCreateFromCurrentMembers",),
    "FAIL_CLOSED_UNREACHABLE",
    "CURRENT_MX_COMPATIBILITY_DESCRIPTOR",
    "NO_STAGE8_HEAP_CALLER",
    "NO_ELIGIBLE_STAGE8_CALLSITE",
    "NO_REFERENCE_TO_DISCHARGE",
    "MXA-T35",
)
_classify_owners(
    "MULTIXACT_PUBLISHER",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("ctrc_cleaner_prepare_current_mx_successor",),
    "SUCCESSOR_BEFORE_PREDECESSOR",
    "CTRC_REF_RECOMPOSED_SURVIVOR",
    "CTRC_TARGET_EXACT_TID",
    "ctrc_cleaner_prepare_current_mx_successor",
    "cluster_ctrc_receipt_discharge_current_mx_shared",
    "MXA-T27",
)

_classify_owners(
    "ROLLED_SEGMENT_RECYCLE",
    "src/backend/cluster/cluster_undo_cleaner.c",
    ("undo_cleaner_run_pass",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ROLLED_CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)
_classify_owners(
    "ROLLED_SEGMENT_RECYCLE",
    "src/backend/cluster/cluster_undo_record.c",
    ("claim_undo_extent", "cluster_undo_segment_advance_recyclable", "cluster_undo_tt_rollover_locked"),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ROLLED_CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)
_classify_owners(
    "ROLLED_SEGMENT_RECYCLE",
    "src/backend/cluster/storage/cluster_undo_alloc.c",
    ("cluster_undo_segment_reuse_in_place", "cluster_undo_segment_try_mark_recyclable"),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ROLLED_CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)
_classify_owners(
    "ROLLED_SEGMENT_RECYCLE",
    "src/backend/cluster/storage/cluster_undo_block0_current.c",
    ("cluster_undo_block0_current_live_owner_recycle_exact",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ROLLED_CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)
_classify_owners(
    "ROLLED_SEGMENT_RECYCLE",
    "src/backend/cluster/storage/cluster_undo_xlog.c",
    (
        "cluster_undo_emit_segment_recycle",
        "cluster_undo_redo",
        "cluster_undo_redo_segment_recycle",
        "cluster_undo_redo_segment_reuse",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "ROLLED_CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)

_classify_owners(
    "TT_RELEASE_FLAG_WRITER",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("cluster_ctrc_terminal_release_sample_exact", "ctrc_cleaner_publish_certificate"),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T30",
)
# This lexical category includes reads of the flag as well as writes. The
# capacity sampler is an observation only; no release producer is delegated.
_classify_owners(
    "TT_RELEASE_FLAG_WRITER",
    "src/backend/cluster/cluster_terminal_ref_census.c",
    ("ctrc_cleaner_terminal_sample_mode",),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "READ_ONLY_CAPACITY_OBSERVATION",
    "NO_RELEASE_MUTATION",
    "MXA-T30-capacity-native-bracket",
)
_classify_owners(
    "TT_RELEASE_FLAG_WRITER",
    "src/backend/cluster/cluster_tt_durable.c",
    ("cluster_tt_durable_redo_ctrc_release_slot_exact", "cluster_tt_terminal_transition_decide"),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "DURABLE_CTRC_RELEASE_CERTIFICATE",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)

_classify_owners(
    "TT_STATUS_WRITER",
    "src/backend/cluster/cluster_tt_durable.c",
    ("cluster_tt_slot_durable_publish_active",),
    "REGISTERED_REFERENCE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "cluster_ctrc_origin_open_shared",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T20",
)
_classify_owners(
    "TT_STATUS_WRITER",
    "src/backend/cluster/cluster_tt_durable.c",
    (
        "cluster_tt_durable_redo_abort_slot",
        "cluster_tt_durable_redo_abort_slot_exact",
        "cluster_tt_slot_durable_abort",
        "tt_slot_write_committed",
    ),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "DURABLE_TERMINAL_STATUS_WITH_RELEASE_BIT_CLEARED",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)
_classify_owners(
    "TT_STATUS_WRITER",
    "src/backend/cluster/storage/cluster_undo_xlog.c",
    ("cluster_tt_durable_redo_bind_slot",),
    "REGISTERED_REFERENCE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "cluster_ctrc_origin_open_shared",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T20",
)
_classify_owners(
    "TT_STATUS_WRITER",
    "src/backend/cluster/storage/cluster_undo_xlog.c",
    ("cluster_tt_durable_redo_stamp_slot", "cluster_tt_durable_redo_stamp_slot_exact"),
    "TERMINAL_PROJECTION_DISCHARGE",
    "ALL_CTRC_REFERENCE_KINDS",
    "CANONICAL_TT_SLOT",
    "DURABLE_TERMINAL_STATUS_WITH_RELEASE_BIT_CLEARED",
    "cluster_ctrc_terminal_release_sample_exact",
    "MXA-T33",
)


CONTROL_WORDS = {"if", "for", "while", "switch", "sizeof", "return"}


def strip_c_comments_and_literals(source: str) -> str:
    out: List[str] = []
    i = 0
    state = "code"
    while i < len(source):
        ch = source[i]
        nxt = source[i + 1] if i + 1 < len(source) else ""
        if state == "code":
            if ch == "/" and nxt == "/":
                out.extend((" ", " "))
                i += 2
                state = "line_comment"
                continue
            if ch == "/" and nxt == "*":
                out.extend((" ", " "))
                i += 2
                state = "block_comment"
                continue
            if ch == '"':
                out.append(" ")
                i += 1
                state = "string"
                continue
            if ch == "'":
                out.append(" ")
                i += 1
                state = "char"
                continue
            out.append(ch)
            i += 1
            continue
        if state == "line_comment":
            if ch == "\n":
                out.append("\n")
                state = "code"
            else:
                out.append(" ")
            i += 1
            continue
        if state == "block_comment":
            if ch == "*" and nxt == "/":
                out.extend((" ", " "))
                i += 2
                state = "code"
            else:
                out.append("\n" if ch == "\n" else " ")
                i += 1
            continue
        if ch == "\\" and i + 1 < len(source):
            out.append(" ")
            out.append("\n" if source[i + 1] == "\n" else " ")
            i += 2
            continue
        if (state == "string" and ch == '"') or (state == "char" and ch == "'"):
            out.append(" ")
            i += 1
            state = "code"
        else:
            out.append("\n" if ch == "\n" else " ")
            i += 1
    return "".join(out)


def function_name_before_brace(source: str, brace: int) -> Optional[Tuple[str, int]]:
    j = brace - 1
    while j >= 0 and source[j].isspace():
        j -= 1
    if j < 0 or source[j] != ")":
        return None
    depth = 1
    j -= 1
    while j >= 0 and depth:
        if source[j] == ")":
            depth += 1
        elif source[j] == "(":
            depth -= 1
        j -= 1
    if depth != 0:
        return None
    k = j
    while k >= 0 and source[k].isspace():
        k -= 1
    end = k + 1
    while k >= 0 and (source[k].isalnum() or source[k] == "_"):
        k -= 1
    name = source[k + 1 : end]
    if not name or name in CONTROL_WORDS:
        return None
    prefix_start = max(
        source.rfind(";", 0, k + 1),
        source.rfind("}", 0, k + 1),
        source.rfind("{", 0, k + 1),
    ) + 1
    if "=" in source[prefix_start : k + 1]:
        return None
    return name, k + 1


def function_spans(source: str) -> List[Tuple[int, int, str]]:
    spans: List[Tuple[int, int, str]] = []
    depth = 0
    active: Optional[Tuple[int, str]] = None
    for i, ch in enumerate(source):
        if ch == "{":
            if depth == 0:
                candidate = function_name_before_brace(source, i)
                if candidate is not None:
                    name, start = candidate
                    active = (start, name)
            depth += 1
        elif ch == "}":
            if depth == 0:
                raise ValueError("unbalanced closing brace")
            depth -= 1
            if depth == 0 and active is not None:
                start, name = active
                spans.append((start, i + 1, name))
                active = None
    if depth != 0:
        raise ValueError("unbalanced source braces")
    return spans


def owner_for_offset(spans: Sequence[Tuple[int, int, str]], offset: int) -> Optional[str]:
    for start, end, owner in spans:
        if start <= offset < end:
            return owner
    return None


def discover_hits() -> List[Hit]:
    cache: Dict[str, Tuple[str, List[Tuple[int, int, str]]]] = {}
    hits: List[Hit] = []
    for rule in SCAN_RULES:
        expression = re.compile(rule.pattern, re.MULTILINE)
        for relative_path in rule.paths:
            if relative_path not in cache:
                source = strip_c_comments_and_literals(
                    (ROOT / relative_path).read_text(encoding="utf-8")
                )
                cache[relative_path] = (source, function_spans(source))
            source, spans = cache[relative_path]
            for match in expression.finditer(source):
                owner = owner_for_offset(spans, match.start())
                if owner is not None:
                    hits.append(Hit(rule.category, relative_path, owner))
    return hits


def grouped_hits(hits: Iterable[Hit]) -> List[Tuple[Hit, int]]:
    counts: Dict[Hit, int] = {}
    for hit in hits:
        counts[hit] = counts.get(hit, 0) + 1
    return sorted(
        counts.items(), key=lambda item: (item[0].category, item[0].path, item[0].owner)
    )


def render_manifest(groups: Sequence[Tuple[Hit, int]]) -> str:
    lines = [
        "# owner_symbol\tpath\tsource_class\treference_kind\ttarget_kind\t"
        "successor_owner\tdischarge_owner\tscan_category\thit_count\tfocused_test"
    ]
    missing: List[Hit] = []
    for hit, count in groups:
        classification = CLASSIFICATIONS.get((hit.category, hit.path, hit.owner))
        if classification is None:
            missing.append(hit)
            continue
        source_class, reference_kind, target_kind, successor, discharge, test = classification
        lines.append(
            "\t".join(
                (
                    hit.owner,
                    hit.path,
                    source_class,
                    reference_kind,
                    target_kind,
                    successor,
                    discharge,
                    hit.category,
                    str(count),
                    test,
                )
            )
        )
    if missing:
        for hit in missing:
            print(f"UNCLASSIFIED\t{hit.category}\t{hit.path}\t{hit.owner}", file=sys.stderr)
        raise SystemExit(1)
    extras = sorted(set(CLASSIFICATIONS) - {
        (hit.category, hit.path, hit.owner) for hit, _ in groups
    })
    if extras:
        for category, path, owner in extras:
            print(f"STALE_CLASSIFICATION\t{category}\t{path}\t{owner}", file=sys.stderr)
        raise SystemExit(1)
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    groups = grouped_hits(discover_hits())
    if args.discover:
        for hit, count in groups:
            print(f"{hit.category}\t{hit.path}\t{hit.owner}\t{count}")
        return 0
    manifest = render_manifest(groups)
    if args.check is not None:
        try:
            current = args.check.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"cannot read {args.check}: {exc}", file=sys.stderr)
            return 1
        if current != manifest:
            print(f"CTRC source census is stale: {args.check}", file=sys.stderr)
            return 1
    elif args.output is not None:
        args.output.write_text(manifest, encoding="utf-8")
    else:
        sys.stdout.write(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
