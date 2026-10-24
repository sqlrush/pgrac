/*-------------------------------------------------------------------------
 * cluster_recovery_native_scan.h
 *    Owned native WAL effects for the cold recovery obligation graph.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/cluster/cluster_recovery_native_scan.h
 * NOTES
 *    PGRAC-original. Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_NATIVE_SCAN_H
#define CLUSTER_RECOVERY_NATIVE_SCAN_H

#ifdef USE_PGRAC_CLUSTER
#include "access/xact.h"
#include "access/xlogreader.h"
#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_wal_source.h"

typedef enum ClusterRecoveryNativeResult {
	CLUSTER_RECOVERY_NATIVE_MORE = 0,
	CLUSTER_RECOVERY_NATIVE_EFFECT,
	CLUSTER_RECOVERY_NATIVE_END,
	CLUSTER_RECOVERY_NATIVE_INVALID,
	CLUSTER_RECOVERY_NATIVE_SHAPE,
	CLUSTER_RECOVERY_NATIVE_UNSUPPORTED,
	CLUSTER_RECOVERY_NATIVE_CAPACITY,
	CLUSTER_RECOVERY_NATIVE_UNCHANGED,
	CLUSTER_RECOVERY_NATIVE_CHANGED
} ClusterRecoveryNativeResult;

typedef enum ClusterRecoveryNativeKind {
	CLUSTER_RECOVERY_NATIVE_TERMINAL = 1,
	CLUSTER_RECOVERY_NATIVE_ASSIGNMENT,
	CLUSTER_RECOVERY_NATIVE_INVALIDATION_BATCH,
	CLUSTER_RECOVERY_NATIVE_CREATE,
	CLUSTER_RECOVERY_NATIVE_SUBXACT,
	CLUSTER_RECOVERY_NATIVE_RELATION_DROP,
	CLUSTER_RECOVERY_NATIVE_STATS_DROP,
	CLUSTER_RECOVERY_NATIVE_INVALIDATION,
	CLUSTER_RECOVERY_NATIVE_TT_COMMIT,
	CLUSTER_RECOVERY_NATIVE_SPACE_DROP,
	CLUSTER_RECOVERY_NATIVE_UNDO, /* original undo WAL; observation only */
	CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE,
	CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER,
	CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_OFFSETS,
	CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_MEMBERS,
	CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE,
	CLUSTER_RECOVERY_NATIVE_CLOG_ZERO,
	CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE,
	CLUSTER_RECOVERY_NATIVE_COMMIT_TS_ZERO,
	CLUSTER_RECOVERY_NATIVE_COMMIT_TS_TRUNCATE,
	CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE,
	CLUSTER_RECOVERY_NATIVE_RELMAP_ENTRY,
	CLUSTER_RECOVERY_NATIVE_PREPARE,
	CLUSTER_RECOVERY_NATIVE_PREPARE_SUBXACT,
	CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_RELATION,
	CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_RELATION,
	CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_STATS,
	CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_STATS,
	CLUSTER_RECOVERY_NATIVE_PREPARE_INVALIDATION,
	CLUSTER_RECOVERY_NATIVE_PREPARE_RESOURCE,
	CLUSTER_RECOVERY_NATIVE_PREPARE_END
} ClusterRecoveryNativeKind;

/* Exact observed source and bytes, not a claim/cut authentication. Keep this
 * alongside every effect until the original cold owner revalidates all inputs.
 * xinfo retains ALL completion flags, including zero-message relcache work.
 * SUBXACT entries associate children with terminal/assignment top_xid; they
 * do not prove each immediate parent of a nested subtransaction chain.
 * Unknown invalidation IDs/stats kinds remain raw owner obligations; syntax
 * acceptance does not authorize applying them or interpreting a terminal. */
typedef struct ClusterRecoveryNativeRecord {
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
	XLogRecPtr read_lsn;
	XLogRecPtr end_lsn;
	XLogRecord header;
	RepOriginId record_origin;
	TransactionId toplevel_xid;
	const char *payload;
	uint32 payload_length;
	uint32 xinfo;
	TransactionId top_xid;
	uint32 subxacts_offset;			   /* bounded original array within owned payload */
	uint32 nsubxacts;				   /* membership, not the immediate parent chain */
	uint32 mappings_offset, nmappings; /* bounded original relmap entries */
	uint32 prepared_gid_offset, prepared_gid_length;
	uint32 prepared_resources_offset, prepared_resource_count; /* excludes END */
	Oid database;
	Oid tablespace;
	TimestampTz xact_time;
	SCN terminal_scn;
	xl_xact_tt_commit tt_commit; /* original parsed delta iff HAS_TT_COMMIT */
	xl_xact_origin origin;
	bool relcache_init_file;
} ClusterRecoveryNativeRecord;

typedef struct ClusterRecoveryNativeEffect {
	ClusterRecoveryNativeKind kind;
	uint32 ordinal; /* index within this effect family */
	const ClusterRecoveryNativeRecord *record;
	const void *bytes; /* possibly unaligned; use memcpy, valid until destroy */
	size_t length;
} ClusterRecoveryNativeEffect;

/* UNDO emits one ClusterUndoDecoded in bytes, while record->payload retains
 * the entire original main data, including images/deltas and certificates.
 * Field presence follows the original kind/version: zero in an absent
 * generation field is not evidence of generation zero. Legacy head/terminal
 * records still need their exact historical binding. No TT transition,
 * CTRC release, apply or recovery completion follows from this observation. */

/* MULTIXACT primary effects retain the complete original native payload:
 * CREATE (mid, member offset/count and array), the distinct offset/member
 * zero pages, and TRUNCATE (database and both wrapping ranges). Each CREATE
 * also emits every original MultiXactMember, with no online projection cap.
 * The source/claim/cut identifies the originating stream, never the local
 * node's SLRU or current epoch. A member is not a terminal proof. Physical
 * offsets/members, retention and heap identity joins remain separate; these
 * observations do not authorize applying, truncating or releasing data. */

typedef struct ClusterRecoveryNativeScan ClusterRecoveryNativeScan;

/* CLOG/COMMIT_TS ZERO and TRUNCATE keep their complete original native
 * payloads. The cutoff xid/database and page are observations, not FullXid,
 * status, timestamp-validity, retention or deletion-completion proofs.
 * ZERO targets only the original source's physical namespace; TRUNCATE
 * retains a separate lifecycle obligation. No shared commit_ts replay is
 * enabled by this inspector, and no SUBTRANS parent relationship is inferred. */

/* RELMAP_UPDATE retains the entire native image (including inactive slots);
 * RELMAP_ENTRY exposes each active original mapping with its exact ordinal.
 * Image CRC/shape acceptance is not authority-format compatibility, pending
 * publication, invalidation completion or RESET proof. */

/* PREPARE keeps both possible outcome arrays separately and every original
 * rmgr header/body/padding. RESOURCE bytes include the native on-disk header
 * and aligned body; END is the original sentinel. Known resource bodies are
 * opaque owner obligations, not validated TT/lock/transaction state. Empty
 * native GIDs remain observations even if a strict apply owner rejects them.
 * No callback, pending-state installation, terminal decision or completion
 * follows. Prepared completion records remain explicitly unsupported. */

/* Borrow an already verified reader only during begin; copy the entire main
 * payload under the explicit budget. No I/O, apply, authority or completion
 * is performed. The native parsers validate the complete shape before any
 * effect can escape. Original undo WAL uses the same owned record and pure
 * decoder/historical shape check. Prepared completions/unknown records and block
 * components refuse;
 * they require their own complete consumers, never an empty effect list. */
extern ClusterRecoveryNativeResult
cluster_recovery_native_scan_begin(XLogReaderState *reader, const ClusterWalSourceRef *source,
								   const RfContributorStreamCutV1 *cut, size_t memory_budget,
								   ClusterRecoveryNativeScan **out);
/* Re-observe the same complete source/cut/header/main bytes from a second
 * original reader pass. Any mismatch poisons the scan and blocks subsequent
 * effect export. UNCHANGED is byte/identity equality only: the caller must
 * also revalidate the complete ROOT/retention scope and every other graph
 * participant. It confers no new authority and does not rewind iteration. */
extern ClusterRecoveryNativeResult
cluster_recovery_native_scan_recheck(ClusterRecoveryNativeScan *scan, XLogReaderState *reader,
									 const ClusterWalSourceRef *source,
									 const RfContributorStreamCutV1 *cut);
/* One item per call, including a primary terminal/batch/create item when
 * its arrays are empty. END only drains this record, not the whole graph,
 * producer namespace, replay, ROOT completion, OPEN or GC. */
extern ClusterRecoveryNativeResult
cluster_recovery_native_scan_next(ClusterRecoveryNativeScan *scan,
								  ClusterRecoveryNativeEffect *out);
/* Read the owned primary observation without changing either effect cursor.
 * Valid after iteration ends, but never after a failed record recheck. */
extern ClusterRecoveryNativeResult
cluster_recovery_native_scan_primary(const ClusterRecoveryNativeScan *scan,
									 ClusterRecoveryNativeEffect *out);
/* Exact owned allocation size for the containing recovery inventory's
 * budget. This excludes allocator-internal/RSS overhead and grants no proof. */
extern size_t cluster_recovery_native_scan_bytes(const ClusterRecoveryNativeScan *scan);
extern void cluster_recovery_native_scan_destroy(ClusterRecoveryNativeScan *scan);

#endif
#endif
