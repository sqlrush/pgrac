/*-------------------------------------------------------------------------
 *
 * cluster_cr_server.h
 *	  pgrac spec-6.12b — cross-instance CR-server data plane (Oracle shape).
 *
 *	  The spec-5.57 read-path coordinator fails a class-③ (runtime-warm
 *	  remote undo origin) CR construction closed with SQLSTATE 53R9G.
 *	  spec-6.12b adds the data plane behind that wall, Oracle-shaped: the
 *	  CR page is CONSTRUCTED AT THE ORIGIN (the instance whose undo holds
 *	  the newest candidate chains) and shipped as one page result, never
 *	  the raw undo blocks.
 *
 *	  Split policy (write_scn-DESC chain order, one origin per chain —
 *	  a chain is one transaction's undo, which lives in its home
 *	  instance's segments):
 *	    FULL     every candidate chain is server-home: the server peels
 *	             them all and ships the finished CR page.
 *	    PARTIAL  a server-home DESC prefix followed by a foreign-only
 *	             suffix: the server peels the prefix and ships; the
 *	             requester re-collects the remaining candidates from the
 *	             shipped page's ITL state and finishes locally.  (The
 *	             cross-chain peel order constraint — a row touched by txB
 *	             then txC must be peeled C→B — stays intact because the
 *	             suffix is strictly older than every applied prefix
 *	             chain.)
 *	    DENY     homes interleave (a server-home chain after a foreign
 *	             one): a one-page one-hop protocol cannot preserve the
 *	             DESC peel order, so the server refuses and the requester
 *	             keeps the unchanged 53R9G fail-closed (Rule 8.A: any
 *	             uncertainty refuses; never a wrong-order construction).
 *
 *	  Runtime split: the origin's LMON only VALIDATES + parks the request
 *	  in a shmem slot (light-work rule — construction walks undo I/O and
 *	  must never run in the IC dispatch loop); LMS constructs into the
 *	  slot; LMON ships the finished result on its next tick (the 72-byte
 *	  outbound ring cannot carry a page, and only LMON owns the IC
 *	  connections).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr_server.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_SERVER_H
#define CLUSTER_CR_SERVER_H

#include "c.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_runtime_visibility.h" /* ClusterLiveAuthority (spec-6.12i) */
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tt_durable.h" /* ClusterTTDurableResolve */
#include "cluster/cluster_undo_verdict.h" /* ClusterUndoVerdictResult (spec-5.22d D4-6) */

/* Split verdict for the server-side construction (see banner). */
typedef enum ClusterCrServerSplit {
	CLUSTER_CR_SPLIT_FULL = 0,
	CLUSTER_CR_SPLIT_PARTIAL = 1,
	CLUSTER_CR_SPLIT_DENY = 2
} ClusterCrServerSplit;

/*
 * cluster_cr_server_split_classify — pure policy: given the per-chain undo
 * origin node ids in write_scn-DESC order, classify the one-hop serve.
 * *out_prefix_len is the number of leading self-home chains the server may
 * peel (only meaningful for FULL / PARTIAL).  nchains == 0 classifies FULL
 * with prefix 0 (nothing to peel — the current copy IS the CR result).
 */
extern ClusterCrServerSplit cluster_cr_server_split_classify(const int32 *chain_origins,
															 int nchains, int32 self_node,
															 int *out_prefix_len);

/*
 * spec-7.1 D1 serve: pure verdict decision for the durable
 * XID_MATCH_INVALID_SCN case (our own xid matched but carries no stamped
 * commit_scn -- the delayed-cleanout window).  8.A: ONLY an explicit CLOG
 * abort upgrades to a positive ABORTED answer; a committed-but-unstamped,
 * in-flight, 2PC-prepared or crashed-without-abort xid stays fail-closed (we
 * never fabricate a commit_scn).  Pure (no shmem / lock / elog) so
 * cluster_unit exercises both branches standalone.
 */
typedef enum ClusterCrInvalidScnVerdict {
	CLUSTER_CR_INVALID_SCN_REFUSE = 0, /* no positive proof -> fail closed */
	CLUSTER_CR_INVALID_SCN_ABORTED = 1 /* CLOG proved abort -> positive invisible */
} ClusterCrInvalidScnVerdict;

extern ClusterCrInvalidScnVerdict cluster_cr_server_invalid_scn_verdict(bool clog_did_abort);

/*
 * TT-P013-RULE25-B: pure classifier for the narrow RESOLVED_SCN pre-commit
 * window.  Positive authority precedence is COMMITTED, ABORTED, IN_PROGRESS;
 * an unproved non-commit remains UNKNOWN_FAIL_CLOSED.  The runtime owns the
 * exact physical-binding and origin-liveness gates.
 */
extern ClusterUndoVerdictKind cluster_cr_server_resolved_scn_verdict(bool clog_did_commit,
																	 bool clog_did_abort,
																	 bool xid_is_in_progress);

/*
 * TT-P013-RULE25-B: exact live-proof conjunction.  The fresh-ref carrier's
 * TT slot is 1-based while the durable resolver reports a 0-based slot.
 */
extern bool cluster_cr_server_live_binding_exact(bool xid_is_mine, uint32 expected_segment_id,
												 uint32 expected_tt_slot_id, uint16 matched_segment,
												 uint16 matched_slot, bool xid_is_in_progress,
												 bool durable_binding_stable);

/*
 * TT-P013-RULE25-C0: pure positive-proof table for a complete committed-only
 * zero-match.  The runtime supplies raw (non-recursive) CLOG status booleans
 * sampled under the existing no-raw-reuse drain.  COMMITTED without an exact
 * SCN, SUB_COMMITTED (all raw booleans false), contradictory samples and every
 * carrier/window doubt remain UNKNOWN_FAIL_CLOSED.
 */
extern ClusterUndoVerdictKind cluster_cr_server_c0_zero_match_verdict(
	bool authoritative, bool xid_is_mine, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, bool no_raw_reuse_window, bool clog_is_committed,
	bool clog_is_aborted, bool clog_is_in_progress, bool xid_is_in_progress);

/* S8-815PRE-FRESHREF-C1B-01: pure exact-pair conjunction.  This classifier
 * can return only COMMITTED_EXACT or UNKNOWN_FAIL_CLOSED; the existing
 * COMMITTED_BOUND contract is intentionally unreachable here. */
extern ClusterUndoVerdictKind cluster_cr_server_freshref_c1b_pair_verdict(
	bool pair_request, bool xid_is_mine, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, bool no_raw_reuse_window, int raw_clog_status,
	ClusterTTDurableResolve resolve, uint16 matched_segment, uint16 matched_slot,
	SCN resolved_scn, SCN proposed_scn, bool retention_ok, SCN horizon_scn);
extern bool cluster_cr_server_freshref_c1b_pair_request_decode(
	const GcsBlockForwardPayload *fwd, int32 authenticated_source_node,
	int32 local_node, uint64 current_epoch, int max_backends,
	uint32 *segment_id, TransactionId *xid, uint32 *expected_tt_slot_id,
	SCN *proposed_scn);
extern bool cluster_cr_server_local_freshref_c1b_pair_exact(
	TransactionId xid, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, SCN proposed_scn, uint16 *out_wrap);

#ifdef USE_CLUSTER_UNIT
extern ClusterUndoVerdictKind cluster_cr_server_test_own_xid_verdict(
	TransactionId xid, uint32 expected_segment_id, uint32 expected_tt_slot_id,
	bool authoritative);
extern ClusterUndoVerdictResult cluster_cr_server_test_own_xid_pair_verdict(
	TransactionId xid, uint32 expected_segment_id, uint32 expected_tt_slot_id,
	SCN proposed_scn);
#endif

/*
 * LMS CR work slots (shmem, embedded in the cluster_lms region).
 *
 *	Slot lifecycle: FREE -(submit CAS)-> FILLING -(submit publish)->
 *	PENDING -(LMS drain)-> BUSY -(LMS result)-> READY -(LMON ship)->
 *	FREE.  Single-producer single-consumer per direction (LMON dispatch
 *	is single-threaded; LMS is one process), so an atomic state word per
 *	slot is the whole protocol.
 *
 *	PGRAC (spec-7.1 integration review): FILLING is the producer-only
 *	reservation.  The submit CAS must NOT land directly on PENDING: the
 *	drain CAS-acquires PENDING slots, so a drain racing the submitter's
 *	field stores would serve garbage, and the submitter's trailing
 *	publish store would then stomp the LMS's BUSY/READY (double serve /
 *	lost reply).  A producer killed between CAS and publish leaks the
 *	slot in FILLING — fail-closed (submits degrade to false = requester
 *	keeps 53R97), never a garbage serve.
 */
#define CLUSTER_LMS_CR_SLOTS 4

typedef enum ClusterLmsCrSlotState {
	CLUSTER_LMS_CR_FREE = 0,
	CLUSTER_LMS_CR_PENDING = 1,
	CLUSTER_LMS_CR_BUSY = 2,
	CLUSTER_LMS_CR_READY = 3,
	CLUSTER_LMS_CR_FILLING = 4, /* producer-reserved; fields not yet published */
	CLUSTER_LMS_CR_R4_QUEUED = 5,
	CLUSTER_LMS_CR_R4_BUILDING = 6,
	CLUSTER_LMS_CR_R4_NEED_UNDO = 7,
	CLUSTER_LMS_CR_R4_UNDO_INFLIGHT = 8,
	CLUSTER_LMS_CR_R4_UNDO_READY = 9,
	CLUSTER_LMS_CR_R4_READY_FULL = 10,
	CLUSTER_LMS_CR_R4_READY_RETRY = 11,
	CLUSTER_LMS_CR_R4_READY_FAIL = 12,
	CLUSTER_LMS_CR_R4_CANCELLED = 13,
	CLUSTER_LMS_CR_R4_SHIPPING = 14,
	CLUSTER_LMS_CR_R4_RECLAIMING = 15
} ClusterLmsCrSlotState;

/* Work-slot request kind (spec-6.12i extends the wave-b CR-only table). */
typedef enum ClusterLmsCrSlotKind {
	CLUSTER_LMS_SLOT_KIND_CR = 0,				 /* spec-6.12b CR construction */
	CLUSTER_LMS_SLOT_KIND_UNDO_FETCH = 1,		 /* spec-6.12i undo-TT block fetch */
	CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT = 2,		 /* spec-6.12i D-i4 complete-scan verdict */
	CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT = 3, /* spec-7.1 D3-b multi member verdict */
	CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD = 4
} ClusterLmsCrSlotKind;

StaticAssertDecl(CLUSTER_LMS_CR_FREE == 0, "legacy slot FREE ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_PENDING == 1, "legacy slot PENDING ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_BUSY == 2, "legacy slot BUSY ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_READY == 3, "legacy slot READY ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_FILLING == 4, "shared slot FILLING ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_QUEUED == 5, "R4 slot QUEUED ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_BUILDING == 6, "R4 slot BUILDING ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_NEED_UNDO == 7, "R4 slot NEED_UNDO ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_UNDO_INFLIGHT == 8, "R4 slot UNDO_INFLIGHT ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_UNDO_READY == 9, "R4 slot UNDO_READY ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_READY_FULL == 10, "R4 slot READY_FULL ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_READY_RETRY == 11, "R4 slot READY_RETRY ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_READY_FAIL == 12, "R4 slot READY_FAIL ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_CANCELLED == 13, "R4 slot CANCELLED ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_SHIPPING == 14, "R4 slot SHIPPING ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_R4_RECLAIMING == 15, "R4 slot RECLAIMING ordinal");
StaticAssertDecl(CLUSTER_LMS_SLOT_KIND_CR == 0, "legacy CR kind ordinal");
StaticAssertDecl(CLUSTER_LMS_SLOT_KIND_UNDO_FETCH == 1, "legacy fetch kind ordinal");
StaticAssertDecl(CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT == 2, "legacy verdict kind ordinal");
StaticAssertDecl(CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT == 3,
				 "legacy multi-verdict kind ordinal");
StaticAssertDecl(CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD == 4, "R4 CR-build kind ordinal");
StaticAssertDecl(CLUSTER_LMS_CR_SLOTS == 4, "R4 must not enlarge the slot table");

typedef struct ClusterR4CrOwnerStamp {
	uint64 edge_owner_incarnation;
	uint64 builder_incarnation;
	int32 edge_owner_pid;
	int32 builder_pid;
	uint8 edge_owner_worker_id;
	uint8 builder_worker_id;
	uint8 edge_owner_role;
	uint8 reserved[5];
} ClusterR4CrOwnerStamp;

StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, edge_owner_incarnation) == 0,
				 "R4 owner edge-incarnation offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, builder_incarnation) == 8,
				 "R4 owner builder-incarnation offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, edge_owner_pid) == 16,
				 "R4 owner edge-pid offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, builder_pid) == 20,
				 "R4 owner builder-pid offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, edge_owner_worker_id) == 24,
				 "R4 owner edge-worker offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, builder_worker_id) == 25,
				 "R4 owner builder-worker offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, edge_owner_role) == 26,
				 "R4 owner edge-role offset");
StaticAssertDecl(offsetof(ClusterR4CrOwnerStamp, reserved) == 27,
				 "R4 owner reserved offset");
StaticAssertDecl(sizeof(ClusterR4CrOwnerStamp) == 32,
				 "R4 owner stamp must remain exactly 32 bytes");

typedef struct ClusterR4CrSlotExtension {
	ClusterR4CrRouteProof route_proof;
	ClusterR4CrOwnerStamp owner;
	uint64 slot_generation;
	uint64 foreign_request_id;
	UBA foreign_uba;
	XLogRecPtr copied_page_lsn;
	SCN copied_page_scn;
	uint64 origin_formation_epoch;
	uint64 origin_live_hwm_lsn;
	uint64 origin_tt_generation;
	SCN origin_authority_scn;
	uint32 requester_capability_generation;
	uint32 master_capability_generation;
	int32 foreign_origin_node;
	uint32 foreign_segment_id;
	uint32 foreign_block_no;
	TransactionId foreign_xid;
	uint32 foreign_wrap;
	uint32 build_steps;
	uint16 foreign_tt_slot_offset;
	uint16 foreign_row_offset;
	uint8 terminal_reason;
	uint8 flags;
	uint8 reserved[26];
} ClusterR4CrSlotExtension;

StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, route_proof) == 0,
				 "R4 slot route-proof offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, owner) == 80,
				 "R4 slot owner-stamp offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, slot_generation) == 112,
				 "R4 slot generation offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_request_id) == 120,
				 "R4 slot foreign-request offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_uba) == 128,
				 "R4 slot foreign-UBA offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, copied_page_lsn) == 144,
				 "R4 slot copied-LSN offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, copied_page_scn) == 152,
				 "R4 slot copied-SCN offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, origin_formation_epoch) == 160,
				 "R4 slot origin-formation offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, origin_live_hwm_lsn) == 168,
				 "R4 slot reply-borne live-HWM offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, origin_tt_generation) == 176,
				 "R4 slot reply-borne TT-generation offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, origin_authority_scn) == 184,
				 "R4 slot reply-borne authority-SCN offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, requester_capability_generation) == 192,
				 "R4 slot requester-capability offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, master_capability_generation) == 196,
				 "R4 slot master-capability offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_origin_node) == 200,
				 "R4 slot foreign-origin offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_segment_id) == 204,
				 "R4 slot foreign-segment offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_block_no) == 208,
				 "R4 slot foreign-block offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_xid) == 212,
				 "R4 slot foreign-xid offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_wrap) == 216,
				 "R4 slot foreign-wrap offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, build_steps) == 220,
				 "R4 slot build-steps offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_tt_slot_offset) == 224,
				 "R4 slot foreign-TT-slot offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, foreign_row_offset) == 226,
				 "R4 slot foreign-row offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, terminal_reason) == 228,
				 "R4 slot terminal-reason offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, flags) == 229,
				 "R4 slot flags offset");
StaticAssertDecl(offsetof(ClusterR4CrSlotExtension, reserved) == 230,
				 "R4 slot reserved offset");
StaticAssertDecl(sizeof(ClusterR4CrSlotExtension) == 256,
				 "R4 slot extension must remain exactly 256 bytes");
StaticAssertDecl(MAXALIGN(sizeof(ClusterR4CrSlotExtension)) == 256,
				 "R4 slot extension must add no alignment padding");
StaticAssertDecl(MAXALIGN(BLCKSZ) == BLCKSZ,
				 "R4 foreign page must add no alignment padding");

typedef struct ClusterLmsCrSlot {
	pg_atomic_uint32 state;	 /* ClusterLmsCrSlotState */
	BufferTag tag;			 /* block identity */
	SCN read_scn;			 /* requester snapshot */
	uint64 request_id;		 /* echo for the reply slot match */
	uint64 epoch;			 /* HC73 freshness echo */
	int32 requester_node;	 /* direct-ship destination */
	int32 requester_backend; /* HC80 compound key echo */
	int32 reply_master_node; /* HC109 forwarding_master echo (== requester) */
	uint8 transition_id;	 /* echo (N->S) for the reply slot match */
	uint8 result_status;	 /* GcsBlockReplyStatus: CR_RESULT_FULL /
							   * CR_RESULT_PARTIAL / UNDO_TT_FETCH_RESULT /
							   * UNDO_VERDICT_RESULT / UNDO_MULTI_VERDICT_RESULT /
							   * DENIED_MASTER_NOT_HOLDER */
	uint8 req_kind;			 /* ClusterLmsCrSlotKind (spec-6.12i / spec-7.1) */
	/* spec-6.12i D-i1: undo address decoded from the synthetic tag at submit
	 * time, and the LMS-co-sampled live authority triple the ship path copies
	 * into the reply (epoch -> hdr.epoch, live_hwm -> hdr.page_lsn,
	 * tt_generation -> trailer).  Meaningful only for KIND_UNDO_FETCH /
	 * KIND_UNDO_VERDICT / KIND_UNDO_MULTI_VERDICT.  undo_xid is the D-i4
	 * verdict subject decoded from the widened watermark carrier at submit time
	 * (KIND_UNDO_VERDICT), or the asked-for MultiXactId in the same carrier
	 * width (KIND_UNDO_MULTI_VERDICT, spec-7.1 D3-b Q-D3b1). */
	uint32 undo_segment_id;
	uint32 undo_block_no;
	TransactionId undo_xid;
	/* spec-5.22f D6-7: the KIND_UNDO_VERDICT request was AUTHORITATIVE (origin
	 * chosen from the fresh ref's physical ITL binding, wire value 3) -> the
	 * serve skips the cluster_xid_is_mine stripe self-check and answers an
	 * underivable own xid over its durable-TT + CLOG authority.  false for the
	 * derived (recycled) path, which keeps the self-check (6.12i P0 guard). */
	bool undo_authoritative;
	/* spec-5.22d D4-6: the dead OWNER a kind-4 AUTHORITY verdict request asks
	 * about, decoded from tag.relNumber at submit time (in-memory only, not
	 * wire ABI).  -1 for every owner-served kind.  >= 0 switches the drain to
	 * the authority serve (triple check + block0 prove), which never consults
	 * this node's own TT. */
	int32 undo_owner;
	ClusterLiveAuthority undo_auth;
	char result_page[BLCKSZ]; /* the constructed CR page (FULL/PARTIAL), the
							   * fetched undo header block (UNDO_FETCH), the
							   * ClusterGcsUndoVerdictPage (UNDO_VERDICT), or the
							   * ClusterGcsUndoMultiVerdictPage (MULTI_VERDICT) */
	ClusterR4CrSlotExtension r4;
	char foreign_undo_page[BLCKSZ];
} ClusterLmsCrSlot;

StaticAssertDecl(sizeof(((ClusterLmsCrSlot *)0)->result_page) == BLCKSZ,
				 "legacy result_page must remain one block");
StaticAssertDecl(offsetof(ClusterLmsCrSlot, r4)
					 == offsetof(ClusterLmsCrSlot, result_page) + BLCKSZ,
				 "R4 metadata must immediately follow legacy result_page");
StaticAssertDecl(offsetof(ClusterLmsCrSlot, foreign_undo_page)
					 == offsetof(ClusterLmsCrSlot, r4) + 256,
				 "R4 foreign scratch must immediately follow metadata");
StaticAssertDecl(sizeof(ClusterLmsCrSlot)
					 == offsetof(ClusterLmsCrSlot, r4) + 256 + BLCKSZ,
				 "R4 slot increment must remain exactly 8448 bytes");
StaticAssertDecl(sizeof(ClusterR4CrSlotExtension) + BLCKSZ == 8448,
				 "R4 per-slot increment must remain exactly 8448 bytes");
StaticAssertDecl(CLUSTER_LMS_CR_SLOTS * (sizeof(ClusterR4CrSlotExtension) + BLCKSZ) == 33792,
				 "R4 four-slot increment must remain exactly 33792 bytes");

/* CR-server counter buckets (bumped into the ClusterCRShared region owned
 * by cluster_cr.c). */
typedef enum ClusterCrServerStat {
	CLUSTER_CR_SERVER_STAT_FULL = 0,
	CLUSTER_CR_SERVER_STAT_PARTIAL = 1,
	CLUSTER_CR_SERVER_STAT_DENIED = 2,
	CLUSTER_CR_SERVER_STAT_UNDO_SERVED = 3,			 /* spec-6.12i D-i1 origin serve */
	CLUSTER_CR_SERVER_STAT_UNDO_DENIED = 4,			 /* spec-6.12i D-i1 origin refuse */
	CLUSTER_CR_SERVER_STAT_VERDICT_SERVED = 5,		 /* spec-6.12i D-i4 verdict serve */
	CLUSTER_CR_SERVER_STAT_VERDICT_DENIED = 6,		 /* spec-6.12i D-i4 verdict refuse */
	CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_SERVED = 7, /* spec-7.1 D3-b multi member serve */
	CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_DENIED = 8, /* spec-7.1 D3-b multi member refuse */
	CLUSTER_CR_SERVER_STAT_FENCE_REFUSED = 9		 /* spec-7.3 D7 write-fenced -> refuse ship */
} ClusterCrServerStat;

extern void cluster_cr_server_stat_bump(ClusterCrServerStat which);

/* LMS-side construction entry (cluster_cr.c): full CR over a stable copy of
 * the current page, peeling only the self-home DESC prefix; throws on every
 * failure (the LMS drain converts throws into DENIED replies). */
extern void cluster_cr_construct_page_for_server(const char *cur_page, SCN read_scn, BufferTag tag,
												 char *dst_page, bool *out_partial);
extern ClusterCrBuildResult cluster_cr_build_on_holder(const BufferTag *tag, SCN read_scn,
										char dst[BLCKSZ],
										ClusterCrBuildReason *reason_out);

/* TARGET-only requester CR entry.  SOURCE retains its private historical
 * bool/PARTIAL path; this ABI exposes the closed R4 result/reason domains and
 * copies a page to dst only after a positive final TARGET recheck. */
extern ClusterCrBuildResult cluster_gcs_block_cr_fetch_and_wait(
	BufferTag tag, SCN read_scn, char dst[BLCKSZ],
	ClusterCrBuildReason *reason_out);

/* Shmem region registration (cluster_shmem.c registry). */
extern void cluster_cr_server_shmem_register(void);

/* LmsMain lifecycle: publish (entry) / retract (NULL, exit) the LMS wake
 * latch so the LMON submit path can cut the idle-poll latency. */
struct Latch;
extern void cluster_cr_server_publish_lms_latch(struct Latch *latch);

/* LMON dispatch side: park a validated CR request; false = no capacity /
 * data plane off (caller replies the fail-closed DENIED immediately). */
extern bool cluster_lms_cr_submit(const GcsBlockForwardPayload *fwd);

/*
 * R4 FORWARD96 holder-submit boundary.  The receive worker retains ownership
 * of receive_admission; the submitter may inspect/recheck it but must never
 * copy, clear, transfer or leave it.  FULL/NONE means the immutable holder
 * work was published, not that a finished CR page already exists.
 */
extern ClusterCrBuildResult cluster_lms_cr_submit_r4(
	const ClusterR4CrForwardPayload *forward,
	const ClusterSemanticAdmissionToken *receive_admission,
	uint32 requester_capability_generation,
	uint32 master_capability_generation,
	ClusterCrBuildReason *reason_out);

/* Worker-0 endpoint for one already authenticated status-24 foreign undo
 * response.  The caller retains every input; true means the exact correlated
 * R4 slot release-published UNDO_READY. */
extern bool cluster_cr_server_r4_land_foreign_undo(
	const ClusterICEnvelope *env, const GcsBlockReplyHeader *header,
	const char undo_page[BLCKSZ], const ClusterGcsUndoAuthTrailer *undo_auth);

/* Worker-0 process-local half of the D4 close proof: every retained build
 * context is canonical empty and no terminal/SHIPPING positive edge remains.
 * Shared stale-slot recovery remains LMON-only. */
extern bool cluster_cr_server_r4_worker0_drained(void);

/* LMON-only recovery after the exact current worker0 drain ACK. */
extern bool cluster_cr_server_r4_lmon_reclaim_closed(uint64 worker_incarnation,
												 uint64 generation);

#ifdef USE_CLUSTER_UNIT
extern bool cluster_cr_server_test_reserve_legacy_slot(ClusterLmsCrSlot *slot,
											uint32 reserved_state);
extern bool cluster_cr_server_test_r4_claim_queued(uint32 slot_index);
extern bool cluster_cr_server_test_r4_build_step(uint32 slot_index);
extern bool cluster_cr_server_test_r4_send_foreign_undo(uint32 slot_index);
extern bool cluster_cr_server_test_r4_freeze_foreign_generation(
	uint32 slot_index, uint32 physical_generation);
extern bool cluster_cr_server_test_r4_ship_terminal(uint32 slot_index);
extern void cluster_cr_server_test_r4_reset_contexts(void);
extern bool cluster_cr_server_test_r4_context_matches(
	uint32 slot_index, bool expect_present, uint64 slot_generation,
	uint64 builder_incarnation, const ClusterSemanticAdmissionToken *admission);
#endif

/* LMON dispatch side (spec-6.12i D-i1): park a validated undo-TT fetch
 * request; false = wave GUC off on this node / malformed synthetic tag / no
 * capacity (caller replies the fail-closed DENIED immediately — the
 * requester keeps its unchanged 53R97). */
extern bool cluster_lms_undo_fetch_submit(const GcsBlockForwardPayload *fwd);

/* LMON dispatch side (spec-6.12i D-i4 / spec-6.15 D4): park a validated
 * undo-verdict request (the asked-for xid rides the widened watermark
 * carrier; a carrier with non-zero upper 32 bits or a non-normal xid is
 * malformed).  false = wave GUC off on this node / malformed tag or carrier
 * / no capacity (caller replies the fail-closed DENIED immediately — the
 * requester keeps its unchanged 53R97). */
extern bool cluster_lms_undo_verdict_submit(const GcsBlockForwardPayload *fwd);

/* spec-5.22c D3-4: resolve an OWN xid into a verdict page over the complete
 * own durable TT + own CLOG.  Shared by the origin-served verdict
 * (lms_undo_verdict_serve) and the master==self local verdict resolve so the
 * two answers over one xid can never diverge (Rule 8.A).  The caller zeroes
 * the page buffer and owns any authority co-sampling; true = *v holds the
 * verdict, false = refuse (caller keeps 53R97 fail-closed).
 * ordinary_single is an explicit consumer permission, not the inverse of
 * the stripe-check relaxation: local fresh and C1b callers keep both false. */
extern bool cluster_lms_undo_verdict_fill_page(TransactionId xid, bool authoritative,
											   bool ordinary_single, ClusterGcsUndoVerdictPage *v);
/* LMON dispatch side (spec-7.1 D3-b): park a validated undo-MULTI-verdict
 * request (the asked-for MXID rides the widened watermark carrier; a carrier
 * with non-zero upper 32 bits or an invalid mxid is malformed).  false = wave
 * GUC off on this node / malformed tag or carrier / no capacity (caller
 * replies the fail-closed DENIED immediately — the requester keeps its
 * unchanged 53R97). */
extern bool cluster_lms_undo_multi_verdict_submit(const GcsBlockForwardPayload *fwd);

/* LMS main-loop side: construct every PENDING slot (errors become DENIED
 * results — fail-closed to the requester, never an LMS restart). */
extern void cluster_lms_cr_drain(void);

/* LMON tick side: ship every READY slot to its requester and free it. */
extern void cluster_lms_cr_ship_ready(void);

/*
 * spec-7.3 D6 — DATA-plane inline serve.  When the GCS block family is on the
 * DATA plane, the worker[shard] that received a GCS_BLOCK_FORWARD CR /
 * undo-fetch / undo-verdict request serves it inline (validate + construct /
 * scan under the PG_TRY -> DENIED envelope) and ships the reply on its own
 * DATA channel — no shmem slot, no worker-0 poll, no 100 ms latency.  kind
 * selects the branch; a refused / failed request ships an immediate
 * fail-closed DENIED (requester keeps 53R9G / 53R97, Rule 8.A).  The caller
 * routes here ONLY when cluster_gcs_block_family_on_data_plane(); on the
 * CONTROL plane the light-work park path (submit) is used instead.  Always
 * ships exactly one reply, so the caller does not itself reply on refusal.
 */
extern void cluster_gcs_block_forward_serve_inline(const GcsBlockForwardPayload *fwd,
											   ClusterLmsCrSlotKind kind);
extern ClusterMxDescribeResult cluster_gcs_current_mx_describe_fetch_and_wait(
	int32 origin_node, const ClusterCurrentMxKey *key, ClusterCurrentMxMemberDesc *members,
	uint16 members_cap, uint16 *members_count, uint32 *reported_total_members);
extern void cluster_gcs_current_mx_describe_serve_inline(
	const struct ClusterICEnvelope *env, const void *payload);
struct ClusterCurrentMxProofForwardV2;
extern void cluster_gcs_current_mx_member_proof_serve_inline(
	const struct ClusterICEnvelope *env, const void *payload);
struct ClusterCurrentMxProofReplyPage;
extern ClusterMxResolveResult cluster_cr_server_current_mx_build_proof_page(
	uint16 source_node_id,
	const struct ClusterCurrentMxProofForwardV2 *request,
	ClusterMxResolveResult result, uint32 requester_capability_generation,
	const ClusterCurrentMemberProof *proofs,
	uint16 proof_count, const ClusterCurrentUpdaterProof *updater_proof,
	struct ClusterCurrentMxProofReplyPage *page);
#ifdef USE_CLUSTER_UNIT
struct ClusterCurrentMxDescribeReplyPage;
extern ClusterMxDescribeResult
cluster_cr_server_test_current_mx_build_describe_page(
	uint16 source_node_id, uint64 request_id, const ClusterCurrentMxKey *key,
	const MultiXactMember *native_members, int native_count,
	struct ClusterCurrentMxDescribeReplyPage *page);
#endif
extern ClusterMxResolveResult cluster_gcs_current_mx_member_proof_fetch_and_wait(
	int32 origin_node, struct ClusterCurrentMxProofForwardV2 *request,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out, TimestampTz deadline);
#ifdef USE_CLUSTER_UNIT
extern ClusterMxResolveResult
cluster_cr_server_test_current_mx_build_proof_page(
	uint16 source_node_id,
	const struct ClusterCurrentMxProofForwardV2 *request,
	ClusterMxResolveResult result, uint32 requester_capability_generation,
	const ClusterCurrentMemberProof *proofs,
	uint16 proof_count, const ClusterCurrentUpdaterProof *updater_proof,
	struct ClusterCurrentMxProofReplyPage *page);
#endif

typedef enum ClusterR4SourceCrOp { CLUSTER_R4_SOURCE_CR_FETCH = 0 } ClusterR4SourceCrOp;

typedef struct ClusterR4SourceCrRequest {
	BufferTag tag;
	SCN read_scn;
	int32 origin_node;
	char *dst_page;
} ClusterR4SourceCrRequest;

typedef struct ClusterR4SourceCrResult {
	bool fetched;
	bool partial;
} ClusterR4SourceCrResult;

extern ClusterSemanticAdmissionResult
cluster_r4_source_cr_dispatch(ClusterR4SourceCrOp op, const ClusterR4SourceCrRequest *request,
							  ClusterR4SourceCrResult *result);

/* Requester side (backend, spec-6.12i D-i1): fetch origin_node's TT-bearing
 * undo header block (segment_id, block_no) over the same wire, together with
 * the co-sampled live authority triple.  On success copies the shipped block
 * into dst_page, fills *auth_out and returns true; false = fail-closed
 * (timeout / DENIED / checksum / trailer missing — caller keeps the
 * unchanged 53R97 refusal, Rule 8.A). */
extern bool cluster_gcs_block_undo_tt_fetch_and_wait(int32 origin_node, uint32 segment_id,
													 uint32 block_no, char *dst_page,
													 ClusterLiveAuthority *auth_out);

/* Requester side (backend, spec-6.12i D-i4 / spec-6.15 D4): ask origin_node
 * for a COMPLETE own-TT by-xid verdict on `xid` over the same wire, together
 * with the co-sampled live authority triple.  On success copies the
 * validated verdict page into *verdict_out (magic / version / xid echo /
 * kind-field consistency already vetted via
 * cluster_vis_undo_verdict_page_usable), fills *auth_out and returns true;
 * false = fail-closed (timeout / DENIED / checksum / trailer missing /
 * malformed page — caller keeps the unchanged 53R97 refusal, Rule 8.A).
 * expected_tt_slot_id is the authoritative fresh ref's exact 1-based slot
 * binding (0 for terminal-only callers).  It rides the existing synthetic
 * tag block-number field and does not change the wire layout.
 * The caller MUST Lamport-observe verdict_out->horizon_scn (and any
 * commit_scn) it consumes — SCNs that crossed the wire (AD-008). */
extern bool cluster_gcs_block_undo_verdict_fetch_and_wait(int32 origin_node, uint32 segment_id,
														  uint32 expected_tt_slot_id,
														  TransactionId xid, bool authoritative,
														  ClusterGcsUndoVerdictPage *verdict_out,
														  ClusterLiveAuthority *auth_out);
extern bool cluster_gcs_block_undo_freshref_c1b_pair_fetch_and_wait(
	int32 origin_node, uint32 segment_id, uint32 expected_tt_slot_id,
	TransactionId xid, uint32 ref_epoch, SCN proposed_scn,
	ClusterGcsUndoVerdictPage *verdict_out, ClusterLiveAuthority *auth_out);

/* Requester side (backend, spec-5.22d D4-6): ask the elected serve AUTHORITY
 * (a live survivor — NOT the dead owner) for a block0-proven verdict on the
 * dead owner_node's `xid` (kind-4 wire; owner rides in tag.relNumber).  The
 * caller has already gated on the peer's HELLO D4 capability.  On success
 * *out holds the mapped COMMITTED_EXACT / ABORTED verdict; false or an
 * UNKNOWN_FAIL_CLOSED kind = fail-closed (timeout / DENIED / checksum /
 * trailer missing / wrong sender / epoch moved / malformed or v1 page —
 * caller keeps the 53R97 refusal, Rule 8.A).  The caller MUST Lamport-
 * observe any commit_scn it consumes (AD-008). */
extern bool cluster_gcs_block_undo_authority_verdict_fetch_and_wait(int32 authority_node,
																	int32 owner_node,
																	uint32 segment_id,
																	TransactionId xid,
																	ClusterUndoVerdictResult *out);
/* Requester side (backend, spec-7.1 D3-b): ask origin_node for a batched
 * member verdict on the foreign multixact `mxid` over the same wire.  On
 * success copies the structurally-validated (cluster_vis_undo_multi_verdict_
 * page_usable) SERVED page into page_out (a BLCKSZ, 8-byte-aligned buffer),
 * Lamport-observes every member SCN that crossed the wire (AD-008), fills
 * *auth_out and returns true; false = fail-closed (timeout / DENIED / checksum
 * / trailer missing / non-SERVED / malformed page — caller keeps the unchanged
 * 53R97, Rule 8.A). */
extern bool cluster_gcs_block_undo_multi_verdict_fetch_and_wait(int32 origin_node, MultiXactId mxid,
																char *page_out,
																ClusterLiveAuthority *auth_out);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_CR_SERVER_H */
