/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block.h
 *	  pgrac cluster GCS block-shipping substrate (Cache Fusion data plane).
 *
 *	  spec-2.33 activates cross-node 8KB block shipping on top of the
 *	  spec-2.32 GCS control plane (request/reply framework).  Wire opcodes
 *	  PGRAC_IC_MSG_GCS_BLOCK_REQUEST=14 / PGRAC_IC_MSG_GCS_BLOCK_REPLY=15
 *	  carry a 64B request and a 48B header + 8192B page payload, gated by
 *	  the I-WAL-before-ship invariant (master XLogFlush(page_lsn) before
 *	  shipping bytes).
 *
 *	  Scope (FROZEN v0.4):
 *	    - Wire ABI definition (GcsBlockRequestPayload 64B /
 *	      GcsBlockReplyHeader 48B + 8192B block_data)
 *	    - GcsBlockReplyStatus enum (GRANTED / STORAGE_FALLBACK / 4 DENIED /
 *	      DENIED_MASTER_NOT_HOLDER)
 *	    - Sender API cluster_gcs_send_block_request_and_wait (BufferDesc-aware)
 *	    - Master-side handler cluster_gcs_handle_block_request_envelope
 *	      (XLogFlush(page_lsn) before ship + revalidate + memcpy 8192B)
 *	    - Sender-side handler cluster_gcs_handle_block_reply_envelope
 *	      (checksum verify + memcpy + PageSetLSN)
 *	    - postmaster-once registration of msg_type 14/15
 *	    - 4 NEW wait events (BLOCK_REQUEST / BLOCK_REPLY / BLOCK_CHECKSUM_FAIL
 *	      / BLOCK_TIMEOUT) + cluster.gcs_reply_timeout_ms PGC_SUSET GUC
 *
 *	  Forward-link spec-2.34+:
 *	    - Retransmit + reconfig epoch cascading invalidation
 *	    - PI buffer copy + dirty-downgrade-with-writeback (spec-2.35)
 *	    - CF 2-way S-to-S read sharing (spec-2.35)
 *	    - CR / MVCC visibility coupling (spec-2.37+ AD-006 round 5)
 *
 *	  HC contracts in this header (HC79-HC89 11 NEW):
 *	    HC79 NEW msg_type 14/15;  spec-2.32 12/13 untouched
 *	    HC80 wire sizes 64B / 48B / 8192B;  reply key = (backend_id, request_id)
 *	    HC81 deterministic hash mod-N over declared node_id array (sparse safe)
 *	    HC82 master-side XLogFlush(page_lsn) BEFORE block bytes ship
 *	    HC83 CRC32C checksum mandatory; fail-closed; receiver must verify
 *	    HC84 PageSetLSN(page, reply.page_lsn) under content_lock EXCLUSIVE
 *	    HC85 reply timeout via cluster.gcs_reply_timeout_ms PGC_SUSET
 *	    HC86 retransmit deferred to spec-2.34
 *	    HC87 reconfig cascading invalidation deferred to spec-2.34
 *	    HC88 master-not-holder state=N → GRANTED_STORAGE_FALLBACK;
 *	         state != N → DENIED_MASTER_NOT_HOLDER fail-closed;
 *	         transition mutation must NOT precede this decision
 *	    HC89 revalidation single-retry; retry exhausted → fail-closed;
 *	         unbounded loop forbidden (hot-page starvation defense);
 *	         0-retry fail-closed forbidden (normal LSN drift false positive)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_gcs_block.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.33-gcs-block-shipping-substrate.md (FROZEN v0.4)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_BLOCK_H
#define CLUSTER_GCS_BLOCK_H

#include "c.h"
#include "cluster/cluster_pcm_lock.h" /* PcmLockTransition */
#include "storage/block.h"			  /* BLCKSZ */
#include "storage/buf_internals.h"	  /* BufferTag, BufferDesc */

#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * GCS_BLOCK_DATA_SIZE -- block bytes carried in every reply.
 *
 *  Locked to BLCKSZ at compile time; StaticAssertDecl in cluster_gcs_block.c
 *  enforces equality.  HC80 anchors this at 8192B per spec-2.33 v0.4.
 * ============================================================ */
#define GCS_BLOCK_DATA_SIZE 8192


/* ============================================================
 * GcsBlockReplyStatus -- reply status code carried in
 * GcsBlockReplyHeader.status (HC83 + HC88).
 *
 *  GRANTED                     transition applied, block bytes valid
 *  GRANTED_STORAGE_FALLBACK    master state=N, no holder; requester keeps
 *                              shared-storage page (HC88 N_TO_S/N_TO_X only;
 *                              cross-node X→N→evict dirty deferred to spec-2.35)
 *  DENIED_INCOMPATIBLE         transition apply rejected (state conflict)
 *  DENIED_VALIDATOR_REJECT     HC75 transition_id illegal
 *  DENIED_EPOCH_STALE          request epoch < current cluster_epoch
 *  DENIED_CHECKSUM_FAIL        (sender-side derived; not master-emitted)
 *  DENIED_MASTER_NOT_HOLDER    master state != N and no buffer (HC88) OR
 *                              HC89 revalidation single-retry exhausted
 * ============================================================ */
typedef enum GcsBlockReplyStatus {
	GCS_BLOCK_REPLY_GRANTED = 0,
	GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK = 1,
	GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE = 2,
	GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT = 3,
	GCS_BLOCK_REPLY_DENIED_EPOCH_STALE = 4,
	GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL = 5,
	GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER = 6,
	GCS_BLOCK_REPLY_DENIED_DEDUP_FULL = 7, /* PGRAC: spec-2.34 D1 NEW;
											 * HC96 transient — sender 走 retry
											 * path 同 timeout 语义,budget 耗尽
											 * 才 ereport 53R90 */
	GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER = 8	  /* PGRAC: spec-2.35 D1 NEW;
											   * holder ships block directly to
											   * original requester (2-way CF read
											   * sharing).  Sender HC108
											   * authorized chain validates that
											   * hdr.forwarding_master_node ==
											   * slot.expected_master_node. */
} GcsBlockReplyStatus;


/* ============================================================
 * GcsBlockRequestPayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_REQUEST.
 *
 *  Layout (64B; HC80; Sprint A Step 1 PG-fact discovery: struct natural
 *  alignment is 8B because of uint64 request_id / epoch, so the trailing
 *  pad rounds 60B claim up to 64B.  Reserved_0 bumped 15 → 19 to make
 *  the size explicit at the declaration and lock the wire ABI to 64B):
 *    [  0,   8) request_id              -- per-sender-backend monotone
 *    [  8,  16) epoch                   -- cluster_epoch snapshot at send
 *    [ 16,  36) tag                     -- BufferTag (PG-fact 20B)
 *    [ 36,  40) sender_node             -- int32 cluster_node_id of sender
 *    [ 40,  44) requester_backend_id    -- int32 backend slot index;
 *                                          compound reply key (HC80)
 *    [ 44,  45) transition_id           -- PcmLockTransition (1..9)
 *    [ 45,  64) reserved_0[19]          -- pad + future fields
 * ============================================================ */
typedef struct GcsBlockRequestPayload {
	uint64 request_id;			/*  8B [  0,   8) */
	uint64 epoch;				/*  8B [  8,  16) */
	BufferTag tag;				/* 20B [ 16,  36) */
	int32 sender_node;			/*  4B [ 36,  40) */
	int32 requester_backend_id; /* 4B [ 40,  44) */
	uint8 transition_id;		/*  1B [ 44,  45) */
	uint8 reserved_0[19];		/* 19B [ 45,  64) */
} GcsBlockRequestPayload;

StaticAssertDecl(sizeof(GcsBlockRequestPayload) == 64,
				 "spec-2.33 D1 GcsBlockRequestPayload wire ABI 64B "
				 "(request_id 8 + epoch 8 + tag 20 + sender_node 4 + "
				 "requester_backend_id 4 + transition_id 1 + reserved 19;"
				 " 64B = natural 8-aligned struct size)");


/* ============================================================
 * GcsBlockReplyHeader -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_REPLY
 *                        (header portion; followed by 8192B block_data).
 *
 *  Total reply envelope payload = sizeof(GcsBlockReplyHeader) +
 *                                 GCS_BLOCK_DATA_SIZE = 48 + 8192 = 8240B.
 *  Receiver decodes header in-place then reads block_data directly out
 *  of the envelope buffer (no separate alloc).
 *
 *  Layout (48B; HC80 + HC83 + HC84 + spec-2.35 HC109):
 *    [  0,   8) request_id              -- match outstanding
 *    [  8,  16) page_lsn                -- PageGetLSN(page) at ship time;
 *                                          receiver MUST PageSetLSN(page,
 *                                          page_lsn) under content_lock
 *                                          EXCLUSIVE (HC84)
 *    [ 16,  24) epoch                   -- cluster_epoch at reply
 *    [ 24,  28) checksum                -- CRC32C(block_data, 8192) (HC83)
 *    [ 28,  32) sender_node             -- int32 of replying node
 *                                          (master for direct, holder for
 *                                          forwarded-from-holder)
 *    [ 32,  36) requester_backend_id    -- compound key match (HC80)
 *    [ 36,  37) transition_id           -- echo from request
 *    [ 37,  38) status                  -- GcsBlockReplyStatus (HC83)
 *    [ 38,  42) forwarding_master_node_bytes[4]
 *                                       -- spec-2.35 HC109 reserved 重解读:
 *                                          stored as uint8[4] (NOT int32) so
 *                                          the compiler does not insert
 *                                          padding before this field;  use
 *                                          GcsBlockReplyHeaderGet/Set
 *                                          ForwardingMasterNode() helpers to
 *                                          encode/decode int32 little-endian.
 *                                          0 == direct from master;  != 0 ==
 *                                          forwarded by this master (sender
 *                                          走 HC108 authorized chain).
 *    [ 42,  48) reserved_0[6]           -- align + future fields
 * ============================================================ */
typedef struct GcsBlockReplyHeader {
	uint64 request_id;					/*  8B [  0,   8) */
	uint64 page_lsn;					/*  8B [  8,  16) HC84 */
	uint64 epoch;						/*  8B [ 16,  24) */
	uint32 checksum;					/*  4B [ 24,  28) HC83 CRC32C */
	int32 sender_node;					/*  4B [ 28,  32) */
	int32 requester_backend_id;			/*  4B [ 32,  36) */
	uint8 transition_id;				/*  1B [ 36,  37) */
	uint8 status;						/*  1B [ 37,  38) GcsBlockReplyStatus */
	uint8 forwarding_master_node_bytes[4];	/* 4B [ 38,  42) HC109 spec-2.35 */
	uint8 reserved_0[6];				/*  6B [ 42,  48) */
} GcsBlockReplyHeader;

StaticAssertDecl(sizeof(GcsBlockReplyHeader) == 48,
				 "spec-2.33 D1 + spec-2.35 HC109 GcsBlockReplyHeader wire ABI 48B "
				 "(request_id 8 + page_lsn 8 + epoch 8 + checksum 4 + "
				 "sender_node 4 + requester_backend_id 4 + transition_id 1 + "
				 "status 1 + forwarding_master_node_bytes 4 + reserved 6)");


/* ============================================================
 * Helpers for the spec-2.35 HC109 forwarding_master_node_bytes[4] field.
 *
 *	The field is stored as uint8[4] so the C compiler does not insert
 *	alignment padding before it (placing an int32 at offset 38 would
 *	otherwise require a 2-byte gap and expand the header from 48 to 56
 *	bytes — that would silently break the wire ABI lock above).  Wire
 *	encoding is little-endian, matching every other multi-byte field in
 *	the envelope (cluster_ic_envelope.h uses LE for magic / payload_crc
 *	/ etc).  0 sentinel marks "direct from master, not forwarded".
 * ============================================================ */
static inline int32
GcsBlockReplyHeaderGetForwardingMasterNode(const GcsBlockReplyHeader *hdr)
{
	int32		v;

	memcpy(&v, hdr->forwarding_master_node_bytes, sizeof(int32));
	return v;
}

static inline void
GcsBlockReplyHeaderSetForwardingMasterNode(GcsBlockReplyHeader *hdr, int32 node_id)
{
	memcpy(hdr->forwarding_master_node_bytes, &node_id, sizeof(int32));
}


/* ============================================================
 * GcsBlockForwardPayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_FORWARD
 *                          (spec-2.35 D2; HC102; master→holder direction).
 *
 *	When master decides to forward a GCS_BLOCK_REQUEST to an authorized
 *	holder (HC101: state==S + master not local-resident + bitmap has the
 *	holder bit), it emits this 64B payload to that holder.  Holder reads
 *	original_requester_node + requester_backend_id to direct-ship the
 *	GCS_BLOCK_REPLY (with status GRANTED_FROM_HOLDER + holder's node id
 *	as sender_node + forwarding_master_node = master_node) back to the
 *	original sender (skipping a proxy round-trip through master).
 *
 *	Layout (64B; same size as GcsBlockRequestPayload for ring slot
 *	commonality, but with independent field semantics):
 *	  [  0,   8) request_id            -- echo from original request
 *	  [  8,  16) epoch                 -- master's epoch at forward time
 *	  [ 16,  36) tag                   -- BufferTag (PG-fact 20B)
 *	  [ 36,  40) original_requester_node -- "ship reply back to whom"
 *	  [ 40,  44) requester_backend_id  -- HC80 compound key
 *	  [ 44,  48) master_node           -- "this forward authorized by me"
 *	                                      (holder copies into reply.
 *	                                      forwarding_master_node)
 *	  [ 48,  49) transition_id         -- PcmLockTransition (1..9)
 *	  [ 49,  64) reserved_0[15]        -- align + future fields
 * ============================================================ */
typedef struct GcsBlockForwardPayload {
	uint64 request_id;				/*  8B [  0,   8) */
	uint64 epoch;					/*  8B [  8,  16) */
	BufferTag tag;					/* 20B [ 16,  36) */
	int32 original_requester_node;	/*  4B [ 36,  40) */
	int32 requester_backend_id;		/*  4B [ 40,  44) */
	int32 master_node;				/*  4B [ 44,  48) */
	uint8 transition_id;			/*  1B [ 48,  49) */
	uint8 reserved_0[15];			/* 15B [ 49,  64) */
} GcsBlockForwardPayload;

StaticAssertDecl(sizeof(GcsBlockForwardPayload) == 64,
				 "spec-2.35 D2 GcsBlockForwardPayload wire ABI 64B "
				 "(request_id 8 + epoch 8 + tag 20 + original_requester_node 4 + "
				 "requester_backend_id 4 + master_node 4 + transition_id 1 + "
				 "reserved 15; 64B = natural 8-aligned struct size; same sizeof "
				 "as GcsBlockRequestPayload but independent semantics; HC102)");

/* Compile-time assertion that block size matches PG BLCKSZ.  HC80. */
StaticAssertDecl(GCS_BLOCK_DATA_SIZE == BLCKSZ,
				 "spec-2.33 D1 GCS_BLOCK_DATA_SIZE must equal BLCKSZ "
				 "(reply payload = header 48B + BLCKSZ block_data)");


/* ============================================================
 * Bufmgr helpers (implemented in src/backend/storage/buffer/bufmgr.c).
 *
 *	D4 lives in bufmgr.c because BufferDesc / partition lock internals are
 *	static there.  Declared here so cluster_gcs_block.c can call them and
 *	bufmgr.c sees a prototype for its definitions.
 * ============================================================ */
#include "access/xlogdefs.h" /* XLogRecPtr */
extern bool cluster_bufmgr_probe_block_for_gcs(BufferTag tag);
extern bool cluster_bufmgr_copy_block_for_gcs(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst);


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_gcs_send_block_request_and_wait -- request a block from the
 * deterministic master and block until the reply arrives (or timeout).
 *
 *  Caller boundary (spec-2.33 v0.2 F1):
 *    caller holds buffer pin on `buf` but MUST NOT hold content_lock
 *    when calling.  On GRANTED, the helper takes content_lock EXCLUSIVE
 *    to install block bytes + PageSetLSN (HC84) before returning.
 *
 *  Steps (HC80 + HC83 + HC84 + HC85):
 *    1. Reserve outstanding-slot (spec-2.32 D6 helper reuse)
 *    2. Build GcsBlockRequestPayload (request_id + requester_backend_id key)
 *    3. cluster_ic_send_envelope(master_node, GCS_BLOCK_REQUEST, ...)
 *    4. ConditionVariableTimedSleep(slot.reply_cv,
 *                                   cluster.gcs_reply_timeout_ms,
 *                                   WAIT_EVENT_GCS_BLOCK_SHIP_WAIT)
 *    5. On wake:
 *       GRANTED:
 *         - Verify checksum (HC83);  fail-closed on mismatch
 *         - LWLockAcquire(buf->content_lock, LW_EXCLUSIVE)
 *         - memcpy reply.block_data → BufferGetPage(buf)
 *         - PageSetLSN(BufferGetPage(buf), reply.page_lsn)  (HC84)
 *         - LWLockRelease(buf->content_lock)
 *         - Update buf->pcm_state + buf->buffer_type
 *         - Return success
 *       GRANTED_STORAGE_FALLBACK:
 *         - Do not memcpy;  requester keeps ReadBuffer() page from shared
 *           storage because master state was N when granting (HC88).
 *         - Update buf->pcm_state + buf->buffer_type
 *         - Return success
 *       DENIED_*: cleanup + ereport
 *       Timeout: cleanup + ereport ERRCODE_QUERY_CANCELED + errhint
 *                "spec-2.34 retransmit"
 *    6. Release slot
 */
extern void cluster_gcs_send_block_request_and_wait(BufferDesc *buf,
													PcmLockTransition transition_id,
													int master_node);

/*
 * cluster_gcs_register_block_msg_types -- postmaster-once registration of
 * GCS_BLOCK_REQUEST + GCS_BLOCK_REPLY in cluster_ic dispatch table.  Called
 * from the same phase as cluster_gcs_register_msg_types (spec-2.32).
 *
 *  broadcast_ok = false (point-to-point only).
 */
extern void cluster_gcs_register_block_msg_types(void);

/*
 * Shmem registry for outstanding block-request table + LWLock.
 */
extern Size cluster_gcs_block_shmem_size(void);
extern void cluster_gcs_block_shmem_init(void);
extern void cluster_gcs_block_module_init(void);


/* ============================================================
 * Receiver handlers -- installed into cluster_ic dispatch table.
 * Exposed for cluster_unit tests to exercise dispatch directly.
 * ============================================================ */

/* Forward decl -- definition lives in cluster_ic_envelope.h */
struct ClusterICEnvelope;

extern void cluster_gcs_handle_block_request_envelope(const struct ClusterICEnvelope *env,
													  const void *payload);
extern void cluster_gcs_handle_block_reply_envelope(const struct ClusterICEnvelope *env,
													const void *payload);
/* PGRAC: spec-2.35 D7 — holder-side forward handler.  Receives
 * PGRAC_IC_MSG_GCS_BLOCK_FORWARD, copies the page bytes, direct-ships
 * the GCS_BLOCK_REPLY (status GRANTED_FROM_HOLDER) to the original
 * requester carried in fwd.original_requester_node.  HC103 + HC104 +
 * HC105 (evict race fallback). */
extern void cluster_gcs_handle_block_forward_envelope(const struct ClusterICEnvelope *env,
													  const void *payload);


/* ============================================================
 * Observability accessors (dump_gcs +8 NEW rows for block plane).
 *
 *  Each accessor returns a uint64 counter.  Returns 0 when module is
 *  not initialized (cluster_pcm_is_active false at startup).
 * ============================================================ */
extern uint64 cluster_gcs_get_block_request_count(void);
extern uint64 cluster_gcs_get_block_reply_count(void);
extern uint64 cluster_gcs_get_block_timeout_count(void);
extern uint64 cluster_gcs_get_block_checksum_fail_count(void);
extern uint64 cluster_gcs_get_block_storage_fallback_count(void);
extern uint64 cluster_gcs_get_block_master_not_holder_count(void);
extern uint64 cluster_gcs_get_block_wal_flush_before_ship_count(void);
extern uint64 cluster_gcs_get_block_ship_bytes_total(void);

/* ============================================================
 * spec-2.34 D1 — reliability hardening counter accessors (9 NEW).
 *
 *	dump_gcs rows 22→31:
 *	  retransmit_attempt_count       — # of retry attempts entered
 *	  retransmit_send_count          — # of resend envelopes emitted
 *	  retransmit_exhausted_count     — # of budget-exhausted 53R90 ereports
 *	  dedup_hit_count                — # of CACHED_REPLY hits on master
 *	  dedup_miss_count               — # of MISS_REGISTERED on master
 *	  dedup_collision_count          — # of HC91 tag/transition mismatch
 *	  dedup_full_count               — # of HC92 cap-full DENIED_DEDUP_FULL
 *	  epoch_invalidate_wake_count    — # of CV signals from eager wake hook
 *	  stale_reply_drop_count         — # of HC100 stale-reply drops
 * ============================================================ */
extern uint64 cluster_gcs_get_block_retransmit_attempt_count(void);
extern uint64 cluster_gcs_get_block_retransmit_send_count(void);
extern uint64 cluster_gcs_get_block_retransmit_exhausted_count(void);
extern uint64 cluster_gcs_get_block_dedup_hit_count(void);
extern uint64 cluster_gcs_get_block_dedup_miss_count(void);
extern uint64 cluster_gcs_get_block_dedup_collision_count(void);
extern uint64 cluster_gcs_get_block_dedup_full_count(void);
extern uint64 cluster_gcs_get_block_epoch_invalidate_wake_count(void);
extern uint64 cluster_gcs_get_block_stale_reply_drop_count(void);

/*
 * PGRAC: spec-2.35 D12 — 7 NEW reliability/lifecycle counter accessors
 * for CF 2-way read sharing.  Mirrors ClusterGcsBlockShared fields.
 *
 *	block_forward_sent_count            — master sent GCS_BLOCK_FORWARD
 *	block_forward_received_count        — holder received FORWARD
 *	block_from_holder_ship_count        — holder shipped GRANTED_FROM_HOLDER
 *	block_forward_holder_evicted_count  — holder evict race DENIED reply
 *	s_holders_bitmap_redirect_count     — master chose forward over fallback
 *	master_holder_lifecycle_count       — HC110 update events
 *	forward_replay_count                — dedup FORWARDED re-forward
 */
extern uint64 cluster_gcs_get_block_forward_sent_count(void);
extern uint64 cluster_gcs_get_block_forward_received_count(void);
extern uint64 cluster_gcs_get_block_from_holder_ship_count(void);
extern uint64 cluster_gcs_get_block_forward_holder_evicted_count(void);
extern uint64 cluster_gcs_get_block_s_holders_bitmap_redirect_count(void);
extern uint64 cluster_gcs_get_block_master_holder_lifecycle_count(void);
extern uint64 cluster_gcs_get_block_forward_replay_count(void);

/*
 * PGRAC: spec-2.35 D3 (HC110) — counter bump invoked from cluster_pcm_
 *	transition_apply each time master_holder is mutated.  Keeping the
 *	bump logic in cluster_gcs_block.c avoids exposing the atomic field
 *	of ClusterGcsBlockShared to other translation units.
 */
extern void cluster_gcs_block_bump_master_holder_lifecycle(void);


/* ============================================================
 * spec-2.34 D4 — eager wake on epoch advance.
 *
 *	Called by spec-2.29 reconfig coordinator inside
 *	cluster_reconfig_apply_epoch_bump_as_coordinator() AFTER
 *	cluster_epoch_advance_for_reconfig() + cluster_epoch_set_changed_at_lsn()
 *	and BEFORE cluster_reconfig_publish_event() (HC95 ordering).
 *
 *	Action: sweep all per-backend block-outstanding slots; mark slots whose
 *	request_epoch < new_epoch as stale + ConditionVariableBroadcast their
 *	reply_cv so the sender wakes immediately rather than waiting for the
 *	reply timeout safety net.
 * ============================================================ */
extern void cluster_gcs_block_on_epoch_advance(uint64 new_epoch);


/* ============================================================
 * Test-only injection (cluster_unit / TAP harness builds only).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT

/*
 * Spy hooks for HC82 / HC83 / HC84 / HC89 unit tests.  When non-NULL the
 * helper invokes the hook at the documented point in its flow (after
 * page_lsn read but before XLogFlush, after checksum verify, etc).  The
 * hook may set static state for retry / fail-closed scenarios.
 *
 *  cluster_gcs_block_test_xlog_flush_hook   -- HC82 invocation order spy
 *  cluster_gcs_block_test_lsn_drift_hook    -- HC89 single-retry simulation
 *                                              (returns count of drift events
 *                                              to inject before stabilizing)
 */
extern void (*cluster_gcs_block_test_xlog_flush_hook)(uint64 page_lsn);
extern int (*cluster_gcs_block_test_lsn_drift_hook)(void);

#endif /* USE_CLUSTER_UNIT */


/* ============================================================
 * Internal constants.
 * ============================================================ */

/* Reply envelope payload total size = header + block_data. */
#define GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE (sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE)


#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_GCS_BLOCK_H */
