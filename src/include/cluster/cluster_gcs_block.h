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

/*
 * spec-2.35 HC108/HC109: forwarding_master_node_bytes stores the master that
 * authorized a holder-to-requester direct ship.  Node 0 is a valid cluster
 * node, so the direct-from-master sentinel must be outside the legal node-id
 * range.
 */
#define GCS_BLOCK_REPLY_NO_FORWARDING_MASTER (-1)


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
	GCS_BLOCK_REPLY_DENIED_DEDUP_FULL = 7,			/* PGRAC: spec-2.34 D1 NEW;
											 * HC96 transient — sender 走 retry
											 * path 同 timeout 语义,budget 耗尽
											 * 才 ereport 53R90 */
	GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER = 8,		/* PGRAC: spec-2.35 D1 NEW;
												 * holder ships block directly to
												 * original requester (2-way CF read
												 * sharing).  Sender HC108
												 * authorized chain validates that
												 * hdr.forwarding_master_node ==
												 * slot.expected_master_node. */
	GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER = 9,		/* PGRAC: spec-2.36 D1 NEW;
												 * X-flavored holder direct ship for
												 * 3-way CF writer transfer.  HC115
												 * + HC118 — same HC108 authorized
												 * chain semantics as GRANTED_FROM_
												 * HOLDER but maps to X transition. */
	GCS_BLOCK_REPLY_DENIED_PENDING_X = 10,			/* PGRAC: spec-2.36 D1 NEW;
												 * HC117 reader starvation guard —
												 * N→S request denied because a
												 * pending X requester is registered;
												 * sender backs off + retries per
												 * cluster.gcs_block_starvation_*. */
	GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT = 11, /* PGRAC: spec-2.36 D1 NEW;
													 * master could not collect all
													 * S/X holder invalidate ACKs
													 * within retransmit budget;
													 * sender maps to 53R91. */
	GCS_BLOCK_REPLY_DENIED_LOST_WRITE = 12,			/* PGRAC: spec-2.37 D1 / spec-2.41 D1;
													 * master direct ship self-check OR
													 * holder forward validate fail-closed
													 * via gcs_block_lost_write_verdict():
													 * shipped page pd_block_scn STALE
													 * (< pi_watermark_scn) or ANOMALY
													 * (tracked block, pd_block_scn
													 * InvalidScn).  Cross-node version is
													 * the global SCN, NOT page_lsn (§0).
													 * sender maps to 53R93 terminal denial
													 * — not retried because lost-write is a
													 * data integrity issue that must
													 * surface, not be papered over. */
	GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER = 13	/* PGRAC: spec-5.2 D2 NEW;
													 * X-holder ships the CURRENT block
													 * image for a one-shot cross-node
													 * read (node1 must see node0's
													 * uncommitted ITL row-lock bits) and
													 * KEEPS its X — no ownership transfer,
													 * no downgrade.  The requester
													 * installs the bytes for this read
													 * only, does NOT send a transition-ack
													 * (never registers as an S holder),
													 * and leaves buf->pcm_state == N so
													 * the next access re-fetches (Rule
													 * 8.A: a cached copy with no
													 * invalidation path would go stale).
													 * Reuses HC103 copy-ship + HC127
													 * watermark. */
	,
	GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING = 14 /* PGRAC: spec-5.16 D3b NEW;
													 * master-side hard gate (INV-R8/R14)
													 * — the master (a rejoining node) is
													 * NOT yet a quorum MEMBER, or the
													 * requested block's joiner-home view
													 * is still being rebuilt (survivors
													 * not all re-declared).  Default-deny
													 * BEFORE dedup/grant so a stale-view
													 * requester routed here never gets a
													 * cold grant.  sender maps to 53R9L
													 * (retry-safe, Class 53). */
} GcsBlockReplyStatus;

/* spec-5.16 D3b / r4 — the new reply status MUST be the tail value (no
 * collision with any shipped status; r3 mis-read a truncated enum as max 8,
 * the real shipped max is READ_IMAGE_FROM_XHOLDER=13). */
StaticAssertDecl(GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING
					 == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER + 1,
				 "GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING must be the tail enum value");

/* ============================================================
 * GcsBlockInvalidatePayload — spec-2.36 D1 NEW.
 *
 *   Wire-ABI for PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE (master → S/X holder).
 *   Carried inside a ClusterICEnvelope; sender backend (which is the
 *   master responding to a foreign X request) emits one per current
 *   holder enumerated from s_holders_bitmap / x_holder_node.
 *
 *   Layout (64B fixed; HC83 CRC32C @ offset 48; pad to 64 with
 *   reserved_1[12]):
 *     [  0,   8) request_id              uint64  — master-side allocator
 *     [  8,  16) epoch                   uint64  — HC73 freshness
 *     [ 16,  36) tag                     BufferTag (PG-fact 20B)
 *     [ 36,  40) master_node             int32   — sender of invalidate
 *     [ 40,  41) invalidating_for_x_node uint8   — original X requester
 *                                                  (observability;
 *                                                  HC117 starvation trace)
 *     [ 41,  48) reserved_0[7]                   — pad to checksum align
 *     [ 48,  52) checksum                uint32  — HC83 CRC32C
 *     [ 52,  64) reserved_1[12]                  — pad to 64B
 * ============================================================ */
typedef struct GcsBlockInvalidatePayload {
	uint64 request_id;			   /*  8B [  0,   8) */
	uint64 epoch;				   /*  8B [  8,  16) */
	BufferTag tag;				   /* 20B [ 16,  36) PG-fact */
	int32 master_node;			   /*  4B [ 36,  40) */
	uint8 invalidating_for_x_node; /*  1B [ 40,  41) HC117 */
	uint8 reserved_0[7];		   /*  7B [ 41,  48) */
	uint32 checksum;			   /*  4B [ 48,  52) HC83 CRC32C */
	uint8 reserved_1[12];		   /* 12B [ 52,  64) */
} GcsBlockInvalidatePayload;

/* ============================================================
 * GcsBlockInvalidateAckPayload — spec-2.36 D1 NEW.
 *
 *   Wire-ABI for PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK (holder → master).
 *   Distinct msg_type from INVALIDATE; same size but separate dispatch
 *   keying (codereview F1 P0).  ack_status field encodes:
 *
 *     0 = OK (holder evicted buffer + applied PCM transition)
 *     1 = epoch_stale (HC100 reject before mutation)
 *     2 = already_invalidated (race: buffer not resident)
 *
 *   Layout (64B fixed; same offsets as request payload to keep header
 *   parsing symmetric through checksum).  spec-2.37 carried the holder
 *   page_lsn here; spec-2.41 D3 REINTERPRETS the same 8B slot as the holder
 *   page's pd_block_scn (the cross-node version) so the master advances the
 *   lost-write detector's SCN watermark after a successful invalidate ACK.
 *   The slot is covered by the ACK checksum (all-bytes-except-checksum), so
 *   the reinterpretation is checksum-neutral.  Mixed-version incompatible —
 *   gated by the spec-2.41 catversion/protocol bump (D8):
 *     [ 52,  60) page_scn_bytes[8]      -- little-endian SCN (was page_lsn)
 *     [ 60,  64) reserved_1[4]          -- pad to 64B
 * ============================================================ */
typedef struct GcsBlockInvalidateAckPayload {
	uint64 request_id;		 /*  8B [  0,   8) */
	uint64 epoch;			 /*  8B [  8,  16) */
	BufferTag tag;			 /* 20B [ 16,  36) PG-fact */
	int32 sender_node;		 /*  4B [ 36,  40) */
	uint8 ack_status;		 /*  1B [ 40,  41) 0/1/2 */
	uint8 reserved_0[7];	 /*  7B [ 41,  48) */
	uint32 checksum;		 /*  4B [ 48,  52) HC83 CRC32C */
	uint8 page_scn_bytes[8]; /*  8B [ 52,  60) spec-2.41 D3 (was page_lsn_bytes) */
	uint8 reserved_1[4];	 /*  4B [ 60,  64) */
} GcsBlockInvalidateAckPayload;

StaticAssertDecl(sizeof(GcsBlockInvalidateAckPayload) == 64,
				 "spec-2.36 D1 / spec-2.41 D3 GcsBlockInvalidateAckPayload wire ABI 64B");

StaticAssertDecl(offsetof(GcsBlockInvalidateAckPayload, page_scn_bytes) == 52,
				 "spec-2.41 D3 — invalidate ACK page_scn_bytes[8] must land at offset 52");

static inline void
GcsBlockInvalidateAckPayloadSetPageScn(GcsBlockInvalidateAckPayload *p, SCN scn)
{
	uint64 v = (uint64)scn;

	p->page_scn_bytes[0] = (uint8)(v & 0xff);
	p->page_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->page_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->page_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->page_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->page_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->page_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->page_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline SCN
GcsBlockInvalidateAckPayloadGetPageScn(const GcsBlockInvalidateAckPayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->page_scn_bytes[0];
	v |= (uint64)p->page_scn_bytes[1] << 8;
	v |= (uint64)p->page_scn_bytes[2] << 16;
	v |= (uint64)p->page_scn_bytes[3] << 24;
	v |= (uint64)p->page_scn_bytes[4] << 32;
	v |= (uint64)p->page_scn_bytes[5] << 40;
	v |= (uint64)p->page_scn_bytes[6] << 48;
	v |= (uint64)p->page_scn_bytes[7] << 56;
	return (SCN)v;
}


/* ============================================================
 *   GcsBlockRedeclarePayload -- wire ABI for PGRAC_IC_MSG_GCS_BLOCK_REDECLARE
 *   (spec-4.7 D2;  survivor → remastered master).
 *
 *   After a reconfiguration, each survivor's P5 chunked scan (in the LMON
 *   reconfig tick) re-declares every locally-held S/X buffer to the block's
 *   current GCS master so the master can rebuild the minimal block-resource
 *   view (holder bitmap / mode / PI watermark — D3).  Fire-and-forget
 *   announce (no ACK):  the master is authoritative once rebuilt and a lost
 *   announce just leaves a holder unrecorded (re-sent next tick until the
 *   barrier completes).  64B fixed.  cluster_epoch is the episode epoch (L235
 *   coherence gate; the master drops a re-declare whose epoch != its accepted
 *   episode epoch).
 *
 *   DUAL version carriers (spec-2.41 D3): page_lsn_bytes@28 keeps the
 *   per-stream replay position for the spec-4.7 D5 redo-coverage serve-gate
 *   (required_lsn); page_scn_bytes@52 (carved from the old reserved_1) carries
 *   the cross-node pd_block_scn for the lost-write detector's SCN watermark.
 *   The rebuild advances BOTH.  Because page_scn@52 falls AFTER checksum@48,
 *   the checksum was extended to all-bytes-except-checksum (D3 mandatory) so a
 *   corrupted holder page_lsn OR page_scn cannot poison the rebuilt watermarks.
 * ============================================================ */
typedef struct GcsBlockRedeclarePayload {
	uint64 cluster_epoch;	 /*  8B [  0,   8) episode epoch (L235) */
	BufferTag tag;			 /* 20B [  8,  28) PG-fact */
	uint8 page_lsn_bytes[8]; /*  8B [ 28,  36) LE XLogRecPtr (redo-coverage required_lsn) */
	int32 holder_node_id;	 /*  4B [ 36,  40) = sender node */
	uint8 held_mode;		 /*  1B [ 40,  41) PcmState: PCM_STATE_S / PCM_STATE_X */
	uint8 reserved_0[7];	 /*  7B [ 41,  48) */
	uint32 checksum;		 /*  4B [ 48,  52) */
	uint8 page_scn_bytes[8]; /*  8B [ 52,  60) spec-2.41 D3 LE SCN (detector watermark) */
	uint8 reserved_1[4];	 /*  4B [ 60,  64) pad to 64B */
} GcsBlockRedeclarePayload;

StaticAssertDecl(sizeof(GcsBlockRedeclarePayload) == 64,
				 "spec-4.7 D2 / spec-2.41 D3 GcsBlockRedeclarePayload wire ABI 64B");
StaticAssertDecl(offsetof(GcsBlockRedeclarePayload, page_lsn_bytes) == 28,
				 "spec-4.7 D2 GcsBlockRedeclarePayload page_lsn_bytes must land at offset 28");
StaticAssertDecl(offsetof(GcsBlockRedeclarePayload, checksum) == 48,
				 "spec-4.7 D2 GcsBlockRedeclarePayload checksum must land at offset 48");
StaticAssertDecl(offsetof(GcsBlockRedeclarePayload, page_scn_bytes) == 52,
				 "spec-2.41 D3 GcsBlockRedeclarePayload page_scn_bytes must land at offset 52");

static inline void
GcsBlockRedeclarePayloadSetPageLsn(GcsBlockRedeclarePayload *p, XLogRecPtr lsn)
{
	uint64 v = (uint64)lsn;

	p->page_lsn_bytes[0] = (uint8)(v & 0xff);
	p->page_lsn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->page_lsn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->page_lsn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->page_lsn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->page_lsn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->page_lsn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->page_lsn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline XLogRecPtr
GcsBlockRedeclarePayloadGetPageLsn(const GcsBlockRedeclarePayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->page_lsn_bytes[0];
	v |= (uint64)p->page_lsn_bytes[1] << 8;
	v |= (uint64)p->page_lsn_bytes[2] << 16;
	v |= (uint64)p->page_lsn_bytes[3] << 24;
	v |= (uint64)p->page_lsn_bytes[4] << 32;
	v |= (uint64)p->page_lsn_bytes[5] << 40;
	v |= (uint64)p->page_lsn_bytes[6] << 48;
	v |= (uint64)p->page_lsn_bytes[7] << 56;
	return (XLogRecPtr)v;
}

/* PGRAC: spec-2.41 D3 — REDECLARE page_scn carrier (@52, detector watermark).
 * Distinct unit from page_lsn@28 (redo-coverage); the rebuild advances both. */
static inline void
GcsBlockRedeclarePayloadSetPageScn(GcsBlockRedeclarePayload *p, SCN scn)
{
	uint64 v = (uint64)scn;

	p->page_scn_bytes[0] = (uint8)(v & 0xff);
	p->page_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->page_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->page_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->page_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->page_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->page_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->page_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline SCN
GcsBlockRedeclarePayloadGetPageScn(const GcsBlockRedeclarePayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->page_scn_bytes[0];
	v |= (uint64)p->page_scn_bytes[1] << 8;
	v |= (uint64)p->page_scn_bytes[2] << 16;
	v |= (uint64)p->page_scn_bytes[3] << 24;
	v |= (uint64)p->page_scn_bytes[4] << 32;
	v |= (uint64)p->page_scn_bytes[5] << 40;
	v |= (uint64)p->page_scn_bytes[6] << 48;
	v |= (uint64)p->page_scn_bytes[7] << 56;
	return (SCN)v;
}


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

/* PGRAC: spec-5.2a D1 — clean-page X-transfer eligibility flag carried in the
 * REQUEST payload's reserved_0[0].
 *
 *	The REQUEST and FORWARD payloads are DISTINCT structs, so request[0] is
 *	free even though forward[0] is the spec-5.2 read-image flag (the eligible
 *	flag on the forward wire uses reserved_0[2] instead — see
 *	GcsBlockForwardPayloadSetCleanEligible).  The requesting backend sets this
 *	when its NEXT cluster PCM X acquire was deliberately armed for a clean
 *	(no active ITL / MVCC) page — sequence refill, spec-5.2a D5 — so the GCS
 *	master takes the dedicated clean-page X-transfer path (spec-5.2a D3)
 *	instead of the conservative HG7 fail-closed DENY.  A normal heap request
 *	leaves it 0 → existing conservative path unchanged (inv ①).  ABI stays
 *	64B (reserved-byte overlay). */
static inline void
GcsBlockRequestPayloadSetCleanEligible(GcsBlockRequestPayload *p, bool eligible)
{
	p->reserved_0[0] = eligible ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockRequestPayloadIsCleanEligible(const GcsBlockRequestPayload *p)
{
	return p->reserved_0[0] != 0;
}


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
 *                                          -1 == direct from master;
 *                                          >= 0 == forwarded by this master
 *                                          (sender 走 HC108 authorized chain).
 *                                          Node 0 is a valid cluster node;
 *                                          never use 0 as the direct sentinel.
 *    [ 42,  48) reserved_0[6]           -- align + future fields
 * ============================================================ */
typedef struct GcsBlockReplyHeader {
	uint64 request_id;					   /*  8B [  0,   8) */
	uint64 page_lsn;					   /*  8B [  8,  16) HC84 */
	uint64 epoch;						   /*  8B [ 16,  24) */
	uint32 checksum;					   /*  4B [ 24,  28) HC83 CRC32C */
	int32 sender_node;					   /*  4B [ 28,  32) */
	int32 requester_backend_id;			   /*  4B [ 32,  36) */
	uint8 transition_id;				   /*  1B [ 36,  37) */
	uint8 status;						   /*  1B [ 37,  38) GcsBlockReplyStatus */
	uint8 forwarding_master_node_bytes[4]; /* 4B [ 38,  42) HC109 spec-2.35 */
	uint8 reserved_0[6];				   /*  6B [ 42,  48) */
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
 *	/ etc).  GCS_BLOCK_REPLY_NO_FORWARDING_MASTER marks "direct from
 *	master, not forwarded"; node 0 is a valid forwarding master.
 * ============================================================ */
static inline int32
GcsBlockReplyHeaderGetForwardingMasterNode(const GcsBlockReplyHeader *hdr)
{
	int32 v;

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
 *	  [ 49,  57) expected_pi_watermark_scn_bytes[8] -- spec-2.41 D1/D3
 *	                                      little-endian SCN (was page_lsn under
 *	                                      spec-2.37 HC127).  Master stamps
 *	                                      pi_watermark_scn(tag) so the holder can
 *	                                      validate the shipped page pd_block_scn
 *	                                      via gcs_block_lost_write_verdict()
 *	                                      before shipping;  InvalidScn = not
 *	                                      SCN-tracked.  Mixed-version incompatible
 *	                                      → gated by the spec-2.41 catversion bump.
 *	  [ 57,  64) reserved_0[7]         -- pad + future fields
 *
 *	HC109 pattern (same as GcsBlockReplyHeader.forwarding_master_node_bytes):
 *	use uint8[8] + memcpy helpers to encode the value little-endian; never
 *	declare `SCN expected_pi_watermark_scn` directly because struct padding
 *	rules would silently expand sizeof past 64B (codereview F1 P0 defense
 *	pattern from spec-2.35).
 * ============================================================ */
typedef struct GcsBlockForwardPayload {
	uint64 request_id;						  /*  8B [  0,   8) */
	uint64 epoch;							  /*  8B [  8,  16) */
	BufferTag tag;							  /* 20B [ 16,  36) */
	int32 original_requester_node;			  /*  4B [ 36,  40) */
	int32 requester_backend_id;				  /*  4B [ 40,  44) */
	int32 master_node;						  /*  4B [ 44,  48) */
	uint8 transition_id;					  /*  1B [ 48,  49) */
	uint8 expected_pi_watermark_scn_bytes[8]; /*  8B [ 49,  57) spec-2.41 D1/D3 (was lsn) */
	uint8 reserved_0[7];					  /*  7B [ 57,  64) */
} GcsBlockForwardPayload;

StaticAssertDecl(sizeof(GcsBlockForwardPayload) == 64,
				 "spec-2.35 D2 / spec-2.41 D1 GcsBlockForwardPayload wire ABI 64B "
				 "(request_id 8 + epoch 8 + tag 20 + original_requester_node 4 + "
				 "requester_backend_id 4 + master_node 4 + transition_id 1 + "
				 "expected_pi_watermark_scn_bytes[8] @ offset 49 + reserved_0[7] @ offset 57;  "
				 "sizeof 64B unchanged — same HC109 pattern as forwarding_master_node_bytes[4])");

StaticAssertDecl(offsetof(GcsBlockForwardPayload, expected_pi_watermark_scn_bytes) == 49,
				 "spec-2.41 D1 — expected_pi_watermark_scn_bytes[8] must land at "
				 "offset 49 immediately after transition_id byte at offset 48");

/* PGRAC: spec-2.41 D1/D3 — little-endian SCN helpers (the @49 carrier now holds
 * the detector's expected pi_watermark_scn, NOT a page_lsn). */
static inline void
GcsBlockForwardPayloadSetExpectedPiWatermarkScn(GcsBlockForwardPayload *p, SCN scn)
{
	uint64 v = (uint64)scn;

	p->expected_pi_watermark_scn_bytes[0] = (uint8)(v & 0xff);
	p->expected_pi_watermark_scn_bytes[1] = (uint8)((v >> 8) & 0xff);
	p->expected_pi_watermark_scn_bytes[2] = (uint8)((v >> 16) & 0xff);
	p->expected_pi_watermark_scn_bytes[3] = (uint8)((v >> 24) & 0xff);
	p->expected_pi_watermark_scn_bytes[4] = (uint8)((v >> 32) & 0xff);
	p->expected_pi_watermark_scn_bytes[5] = (uint8)((v >> 40) & 0xff);
	p->expected_pi_watermark_scn_bytes[6] = (uint8)((v >> 48) & 0xff);
	p->expected_pi_watermark_scn_bytes[7] = (uint8)((v >> 56) & 0xff);
}

static inline SCN
GcsBlockForwardPayloadGetExpectedPiWatermarkScn(const GcsBlockForwardPayload *p)
{
	uint64 v = 0;

	v |= (uint64)p->expected_pi_watermark_scn_bytes[0];
	v |= (uint64)p->expected_pi_watermark_scn_bytes[1] << 8;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[2] << 16;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[3] << 24;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[4] << 32;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[5] << 40;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[6] << 48;
	v |= (uint64)p->expected_pi_watermark_scn_bytes[7] << 56;
	return (SCN)v;
}

/* PGRAC: spec-2.41 D1 — pure lost-write verdict (the detector's SCN decision).
 *
 *	Compares a master's expected pi_watermark_scn(tag) against a shipped page's
 *	pd_block_scn (§2.6 three-branch).  Pure (no shmem / no locks) so the
 *	master-direct and holder-forward detectors share ONE decision and the unit
 *	tests can exercise every branch directly. */
typedef enum GcsLostWriteVerdict {
	GCS_LOST_WRITE_SKIP,		 /* expected InvalidScn: block not SCN-tracked (no fire) */
	GCS_LOST_WRITE_PASS,		 /* shipped >= expected: current version */
	GCS_LOST_WRITE_FAIL_STALE,	 /* both valid, shipped < expected: stale page */
	GCS_LOST_WRITE_FAIL_ANOMALY, /* expected valid, shipped InvalidScn: tracked-but-unstamped */
} GcsLostWriteVerdict;

/*
 * The single SCN lost-write decision shared by the master-direct and
 * holder-forward detectors (§2.6 three-branch).  `expected_scn` is the
 * master's pi_watermark_scn(tag);  `shipped_scn` is the pd_block_scn of the
 * page about to be shipped.  Cross-node version order is the global Lamport
 * SCN (AD-008), NEVER page_lsn (per-node WAL position; §0).  static inline so
 * it is pure (no shmem / no locks), inlinable in both detector paths, and
 * unit-testable from the header-only test binary.
 *
 *	expected InvalidScn                 -> SKIP    (block not SCN-tracked; no fire)
 *	expected valid, shipped InvalidScn  -> ANOMALY (tracked block ships an
 *	                                                 unstamped page — never PASS)
 *	both valid, shipped < expected      -> STALE   (true lost write)
 *	shipped >= expected                 -> PASS    (current)
 */
static inline GcsLostWriteVerdict
gcs_block_lost_write_verdict(SCN expected_scn, SCN shipped_scn)
{
	if (!SCN_VALID(expected_scn))
		return GCS_LOST_WRITE_SKIP;
	if (!SCN_VALID(shipped_scn))
		return GCS_LOST_WRITE_FAIL_ANOMALY;
	/* Compare by local_scn (the Lamport time order) — this IS scn_time_cmp's
	 * "only local_scn matters" contract.  A raw uint64 compare would be wrong:
	 * the SCN encodes node_id in the high 8 bits (cluster_scn.h), so raw `<`
	 * would let a higher-node_id watermark falsely flag a lower-node_id node's
	 * newer write as stale (the very cross-stream false-fire spec-2.41 fixes).
	 * scn_local() is extracted inline so the verdict stays pure / header-only
	 * testable; both operands are valid SCNs here (branches above). */
	if (scn_local(shipped_scn)
		< scn_local(expected_scn)) /* SCN_CMP_OK: scn_time_cmp via scn_local */
		return GCS_LOST_WRITE_FAIL_STALE;
	return GCS_LOST_WRITE_PASS;
}

/* PGRAC: spec-5.2 D2 — read-image intent flag carried in reserved_0[0].
 *
 *	When the master forwards an N→S read request to a node that holds the
 *	block in X, it sets this flag so the holder ships a one-shot read image
 *	(status READ_IMAGE_FROM_XHOLDER) and KEEPS its X, instead of the
 *	2-way-share GRANTED_FROM_HOLDER.  Reuses the existing 64B forward wire
 *	(no size change) — same reserved-byte-overlay pattern as HC127. */
static inline void
GcsBlockForwardPayloadSetReadImage(GcsBlockForwardPayload *p, bool read_image)
{
	p->reserved_0[0] = read_image ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsReadImage(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[0] != 0;
}

/* PGRAC: spec-5.2 D11 — X-transfer (writer-transfer-revoke) intent flag carried
 * in reserved_0[1].
 *
 *	When THIS node is the GCS master for a block held in X by a REMOTE node and
 *	a LOCAL writer needs X (cross-node TX row-lock wait), the master forwards an
 *	N→X request to the holder with this flag set.  Unlike the 3-way
 *	X_GRANTED_FROM_HOLDER path (master is a third node; holder retains its X
 *	until the requester's post-install transition ACK reaches the master), the
 *	2-node local-master case has no separate ACK round-trip — the master IS the
 *	requester — so the holder must RELEASE its own X as it ships (invalidating
 *	its local copy so it can never flush a stale page; Rule 8.A no-stale-flush).
 *	The brief no-holder window is safe (no double-X);  the local master records
 *	itself as the new x_holder on install.  Reuses the existing 64B forward wire
 *	(no size change) — same reserved-byte-overlay pattern as read-image / HC127. */
static inline void
GcsBlockForwardPayloadSetXTransfer(GcsBlockForwardPayload *p, bool x_transfer)
{
	p->reserved_0[1] = x_transfer ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsXTransfer(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[1] != 0;
}

/* PGRAC: spec-5.2a D1 — clean-page X-transfer eligibility flag carried in the
 * FORWARD payload's reserved_0[2].
 *
 *	v0.3 P0 FIX (reserved-byte collision):  reserved_0[0] is the spec-5.2 D2
 *	read-image flag and reserved_0[1] is the spec-5.2 D11 X-transfer flag
 *	(above).  The clean-page eligibility flag on the FORWARD wire therefore
 *	MUST NOT reuse [0]/[1] — it uses reserved_0[2] (the [2..6] range is free;
 *	reserved_0 is 7B at offset 57).  Set by the master when forwarding an
 *	eligible (sequence-refill) N→X to the holder so the holder uses the
 *	flush-data-before-drop path (spec-5.2a D4) rather than the no-data
 *	drop_no_wire path: the shared data file must reflect the current value
 *	after the drop so a later storage-fallback (stale-holder recovery) reads
 *	the current page, not a stale one (inv③, F0-11).  A heap / non-eligible
 *	forward leaves this 0 → existing behaviour unchanged (inv①). */
static inline void
GcsBlockForwardPayloadSetCleanEligible(GcsBlockForwardPayload *p, bool eligible)
{
	p->reserved_0[2] = eligible ? (uint8)1 : (uint8)0;
}

static inline bool
GcsBlockForwardPayloadIsCleanEligible(const GcsBlockForwardPayload *p)
{
	return p->reserved_0[2] != 0;
}

/* PGRAC: spec-5.2 D2 — pure master-side decision for an N→S read request
 * when the block is held in X.  Kept pure (no shmem / no I/O) so the gate
 * truth table is unit-tested standalone (U3). */
typedef enum GcsXheldReadShipDecision {
	GCS_XHELD_READ_NOT_APPLICABLE = 0, /* not an X-held N→S read — existing logic */
	GCS_XHELD_READ_DIRECT_FROM_MASTER, /* master itself holds X resident → ship its image */
	GCS_XHELD_READ_FORWARD_TO_HOLDER,  /* a remote node holds X → forward read-image */
	GCS_XHELD_READ_DENY				   /* cannot satisfy safely → fail-closed (unchanged) */
} GcsXheldReadShipDecision;

static inline GcsXheldReadShipDecision
gcs_block_xheld_read_ship_decision(uint8 transition_id, int pre_state, int32 holder_node,
								   int32 requester_node, int32 master_node, bool master_resident)
{
	/* Only plain cross-node reads (N→S) on an X-held block are in scope. */
	if (transition_id != (uint8)PCM_TRANS_N_TO_S || pre_state != (int)PCM_LOCK_MODE_X)
		return GCS_XHELD_READ_NOT_APPLICABLE;

	/* A valid live holder must exist and it must not be the requester itself
	 * (a node never read-ships to itself). */
	if (holder_node < 0 || holder_node == requester_node)
		return GCS_XHELD_READ_DENY;

	/* The master holds X and the buffer is resident here → it can copy and
	 * ship its own current image directly. */
	if (holder_node == master_node && master_resident)
		return GCS_XHELD_READ_DIRECT_FROM_MASTER;

	/* A different live node holds X → forward a read-image request to it. */
	if (holder_node != master_node)
		return GCS_XHELD_READ_FORWARD_TO_HOLDER;

	/* Master is recorded as holder but the buffer is not resident (evicted /
	 * race) — cannot ship safely (Rule 8.A: never a silent stale read). */
	return GCS_XHELD_READ_DENY;
}

/* PGRAC: spec-5.2a D3 — pure master-side decision for an eligible clean-page
 * (sequence) X request.  Kept pure (no shmem / no I/O) so the 5-branch truth
 * table is unit-tested standalone (U3).  The handler runs ON the GCS master,
 * so `master` == cluster_node_id; `requester` is req->sender_node; `x_holder`
 * is the GRD-recorded X holder (or < 0 for none). */
typedef enum GcsCleanXferDecision {
	GCS_CLEAN_XFER_IDEMPOTENT = 0,	  /* x_holder == requester — already holds X */
	GCS_CLEAN_XFER_STORAGE_FALLBACK,  /* no holder — grant + read storage */
	GCS_CLEAN_XFER_SELF_SHIP,		  /* x_holder == master — path-B self-ship */
	GCS_CLEAN_XFER_FORWARD_TO_HOLDER, /* x_holder is other live, master == requester */
	GCS_CLEAN_XFER_THIRD_PARTY_DENY	  /* x_holder is other live, master ∉ {req,holder} (≥3 nodes) */
} GcsCleanXferDecision;

static inline GcsCleanXferDecision
gcs_block_clean_xfer_master_decision(int32 x_holder, int32 requester, int32 master)
{
	if (x_holder == requester)
		return GCS_CLEAN_XFER_IDEMPOTENT;
	if (x_holder < 0)
		return GCS_CLEAN_XFER_STORAGE_FALLBACK;
	if (x_holder == master)
		return GCS_CLEAN_XFER_SELF_SHIP;
	if (master == requester)
		return GCS_CLEAN_XFER_FORWARD_TO_HOLDER;
	return GCS_CLEAN_XFER_THIRD_PARTY_DENY;
}

/* PGRAC: spec-5.2a D3 — pure stale-holder predicate (U4).  True when an
 * eligible clean-page X-transfer got a holder DENIED_MASTER_NOT_HOLDER reply:
 * the holder is LIVE but no longer resident (it dropped to N), yet the master
 * still records it — the F0-4 stale-holder window.  Q3 amended 2026-06-21: the
 * action is now FAIL CLOSED (53R9X retryable), NOT storage-fallback recovery —
 * Stage-5 shared storage is not cross-instance coherent, so reading the page
 * from storage on the recovering node returns a stale view and reissues
 * sequence values (Rule 8.A violation, proven by t/284 L5).  The normal CF
 * image-ship path self-heals; a sound storage-fallback lands in Stage 6.  A
 * timeout (got_reply == false) is NOT this case (it cannot prove the holder
 * dropped) and stays fail-closed via the generic path. */
static inline bool
gcs_block_clean_xfer_should_stale_break(bool clean_eligible, bool got_reply, uint8 reply_status)
{
	return clean_eligible && got_reply
		   && reply_status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
}

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
#include "access/xlogdefs.h"		  /* XLogRecPtr */
#include "cluster/cluster_pcm_lock.h" /* PcmLockMode for invalidate helper */
extern bool cluster_bufmgr_probe_block_for_gcs(BufferTag tag);
extern bool cluster_bufmgr_copy_block_for_gcs(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst);
/* PGRAC: spec-2.36 D4 (HC118 / HC123) — by-tag invalidate wrapper for
 * holder-side INVALIDATE handler.  XLogFlush+InvalidateBuffer. */
extern bool cluster_bufmgr_invalidate_block_for_gcs(BufferTag tag, PcmLockMode expected_mode,
													XLogRecPtr *out_page_lsn, SCN *out_page_scn);
/* PGRAC: spec-5.2 D11 (writer-transfer-revoke) — by-tag local buffer drop
 * with NO GCS release wire, for the holder-side X-transfer branch running in
 * the §3.5 IC-dispatch (LMON) context.  XLogFlush+InvalidateBuffer, with the
 * cache-eviction release wire suppressed (clears pcm_state=N first). */
extern bool cluster_bufmgr_drop_block_for_gcs_no_wire(BufferTag tag, XLogRecPtr *out_page_lsn);

/* PGRAC: spec-5.2a D4 (backend eager flush) — flush a cluster sequence page to
 * shared storage from the BACKEND that just wrote it.  Caller holds a pin and
 * the buffer content lock (any mode; nextval/setval hold EXCLUSIVE).  Runs
 * FlushOneBuffer -> FlushBuffer (XLogFlush(page_lsn) WAL-before-data + smgrwrite
 * to shared storage), which is safe HERE because the backend's own WAL insert
 * is complete and flushable.  After this returns the page is clean and
 * storage-current, so a later cross-node clean X-transfer (LMON) only has to
 * drop a clean page (drop_block_for_gcs_clean_only) and a stale-holder
 * storage-fallback reads the current value.  Fails closed (ereport) on write
 * error via the underlying smgr path. */
extern void cluster_bufmgr_flush_seq_page_to_storage(Buffer buffer);

/* PGRAC: spec-5.2 §3.5 D11 (writer-transfer-revoke) — false
 * ONLY when this buffer is a deferred-writer read-image of a remote-X-held
 * block (pcm_state == PCM_STATE_READ_IMAGE); true otherwise.  The cluster_itl
 * forward-write path fails closed (retryable) on false so a writer never
 * mutates a non-owned copy (Rule 8.A multi-row fail-closed leg). */
extern bool cluster_bufmgr_block_write_permitted(Buffer buffer);

/* PGRAC: spec-5.13 D5b (clean-leave GCS flush seam) — a leaving node force-
 * persists every dirty block it holds X on to shared storage and releases that
 * X (pcm_state X -> N).  FlushBuffer is a bufmgr private static, so this seam
 * lives in bufmgr.c.  Runs in the leaving node's own backend/checkpointer
 * (CL-I9), never LMON.  Fail-closed (ereport) on write error.  Returns the
 * count of blocks flushed + X-released (CL-I5 / §0.3 命门). */
extern uint32 cluster_bufmgr_flush_and_release_x_for_leave(void);

/* PGRAC: spec-4.7 D2 (Q6-A' worker-centric) — bounded chunked scan of the
 * shared buffer pool that re-declares each locally-held S/X buffer.  The
 * callback receives (tag, held_mode, page_lsn, arg) per qualifying buffer;
 * cluster_bufmgr_redeclare_scan_chunk returns the next cursor (== NBuffers
 * once the whole pool has been scanned) so the LMON reconfig tick can drive
 * it in bounded chunks without blocking the heartbeat. */
typedef void (*ClusterGcsRedeclareCallback)(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn,
											SCN page_scn, void *arg); /* spec-2.41 D3 +page_scn */
extern int cluster_bufmgr_redeclare_scan_chunk(int start_buf, int max_scan,
											   ClusterGcsRedeclareCallback cb, void *arg);


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
/*
 * Returns true if a DURABLE PCM grant was acquired (GRANTED / STORAGE_
 * FALLBACK — the caller mirrors PCM ownership into buf->pcm_state).  Returns
 * false for a spec-5.2 D2 one-shot READ_IMAGE: the bytes were installed for
 * this read only, no S holder was registered, and the caller MUST leave
 * buf->pcm_state == N so the next access re-fetches.  Terminal denials
 * ereport(ERROR) and do not return.
 */
extern bool cluster_gcs_send_block_request_and_wait(BufferDesc *buf,
													PcmLockTransition transition_id,
													int master_node, bool clean_eligible);

/*
 * spec-5.2 D2 (sub-case B) — local-master read-image forward.  Used by
 * cluster_pcm_lock_acquire_buffer when THIS node is the GCS master for a
 * block a REMOTE node holds in X and a local reader needs an N→S image.
 * Forwards a read-image request to the holder and installs the shipped
 * current image for one read.  Returns false (non-durable; caller leaves
 * buf->pcm_state == N); fails closed (ereport) if no image is obtained.
 */
extern bool cluster_gcs_local_master_read_image_and_wait(BufferDesc *buf, int32 holder_node);
/* PGRAC: spec-5.2 D11 — local-master writer-transfer (revoke); durable X grant.
 * spec-5.2a D2/D3: clean_eligible routes a clean (sequence) page through the
 * flush-data-before-drop holder path + stale-holder storage-fallback recovery. */
extern bool cluster_gcs_local_master_x_transfer_and_wait(BufferDesc *buf, int32 holder_node,
														 bool clean_eligible);

/*
 * spec-4.7 D1 — GCS/PCM block resource recovery phase.
 *
 *	AD-002 资源级 {GRANTED, CONVERTING, RECOVERING} 的 RECOVERING 兑现.
 *	A block resource is RECOVERING when its GCS master is being recovered
 *	after a reconfiguration: the master node is DEAD, and block-protocol
 *	state (holders / mode / PI watermark) is volatile shmem with no
 *	transition log, so it must be REBUILT (spec-4.7 D2/D3), not recovered.
 *	cluster_gcs_lookup_master hashes over the STATIC declared node list
 *	(cluster_gcs.c), so a dead master still routes here;  spec-4.6 GRD/GES
 *	remaster rebuilds only the logical-lock layer, NOT block/PCM state.
 *
 *	The bufmgr acquire gate (cluster_pcm_lock_acquire_buffer) fail-closes
 *	53R9L (ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING) for a RECOVERING
 *	block after a bounded cluster.gcs_block_recovery_wait_ms wait — never a
 *	stale local / old-master fallback.  master == self (own master or
 *	single-node fallback) is NOT RECOVERING (it is the clean-restart
 *	lazy-rebuild path landed by spec-4.7 D3).
 */
typedef enum ClusterGcsBlockPhase {
	GCS_BLOCK_NORMAL = 0,
	GCS_BLOCK_RECOVERING = 1,
} ClusterGcsBlockPhase;

extern ClusterGcsBlockPhase cluster_gcs_block_phase_for_tag(BufferTag tag);

/*
 * spec-5.16 D3 — online-join PCM block snap-back fence predicates (impl in
 * cluster_grd.c;  declared here because BufferTag is in scope and both the
 * requester-side phase gate and the master-side envelope handler consume them).
 *
 *	cluster_grd_join_remaster_active_for_shard:  the block's STATIC PCM home
 *	    (cluster_gcs_lookup_master_static) is a rejoining RECIPIENT of the current
 *	    fence episode (join_pcm_fence_member_epoch[home] == join_pcm_fence_epoch;
 *	    bound to online_join, INDEPENDENT of any GRD master[] movement — so
 *	    join_remaster_enabled=off still fences, r2 P1-①).  false when the fence is
 *	    not armed (join_pcm_fence_epoch == 0) or the home is a steady member.
 *	cluster_grd_block_view_rebuilt:  the joiner-home view is rebuilt — i.e.
 *	    EVERY declared member's recovery_done_epoch >= join_pcm_fence_epoch
 *	    (Hardening v1.1:  the all-members all_done barrier, NOT the joiner's own
 *	    done-epoch, which advances before survivors finish re-declaring → 8.A).
 *	    true when the fence is not armed.
 */
extern bool cluster_grd_join_remaster_active_for_shard(BufferTag tag);
extern bool cluster_grd_block_view_rebuilt(BufferTag tag);

/*
 * spec-4.7 D5 — redo-before-unfreeze gate (Q5):  true iff the dead origin's
 * merged WAL recovery on this node reached >= required_lsn (the survivor's
 * observed max page_lsn).  Below that → lost-write risk → fail-closed 53R9M.
 */
extern bool cluster_gcs_block_redo_lsn_covered(int dead_origin, XLogRecPtr required_lsn);

/*
 * spec-4.7 D2 — survivor block re-declare wire (PGRAC_IC_MSG_GCS_BLOCK_REDECLARE).
 *	cluster_gcs_block_send_redeclare:  the P5 chunked scan sends one
 *		fire-and-forget announce per locally-held S/X buffer to the block's
 *		current (remastered) master.
 *	cluster_gcs_handle_block_redeclare_envelope:  master-side receive —
 *		validate checksum + episode epoch (L235/L236), then rebuild the
 *		minimal block-resource view via
 *		cluster_gcs_block_master_rebuild_from_redeclare (cluster_pcm_lock.c).
 */
extern void cluster_gcs_block_send_redeclare(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn,
											 SCN page_scn, uint64 cluster_epoch, int master_node);
extern void cluster_gcs_handle_block_redeclare_envelope(const struct ClusterICEnvelope *env,
														const void *payload);

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

/* PGRAC: spec-2.36 D10 — 6 NEW counter accessors for CF 3-way protocol. */
extern uint64 cluster_gcs_get_block_invalidate_broadcast_count(void);
extern uint64 cluster_gcs_get_block_invalidate_ack_received_count(void);
extern uint64 cluster_gcs_get_block_invalidate_timeout_count(void);
extern uint64 cluster_gcs_get_block_x_forward_sent_count(void);
extern uint64 cluster_gcs_get_block_x_granted_from_holder_count(void);
extern uint64 cluster_gcs_get_starvation_denied_pending_x_count(void);

/* PGRAC: spec-2.37 D12 — 4 NEW counter accessors for PI watermark + lost-write. */
extern uint64 cluster_gcs_get_pi_watermark_advance_count(void);
extern uint64 cluster_gcs_get_pi_watermark_retire_count(void);
extern uint64 cluster_gcs_get_lost_write_detected_count(void);
extern uint64 cluster_gcs_get_lost_write_avoid_count(void);
/* PGRAC: spec-2.41 D7 — SCN detector + redo-coverage observability accessors. */
extern uint64 cluster_gcs_get_lost_write_invalidscn_failclosed_count(void);
extern uint64 cluster_gcs_get_lost_write_not_scn_tracked_skip_count(void);
extern uint64 cluster_gcs_get_redo_coverage_required_lsn_zero_count(void);
extern uint64 cluster_gcs_get_redo_coverage_gate_block_count(void);

/* PGRAC: spec-5.2 D2 — X-holder read-image ship counter accessor. */
extern uint64 cluster_gcs_get_cf_xheld_read_ship_count(void);
/* PGRAC: spec-5.2a D6 — clean-page X-transfer enabler counters (5). */
extern uint64 cluster_gcs_get_clean_page_xfer_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_storage_fallback_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_fail_closed_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_stale_holder_recover_count(void);
extern uint64 cluster_gcs_get_clean_page_xfer_third_party_denied_count(void);
/* PGRAC: spec-5.2 D11 — writer-transfer-revoke ship counters (A: path-A
 * forward-to-holder revoke; B: master==holder self-ship). */
extern uint64 cluster_gcs_get_block_x_transfer_ship_count(void);
extern uint64 cluster_gcs_get_block_x_self_ship_count(void);

/* PGRAC: spec-4.7 D6 — 8 warm-recovery observability accessors. */
extern uint64 cluster_gcs_get_recovery_block_resources_recovering(void);
extern uint64 cluster_gcs_get_recovery_buffers_redeclared(void);
extern uint64 cluster_gcs_get_recovery_block_state_rebuilt(void);
extern uint64 cluster_gcs_get_recovery_redo_boundary_waits(void);
extern uint64 cluster_gcs_get_recovery_redo_boundary_reached(void);
extern uint64 cluster_gcs_get_recovery_stale_block_drop(void);
extern uint64 cluster_gcs_get_recovery_ambiguous_owner_failclosed(void);
extern uint64 cluster_gcs_get_recovery_before_boundary_failclosed(void);

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
 * spec-5.13 D5 — clean-leave GCS data-plane drain.
 *
 *	flush_all_dirty: leaving node, thin orchestration over the bufmgr D5b
 *	seam (runs in the leaving node's backend/checkpointer, CL-I9).
 *	invalidate_for: survivor, POST-epoch cache invalidate of the leaving
 *	node's blocks (reuses on_epoch_advance; CL-I5 happens-before boundary).
 * ============================================================ */
extern uint32 cluster_gcs_block_clean_leave_flush_all_dirty(void);
extern void cluster_gcs_block_clean_leave_invalidate_for(int32 leaving_node, uint64 new_epoch);


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
