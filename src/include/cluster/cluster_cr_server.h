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
#include "cluster/cluster_runtime_visibility.h" /* ClusterLiveAuthority (spec-6.12i) */
#include "cluster/cluster_scn.h"
#include "storage/buf_internals.h" /* BufferTag */

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
 * LMS CR work slots (shmem, embedded in the cluster_lms region).
 *
 *	Slot lifecycle: FREE -(LMON submit)-> PENDING -(LMS drain)-> BUSY
 *	-(LMS result)-> READY -(LMON ship)-> FREE.  Single-producer single-
 *	consumer per direction (LMON dispatch is single-threaded; LMS is one
 *	process), so an atomic state word per slot is the whole protocol.
 */
#define CLUSTER_LMS_CR_SLOTS 4

typedef enum ClusterLmsCrSlotState {
	CLUSTER_LMS_CR_FREE = 0,
	CLUSTER_LMS_CR_PENDING = 1,
	CLUSTER_LMS_CR_BUSY = 2,
	CLUSTER_LMS_CR_READY = 3
} ClusterLmsCrSlotState;

/* Work-slot request kind (spec-6.12i extends the wave-b CR-only table). */
typedef enum ClusterLmsCrSlotKind {
	CLUSTER_LMS_SLOT_KIND_CR = 0,		   /* spec-6.12b CR construction */
	CLUSTER_LMS_SLOT_KIND_UNDO_FETCH = 1,  /* spec-6.12i undo-TT block fetch */
	CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT = 2 /* spec-6.12i D-i4 complete-scan verdict */
} ClusterLmsCrSlotKind;

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
							   * UNDO_VERDICT_RESULT /
							   * DENIED_MASTER_NOT_HOLDER */
	uint8 req_kind;			 /* ClusterLmsCrSlotKind (spec-6.12i) */
	/* spec-6.12i D-i1: undo address decoded from the synthetic tag at submit
	 * time, and the LMS-co-sampled live authority triple the ship path copies
	 * into the reply (epoch -> hdr.epoch, live_hwm -> hdr.page_lsn,
	 * tt_generation -> trailer).  Meaningful only for KIND_UNDO_FETCH /
	 * KIND_UNDO_VERDICT.  undo_xid is the D-i4 verdict subject, decoded from
	 * the widened watermark carrier at submit time (KIND_UNDO_VERDICT only). */
	uint32 undo_segment_id;
	uint32 undo_block_no;
	TransactionId undo_xid;
	ClusterLiveAuthority undo_auth;
	char result_page[BLCKSZ]; /* the constructed CR page (FULL/PARTIAL), the
							   * fetched undo header block (UNDO_FETCH), or
							   * the ClusterGcsUndoVerdictPage (UNDO_VERDICT) */
} ClusterLmsCrSlot;

/* CR-server counter buckets (bumped into the ClusterCRShared region owned
 * by cluster_cr.c). */
typedef enum ClusterCrServerStat {
	CLUSTER_CR_SERVER_STAT_FULL = 0,
	CLUSTER_CR_SERVER_STAT_PARTIAL = 1,
	CLUSTER_CR_SERVER_STAT_DENIED = 2,
	CLUSTER_CR_SERVER_STAT_UNDO_SERVED = 3,	   /* spec-6.12i D-i1 origin serve */
	CLUSTER_CR_SERVER_STAT_UNDO_DENIED = 4,	   /* spec-6.12i D-i1 origin refuse */
	CLUSTER_CR_SERVER_STAT_VERDICT_SERVED = 5, /* spec-6.12i D-i4 verdict serve */
	CLUSTER_CR_SERVER_STAT_VERDICT_DENIED = 6  /* spec-6.12i D-i4 verdict refuse */
} ClusterCrServerStat;

extern void cluster_cr_server_stat_bump(ClusterCrServerStat which);

/* LMS-side construction entry (cluster_cr.c): full CR over a stable copy of
 * the current page, peeling only the self-home DESC prefix; throws on every
 * failure (the LMS drain converts throws into DENIED replies). */
extern void cluster_cr_construct_page_for_server(const char *cur_page, SCN read_scn, BufferTag tag,
												 char *dst_page, bool *out_partial);

/* Shmem region registration (cluster_shmem.c registry). */
extern void cluster_cr_server_shmem_register(void);

/* LmsMain lifecycle: publish (entry) / retract (NULL, exit) the LMS wake
 * latch so the LMON submit path can cut the idle-poll latency. */
struct Latch;
extern void cluster_cr_server_publish_lms_latch(struct Latch *latch);

/* LMON dispatch side: park a validated CR request; false = no capacity /
 * data plane off (caller replies the fail-closed DENIED immediately). */
extern bool cluster_lms_cr_submit(const GcsBlockForwardPayload *fwd);

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

/* LMS main-loop side: construct every PENDING slot (errors become DENIED
 * results — fail-closed to the requester, never an LMS restart). */
extern void cluster_lms_cr_drain(void);

/* LMON tick side: ship every READY slot to its requester and free it. */
extern void cluster_lms_cr_ship_ready(void);

/* Requester side (backend): fetch a CR page for (locator, fork, block) at
 * read_scn from origin_node.  On success copies the shipped page into
 * dst_page and returns true; *out_partial says whether the local
 * construction must continue on it.  false = fail-closed (caller keeps the
 * unchanged 53R9G refusal). */
extern bool cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN read_scn, int32 origin_node,
												char *dst_page, bool *out_partial);

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
 * The caller MUST Lamport-observe verdict_out->horizon_scn (and any
 * commit_scn) it consumes — SCNs that crossed the wire (AD-008). */
extern bool cluster_gcs_block_undo_verdict_fetch_and_wait(int32 origin_node, uint32 segment_id,
														  TransactionId xid,
														  ClusterGcsUndoVerdictPage *verdict_out,
														  ClusterLiveAuthority *auth_out);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_CR_SERVER_H */
