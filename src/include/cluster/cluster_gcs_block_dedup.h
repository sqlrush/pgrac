/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_dedup.h
 *	  pgrac cluster GCS block reliability hardening — master-side dedup HTAB.
 *
 *	  spec-2.34 D2 introduces an LMON-owned dedup HTAB on every node that
 *	  serves as GCS block-shipping master.  When the same logical request
 *	  is retransmitted by the sender (after timeout, eager wake, or epoch
 *	  stale retry), the master-side handler can return the cached reply
 *	  without re-flushing WAL or re-copying the page (HC99).  The HTAB
 *	  uses a 4-tuple key {origin_node, requester_backend_id, request_id,
 *	  cluster_epoch} (HC90), with entry value containing the full reply
 *	  payload plus tag + transition_id for collision validation (HC91).
 *
 *	  Key safety properties:
 *	    HC90  4-tuple key; LMON-owned shmem region; built-in tranche
 *	    HC91  duplicate hit must validate entry.tag == req.tag &&
 *	          entry.transition_id == req.transition_id; mismatch →
 *	          DENIED_VALIDATOR_REJECT + dedup_collision_count++
 *	    HC92  fixed-size sizeof(GcsBlockDedupEntry) == 8312B (PG dynahash
 *	          cap × 8.3KB master memory ceiling; default 1024 → 8.4MB on
 *	          configured cluster nodes; bootstrap/initdb with node_id=-1
 *	          does not allocate the HTAB)
 *	    HC93  TTL sweep (completed_at_ts + registered_at_ts) + local
 *	          before_shmem_exit cleanup + CSSD DEAD cleanup — three-fold
 *	          GC; not solely epoch-based
 *	    HC96  cap-full → DENIED_DEDUP_FULL transient, sender retries
 *	    HC99  entry stores complete reply replay payload
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_gcs_block_dedup.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.34-gcs-block-reliability-hardening.md (FROZEN v0.3)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_BLOCK_DEDUP_H
#define CLUSTER_GCS_BLOCK_DEDUP_H

#include "c.h"
#include "cluster/cluster_gcs_block.h" /* GcsBlockReplyHeader / status */
#include "datatype/timestamp.h"		   /* TimestampTz */
#include "storage/buf_internals.h"	   /* BufferTag */

#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * GcsBlockDedupKey — 4-tuple key (HC90; 24B).
 *
 *	Layout:
 *	  [ 0,  4) origin_node_id           uint32
 *	  [ 4,  8) requester_backend_id     int32
 *	  [ 8, 16) request_id               uint64
 *	  [16, 24) cluster_epoch            uint64
 *
 *	Routing-wise the 4-tuple is sufficient.  Tag/transition collision
 *	protection lives in the entry value (HC91), not the key — keeps key
 *	small (HTAB partition lock friendly) while still preventing
 *	backend_id reuse + request_id reset from silent replay.
 * ============================================================ */
typedef struct GcsBlockDedupKey {
	uint32 origin_node_id;
	int32 requester_backend_id;
	uint64 request_id;
	uint64 cluster_epoch;
} GcsBlockDedupKey;

StaticAssertDecl(sizeof(GcsBlockDedupKey) == 24, "spec-2.34 D2 GcsBlockDedupKey 24B "
												 "(origin 4 + backend 4 + req 8 + epoch 8)");


/* ============================================================
 * GcsBlockDedupEntry — fixed-size HTAB entry (HC92 + HC99; 8312B).
 *
 *	Layout (offsets explicit so alignment review is mechanical):
 *	  [    0,    24) key                 GcsBlockDedupKey (24B)
 *	  [   24,    44) tag                 BufferTag (20B)         HC91
 *	  [   44,    45) transition_id       uint8                   HC91
 *	  [   45,    46) status              uint8 (GcsBlockReplyStatus)
 *	  [   46,    56) _pad0[10]           explicit pad to 8-align
 *	  [   56,   104) reply_header        GcsBlockReplyHeader (48B)
 *	  [  104,  8296) block_data          char[GCS_BLOCK_DATA_SIZE]
 *	  [ 8296,  8304) completed_at_ts     TimestampTz (TTL sweep — replied)
 *	  [ 8304,  8312) registered_at_ts    TimestampTz (TTL sweep — in-flight)
 *
 *	reply_header lands at offset 56 = 8 × 7, satisfying the 8-byte
 *	alignment required by reply_header.request_id (uint64).  block_data
 *	is BLCKSZ.  Both TimestampTz fields are 8-aligned (int64) at offsets
 *	8296 and 8304.
 *
 *	GRANTED replies fill block_data with the page bytes; non-GRANTED
 *	replies leave block_data zeroed but still occupy 8KB (PG dynahash
 *	does not support variable-length entries; accepted trade-off per
 *	spec-2.34 §1.4 example).
 *
 *	completed_at_ts is set when the master finishes producing the reply
 *	(success or rejected).  registered_at_ts is set at the moment the
 *	HTAB slot is first inserted (in-flight); the TTL sweep uses the
 *	earlier of the two thresholds so abandoned in-flight slots (master
 *	crashed mid-reply, network drop before reply install) are also
 *	garbage-collected.
 * ============================================================ */
typedef struct GcsBlockDedupEntry {
	GcsBlockDedupKey key;				  /* 24B — HTAB key */
	BufferTag tag;						  /* 20B — HC91 collision check */
	uint8 transition_id;				  /*  1B — HC91 collision check */
	uint8 status;						  /*  1B — GcsBlockReplyStatus */
	uint8 _pad0[10];					  /* 10B — explicit pad; header @ 56 */
	GcsBlockReplyHeader reply_header;	  /* 48B — full reply header (HC99) */
	char block_data[GCS_BLOCK_DATA_SIZE]; /* 8192B — full page payload */
	TimestampTz completed_at_ts;		  /*  8B — TTL sweep replied */
	TimestampTz registered_at_ts;		  /*  8B — TTL sweep in-flight */
} GcsBlockDedupEntry;

StaticAssertDecl(sizeof(GcsBlockDedupEntry) == 8312,
				 "spec-2.34 D2 GcsBlockDedupEntry 8312B "
				 "(key 24 + tag 20 + tx 1 + status 1 + pad 10 + header 48 + "
				 "block 8192 + completed 8 + registered 8)");


/* ============================================================
 * GcsBlockDedupResult — outcome of lookup_or_register.
 *
 *	MISS_REGISTERED       new slot installed; caller proceeds with
 *	                      normal master-side flow then calls
 *	                      cluster_gcs_block_dedup_install_reply.
 *	IN_FLIGHT_DUPLICATE   same key seen but no reply installed yet
 *	                      (concurrent retry; first arrival is still
 *	                      processing).  Caller silently drops; the
 *	                      first arrival's reply will broadcast.
 *	CACHED_REPLY          same key + tag + transition_id match;
 *	                      caller may re-send the cached reply payload
 *	                      without re-flushing WAL or re-copying page.
 *	FORWARDED_DUPLICATE   PGRAC spec-2.35 HC113 NEW — same key seen
 *	                      previously forwarded to a holder (entry was
 *	                      installed with status GRANTED_FROM_HOLDER but
 *	                      master holds no 8KB cached block).  Caller
 *	                      must re-forward GCS_BLOCK_FORWARD to the
 *	                      stored holder (holder side is idempotent;
 *	                      counter forward_replay_count++).  Without
 *	                      this distinct return, the generic IN_FLIGHT_
 *	                      DUPLICATE branch would silently drop the
 *	                      retry and the sender's retransmit budget
 *	                      would never reach a holder reply.
 *	VALIDATION_FAIL       HC91 — same key but different tag or
 *	                      transition_id;  caller replies
 *	                      DENIED_VALIDATOR_REJECT + counter++.
 *	FULL                  HC92 — HTAB at cap;  caller replies
 *	                      DENIED_DEDUP_FULL (sender retries via
 *	                      HC96 transient path).
 * ============================================================ */
typedef enum GcsBlockDedupResult {
	GCS_BLOCK_DEDUP_MISS_REGISTERED = 0,
	GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE = 1,
	GCS_BLOCK_DEDUP_CACHED_REPLY = 2,
	GCS_BLOCK_DEDUP_VALIDATION_FAIL = 3,
	GCS_BLOCK_DEDUP_FULL = 4,
	GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE = 5,	/* HC113 spec-2.35 NEW */
} GcsBlockDedupResult;


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_gcs_block_dedup_lookup_or_register — atomically look up or
 * register a request key.  Returns one of GcsBlockDedupResult.
 *
 *	If MISS_REGISTERED, an in-flight slot has been inserted.
 *	If CACHED_REPLY, cached_reply_out receives a by-value copy of the
 *	cached slot with valid reply_header + block_data ready to re-send.
 *
 *	The API never returns an internal HTAB entry pointer.  TTL sweep,
 *	node-dead cleanup, and backend-exit cleanup can remove entries as soon
 *	as this function releases the dedup lock, so CACHED_REPLY must be
 *	replayed from the copied entry.
 */
extern GcsBlockDedupResult
cluster_gcs_block_dedup_lookup_or_register(const GcsBlockDedupKey *key, BufferTag tag,
										   uint8 transition_id,
										   GcsBlockDedupEntry *cached_reply_out);

/*
 * Register the local backend cleanup hook.  This must be called from
 * sender/backend context, not from the master-side GCS handler, because
 * the handler may run in an auxiliary process without a backend id.
 */
extern void cluster_gcs_block_dedup_register_backend_exit_hook(void);

/*
 * cluster_gcs_block_dedup_install_reply — populate the in-flight slot
 * with the produced reply payload + set completed_at_ts.  Caller MUST
 * have first received MISS_REGISTERED from lookup_or_register.
 *
 *	block_data may be NULL for non-GRANTED status; the entry's block_data
 *	field is zero-filled in that case.
 */
extern void cluster_gcs_block_dedup_install_reply(const GcsBlockDedupKey *key,
												  GcsBlockReplyStatus status,
												  const GcsBlockReplyHeader *header,
												  const char *block_data);

/* Remove a specific entry by key (rare path; mostly used by tests). */
extern void cluster_gcs_block_dedup_remove(const GcsBlockDedupKey *key);


/* ============================================================
 * GC hooks (HC93 — three-fold; HC95 callsite for epoch sweep separate).
 * ============================================================ */

/*
 * cluster_gcs_block_dedup_sweep_expired — TTL sweep called from LMON
 * tick body.  Removes any entry where:
 *
 *	  status != IN_FLIGHT && now - completed_at_ts > expiry_us
 *	OR
 *	  status == IN_FLIGHT && now - registered_at_ts > expiry_us
 *
 *	The expiry threshold is computed from the retransmit budget:
 *	  expiry_us = 2 × max_total_backoff_ms × 1000
 *	(default 2 × 1500 = 3 000 000 us).
 */
extern void cluster_gcs_block_dedup_sweep_expired(TimestampTz now);

/*
 * cluster_gcs_block_dedup_cleanup_on_backend_exit — local hook invoked
 * via before_shmem_exit.  Removes entries where origin_node_id matches
 * the local cluster_node_id and requester_backend_id matches the
 * exiting backend.  Remote master cleanup (origin = some_remote_node,
 * backend exits on remote) is NOT handled here; those entries are
 * reclaimed by TTL or by cleanup_on_node_dead.
 */
extern void cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 origin_node_id,
															int32 backend_id);

/*
 * cluster_gcs_block_dedup_cleanup_on_node_dead — called from spec-2.29
 * CSSD DEAD callback.  Removes all entries with the given origin_node_id.
 */
extern void cluster_gcs_block_dedup_cleanup_on_node_dead(uint32 node_id);


/* ============================================================
 * Observability accessors (counter exposure).
 * ============================================================ */
extern uint64 cluster_gcs_block_dedup_get_hit_count(void);
extern uint64 cluster_gcs_block_dedup_get_miss_count(void);
extern uint64 cluster_gcs_block_dedup_get_collision_count(void);
extern uint64 cluster_gcs_block_dedup_get_full_count(void);
extern uint64 cluster_gcs_block_dedup_get_in_flight_count(void);


/* ============================================================
 * Shmem registry — registered via cluster_shmem_register_region().
 * ============================================================ */
extern Size cluster_gcs_block_dedup_shmem_size(void);
extern void cluster_gcs_block_dedup_shmem_init(void);
extern void cluster_gcs_block_dedup_module_init(void);


#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_GCS_BLOCK_DEDUP_H */
