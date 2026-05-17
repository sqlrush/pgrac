/*-------------------------------------------------------------------------
 *
 * cluster_ges_dedup.h
 *	  pgrac GES retransmit dedup HTAB — spec-2.27 D2.
 *
 *	  spec-2.27 reliability hardening introduces retransmit for GES
 *	  REQUEST / RELEASE messages (cluster_ges.c send helpers gain
 *	  exponential backoff loops).  Retransmit safety requires deduplication
 *	  at the receiver:  re-processing a REQUEST twice would double-grant
 *	  the holder count;  re-processing a RELEASE twice would double-pop
 *	  the wait queue.
 *
 *	  Design (HC51 / HC52):
 *	    - LMS-owned shmem region 'pgrac cluster ges dedup' (entries
 *	      persist across LMS process restart).
 *	    - 5-tuple key:  {origin_node_id, opcode, request_id, cluster_epoch,
 *	      shard_master_generation}.  shard_master_generation is supplied
 *	      by the caller in the wire payload (GesRequestPayload, spec-2.27
 *	      bump 48B→56B).
 *	    - Entry value:  {processed_ts, cached_reply_blob[GesReplyPayload
 *	      size = 52B], cached_reply_len, status}.
 *	    - lookup_or_register() returns an explicit enum (never a bool —
 *	      bool semantics were flagged in v0.1 codex review as silent
 *	      double-grant risk because IN_FLIGHT_DUPLICATE collapsed into
 *	      "miss" and the handler re-processed).
 *	    - Cap `cluster.ges_dedup_max_entries` PGC_POSTMASTER default 8192;
 *	      reaching cap returns FULL fail-closed (caller emits
 *	      GES_REJECT_REASON_WORK_QUEUE_FULL — never evicts in-flight
 *	      entries because eviction would re-introduce double-grant risk).
 *	    - LMS restart causes generation bump → LMS restart sweep
 *	      (cluster_ges_dedup_drop_stale_entries) removes entries whose
 *	      shard_master_generation < current LMS generation BEFORE the
 *	      caller's retransmit arrives.  Caller's retransmit then hits
 *	      the MISS_REGISTERED path with the fresh generation key.
 *	      STALE_REPROCESS is sweep-side bookkeeping only — it counts
 *	      entries swept, and is NEVER returned by
 *	      cluster_ges_dedup_lookup_or_register (an inline stale check
 *	      would loop because the caller would re-insert under the same
 *	      stale key; HC51 in cluster_ges_dedup.c §invalidation-model).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ges_dedup.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-2.27-ges-reliability-hardening.md (FROZEN v0.2).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_DEDUP_H
#define CLUSTER_GES_DEDUP_H

#ifndef FRONTEND

#include "postgres.h"
#include "datatype/timestamp.h"

/* spec-2.27 D2 — 5-tuple dedup key. */
typedef struct ClusterGesDedupKey {
	uint32 origin_node_id;
	uint32 opcode;
	uint64 request_id;
	uint64 cluster_epoch;
	uint64 shard_master_generation;
} ClusterGesDedupKey;

StaticAssertDecl(sizeof(ClusterGesDedupKey) == 32,
				 "ClusterGesDedupKey 32-byte HASH_BLOBS key (5-tuple wire ABI lock)");

/* Lookup status — explicit enum (HC51 / HC52);  never collapse states. */
typedef enum ClusterGesDedupLookupStatus {
	/* New key registered; caller MUST process + call record_reply. */
	CLUSTER_GES_DEDUP_MISS_REGISTERED = 0,
	/* Same key already in flight (registered but reply not yet cached).
	 * Caller MUST drop/defer — do NOT re-process (double-grant risk). */
	CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE = 1,
	/* Cached reply available; caller MUST resend (idempotent retransmit). */
	CLUSTER_GES_DEDUP_CACHED_REPLY = 2,
	/* Sweep-side bookkeeping only — counts entries removed by
	 * cluster_ges_dedup_drop_stale_entries (LMS restart) whose
	 * shard_master_generation < current LMS generation.  **Never returned
	 * by cluster_ges_dedup_lookup_or_register** (inline stale check would
	 * create drop-and-reregister loop; HC51 in cluster_ges_dedup.c
	 * §invalidation-model).  Reserved as enum value 3 for stat surface
	 * (cluster_ges_dedup_stale_reprocess_count accessor). */
	CLUSTER_GES_DEDUP_STALE_REPROCESS = 3,
	/* HTAB at cap; caller MUST fail-closed (REJECT_BUSY) — no eviction. */
	CLUSTER_GES_DEDUP_FULL = 4
} ClusterGesDedupLookupStatus;

/*
 * Probe the dedup HTAB and optionally register a fresh entry.
 *
 *	On MISS_REGISTERED:  fresh entry inserted with status = in-flight;
 *	  caller MUST follow up with cluster_ges_dedup_record_reply().
 *	On CACHED_REPLY:  reply_out is filled with the cached
 *	  GesReplyPayload blob;  *reply_len_out set to the cached length.
 *	On IN_FLIGHT_DUPLICATE / STALE_REPROCESS / FULL:  reply_out untouched.
 */
extern ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register(const ClusterGesDedupKey *key, uint8 *reply_out,
									 uint16 reply_buf_len, uint16 *reply_len_out);

/*
 * Store the cached reply blob for an in-flight entry.
 *
 *	Caller invokes this after generating the GES_REPLY so subsequent
 *	retransmits hit CACHED_REPLY rather than IN_FLIGHT_DUPLICATE.
 */
extern void cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply,
										   uint16 reply_len);

/*
 * Sweep entries whose shard_master_generation < current LMS generation.
 *
 *	Called by LMS at startup after bumping lms_restart_generation so
 *	the prior generation's cached replies are forcibly invalidated.
 *	Returns count swept.
 */
extern uint32 cluster_ges_dedup_drop_stale_entries(void);

/* Observability accessors. */
extern uint32 cluster_ges_dedup_entry_count(void);
extern uint32 cluster_ges_dedup_capacity(void);
extern uint64 cluster_ges_dedup_hit_cached_count(void);
extern uint64 cluster_ges_dedup_in_flight_dup_count(void);
extern uint64 cluster_ges_dedup_stale_reprocess_count(void);
extern uint64 cluster_ges_dedup_full_reject_count(void);

/* Shmem region lifecycle (registered via cluster_init_shmem_module). */
extern Size cluster_ges_dedup_shmem_size(void);
extern void cluster_ges_dedup_shmem_request(void);
extern void cluster_ges_dedup_shmem_init(void);
extern void cluster_ges_dedup_shmem_register(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_DEDUP_H */
