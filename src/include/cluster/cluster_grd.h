/*-------------------------------------------------------------------------
 *
 * cluster_grd.h
 *	  Global Resource Directory (GRD) routing substrate — spec-2.14.
 *
 *	  GRD is the shared substrate (GES + GCS routing) per AD-002 PCM vs
 *	  GES 分工 + feature-011.  This module ships the routing layer only:
 *	    - ClusterResId 16-byte canonical wire encoding
 *	    - 4096 shard fixed via hash_bytes_extended (PG-native)
 *	    - declared-node-aware static master map (atomic master[4096])
 *	    - Observability (pg_cluster_grd_shards view + dump_grd 8 emit_row)
 *
 *	  NOT in this spec (deferred):
 *	    - GRD entry holders[] / waiters / convert queue (spec-2.15)
 *	    - per-shard hash table for entry storage (spec-2.15)
 *	    - per-shard LWLock/slock_t (spec-2.15 true entry table)
 *	    - caller-side LockAcquire integration (spec-2.15+)
 *	    - cross-node real send (spec-2.16; spec-2.13 producer_mask NONE保持)
 *	    - DRM (Stage 6)
 *	    - PCM 复用 (spec-3.X — namespace cluster_grd 已为复用预留)
 *
 *	  Performance hook design (Stage 6 swap point):
 *	    cluster_grd_hash_resource(resid)        -> uint64 hash
 *	        (ONLY function whose body changes when Stage 6 swaps to
 *	         xxhash3 / RDMA-aware locality hash)
 *	    cluster_grd_shard_for_hash(uint64)      -> uint32 shard_id
 *	    cluster_grd_shard_for_resource(resid)   -> uint32 (compose)
 *	    cluster_grd_lookup_master(resid)        -> int32 master_node_id
 *	        (full lookup with local/remote counter)
 *	    cluster_grd_shard_lookup(resid)         -> uint32 shard_id
 *	        (thin compat wrapper; only total counter)
 *
 *	  Hash input is 14 bytes (P1.1 v0.2 user correction):
 *	    field1[4] + field2[4] + field3[4] + type[1] + lockmethodid[1]
 *	    Skip ONLY field4 (tuple offset — co-locate same-page tuples).
 *	    lockmethodid IS included (identity preservation).
 *
 *	  AD-002 PCM vs GES 分工:  GRD is shared substrate (GES + GCS routing).
 *	  AD-011 不移植 LC/RC Lock.
 *
 *	  Spec: spec-2.14-grd-resource-identity-shard-routing.md (frozen v0.4)
 *	  Design: docs/ges-lock-protocol-design.md v1.0 §4 §5
 *	  Feature: feature-011 GRD resource sharding (本 spec land 路由 substrate 子集)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_grd.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All symbols are backend-only (#ifndef FRONTEND) to prevent frontend
 *	  tools from accidentally pulling in cluster_grd_state references
 *	  (L8 inheritance + spec-2.11 P2 pattern).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GRD_H
#define CLUSTER_GRD_H

#ifndef FRONTEND

#include "port/atomics.h"
#include "storage/lock.h" /* LOCKTAG */

/*
 * 4096 shard fixed (Q8 user-approved).  Future amend path:
 *   - 实测 hot shard → 升 GUC cluster.grd_shard_count (spec-2.X future)
 *   - shard count change = wire ABI 不变(routing 仅本节点 in-memory);只需
 *     reconfig 时全集群同步 amend(LMON 协调)
 */
#define PGRAC_GRD_SHARD_COUNT 4096

/*
 * ClusterResId — 16-byte canonical wire encoding (Q4 user-correction).
 *
 *	Maps to LOCKTAG fields via explicit cluster_grd_resid_encode/decode
 *	functions.  DO NOT memcpy(LOCKTAG) directly — wire ABI must be
 *	stable across PG version升级.  Layout intentionally mirrors LOCKTAG
 *	field sizes (16 bytes total) to allow trivial 1:1 mapping today
 *	while keeping encode/decode functions as the wire-ABI boundary.
 */
typedef struct ClusterResId {
	uint32 field1;		/* maps from LOCKTAG.locktag_field1 */
	uint32 field2;		/* maps from LOCKTAG.locktag_field2 */
	uint32 field3;		/* maps from LOCKTAG.locktag_field3 */
	uint16 field4;		/* maps from LOCKTAG.locktag_field4 */
	uint8 type;			/* maps from LOCKTAG.locktag_type */
	uint8 lockmethodid; /* maps from LOCKTAG.locktag_lockmethodid */
} ClusterResId;

StaticAssertDecl(sizeof(ClusterResId) == 16, "ClusterResId wire ABI 16-byte lock (spec-2.14 §2.1)");

/*
 * Opaque placeholder typedef (Q12 user-approved).  Real holders[] +
 * waiters + convert queue lands in spec-2.15 GES lock mode + grant
 * table.  Forward-declared here so spec-2.15+ can reference symbol
 * without amending this header (only struct body grows).
 */
typedef struct ClusterGrdEntry ClusterGrdEntry;

/*
 * Shmem region "pgrac cluster grd" — Q9 lock-free + Q11 shmem cache.
 *	master[i] = node_id that owns shard i (atomic uint32; LMON-mediated
 *	init + future DRM refresh).
 *	v0.2 observability expansion + v0.4 P1.1 补全:  6 counters for
 *	answer "lookup volume / local vs remote ratio / refresh history /
 *	encode volume" without後续 spec 追加 emit_row.
 */
typedef struct ClusterGrdShared {
	pg_atomic_uint32 master[PGRAC_GRD_SHARD_COUNT];
	pg_atomic_uint32 master_map_initialized;	 /* 0 until LMON init */
	pg_atomic_uint64 resid_encode_count;		 /* incremented per encode */
	pg_atomic_uint64 shard_lookup_count;		 /* total lookups */
	pg_atomic_uint64 local_master_lookup_count;	 /* lookup_master == self */
	pg_atomic_uint64 remote_master_lookup_count; /* lookup_master != self */
	pg_atomic_uint64 master_map_refresh_count;	 /* init + future DRM */
} ClusterGrdShared;

extern Size cluster_grd_shmem_size(void);
extern void cluster_grd_shmem_init(void);
extern void cluster_grd_shmem_register(void);

/*
 * Wire encoding / decoding — Q4 user-correction:  explicit
 * field-by-field encode/decode,  NOT memcpy(LOCKTAG).  Wire ABI boundary.
 *
 *	v0.4 P1.1: cluster_grd_resid_encode() must fetch_add resid_encode_count
 *	each call (observability — was missing in v0.2/v0.3 spec body).
 */
extern void cluster_grd_resid_encode(const LOCKTAG *src, ClusterResId *dst);
extern void cluster_grd_resid_decode(const ClusterResId *src, LOCKTAG *dst);

/*
 * Cluster-aware lock type classifier — Q5 user-correction:  pgrac
 * mapping function,  NOT new LockTagType enum value.  Returns true if
 * LOCKTAG is a cluster-coordinated lock (currently 4 classes:
 * RELATION / TRANSACTION / OBJECT / ADVISORY).  TT/IS/CI/XR/CLUSTER_*
 * deferred to spec-2.X.
 */
extern bool cluster_grd_is_cluster_aware(const LOCKTAG *tag);

/*
 * Performance hook API (P1.2 v0.2 split — Stage 6 single-swap point).
 *
 *	cluster_grd_hash_resource:  pure hash function.  ONLY function
 *	  whose body Stage 6 替换 (xxhash3 / RDMA-aware locality hash).
 *	  Hash input is 14 bytes (P1.1 v0.2):
 *	    field1[4] + field2[4] + field3[4] + type[1] + lockmethodid[1]
 *	  Skip ONLY field4 (tuple offset);  lockmethodid IS included for
 *	  identity preservation.
 *	cluster_grd_shard_for_hash:  pure modulo.
 *	cluster_grd_shard_for_resource:  compose hash_resource +
 *	  shard_for_hash (no counter increment).
 *	cluster_grd_lookup_master:  full lookup (shard_for_resource +
 *	  master[shard] + total counter + local-or-remote counter).
 *	  Returns master node_id, or -1 when the master map is not initialized.
 *	cluster_grd_shard_lookup:  thin compat wrapper.  Returns shard_id
 *	  + increments total counter only (does NOT read master, hence
 *	  does NOT increment local/remote counter).
 *
 *	Counter invariant (v0.4 P1.2):
 *	  shard_lookup_count >= local_master_lookup_count +
 *	                        remote_master_lookup_count
 *	  (>= not =;  shard_lookup increments total without master read).
 */
extern uint64 cluster_grd_hash_resource(const ClusterResId *resid);
extern uint32 cluster_grd_shard_for_hash(uint64 hash);
extern uint32 cluster_grd_shard_for_resource(const ClusterResId *resid);
extern int32 cluster_grd_lookup_master(const ClusterResId *resid);
extern uint32 cluster_grd_shard_lookup(const ClusterResId *resid);

/*
 * Master mapping — Q10 + Q11 user-correction:
 *	master[shard_id] is initialized via cluster_grd_master_map_init()
 *	on LMON startup.  Mapping is declared-node-aware:
 *	  declared_list = scan 0..CLUSTER_MAX_NODES via existing
 *	    cluster_conf_lookup_node() (skip NULL);  scan order = sorted
 *	    node_id ascending;  Assert(len == cluster_conf_node_count())
 *	  idx = shard_id % len(declared_list)
 *	  master[shard_id] = declared_list[idx]
 *	NOT modulo cluster_node_id directly (node_id can be sparse;
 *	pgrac.conf allows 0/2/5 declared without 1/3/4).
 */
extern int32 cluster_grd_shard_master(uint32 shard_id);
extern bool cluster_grd_is_local_master(uint32 shard_id);

/*
 * Master map lifecycle — LMON entry point (D3).
 */
extern void cluster_grd_master_map_init(void);
extern void cluster_grd_master_map_refresh(void); /* Stage 6 DRM placeholder */

/*
 * Observability accessors — D6 dump_grd consumers (8 emit_row in
 * cluster_debug.c).
 *
 *	v0.4 P1.1 修正:  v0.3 D6 列 8 emit_row 但 v0.3 §2.1 extern 缺
 *	cluster_grd_shard_lookup_count + cluster_grd_resid_encode_count;
 *	v0.4 补 2 accessor → 7 total.
 */
extern uint32 cluster_grd_local_master_count(void);
extern uint32 cluster_grd_remote_master_count(void);
extern uint64 cluster_grd_shard_lookup_count(void);
extern uint64 cluster_grd_local_master_lookup_count(void);
extern uint64 cluster_grd_remote_master_lookup_count(void);
extern uint64 cluster_grd_resid_encode_count(void);
extern uint64 cluster_grd_master_map_refresh_count_get(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GRD_H */
