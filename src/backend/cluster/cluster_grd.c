/*-------------------------------------------------------------------------
 *
 * cluster_grd.c
 *	  Global Resource Directory (GRD) routing substrate — spec-2.14.
 *
 *	  Implements ClusterResId 16-byte canonical wire encoding + 4096 shard
 *	  routing via hash_bytes_extended (PG-native) + declared-node-aware
 *	  static master map + observability accessors.
 *
 *	  See cluster_grd.h for the protocol contract, scope边界, performance
 *	  hook design (Stage 6 swap point), counter invariant.
 *	  See spec-2.14-grd-resource-identity-shard-routing.md (frozen v0.4)
 *	  for design rationale.
 *
 *	  AD-002 PCM vs GES 分工 + AD-011 不移植 LC/RC Lock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_grd.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Substrate routing layer only;
 *	  entry holders/waiters/hash table lands in spec-2.15;  caller-side
 *	  integration in spec-2.15+; cross-node real send in spec-2.16.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h" /* cluster_node_id */
#include "cluster/cluster_shmem.h"
#include "common/hashfn.h" /* hash_bytes_extended (spec-2.29 同款) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lock.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/* ============================================================
 * Shmem region state.
 * ============================================================ */

static ClusterGrdShared *cluster_grd_state = NULL;


/* ============================================================
 * Shmem region lifecycle (mirror cluster_ges pattern).
 * ============================================================ */

Size
cluster_grd_shmem_size(void)
{
	return sizeof(ClusterGrdShared);
}

void
cluster_grd_shmem_init(void)
{
	bool found;

	cluster_grd_state = ShmemInitStruct("pgrac cluster grd", cluster_grd_shmem_size(), &found);
	if (!found) {
		int i;

		/* spec-2.14 D3 init zero (Q9 all-atomic, no LWLock). */
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
			pg_atomic_init_u32(&cluster_grd_state->master[i], 0);
		pg_atomic_init_u32(&cluster_grd_state->master_map_initialized, 0);
		pg_atomic_init_u64(&cluster_grd_state->resid_encode_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->shard_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->local_master_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remote_master_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->master_map_refresh_count, 0);
	}
}

static const ClusterShmemRegion cluster_grd_region = {
	.name = "pgrac cluster grd",
	.size_fn = cluster_grd_shmem_size,
	.init_fn = cluster_grd_shmem_init,
	.lwlock_count = 0, /* spec-2.14 Q9: lock-free (L106 inherit) */
	.owner_subsys = "cluster_grd",
	.reserved_flags = 0,
};

void
cluster_grd_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_grd_region);
}


/* ============================================================
 * Wire encoding / decoding (D2; Q4 user-correction).
 *
 *	Explicit field-by-field encode/decode — NOT memcpy(LOCKTAG).
 *	Wire ABI boundary;  LOCKTAG internal layout 与 ClusterResId 16B
 *	wire ABI 解耦.
 *
 *	v0.4 P1.1:  cluster_grd_resid_encode() must fetch_add resid_encode_count
 *	each call (observability;  was missing in v0.2/v0.3 spec body).
 * ============================================================ */

void
cluster_grd_resid_encode(const LOCKTAG *src, ClusterResId *dst)
{
	Assert(src != NULL);
	Assert(dst != NULL);

	dst->field1 = src->locktag_field1;
	dst->field2 = src->locktag_field2;
	dst->field3 = src->locktag_field3;
	dst->field4 = src->locktag_field4;
	dst->type = src->locktag_type;
	dst->lockmethodid = src->locktag_lockmethodid;

	/* v0.4 P1.1:  increment observability counter on every encode. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->resid_encode_count, 1);
}

void
cluster_grd_resid_decode(const ClusterResId *src, LOCKTAG *dst)
{
	Assert(src != NULL);
	Assert(dst != NULL);

	dst->locktag_field1 = src->field1;
	dst->locktag_field2 = src->field2;
	dst->locktag_field3 = src->field3;
	dst->locktag_field4 = src->field4;
	dst->locktag_type = src->type;
	dst->locktag_lockmethodid = src->lockmethodid;
}


/* ============================================================
 * Cluster-aware lock type classifier (Q5 user-correction).
 *
 *	pgrac mapping function — NOT new LockTagType enum.  Returns true
 *	for the 4 cluster-coordinated lock classes; false for all others
 *	(PG-native lock manager handles them locally).
 * ============================================================ */

bool
cluster_grd_is_cluster_aware(const LOCKTAG *tag)
{
	Assert(tag != NULL);

	switch ((LockTagType)tag->locktag_type) {
	case LOCKTAG_RELATION:	  /* TM 表锁跨节点 (feature-024) */
	case LOCKTAG_TRANSACTION: /* TX 行锁等待图 (feature-023) */
	case LOCKTAG_OBJECT:	  /* catalog object 跨节点 */
	case LOCKTAG_ADVISORY:	  /* user lock 跨节点 (feature-078) */
		return true;
	default:
		return false; /* PAGE / TUPLE / RELATION_EXTEND /
									 * VIRTUALTRANSACTION / etc 本地 only */
	}
}


/* ============================================================
 * Performance hook API split (P1.2 v0.2; Stage 6 swap point).
 *
 *	cluster_grd_hash_resource is the ONLY function whose body Stage 6
 *	replaces (xxhash3 / RDMA-aware locality hash).  Hash input is
 *	14 bytes (P1.1 v0.2): field1-3 + type + lockmethodid;  skip ONLY
 *	field4 (tuple offset, co-locates same-page tuples in spec-2.16
 *	batched routing).
 *
 *	Counter invariant (v0.4 P1.2):
 *	  shard_lookup_count >= local_master_lookup_count +
 *	                        remote_master_lookup_count
 *	  shard_lookup() thin wrapper increments total only;
 *	  lookup_master() increments total + local-or-remote.
 * ============================================================ */

uint64
cluster_grd_hash_resource(const ClusterResId *resid)
{
	uint8 hash_input[14];

	Assert(resid != NULL);

	/* Pack 14B input: field1-3 + type + lockmethodid.  Skip ONLY field4. */
	memcpy(&hash_input[0], &resid->field1, 4);
	memcpy(&hash_input[4], &resid->field2, 4);
	memcpy(&hash_input[8], &resid->field3, 4);
	hash_input[12] = resid->type;
	hash_input[13] = resid->lockmethodid; /* v0.2 P1.1: identity 必含 */

	return hash_bytes_extended(hash_input, sizeof(hash_input), 0);
}

uint32
cluster_grd_shard_for_hash(uint64 hash)
{
	return (uint32)(hash % PGRAC_GRD_SHARD_COUNT);
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid)
{
	/* compose hash_resource + shard_for_hash;  no counter (pure). */
	return cluster_grd_shard_for_hash(cluster_grd_hash_resource(resid));
}

int32
cluster_grd_lookup_master(const ClusterResId *resid)
{
	uint32 shard_id;
	int32 master;

	Assert(cluster_grd_state != NULL);

	shard_id = cluster_grd_shard_for_resource(resid);
	pg_atomic_fetch_add_u64(&cluster_grd_state->shard_lookup_count, 1);

	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return -1;

	master = (int32)pg_atomic_read_u32(&cluster_grd_state->master[shard_id]);
	if (master == cluster_node_id)
		pg_atomic_fetch_add_u64(&cluster_grd_state->local_master_lookup_count, 1);
	else
		pg_atomic_fetch_add_u64(&cluster_grd_state->remote_master_lookup_count, 1);

	return master;
}

uint32
cluster_grd_shard_lookup(const ClusterResId *resid)
{
	uint32 shard_id;

	Assert(cluster_grd_state != NULL);

	shard_id = cluster_grd_shard_for_resource(resid);
	/* Thin compat wrapper:  total counter only;  does NOT read master
	 * so local/remote counters NOT incremented (Counter invariant
	 * v0.4 P1.2:  shard_lookup_count >= local + remote). */
	pg_atomic_fetch_add_u64(&cluster_grd_state->shard_lookup_count, 1);
	return shard_id;
}


/* ============================================================
 * Master mapping (Q10 + Q11; declared-node-aware).
 *
 *	v0.4 P2.1 修正:  use existing cluster_conf_lookup_node() scan +
 *	cluster_conf_node_count() cross-check (NOT cluster_conf_get_declared_nodes
 *	which does NOT exist;  规则 23 linkdb SSOT).
 * ============================================================ */

int32
cluster_grd_shard_master(uint32 shard_id)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return -1;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return -1;
	return (int32)pg_atomic_read_u32(&cluster_grd_state->master[shard_id]);
}

bool
cluster_grd_is_local_master(uint32 shard_id)
{
	int32 master = cluster_grd_shard_master(shard_id);

	return master >= 0 && master == cluster_node_id;
}

void
cluster_grd_master_map_init(void)
{
	int32 declared[CLUSTER_MAX_NODES];
	int declared_count = 0;
	int i;

	Assert(cluster_grd_state != NULL);

	/* Q10 + P2.1:  collect declared node_ids in scan order (= 升序)
	 * via existing cluster_conf_lookup_node().  Sparse node_id
	 * (e.g. pgrac.conf declares 0/2/5) yields declared = [0, 2, 5]. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) != NULL)
			declared[declared_count++] = i;
	}
	if (declared_count <= 0) {
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster_grd: no declared nodes in pgrac.conf"),
						errhint("Declare at least one [node.N] entry in pgrac.conf "
								"before initializing the GRD master map.")));
		return;
	}
	Assert(declared_count == cluster_conf_node_count());

	/* Distribute 4096 shards over declared nodes (round-robin in
	 * declared-list order, NOT modulo node_id directly). */
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		int idx = i % declared_count;

		pg_atomic_write_u32(&cluster_grd_state->master[i], (uint32)declared[idx]);
	}
	pg_atomic_write_u32(&cluster_grd_state->master_map_initialized, 1);
	pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}

void
cluster_grd_master_map_refresh(void)
{
	/* Stage 6 DRM placeholder — real implementation will be LMON-mediated
	 * full master[] refresh + epoch field check.  Body for now is a no-op
	 * except for the observability counter increment so future spec-2.X
	 * test可以 observe call site exists. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}


/* ============================================================
 * Observability accessors (D6 dump_grd consumers; 7 accessors).
 *
 *	v0.4 P1.1 修正:  补 shard_lookup_count + resid_encode_count
 *	accessor (v0.2/v0.3 shmem 有 field 但漏 accessor extern).
 * ============================================================ */

uint32
cluster_grd_local_master_count(void)
{
	uint32 count = 0;
	int i;

	Assert(cluster_grd_state != NULL);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		if ((int32)pg_atomic_read_u32(&cluster_grd_state->master[i]) == cluster_node_id)
			count++;
	}
	return count;
}

uint32
cluster_grd_remote_master_count(void)
{
	Assert(cluster_grd_state != NULL);
	return PGRAC_GRD_SHARD_COUNT - cluster_grd_local_master_count();
}

uint64
cluster_grd_shard_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->shard_lookup_count);
}

uint64
cluster_grd_local_master_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->local_master_lookup_count);
}

uint64
cluster_grd_remote_master_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->remote_master_lookup_count);
}

uint64
cluster_grd_resid_encode_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->resid_encode_count);
}

uint64
cluster_grd_master_map_refresh_count_get(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->master_map_refresh_count);
}


/* cluster_get_grd_shards SRF body lives in cluster_grd_srf.c (mirror
 * spec-2.3 cluster_ic_msg_types_srf.c split pattern) so test_cluster_grd
 * standalone unit test可以 link cluster_grd.o without pulling in
 * InitMaterializedSRF / tuplestore_putvalues / etc PG runtime symbols. */
