/*-------------------------------------------------------------------------
 *
 * cluster_ko.c
 *	  KO (object-reuse flush) cross-node barrier -- PURE layer (spec-5.7 §3.5 /
 *	  D6).  Resource-id and shared message codecs, standalone-linkable so the cluster_unit
 *	  test links it directly.  The backend (KO(X) GES lock, the apply-after-drop
 *	  flush fanout + ACK barrier, the peer-side drain, shmem counters) lives in
 *	  cluster_ko_lock.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ko.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ko.h"


/*
 * cluster_ko_resid_encode -- build the KO resource id for a relfilenode.  KO is
 * per-RELFILENODE (field4 = 0, no fork): a DROP/TRUNCATE removes all forks of
 * the relfilenode, so the flush barrier and KO(X) serialise on the relfilenode
 * triple.  field2 = relNumber is the relfilenode (ABA defence).
 */
void
cluster_ko_resid_encode(RelFileLocator rloc, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)rloc.dbOid;
	dst->field2 = (uint32)rloc.relNumber;
	dst->field3 = (uint32)rloc.spcOid;
	dst->field4 = 0;
	dst->type = CLUSTER_KO_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

#ifdef USE_PGRAC_CLUSTER
/* Explicit byte encoding; these helpers grant no lifecycle authority.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
ko_shared_nonzero(const uint8 *bytes, size_t length)
{
	for (size_t i = 0; i < length; i++)
		if (bytes[i] != 0)
			return true;
	return false;
}

static bool
ko_shared_overlap(const void *a, size_t alen, const void *b, size_t blen)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;

	return x <= y ? y - x < alen : x - y < blen;
}

static void
ko_shared_put(uint8 *bytes, uint64 value, unsigned count)
{
	for (unsigned i = 0; i < count; i++)
		bytes[i] = (uint8)(value >> (i * 8));
}

static uint64
ko_shared_get(const uint8 *bytes, unsigned count)
{
	uint64 value = 0;

	for (unsigned i = 0; i < count; i++)
		value |= (uint64)bytes[i] << (i * 8);
	return value;
}

static bool
ko_shared_valid(const ClusterKoSharedMessageV2 *m)
{
	if (m == NULL || m->batch_id == 0 || m->batch_id == UINT64_MAX || m->epoch == 0
		|| m->epoch == UINT64_MAX || m->origin_boot == 0 || m->origin_boot == UINT64_MAX
		|| m->peer_boot == 0 || m->peer_boot == UINT64_MAX || m->key.system_identifier == 0
		|| m->key.database_incarnation == 0 || !ko_shared_nonzero(m->key.storage_uuid, 16)
		|| !ko_shared_nonzero(m->incarnation, 16) || m->key.locator.spcOid == InvalidOid
		|| m->key.locator.relNumber == InvalidRelFileNumber
		|| m->origin_node >= CLUSTER_KO_SHARED_MEMBER_BYTES * 8
		|| m->peer_node >= CLUSTER_KO_SHARED_MEMBER_BYTES * 8 || m->origin_node == m->peer_node
		|| (m->members[m->origin_node / 8] & (1u << (m->origin_node % 8))) == 0
		|| (m->members[m->peer_node / 8] & (1u << (m->peer_node % 8))) == 0
		|| !ko_shared_nonzero(m->member_digest, sizeof(m->member_digest)))
		return false;
	return (m->verb == CLUSTER_KO_SHARED_REQUEST && m->status == CLUSTER_KO_SHARED_REQUEST_STATUS)
		   || (m->verb == CLUSTER_KO_SHARED_ACK
			   && (m->status == CLUSTER_KO_SHARED_DONE || m->status == CLUSTER_KO_SHARED_FAILED));
}

bool
cluster_ko_shared_encode_v2(const ClusterKoSharedMessageV2 *m, void *out, size_t length)
{
	uint8 *bytes = out;

	if (out == NULL || length != CLUSTER_KO_SHARED_V2_BYTES || !ko_shared_valid(m)
		|| ko_shared_overlap(m, sizeof(*m), out, length))
		return false;
	memset(bytes, 0, length);
	memcpy(bytes, "PKO2", 4);
	ko_shared_put(bytes + 4, 2, 2);
	ko_shared_put(bytes + 6, CLUSTER_KO_SHARED_V2_BYTES, 2);
	ko_shared_put(bytes + 8, m->verb, 2);
	ko_shared_put(bytes + 10, m->status, 2);
	ko_shared_put(bytes + 16, m->batch_id, 8);
	ko_shared_put(bytes + 24, m->epoch, 8);
	ko_shared_put(bytes + 32, m->origin_boot, 8);
	ko_shared_put(bytes + 40, m->peer_boot, 8);
	ko_shared_put(bytes + 48, m->key.system_identifier, 8);
	ko_shared_put(bytes + 56, m->key.database_incarnation, 8);
	memcpy(bytes + 64, m->key.storage_uuid, 16);
	memcpy(bytes + 80, m->incarnation, 16);
	ko_shared_put(bytes + 96, m->key.locator.spcOid, 4);
	ko_shared_put(bytes + 100, m->key.locator.dbOid, 4);
	ko_shared_put(bytes + 104, m->key.locator.relNumber, 4);
	ko_shared_put(bytes + 108, m->origin_node, 2);
	ko_shared_put(bytes + 110, m->peer_node, 2);
	memcpy(bytes + 112, m->members, sizeof(m->members));
	memcpy(bytes + 128, m->member_digest, sizeof(m->member_digest));
	return true;
}

bool
cluster_ko_shared_decode_v2(const void *data, size_t length, ClusterKoSharedMessageV2 *out)
{
	const uint8 *bytes = data;
	ClusterKoSharedMessageV2 m = { 0 };

	if (bytes == NULL || out == NULL || length != CLUSTER_KO_SHARED_V2_BYTES
		|| ko_shared_overlap(bytes, length, out, sizeof(*out)) || memcmp(bytes, "PKO2", 4) != 0
		|| ko_shared_get(bytes + 4, 2) != 2
		|| ko_shared_get(bytes + 6, 2) != CLUSTER_KO_SHARED_V2_BYTES
		|| ko_shared_get(bytes + 12, 4) != 0)
		return false;
	m.verb = ko_shared_get(bytes + 8, 2);
	m.status = ko_shared_get(bytes + 10, 2);
	m.batch_id = ko_shared_get(bytes + 16, 8);
	m.epoch = ko_shared_get(bytes + 24, 8);
	m.origin_boot = ko_shared_get(bytes + 32, 8);
	m.peer_boot = ko_shared_get(bytes + 40, 8);
	m.key.system_identifier = ko_shared_get(bytes + 48, 8);
	m.key.database_incarnation = ko_shared_get(bytes + 56, 8);
	memcpy(m.key.storage_uuid, bytes + 64, 16);
	memcpy(m.incarnation, bytes + 80, 16);
	m.key.locator.spcOid = ko_shared_get(bytes + 96, 4);
	m.key.locator.dbOid = ko_shared_get(bytes + 100, 4);
	m.key.locator.relNumber = ko_shared_get(bytes + 104, 4);
	m.origin_node = ko_shared_get(bytes + 108, 2);
	m.peer_node = ko_shared_get(bytes + 110, 2);
	memcpy(m.members, bytes + 112, sizeof(m.members));
	memcpy(m.member_digest, bytes + 128, sizeof(m.member_digest));
	if (!ko_shared_valid(&m))
		return false;
	*out = m;
	return true;
}

bool
cluster_ko_shared_ack_matches_v2(const ClusterKoSharedMessageV2 *request,
								 const ClusterKoSharedMessageV2 *ack)
{
	ClusterKoSharedMessageV2 normalized;
	uint8 expected[CLUSTER_KO_SHARED_V2_BYTES], observed[CLUSTER_KO_SHARED_V2_BYTES];

	if (!ko_shared_valid(request) || !ko_shared_valid(ack)
		|| request->verb != CLUSTER_KO_SHARED_REQUEST || ack->verb != CLUSTER_KO_SHARED_ACK
		|| ack->status != CLUSTER_KO_SHARED_DONE)
		return false;
	normalized = *ack;
	normalized.verb = CLUSTER_KO_SHARED_REQUEST;
	normalized.status = CLUSTER_KO_SHARED_REQUEST_STATUS;
	return cluster_ko_shared_encode_v2(request, expected, sizeof(expected))
		   && cluster_ko_shared_encode_v2(&normalized, observed, sizeof(observed))
		   && memcmp(expected, observed, sizeof(expected)) == 0;
}

static bool
ko_shared_v3_valid(const ClusterKoSharedMessageV3 *m)
{
	const ClusterKoSharedMessageV2 *base;
	uint32 master;

	if (m == NULL || !ko_shared_valid(&m->base))
		return false;
	base = &m->base;
	master = (uint32)m->ax_master_node;
	if (m->ax_resid.type != LOCKTAG_RELATION
		|| m->ax_resid.lockmethodid != DEFAULT_LOCKMETHOD
		|| m->ax_resid.field3 != 0 || m->ax_resid.field4 != 0
		|| m->ax_resid.field1 != base->key.locator.dbOid
		|| m->ax_resid.field2 == InvalidOid
		|| m->ax_holder.node_id >= CLUSTER_KO_SHARED_MEMBER_BYTES * 8
		|| m->ax_holder.cluster_epoch == 0 || m->ax_holder.cluster_epoch == UINT64_MAX
		|| m->ax_holder.cluster_epoch != base->epoch
		|| m->ax_holder.request_id == 0 || m->ax_holder.request_id == UINT64_MAX
		|| m->ax_request_id != m->ax_holder.request_id
		|| m->ax_master_generation == 0 || m->ax_master_generation == UINT64_MAX
		|| m->ax_master_node < 0 || master >= CLUSTER_KO_SHARED_MEMBER_BYTES * 8
		|| (base->members[m->ax_holder.node_id / 8] & (1u << (m->ax_holder.node_id % 8))) == 0
		|| (base->members[master / 8] & (1u << (master % 8))) == 0
		|| m->ax_mode != AccessExclusiveLock
		|| m->full_fork_mask != CLUSTER_KO_SHARED_V3_FULL_FORK_MASK
		|| m->space_mutation_token == 0 || m->space_mutation_token == UINT64_MAX
		|| (m->operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
			&& m->operation != CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE
			&& m->operation != CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD))
		return false;
	return true;
}

bool
cluster_ko_shared_encode_v3(const ClusterKoSharedMessageV3 *m, void *out, size_t length)
{
	uint8 prefix[CLUSTER_KO_SHARED_V2_BYTES];
	uint8 *bytes = out;

	if (out == NULL || length != CLUSTER_KO_SHARED_V3_BYTES || !ko_shared_v3_valid(m)
		|| ko_shared_overlap(m, sizeof(*m), out, length)
		|| !cluster_ko_shared_encode_v2(&m->base, prefix, sizeof(prefix)))
		return false;
	memset(bytes, 0, length);
	memcpy(bytes, prefix, sizeof(prefix));
	memcpy(bytes, "PKO3", 4);
	ko_shared_put(bytes + 4, CLUSTER_KO_SHARED_V3_VERSION, 2);
	ko_shared_put(bytes + 6, CLUSTER_KO_SHARED_V3_BYTES, 2);
	ko_shared_put(bytes + 160, m->ax_resid.field1, 4);
	ko_shared_put(bytes + 164, m->ax_resid.field2, 4);
	ko_shared_put(bytes + 168, m->ax_resid.field3, 4);
	ko_shared_put(bytes + 172, m->ax_resid.field4, 2);
	bytes[174] = m->ax_resid.type;
	bytes[175] = m->ax_resid.lockmethodid;
	ko_shared_put(bytes + 176, m->ax_holder.node_id, 4);
	ko_shared_put(bytes + 180, m->ax_holder.procno, 4);
	ko_shared_put(bytes + 184, m->ax_holder.cluster_epoch, 8);
	ko_shared_put(bytes + 192, m->ax_holder.request_id, 8);
	ko_shared_put(bytes + 200, m->ax_request_id, 8);
	ko_shared_put(bytes + 208, m->ax_master_generation, 8);
	ko_shared_put(bytes + 216, (uint32)m->ax_master_node, 4);
	ko_shared_put(bytes + 220, m->ax_mode, 4);
	ko_shared_put(bytes + 224, m->operation, 2);
	ko_shared_put(bytes + 226, m->full_fork_mask, 2);
	ko_shared_put(bytes + 232, m->space_mutation_token, 8);
	return true;
}

bool
cluster_ko_shared_decode_v3(const void *data, size_t length, ClusterKoSharedMessageV3 *out)
{
	const uint8 *bytes = data;
	ClusterKoSharedMessageV3 m = { 0 };
	uint8 prefix[CLUSTER_KO_SHARED_V2_BYTES];
	ClusterKoSharedMessageV2 base;

	if (bytes == NULL || out == NULL || length != CLUSTER_KO_SHARED_V3_BYTES
		|| ko_shared_overlap(bytes, length, out, sizeof(*out))
		|| memcmp(bytes, "PKO3", 4) != 0
		|| ko_shared_get(bytes + 4, 2) != CLUSTER_KO_SHARED_V3_VERSION
		|| ko_shared_get(bytes + 6, 2) != CLUSTER_KO_SHARED_V3_BYTES
		|| ko_shared_get(bytes + 12, 4) != 0
		|| ko_shared_get(bytes + 228, 4) != 0)
		return false;
	memcpy(prefix, bytes, sizeof(prefix));
	memcpy(prefix, "PKO2", 4);
	ko_shared_put(prefix + 4, 2, 2);
	ko_shared_put(prefix + 6, CLUSTER_KO_SHARED_V2_BYTES, 2);
	if (!cluster_ko_shared_decode_v2(prefix, sizeof(prefix), &base))
		return false;
	m.base = base;
	m.ax_resid.field1 = ko_shared_get(bytes + 160, 4);
	m.ax_resid.field2 = ko_shared_get(bytes + 164, 4);
	m.ax_resid.field3 = ko_shared_get(bytes + 168, 4);
	m.ax_resid.field4 = ko_shared_get(bytes + 172, 2);
	m.ax_resid.type = bytes[174];
	m.ax_resid.lockmethodid = bytes[175];
	m.ax_holder.node_id = ko_shared_get(bytes + 176, 4);
	m.ax_holder.procno = ko_shared_get(bytes + 180, 4);
	m.ax_holder.cluster_epoch = ko_shared_get(bytes + 184, 8);
	m.ax_holder.request_id = ko_shared_get(bytes + 192, 8);
	m.ax_request_id = ko_shared_get(bytes + 200, 8);
	m.ax_master_generation = ko_shared_get(bytes + 208, 8);
	m.ax_master_node = (int32)ko_shared_get(bytes + 216, 4);
	m.ax_mode = ko_shared_get(bytes + 220, 4);
	m.operation = ko_shared_get(bytes + 224, 2);
	m.full_fork_mask = ko_shared_get(bytes + 226, 2);
	m.space_mutation_token = ko_shared_get(bytes + 232, 8);
	if (!ko_shared_v3_valid(&m))
		return false;
	*out = m;
	return true;
}

bool
cluster_ko_shared_result_matches_v3(const ClusterKoSharedMessageV3 *request,
									const ClusterKoSharedMessageV3 *ack)
{
	ClusterKoSharedMessageV3 normalized;
	uint8 expected[CLUSTER_KO_SHARED_V3_BYTES], observed[sizeof(expected)];

	if (!ko_shared_v3_valid(request) || !ko_shared_v3_valid(ack)
		|| request->base.verb != CLUSTER_KO_SHARED_REQUEST
		|| ack->base.verb != CLUSTER_KO_SHARED_ACK
		|| (ack->base.status != CLUSTER_KO_SHARED_DONE
			&& ack->base.status != CLUSTER_KO_SHARED_FAILED))
		return false;
	normalized = *ack;
	normalized.base.verb = CLUSTER_KO_SHARED_REQUEST;
	normalized.base.status = CLUSTER_KO_SHARED_REQUEST_STATUS;
	return cluster_ko_shared_encode_v3(request, expected, sizeof(expected))
		   && cluster_ko_shared_encode_v3(&normalized, observed, sizeof(observed))
		   && memcmp(expected, observed, sizeof(expected)) == 0;
}

bool
cluster_ko_shared_ack_matches_v3(const ClusterKoSharedMessageV3 *request,
								 const ClusterKoSharedMessageV3 *ack)
{
	return ack != NULL && ack->base.status == CLUSTER_KO_SHARED_DONE
		   && cluster_ko_shared_result_matches_v3(request, ack);
}
#endif
