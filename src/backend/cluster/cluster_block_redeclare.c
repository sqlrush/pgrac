/* PGRAC: LMON-owned frozen block census with exact application receipts.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlog.h"
#include "cluster/cluster_block_redeclare.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

static ClusterBlockRedeclareV1 pending;
static bool pending_acked;
static TimestampTz last_send;

static uint64
rd_get(const uint8 *p, unsigned n)
{
	uint64 v = 0;
	for (unsigned i = 0; i < n; i++)
		v |= (uint64)p[i] << (8 * i);
	return v;
}

static void
rd_put(uint8 *p, uint64 v, unsigned n)
{
	for (unsigned i = 0; i < n; i++)
		p[i] = (uint8)(v >> (8 * i));
}

static bool
rd_valid(const ClusterBlockRedeclareV1 *m)
{
	static const uint8 zero[16];
	return m != NULL && m->epoch != 0 && m->nonce != 0 && m->census_hash != 0 && m->source_boot != 0
		   && m->source_boot != UINT64_MAX && m->master_boot != 0 && m->master_boot != UINT64_MAX
		   && m->system_identifier != 0 && memcmp(m->storage_uuid, zero, 16) != 0
		   && m->source_node >= 0 && m->source_node < 32 && m->master_node >= 0
		   && m->master_node < 32
		   && (m->kind == CLUSTER_BLOCK_REDECLARE_REQUEST || m->kind == CLUSTER_BLOCK_REDECLARE_ACK)
		   && (m->mode == PCM_STATE_N || m->mode == PCM_STATE_S || m->mode == PCM_STATE_X)
		   && OidIsValid(m->tag.spcOid) && OidIsValid(m->tag.relNumber)
		   && m->tag.forkNum >= MAIN_FORKNUM && m->tag.forkNum <= MAX_FORKNUM
		   && BlockNumberIsValid(m->tag.blockNum);
}

bool
cluster_block_redeclare_encode_v1(const ClusterBlockRedeclareV1 *m,
								  uint8 bytes[CLUSTER_BLOCK_REDECLARE_BYTES])
{
	if (bytes == NULL)
		return false;
	memset(bytes, 0, CLUSTER_BLOCK_REDECLARE_BYTES);
	if (!rd_valid(m))
		return false;
	memcpy(bytes, "PRDC", 4);
	rd_put(bytes + 4, 1, 2);
	rd_put(bytes + 6, CLUSTER_BLOCK_REDECLARE_BYTES, 2);
	bytes[8] = m->kind;
	bytes[9] = m->mode;
	rd_put(bytes + 16, m->epoch, 8);
	rd_put(bytes + 24, m->nonce, 8);
	rd_put(bytes + 32, m->source_boot, 8);
	rd_put(bytes + 40, m->master_boot, 8);
	rd_put(bytes + 48, m->system_identifier, 8);
	memcpy(bytes + 56, m->storage_uuid, 16);
	rd_put(bytes + 72, m->source_node, 4);
	rd_put(bytes + 76, m->master_node, 4);
	rd_put(bytes + 80, m->tag.spcOid, 4);
	rd_put(bytes + 84, m->tag.dbOid, 4);
	rd_put(bytes + 88, m->tag.relNumber, 4);
	rd_put(bytes + 92, m->tag.forkNum, 4);
	rd_put(bytes + 96, m->tag.blockNum, 4);
	rd_put(bytes + 104, m->page_lsn, 8);
	rd_put(bytes + 112, m->page_scn, 8);
	rd_put(bytes + 120, m->census_hash, 8);
	return true;
}

bool
cluster_block_redeclare_decode_v1(const void *data, Size length, ClusterBlockRedeclareV1 *out)
{
	const uint8 *p = data;
	ClusterBlockRedeclareV1 m = { 0 };
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (p == NULL || length != CLUSTER_BLOCK_REDECLARE_BYTES || memcmp(p, "PRDC", 4) != 0
		|| rd_get(p + 4, 2) != 1 || rd_get(p + 6, 2) != length || rd_get(p + 10, 6) != 0
		|| rd_get(p + 100, 4) != 0)
		return false;
	m.kind = p[8];
	m.mode = p[9];
	m.epoch = rd_get(p + 16, 8);
	m.nonce = rd_get(p + 24, 8);
	m.source_boot = rd_get(p + 32, 8);
	m.master_boot = rd_get(p + 40, 8);
	m.system_identifier = rd_get(p + 48, 8);
	memcpy(m.storage_uuid, p + 56, 16);
	m.source_node = (int32)rd_get(p + 72, 4);
	m.master_node = (int32)rd_get(p + 76, 4);
	m.tag.spcOid = rd_get(p + 80, 4);
	m.tag.dbOid = rd_get(p + 84, 4);
	m.tag.relNumber = rd_get(p + 88, 4);
	m.tag.forkNum = rd_get(p + 92, 4);
	m.tag.blockNum = rd_get(p + 96, 4);
	m.page_lsn = rd_get(p + 104, 8);
	m.page_scn = rd_get(p + 112, 8);
	m.census_hash = rd_get(p + 120, 8);
	if (!rd_valid(&m))
		return false;
	*out = m;
	return true;
}

static bool
rd_current(const ClusterBlockRedeclareV1 *m)
{
	ClusterWalSourceRef local;
	uint64 census_hash;
	uint64 boot = cluster_qvotec_get_self_incarnation();
	return MyBackendType == B_LMON && cluster_enabled && cluster_shared_config && rd_valid(m)
		   && cluster_node_id >= 0 && cluster_node_id < 32
		   && cluster_epoch_get_current() == m->epoch && cluster_qvotec_in_quorum()
		   && !cluster_reconfig_has_pending_prebump_stage()
		   && cluster_membership_get_state(m->source_node) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_state(m->master_node) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(m->source_node) == m->source_boot
		   && cluster_membership_get_last_admitted_incarnation(m->master_node) == m->master_boot
		   && ((cluster_node_id == m->source_node && boot == m->source_boot)
			   || (cluster_node_id == m->master_node && boot == m->master_boot))
		   && cluster_gcs_lookup_master(m->tag) == m->master_node
		   && cluster_grd_block_redeclare_state_v1(m->tag, m->epoch, &census_hash) == 1
		   && census_hash == m->census_hash && GetSystemIdentifier() == m->system_identifier
		   && cluster_wal_thread_current_v2_ref(&local)
		   && local.claim.identity.origin_node_id == cluster_node_id
		   && local.claim.identity.origin_owner_incarnation == boot
		   && local.claim.identity.system_identifier == m->system_identifier
		   && memcmp(local.claim.identity.storage_uuid, m->storage_uuid, 16) == 0;
}

static bool
rd_apply(const ClusterBlockRedeclareV1 *m)
{
	return cluster_node_id == m->master_node && rd_current(m)
		   && cluster_gcs_block_master_rebuild_from_redeclare(m->tag, m->mode, m->page_lsn,
															  m->page_scn, m->source_node, m->epoch)
		   && rd_current(m);
}

bool
cluster_block_redeclare_poll_v1(BufferTag tag, uint8 mode, XLogRecPtr page_lsn, SCN page_scn,
								uint64 epoch, int master)
{
	ClusterBlockRedeclareV1 m = { 0 };
	ClusterWalSourceRef local;
	uint8 bytes[CLUSTER_BLOCK_REDECLARE_BYTES];
	TimestampTz now;
	int state;
	if (MyBackendType != B_LMON || !cluster_enabled || !cluster_shared_config || master < 0
		|| master >= 32)
		return false;
	state = cluster_grd_block_redeclare_state_v1(tag, epoch, &m.census_hash);
	if (state != 1) {
		memset(&pending, 0, sizeof(pending));
		pending_acked = false;
		return state == 0;
	}
	if (!cluster_wal_thread_current_v2_ref(&local))
		return false;
	m.kind = CLUSTER_BLOCK_REDECLARE_REQUEST;
	m.mode = mode;
	m.epoch = epoch;
	m.nonce = pending.nonce != 0 ? pending.nonce : 1;
	m.source_node = cluster_node_id;
	m.master_node = master;
	m.source_boot = cluster_qvotec_get_self_incarnation();
	m.master_boot = cluster_membership_get_last_admitted_incarnation(master);
	m.system_identifier = local.claim.identity.system_identifier;
	memcpy(m.storage_uuid, local.claim.identity.storage_uuid, 16);
	m.tag = tag;
	m.page_lsn = page_lsn;
	m.page_scn = page_scn;
	if (!rd_current(&m))
		return false;
	if (master == cluster_node_id)
		return rd_apply(&m);
	if (pending.nonce == 0 || memcmp(&pending, &m, sizeof(m)) != 0) {
		if (!pg_strong_random(&m.nonce, sizeof(m.nonce)) || m.nonce == 0)
			return false;
		pending = m;
		pending_acked = false;
		last_send = 0;
	}
	if (pending_acked) {
		memset(&pending, 0, sizeof(pending));
		pending_acked = false;
		return true;
	}
	now = GetCurrentTimestamp();
	if (last_send == 0 || now < last_send || now - last_send >= INT64CONST(100000)) {
		if (cluster_block_redeclare_encode_v1(&pending, bytes)) {
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REDECLARE, master, bytes,
										   sizeof(bytes));
			last_send = now;
		}
	}
	return false;
}

void
cluster_block_redeclare_ingress_v1(const ClusterICEnvelope *env, const void *bytes)
{
	ClusterBlockRedeclareV1 m;
	uint8 reply[CLUSTER_BLOCK_REDECLARE_BYTES];
	if (MyBackendType != B_LMON || env == NULL || env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REDECLARE
		|| env->dest_node_id != cluster_node_id
		|| !cluster_block_redeclare_decode_v1(bytes, env->payload_length, &m)
		|| env->epoch != m.epoch || !rd_current(&m))
		return;
	if (m.kind == CLUSTER_BLOCK_REDECLARE_REQUEST) {
		if (env->source_node_id != m.source_node || cluster_node_id != m.master_node
			|| !rd_apply(&m))
			return;
		m.kind = CLUSTER_BLOCK_REDECLARE_ACK;
		if (cluster_block_redeclare_encode_v1(&m, reply))
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REDECLARE, m.source_node, reply,
										   sizeof(reply));
	} else if (env->source_node_id == m.master_node && cluster_node_id == m.source_node) {
		m.kind = CLUSTER_BLOCK_REDECLARE_REQUEST;
		if (pending.nonce != 0 && memcmp(&m, &pending, sizeof(m)) == 0)
			pending_acked = true;
	}
}
