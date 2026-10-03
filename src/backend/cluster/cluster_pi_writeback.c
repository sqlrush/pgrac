/* PGRAC: bounded background DATA notifications and physical PI completion.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "cluster_pi_data_wire.h"

#define WB_RETRY_US INT64CONST(100000)
enum { WB_EMPTY, WB_QUEUED, WB_RUNNING, WB_REPLIED };

typedef struct WritebackShared {
	slock_t lock;
	uint64 outbound_revision;
	int32 outbound_pid;
	bool outbound_complete;
	ClusterPiWritebackMessageV1 outbound;
	ClusterPiWritebackMessageV1 completed;
	uint64 inbound_revision;
	uint32 inbound_state;
	int32 inbound_pid;
	bool reply_pending;
	ClusterPiWritebackMessageV1 inbound;
	ClusterPiWritebackMessageV1 reply;
	int32 bgwriter_procno;
	int32 bgwriter_pid;
	int32 checkpointer_pid;
	ClusterPiWritebackRejectionsV1 rejections;
	uint64 logged_epoch[CLUSTER_PI_WRITEBACK_REJECTION_COUNT];
	uint64 logged_boot[CLUSTER_PI_WRITEBACK_REJECTION_COUNT];
} WritebackShared;

struct ClusterPiWritebackJobV1 {
	ResourceOwner owner;
	pid_t pid;
	uint64 revision;
	bool stale;
	bool complete;
	ClusterPiWritebackMessageV1 requested;
	ClusterPiWritebackMessageV1 accepted;
};

struct ClusterPiWritebackNoticeV1 {
	ResourceOwner owner;
	pid_t pid;
	uint64 revision;
	ClusterPiWritebackMessageV1 request;
	ClusterWalInputsV1 *inputs;
	ClusterThreadRecoveryFabricPlanV1 *plan;
};

typedef struct WritebackBatch {
	ResourceOwner owner;
	pid_t pid;
	uint64 epoch;
	ClusterWalSourceRef local;
	uint32 count, data_index, peer_index, group_count, source_count;
	BufferTag tags[CLUSTER_PI_WRITEBACK_MAX];
	ClusterPcmPiWriteCutV1 write_cuts[CLUSTER_PI_WRITEBACK_MAX];
	ClusterPcmPiStorageCutV1 storage_cuts[CLUSTER_PI_WRITEBACK_MAX];
	ClusterPageDataReceiptV1 *receipts[CLUSTER_PI_WRITEBACK_MAX];
	ClusterPiPhysicalAckV1 *acks[CLUSTER_PI_WRITEBACK_MAX][RESOURCE_X_PROTOCOL_NODE_LIMIT];
	uint32 ack_count[CLUSTER_PI_WRITEBACK_MAX];
	uint32 group_indices[CLUSTER_PI_WRITEBACK_MAX];
	bool qualified[CLUSTER_PI_WRITEBACK_MAX];
	ClusterPiDataV1 *data_job;
	ClusterPiWritebackJobV1 *physical_job;
	ClusterWalInputsV1 *inputs;
	ClusterThreadRecoveryFabricPlanV1 *plan;
	ClusterWalSourceRef sources[CLUSTER_WAL_INPUTS_MAX];
} WritebackBatch;

static WritebackShared *wb_shared;
static ClusterPiWritebackJobV1 *wb_active;
static ClusterPiWritebackNoticeV1 *wb_notice;
static WritebackBatch *wb_batch;
static uint32 wb_scan_cursor;
static bool wb_callbacks;
static TimestampTz wb_last_send;
static uint64 wb_sent_revision, wb_sent_inbound;
static bool wb_sent_reply;

bool
cluster_pi_writeback_rejections_v1(ClusterPiWritebackRejectionsV1 *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (wb_shared == NULL)
		return false;
	SpinLockAcquire(&wb_shared->lock);
	*out = wb_shared->rejections;
	SpinLockRelease(&wb_shared->lock);
	return true;
}

/* Count every rejected page attempt, but do not flood the log when a hot or
 * unavailable page is selected again. The throttle belongs to this node's
 * boot/membership cut, not a batch nonce or a checkpoint. No diagnostic
 * observation authorizes retirement or removes the page from later scans. */
static void
wb_rejected(ClusterPiWritebackRejectionV1 reason, const BufferTag *tag, int32 peer, uint64 epoch,
			uint64 boot)
{
	static const char *const names[]
		= { "DATA_PROOF", "LOCAL_ACK", "REMOTE_ACK", "MASTER_CUT", "PEER_PHYSICAL", "RECOVERY_PROOF" };
	bool log;
	if (wb_shared == NULL || tag == NULL || (uint32)reason >= CLUSTER_PI_WRITEBACK_REJECTION_COUNT)
		return;
	SpinLockAcquire(&wb_shared->lock);
	if (wb_shared->rejections.attempts[reason] != UINT64_MAX)
		wb_shared->rejections.attempts[reason]++;
	wb_shared->rejections.last_resource = *tag;
	wb_shared->rejections.last_reason = reason;
	wb_shared->rejections.last_peer = peer;
	log = wb_shared->logged_epoch[reason] != epoch || wb_shared->logged_boot[reason] != boot;
	if (log) {
		wb_shared->logged_epoch[reason] = epoch;
		wb_shared->logged_boot[reason] = boot;
		if (wb_shared->rejections.log_events != UINT64_MAX)
			wb_shared->rejections.log_events++;
	}
	SpinLockRelease(&wb_shared->lock);
	if (log)
		elog(LOG,
			 "PGRAC_FAMILY=PI_WRITEBACK action=PAGE_DEFERRED stage=%s "
			 "resource=%u/%u/%u/%u/%u peer=%d epoch=" UINT64_FORMAT " boot=" UINT64_FORMAT,
			 names[reason], tag->spcOid, tag->dbOid, tag->relNumber, tag->forkNum, tag->blockNum,
			 peer, epoch, boot);
}

static void
wb_forget_batch(void)
{
	if (wb_shared != NULL) {
		SpinLockAcquire(&wb_shared->lock);
		if (wb_shared->checkpointer_pid == MyProcPid)
			wb_shared->checkpointer_pid = 0;
		SpinLockRelease(&wb_shared->lock);
	}
	wb_batch = NULL;
}

static const BufferTag *
wb_tag(const ClusterPiDataFactV1 *f)
{
	return f->write_cut.binding_generation != 0 ? &f->write_cut.holder.assertion.resource
												: &f->storage_cut.resource;
}

static int32
wb_master(const ClusterPiDataFactV1 *f)
{
	return f->write_cut.binding_generation != 0 ? f->write_cut.master_node
												: f->storage_cut.master_node;
}

static uint64
wb_master_boot(const ClusterPiDataFactV1 *f)
{
	return f->write_cut.binding_generation != 0 ? f->write_cut.holder.master_session_incarnation
												: f->storage_cut.master_session_incarnation;
}

static uint32
wb_holders(const ClusterPiDataFactV1 *f)
{
	return f->write_cut.binding_generation != 0 ? f->write_cut.pi_holders_bitmap
												: f->storage_cut.pi_holders_bitmap;
}

static bool
wb_namespace(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return a->claim.identity.system_identifier == b->claim.identity.system_identifier
		   && a->claim.database_incarnation == b->claim.database_incarnation
		   && memcmp(a->claim.identity.storage_uuid, b->claim.identity.storage_uuid, 16) == 0;
}

/* A later selected ROOT ceiling may cover the original immutable claim. */
static bool
wb_source_covered(const ClusterWalSourceRef *native, const ClusterWalSourceRef *selected)
{
	return cluster_wal_claim_v2_ref_valid(&native->claim)
		   && cluster_wal_claim_v2_ref_valid(&selected->claim) && native->timeline != 0
		   && native->timeline == selected->timeline
		   && memcmp(&native->claim.identity, &selected->claim.identity,
					 sizeof(native->claim.identity))
				  == 0
		   && native->claim.database_incarnation == selected->claim.database_incarnation
		   && native->claim.max_config_generation <= selected->claim.max_config_generation
		   && memcmp(native->claim.claim_sha256, selected->claim.claim_sha256, 32) == 0;
}

static bool
wb_fact_valid(const ClusterPiDataFactV1 *f)
{
	static const ClusterPcmPiWriteCutV1 no_x;
	static const ClusterPcmPiStorageCutV1 no_storage;
	BufferTag tag;
	if (f == NULL || !cluster_page_wal_binding_shape_v1(&f->binding))
		return false;
	if (!(cluster_pcm_pi_write_cut_valid_v1(&f->write_cut) && f->write_cut.pi_holders_bitmap != 0
		  && memcmp(&f->storage_cut, &no_storage, sizeof(no_storage)) == 0
		  && f->binding.flags == CLUSTER_PAGE_WAL_NATIVE_FLUSHED)
		&& !(cluster_pcm_pi_storage_cut_valid_v1(&f->storage_cut)
			 && f->storage_cut.pi_holders_bitmap != 0
			 && memcmp(&f->write_cut, &no_x, sizeof(no_x)) == 0))
		return false;
	InitBufferTag(&tag, &f->binding.identity.locator, f->binding.identity.forknum,
				  f->binding.identity.blockno);
	return BufferTagsEqual(&tag, wb_tag(f));
}

static bool
wb_message_valid(const ClusterPiWritebackMessageV1 *m)
{
	int32 peer;
	if (m == NULL || (m->verb != CLUSTER_PI_WRITEBACK_NOTIFY && m->verb != CLUSTER_PI_WRITEBACK_ACK)
		|| m->nonce == 0 || m->epoch == 0 || m->epoch == UINT64_MAX
		|| (m->count == 0 && m->verb != CLUSTER_PI_WRITEBACK_ACK)
		|| m->count > CLUSTER_PI_WRITEBACK_MAX || !cluster_wal_claim_v2_ref_valid(&m->peer.claim)
		|| m->peer.timeline == 0)
		return false;
	peer = m->peer.claim.identity.origin_node_id;
	if (peer < 0 || peer >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return false;
	for (uint32 i = 0; i < m->count; i++) {
		const ClusterPiDataFactV1 *f = &m->facts[i];
		if (!wb_fact_valid(f) || peer == wb_master(f) || (wb_holders(f) & ((uint32)1u << peer)) == 0
			|| wb_master(f) != wb_master(&m->facts[0])
			|| wb_master_boot(f) != wb_master_boot(&m->facts[0])
			|| !wb_namespace(&f->binding.source, &m->peer))
			return false;
		for (uint32 j = 0; j < i; j++)
			if (BufferTagsEqual(wb_tag(f), wb_tag(&m->facts[j])))
				return false;
	}
	return true;
}

static void
wb_storage_encode(uint8 *p, const ClusterPcmPiStorageCutV1 *c)
{
	const PcmAuthoritySnapshot *a = &c->authority;
	ClusterPcmPiWriteCutV1 temporary = { 0 };
	uint8 x[128] = { 0 };
	pi_data_put(p, a->master_holder.node_id, 4);
	pi_data_put(p + 4, a->master_holder.procno, 4);
	pi_data_put(p + 8, a->master_holder.cluster_epoch, 8);
	pi_data_put(p + 16, a->master_holder.request_id, 8);
	pi_data_put(p + 24, a->transition_count, 8);
	pi_data_put(p + 32, a->pending_x_since_lsn, 8);
	pi_data_put(p + 40, a->state, 4);
	pi_data_put(p + 44, a->x_holder_node, 4);
	pi_data_put(p + 48, a->s_holders_bitmap, 4);
	pi_data_put(p + 52, a->pending_x_requester_node, 4);
	temporary.holder = c->waiting;
	pi_data_cut_encode(x, &temporary);
	memcpy(p + 64, x, 96);
	pi_data_put(p + 160, c->resource.spcOid, 4);
	pi_data_put(p + 164, c->resource.dbOid, 4);
	pi_data_put(p + 168, c->resource.relNumber, 4);
	pi_data_put(p + 172, c->resource.forkNum, 4);
	pi_data_put(p + 176, c->resource.blockNum, 4);
	pi_data_put(p + 180, c->pi_holders_bitmap, 4);
	pi_data_put(p + 184, c->binding_generation, 8);
	pi_data_put(p + 192, c->resource_formation, 8);
	pi_data_put(p + 200, c->authority_generation, 8);
	pi_data_put(p + 208, c->master_generation, 8);
	pi_data_put(p + 216, c->master_session_incarnation, 8);
	pi_data_put(p + 224, c->master_node, 4);
}

static bool
wb_storage_decode(const uint8 *p, ClusterPcmPiStorageCutV1 *c)
{
	PcmAuthoritySnapshot *a = &c->authority;
	ClusterPcmPiWriteCutV1 temporary = { 0 };
	uint8 x[128] = { 0 };
	if (pi_data_get(p + 56, 8) || pi_data_get(p + 228, 4))
		return false;
	a->master_holder.node_id = pi_data_get(p, 4);
	a->master_holder.procno = pi_data_get(p + 4, 4);
	a->master_holder.cluster_epoch = pi_data_get(p + 8, 8);
	a->master_holder.request_id = pi_data_get(p + 16, 8);
	a->transition_count = pi_data_get(p + 24, 8);
	a->pending_x_since_lsn = pi_data_get(p + 32, 8);
	a->state = pi_data_get(p + 40, 4);
	a->x_holder_node = (int32)pi_data_get(p + 44, 4);
	a->s_holders_bitmap = pi_data_get(p + 48, 4);
	a->pending_x_requester_node = (int32)pi_data_get(p + 52, 4);
	memcpy(x, p + 64, 96);
	pi_data_cut_decode(x, &temporary);
	c->waiting = temporary.holder;
	c->resource.spcOid = pi_data_get(p + 160, 4);
	c->resource.dbOid = pi_data_get(p + 164, 4);
	c->resource.relNumber = pi_data_get(p + 168, 4);
	c->resource.forkNum = pi_data_get(p + 172, 4);
	c->resource.blockNum = pi_data_get(p + 176, 4);
	c->pi_holders_bitmap = pi_data_get(p + 180, 4);
	c->binding_generation = pi_data_get(p + 184, 8);
	c->resource_formation = pi_data_get(p + 192, 8);
	c->authority_generation = pi_data_get(p + 200, 8);
	c->master_generation = pi_data_get(p + 208, 8);
	c->master_session_incarnation = pi_data_get(p + 216, 8);
	c->master_node = (int32)pi_data_get(p + 224, 4);
	return true;
}

bool
cluster_pi_writeback_encode_v1(const ClusterPiWritebackMessageV1 *m, uint8 *bytes, Size capacity,
							   Size *length)
{
	ClusterPageWalBindingV1 peer = { 0 };
	uint8 source[232] = { 0 };
	Size n;
	if (pi_data_overlap(m, sizeof(*m), bytes, capacity)
		|| pi_data_overlap(length, sizeof(*length), bytes, capacity)
		|| pi_data_overlap(length, sizeof(*length), m, sizeof(*m)))
		return false;
	if (length != NULL)
		*length = 0;
	if (bytes == NULL || length == NULL || !wb_message_valid(m))
		return false;
	n = CLUSTER_PI_WRITEBACK_HEADER_BYTES + (Size)m->count * CLUSTER_PI_WRITEBACK_FACT_BYTES;
	if (capacity < n)
		return false;
	memset(bytes, 0, n);
	memcpy(bytes, "PPWB", 4);
	pi_data_put(bytes + 4, 1, 2);
	pi_data_put(bytes + 6, CLUSTER_PI_WRITEBACK_HEADER_BYTES, 2);
	pi_data_put(bytes + 8, m->verb, 4);
	pi_data_put(bytes + 12, m->count, 4);
	pi_data_put(bytes + 16, m->nonce, 8);
	pi_data_put(bytes + 24, m->epoch, 8);
	peer.source = m->peer;
	pi_data_binding_encode(source, &peer);
	memcpy(bytes + 32, source, 136);
	for (uint32 i = 0; i < m->count; i++) {
		uint8 *p
			= bytes + CLUSTER_PI_WRITEBACK_HEADER_BYTES + (Size)i * CLUSTER_PI_WRITEBACK_FACT_BYTES;
		pi_data_binding_encode(p, &m->facts[i].binding);
		pi_data_cut_encode(p + 232, &m->facts[i].write_cut);
		wb_storage_encode(p + 360, &m->facts[i].storage_cut);
	}
	*length = n;
	return true;
}

bool
cluster_pi_writeback_decode_v1(const void *data, Size length, ClusterPiWritebackMessageV1 *out)
{
	const uint8 *bytes = data;
	ClusterPiWritebackMessageV1 m = { 0 };
	ClusterPageWalBindingV1 peer = { 0 };
	uint8 source[232] = { 0 };
	if (pi_data_overlap(data, length, out, sizeof(*out)))
		return false;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (bytes == NULL || out == NULL || length < CLUSTER_PI_WRITEBACK_HEADER_BYTES
		|| length > CLUSTER_PI_WRITEBACK_MAX_BYTES || memcmp(bytes, "PPWB", 4) != 0
		|| pi_data_get(bytes + 4, 2) != 1
		|| pi_data_get(bytes + 6, 2) != CLUSTER_PI_WRITEBACK_HEADER_BYTES
		|| pi_data_get(bytes + 168, 8))
		return false;
	m.verb = pi_data_get(bytes + 8, 4);
	m.count = pi_data_get(bytes + 12, 4);
	m.nonce = pi_data_get(bytes + 16, 8);
	m.epoch = pi_data_get(bytes + 24, 8);
	if ((m.count == 0 && m.verb != CLUSTER_PI_WRITEBACK_ACK) || m.count > CLUSTER_PI_WRITEBACK_MAX
		|| length
			   != CLUSTER_PI_WRITEBACK_HEADER_BYTES
					  + (Size)m.count * CLUSTER_PI_WRITEBACK_FACT_BYTES)
		return false;
	memcpy(source, bytes + 32, 136);
	if (!pi_data_binding_decode(source, &peer))
		return false;
	m.peer = peer.source;
	for (uint32 i = 0; i < m.count; i++) {
		const uint8 *p
			= bytes + CLUSTER_PI_WRITEBACK_HEADER_BYTES + (Size)i * CLUSTER_PI_WRITEBACK_FACT_BYTES;
		if (!pi_data_binding_decode(p, &m.facts[i].binding)
			|| !wb_storage_decode(p + 360, &m.facts[i].storage_cut))
			return false;
		pi_data_cut_decode(p + 232, &m.facts[i].write_cut);
	}
	if (!wb_message_valid(&m))
		return false;
	*out = m;
	return true;
}

static bool
wb_same_request(const ClusterPiWritebackMessageV1 *request,
				const ClusterPiWritebackMessageV1 *reply)
{
	return request->count == reply->count && request->nonce == reply->nonce
		   && request->epoch == reply->epoch && wb_source_covered(&reply->peer, &request->peer)
		   && memcmp(request->facts, reply->facts, (Size)request->count * sizeof(request->facts[0]))
				  == 0;
}

/* An ACK is an ordered exact subset of one immutable request. Missing
 * items carry no evidence; an empty response only finishes this attempt. */
static bool
wb_reply_matches(const ClusterPiWritebackMessageV1 *request,
				 const ClusterPiWritebackMessageV1 *reply)
{
	uint32 at = 0;
	if (!wb_message_valid(request) || !wb_message_valid(reply)
		|| request->verb != CLUSTER_PI_WRITEBACK_NOTIFY || reply->verb != CLUSTER_PI_WRITEBACK_ACK
		|| reply->count > request->count || request->nonce != reply->nonce
		|| request->epoch != reply->epoch || !wb_source_covered(&reply->peer, &request->peer))
		return false;
	for (uint32 i = 0; i < reply->count; i++) {
		while (at < request->count
			   && memcmp(&request->facts[at], &reply->facts[i], sizeof(reply->facts[i])) != 0)
			at++;
		if (at == request->count)
			return false;
		at++;
	}
	return true;
}

static bool
wb_background(void)
{
	return (MyBackendType == B_BG_WRITER || MyBackendType == B_CHECKPOINTER
			|| MyBackendType == B_BG_WORKER)
		   && CritSectionCount == 0 && CurrentResourceOwner != NULL;
}

static bool
wb_fact_current(const ClusterPiDataFactV1 *f, const ClusterWalSourceRef *peer, uint64 epoch,
				bool master)
{
	ClusterWalSourceRef local;
	int32 node = peer->claim.identity.origin_node_id, owner = wb_master(f);
	uint64 boot = peer->claim.identity.origin_owner_incarnation, owner_boot = wb_master_boot(f);
	return cluster_enabled && cluster_shared_config && wb_fact_valid(f) && node >= 0
		   && node < RESOURCE_X_PROTOCOL_NODE_LIMIT && node != owner
		   && cluster_wal_claim_v2_ref_valid(&peer->claim) && peer->timeline != 0
		   && !RecoveryInProgress() && !cluster_normal_stop_requested()
		   && cluster_qvotec_in_quorum() && !cluster_reconfig_has_pending_prebump_stage()
		   && cluster_epoch_get_current() == epoch && cluster_node_id == (master ? owner : node)
		   && cluster_qvotec_get_self_incarnation() == (master ? owner_boot : boot)
		   && cluster_membership_get_state(owner) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(owner) == owner_boot
		   && cluster_membership_get_state(node) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(node) == boot
		   && cluster_gcs_lookup_master(*wb_tag(f)) == owner
		   && GetSystemIdentifier() == peer->claim.identity.system_identifier
		   && cluster_wal_thread_current_v2_ref(&local)
		   && local.claim.identity.origin_node_id == cluster_node_id
		   && local.claim.identity.origin_owner_incarnation == (master ? owner_boot : boot)
		   && wb_namespace(&local, peer) && wb_namespace(&f->binding.source, peer)
		   && (master || wb_source_covered(&local, peer));
}

static bool
wb_current(const ClusterPiWritebackMessageV1 *m, bool master)
{
	if (!wb_message_valid(m))
		return false;
	for (uint32 i = 0; i < m->count; i++)
		if (!wb_fact_current(&m->facts[i], &m->peer, m->epoch, master))
			return false;
	return true;
}

static bool
wb_master_fact_cut(const ClusterPiDataFactV1 *f)
{
	if (f->write_cut.binding_generation != 0) {
		ClusterPcmPiWriteCutV1 cut;
		return cluster_pcm_lock_pi_write_snapshot_v1(*wb_tag(f), &cut)
			   && memcmp(&cut, &f->write_cut, sizeof(cut)) == 0;
	} else {
		ClusterPcmPiStorageCutV1 cut;
		return cluster_pcm_lock_pi_storage_snapshot_v1(*wb_tag(f), &cut)
			   && memcmp(&cut, &f->storage_cut, sizeof(cut)) == 0;
	}
}

bool
cluster_pi_writeback_ack_current_v1(const ClusterPiDataFactV1 *fact,
									const ClusterWalWriterToken *peer)
{
	return fact != NULL && peer != NULL && peer->startup_first_lsn == 0
		   && wb_fact_current(fact, &peer->ref, peer->epoch, true);
}

static void
wb_cancel(ClusterPiWritebackJobV1 *job)
{
	if (job == NULL || job->pid != getpid())
		return;
	if (wb_shared != NULL) {
		SpinLockAcquire(&wb_shared->lock);
		if (wb_shared->outbound_pid == MyProcPid && wb_shared->outbound_revision == job->revision)
			wb_shared->outbound_pid = 0;
		SpinLockRelease(&wb_shared->lock);
	}
	job->stale = true;
	if (wb_active == job)
		wb_active = NULL;
}

static void
wb_forget_server(void)
{
	if (wb_shared != NULL) {
		SpinLockAcquire(&wb_shared->lock);
		if (wb_shared->inbound_pid == MyProcPid) {
			wb_shared->inbound_state = WB_EMPTY;
			wb_shared->inbound_pid = 0;
			wb_shared->reply_pending = false;
		}
		SpinLockRelease(&wb_shared->lock);
	}
	wb_notice = NULL;
}

static void
wb_resource_release(ResourceReleasePhase phase, bool commit pg_attribute_unused(),
					bool top pg_attribute_unused(), void *arg pg_attribute_unused())
{
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;
	if (wb_active != NULL && wb_active->owner == CurrentResourceOwner)
		wb_cancel(wb_active);
	/* Native ROOT/WALR callbacks own their handles during ERROR. Do not try
	 * releasing an input scope after its native pin has already unwound. */
	if (wb_notice != NULL && wb_notice->owner == CurrentResourceOwner)
		wb_forget_server();
	if (wb_batch != NULL && wb_batch->owner == CurrentResourceOwner)
		wb_forget_batch();
}

static void
wb_exit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	wb_cancel(wb_active);
	wb_forget_server();
	wb_forget_batch();
	if (wb_shared != NULL) {
		SpinLockAcquire(&wb_shared->lock);
		if (wb_shared->bgwriter_pid == MyProcPid) {
			wb_shared->bgwriter_pid = 0;
			wb_shared->bgwriter_procno = -1;
		}
		SpinLockRelease(&wb_shared->lock);
	}
}

static void
wb_cleanup_register(void)
{
	if (!wb_callbacks) {
		RegisterResourceReleaseCallback(wb_resource_release, NULL);
		before_shmem_exit(wb_exit, (Datum)0);
		wb_callbacks = true;
	}
}

ClusterControlRootResult
cluster_pi_writeback_begin_v1(const ClusterPageDataReceiptV1 *const *receipts, uint32 count,
							  const ClusterWalSourceRef *peer, ClusterPiWritebackJobV1 **out)
{
	ClusterPiWritebackMessageV1 m = { 0 };
	ClusterPiWritebackJobV1 *job;
	bool acquired = false;
	if (receipts == NULL || peer == NULL || out == NULL || *out != NULL || count == 0
		|| count > CLUSTER_PI_WRITEBACK_MAX || !wb_background() || wb_shared == NULL
		|| MyProcPid <= 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (wb_active != NULL)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	m.verb = CLUSTER_PI_WRITEBACK_NOTIFY;
	m.count = count;
	m.peer = *peer;
	m.epoch = cluster_epoch_get_current();
	for (uint32 i = 0; i < count; i++)
		if (!cluster_page_data_pi_fact_v1(receipts[i], &m.facts[i]))
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!pg_strong_random(&m.nonce, sizeof(m.nonce)) || !wb_current(&m, true))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	job = palloc0(sizeof(*job));
	job->owner = CurrentResourceOwner;
	job->pid = getpid();
	job->requested = m;
	wb_cleanup_register();
	SpinLockAcquire(&wb_shared->lock);
	if (wb_shared->outbound_pid == 0 && wb_shared->outbound_revision != UINT64_MAX) {
		job->revision = ++wb_shared->outbound_revision;
		wb_shared->outbound_pid = MyProcPid;
		wb_shared->outbound_complete = false;
		wb_shared->outbound = m;
		acquired = true;
	}
	SpinLockRelease(&wb_shared->lock);
	if (!acquired) {
		pfree(job);
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	wb_active = job;
	*out = job;
	cluster_lmon_wakeup();
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_pi_writeback_poll_v1(ClusterPiWritebackJobV1 *job)
{
	ClusterPiWritebackMessageV1 reply;
	bool owned, completed;
	if (job == NULL || job->pid != getpid() || job->owner != CurrentResourceOwner
		|| !wb_background() || wb_shared == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (job->stale || !wb_current(&job->requested, true)) {
		wb_cancel(job);
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (job->complete)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	SpinLockAcquire(&wb_shared->lock);
	owned = wb_shared->outbound_pid == MyProcPid && wb_shared->outbound_revision == job->revision;
	completed = owned && wb_shared->outbound_complete;
	if (completed)
		reply = wb_shared->completed;
	SpinLockRelease(&wb_shared->lock);
	if (!owned) {
		wb_cancel(job);
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (!completed)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	if (!wb_reply_matches(&job->requested, &reply) || !wb_current(&job->requested, true)) {
		wb_cancel(job);
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	job->accepted = reply;
	wb_cancel(job);
	job->complete = true;
	job->stale = false;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_pi_writeback_ack_read_v1(const ClusterPiWritebackJobV1 *job, uint32 index,
								 const ClusterPageDataReceiptV1 *receipt,
								 ClusterWalWriterToken *out)
{
	ClusterPiDataFactV1 fact;
	ClusterWalWriterToken peer = { 0 };
	bool found = false;
	if (job == NULL || out == NULL || job->pid != getpid() || job->owner != CurrentResourceOwner
		|| !wb_background() || !job->complete || job->stale || index >= job->requested.count
		|| !wb_reply_matches(&job->requested, &job->accepted)
		|| !cluster_page_data_pi_fact_v1(receipt, &fact)
		|| memcmp(&fact, &job->requested.facts[index], sizeof(fact)) != 0
		|| !wb_master_fact_cut(&fact))
		return false;
	for (uint32 i = 0; i < job->accepted.count; i++)
		if (memcmp(&fact, &job->accepted.facts[i], sizeof(fact)) == 0) {
			found = true;
			break;
		}
	if (!found)
		return false;
	peer.ref = job->accepted.peer;
	peer.epoch = job->accepted.epoch;
	if (!cluster_pi_writeback_ack_current_v1(&fact, &peer))
		return false;
	*out = peer;
	return true;
}

void
cluster_pi_writeback_release_v1(ClusterPiWritebackJobV1 **job)
{
	if (job == NULL || *job == NULL || (*job)->pid != getpid())
		return;
	wb_cancel(*job);
	explicit_bzero(*job, sizeof(**job));
	pfree(*job);
	*job = NULL;
}

bool
cluster_pi_writeback_notice_read_v1(const ClusterPiWritebackNoticeV1 *notice, uint32 index,
									ClusterPiDataFactV1 *out)
{
	if (notice == NULL || notice != wb_notice || out == NULL || notice->pid != getpid()
		|| notice->owner != CurrentResourceOwner || MyBackendType != B_BG_WRITER || !wb_background()
		|| index >= notice->request.count || !wb_current(&notice->request, false))
		return false;
	*out = notice->request.facts[index];
	return true;
}

void
cluster_pi_writeback_ingress_v1(const ClusterICEnvelope *env, const void *payload)
{
	ClusterPiWritebackMessageV1 m;
	int32 procno = -1, pid = 0;
	if (MyBackendType != B_LMON || wb_shared == NULL || env == NULL
		|| env->msg_type != PGRAC_IC_MSG_PI_WRITEBACK
		|| env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_pi_writeback_decode_v1(payload, env->payload_length, &m)
		|| env->epoch != m.epoch)
		return;
	if (m.verb == CLUSTER_PI_WRITEBACK_NOTIFY) {
		if (env->source_node_id != (uint32)wb_master(&m.facts[0]) || !wb_current(&m, false))
			return;
		SpinLockAcquire(&wb_shared->lock);
		if ((wb_shared->inbound_state == WB_EMPTY || wb_shared->inbound_state == WB_REPLIED)
			&& wb_shared->inbound_revision != UINT64_MAX) {
			if (wb_shared->inbound_state != WB_REPLIED
				|| !wb_same_request(&m, &wb_shared->inbound)) {
				wb_shared->inbound = m;
				wb_shared->inbound_revision++;
				wb_shared->inbound_state = WB_QUEUED;
				wb_shared->reply_pending = false;
			} else
				wb_shared->reply_pending = true;
			procno = wb_shared->bgwriter_procno;
			pid = wb_shared->bgwriter_pid;
		}
		SpinLockRelease(&wb_shared->lock);
		if (ProcGlobal != NULL && procno >= 0 && (uint32)procno < ProcGlobal->allProcCount
			&& pid > 0 && ProcGlobal->allProcs[procno].pid == pid)
			SetLatch(&ProcGlobal->allProcs[procno].procLatch);
		return;
	}
	if (env->source_node_id != (uint32)m.peer.claim.identity.origin_node_id
		|| !wb_current(&m, true))
		return;
	SpinLockAcquire(&wb_shared->lock);
	if (wb_shared->outbound_pid != 0 && !wb_shared->outbound_complete
		&& wb_reply_matches(&wb_shared->outbound, &m)) {
		wb_shared->completed = m;
		wb_shared->outbound_complete = true;
	}
	SpinLockRelease(&wb_shared->lock);
}

static bool
wb_plan_sources(const ClusterThreadRecoveryFabricPlanV1 *fabric, ClusterWalSourceRef *sources,
				uint32 *count)
{
	const RfPageOnlinePlanV1 *page = cluster_thread_recovery_fabric_page_plan_v1(fabric);
	*count = cluster_thread_recovery_fabric_participant_count_v1(fabric);
	if (page == NULL || *count == 0 || *count > CLUSTER_WAL_INPUTS_MAX)
		return false;
	for (uint32 i = 0; i < *count; i++)
		if (!rf_page_online_plan_source_v1(page, i, &sources[i]))
			return false;
	return true;
}

static void
wb_server_finish(const ClusterPiWritebackMessageV1 *reply)
{
	uint64 revision = wb_notice->revision;
	cluster_thread_recovery_fabric_plan_destroy_v1(&wb_notice->plan);
	cluster_wal_inputs_release_v1(&wb_notice->inputs);
	pfree(wb_notice);
	wb_notice = NULL;
	SpinLockAcquire(&wb_shared->lock);
	if (wb_shared->inbound_state == WB_RUNNING && wb_shared->inbound_pid == MyProcPid
		&& wb_shared->inbound_revision == revision) {
		if (reply != NULL)
			wb_shared->reply = *reply;
		wb_shared->inbound_state = reply != NULL ? WB_REPLIED : WB_EMPTY;
		wb_shared->inbound_pid = 0;
		wb_shared->reply_pending = reply != NULL;
	}
	SpinLockRelease(&wb_shared->lock);
	cluster_lmon_wakeup();
}

bool
cluster_pi_writeback_bgwriter_tick_v1(void)
{
	ClusterPiWritebackMessageV1 request, reply;
	ClusterWalSourceRef sources[CLUSTER_WAL_INPUTS_MAX];
	const RfPageOnlinePlanV1 *page;
	uint64 revision, records;
	uint32 count;
	bool queued;
	ClusterControlRootResult result;
	RfPageProofDetailV1 detail;
	if (MyBackendType != B_BG_WRITER || wb_shared == NULL || !wb_background() || !cluster_enabled
		|| !cluster_shared_config)
		return false;
	wb_cleanup_register();
	SpinLockAcquire(&wb_shared->lock);
	if (MyProc != NULL) {
		wb_shared->bgwriter_procno = MyProc->pgprocno;
		wb_shared->bgwriter_pid = MyProcPid;
	}
	queued = wb_shared->inbound_state == WB_QUEUED;
	request = wb_shared->inbound;
	revision = wb_shared->inbound_revision;
	SpinLockRelease(&wb_shared->lock);
	if (queued && wb_notice == NULL) {
		ClusterPiWritebackNoticeV1 *notice = palloc0(sizeof(*notice));
		bool claimed = false;
		/* Allocation may ERROR and resume this same bgwriter. Establish its
		 * ResourceOwner before exposing RUNNING; recheck cancellation or a
		 * replacement queue item after the allocating interval. */
		notice->pid = getpid();
		notice->owner = CurrentResourceOwner;
		notice->revision = revision;
		notice->request = request;
		SpinLockAcquire(&wb_shared->lock);
		if (wb_shared->inbound_state == WB_QUEUED && wb_shared->inbound_pid == 0
			&& wb_shared->inbound_revision == revision) {
			wb_notice = notice;
			wb_shared->inbound_state = WB_RUNNING;
			wb_shared->inbound_pid = MyProcPid;
			claimed = true;
		}
		SpinLockRelease(&wb_shared->lock);
		if (!claimed)
			pfree(notice);
	}
	if (wb_notice == NULL)
		return false;
	request = wb_notice->request;
	if (wb_notice->pid != getpid() || wb_notice->owner != CurrentResourceOwner
		|| !wb_current(&request, false)) {
		wb_server_finish(NULL);
		return true;
	}
	if (wb_notice->inputs == NULL) {
		result = cluster_wal_inputs_begin_v1(request.peer.claim.identity.storage_uuid,
											 request.peer.claim.identity.system_identifier,
											 &wb_notice->inputs);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			return true;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto invalid;
	}
	result = cluster_wal_inputs_resume_v1(wb_notice->inputs);
	if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		goto wait;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto invalid;
	if (wb_notice->plan == NULL) {
		result = cluster_wal_inputs_contributions_v1(wb_notice->inputs, true, &wb_notice->plan,
													 &records, &detail);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			goto wait;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto invalid;
	}
	if (!wb_current(&request, false) || !wb_plan_sources(wb_notice->plan, sources, &count)
		|| cluster_wal_inputs_revalidate_v1(wb_notice->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto invalid;
	page = cluster_thread_recovery_fabric_page_plan_v1(wb_notice->plan);
	reply = request;
	reply.verb = CLUSTER_PI_WRITEBACK_ACK;
	reply.count = 0;
	for (uint32 i = 0; i < request.count; i++) {
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		ClusterWalWriterToken native;
		bool valid
			= cluster_page_data_from_notice_v1(wb_notice, i, &receipt)
			  && cluster_page_data_bind_plan_v1(receipt, wb_notice->plan)
			  && cluster_bufmgr_ack_pi_at_data_v1(receipt, page, sources, count, wb_notice->inputs,
												  &ack)
			  && cluster_page_data_pi_ack_export_v1(ack, receipt, &native)
			  && native.epoch == request.epoch && native.startup_first_lsn == 0
			  && wb_source_covered(&native.ref, &request.peer)
			  && (reply.count == 0 || memcmp(&native.ref, &reply.peer, sizeof(reply.peer)) == 0);
		if (valid) {
			reply.peer = native.ref;
			reply.facts[reply.count++] = request.facts[i];
		} else
			wb_rejected(CLUSTER_PI_WRITEBACK_PEER_PHYSICAL, wb_tag(&request.facts[i]),
						wb_master(&request.facts[i]), request.epoch,
						request.peer.claim.identity.origin_owner_incarnation);
		cluster_page_data_pi_ack_free_v1(&ack);
		cluster_page_data_receipt_free_v1(&receipt);
	}
	if (!wb_current(&request, false) || !wb_current(&reply, false)
		|| cluster_wal_inputs_revalidate_v1(wb_notice->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto invalid;
	wb_server_finish(&reply);
	return true;
wait:
	if (cluster_wal_inputs_suspend_v1(wb_notice->inputs) == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return true;
invalid:
	wb_server_finish(NULL);
	return true;
}

void
cluster_pi_writeback_checkpointer_release_v1(void)
{
	WritebackBatch *batch = wb_batch;
	if (batch == NULL || batch->pid != getpid() || batch->owner != CurrentResourceOwner)
		return;
	cluster_pi_writeback_release_v1(&batch->physical_job);
	cluster_pi_data_release_v1(&batch->data_job);
	for (uint32 i = 0; i < batch->count; i++) {
		for (uint32 j = 0; j < batch->ack_count[i]; j++)
			cluster_page_data_pi_ack_free_v1(&batch->acks[i][j]);
		cluster_page_data_receipt_free_v1(&batch->receipts[i]);
	}
	cluster_thread_recovery_fabric_plan_destroy_v1(&batch->plan);
	cluster_wal_inputs_release_v1(&batch->inputs);
	wb_forget_batch();
	pfree(batch);
}

static bool
wb_batch_current(void)
{
	ClusterWalSourceRef local;
	return wb_batch != NULL && wb_batch->pid == getpid() && wb_batch->owner == CurrentResourceOwner
		   && !RecoveryInProgress() && !cluster_normal_stop_requested()
		   && cluster_qvotec_in_quorum() && !cluster_reconfig_has_pending_prebump_stage()
		   && cluster_epoch_get_current() == wb_batch->epoch
		   && cluster_node_id == wb_batch->local.claim.identity.origin_node_id
		   && cluster_qvotec_get_self_incarnation()
				  == wb_batch->local.claim.identity.origin_owner_incarnation
		   && cluster_wal_thread_current_v2_ref(&local)
		   && memcmp(&local, &wb_batch->local, sizeof(local)) == 0;
}

static bool
wb_batch_start(void)
{
	BufferTag tags[CLUSTER_PI_WRITEBACK_MAX];
	ClusterWalSourceRef local;
	uint32 count;
	if (RecoveryInProgress() || cluster_normal_stop_requested() || !cluster_qvotec_in_quorum()
		|| cluster_reconfig_has_pending_prebump_stage()
		|| !cluster_wal_thread_current_v2_ref(&local)
		|| !cluster_wal_claim_v2_ref_valid(&local.claim) || local.timeline == 0
		|| local.claim.identity.origin_node_id != cluster_node_id
		|| local.claim.identity.origin_owner_incarnation != cluster_qvotec_get_self_incarnation())
		return false;
	count = cluster_pcm_lock_pi_candidates_v1(&wb_scan_cursor, 128, tags, CLUSTER_PI_WRITEBACK_MAX);
	if (count == 0 || count > CLUSTER_PI_WRITEBACK_MAX)
		return false;
	wb_batch = palloc0(sizeof(*wb_batch));
	wb_batch->owner = CurrentResourceOwner;
	wb_batch->pid = getpid();
	wb_batch->local = local;
	wb_batch->epoch = cluster_epoch_get_current();
	for (uint32 i = 0; i < count; i++) {
		uint32 next = wb_batch->count;
		if (!cluster_pcm_lock_pi_write_snapshot_v1(tags[i], &wb_batch->write_cuts[next])
			&& !cluster_pcm_lock_pi_storage_snapshot_v1(tags[i], &wb_batch->storage_cuts[next]))
			continue;
		wb_batch->tags[next] = tags[i];
		wb_batch->count++;
	}
	if (wb_batch->count == 0) {
		pfree(wb_batch);
		wb_batch = NULL;
		return false;
	}
	SpinLockAcquire(&wb_shared->lock);
	wb_shared->checkpointer_pid = MyProcPid;
	SpinLockRelease(&wb_shared->lock);
	return true;
}

/* Obtain current-holder DATA before fixing live contribution endpoints. */
static bool
wb_batch_data(void)
{
	while (wb_batch->data_index < wb_batch->count) {
		uint32 i = wb_batch->data_index;
		ClusterPcmPiWriteCutV1 *cut = &wb_batch->write_cuts[i];
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		if (cut->binding_generation != 0) {
			ClusterSpaceIdentityKey key = { 0 };
			key.system_identifier = wb_batch->local.claim.identity.system_identifier;
			key.database_incarnation = wb_batch->local.claim.database_incarnation;
			memcpy(key.storage_uuid, wb_batch->local.claim.identity.storage_uuid, 16);
			key.locator = BufTagGetRelFileLocator(&wb_batch->tags[i]);
			if (cut->holder.assertion.requester_node == cluster_node_id) {
				(void)cluster_bufmgr_write_tag_data_at_cut_v1(&key, cut, &wb_batch->receipts[i]);
			} else {
				if (wb_batch->data_job == NULL)
					result = cluster_pi_data_begin_v1(&key, cut, &wb_batch->data_job);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
					result = cluster_pi_data_poll_v1(wb_batch->data_job, &wb_batch->receipts[i]);
				if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
					return false;
				cluster_pi_data_release_v1(&wb_batch->data_job);
			}
		}
		wb_batch->data_index++;
		AbsorbSyncRequests();
	}
	return true;
}

static bool
wb_batch_storage(uint32 index, const RfPageOnlinePlanV1 *page)
{
	uint32 count = rf_page_online_plan_target_count_v1(page);
	if (wb_batch->tags[index].forkNum == SPACE_FORKNUM) {
		ClusterSpaceIdentityKey key = { 0 };
		key.system_identifier = wb_batch->local.claim.identity.system_identifier;
		key.database_incarnation = wb_batch->local.claim.database_incarnation;
		memcpy(key.storage_uuid, wb_batch->local.claim.identity.storage_uuid, 16);
		key.locator = BufTagGetRelFileLocator(&wb_batch->tags[index]);
		return cluster_bufmgr_observe_pi_space_storage_v1(
			&key, &wb_batch->storage_cuts[index], wb_batch->plan, wb_batch->sources,
			wb_batch->source_count, &wb_batch->receipts[index]);
	}
	for (uint32 i = 0; i < count; i++) {
		RfPageOnlineTargetViewV1 view;
		BufferTag tag;
		ClusterPageDataTargetV1 target = { 0 };
		if (!rf_page_online_plan_target_v1(page, i, &view))
			return false;
		InitBufferTag(&tag, &view.page_identity.locator, view.page_identity.forknum,
					  view.page_identity.blockno);
		if (!BufferTagsEqual(&tag, &wb_batch->tags[index]))
			continue;
		target.database_incarnation = wb_batch->local.claim.database_incarnation;
		target.identity = view.page_identity;
		target.version = view.expected_result;
		return cluster_bufmgr_observe_pi_storage_v1(&target, &wb_batch->storage_cuts[index], page,
													wb_batch->sources, wb_batch->source_count,
													&wb_batch->receipts[index]);
	}
	return false;
}

static bool
wb_batch_peer(int32 node, ClusterWalSourceRef *out)
{
	uint32 count = cluster_wal_inputs_count_v1(wb_batch->inputs);
	bool found = false;
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *input = cluster_wal_inputs_at_v1(wb_batch->inputs, i);
		if (input == NULL)
			return false;
		if (!input->current || input->kind != CLUSTER_WAL_INPUT_CHECKPOINT
			|| input->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			|| input->source.claim.identity.origin_node_id != node)
			continue;
		if (found
			|| input->source.claim.identity.origin_owner_incarnation
				   != cluster_membership_get_last_admitted_incarnation(node))
			return false;
		*out = input->source;
		found = true;
	}
	return found;
}

bool
cluster_pi_writeback_checkpointer_tick_v1(void)
{
	const RfPageOnlinePlanV1 *page;
	ClusterControlRootResult result;
	if (MyBackendType != B_CHECKPOINTER || wb_shared == NULL || !wb_background() || !cluster_enabled
		|| !cluster_shared_config)
		return false;
	wb_cleanup_register();
	if (wb_batch == NULL && !wb_batch_start())
		return false;
	if (!wb_batch_current())
		goto done;
	if (!wb_batch_data())
		return true;
	/* Poll a peer's in-memory reply before resuming any WAL input. A silent
	 * or busy peer must leave the GC writer a real lock-free interval. */
	if (wb_batch->physical_job != NULL) {
		result = cluster_pi_writeback_poll_v1(wb_batch->physical_job);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			goto wait;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
	}
	if (wb_batch->inputs == NULL) {
		result = cluster_wal_inputs_begin_v1(wb_batch->local.claim.identity.storage_uuid,
											 wb_batch->local.claim.identity.system_identifier,
											 &wb_batch->inputs);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			return true;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
	}
	result = cluster_wal_inputs_resume_v1(wb_batch->inputs);
	if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		goto wait;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (wb_batch->plan == NULL) {
		uint64 records;
		RfPageProofDetailV1 detail;
		result = cluster_wal_inputs_contributions_v1(wb_batch->inputs, true, &wb_batch->plan,
													 &records, &detail);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			goto wait;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| !wb_plan_sources(wb_batch->plan, wb_batch->sources, &wb_batch->source_count))
			goto done;
		page = cluster_thread_recovery_fabric_page_plan_v1(wb_batch->plan);
		for (uint32 i = 0; i < wb_batch->count; i++) {
			ClusterPcmPiWriteCutV1 x;
			ClusterPcmPiStorageCutV1 s;
			if (wb_batch->storage_cuts[i].binding_generation != 0)
				(void)wb_batch_storage(i, page);
			AbsorbSyncRequests();
			wb_batch->qualified[i]
				= wb_batch->receipts[i] != NULL
				  && cluster_page_data_bind_plan_v1(wb_batch->receipts[i], wb_batch->plan)
				  && (cluster_page_data_pi_proof_v1(wb_batch->receipts[i], page, wb_batch->sources,
													wb_batch->source_count, &x)
					  || cluster_page_data_pi_storage_proof_v1(wb_batch->receipts[i], page,
															   wb_batch->sources,
															   wb_batch->source_count, &s));
			if (!wb_batch->qualified[i])
				wb_rejected(CLUSTER_PI_WRITEBACK_DATA_PROOF, &wb_batch->tags[i], cluster_node_id,
							wb_batch->epoch,
							wb_batch->local.claim.identity.origin_owner_incarnation);
		}
	}
	page = cluster_thread_recovery_fabric_page_plan_v1(wb_batch->plan);
	if (!wb_batch_current()
		|| cluster_wal_inputs_revalidate_v1(wb_batch->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	for (; wb_batch->peer_index < RESOURCE_X_PROTOCOL_NODE_LIMIT; wb_batch->peer_index++) {
		int32 node = wb_batch->peer_index;
		if (wb_batch->physical_job == NULL) {
			const ClusterPageDataReceiptV1 *receipts[CLUSTER_PI_WRITEBACK_MAX];
			ClusterWalSourceRef peer;
			wb_batch->group_count = 0;
			for (uint32 i = 0; i < wb_batch->count; i++) {
				uint32 holders = wb_batch->write_cuts[i].binding_generation != 0
									 ? wb_batch->write_cuts[i].pi_holders_bitmap
									 : wb_batch->storage_cuts[i].pi_holders_bitmap;
				if (!wb_batch->qualified[i] || (holders & ((uint32)1u << node)) == 0)
					continue;
				wb_batch->group_indices[wb_batch->group_count] = i;
				receipts[wb_batch->group_count++] = wb_batch->receipts[i];
			}
			if (wb_batch->group_count == 0)
				continue;
			if (node == cluster_node_id) {
				for (uint32 j = 0; j < wb_batch->group_count; j++) {
					uint32 i = wb_batch->group_indices[j], n = wb_batch->ack_count[i];
					if (n >= RESOURCE_X_PROTOCOL_NODE_LIMIT
						|| !cluster_bufmgr_ack_pi_at_data_v1(
							wb_batch->receipts[i], page, wb_batch->sources, wb_batch->source_count,
							wb_batch->inputs, &wb_batch->acks[i][n])) {
						wb_batch->qualified[i] = false;
						wb_rejected(CLUSTER_PI_WRITEBACK_LOCAL_ACK, &wb_batch->tags[i], node,
									wb_batch->epoch,
									wb_batch->local.claim.identity.origin_owner_incarnation);
						continue;
					}
					wb_batch->ack_count[i]++;
				}
				continue;
			}
			if (!wb_batch_peer(node, &peer)) {
				for (uint32 j = 0; j < wb_batch->group_count; j++) {
					uint32 i = wb_batch->group_indices[j], n = wb_batch->ack_count[i];
					if (n >= RESOURCE_X_PROTOCOL_NODE_LIMIT
						|| !cluster_bufmgr_ack_recovered_pi_at_data_v1(
							wb_batch->receipts[i], page, wb_batch->sources, wb_batch->source_count,
							wb_batch->inputs, node, &wb_batch->acks[i][n])) {
						wb_batch->qualified[i] = false;
						wb_rejected(CLUSTER_PI_WRITEBACK_RECOVERY_PROOF, &wb_batch->tags[i], node,
									wb_batch->epoch,
									wb_batch->local.claim.identity.origin_owner_incarnation);
						continue;
					}
					wb_batch->ack_count[i]++;
				}
				continue;
			}
			result = cluster_pi_writeback_begin_v1(receipts, wb_batch->group_count, &peer,
												   &wb_batch->physical_job);
			if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
				goto wait;
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				goto done;
		}
		result = cluster_pi_writeback_poll_v1(wb_batch->physical_job);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			goto wait;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		for (uint32 j = 0; j < wb_batch->group_count; j++) {
			uint32 i = wb_batch->group_indices[j], n = wb_batch->ack_count[i];
			if (n >= RESOURCE_X_PROTOCOL_NODE_LIMIT
				|| !cluster_page_data_pi_ack_import_v1(
					wb_batch->physical_job, j, wb_batch->receipts[i], &wb_batch->acks[i][n])) {
				wb_batch->qualified[i] = false;
				wb_rejected(CLUSTER_PI_WRITEBACK_REMOTE_ACK, &wb_batch->tags[i], node,
							wb_batch->epoch,
							wb_batch->local.claim.identity.origin_owner_incarnation);
				continue;
			}
			wb_batch->ack_count[i]++;
		}
		cluster_pi_writeback_release_v1(&wb_batch->physical_job);
	}
	if (!wb_batch_current()
		|| cluster_wal_inputs_revalidate_v1(wb_batch->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	for (uint32 i = 0; i < wb_batch->count; i++) {
		uint32 holders;
		bool completed;
		if (!wb_batch->qualified[i])
			continue;
		if (wb_batch->write_cuts[i].binding_generation != 0)
			completed = cluster_pcm_lock_pi_write_complete_v1(
				wb_batch->receipts[i], page, wb_batch->sources, wb_batch->source_count,
				(const ClusterPiPhysicalAckV1 *const *)wb_batch->acks[i], wb_batch->ack_count[i],
				&holders);
		else
			completed = cluster_pcm_lock_pi_storage_complete_v1(
				wb_batch->receipts[i], page, wb_batch->sources, wb_batch->source_count,
				(const ClusterPiPhysicalAckV1 *const *)wb_batch->acks[i], wb_batch->ack_count[i],
				&holders);
		if (!completed)
			wb_rejected(CLUSTER_PI_WRITEBACK_MASTER_CUT, &wb_batch->tags[i], cluster_node_id,
						wb_batch->epoch, wb_batch->local.claim.identity.origin_owner_incarnation);
	}
	goto done;
wait:
	if (wb_batch->inputs == NULL
		|| cluster_wal_inputs_suspend_v1(wb_batch->inputs) == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return true;
done:
	cluster_pi_writeback_checkpointer_release_v1();
	return true;
}

void
cluster_pi_writeback_lmon_tick_v1(void)
{
	ClusterPiWritebackMessageV1 request, reply, inbound;
	uint64 revision, inbound_revision;
	bool pending, replied;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES];
	Size length;
	TimestampTz now;
	if (MyBackendType != B_LMON || wb_shared == NULL)
		return;
	if (cluster_normal_stop_requested()) {
		SpinLockAcquire(&wb_shared->lock);
		wb_shared->outbound_pid = 0;
		if (wb_shared->inbound_state != WB_RUNNING) {
			wb_shared->inbound_state = WB_EMPTY;
			wb_shared->reply_pending = false;
		}
		SpinLockRelease(&wb_shared->lock);
		return;
	}
	SpinLockAcquire(&wb_shared->lock);
	pending = wb_shared->outbound_pid != 0 && !wb_shared->outbound_complete;
	request = wb_shared->outbound;
	revision = wb_shared->outbound_revision;
	inbound_revision = wb_shared->inbound_revision;
	replied = wb_shared->inbound_state == WB_REPLIED && wb_shared->reply_pending;
	reply = wb_shared->reply;
	inbound = wb_shared->inbound;
	SpinLockRelease(&wb_shared->lock);
	now = GetCurrentTimestamp();
	if (wb_sent_revision == revision && wb_sent_inbound == inbound_revision
		&& wb_sent_reply == replied && now >= wb_last_send && now - wb_last_send < WB_RETRY_US)
		return;
	if (pending && wb_current(&request, true)
		&& cluster_pi_writeback_encode_v1(&request, bytes, sizeof(bytes), &length))
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_PI_WRITEBACK,
									   request.peer.claim.identity.origin_node_id, bytes, length);
	if (replied && wb_current(&inbound, false) && wb_reply_matches(&inbound, &reply)
		&& wb_current(&reply, false)
		&& cluster_pi_writeback_encode_v1(&reply, bytes, sizeof(bytes), &length)) {
		bool send = false;
		ClusterICSendResult sent;
		SpinLockAcquire(&wb_shared->lock);
		if (wb_shared->inbound_revision == inbound_revision
			&& wb_shared->inbound_state == WB_REPLIED && wb_shared->reply_pending) {
			wb_shared->reply_pending = false;
			send = true;
		}
		SpinLockRelease(&wb_shared->lock);
		if (send) {
			sent = cluster_ic_send_envelope(PGRAC_IC_MSG_PI_WRITEBACK, wb_master(&inbound.facts[0]),
											bytes, length);
			if (sent != CLUSTER_IC_SEND_DONE && sent != CLUSTER_IC_SEND_WOULD_BLOCK) {
				SpinLockAcquire(&wb_shared->lock);
				if (wb_shared->inbound_revision == inbound_revision
					&& wb_shared->inbound_state == WB_REPLIED)
					wb_shared->reply_pending = true;
				SpinLockRelease(&wb_shared->lock);
			}
		}
	}
	wb_sent_revision = revision;
	wb_sent_inbound = inbound_revision;
	wb_sent_reply = replied;
	wb_last_send = now;
}

ClusterNormalStopPollResult
cluster_pi_writeback_normal_stop_poll_v1(const char **reason)
{
	bool active;
	if (reason != NULL)
		*reason = NULL;
	if (!cluster_shared_config)
		return CLUSTER_NORMAL_STOP_READY;
	if (wb_shared == NULL) {
		if (reason != NULL)
			*reason = "PI_WRITEBACK_UNINITIALIZED";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	SpinLockAcquire(&wb_shared->lock);
	active = wb_shared->outbound_pid != 0 || wb_shared->inbound_state == WB_QUEUED
			 || wb_shared->inbound_state == WB_RUNNING || wb_shared->checkpointer_pid != 0;
	SpinLockRelease(&wb_shared->lock);
	if (active && reason != NULL)
		*reason = "PI_WRITEBACK_PHYSICAL_COMPLETION_PENDING";
	return active ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
}

void
cluster_pi_writeback_register_v1(void)
{
	static const ClusterICMsgTypeInfo info = { .msg_type = PGRAC_IC_MSG_PI_WRITEBACK,
											   .name = "pi_physical_writeback",
											   .allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
											   .broadcast_ok = false,
											   .handler = cluster_pi_writeback_ingress_v1,
											   .plane = CLUSTER_IC_PLANE_CONTROL };
	cluster_ic_register_msg_type(&info);
}

static Size
wb_size(void)
{
	return MAXALIGN(sizeof(WritebackShared));
}

static void
wb_init(void)
{
	bool found;
	wb_shared = ShmemInitStruct("pgrac PI writeback", wb_size(), &found);
	if (!found) {
		memset(wb_shared, 0, sizeof(*wb_shared));
		wb_shared->bgwriter_procno = -1;
		SpinLockInit(&wb_shared->lock);
	}
}

void
cluster_pi_writeback_shmem_register_v1(void)
{
	static const ClusterShmemRegion region = { .name = "pgrac PI writeback",
											   .size_fn = wb_size,
											   .init_fn = wb_init,
											   .lwlock_count = 0,
											   .owner_subsys = "cluster_pi_writeback",
											   .reserved_flags = 0 };
	cluster_shmem_register_region(&region);
}
