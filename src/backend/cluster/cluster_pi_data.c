/* PGRAC: GCS background current-holder DATA write/completion. LMON only
 * routes; the native bgwriter owns buffer/WAL/DATA I/O. No PI clear or GC.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>

#include "access/xlog.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_pi_data.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_writer.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "cluster_pi_data_wire.h"

#define PI_DATA_RETRY_US INT64CONST(100000)
enum { PI_DATA_EMPTY, PI_DATA_QUEUED, PI_DATA_RUNNING, PI_DATA_REPLIED };

typedef struct PiDataShared {
	slock_t lock;
	uint64 outbound_revision;
	int32 outbound_pid;
	bool outbound_complete;
	ClusterPiDataMessageV1 outbound;
	ClusterPiDataMessageV1 completed;
	uint64 inbound_revision;
	uint32 inbound_state;
	int32 inbound_pid;
	bool inbound_reply_pending;
	ClusterPiDataMessageV1 inbound;
	int32 bgwriter_procno;
	int32 bgwriter_pid;
} PiDataShared;

struct ClusterPiDataV1 {
	ResourceOwner owner;
	pid_t pid;
	uint64 revision;
	bool stale;
	bool complete;
	ClusterPiDataMessageV1 requested;
	ClusterPiDataMessageV1 accepted;
};

static PiDataShared *pi_data_shared;
static ClusterPiDataV1 *pi_data_active;
static bool pi_data_callbacks;
static TimestampTz pi_data_last_send;
static uint64 pi_data_sent_revision;
static uint64 pi_data_sent_inbound_revision;
static bool pi_data_sent_reply;


static bool
pi_data_message_valid(const ClusterPiDataMessageV1 *m)
{
	static const ClusterPageWalBindingV1 empty = { 0 };
	static const uint8 zero[16] = { 0 };
	const BufferTag *t;
	if (m == NULL || m->nonce == 0 || m->epoch == 0 || m->epoch == UINT64_MAX
		|| m->holder_incarnation == 0 || m->holder_incarnation == UINT64_MAX
		|| !cluster_pcm_pi_write_cut_valid_v1(&m->cut) || m->cut.pi_holders_bitmap == 0
		|| m->cut.master_node == m->cut.holder.assertion.requester_node
		|| m->key.system_identifier == 0 || m->key.database_incarnation == 0
		|| memcmp(m->key.storage_uuid, zero, 16) == 0)
		return false;
	t = &m->cut.holder.assertion.resource;
	if (t->spcOid == InvalidOid || t->relNumber == 0 || t->blockNum == InvalidBlockNumber
		|| (t->dbOid == InvalidOid && t->spcOid != GLOBALTABLESPACE_OID)
		|| !RelFileLocatorEquals(m->key.locator, BufTagGetRelFileLocator(t)))
		return false;
	if (m->verb == CLUSTER_PI_DATA_WRITE)
		return memcmp(&m->binding, &empty, sizeof(empty)) == 0;
	return m->verb == CLUSTER_PI_DATA_WRITTEN && cluster_page_wal_binding_shape_v1(&m->binding)
		   && m->binding.flags == CLUSTER_PAGE_WAL_NATIVE_FLUSHED
		   && m->binding.source.claim.database_incarnation == m->key.database_incarnation
		   && m->binding.identity.system_identifier == m->key.system_identifier
		   && memcmp(m->binding.identity.storage_uuid, m->key.storage_uuid, 16) == 0
		   && RelFileLocatorEquals(m->binding.identity.locator, m->key.locator)
		   && m->binding.identity.forknum == t->forkNum
		   && m->binding.identity.blockno == t->blockNum;
}


bool
cluster_pi_data_encode_v1(const ClusterPiDataMessageV1 *m, uint8 bytes[CLUSTER_PI_DATA_BYTES])
{
	if (pi_data_overlap(m, sizeof(*m), bytes, CLUSTER_PI_DATA_BYTES))
		return false;
	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_PI_DATA_BYTES);
	if (bytes == NULL || !pi_data_message_valid(m))
		return false;
	memcpy(bytes, "PPDT", 4);
	pi_data_put(bytes + 4, 1, 2);
	pi_data_put(bytes + 6, CLUSTER_PI_DATA_BYTES, 2);
	pi_data_put(bytes + 8, m->verb, 4);
	pi_data_put(bytes + 16, m->nonce, 8);
	pi_data_put(bytes + 24, m->epoch, 8);
	pi_data_put(bytes + 32, m->holder_incarnation, 8);
	pi_data_put(bytes + 40, m->key.system_identifier, 8);
	pi_data_put(bytes + 48, m->key.database_incarnation, 8);
	memcpy(bytes + 56, m->key.storage_uuid, 16);
	pi_data_cut_encode(bytes + 72, &m->cut);
	pi_data_binding_encode(bytes + 200, &m->binding);
	return true;
}

bool
cluster_pi_data_decode_v1(const void *data, Size length, ClusterPiDataMessageV1 *out)
{
	const uint8 *bytes = data;
	ClusterPiDataMessageV1 m = { 0 };
	if (pi_data_overlap(data, length, out, sizeof(*out)))
		return false;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (bytes == NULL || out == NULL || length != CLUSTER_PI_DATA_BYTES
		|| memcmp(bytes, "PPDT", 4) != 0 || pi_data_get(bytes + 4, 2) != 1
		|| pi_data_get(bytes + 6, 2) != CLUSTER_PI_DATA_BYTES || pi_data_get(bytes + 12, 4))
		return false;
	m.verb = pi_data_get(bytes + 8, 4);
	m.nonce = pi_data_get(bytes + 16, 8);
	m.epoch = pi_data_get(bytes + 24, 8);
	m.holder_incarnation = pi_data_get(bytes + 32, 8);
	m.key.system_identifier = pi_data_get(bytes + 40, 8);
	m.key.database_incarnation = pi_data_get(bytes + 48, 8);
	memcpy(m.key.storage_uuid, bytes + 56, 16);
	pi_data_cut_decode(bytes + 72, &m.cut);
	m.key.locator = BufTagGetRelFileLocator(&m.cut.holder.assertion.resource);
	if (!pi_data_binding_decode(bytes + 200, &m.binding) || !pi_data_message_valid(&m))
		return false;
	*out = m;
	return true;
}

static bool
pi_data_same_request(const ClusterPiDataMessageV1 *a, const ClusterPiDataMessageV1 *b)
{
	return a->nonce == b->nonce && a->epoch == b->epoch
		   && a->holder_incarnation == b->holder_incarnation
		   && memcmp(&a->key, &b->key, sizeof(a->key)) == 0
		   && memcmp(&a->cut, &b->cut, sizeof(a->cut)) == 0;
}

static bool
pi_data_namespace(const ClusterWalSourceRef *source, const ClusterPiDataMessageV1 *m)
{
	return source->claim.identity.system_identifier == m->key.system_identifier
		   && source->claim.database_incarnation == m->key.database_incarnation
		   && memcmp(source->claim.identity.storage_uuid, m->key.storage_uuid, 16) == 0;
}

static bool
pi_data_current(const ClusterPiDataMessageV1 *m, bool master)
{
	ClusterWalSourceRef source;
	int32 holder = m->cut.holder.assertion.requester_node;
	uint64 master_boot = m->cut.holder.master_session_incarnation;
	uint64 boot = master ? master_boot : m->holder_incarnation;
	return cluster_enabled && cluster_shared_config && pi_data_message_valid(m)
		   && !RecoveryInProgress() && !cluster_normal_stop_requested()
		   && cluster_node_id == (master ? m->cut.master_node : holder)
		   && cluster_qvotec_get_self_incarnation() == boot && cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == m->epoch
		   && !cluster_reconfig_has_pending_prebump_stage()
		   && GetSystemIdentifier() == m->key.system_identifier
		   && cluster_membership_get_state(m->cut.master_node) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(m->cut.master_node) == master_boot
		   && cluster_membership_get_state(holder) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(holder) == m->holder_incarnation
		   && cluster_gcs_lookup_master(m->cut.holder.assertion.resource) == m->cut.master_node
		   && cluster_wal_thread_current_v2_ref(&source) && pi_data_namespace(&source, m)
		   && source.claim.identity.origin_node_id == cluster_node_id
		   && source.claim.identity.origin_owner_incarnation == boot;
}

static bool
pi_data_master_cut(const ClusterPiDataMessageV1 *m)
{
	ClusterPcmPiWriteCutV1 now;
	return pi_data_current(m, true)
		   && cluster_pcm_lock_pi_write_snapshot_v1(m->cut.holder.assertion.resource, &now)
		   && memcmp(&now, &m->cut, sizeof(now)) == 0;
}

static bool
pi_data_background(void)
{
	return (MyBackendType == B_BG_WORKER || MyBackendType == B_BG_WRITER
			|| MyBackendType == B_CHECKPOINTER)
		   && CritSectionCount == 0 && CurrentResourceOwner != NULL;
}

static void
pi_data_cancel(ClusterPiDataV1 *job)
{
	if (job == NULL || job->pid != getpid())
		return;
	if (pi_data_shared != NULL) {
		SpinLockAcquire(&pi_data_shared->lock);
		if (pi_data_shared->outbound_pid == MyProcPid
			&& pi_data_shared->outbound_revision == job->revision)
			pi_data_shared->outbound_pid = 0;
		SpinLockRelease(&pi_data_shared->lock);
	}
	job->stale = true;
	if (pi_data_active == job)
		pi_data_active = NULL;
}

static void
pi_data_resource_release(ResourceReleasePhase phase, bool commit pg_attribute_unused(),
						 bool top pg_attribute_unused(), void *arg pg_attribute_unused())
{
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && pi_data_active != NULL
		&& pi_data_active->owner == CurrentResourceOwner)
		pi_data_cancel(pi_data_active);
}

static void
pi_data_exit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	pi_data_cancel(pi_data_active);
	if (pi_data_shared != NULL) {
		SpinLockAcquire(&pi_data_shared->lock);
		if (pi_data_shared->inbound_pid == MyProcPid) {
			pi_data_shared->inbound_state = PI_DATA_EMPTY;
			pi_data_shared->inbound_pid = 0;
			pi_data_shared->inbound_reply_pending = false;
		}
		if (pi_data_shared->bgwriter_pid == MyProcPid) {
			pi_data_shared->bgwriter_pid = 0;
			pi_data_shared->bgwriter_procno = -1;
		}
		SpinLockRelease(&pi_data_shared->lock);
	}
}

static void
pi_data_register_cleanup(void)
{
	if (!pi_data_callbacks) {
		RegisterResourceReleaseCallback(pi_data_resource_release, NULL);
		before_shmem_exit(pi_data_exit, (Datum)0);
		pi_data_callbacks = true;
	}
}

ClusterControlRootResult
cluster_pi_data_begin_v1(const ClusterSpaceIdentityKey *key, const ClusterPcmPiWriteCutV1 *cut,
						 ClusterPiDataV1 **out)
{
	ClusterPiDataMessageV1 request = { 0 };
	ClusterPiDataV1 *job;
	bool acquired = false;
	if (key == NULL || cut == NULL || out == NULL || *out != NULL || !pi_data_background()
		|| pi_data_shared == NULL || MyProcPid <= 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (pi_data_active != NULL)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	request.verb = CLUSTER_PI_DATA_WRITE;
	/* Copy fields, not unspecified tail padding of the namespace key. */
	request.key.system_identifier = key->system_identifier;
	request.key.database_incarnation = key->database_incarnation;
	memcpy(request.key.storage_uuid, key->storage_uuid, 16);
	request.key.locator = key->locator;
	request.cut = *cut;
	request.epoch = cluster_epoch_get_current();
	if (!cluster_pcm_pi_write_cut_valid_v1(cut))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	request.holder_incarnation
		= cluster_membership_get_last_admitted_incarnation(cut->holder.assertion.requester_node);
	if (!pg_strong_random(&request.nonce, sizeof(request.nonce)) || !pi_data_master_cut(&request))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	job = palloc0(sizeof(*job));
	job->owner = CurrentResourceOwner;
	job->pid = getpid();
	job->requested = request;
	pi_data_register_cleanup();
	SpinLockAcquire(&pi_data_shared->lock);
	if (pi_data_shared->outbound_pid == 0 && pi_data_shared->outbound_revision != UINT64_MAX) {
		job->revision = ++pi_data_shared->outbound_revision;
		pi_data_shared->outbound_pid = MyProcPid;
		pi_data_shared->outbound_complete = false;
		pi_data_shared->outbound = request;
		memset(&pi_data_shared->completed, 0, sizeof(pi_data_shared->completed));
		acquired = true;
	}
	SpinLockRelease(&pi_data_shared->lock);
	if (!acquired) {
		pfree(job);
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	pi_data_active = job;
	*out = job;
	cluster_lmon_wakeup();
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_pi_data_read_v1(const ClusterPiDataV1 *job, ClusterPageWalBindingV1 *binding,
						ClusterPcmPiWriteCutV1 *cut)
{
	if (job == NULL || binding == NULL || cut == NULL || job->pid != getpid()
		|| job->owner != CurrentResourceOwner || !pi_data_background() || job->stale
		|| !job->complete || !pi_data_same_request(&job->requested, &job->accepted)
		|| job->accepted.verb != CLUSTER_PI_DATA_WRITTEN || !pi_data_master_cut(&job->accepted))
		return false;
	*binding = job->accepted.binding;
	*cut = job->accepted.cut;
	return true;
}

ClusterControlRootResult
cluster_pi_data_poll_v1(ClusterPiDataV1 *job, ClusterPageDataReceiptV1 **out)
{
	ClusterPiDataMessageV1 reply = { 0 };
	bool accepted = false, owned = false;
	if (job == NULL || out == NULL || *out != NULL || job->pid != getpid()
		|| job->owner != CurrentResourceOwner || !pi_data_background())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (job->stale || !pi_data_master_cut(&job->requested)) {
		pi_data_cancel(job);
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (!job->complete) {
		SpinLockAcquire(&pi_data_shared->lock);
		owned = pi_data_shared->outbound_pid == MyProcPid
				&& pi_data_shared->outbound_revision == job->revision;
		if (owned && pi_data_shared->outbound_complete) {
			reply = pi_data_shared->completed;
			accepted = true;
		}
		SpinLockRelease(&pi_data_shared->lock);
		if (!owned) {
			pi_data_cancel(job);
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		if (!accepted)
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		if (!pi_data_same_request(&job->requested, &reply) || !pi_data_master_cut(&reply)) {
			pi_data_cancel(job);
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		job->accepted = reply;
		pi_data_cancel(job);
		job->stale = false;
		job->complete = true;
	}
	return cluster_page_data_from_remote_v1(job, out) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
													  : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

void
cluster_pi_data_release_v1(ClusterPiDataV1 **job)
{
	if (job == NULL || *job == NULL || (*job)->pid != getpid())
		return;
	pi_data_cancel(*job);
	explicit_bzero(*job, sizeof(**job));
	pfree(*job);
	*job = NULL;
}

void
cluster_pi_data_ingress_v1(const ClusterICEnvelope *env, const void *payload)
{
	ClusterPiDataMessageV1 m;
	int32 procno = -1, pid = 0;
	if (MyBackendType != B_LMON || pi_data_shared == NULL || env == NULL
		|| env->msg_type != PGRAC_IC_MSG_PI_DATA || env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_pi_data_decode_v1(payload, env->payload_length, &m) || env->epoch != m.epoch)
		return;
	/* CONTROL authenticates source against its HELLO. Only the actual master
	 * can ask, and only that job's original holder can return completion. */
	if (m.verb == CLUSTER_PI_DATA_WRITE) {
		if (env->source_node_id != (uint32)m.cut.master_node || !pi_data_current(&m, false))
			return;
		SpinLockAcquire(&pi_data_shared->lock);
		if ((pi_data_shared->inbound_state == PI_DATA_EMPTY
			 || pi_data_shared->inbound_state == PI_DATA_REPLIED)
			&& pi_data_shared->inbound_revision != UINT64_MAX) {
			/* Retain a completed duplicate without rewriting a later version. */
			if (pi_data_shared->inbound_state != PI_DATA_REPLIED
				|| !pi_data_same_request(&pi_data_shared->inbound, &m)) {
				pi_data_shared->inbound = m;
				pi_data_shared->inbound_revision++;
				pi_data_shared->inbound_state = PI_DATA_QUEUED;
				pi_data_shared->inbound_reply_pending = false;
			} else
				pi_data_shared->inbound_reply_pending = true;
			procno = pi_data_shared->bgwriter_procno;
			pid = pi_data_shared->bgwriter_pid;
		}
		SpinLockRelease(&pi_data_shared->lock);
		if (ProcGlobal != NULL && procno >= 0 && (uint32)procno < ProcGlobal->allProcCount
			&& pid > 0 && ProcGlobal->allProcs[procno].pid == pid)
			SetLatch(&ProcGlobal->allProcs[procno].procLatch);
		return;
	}
	if (env->source_node_id != (uint32)m.cut.holder.assertion.requester_node
		|| !pi_data_current(&m, true))
		return;
	SpinLockAcquire(&pi_data_shared->lock);
	if (pi_data_shared->outbound_pid != 0 && !pi_data_shared->outbound_complete
		&& pi_data_same_request(&pi_data_shared->outbound, &m)) {
		pi_data_shared->completed = m;
		pi_data_shared->outbound_complete = true;
	}
	SpinLockRelease(&pi_data_shared->lock);
}

static bool
pi_data_native_writer(const ClusterPiDataMessageV1 *m, ClusterWalWriterToken *out)
{
	ClusterWalSourceRef source;
	return pi_data_current(m, false) && cluster_wal_thread_current_v2_ref(&source)
		   && cluster_wal_writer_begin(source.timeline, out) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		   && out->epoch == m->epoch && out->startup_first_lsn == 0
		   && memcmp(&out->ref, &source, sizeof(source)) == 0;
}

bool
cluster_pi_data_bgwriter_tick_v1(void)
{
	ClusterPiDataMessageV1 m;
	ClusterWalWriterToken writer, after;
	ClusterPageDataReceiptV1 *volatile receipt = NULL;
	uint64 revision;
	bool queued;
	volatile bool complete = false;
	if (MyBackendType != B_BG_WRITER || pi_data_shared == NULL || !pi_data_background()
		|| !cluster_enabled || !cluster_shared_config)
		return false;
	pi_data_register_cleanup();
	SpinLockAcquire(&pi_data_shared->lock);
	if (MyProc != NULL) {
		pi_data_shared->bgwriter_procno = MyProc->pgprocno;
		pi_data_shared->bgwriter_pid = MyProcPid;
	}
	queued = pi_data_shared->inbound_state == PI_DATA_QUEUED;
	m = pi_data_shared->inbound;
	revision = pi_data_shared->inbound_revision;
	if (queued) {
		pi_data_shared->inbound_state = PI_DATA_RUNNING;
		pi_data_shared->inbound_pid = MyProcPid;
	}
	SpinLockRelease(&pi_data_shared->lock);
	if (!queued)
		return false;
	PG_TRY();
	{
		ClusterPcmPiWriteCutV1 observed;
		if (pi_data_native_writer(&m, &writer)
			&& cluster_bufmgr_write_tag_data_at_cut_v1(&m.key, &m.cut,
													   (ClusterPageDataReceiptV1 **)&receipt)
			&& cluster_page_data_pi_export_v1(receipt, &m.binding, &observed)
			&& memcmp(&observed, &m.cut, sizeof(observed)) == 0) {
			m.verb = CLUSTER_PI_DATA_WRITTEN;
			complete
				= pi_data_native_writer(&m, &after) && memcmp(&writer, &after, sizeof(writer)) == 0;
		}
	}
	PG_FINALLY();
	{
		cluster_page_data_receipt_free_v1((ClusterPageDataReceiptV1 **)&receipt);
		SpinLockAcquire(&pi_data_shared->lock);
		if (pi_data_shared->inbound_state == PI_DATA_RUNNING
			&& pi_data_shared->inbound_pid == MyProcPid
			&& pi_data_shared->inbound_revision == revision) {
			if (complete)
				pi_data_shared->inbound = m;
			pi_data_shared->inbound_state = complete ? PI_DATA_REPLIED : PI_DATA_EMPTY;
			pi_data_shared->inbound_pid = 0;
			pi_data_shared->inbound_reply_pending = complete;
		}
		SpinLockRelease(&pi_data_shared->lock);
		cluster_lmon_wakeup();
	}
	PG_END_TRY();
	return true;
}

void
cluster_pi_data_lmon_tick_v1(void)
{
	ClusterPiDataMessageV1 request, reply;
	uint64 revision, inbound_revision;
	bool pending, replied;
	uint8 bytes[CLUSTER_PI_DATA_BYTES];
	TimestampTz now;
	if (MyBackendType != B_LMON || pi_data_shared == NULL)
		return;
	SpinLockAcquire(&pi_data_shared->lock);
	pending = pi_data_shared->outbound_pid != 0 && !pi_data_shared->outbound_complete;
	request = pi_data_shared->outbound;
	revision = pi_data_shared->outbound_revision;
	inbound_revision = pi_data_shared->inbound_revision;
	replied
		= pi_data_shared->inbound_state == PI_DATA_REPLIED && pi_data_shared->inbound_reply_pending;
	reply = pi_data_shared->inbound;
	SpinLockRelease(&pi_data_shared->lock);
	/* Producer cut cancels queued work, never erases an in-flight I/O owner. */
	if (cluster_normal_stop_requested()) {
		SpinLockAcquire(&pi_data_shared->lock);
		pi_data_shared->outbound_pid = 0;
		if (pi_data_shared->inbound_state != PI_DATA_RUNNING) {
			pi_data_shared->inbound_state = PI_DATA_EMPTY;
			pi_data_shared->inbound_reply_pending = false;
		}
		SpinLockRelease(&pi_data_shared->lock);
		return;
	}
	now = GetCurrentTimestamp();
	if (pi_data_sent_revision == revision && pi_data_sent_inbound_revision == inbound_revision
		&& pi_data_sent_reply == replied && now >= pi_data_last_send
		&& now - pi_data_last_send < PI_DATA_RETRY_US)
		return;
	if (pending && pi_data_current(&request, true) && cluster_pi_data_encode_v1(&request, bytes))
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_PI_DATA,
									   request.cut.holder.assertion.requester_node, bytes,
									   sizeof(bytes));
	if (replied && pi_data_current(&reply, false) && cluster_pi_data_encode_v1(&reply, bytes)) {
		bool send = false;
		ClusterICSendResult result;
		/* Retire this notification before invoking transport. A concurrent
		 * duplicate may request another cached reply, but a successfully
		 * admitted frame never becomes a periodic background obligation. */
		SpinLockAcquire(&pi_data_shared->lock);
		if (pi_data_shared->inbound_revision == inbound_revision
			&& pi_data_shared->inbound_state == PI_DATA_REPLIED
			&& pi_data_shared->inbound_reply_pending) {
			pi_data_shared->inbound_reply_pending = false;
			send = true;
		}
		SpinLockRelease(&pi_data_shared->lock);
		if (send) {
			result = cluster_ic_send_envelope(PGRAC_IC_MSG_PI_DATA, reply.cut.master_node, bytes,
											  sizeof(bytes));
			if (result != CLUSTER_IC_SEND_DONE && result != CLUSTER_IC_SEND_WOULD_BLOCK) {
				SpinLockAcquire(&pi_data_shared->lock);
				if (pi_data_shared->inbound_revision == inbound_revision
					&& pi_data_shared->inbound_state == PI_DATA_REPLIED)
					pi_data_shared->inbound_reply_pending = true;
				SpinLockRelease(&pi_data_shared->lock);
			}
		}
	}
	pi_data_sent_revision = revision;
	pi_data_sent_inbound_revision = inbound_revision;
	pi_data_sent_reply = replied;
	pi_data_last_send = now;
}

ClusterNormalStopPollResult
cluster_pi_data_normal_stop_poll_v1(const char **reason)
{
	bool active;
	if (reason != NULL)
		*reason = NULL;
	if (!cluster_shared_config)
		return CLUSTER_NORMAL_STOP_READY;
	if (pi_data_shared == NULL) {
		if (reason != NULL)
			*reason = "PI_DATA_OWNER_UNINITIALIZED";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	SpinLockAcquire(&pi_data_shared->lock);
	active = pi_data_shared->outbound_pid != 0 || pi_data_shared->inbound_state == PI_DATA_QUEUED
			 || pi_data_shared->inbound_state == PI_DATA_RUNNING;
	SpinLockRelease(&pi_data_shared->lock);
	if (active && reason != NULL)
		*reason = "PI_DATA_BACKGROUND_COMPLETION_PENDING";
	return active ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
}

void
cluster_pi_data_register_v1(void)
{
	static const ClusterICMsgTypeInfo info = { .msg_type = PGRAC_IC_MSG_PI_DATA,
											   .name = "current_holder_data_write",
											   .allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
											   .broadcast_ok = false,
											   .handler = cluster_pi_data_ingress_v1,
											   .plane = CLUSTER_IC_PLANE_CONTROL };
	cluster_ic_register_msg_type(&info);
}
static Size
pi_data_size(void)
{
	return MAXALIGN(sizeof(PiDataShared));
}
static void
pi_data_init(void)
{
	bool found;
	pi_data_shared = ShmemInitStruct("pgrac current holder DATA write", pi_data_size(), &found);
	if (!found) {
		memset(pi_data_shared, 0, sizeof(*pi_data_shared));
		pi_data_shared->bgwriter_procno = -1;
		SpinLockInit(&pi_data_shared->lock);
	}
}
void
cluster_pi_data_shmem_register_v1(void)
{
	static const ClusterShmemRegion region = { .name = "pgrac current holder DATA write",
											   .size_fn = pi_data_size,
											   .init_fn = pi_data_init,
											   .lwlock_count = 0,
											   .owner_subsys = "cluster_pi_data",
											   .reserved_flags = 0 };
	cluster_shmem_register_region(&region);
}
