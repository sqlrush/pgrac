/* PGRAC: asynchronous observations from the original native WAL writer.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_cut.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#define WAL_CUT_RETRY_US INT64CONST(100000)

typedef enum WalCutSourceState {
	WAL_CUT_SOURCE_EMPTY,
	WAL_CUT_SOURCE_QUEUED,
	WAL_CUT_SOURCE_RUNNING,
	WAL_CUT_SOURCE_REPLIED
} WalCutSourceState;

typedef struct WalCutShared {
	slock_t lock;
	int32 owner_pid;
	uint64 revision;
	ClusterControlRootResult result;
	ClusterWalCutMessageV1 request;
	ClusterWalCutMessageV1 reply;
	uint64 source_revision;
	WalCutSourceState source_state;
	int32 source_pid;
	bool source_reply_pending;
	ClusterWalCutMessageV1 source_request;
	ClusterWalWriterSampleV1 source_sample;
	ClusterWalCutMessageV1 source_reply;
	int32 bgwriter_procno;
	int32 bgwriter_pid;
} WalCutShared;

struct ClusterWalCutV1 {
	ResourceOwner owner;
	pid_t pid;
	uint64 revision;
	bool stale;
	bool complete;
	ClusterWalCutMessageV1 requested;
	ClusterWalWriterFlushV1 accepted;
};

static WalCutShared *cut_shared;
static ClusterWalCutV1 *active_cut;
static bool callbacks_registered;
static uint64 sent_revision;
static uint32 sent_verb;
static TimestampTz last_send;

static bool
cut_overlap(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

static uint64
cut_get(const uint8 *p, unsigned n)
{
	uint64 v = 0;
	for (unsigned i = 0; i < n; i++)
		v |= (uint64)p[i] << (8 * i);
	return v;
}

static void
cut_put(uint8 *p, uint64 v, unsigned n)
{
	for (unsigned i = 0; i < n; i++)
		p[i] = (uint8)(v >> (8 * i));
}

static bool
cut_message_valid(const ClusterWalCutMessageV1 *m)
{
	if (m == NULL || m->collector >= CLUSTER_MAX_NODES || m->nonce == 0
		|| m->collector_incarnation == 0 || m->collector_incarnation == UINT64_MAX || m->epoch == 0
		|| m->epoch == UINT64_MAX || m->source.timeline == 0
		|| !cluster_wal_claim_v2_ref_valid(&m->source.claim)
		|| m->source.claim.identity.origin_node_id == (int32)m->collector)
		return false;
	if (m->verb == CLUSTER_WAL_CUT_SAMPLE)
		return m->native_config_generation == 0 && m->reserved_end == 0 && m->flushed_end == 0;
	if (m->native_config_generation == 0
		|| m->native_config_generation > m->source.claim.max_config_generation
		|| m->reserved_end == InvalidXLogRecPtr || m->reserved_end == UINT64_MAX
		|| m->flushed_end == UINT64_MAX)
		return false;
	if (m->verb == CLUSTER_WAL_CUT_SAMPLED)
		return m->flushed_end == 0 || m->flushed_end >= m->reserved_end;
	if (m->verb == CLUSTER_WAL_CUT_CONFIRM)
		return m->flushed_end == 0;
	return m->verb == CLUSTER_WAL_CUT_CONFIRMED && m->flushed_end >= m->reserved_end;
}

bool
cluster_wal_cut_encode_v1(const ClusterWalCutMessageV1 *m, uint8 bytes[CLUSTER_WAL_CUT_BYTES])
{
	const ClusterControlRootIdentity *id;
	uint8 *p;
	if (cut_overlap(m, sizeof(*m), bytes, CLUSTER_WAL_CUT_BYTES))
		return false;
	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_WAL_CUT_BYTES);
	if (bytes == NULL || !cut_message_valid(m))
		return false;
	memcpy(bytes, "PWCT", 4);
	cut_put(bytes + 4, 1, 2);
	cut_put(bytes + 6, CLUSTER_WAL_CUT_BYTES, 2);
	cut_put(bytes + 8, m->verb, 4);
	cut_put(bytes + 12, m->collector, 4);
	cut_put(bytes + 16, m->nonce, 8);
	cut_put(bytes + 24, m->collector_incarnation, 8);
	cut_put(bytes + 32, m->epoch, 8);
	p = bytes + 40;
	id = &m->source.claim.identity;
	cut_put(p, id->system_identifier, 8);
	memcpy(p + 8, id->storage_uuid, 16);
	memcpy(p + 24, id->authority_uuid, 16);
	cut_put(p + 40, id->origin_thread_id, 2);
	cut_put(p + 44, (uint32)id->origin_node_id, 4);
	cut_put(p + 48, (uint64)id->thread_claim_created_at, 8);
	cut_put(p + 56, id->thread_claim_crc32c, 4);
	cut_put(p + 64, id->origin_owner_incarnation, 8);
	cut_put(p + 72, id->root_lineage_seq, 8);
	cut_put(p + 80, m->source.claim.database_incarnation, 8);
	cut_put(p + 88, m->source.claim.max_config_generation, 8);
	memcpy(p + 96, m->source.claim.claim_sha256, 32);
	cut_put(p + 128, m->source.timeline, 4);
	cut_put(bytes + 176, m->native_config_generation, 8);
	cut_put(bytes + 184, m->reserved_end, 8);
	cut_put(bytes + 192, m->flushed_end, 8);
	return true;
}

bool
cluster_wal_cut_decode_v1(const void *data, Size length, ClusterWalCutMessageV1 *out)
{
	const uint8 *bytes = data, *p;
	ClusterWalCutMessageV1 m = { 0 };
	ClusterControlRootIdentity *id = &m.source.claim.identity;
	if (cut_overlap(data, length, out, sizeof(*out)))
		return false;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (bytes == NULL || out == NULL || length != CLUSTER_WAL_CUT_BYTES
		|| memcmp(bytes, "PWCT", 4) != 0 || cut_get(bytes + 4, 2) != 1
		|| cut_get(bytes + 6, 2) != CLUSTER_WAL_CUT_BYTES)
		return false;
	m.verb = cut_get(bytes + 8, 4);
	m.collector = cut_get(bytes + 12, 4);
	m.nonce = cut_get(bytes + 16, 8);
	m.collector_incarnation = cut_get(bytes + 24, 8);
	m.epoch = cut_get(bytes + 32, 8);
	p = bytes + 40;
	if (cut_get(p + 42, 2) != 0 || cut_get(p + 60, 4) != 0 || cut_get(p + 132, 4) != 0)
		return false;
	id->system_identifier = cut_get(p, 8);
	memcpy(id->storage_uuid, p + 8, 16);
	memcpy(id->authority_uuid, p + 24, 16);
	id->origin_thread_id = cut_get(p + 40, 2);
	id->origin_node_id = (int32)cut_get(p + 44, 4);
	id->thread_claim_created_at = (int64)cut_get(p + 48, 8);
	id->thread_claim_crc32c = cut_get(p + 56, 4);
	id->origin_owner_incarnation = cut_get(p + 64, 8);
	id->root_lineage_seq = cut_get(p + 72, 8);
	m.source.claim.database_incarnation = cut_get(p + 80, 8);
	m.source.claim.max_config_generation = cut_get(p + 88, 8);
	memcpy(m.source.claim.claim_sha256, p + 96, 32);
	m.source.timeline = cut_get(p + 128, 4);
	m.native_config_generation = cut_get(bytes + 176, 8);
	m.reserved_end = cut_get(bytes + 184, 8);
	m.flushed_end = cut_get(bytes + 192, 8);
	if (!cut_message_valid(&m))
		return false;
	*out = m;
	return true;
}

static bool
cut_current(const ClusterWalCutMessageV1 *m, bool collector)
{
	int32 source = m->source.claim.identity.origin_node_id;
	uint64 boot
		= collector ? m->collector_incarnation : m->source.claim.identity.origin_owner_incarnation;
	return cluster_enabled && cluster_shared_config && !cluster_normal_stop_requested()
		   && cut_message_valid(m) && cluster_node_id == (collector ? (int32)m->collector : source)
		   && GetSystemIdentifier() == m->source.claim.identity.system_identifier
		   && cluster_qvotec_get_self_incarnation() == boot && cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == m->epoch
		   && !cluster_reconfig_has_pending_prebump_stage()
		   && cluster_membership_get_state(m->collector) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(m->collector)
				  == m->collector_incarnation
		   && cluster_membership_get_state(source) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(source)
				  == m->source.claim.identity.origin_owner_incarnation;
}

static bool
cut_same_request(const ClusterWalCutMessageV1 *a, const ClusterWalCutMessageV1 *b)
{
	return a->collector == b->collector && a->collector_incarnation == b->collector_incarnation
		   && a->epoch == b->epoch && a->nonce == b->nonce
		   && memcmp(&a->source, &b->source, sizeof(a->source)) == 0;
}

static bool
cut_source_covered(const ClusterWalSourceRef *native, const ClusterWalSourceRef *selected)
{
	ClusterWalSourceRef expanded = *native;
	if (native->claim.max_config_generation == 0
		|| native->claim.max_config_generation > selected->claim.max_config_generation)
		return false;
	expanded.claim.max_config_generation = selected->claim.max_config_generation;
	return memcmp(&expanded, selected, sizeof(expanded)) == 0;
}

static void
cut_cancel(ClusterWalCutV1 *cut)
{
	if (cut == NULL || cut->pid != getpid())
		return;
	if (cut_shared != NULL) {
		SpinLockAcquire(&cut_shared->lock);
		if (cut_shared->owner_pid == MyProcPid && cut_shared->revision == cut->revision) {
			cut_shared->owner_pid = 0;
			cut_shared->result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		SpinLockRelease(&cut_shared->lock);
	}
	cut->stale = true;
	if (active_cut == cut)
		active_cut = NULL;
}

static void
cut_resource_release(ResourceReleasePhase phase, bool isCommit pg_attribute_unused(),
					 bool isTopLevel pg_attribute_unused(), void *arg pg_attribute_unused())
{
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && active_cut != NULL
		&& active_cut->owner == CurrentResourceOwner)
		cut_cancel(active_cut);
}

static void
cut_child_exit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	cut_cancel(active_cut);
	if (cut_shared != NULL) {
		SpinLockAcquire(&cut_shared->lock);
		if (cut_shared->source_pid == MyProcPid) {
			cut_shared->source_state = WAL_CUT_SOURCE_EMPTY;
			cut_shared->source_pid = 0;
		}
		if (cut_shared->bgwriter_pid == MyProcPid) {
			cut_shared->bgwriter_pid = 0;
			cut_shared->bgwriter_procno = -1;
		}
		SpinLockRelease(&cut_shared->lock);
	}
}

static void
cut_register_cleanup(void)
{
	if (!callbacks_registered) {
		RegisterResourceReleaseCallback(cut_resource_release, NULL);
		before_shmem_exit(cut_child_exit, (Datum)0);
		callbacks_registered = true;
	}
}

static bool
cut_background(void)
{
	return MyBackendType == B_BG_WORKER || MyBackendType == B_BG_WRITER
		   || MyBackendType == B_CHECKPOINTER;
}

ClusterControlRootResult
cluster_wal_cut_begin_v1(const ClusterWalSourceRef *source, ClusterWalCutV1 **out)
{
	ClusterWalCutMessageV1 request = { 0 };
	ClusterWalCutV1 *cut;
	bool acquired = false;
	if (source == NULL || out == NULL || *out != NULL || !cut_background()
		|| CurrentResourceOwner == NULL || cut_shared == NULL || MyProcPid <= 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (active_cut != NULL)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	request.verb = CLUSTER_WAL_CUT_SAMPLE;
	request.collector = cluster_node_id;
	request.collector_incarnation = cluster_qvotec_get_self_incarnation();
	request.epoch = cluster_epoch_get_current();
	request.source = *source;
	if (!pg_strong_random(&request.nonce, sizeof(request.nonce)) || !cut_current(&request, true))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	/* The input scope keeps its ResourceOwner/current memory context alive.
	 * Native ERROR cleanup releases that owner before resetting its context. */
	cut = palloc0(sizeof(*cut));
	cut->pid = getpid();
	cut->owner = CurrentResourceOwner;
	cut->requested = request;
	cut_register_cleanup();
	SpinLockAcquire(&cut_shared->lock);
	if (cut_shared->owner_pid == 0 && cut_shared->revision != UINT64_MAX) {
		cut->revision = ++cut_shared->revision;
		cut_shared->owner_pid = MyProcPid;
		cut_shared->request = request;
		memset(&cut_shared->reply, 0, sizeof(cut_shared->reply));
		cut_shared->result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		acquired = true;
	}
	SpinLockRelease(&cut_shared->lock);
	if (!acquired) {
		pfree(cut);
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	active_cut = cut;
	*out = cut;
	cluster_lmon_wakeup();
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_cut_poll_v1(ClusterWalCutV1 *cut, ClusterWalWriterFlushV1 *out)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	ClusterWalCutMessageV1 reply = { 0 };
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (cut == NULL || cut->pid != getpid() || cut->owner != CurrentResourceOwner
		|| !cut_background())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cut->stale || !cut_current(&cut->requested, true)) {
		cut_cancel(cut);
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (cut->complete) {
		*out = cut->accepted;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	if (cut_shared != NULL) {
		SpinLockAcquire(&cut_shared->lock);
		if (cut_shared->owner_pid == MyProcPid && cut_shared->revision == cut->revision) {
			result = cut_shared->result;
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				reply = cut_shared->reply;
		}
		SpinLockRelease(&cut_shared->lock);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (!cut_same_request(&cut->requested, &reply) || !cut_message_valid(&reply)
			|| reply.flushed_end < reply.reserved_end || !cut_current(&reply, true)) {
			cut_cancel(cut);
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		cut->accepted.writer.ref = reply.source;
		cut->accepted.writer.ref.claim.max_config_generation = reply.native_config_generation;
		cut->accepted.writer.epoch = reply.epoch;
		cut->accepted.complete_end = reply.reserved_end;
		cut->accepted.flushed_end = reply.flushed_end;
		/* Release shared capacity before the caller starts physical I/O.
		 * Cached completion remains bound to this private job/boot/epoch. */
		cut_cancel(cut);
		cut->stale = false;
		cut->complete = true;
		*out = cut->accepted;
	} else if (result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
		cut_cancel(cut);
	return result;
}

void
cluster_wal_cut_release_v1(ClusterWalCutV1 **cut)
{
	if (cut == NULL || *cut == NULL || (*cut)->pid != getpid())
		return;
	cut_cancel(*cut);
	explicit_bzero(*cut, sizeof(**cut));
	pfree(*cut);
	*cut = NULL;
}

static void
cut_source_enqueue(const ClusterWalCutMessageV1 *request)
{
	ClusterWalWriterSampleV1 sample = { 0 };
	WalCutSourceState state;
	uint64 revision;
	int32 procno = -1, pid = 0;
	bool same;
	if (cut_shared == NULL)
		return;
	SpinLockAcquire(&cut_shared->lock);
	state = cut_shared->source_state;
	revision = cut_shared->source_revision;
	same = state != WAL_CUT_SOURCE_EMPTY && cut_same_request(&cut_shared->source_request, request);
	if (same
		&& (request->verb == CLUSTER_WAL_CUT_SAMPLE
			|| (request->reserved_end == cut_shared->source_sample.reserved_end
				&& request->native_config_generation
					   == cut_shared->source_sample.writer.ref.claim.max_config_generation))) {
		/* A duplicate reuses the original reservation, including while the
		 * source is idle. It cannot make the required flush a moving target. */
		if (request->verb == CLUSTER_WAL_CUT_CONFIRM)
			cut_shared->source_request = *request;
		if (state == WAL_CUT_SOURCE_REPLIED) {
			cut_shared->source_reply.verb
				= cut_shared->source_request.verb == CLUSTER_WAL_CUT_SAMPLE
					  ? CLUSTER_WAL_CUT_SAMPLED
					  : CLUSTER_WAL_CUT_CONFIRMED;
			cut_shared->source_reply_pending = true;
		}
	}
	SpinLockRelease(&cut_shared->lock);
	if (same || state == WAL_CUT_SOURCE_QUEUED || state == WAL_CUT_SOURCE_RUNNING
		|| revision == UINT64_MAX)
		return;
	if (request->verb == CLUSTER_WAL_CUT_SAMPLE) {
		if (cluster_wal_writer_sample_v1(&sample) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| sample.writer.epoch != request->epoch || sample.writer.startup_first_lsn != 0
			|| !cut_source_covered(&sample.writer.ref, &request->source))
			return;
	} else {
		sample.writer.ref = request->source;
		sample.writer.ref.claim.max_config_generation = request->native_config_generation;
		sample.writer.epoch = request->epoch;
		sample.reserved_end = request->reserved_end;
	}
	if (!cut_current(request, false))
		return;
	SpinLockAcquire(&cut_shared->lock);
	if (cut_shared->source_revision == revision
		&& (cut_shared->source_state == WAL_CUT_SOURCE_EMPTY
			|| cut_shared->source_state == WAL_CUT_SOURCE_REPLIED)) {
		cut_shared->source_revision++;
		cut_shared->source_request = *request;
		cut_shared->source_sample = sample;
		cut_shared->source_state = WAL_CUT_SOURCE_QUEUED;
		cut_shared->source_reply_pending = false;
		procno = cut_shared->bgwriter_procno;
		pid = cut_shared->bgwriter_pid;
	}
	SpinLockRelease(&cut_shared->lock);
	if (ProcGlobal != NULL && procno >= 0 && (uint32)procno < ProcGlobal->allProcCount && pid > 0
		&& ProcGlobal->allProcs[procno].pid == pid)
		SetLatch(&ProcGlobal->allProcs[procno].procLatch);
}

void
cluster_wal_cut_ingress_v1(const ClusterICEnvelope *env, const void *payload)
{
	ClusterWalCutMessageV1 m;
	if (MyBackendType != B_LMON || env == NULL || env->msg_type != PGRAC_IC_MSG_WAL_CUT
		|| env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_wal_cut_decode_v1(payload, env->payload_length, &m) || env->epoch != m.epoch)
		return;
	/* The CONTROL router binds envelope source to the peer's HELLO. Every
	 * semantic boot, source, epoch and job identity is checked independently. */
	if (m.verb == CLUSTER_WAL_CUT_SAMPLE || m.verb == CLUSTER_WAL_CUT_CONFIRM) {
		if (env->source_node_id != m.collector || !cut_current(&m, false))
			return;
		cut_source_enqueue(&m);
		return;
	}
	if (cut_shared == NULL || env->source_node_id != (uint32)m.source.claim.identity.origin_node_id
		|| !cut_current(&m, true))
		return;
	SpinLockAcquire(&cut_shared->lock);
	if (cut_shared->owner_pid != 0 && cut_shared->result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
		&& cut_same_request(&cut_shared->request, &m)
		&& ((cut_shared->request.verb == CLUSTER_WAL_CUT_SAMPLE
			 && m.verb == CLUSTER_WAL_CUT_SAMPLED)
			|| (cut_shared->request.verb == CLUSTER_WAL_CUT_CONFIRM
				&& m.verb == CLUSTER_WAL_CUT_CONFIRMED
				&& m.reserved_end == cut_shared->request.reserved_end
				&& m.native_config_generation == cut_shared->request.native_config_generation))) {
		if (m.flushed_end == 0) {
			cut_shared->request = m;
			cut_shared->request.verb = CLUSTER_WAL_CUT_CONFIRM;
		} else {
			cut_shared->reply = m;
			cut_shared->result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		}
	}
	SpinLockRelease(&cut_shared->lock);
}

bool
cluster_wal_cut_bgwriter_tick_v1(void)
{
	ClusterWalCutMessageV1 request, reply;
	ClusterWalWriterSampleV1 sample;
	ClusterWalWriterFlushV1 flushed;
	volatile ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	uint64 revision;
	bool queued;
	if (MyBackendType != B_BG_WRITER || cut_shared == NULL || !cluster_enabled
		|| !cluster_shared_config || CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	cut_register_cleanup();
	SpinLockAcquire(&cut_shared->lock);
	if (MyProc != NULL) {
		cut_shared->bgwriter_procno = MyProc->pgprocno;
		cut_shared->bgwriter_pid = MyProcPid;
	}
	queued = cut_shared->source_state == WAL_CUT_SOURCE_QUEUED;
	request = cut_shared->source_request;
	sample = cut_shared->source_sample;
	revision = cut_shared->source_revision;
	if (queued) {
		cut_shared->source_state = WAL_CUT_SOURCE_RUNNING;
		cut_shared->source_pid = MyProcPid;
	}
	SpinLockRelease(&cut_shared->lock);
	if (!queued)
		return false;
	reply = request;
	PG_TRY();
	{
		if (cut_current(&request, false))
			result = cluster_wal_writer_flush_sample_v1(&sample, &flushed);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			reply.verb = request.verb == CLUSTER_WAL_CUT_SAMPLE ? CLUSTER_WAL_CUT_SAMPLED
																: CLUSTER_WAL_CUT_CONFIRMED;
			reply.native_config_generation = sample.writer.ref.claim.max_config_generation;
			reply.reserved_end = sample.reserved_end;
			reply.flushed_end = flushed.flushed_end;
			if (!cut_current(&reply, false))
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
	}
	PG_FINALLY();
	{
		SpinLockAcquire(&cut_shared->lock);
		if (cut_shared->source_revision == revision && cut_shared->source_pid == MyProcPid) {
			cut_shared->source_pid = 0;
			cut_shared->source_reply_pending = result == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
				reply.verb = cut_shared->source_request.verb == CLUSTER_WAL_CUT_SAMPLE
								 ? CLUSTER_WAL_CUT_SAMPLED
								 : CLUSTER_WAL_CUT_CONFIRMED;
				cut_shared->source_reply = reply;
				cut_shared->source_state = WAL_CUT_SOURCE_REPLIED;
			} else
				cut_shared->source_state = result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
											   ? WAL_CUT_SOURCE_QUEUED
											   : WAL_CUT_SOURCE_EMPTY;
		}
		SpinLockRelease(&cut_shared->lock);
		cluster_lmon_wakeup();
	}
	PG_END_TRY();
	return true;
}

void
cluster_wal_cut_lmon_tick_v1(void)
{
	ClusterWalCutMessageV1 request, reply;
	ClusterControlRootResult result;
	uint64 revision, source_revision;
	int32 owner;
	bool reply_pending;
	bool stopping;
	uint8 wire[CLUSTER_WAL_CUT_BYTES];
	TimestampTz now;
	if (MyBackendType != B_LMON || cut_shared == NULL)
		return;
	stopping = cluster_normal_stop_requested();
	SpinLockAcquire(&cut_shared->lock);
	if (stopping) {
		cut_shared->owner_pid = 0;
		cut_shared->result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (cut_shared->source_state != WAL_CUT_SOURCE_RUNNING) {
			cut_shared->source_state = WAL_CUT_SOURCE_EMPTY;
			cut_shared->source_reply_pending = false;
		}
		SpinLockRelease(&cut_shared->lock);
		return;
	}
	owner = cut_shared->owner_pid;
	revision = cut_shared->revision;
	result = cut_shared->result;
	request = cut_shared->request;
	source_revision = cut_shared->source_revision;
	reply_pending
		= cut_shared->source_state == WAL_CUT_SOURCE_REPLIED && cut_shared->source_reply_pending;
	reply = cut_shared->source_reply;
	if (reply_pending)
		cut_shared->source_reply_pending = false;
	SpinLockRelease(&cut_shared->lock);
	if (reply_pending && cut_current(&reply, false) && cluster_wal_cut_encode_v1(&reply, wire)) {
		ClusterICSendResult sent
			= cluster_ic_send_envelope(PGRAC_IC_MSG_WAL_CUT, reply.collector, wire, sizeof(wire));
		if (sent != CLUSTER_IC_SEND_DONE && sent != CLUSTER_IC_SEND_WOULD_BLOCK) {
			SpinLockAcquire(&cut_shared->lock);
			if (cut_shared->source_revision == source_revision
				&& cut_shared->source_state == WAL_CUT_SOURCE_REPLIED)
				cut_shared->source_reply_pending = true;
			SpinLockRelease(&cut_shared->lock);
		}
	}
	if (owner == 0 || result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
		return;
	if (!cut_current(&request, true)) {
		SpinLockAcquire(&cut_shared->lock);
		if (cut_shared->owner_pid == owner && cut_shared->revision == revision)
			cut_shared->result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		SpinLockRelease(&cut_shared->lock);
		return;
	}
	now = GetCurrentTimestamp();
	/* A timer only paces retransmission; it never produces a positive cut. */
	if (sent_revision == revision && sent_verb == request.verb && now >= last_send
		&& now - last_send < WAL_CUT_RETRY_US)
		return;
	if (cluster_wal_cut_encode_v1(&request, wire))
		(void)cluster_ic_send_envelope(
			PGRAC_IC_MSG_WAL_CUT, request.source.claim.identity.origin_node_id, wire, sizeof(wire));
	sent_revision = revision;
	sent_verb = request.verb;
	last_send = now;
}

ClusterNormalStopPollResult
cluster_wal_cut_normal_stop_poll_v1(const char **reason)
{
	bool active;
	if (reason != NULL)
		*reason = NULL;
	if (!cluster_shared_config)
		return CLUSTER_NORMAL_STOP_READY;
	if (cut_shared == NULL) {
		if (reason != NULL)
			*reason = "WAL_CUT_OWNER_UNINITIALIZED";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	SpinLockAcquire(&cut_shared->lock);
	active = cut_shared->owner_pid != 0 || cut_shared->source_state == WAL_CUT_SOURCE_QUEUED
			 || cut_shared->source_state == WAL_CUT_SOURCE_RUNNING;
	SpinLockRelease(&cut_shared->lock);
	if (active && reason != NULL)
		*reason = "WAL_CUT_NATIVE_FLUSH_PENDING";
	return active ? CLUSTER_NORMAL_STOP_PENDING : CLUSTER_NORMAL_STOP_READY;
}

void
cluster_wal_cut_register_v1(void)
{
	static const ClusterICMsgTypeInfo info = { .msg_type = PGRAC_IC_MSG_WAL_CUT,
											   .name = "native_wal_read_cut",
											   .allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
											   .broadcast_ok = false,
											   .handler = cluster_wal_cut_ingress_v1,
											   .plane = CLUSTER_IC_PLANE_CONTROL };
	cluster_ic_register_msg_type(&info);
}

static Size
cut_shmem_size(void)
{
	return MAXALIGN(sizeof(WalCutShared));
}

static void
cut_shmem_init(void)
{
	bool found;
	cut_shared = ShmemInitStruct("pgrac native WAL read cut", cut_shmem_size(), &found);
	if (!found) {
		memset(cut_shared, 0, sizeof(*cut_shared));
		SpinLockInit(&cut_shared->lock);
		cut_shared->bgwriter_procno = -1;
	}
}

void
cluster_wal_cut_shmem_register_v1(void)
{
	static const ClusterShmemRegion region = { .name = "pgrac native WAL read cut",
											   .size_fn = cut_shmem_size,
											   .init_fn = cut_shmem_init,
											   .lwlock_count = 0,
											   .owner_subsys = "cluster_wal_cut",
											   .reserved_flags = 0 };
	cluster_shmem_register_region(&region);
}
