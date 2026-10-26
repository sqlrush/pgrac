/*-------------------------------------------------------------------------
 * cluster_content_trace.c
 *   Local diagnostic projection of actual LWLock ownership and wait targets.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#ifdef USE_PGRAC_CLUSTER
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include "postmaster/postmaster.h"
typedef struct ContentTraceSlot {
	pg_atomic_uint32 busy;
	pg_atomic_uint64 revision;
	ClusterContentTraceOwner owner;
} ContentTraceSlot;

typedef struct ContentTraceShared {
	uint32 nslots;
	ContentTraceSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ContentTraceShared;

static ContentTraceShared *content_trace;
static ClusterContentTraceOwner local_owner;
static uint32 emitted_rows;
static uint32 pair_depth;
static bool pair_recording;
static int pair_slot = -1;
static Buffer pair_buffers[2];
static uint64 service_serial, service_current;
static uint64 lifetime_serial;
static uint32 held_lifetime_count;
static int deadline_fd = -1;
static struct sockaddr_in deadline_address;
static uint32 deadline_sequence, deadline_dropped;
static uint32 deadline_capture, gcs_captures;
static struct { LWLock *lock; uint64 lifetime; } held_lifetimes[CLUSTER_CONTENT_TRACE_LOCKS];

static uint64
content_trace_lifetime(void)
{
	return lifetime_serial == PG_UINT64_MAX ? 0 : ++lifetime_serial;
}

/* Only called from the original successful native acquisition hook. A held
 * list enumeration by an unrelated publisher must never invent a lifetime. */
static void
content_trace_acquired(LWLock *lock, LWLockMode mode, void *target)
{
	uint32 empty = CLUSTER_CONTENT_TRACE_LOCKS;
	(void)mode;
	if (lock != target) return;
	for (uint32 i = 0; i < CLUSTER_CONTENT_TRACE_LOCKS; i++) {
		if (held_lifetimes[i].lock == lock) return;
		if (held_lifetimes[i].lock == NULL) empty = i;
	}
	if (empty < CLUSTER_CONTENT_TRACE_LOCKS) {
		held_lifetimes[empty].lock = lock;
		held_lifetimes[empty].lifetime = content_trace_lifetime();
		held_lifetime_count++;
	}
}

static Size
content_trace_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(add_size(offsetof(ContentTraceShared, slots),
		mul_size(MaxBackends + NUM_AUXILIARY_PROCS, sizeof(ContentTraceSlot))));
}

static void
content_trace_init(void)
{
	bool found;
	Size size = content_trace_size();
	if (size == 0)
		return;
	content_trace = ShmemInitStruct("pgrac content lock trace", size, &found);
	if (!found) {
		memset(content_trace, 0, size);
		content_trace->nslots = MaxBackends + NUM_AUXILIARY_PROCS;
		for (uint32 i = 0; i < content_trace->nslots; i++) {
			pg_atomic_init_u32(&content_trace->slots[i].busy, 0);
			pg_atomic_init_u64(&content_trace->slots[i].revision, 0);
		}
	}
}

void
cluster_content_trace_shmem_register(void)
{
	static const ClusterShmemRegion region = {
		.name = "pgrac content lock trace", .size_fn = content_trace_size,
		.init_fn = content_trace_init, .lwlock_count = 0,
		.owner_subsys = "cluster_content_trace", .reserved_flags = 0
	};
	cluster_shmem_register_region(&region);
}

void
cluster_content_trace_process_start(uint32 procno)
{
	ContentTraceSlot *slot;
	uint32 expected = 0;
	uint64 revision;

	/* Invalidate a prior incarnation even if its PID/backendId are reused.
	 * Never take busy away from a reader or a previous, interrupted publisher. */
	if (content_trace == NULL || procno >= content_trace->nslots)
		return;
	slot = &content_trace->slots[procno];
	revision = pg_atomic_fetch_add_u64(&slot->revision, 1) + 1;
	if (!pg_atomic_compare_exchange_u32(&slot->busy, &expected, 1))
		return;
	/* Only the native allocator calls this, before publishing its new PID.
	 * The previous allocation has released its content holds; the new one
	 * has none yet. Publish that empty baseline, not a stale incarnation.
	 * A missed busy acquisition above must instead retain UNKNOWN. */
	memset(&slot->owner, 0, sizeof(slot->owner));
	slot->owner.revision = revision;
	(void)pg_atomic_exchange_u32(&slot->busy, 0);
}

static bool
content_trace_tag_equal(const BufferTag *a, const BufferTag *b)
{
	return a->spcOid == b->spcOid && a->dbOid == b->dbOid &&
		a->relNumber == b->relNumber && a->forkNum == b->forkNum &&
		a->blockNum == b->blockNum;
}

static void
content_trace_collect_lock(LWLock *lock, LWLockMode mode, void *context)
{
	ClusterContentTraceOwner *owner = context;
	BufferDesc *buf = BufferDescriptorFromContentLock(lock);
	ClusterContentTraceLock *item;

	if (buf == NULL)
		return;
	if (owner->nlocks == CLUSTER_CONTENT_TRACE_LOCKS) {
		owner->omitted_locks++;
		return;
	}
	item = &owner->locks[owner->nlocks++];
	for (uint32 i = 0; i < CLUSTER_CONTENT_TRACE_LOCKS; i++)
		if (held_lifetimes[i].lock == lock)
			item->lifetime = held_lifetimes[i].lifetime;
	item->buffer = BufferDescriptorGetBuffer(buf);
	item->mode = mode;
	item->tag_known = (pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) != 0;
	if (item->tag_known)
		item->tag = buf->tag;
}

/* Never allocate, log, or wait: this also runs inside LWLockReleaseAll. A
 * reader may briefly own busy. Invalidate its old payload BEFORE trying busy,
 * so a missed publication can only yield UNKNOWN, never a stale owner claim. */
static void
content_trace_publish(bool entering_wait)
{
	ContentTraceSlot *slot;
	uint32 expected = 0;
	uint64 revision;
	bool was_live = local_owner.pid != 0;

	if (!cluster_trace_gcs_capacity)
		memset(&local_owner.gcs_scan, 0, sizeof(local_owner.gcs_scan));
	if (!cluster_trace_tx_identity) {
		memset(held_lifetimes, 0, sizeof(held_lifetimes));
		held_lifetime_count = 0;
		/* A missed shared publication must not preserve private diagnostic
		 * resources for a later re-enable to resurrect. */
		memset(&local_owner, 0, sizeof(local_owner));
		service_current = 0;
	}
	if (content_trace == NULL || MyProc == NULL ||
		MyProc->pgprocno >= content_trace->nslots)
		return;
	if (!cluster_trace_tx_identity && !was_live)
		return;
	slot = &content_trace->slots[MyProc->pgprocno];
	revision = pg_atomic_fetch_add_u64(&slot->revision, 1) + 1;
	if (!pg_atomic_compare_exchange_u32(&slot->busy, &expected, 1))
		return;
	if (!cluster_trace_tx_identity) {
		memset(&local_owner, 0, sizeof(local_owner));
	} else {
		local_owner.pid = MyProcPid;
		local_owner.procno = MyProc->pgprocno;
		local_owner.backend_id = MyProc->backendId;
		local_owner.backend_start = MyStartTimestamp;
		local_owner.proc_generation = MyProc->cluster_grd_generation;
		local_owner.nlocks = local_owner.omitted_locks = 0;
		memset(local_owner.locks, 0, sizeof(local_owner.locks));
		ForEachLWLockHeldByMe(content_trace_collect_lock, &local_owner);
		if (entering_wait && local_owner.nlocks != 0)
			local_owner.waits_with_locks++;
	}
	local_owner.revision = revision;
	slot->owner = local_owner;
	/* Full barrier: publish the payload before allowing another accessor. */
	(void)pg_atomic_exchange_u32(&slot->busy, 0);
}

void
cluster_content_trace_lock_changed(LWLock *lock)
{
	if (!cluster_trace_tx_identity && local_owner.pid == 0)
		return;
	/* Do not read the released buffer's tag: another backend can rebind it. */
	if (BufferDescriptorFromContentLock(lock) != NULL) {
		if (cluster_trace_tx_identity)
			ForEachLWLockHeldByMe(content_trace_acquired, lock);
		content_trace_publish(false);
	}
}

/* Invalidate before native ownership changes. A delayed post-release
 * publication must not leave the previous owner looking current. */
void
cluster_content_trace_lock_invalidating(LWLock *lock)
{
	if (BufferDescriptorFromContentLock(lock) == NULL) return;
	/* Retire even when disabled or a shared publication is missed. */
	for (uint32 i = 0; held_lifetime_count != 0 && i < CLUSTER_CONTENT_TRACE_LOCKS; i++)
		if (held_lifetimes[i].lock == lock)
		{
			memset(&held_lifetimes[i], 0, sizeof(held_lifetimes[i]));
			held_lifetime_count--;
			break;
		}
	if ((!cluster_trace_tx_identity && local_owner.pid == 0) ||
		content_trace == NULL || MyProc == NULL ||
		MyProc->pgprocno >= content_trace->nslots ||
		BufferDescriptorFromContentLock(lock) == NULL)
		return;
	(void)pg_atomic_fetch_add_u64(&content_trace->slots[MyProc->pgprocno].revision, 1);
}

void
cluster_content_trace_native_wait_set(LWLock *lock, LWLockMode mode)
{
	BufferDesc *buf;
	ClusterContentTraceLock *wait = &local_owner.native_wait;

	if (!cluster_trace_tx_identity ||
		(buf = BufferDescriptorFromContentLock(lock)) == NULL)
		return;
	memset(wait, 0, sizeof(*wait));
	wait->lifetime = content_trace_lifetime();
	wait->buffer = BufferDescriptorGetBuffer(buf);
	wait->mode = mode;
	/* The original native caller owns its pin; no remote tag inference. */
	wait->tag_known = (pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) != 0;
	if (wait->tag_known)
		wait->tag = buf->tag;
	local_owner.native_waiting = true;
	content_trace_publish(true);
}

void
cluster_content_trace_native_wait_clear(void)
{
	if (!local_owner.native_waiting)
		return;
	local_owner.native_waiting = false;
	memset(&local_owner.native_wait, 0, sizeof(local_owner.native_wait));
	content_trace_publish(false);
}

void
cluster_content_trace_enqueue_wait_set(const ClusterResId *resid,
	const ClusterGrdHolderId *holder, int32 mode, int32 current_mode,
	bool convert, uint64 request, uint64 epoch, uint64 wait_seq)
{
	ClusterContentTraceEnqueue *wait = &local_owner.enqueue;

	if (!cluster_trace_tx_identity || resid == NULL || holder == NULL)
		return;
	memset(wait, 0, sizeof(*wait));
	wait->lifetime = content_trace_lifetime();
	wait->field1 = resid->field1;
	wait->field2 = resid->field2;
	wait->field3 = resid->field3;
	wait->field4 = resid->field4;
	wait->type = resid->type;
	wait->lockmethodid = resid->lockmethodid;
	wait->mode = mode;
	wait->current_mode = current_mode;
	wait->convert = convert;
	wait->holder_node = holder->node_id;
	wait->holder_procno = holder->procno;
	wait->holder_epoch = holder->cluster_epoch;
	wait->holder_request = holder->request_id;
	wait->request = request;
	wait->epoch = epoch;
	wait->wait_seq = wait_seq;
	local_owner.enqueue_waiting = true;
	content_trace_publish(true);
}

static bool
content_trace_grant_matches(const ClusterContentTraceGrant *row,
	const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	return row->field1 == resid->field1 && row->field2 == resid->field2 &&
		row->field3 == resid->field3 && row->field4 == resid->field4 &&
		row->type == resid->type && row->lockmethodid == resid->lockmethodid &&
		row->holder_node == holder->node_id && row->holder_procno == holder->procno &&
		row->holder_epoch == holder->cluster_epoch && row->holder_request == holder->request_id;
}

void
cluster_content_trace_enqueue_granted(const ClusterResId *resid,
	const ClusterGrdHolderId *holder, int32 mode)
{
	ClusterContentTraceGrant *row;
	uint32 i;

	if (!cluster_trace_tx_identity || resid == NULL || holder == NULL || MyProc == NULL ||
		holder->node_id != (uint32)cluster_node_id || holder->procno != (uint32)MyProc->pgprocno ||
		holder->request_id == 0 || holder->cluster_epoch == 0 ||
		mode < AccessShareLock || mode > AccessExclusiveLock)
		return;
	for (i = 0; i < local_owner.ngrants; i++)
		if (content_trace_grant_matches(&local_owner.grants[i], resid, holder))
			break;
	if (i == CLUSTER_CONTENT_TRACE_GRANTS) {
		/* Identity of omitted rows is unknown. Keep this sticky until process
		 * exit/trace disable, rather than guess which later release retires it. */
		if (local_owner.omitted_grants != UINT32_MAX)
			local_owner.omitted_grants++;
	} else {
		if (i == local_owner.ngrants) local_owner.ngrants++;
		row = &local_owner.grants[i];
		memset(row, 0, sizeof(*row));
		row->lifetime = content_trace_lifetime();
		row->field1 = resid->field1; row->field2 = resid->field2; row->field3 = resid->field3;
		row->field4 = resid->field4; row->type = resid->type; row->lockmethodid = resid->lockmethodid;
		row->holder_node = holder->node_id; row->holder_procno = holder->procno;
		row->holder_epoch = holder->cluster_epoch; row->holder_request = holder->request_id;
		row->mode = mode;
	}
	content_trace_publish(false);
}

void
cluster_content_trace_enqueue_releasing(const ClusterResId *resid,
	const ClusterGrdHolderId *holder)
{
	if (resid == NULL || holder == NULL)
		return;
	if (!cluster_trace_tx_identity) {
		if (local_owner.pid != 0) content_trace_publish(false);
		return;
	}
	for (uint32 i = 0; i < local_owner.ngrants; i++) {
		if (!content_trace_grant_matches(&local_owner.grants[i], resid, holder)) continue;
		local_owner.ngrants--;
		memmove(&local_owner.grants[i], &local_owner.grants[i + 1],
			(local_owner.ngrants - i) * sizeof(local_owner.grants[0]));
		memset(&local_owner.grants[local_owner.ngrants], 0, sizeof(local_owner.grants[0]));
		content_trace_publish(false); /* Invalidate before original S6 can mutate/wait. */
		return;
	}
}

void
cluster_content_trace_enqueue_wait_clear(void)
{
	if (!local_owner.enqueue_waiting)
		return;
	local_owner.enqueue_waiting = false;
	memset(&local_owner.enqueue, 0, sizeof(local_owner.enqueue));
	content_trace_publish(false);
}

void
cluster_content_trace_wait_set(ClusterContentTraceWait kind,
	const ClusterWaitTrace *wait)
{
	if (!cluster_trace_tx_identity || kind < 0 || kind >= CLCONTENT_NWAITS || wait == NULL)
		return;
	/* A polling sample may change counters/entry details within one actual
	 * wait. A changed caller resource/request/binding starts a new episode. */
	{
		const ClusterWaitTrace *old = &local_owner.waits[kind];
		if (!(local_owner.wait_mask & (1U << kind)) ||
			old->tag_known != wait->tag_known ||
			(old->tag_known && !content_trace_tag_equal(&old->tag, &wait->tag)) ||
			old->buffer != wait->buffer || old->binding != wait->binding ||
			old->request_seq != wait->request_seq || old->formation != wait->formation ||
			old->master_session != wait->master_session || old->r4 != wait->r4 ||
			old->mode != wait->mode || old->site != wait->site)
			local_owner.wait_lifetimes[kind] = content_trace_lifetime();
	}
	local_owner.waits[kind] = *wait;
	local_owner.wait_mask |= 1U << kind;
	content_trace_publish(true);
}

void
cluster_content_trace_wait_clear(ClusterContentTraceWait kind)
{
	if (kind < 0 || kind >= CLCONTENT_NWAITS ||
		(local_owner.wait_mask & (1U << kind)) == 0)
		return;
	local_owner.wait_mask &= ~(1U << kind);
	local_owner.wait_lifetimes[kind] = 0;
	memset(&local_owner.waits[kind], 0, sizeof(local_owner.waits[kind]));
	content_trace_publish(false);
}

/* A nested finish can carry the enclosing entry checks only for the same
 * complete native request. Unknown or distinct work must not borrow them. */
static bool
content_trace_same_pcm_service(const ClusterWaitTrace *a, const ClusterWaitTrace *b)
{
	return a->tag_known && b->tag_known && a->request_known && b->request_known
		&& a->request_seq != 0 && a->formation != 0 && a->master_session != 0 && a->r4 != 0
		&& content_trace_tag_equal(&a->tag, &b->tag)
		&& a->request_seq == b->request_seq && a->requester == b->requester
		&& a->formation == b->formation && a->master_session == b->master_session
		&& a->r4 == b->r4 && a->base == b->base && a->authority == b->authority
		&& a->expected_master == b->expected_master && a->source == b->source
		&& a->wire_sender_connection == b->wire_sender_connection
		&& a->ordered_lane == b->ordered_lane && a->semantic_crc32c == b->semantic_crc32c
		&& a->source_candidate == b->source_candidate && a->retain_pi_if_dirty == b->retain_pi_if_dirty
		&& a->common_flags == b->common_flags && a->common_outcome == b->common_outcome
		&& a->state == b->state && a->mode == b->mode;
}

void
cluster_content_trace_service_begin(ClusterContentTraceServiceScope *scope,
	const ClusterWaitTrace *service)
{
	memset(scope, 0, sizeof(*scope));
	if (!cluster_trace_tx_identity || service == NULL)
		return;
	/* Exhaustion cannot create an alias to an outstanding diagnostic scope. */
	if (service_serial == PG_UINT64_MAX) {
		service_current = 0;
		local_owner.pcm_serving = false;
		local_owner.service_lifetime = 0;
		memset(&local_owner.pcm_service, 0, sizeof(local_owner.pcm_service));
		content_trace_publish(false);
		return;
	}
	scope->serial = ++service_serial;
	scope->previous_serial = service_current;
	scope->previous_active = local_owner.pcm_serving;
	scope->previous = local_owner.pcm_service;
	scope->previous_lifetime = local_owner.service_lifetime;
	service_current = scope->serial;
	local_owner.pcm_service = *service;
	if (!service->service_admission.supplied && scope->previous_active
		&& content_trace_same_pcm_service(service, &scope->previous))
		local_owner.pcm_service.service_admission = scope->previous.service_admission;
	local_owner.pcm_serving = true;
	local_owner.service_lifetime = content_trace_lifetime();
	content_trace_publish(false);
}

void
cluster_content_trace_service_end(ClusterContentTraceServiceScope *scope)
{
	if (scope->serial == 0)
		return;
	if (service_current == scope->serial) {
		service_current = scope->previous_serial;
		local_owner.pcm_serving = scope->previous_active;
		local_owner.pcm_service = scope->previous;
		local_owner.service_lifetime = scope->previous_lifetime;
		content_trace_publish(false);
	}
	scope->serial = 0;
}

void
cluster_content_trace_error_cleanup(void)
{
	if (pair_recording && pair_slot >= 0) {
		local_owner.pair[pair_slot].error = true;
		local_owner.pair[pair_slot].finished = true;
	}
	pair_depth = 0;
	pair_recording = false;
	pair_slot = -1;
	local_owner.wait_mask = 0;
	memset(local_owner.wait_lifetimes, 0, sizeof(local_owner.wait_lifetimes));
	memset(local_owner.waits, 0, sizeof(local_owner.waits));
	local_owner.service_lifetime = 0;
	local_owner.native_waiting = local_owner.enqueue_waiting = false;
	service_current = 0;
	local_owner.pcm_serving = false;
	memset(&local_owner.pcm_service, 0, sizeof(local_owner.pcm_service));
	memset(&local_owner.native_wait, 0, sizeof(local_owner.native_wait));
	memset(&local_owner.enqueue, 0, sizeof(local_owner.enqueue));
	content_trace_publish(false);
}

/* Only the outer pair owns this bounded history. Nested pairs are counted,
 * never attributed to the outer targets. No producer allocates, logs or waits. */
void
cluster_content_trace_pair_begin(Buffer first, Buffer second)
{
	if (pair_depth++ != 0) {
		if (pair_recording)
			local_owner.pair_nested_omitted++;
		return;
	}
	pair_recording = cluster_trace_tx_identity;
	pair_slot = -1;
	if (!pair_recording)
		return;
	pair_buffers[0] = first;
	pair_buffers[1] = second;
	local_owner.pair_invocations++;
	local_owner.pair_cycles = local_owner.pair_omitted = 0;
	local_owner.npair_cycles = 0;
	memset(local_owner.pair, 0, sizeof(local_owner.pair));
}

void
cluster_content_trace_pair_cycle(void)
{
	ClusterContentTracePairCycleData *cycle;

	if (!cluster_trace_tx_identity)
		pair_recording = false;
	if (!pair_recording || pair_depth != 1)
		return;
	if (pair_slot >= 0)
		local_owner.pair[pair_slot].finished = true;
	pair_slot = local_owner.pair_cycles++ % CLUSTER_CONTENT_TRACE_PAIR_CYCLES;
	if (local_owner.npair_cycles < CLUSTER_CONTENT_TRACE_PAIR_CYCLES)
		local_owner.npair_cycles++;
	else
		local_owner.pair_omitted++;
	cycle = &local_owner.pair[pair_slot];
	memset(cycle, 0, sizeof(*cycle));
	cycle->invocation = local_owner.pair_invocations;
	cycle->cycle = local_owner.pair_cycles;
	cycle->phase = CLPAIR_FIRST;
	for (unsigned i = 0; i < 2; i++) {
		ClusterContentTraceLock *target = &cycle->targets[i];

		target->buffer = pair_buffers[i];
		target->mode = LW_EXCLUSIVE;
		/* Caller still owns both pins. An untagged/local target stays UNKNOWN. */
		if (target->buffer > 0 && target->buffer <= NBuffers) {
			BufferDesc *buf = GetBufferDescriptor(target->buffer - 1);
			target->tag_known = (pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) != 0;
			if (target->tag_known)
				target->tag = buf->tag;
		}
	}
	content_trace_publish(false);
}

bool
cluster_content_trace_pair_active(Buffer target)
{
	if (!cluster_trace_tx_identity)
		pair_recording = false;
	return pair_recording && pair_depth == 1 && pair_slot >= 0 &&
		(target == pair_buffers[0] || target == pair_buffers[1]);
}

void
cluster_content_trace_pair_phase(ClusterContentTracePairPhase phase)
{
	if (!cluster_content_trace_pair_active(pair_buffers[0]))
		return;
	local_owner.pair[pair_slot].phase = phase;
	content_trace_publish(false);
}

void
cluster_content_trace_pair_event(Buffer target, ClusterContentTracePairEvent event,
	int32 result)
{
	ClusterContentTracePairCycleData *cycle;
	unsigned leg;

	if (!cluster_content_trace_pair_active(target) || event < 0 || event >= CLPAIR_NEVENTS)
		return;
	cycle = &local_owner.pair[pair_slot];
	leg = target == pair_buffers[0] ? 0 : 1;
	cycle->events[leg][event]++;
	cycle->last_results[leg][event] = result;
	content_trace_publish(false);
}

void
cluster_content_trace_pair_request(Buffer target,
	const ClusterContentTracePairRequest *request)
{
	ClusterContentTracePairCycleData *cycle;
	unsigned leg, slot;

	if (!cluster_content_trace_pair_active(target) || request == NULL)
		return;
	cycle = &local_owner.pair[pair_slot];
	leg = target == pair_buffers[0] ? 0 : 1;
	if (!cycle->targets[leg].tag_known ||
		!content_trace_tag_equal(&cycle->targets[leg].tag, &request->tag))
		return;
	/* Retain the first and latest actual dispatch; all attempts remain counted. */
	if (cycle->nrequests < CLUSTER_CONTENT_TRACE_PAIR_REQUESTS)
		slot = cycle->nrequests++;
	else {
		slot = CLUSTER_CONTENT_TRACE_PAIR_REQUESTS - 1;
		cycle->omitted_requests++;
	}
	cycle->requests[slot] = *request;
	cycle->requests[slot].leg = leg;
	cycle->requests[slot].phase = cycle->phase;
	cycle->events[leg][CLPAIR_DISPATCH]++;
	if (!request->stage_ok)
		cycle->events[leg][CLPAIR_STAGE_REJECT]++;
	if (request->master != cluster_node_id)
		cycle->events[leg][CLPAIR_REMOTE_MASTER_DISPATCH]++;
	content_trace_publish(false);
}

void
cluster_content_trace_pair_end(void)
{
	if (pair_depth == 0 || --pair_depth != 0)
		return;
	if (pair_recording && pair_slot >= 0) {
		local_owner.pair[pair_slot].phase = CLPAIR_DONE;
		local_owner.pair[pair_slot].finished = true;
	}
	pair_recording = false;
	pair_slot = -1;
	content_trace_publish(false);
}

void
cluster_content_trace_pair(Buffer target, bool missed)
{
	ClusterWaitTrace wait = {0};

	if (!cluster_trace_tx_identity)
		return;
	if (missed)
		local_owner.pair_misses++;
	else
		local_owner.pair_attempts++;
	if (missed && cluster_content_trace_pair_active(target))
		local_owner.pair[pair_slot].conditional_miss = true;
	wait.buffer = target;
	if (target > 0 && target <= NBuffers) {
		BufferDesc *buf = GetBufferDescriptor(target - 1);
		wait.tag_known = (pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) != 0;
		if (wait.tag_known)
			wait.tag = buf->tag;
	}
	wait.mode = LW_EXCLUSIVE;
	/* Pair is a conditional attempt, not proof that a PCM request was sent. */
	cluster_content_trace_wait_set(CLCONTENT_PAIR, &wait);
}

static void
content_trace_native_proc(uint32 slot, ClusterContentTraceNativeProc *out)
{
	const PGPROC *proc;

	memset(out, 0, sizeof(*out));
	if (ProcGlobal == NULL || slot >= ProcGlobal->allProcCount)
		return;
	proc = &ProcGlobal->allProcs[slot];
	out->available = true;
	out->procno = proc->pgprocno;
	out->pid = *(volatile const int *)&proc->pid;
	out->backend_id = *(volatile const BackendId *)&proc->backendId;
	out->generation = *(volatile const uint64 *)&proc->cluster_grd_generation;
	out->wait_event_info = *(volatile const uint32 *)&proc->wait_event_info;
}

static void content_trace_deadline_send(const char *fmt,...) pg_attribute_printf(1,2);

/* Shared local transport for original DEADLINE and separately identified GCS
 * observations. The caller restores errno and never retries a failed send. */
static bool
content_trace_udp_open(uint32 capture)
{
	deadline_sequence = deadline_dropped = 0;
	deadline_capture = capture;
	deadline_fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (deadline_fd < 0)
		return false;
	if (fcntl(deadline_fd, F_SETFL, O_NONBLOCK) < 0) {
		close(deadline_fd); deadline_fd = -1;
		return false;
	}
	memset(&deadline_address, 0, sizeof(deadline_address));
	deadline_address.sin_family = AF_INET;
	deadline_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	deadline_address.sin_port = htons((uint16)PostPortNumber);
	return true;
}

/* Dedicated local UDP socket, O_NONBLOCK plus MSG_DONTWAIT. Never stdio,
 * logging, a file write or a retry while the original stop lock is held.
 * Receiver must check sequence/END/dropped; missing datagrams are UNKNOWN. */
static void
content_trace_deadline_send(const char *fmt,...)
{
	char row[2048];
	va_list ap;
	int prefix, n;
	prefix = snprintf(row, sizeof(row), "node=%d observer=%d sequence=%u ",
		cluster_node_id, MyProcPid, ++deadline_sequence);
	if (deadline_capture != 0)
		prefix += snprintf(row + prefix, sizeof(row) - prefix, "capture=%u ", deadline_capture);
	va_start(ap, fmt);
	n = vsnprintf(row + prefix, sizeof(row) - prefix, fmt, ap);
	va_end(ap);
	if (n < 0 || n >= (int)sizeof(row) - prefix ||
		sendto(deadline_fd, row, prefix + n, MSG_DONTWAIT,
			(struct sockaddr *)&deadline_address, sizeof(deadline_address)) != prefix + n)
		deadline_dropped++;
}

void
cluster_content_trace_gcs_scan(int worker, int workers, uint64 begin,
	uint64 end, uint64 cap, int64 sampled_at)
{
	if (!cluster_trace_tx_identity || !cluster_trace_gcs_capacity)
		return;
	local_owner.gcs_scan = (ClusterContentTraceGcsScan){
		.active = true, .worker = worker, .workers = workers,
		.begin = begin, .end = end, .cap = cap, .sampled_at = sampled_at
	};
	content_trace_publish(false);
}

static void
content_trace_gcs_scanner(const ClusterContentTraceOwner *owner, void *arg)
{
	const ClusterContentTraceGcsScan *s = &owner->gcs_scan;
	(void)arg;
	if (!s->active)
		return;
	content_trace_deadline_send("PGRAC_WAIT_GCS_SCAN node=%d pid=%d proc=%u backend=%d start=" INT64_FORMAT
		" generation=" UINT64_FORMAT " revision=" UINT64_FORMAT
		" worker=%d workers=%d begin=" UINT64_FORMAT " end=" UINT64_FORMAT
		" cap=" UINT64_FORMAT " sampled_at=" INT64_FORMAT "\n",
		cluster_node_id, owner->pid, owner->procno, owner->backend_id, owner->backend_start,
		owner->proc_generation, owner->revision, s->worker, s->workers,
		s->begin, s->end, s->cap, s->sampled_at);
}

void
cluster_content_trace_gcs_capacity(const ClusterContentTraceGcsCapacity *cut)
{
	ClusterContentTraceWalkStats stats;
	int saved_errno = errno;

	if (!cluster_trace_tx_identity || !cluster_trace_gcs_capacity || MyProc == NULL ||
		cut == NULL || cut->nslots != CLUSTER_CONTENT_TRACE_GCS_SLOTS || gcs_captures >= 8)
		return;
	/* A failed socket/send consumes its bounded capture too. No new wait, file,
	 * SQL connection, or fallback logging on the original capacity-wait path. */
	if (!content_trace_udp_open(++gcs_captures)) {
		errno = saved_errno; return;
	}
	content_trace_deadline_send("PGRAC_WAIT_GCS_BEGIN node=%d pid=%d proc=%u backend=%d start=" INT64_FORMAT
		" generation=" UINT64_FORMAT " identity_valid=%d site=capacity_before_sleep"
		" sampled_at=" INT64_FORMAT " max_backends=%u slots=%u tag=%u/%u/%u/%d/%u transition=%u master=%d atomic_graph=0\n",
		cluster_node_id, MyProcPid, MyProc->pgprocno, MyProc->backendId, MyStartTimestamp,
		MyProc->cluster_grd_generation, MyProc->pid == MyProcPid && MyProc->cluster_grd_generation != 0,
		cut->sampled_at, cut->max_backends, cut->nslots, cut->tag.spcOid, cut->tag.dbOid,
		cut->tag.relNumber, cut->tag.forkNum, cut->tag.blockNum, cut->transition_id, cut->master_node);
	for (uint32 i = 0; i < cut->nslots; i++) {
		const ClusterContentTraceGcsSlot *s = &cut->slots[i];
		const ClusterPcmReadSource *r = &s->source;
		content_trace_deadline_send("PGRAC_WAIT_GCS_SLOT node=%d pid=%d slot=%u in_use=%d request=" UINT64_FORMAT
			" epoch=" UINT64_FORMAT " master=%d expected_master=%d tag=%u/%u/%u/%d/%u transition=%u domain=%u"
			" reply=%d stale=%d revoked=%d source_request=" UINT64_FORMAT " source_epoch=" UINT64_FORMAT
			" source_master=%d source_tag=%u/%u/%u/%d/%u base=" UINT64_FORMAT " token=" UINT64_FORMAT
			" watermark=" UINT64_FORMAT " kind=%d durable=%d quiescent=%d pending_s=%d"
			" retry_at=" INT64_FORMAT " buffer=%d owner_pid=%d incarnation=" UINT64_FORMAT " session=" UINT64_FORMAT
			" armed=%d delivery_closed=%d admitted=%d quiet=%d ack_ready=%d ack_handed=%d ack_consumed=%d"
			" confirm_pending=%d\n",
			cluster_node_id, MyProcPid, i, s->in_use, s->request_id, s->request_epoch,
			s->master_node, s->expected_master_node, s->tag.spcOid, s->tag.dbOid, s->tag.relNumber,
			s->tag.forkNum, s->tag.blockNum, s->transition_id, s->reply_domain,
			s->reply_received, s->stale, s->first_fill_revoked, r->request_id, r->request_epoch,
			r->master_node, r->tag.spcOid, r->tag.dbOid, r->tag.relNumber, r->tag.forkNum, r->tag.blockNum,
			r->base_generation, r->reservation_token, r->watermark, r->kind, r->durable, r->quiescent,
			r->pending_s, s->retry_at, s->buf_id, s->owner_pid,
			s->backend_incarnation, s->master_session, s->armed, s->delivery_closed,
			s->request_admitted, s->quiet, s->ack_ready, s->ack_handed, s->ack_consumed,
			s->confirm_pending);
	}
	/* Existing S2 native PGPROC-generation/revision validation. Missing/racing
	 * scanners are UNKNOWN. This is not an atomic cross-process wait graph. */
	cluster_content_trace_walk(content_trace_gcs_scanner, NULL, &stats);
	content_trace_deadline_send("PGRAC_WAIT_GCS_END node=%d available=%d scanned=%u owners=%u busy=%u changed=%u dead=%u omitted=%u dropped=%u\n",
		cluster_node_id, stats.available, stats.scanned, stats.owners, stats.busy,
		stats.changed, stats.dead, stats.omitted_locks, deadline_dropped);
	close(deadline_fd); deadline_fd = -1;
	errno = saved_errno;
}

static void
content_trace_deadline_owner(const ClusterContentTraceOwner *owner, void *arg)
{
	(void)arg;
	content_trace_deadline_send("PGRAC_WAIT_DEADLINE_OWNER node=%d pid=%d proc=%u generation=" UINT64_FORMAT
		" revision=" UINT64_FORMAT " native_wait=%d native_lifetime=" UINT64_FORMAT
		" native_buffer=%d enqueue_wait=%d enqueue_lifetime=" UINT64_FORMAT
		" service=%d service_lifetime=" UINT64_FORMAT "\n", cluster_node_id,
		owner->pid, owner->procno, owner->proc_generation, owner->revision,
		owner->native_waiting, owner->native_wait.lifetime, owner->native_wait.buffer,
		owner->enqueue_waiting, owner->enqueue.lifetime, owner->pcm_serving,
		owner->service_lifetime);
	for (uint32 i = 0; i < owner->nlocks + (owner->native_waiting ? 1U : 0U); i++) {
		const ClusterContentTraceLock *lock = i < owner->nlocks ? &owner->locks[i] : &owner->native_wait;
		content_trace_deadline_send("PGRAC_WAIT_DEADLINE_CONTENT node=%d pid=%d waiting=%d lifetime=" UINT64_FORMAT
			" buffer=%d mode=%d tag_known=%d tag=%u/%u/%u/%d/%u\n", cluster_node_id,
			owner->pid, i == owner->nlocks, lock->lifetime, lock->buffer, lock->mode,
			lock->tag_known, lock->tag.spcOid, lock->tag.dbOid, lock->tag.relNumber,
			lock->tag.forkNum, lock->tag.blockNum);
	}
	if (owner->enqueue_waiting) {
		const ClusterContentTraceEnqueue *w = &owner->enqueue;
		content_trace_deadline_send("PGRAC_WAIT_DEADLINE_ENQUEUE node=%d pid=%d lifetime=" UINT64_FORMAT
			" resid=%u/%u/%u/%u/%u/%u mode=%d holder=%u/%u/" UINT64_FORMAT "/" UINT64_FORMAT
			" request=" UINT64_FORMAT " epoch=" UINT64_FORMAT " wait_seq=" UINT64_FORMAT "\n",
			cluster_node_id, owner->pid, w->lifetime, w->field1, w->field2, w->field3,
			w->field4, w->type, w->lockmethodid, w->mode, w->holder_node, w->holder_procno,
			w->holder_epoch, w->holder_request, w->request, w->epoch, w->wait_seq);
	}
	if (owner->pcm_serving) {
		const ClusterWaitTrace *w = &owner->pcm_service;
		content_trace_deadline_send("PGRAC_WAIT_DEADLINE_SERVICE node=%d pid=%d lifetime=" UINT64_FORMAT
			" site=%d tag_known=%d tag=%u/%u/%u/%d/%u requester=%d request=" UINT64_FORMAT
			" formation=" UINT64_FORMAT " session=" UINT64_FORMAT " r4=" UINT64_FORMAT "\n",
			cluster_node_id, owner->pid, owner->service_lifetime, w->site, w->tag_known,
			w->tag.spcOid, w->tag.dbOid, w->tag.relNumber, w->tag.forkNum, w->tag.blockNum,
			w->requester, w->request_seq, w->formation, w->master_session, w->r4);
	}
	for (uint32 i = 0; i < owner->ngrants; i++) {
		const ClusterContentTraceGrant *w = &owner->grants[i];
		content_trace_deadline_send("PGRAC_WAIT_DEADLINE_GRANT node=%d pid=%d lifetime=" UINT64_FORMAT
			" resid=%u/%u/%u/%u/%u/%u mode=%d holder=%u/%u/" UINT64_FORMAT "/" UINT64_FORMAT "\n",
			cluster_node_id, owner->pid, w->lifetime, w->field1, w->field2, w->field3, w->field4,
			w->type, w->lockmethodid, w->mode, w->holder_node, w->holder_procno, w->holder_epoch, w->holder_request);
	}
	for (uint32 i = 0; i < CLCONTENT_NWAITS; i++) {
		const ClusterWaitTrace *w = &owner->waits[i];
		if (!(owner->wait_mask & (1U << i))) continue;
		content_trace_deadline_send("PGRAC_WAIT_DEADLINE_TARGET node=%d pid=%d proc=%u kind=%u lifetime=" UINT64_FORMAT
			" site=%u buffer=%d tag_known=%d tag=%u/%u/%u/%d/%u binding=" UINT64_FORMAT
			" request=" UINT64_FORMAT " formation=" UINT64_FORMAT " session=" UINT64_FORMAT "\n",
			cluster_node_id, owner->pid, owner->procno, i, owner->wait_lifetimes[i],
			w->site, w->buffer, w->tag_known, w->tag.spcOid, w->tag.dbOid,
			w->tag.relNumber, w->tag.forkNum, w->tag.blockNum, w->binding,
			w->request_seq, w->formation, w->master_session);
	}
}

void
cluster_content_trace_deadline_dump(uint32 reason)
{
	static bool emitted;
	ClusterContentTraceWalkStats stats;
	int saved_errno = errno;
	if (!cluster_trace_tx_identity || emitted || ProcGlobal == NULL) return;
	emitted = true;
	if (!content_trace_udp_open(0)) {
		errno = saved_errno; return;
	}
	content_trace_deadline_send("PGRAC_WAIT_DEADLINE_BEGIN node=%d observer=%d reason=%u slots=%u termination_receipt=UNKNOWN\n",
		cluster_node_id, MyProcPid, reason, ProcGlobal->allProcCount);
	for (uint32 i = 0; i < ProcGlobal->allProcCount; i++) {
		ClusterContentTraceNativeProc a, b;
		content_trace_native_proc(i, &a);
		pg_read_barrier();
		content_trace_native_proc(i, &b);
		if (a.pid == 0 && b.pid == 0) continue;
		content_trace_deadline_send("PGRAC_WAIT_DEADLINE_PROC node=%d slot=%u pid=%d backend=%d generation=" UINT64_FORMAT
			" wait=%u after_pid=%d after_backend=%d after_generation=" UINT64_FORMAT
			" after_wait=%u stable=%d\n", cluster_node_id, i, a.pid, a.backend_id,
			a.generation, a.wait_event_info, b.pid, b.backend_id, b.generation, b.wait_event_info,
			a.procno == i && b.procno == i && a.pid == b.pid && a.backend_id == b.backend_id &&
			a.generation != 0 && a.generation == b.generation && a.wait_event_info == b.wait_event_info);
	}
	/* Nonwaiting busy-CAS copies. Unavailable/racing publishers are counted,
	 * not treated as evidence that a native live process has no resource. */
	cluster_content_trace_walk(content_trace_deadline_owner, NULL, &stats);
	content_trace_deadline_send("PGRAC_WAIT_DEADLINE_END node=%d available=%d scanned=%u owners=%u busy=%u changed=%u dead=%u omitted=%u dropped=%u\n",
		cluster_node_id, stats.available, stats.scanned, stats.owners, stats.busy,
		stats.changed, stats.dead, stats.omitted_locks, deadline_dropped);
	close(deadline_fd); deadline_fd = -1;
	errno = saved_errno;
}

static void
content_trace_reject(uint32 slot, ClusterContentTraceRejectReason reason,
	const ClusterContentTraceOwner *owner, uint64 revision,
	ClusterContentTraceRejectVisitor visitor, void *context)
{
	ClusterContentTraceRejection row = {.slot = slot, .reason = reason};

	if (visitor == NULL)
		return;
	row.slot_revision = revision;
	if (owner != NULL) {
		row.payload_copied = true;
		row.payload_pid = owner->pid;
		row.payload_procno = owner->procno;
		row.payload_backend_id = owner->backend_id;
		row.payload_start = owner->backend_start;
		row.payload_generation = owner->proc_generation;
		row.payload_revision = owner->revision;
		row.payload_nlocks = owner->nlocks;
		row.payload_ngrants = owner->ngrants;
		row.payload_wait_mask = owner->wait_mask;
		row.payload_native_waiting = owner->native_waiting;
		row.payload_enqueue_waiting = owner->enqueue_waiting;
		row.payload_pcm_serving = owner->pcm_serving;
	}
	/* Read-only cuts after rejecting the source. A concurrent allocation or
	 * publication may change these, so export both and retain UNKNOWN. */
	content_trace_native_proc(slot, &row.native_before);
	pg_read_barrier();
	content_trace_native_proc(slot, &row.native_after);
	row.final_revision = pg_atomic_read_u64(&content_trace->slots[slot].revision);
	visitor(&row, context);
}

void
cluster_content_trace_walk_rejections(ClusterContentTraceVisitor visitor, void *context,
	ClusterContentTraceRejectVisitor rejected, void *rejected_context,
	ClusterContentTraceWalkStats *stats)
{
	memset(stats, 0, sizeof(*stats));
#ifdef EXEC_BACKEND
	/* This build attaches the diagnostic mapping after native PID publication.
	 * Pre-PID incarnation invalidation is unproved, so return UNKNOWN. */
	return;
#endif
	if (!cluster_trace_tx_identity || content_trace == NULL ||
		ProcGlobal == NULL || visitor == NULL)
		return;
	stats->available = true;
	for (uint32 i = 0; i < content_trace->nslots; i++) {
		ContentTraceSlot *slot = &content_trace->slots[i];
		ClusterContentTraceOwner owner;
		uint32 expected = 0;
		uint64 revision;

		stats->scanned++;
		if (!pg_atomic_compare_exchange_u32(&slot->busy, &expected, 1)) {
			stats->busy++;
			if (rejected != NULL)
				content_trace_reject(i, CLCONTENT_REJECT_BUSY, NULL,
					pg_atomic_read_u64(&slot->revision), rejected, rejected_context);
			continue;
		}
		owner = slot->owner;
		revision = pg_atomic_read_u64(&slot->revision);
		/* Finish the owned copy before a publisher may overwrite its source. */
		(void)pg_atomic_exchange_u32(&slot->busy, 0);
		if (owner.revision != revision) {
			stats->changed++;
			content_trace_reject(i, CLCONTENT_REJECT_COPY_REVISION, &owner,
				revision, rejected, rejected_context);
			continue;
		}
		if (owner.pid == 0)
			continue;
		/* Native generation also identifies auxiliaries without a backendId.
		 * Zero/uninitialized or reused native identities remain UNKNOWN. */
		if (i >= ProcGlobal->allProcCount ||
			(owner.backend_id <= 0 && owner.backend_id != InvalidBackendId) ||
			owner.proc_generation == 0 || owner.procno != i ||
			ProcGlobal->allProcs[i].pgprocno != i ||
			ProcGlobal->allProcs[i].cluster_grd_generation != owner.proc_generation ||
			ProcGlobal->allProcs[i].pid != owner.pid ||
			ProcGlobal->allProcs[i].backendId != owner.backend_id) {
			stats->dead++;
			content_trace_reject(i, CLCONTENT_REJECT_IDENTITY, &owner,
				revision, rejected, rejected_context);
			continue;
		}
		/* Process allocation invalidates before publishing its native identity.
		 * Recheck after the census reads so a simultaneous reuse stays UNKNOWN. */
		pg_read_barrier();
		if (revision != pg_atomic_read_u64(&slot->revision)) {
			stats->changed++;
			content_trace_reject(i, CLCONTENT_REJECT_CENSUS_REVISION, &owner,
				revision, rejected, rejected_context);
			continue;
		}
		stats->omitted_locks += owner.omitted_locks;
		for (uint32 j = 0; j < owner.nlocks; j++) {
			if (!owner.locks[j].tag_known)
				stats->omitted_locks++;
		}
		stats->owners++;
		visitor(&owner, context);
	}
}

void
cluster_content_trace_walk(ClusterContentTraceVisitor visitor, void *context,
	ClusterContentTraceWalkStats *stats)
{
	cluster_content_trace_walk_rejections(visitor, context, NULL, NULL, stats);
}

typedef struct ContentTraceCapture {
	const BufferTag *tag;
	ClusterContentTraceCut *cut;
} ContentTraceCapture;

static void
content_trace_capture_owner(const ClusterContentTraceOwner *owner, void *context)
{
	ContentTraceCapture *capture = context;
	ClusterContentTraceCut *cut = capture->cut;
	bool matches = capture->tag == NULL;

	for (uint32 j = 0; !matches && j < owner->nlocks; j++) {
		if (owner->locks[j].tag_known &&
			content_trace_tag_equal(capture->tag, &owner->locks[j].tag))
			matches = true;
	}
	if (!matches)
		return;
	cut->matches++;
	if (cut->nowners == CLUSTER_CONTENT_TRACE_OWNERS)
		cut->omitted_owners++;
	else
		cut->owners[cut->nowners++] = *owner;
}

void
cluster_content_trace_capture(const BufferTag *tag, ClusterContentTraceCut *cut)
{
	ContentTraceCapture capture = {.tag = tag, .cut = cut};
	ClusterContentTraceWalkStats stats;

	memset(cut, 0, sizeof(*cut));
	cluster_content_trace_walk(content_trace_capture_owner, &capture, &stats);
	cut->scanned = stats.scanned;
	cut->busy = stats.busy;
	cut->changed = stats.changed;
	cut->dead = stats.dead;
	cut->omitted_locks = stats.omitted_locks;
}

static bool
content_trace_log_budget(void)
{
	if (emitted_rows >= CLUSTER_WAIT_TRACE_LIMIT) {
		if (emitted_rows++ == CLUSTER_WAIT_TRACE_LIMIT)
			elog(LOG, "CONTENT_OWNER event=LIMIT node=%d pid=%d evidence_complete=0", cluster_node_id, MyProcPid);
		/* Saturate: repeated omitted rows must never wrap the budget. */
		emitted_rows = CLUSTER_WAIT_TRACE_LIMIT + 1;
		return false;
	}
	emitted_rows++;
	return true;
}

/* Only consume the owned cut here, after the caller's original cleanup. All
 * producer hooks above stay allocation/LOG/wait free, including ERROR unwind.
 * A cycle is a local observation; a dispatch is not a completed current move. */
static bool
content_trace_pair_emit(const ClusterWaitTrace *guard,
	const ClusterContentTraceOwner *owner)
{
	static const char *const event_names[CLPAIR_NEVENTS] = {
		"TARGET_BEGIN", "TARGET_RESULT", "ROUND_STEP", "CACHED_X_SAMPLE",
		"CONTENT_BUSY", "PCM_BLOCKED", "OBJECT_BLOCKED", "PREPARE_DRIFT",
		"ACTIVATE_WAIT", "ACTIVATE_DRIFT", "ACTIVATE_OBJECT", "DISPATCH",
		"STAGE_REJECT", "REMOTE_MASTER_DISPATCH"
	};

	for (uint32 i = 0; i < owner->npair_cycles; i++) {
		const ClusterContentTracePairCycleData *cycle = &owner->pair[i];

		if (!content_trace_log_budget()) return false;
		elog(LOG, "CONTENT_OWNER event=PAIR_CYCLE node=%d guard_snapshot=" UINT64_FORMAT
			 " owner_pid=%d owner_proc=%u backend_id=%d backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
			 " invocation=" UINT64_FORMAT " cycle=" UINT64_FORMAT " phase=%u finished=%d error=%d conditional_miss=%d"
			 " cycles_total=" UINT64_FORMAT " omitted_cycles=" UINT64_FORMAT " nested_omitted=" UINT64_FORMAT
			 " requests=%u omitted_requests=%u atomic_graph=0 authority=0 current_transfer_known=0",
			 cluster_node_id, guard->snapshot, owner->pid, owner->procno,
			 owner->backend_id, owner->backend_start, owner->revision,
			 cycle->invocation, cycle->cycle, cycle->phase, cycle->finished,
			 cycle->error, cycle->conditional_miss, owner->pair_cycles,
			 owner->pair_omitted, owner->pair_nested_omitted,
			 cycle->nrequests, cycle->omitted_requests);
		for (unsigned leg = 0; leg < 2; leg++) {
			const ClusterContentTraceLock *target = &cycle->targets[leg];

			if (!content_trace_log_budget()) return false;
			elog(LOG, "CONTENT_OWNER event=PAIR_LEG node=%d guard_snapshot=" UINT64_FORMAT
				 " owner_pid=%d owner_proc=%u backend_id=%d backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
				 " invocation=" UINT64_FORMAT " cycle=" UINT64_FORMAT " leg=%u buffer=%d mode=%d"
				 " tag_known=%d spc=%u db=%u rel=%u fork=%d block=%u",
				 cluster_node_id, guard->snapshot, owner->pid, owner->procno,
				 owner->backend_id, owner->backend_start, owner->revision,
				 cycle->invocation, cycle->cycle, leg, target->buffer, target->mode,
				 target->tag_known, target->tag.spcOid, target->tag.dbOid,
				 target->tag.relNumber, (int)target->tag.forkNum, target->tag.blockNum);
			for (unsigned event = 0; event < CLPAIR_NEVENTS; event++) {
				if (cycle->events[leg][event] == 0) continue;
				if (!content_trace_log_budget()) return false;
				elog(LOG, "CONTENT_OWNER event=PAIR_EVENT node=%d guard_snapshot=" UINT64_FORMAT
					 " owner_pid=%d owner_proc=%u backend_id=%d backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
					 " invocation=" UINT64_FORMAT " cycle=" UINT64_FORMAT " leg=%u reason=%s"
					 " count=" UINT64_FORMAT " last_result=%d",
					 cluster_node_id, guard->snapshot, owner->pid, owner->procno,
					 owner->backend_id, owner->backend_start, owner->revision,
					 cycle->invocation, cycle->cycle, leg, event_names[event],
					 cycle->events[leg][event], cycle->last_results[leg][event]);
			}
		}
		for (uint32 j = 0; j < cycle->nrequests; j++) {
			const ClusterContentTracePairRequest *request = &cycle->requests[j];

			if (!content_trace_log_budget()) return false;
			elog(LOG, "CONTENT_OWNER event=PAIR_REQUEST node=%d guard_snapshot=" UINT64_FORMAT
				 " owner_pid=%d owner_proc=%u backend_id=%d backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
				 " invocation=" UINT64_FORMAT " cycle=" UINT64_FORMAT " leg=%u phase=%u"
				 " spc=%u db=%u rel=%u fork=%d block=%u physical_generation=" UINT64_FORMAT
				 " requester=%d master=%d r4=" UINT64_FORMAT " formation=" UINT64_FORMAT
				 " session=" UINT64_FORMAT " request=" UINT64_FORMAT " base=" UINT64_FORMAT
				 " authority_epoch=" UINT64_FORMAT " requester_connection=" UINT64_FORMAT
				 " ingress_connection=" UINT64_FORMAT " wire_sender_connection=" UINT64_FORMAT
				 " action=%d action_node=%d ordered_lane=%u observed_mode=%u target_mode=%u"
				 " source_candidate=%u retain_pi_if_dirty=%u outcome=%u flags=%u stage_ok=%d"
				 " head_binding_known=0 current_transfer_known=0 atomic_graph=0 authority=0",
				 cluster_node_id, guard->snapshot, owner->pid, owner->procno,
				 owner->backend_id, owner->backend_start, owner->revision,
				 cycle->invocation, cycle->cycle, request->leg, request->phase,
				 request->tag.spcOid, request->tag.dbOid, request->tag.relNumber,
				 (int)request->tag.forkNum, request->tag.blockNum,
				 request->physical_generation, request->requester, request->master,
				 request->r4, request->formation, request->master_session,
				 request->request, request->base, request->authority,
				 request->requester_connection, request->ingress_connection,
				 request->wire_sender_connection, request->action, request->action_node,
				 request->ordered_lane, request->observed_mode, request->target_mode,
				 request->source_candidate, request->retain_pi_if_dirty,
				 request->outcome, request->flags, request->stage_ok);
		}
	}
	return true;
}

void
cluster_content_trace_emit(const ClusterWaitTrace *guard, const ClusterContentTraceCut *cut)
{
	if (!cluster_trace_tx_identity || guard == NULL || cut == NULL || guard->snapshot == 0)
		return;
	if (!content_trace_log_budget())
		return;
	elog(LOG, "CONTENT_OWNER event=CUT node=%d pid=%d guard_snapshot=" UINT64_FORMAT
		 " spc=%u db=%u rel=%u fork=%d block=%u scanned=%u busy=%u changed=%u dead=%u"
		 " matches=%u owners=%u omitted_owners=%u omitted_locks=%u atomic_graph=0 authority=0",
		 cluster_node_id, MyProcPid, guard->snapshot, guard->tag.spcOid, guard->tag.dbOid,
		 guard->tag.relNumber, (int)guard->tag.forkNum, guard->tag.blockNum,
		 cut->scanned, cut->busy, cut->changed, cut->dead, cut->matches, cut->nowners,
		 cut->omitted_owners, cut->omitted_locks);
	for (uint32 i = 0; i < cut->nowners; i++) {
		const ClusterContentTraceOwner *owner = &cut->owners[i];

		for (uint32 j = 0; j < owner->nlocks; j++) {
			const ClusterContentTraceLock *lock = &owner->locks[j];
			if (!content_trace_log_budget()) return;
			elog(LOG, "CONTENT_OWNER event=LOCK node=%d guard_snapshot=" UINT64_FORMAT
				 " owner_pid=%d owner_proc=%u backend_id=%d backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
				 " wait_mask=%u waits_with_locks=" UINT64_FORMAT " pair_attempts=" UINT64_FORMAT " pair_misses=" UINT64_FORMAT
				 " held=%u omitted=%u buffer=%d mode=%d tag_known=%d spc=%u db=%u rel=%u fork=%d block=%u",
				 cluster_node_id, guard->snapshot, owner->pid, owner->procno, owner->backend_id, owner->backend_start, owner->revision,
				 owner->wait_mask, owner->waits_with_locks, owner->pair_attempts, owner->pair_misses,
				 owner->nlocks, owner->omitted_locks, lock->buffer, lock->mode, lock->tag_known,
				 lock->tag.spcOid, lock->tag.dbOid, lock->tag.relNumber, (int)lock->tag.forkNum, lock->tag.blockNum);
		}
		for (unsigned k = 0; k < CLCONTENT_NWAITS; k++) {
			const ClusterWaitTrace *wait = &owner->waits[k];
			if ((owner->wait_mask & (1U << k)) == 0) continue;
			if (!content_trace_log_budget()) return;
			elog(LOG, "CONTENT_OWNER event=WAIT node=%d guard_snapshot=" UINT64_FORMAT
				 " owner_pid=%d owner_proc=%u backend_id=%d backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
				 " kind=%u site=%d mode=%d wait_seq=" UINT64_FORMAT " xid=%u tag_known=%d spc=%u db=%u rel=%u fork=%d block=%u"
				 " request_known=%d transport_known=%d requester=%d master=%d request=" UINT64_FORMAT
				 " binding=" UINT64_FORMAT " formation=" UINT64_FORMAT " session=" UINT64_FORMAT
				 " r4=" UINT64_FORMAT " acquisition=" UINT64_FORMAT " base=" UINT64_FORMAT " authority_epoch=" UINT64_FORMAT
				 " requester_connection=" UINT64_FORMAT " ingress_connection=" UINT64_FORMAT " wire_sender_connection=" UINT64_FORMAT,
				 cluster_node_id, guard->snapshot, owner->pid, owner->procno, owner->backend_id, owner->backend_start, owner->revision,
				 k, wait->site, wait->mode, wait->wait_seq, wait->tx_xid, wait->tag_known,
				 wait->tag.spcOid, wait->tag.dbOid, wait->tag.relNumber, (int)wait->tag.forkNum, wait->tag.blockNum,
				 wait->request_known, wait->transport_known, wait->requester, wait->expected_master, wait->request_seq,
				 wait->binding, wait->formation, wait->master_session, wait->r4, wait->acquisition, wait->base,
				 wait->authority, wait->requester_connection, wait->ingress_connection, wait->wire_sender_connection);
		}
		if (!content_trace_pair_emit(guard, owner)) return;
	}
}
#endif
