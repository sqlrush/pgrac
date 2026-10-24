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
	/* Invalidate a prior incarnation even if its PID/backendId are reused.
	 * Never take busy away from a reader or a previous, interrupted publisher. */
	if (content_trace != NULL && procno < content_trace->nslots)
		(void)pg_atomic_fetch_add_u64(&content_trace->slots[procno].revision, 1);
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

	if (content_trace == NULL || MyProc == NULL ||
		MyProc->pgprocno >= content_trace->nslots)
		return;
	if (!cluster_trace_tx_identity && local_owner.pid == 0)
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
	if (BufferDescriptorFromContentLock(lock) != NULL)
		content_trace_publish(false);
}

/* Invalidate before native ownership changes. A delayed post-release
 * publication must not leave the previous owner looking current. */
void
cluster_content_trace_lock_invalidating(LWLock *lock)
{
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
	content_trace_publish(false);
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
	local_owner.native_waiting = local_owner.enqueue_waiting = false;
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

void
cluster_content_trace_walk(ClusterContentTraceVisitor visitor, void *context,
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
			continue;
		}
		owner = slot->owner;
		revision = pg_atomic_read_u64(&slot->revision);
		/* Finish the owned copy before a publisher may overwrite its source. */
		(void)pg_atomic_exchange_u32(&slot->busy, 0);
		if (owner.revision != revision) {
			stats->changed++;
			continue;
		}
		if (owner.pid == 0)
			continue;
		/* PID alone is not an incarnation key: reject a reused proc slot or
		 * an auxiliary owner without a stable backend identity as UNKNOWN. */
		if (i >= ProcGlobal->allProcCount ||
			owner.backend_id <= 0 ||
			ProcGlobal->allProcs[i].pid != owner.pid ||
			ProcGlobal->allProcs[i].backendId != owner.backend_id) {
			stats->dead++;
			continue;
		}
		/* Process allocation invalidates before publishing its native identity.
		 * Recheck after the census reads so a simultaneous reuse stays UNKNOWN. */
		pg_read_barrier();
		if (revision != pg_atomic_read_u64(&slot->revision)) {
			stats->changed++;
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
