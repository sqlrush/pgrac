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
	local_owner.wait_mask = 0;
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
cluster_content_trace_capture(const BufferTag *tag, ClusterContentTraceCut *cut)
{
	memset(cut, 0, sizeof(*cut));
	if (!cluster_trace_tx_identity || tag == NULL || content_trace == NULL || ProcGlobal == NULL)
		return;
	for (uint32 i = 0; i < content_trace->nslots; i++) {
		ContentTraceSlot *slot = &content_trace->slots[i];
		ClusterContentTraceOwner owner;
		uint32 expected = 0;
		uint64 revision;
		bool matches = false;

		cut->scanned++;
		if (!pg_atomic_compare_exchange_u32(&slot->busy, &expected, 1)) {
			cut->busy++;
			continue;
		}
		owner = slot->owner;
		revision = pg_atomic_read_u64(&slot->revision);
		/* Finish the owned copy before a publisher may overwrite its source. */
		(void)pg_atomic_exchange_u32(&slot->busy, 0);
		if (owner.revision != revision) {
			cut->changed++;
			continue;
		}
		if (owner.pid == 0)
			continue;
		if (i >= ProcGlobal->allProcCount || ProcGlobal->allProcs[i].pid != owner.pid) {
			cut->dead++;
			continue;
		}
		cut->omitted_locks += owner.omitted_locks;
		for (uint32 j = 0; j < owner.nlocks; j++) {
			if (!owner.locks[j].tag_known)
				cut->omitted_locks++;
			else if (content_trace_tag_equal(tag, &owner.locks[j].tag))
				matches = true;
		}
		if (!matches)
			continue;
		cut->matches++;
		if (cut->nowners == CLUSTER_CONTENT_TRACE_OWNERS)
			cut->omitted_owners++;
		else
			cut->owners[cut->nowners++] = owner;
	}
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
				 " owner_pid=%d owner_proc=%u backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
				 " wait_mask=%u waits_with_locks=" UINT64_FORMAT " pair_attempts=" UINT64_FORMAT " pair_misses=" UINT64_FORMAT
				 " held=%u omitted=%u buffer=%d mode=%d tag_known=%d spc=%u db=%u rel=%u fork=%d block=%u",
				 cluster_node_id, guard->snapshot, owner->pid, owner->procno, owner->backend_start, owner->revision,
				 owner->wait_mask, owner->waits_with_locks, owner->pair_attempts, owner->pair_misses,
				 owner->nlocks, owner->omitted_locks, lock->buffer, lock->mode, lock->tag_known,
				 lock->tag.spcOid, lock->tag.dbOid, lock->tag.relNumber, (int)lock->tag.forkNum, lock->tag.blockNum);
		}
		for (unsigned k = 0; k < CLCONTENT_NWAITS; k++) {
			const ClusterWaitTrace *wait = &owner->waits[k];
			if ((owner->wait_mask & (1U << k)) == 0) continue;
			if (!content_trace_log_budget()) return;
			elog(LOG, "CONTENT_OWNER event=WAIT node=%d guard_snapshot=" UINT64_FORMAT
				 " owner_pid=%d owner_proc=%u backend_start=" INT64_FORMAT " revision=" UINT64_FORMAT
				 " kind=%u site=%d mode=%d wait_seq=" UINT64_FORMAT " xid=%u tag_known=%d spc=%u db=%u rel=%u fork=%d block=%u"
				 " request_known=%d transport_known=%d requester=%d master=%d request=" UINT64_FORMAT
				 " binding=" UINT64_FORMAT " formation=" UINT64_FORMAT " session=" UINT64_FORMAT
				 " r4=" UINT64_FORMAT " acquisition=" UINT64_FORMAT " base=" UINT64_FORMAT " authority_epoch=" UINT64_FORMAT
				 " requester_connection=" UINT64_FORMAT " ingress_connection=" UINT64_FORMAT " wire_sender_connection=" UINT64_FORMAT,
				 cluster_node_id, guard->snapshot, owner->pid, owner->procno, owner->backend_start, owner->revision,
				 k, wait->site, wait->mode, wait->wait_seq, wait->tx_xid, wait->tag_known,
				 wait->tag.spcOid, wait->tag.dbOid, wait->tag.relNumber, (int)wait->tag.forkNum, wait->tag.blockNum,
				 wait->request_known, wait->transport_known, wait->requester, wait->expected_master, wait->request_seq,
				 wait->binding, wait->formation, wait->master_session, wait->r4, wait->acquisition, wait->base,
				 wait->authority, wait->requester_connection, wait->ingress_connection, wait->wire_sender_connection);
		}
	}
}
#endif
