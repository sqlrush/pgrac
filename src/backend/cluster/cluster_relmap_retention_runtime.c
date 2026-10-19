/* PGRAC: private shared relmap retention coordination.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#define RELMAP_RETENTION_RUNTIME_NAME "pgrac relmap retention"
#define RELMAP_RETENTION_RUNTIME_MAGIC 0x52525432U
#define RELMAP_RETENTION_RUNTIME_VERSION 2

typedef struct RelmapRetentionRuntimeShmem {
	uint32 magic;
	uint32 version;
	Size bytes;
	int tranche;
	/* All origins, begin, ACTIVE, future release and truncate/allocation guards
	 * must share this one lock. XidGenLock precedes it; no native owner call,
	 * SLRU acquisition or I/O is allowed while it is held. */
	LWLock lock;
	ClusterRelmapRetentionDirectory directories[CLUSTER_MAX_NODES];
} RelmapRetentionRuntimeShmem;

static RelmapRetentionRuntimeShmem *relmap_runtime_shmem;

static Size
relmap_runtime_shmem_size(void)
{
	return MAXALIGN(sizeof(RelmapRetentionRuntimeShmem));
}

static void
relmap_runtime_shmem_init(void)
{
	RelmapRetentionRuntimeShmem *shared;
	bool found;

	/* Never retain a previously usable process-local pointer on refusal. */
	relmap_runtime_shmem = NULL;
	shared = ShmemInitStruct(RELMAP_RETENTION_RUNTIME_NAME, relmap_runtime_shmem_size(), &found);
	if (!found) {
		memset(shared, 0, relmap_runtime_shmem_size());
		shared->magic = RELMAP_RETENTION_RUNTIME_MAGIC;
		shared->version = RELMAP_RETENTION_RUNTIME_VERSION;
		shared->bytes = relmap_runtime_shmem_size();
		shared->tranche = LWLockNewTrancheId();
		LWLockInitialize(&shared->lock, shared->tranche);
		/* Zero is UNKNOWN, never evidence of an empty qualified origin. There
		 * is deliberately no ordinary-backend COMPLETE publication interface. */
	} else if (shared->magic != RELMAP_RETENTION_RUNTIME_MAGIC
			   || shared->version != RELMAP_RETENTION_RUNTIME_VERSION
			   || shared->bytes != relmap_runtime_shmem_size() || shared->tranche <= 0
			   || shared->tranche != shared->lock.tranche)
		ereport(FATAL, (errmsg("incompatible shared relmap retention state")));

	/* EXEC_BACKEND attaches the existing lock/tranche without resetting it or
	 * touching any origin's UNKNOWN/BUILDING/COMPLETE state, pins or inflight. */
	LWLockRegisterTranche(shared->tranche, "ClusterRelmapRetention");
	relmap_runtime_shmem = shared;
}

void
cluster_relmap_retention_runtime_shmem_register(void)
{
	static const ClusterShmemRegion region = {
		.name = RELMAP_RETENTION_RUNTIME_NAME,
		.size_fn = relmap_runtime_shmem_size,
		.init_fn = relmap_runtime_shmem_init,
		.lwlock_count = 1,
		.owner_subsys = "cluster_relmap",
		.reserved_flags = 0,
	};

	cluster_shmem_register_region(&region);
}

bool
cluster_relmap_retention_runtime_recovery_invalidate_v2(void)
{
	if (!AmStartupProcess() || relmap_runtime_shmem == NULL || CritSectionCount != 0
		|| ShutdownRequestPending || LWLockHeldByMe(RelationMappingLock)
		|| LWLockHeldByMe(XidGenLock) || LWLockHeldByMe(&relmap_runtime_shmem->lock))
		return false;

	/* Revoke the old recovery generation as one cut before observing any new
	 * source set. Invalidation preserves obligations and inflight native cuts;
	 * it neither rebuilds a directory nor authorizes a destructive consumer. */
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin++)
		cluster_relmap_retention_invalidate(&relmap_runtime_shmem->directories[origin]);
	LWLockRelease(&relmap_runtime_shmem->lock);
	return true;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_begin_v2(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRelmapV2Namespace *identity, uint64 generation)
{
	if (sample == NULL || identity == NULL || sample->owner.node >= CLUSTER_MAX_NODES)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (relmap_runtime_shmem == NULL)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	/* The original adapter rechecks provenance/namespace and samples the real
	 * published TT receipt outside the lock. Only its pure core runs inside. */
	return cluster_relmap_owner_retention_begin_v2(sample, identity, generation,
		&relmap_runtime_shmem->directories[sample->owner.node], &relmap_runtime_shmem->lock);
}

static ClusterRelmapRetentionResult
relmap_runtime_producer_key(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRelmapV2Namespace *identity, uint64 generation, ClusterRelmapRetentionKey *key)
{
	if (sample == NULL || identity == NULL || generation == 0
		|| sample->owner.node >= CLUSTER_MAX_NODES)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (relmap_runtime_shmem == NULL || LWLockHeldByMe(&relmap_runtime_shmem->lock)
		|| !cluster_relmap_owner_matches_v2(sample))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (identity->system_identifier != sample->writer.ref.claim.identity.system_identifier
		|| identity->database_incarnation != sample->writer.ref.claim.database_incarnation
		|| (identity->shared ? identity->dbid != InvalidOid
			: !OidIsValid(MyDatabaseId) || identity->dbid != MyDatabaseId))
		return CLUSTER_RELMAP_RETENTION_INVALID;
	key->identity = *identity;
	key->generation = generation;
	key->origin = sample->owner.node;
	key->top_full_xid = sample->owner.top_full_xid;
	key->membership_epoch = sample->owner.membership_epoch;
	key->admitted_incarnation = sample->owner.admitted_incarnation;
	key->boot_incarnation = sample->owner.boot_incarnation;
	return CLUSTER_RELMAP_RETENTION_OK;
}

/* Caller holds the one runtime lock. This observation alone is never stage
 * permission: the storage writer must still mark the exact pin under it. */
static ClusterRelmapRetentionResult
relmap_runtime_directory_matches(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionKey *key)
{
	if (directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (directory->origin != key->origin
		|| directory->system_identifier != key->identity.system_identifier
		|| directory->database_incarnation != key->identity.database_incarnation)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	return CLUSTER_RELMAP_RETENTION_OK;
}

bool
cluster_relmap_retention_runtime_write_pending_v2(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRelmapV2Namespace *identity, uint64 generation, XLogRecPtr wal_end,
	const RelMapFile *pending)
{
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapV2Owner owner;
	ClusterRelmapRetentionResult result;

	if (pending == NULL || wal_end == InvalidXLogRecPtr
		|| relmap_runtime_producer_key(sample, identity, generation, &key) != CLUSTER_RELMAP_RETENTION_OK)
		return false;
	directory = &relmap_runtime_shmem->directories[key.origin];
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	result = relmap_runtime_directory_matches(directory, &key);
	LWLockRelease(&relmap_runtime_shmem->lock);
	if (result != CLUSTER_RELMAP_RETENTION_OK)
		return false;
	owner = sample->owner;
	owner.wal_end = wal_end;
	/* Never cancel here, including a false return before the caller arms its
	 * ordinary cleanup. The storage writer owns the pre-write stage marker. */
	return cluster_relmap_authority_v2_write_pending(identity, generation, &owner, pending,
		directory, &relmap_runtime_shmem->lock) && cluster_relmap_owner_matches_v2(sample);
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_cancel_prepare_v2(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRelmapV2Namespace *identity, uint64 generation)
{
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionResult result;

	result = relmap_runtime_producer_key(sample, identity, generation, &key);
	if (result != CLUSTER_RELMAP_RETENTION_OK)
		return result;
	directory = &relmap_runtime_shmem->directories[key.origin];
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	result = relmap_runtime_directory_matches(directory, &key);
	if (result == CLUSTER_RELMAP_RETENTION_OK)
		result = cluster_relmap_retention_cancel_prepare(directory, &key);
	LWLockRelease(&relmap_runtime_shmem->lock);
	return result;
}

static bool
relmap_runtime_binding_context(void)
{
	return cluster_enabled && cluster_shared_config
		&& cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES
		&& MyBackendType == B_BACKEND && MyProcPid > 0 && CritSectionCount == 0
		&& !ShutdownRequestPending && IsTransactionState() && !RecoveryInProgress();
}

/*
 * Attach only the original ACTIVE owner's existing receipt. No assignment,
 * publication or owner query occurs inside the retention lock. The global
 * relmap X lock is unnecessary: this can only strengthen existing pins.
 * Refusal returns without cancelling ACTIVE or any retention responsibility.
 */
ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_bind_current_v2(void)
{
	ClusterWalWriterToken writer;
	ClusterCanonicalTxnBinding binding;
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionResult result;
	FullTransactionId top;
	int origin, pid;

	if (relmap_runtime_shmem == NULL || !relmap_runtime_binding_context()
		|| LWLockHeldByMe(RelationMappingLock) || LWLockHeldByMe(&relmap_runtime_shmem->lock))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	origin = cluster_node_id;
	pid = MyProcPid;
	top = GetTopFullTransactionIdIfAny();
	if (!FullTransactionIdIsValid(top) || !TransactionIdIsNormal(XidFromFullTransactionId(top))
		|| cluster_wal_writer_begin(GetWALInsertionTimeLine(), &writer)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !cluster_tt_local_get_published_binding(XidFromFullTransactionId(top), &binding))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (!relmap_runtime_binding_context() || cluster_node_id != origin || MyProcPid != pid
		|| U64FromFullTransactionId(GetTopFullTransactionIdIfAny()) != U64FromFullTransactionId(top)
		|| GetWALInsertionTimeLine() != writer.ref.timeline
		|| cluster_wal_writer_check(&writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;

	directory = &relmap_runtime_shmem->directories[origin];
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	if (directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		result = CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	else if (directory->origin != origin
		|| directory->system_identifier != writer.ref.claim.identity.system_identifier
		|| directory->database_incarnation != writer.ref.claim.database_incarnation)
		result = CLUSTER_RELMAP_RETENTION_INVALID;
	else
		result = cluster_relmap_retention_bind(directory, U64FromFullTransactionId(top), &binding);
	LWLockRelease(&relmap_runtime_shmem->lock);
	return result;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_tt_release_check_v2(uint64 system_identifier,
	uint64 database_incarnation, const ClusterCanonicalTxnBinding *binding)
{
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionResult result;
	int origin;

	if (system_identifier == 0 || database_incarnation == 0 || binding == NULL
		|| binding->origin_instance == 0 || binding->origin_instance > CLUSTER_MAX_NODES)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (relmap_runtime_shmem == NULL || LWLockHeldByMe(&relmap_runtime_shmem->lock))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	origin = binding->origin_instance - 1;
	directory = &relmap_runtime_shmem->directories[origin];
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	if (directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		result = CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	else if (directory->origin != origin || directory->system_identifier != system_identifier
		|| directory->database_incarnation != database_incarnation)
		result = CLUSTER_RELMAP_RETENTION_INVALID;
	else
		result = cluster_relmap_retention_tt_release_check(directory, binding);
	LWLockRelease(&relmap_runtime_shmem->lock);
	return result;
}

/* Caller holds the sole runtime lock; no ordinary owner can publish COMPLETE
 * through these adapters. The receipt identifies the original directory even
 * when this backend's current node differs from the original owner's origin. */
static ClusterRelmapRetentionResult
relmap_runtime_origin_matches(const ClusterRelmapRetentionDirectory *directory,
	uint64 system_identifier, uint64 database_incarnation, uint16 origin)
{
	if (directory->state != CLUSTER_RELMAP_RETENTION_COMPLETE)
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (directory->origin != origin || directory->system_identifier != system_identifier
		|| directory->database_incarnation != database_incarnation)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	return CLUSTER_RELMAP_RETENTION_OK;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_truncate_begin_v2(uint64 system_identifier,
	uint64 database_incarnation, uint16 origin, uint64 requested_cutoff,
	ClusterRelmapRetentionRuntimeTruncateV2 *reservation)
{
	ClusterRelmapRetentionRuntimeTruncateV2 prepared = { 0 };
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionResult result;

	if (system_identifier == 0 || database_incarnation == 0 || origin >= CLUSTER_MAX_NODES
		|| reservation == NULL)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (relmap_runtime_shmem == NULL || LWLockHeldByMe(&relmap_runtime_shmem->lock))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	directory = &relmap_runtime_shmem->directories[origin];
	prepared.system_identifier = system_identifier;
	prepared.database_incarnation = database_incarnation;
	prepared.origin = origin;
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	result = relmap_runtime_origin_matches(directory, system_identifier, database_incarnation, origin);
	if (result == CLUSTER_RELMAP_RETENTION_OK)
		result = cluster_relmap_retention_truncate_begin(directory, requested_cutoff, &prepared.reservation);
	/* The pure core clears its output on refusal. Keep that local so a BUSY
	 * retry cannot erase the original owner's still-live routing receipt. */
	if (result == CLUSTER_RELMAP_RETENTION_OK)
		*reservation = prepared;
	LWLockRelease(&relmap_runtime_shmem->lock);
	return result;
}

static ClusterRelmapRetentionResult
relmap_runtime_truncate_finish(const ClusterRelmapRetentionRuntimeTruncateV2 *reservation,
	bool uncertain)
{
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionResult result;

	if (reservation == NULL || reservation->system_identifier == 0
		|| reservation->database_incarnation == 0 || reservation->origin >= CLUSTER_MAX_NODES)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (relmap_runtime_shmem == NULL || LWLockHeldByMe(&relmap_runtime_shmem->lock))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	directory = &relmap_runtime_shmem->directories[reservation->origin];
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	result = relmap_runtime_origin_matches(directory, reservation->system_identifier,
		reservation->database_incarnation, reservation->origin);
	if (result == CLUSTER_RELMAP_RETENTION_OK)
		result = uncertain
			? cluster_relmap_retention_truncate_uncertain(directory, &reservation->reservation)
			: cluster_relmap_retention_truncate_complete(directory, &reservation->reservation);
	LWLockRelease(&relmap_runtime_shmem->lock);
	return result;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_truncate_complete_v2(
	const ClusterRelmapRetentionRuntimeTruncateV2 *reservation)
{
	return relmap_runtime_truncate_finish(reservation, false);
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_truncate_uncertain_v2(
	const ClusterRelmapRetentionRuntimeTruncateV2 *reservation)
{
	return relmap_runtime_truncate_finish(reservation, true);
}

static ClusterRelmapRetentionResult
relmap_runtime_allocation_guard(uint64 system_identifier, uint64 database_incarnation,
	uint16 origin, uint64 candidate, ClusterRelmapRetentionAllocationPublishV2 publish,
	void *argument)
{
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionResult result;

	if (system_identifier == 0 || database_incarnation == 0 || origin >= CLUSTER_MAX_NODES)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (relmap_runtime_shmem == NULL || !LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE)
		|| LWLockHeldByMe(&relmap_runtime_shmem->lock))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	directory = &relmap_runtime_shmem->directories[origin];
	LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
	result = relmap_runtime_origin_matches(directory, system_identifier, database_incarnation, origin);
	if (result == CLUSTER_RELMAP_RETENTION_OK)
		result = publish != NULL ? cluster_relmap_retention_allocation_observe(directory, candidate)
			: cluster_relmap_retention_allocation_check(directory, candidate);
	if (publish != NULL && result == CLUSTER_RELMAP_RETENTION_OK) {
		/* Do not escape this acquisition or unlock before the original nofail
		 * stores: a previous top could otherwise add a pin after the final cut.
		 * Keep cleanup lexical so ERROR cannot leave a stale begin/end marker
		 * that later releases some unrelated acquisition by this backend. */
		PG_TRY();
		{
			publish(argument);
		}
		PG_CATCH();
		{
			/* ERROR resets InterruptHoldoffCount. Match LWLockReleaseAll
			 * before the release resumes interrupts; never rewind even a
			 * partial publication. Native cleanup still owns XidGenLock. */
			HOLD_INTERRUPTS();
			LWLockRelease(&relmap_runtime_shmem->lock);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	LWLockRelease(&relmap_runtime_shmem->lock);
	return result;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_allocation_check_v2(uint64 system_identifier,
	uint64 database_incarnation, uint16 origin, uint64 candidate)
{
	return relmap_runtime_allocation_guard(system_identifier, database_incarnation,
		origin, candidate, NULL, NULL);
}

ClusterRelmapRetentionResult
cluster_relmap_retention_runtime_allocation_publish_v2(uint64 system_identifier,
	uint64 database_incarnation, uint16 origin, uint64 candidate,
	ClusterRelmapRetentionAllocationPublishV2 publish, void *argument)
{
	if (publish == NULL)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	return relmap_runtime_allocation_guard(system_identifier, database_incarnation,
		origin, candidate, publish, argument);
}
