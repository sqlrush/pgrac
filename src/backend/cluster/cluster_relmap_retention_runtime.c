/* PGRAC: private shared relmap retention coordination.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
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
