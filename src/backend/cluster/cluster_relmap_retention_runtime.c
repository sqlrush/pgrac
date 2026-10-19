/* PGRAC: private shared relmap retention coordination.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_shmem.h"
#include "storage/shmem.h"

#define RELMAP_RETENTION_RUNTIME_NAME "pgrac relmap retention"
#define RELMAP_RETENTION_RUNTIME_MAGIC 0x52525432U
#define RELMAP_RETENTION_RUNTIME_VERSION 2

typedef struct RelmapRetentionRuntimeShmem {
	uint32 magic;
	uint32 version;
	Size bytes;
	int tranche;
	/* All origins, begin, future ACTIVE/release and truncate/allocation guards
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
