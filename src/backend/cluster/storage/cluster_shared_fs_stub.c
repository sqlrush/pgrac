/*-------------------------------------------------------------------------
 *
 * cluster_shared_fs_stub.c
 *	  Placeholder cluster_shared_fs backend (Stage 1.1).
 *
 *	  Every callback ereports FEATURE_NOT_SUPPORTED with an errhint
 *	  pointing the DBA at the right Stage / GUC value.  Used as the
 *	  default backend so that postmaster startup with the boot default
 *	  GUC succeeds without committing to any real backend; the very
 *	  first call into the cluster_shared_fs API then fails loudly with
 *	  guidance rather than crashing or silently no-oping.
 *
 *	  See specs/spec-1.1-shared-fs-skeleton.md for context;
 *	  docs/cluster-shared-fs-design.md §0 principle 4 for the "no
 *	  silent downgrade" rationale.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_shared_fs_stub.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds; see src/backend/cluster/Makefile for the OBJS rules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/storage/cluster_shared_fs.h"


#ifdef USE_PGRAC_CLUSTER

/*
 * One canonical errhint, reused by every I/O callback (eleven storage
 * callbacks plus two lifecycle callbacks, thirteen function pointers
 * total — the lifecycle init/shutdown stubs do nothing, so only the
 * storage callbacks reach this errhint).  Centralised
 * so that wording stays consistent if the Stage 2 backends move (the
 * test harness in 018_shared_fs.pl matches part of this hint).
 */
#define STUB_ERRHINT_MESSAGE                                                                       \
	"shared_storage_backend=stub is a placeholder. "                                               \
	"Set cluster.shared_storage_backend=local for single-node passthrough; "                       \
	"\"block_device\", \"cluster_fs\", \"rbd\", and \"multi_attach\" land in Stage 2."

static const ClusterSharedFsCaps cluster_shared_fs_stub_caps = {
	.supports_odirect = false,
	.required_io_alignment = 0,
	.supports_scsi3_pr = false,
	.durability_class = CLUSTER_DURABILITY_NONE,
	.max_nodes = 0,
};


pg_attribute_noreturn() static void cluster_shared_fs_stub_reject(const char *callsite)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_shared_fs.%s is not available with shared_storage_backend=stub",
						   callsite),
					errhint("%s", STUB_ERRHINT_MESSAGE)));
}


static bool
cluster_shared_fs_stub_exists(RelFileLocator rlocator, ForkNumber forknum)
{
	(void)rlocator;
	(void)forknum;
	cluster_shared_fs_stub_reject("exists");
	/* unreachable */
	return false;
}

static void
cluster_shared_fs_stub_open_existing(RelFileLocator rlocator, ForkNumber forknum,
									 ClusterSharedFsHandle **out_handle)
{
	(void)rlocator;
	(void)forknum;
	(void)out_handle;
	cluster_shared_fs_stub_reject("open_existing");
}

static void
cluster_shared_fs_stub_create(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
							  ClusterSharedFsHandle **out_handle)
{
	(void)rlocator;
	(void)forknum;
	(void)isRedo;
	(void)out_handle;
	cluster_shared_fs_stub_reject("create");
}


static void
cluster_shared_fs_stub_close(ClusterSharedFsHandle *handle)
{
	(void)handle;
	cluster_shared_fs_stub_reject("close");
}


static int
cluster_shared_fs_stub_read(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf)
{
	(void)handle;
	(void)blocknum;
	(void)buf;
	cluster_shared_fs_stub_reject("read");
}


static int
cluster_shared_fs_stub_write(ClusterSharedFsHandle *handle, BlockNumber blocknum, const char *buf)
{
	(void)handle;
	(void)blocknum;
	(void)buf;
	cluster_shared_fs_stub_reject("write");
}


static void
cluster_shared_fs_stub_extend(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	(void)handle;
	(void)blocknum;
	cluster_shared_fs_stub_reject("extend");
}


static BlockNumber
cluster_shared_fs_stub_nblocks(ClusterSharedFsHandle *handle)
{
	(void)handle;
	cluster_shared_fs_stub_reject("nblocks");
}


static void
cluster_shared_fs_stub_truncate(ClusterSharedFsHandle *handle, BlockNumber nblocks)
{
	(void)handle;
	(void)nblocks;
	cluster_shared_fs_stub_reject("truncate");
}


static void
cluster_shared_fs_stub_immedsync(ClusterSharedFsHandle *handle)
{
	(void)handle;
	cluster_shared_fs_stub_reject("immedsync");
}


static void
cluster_shared_fs_stub_unlink(RelFileLocator rlocator, ForkNumber forknum)
{
	(void)rlocator;
	(void)forknum;
	cluster_shared_fs_stub_reject("unlink");
}


/* Lifecycle: stub has nothing to set up or tear down. */
static void
cluster_shared_fs_stub_init(void)
{}
static void
cluster_shared_fs_stub_shutdown(void)
{}

static int
cluster_shared_fs_stub_barrier_sync(ClusterSharedFsHandle *handle)
{
	(void)handle;
	cluster_shared_fs_stub_reject("barrier_sync");
}

static int
cluster_shared_fs_stub_register_fence_key(int node_id)
{
	(void)node_id;
	cluster_shared_fs_stub_reject("register_fence_key");
}

static ClusterFenceCapability
cluster_shared_fs_stub_fence_capability(void)
{
	return CLUSTER_FENCE_CAP_NONE;
}

static bool
cluster_shared_fs_stub_prefetch(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	(void)handle;
	(void)blocknum;
	cluster_shared_fs_stub_reject("prefetch");
}

static void
cluster_shared_fs_stub_writeback(ClusterSharedFsHandle *handle, BlockNumber blocknum,
								 BlockNumber nblocks)
{
	(void)handle;
	(void)blocknum;
	(void)nblocks;
	cluster_shared_fs_stub_reject("writeback");
}


const ClusterSharedFsOps cluster_shared_fs_stub_ops = {
	.name = "stub",
	.id = CLUSTER_SHARED_FS_BACKEND_STUB,
	.caps = &cluster_shared_fs_stub_caps,

	.exists = cluster_shared_fs_stub_exists,
	.open_existing = cluster_shared_fs_stub_open_existing,
	.create = cluster_shared_fs_stub_create,
	.close = cluster_shared_fs_stub_close,
	.read = cluster_shared_fs_stub_read,
	.write = cluster_shared_fs_stub_write,
	.extend = cluster_shared_fs_stub_extend,
	.nblocks = cluster_shared_fs_stub_nblocks,
	.truncate = cluster_shared_fs_stub_truncate,
	.immedsync = cluster_shared_fs_stub_immedsync,
	.unlink = cluster_shared_fs_stub_unlink,

	.init = cluster_shared_fs_stub_init,
	.shutdown = cluster_shared_fs_stub_shutdown,

	.barrier_sync = cluster_shared_fs_stub_barrier_sync,
	.register_fence_key = cluster_shared_fs_stub_register_fence_key,
	.fence_capability = cluster_shared_fs_stub_fence_capability,
	.prefetch = cluster_shared_fs_stub_prefetch,
	.writeback = cluster_shared_fs_stub_writeback,
};

#endif /* USE_PGRAC_CLUSTER */
