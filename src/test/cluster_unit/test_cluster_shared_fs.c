/*-------------------------------------------------------------------------
 *
 * test_cluster_shared_fs.c
 *	  Compile-time / link-level invariants for the cluster_shared_fs
 *	  abstraction layer shipped at stage 1.1.
 *
 *	  Locks:
 *	    - Backend ID enum positions (stub=0, local=1) and the 16-slot
 *	      ClusterSharedFsBackendId reservation.
 *	    - Built-in vtables expose every callback as a non-NULL function
 *	      pointer (rejects half-initialised vtables at link time).
 *	    - All public symbols declared in cluster_shared_fs.h resolve at
 *	      link time (init / shutdown / register / dispatch / accessors).
 *
 *	  End-to-end runtime behaviour (GUC resolution, FATAL on missing
 *	  backend, pg_cluster_state shared_fs category) is verified on a
 *	  real PG instance by cluster_tap t/018_shared_fs.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shared_fs.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking the storage/*.o objects
 *	  standalone pulls in references to ereport, palloc, fd.c VFD
 *	  helpers, before_shmem_exit, and the cluster.shared_storage_backend
 *	  GUC storage; the unit test stubs every one of those because it
 *	  only takes function addresses (never invokes a path that would
 *	  touch them in steady state).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/storage/cluster_shared_fs.h"
#include "storage/relfilelocator.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_shared_fs.o + _stub.o + _local.o
 * standalone.  None of these paths run during the unit test -- we
 * only take addresses and read static vtable contents.
 * ----------
 */
#include "fmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/* GUC variables accessed by cluster_shared_fs_init.  Stage 1.2 added
 * the smgr_user_relations cross-check. */
int cluster_shared_storage_backend = 0;
bool cluster_smgr_user_relations = false;

/* Cluster injection support (CLUSTER_INJECTION_POINT() macro expansion). */
#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
char *cluster_injection_points = NULL;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ereport machinery (the fast-path stubs are sufficient -- never invoked). */
bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
elog_start(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		   const char *funcname pg_attribute_unused())
{}

void
elog_finish(int elevel pg_attribute_unused(), const char *fmt pg_attribute_unused(), ...)
{}

void
pre_format_elog_string(int errnumber pg_attribute_unused(),
					   const char *domain pg_attribute_unused())
{}

char *
format_elog_string(const char *fmt pg_attribute_unused(), ...)
{
	return NULL;
}

/*
 * Memory context machinery (used by local backend's palloc0).  PG's
 * palloc.h defines MemoryContextSwitchTo as a static inline; we only
 * provide the storage backings + the extern palloc0 / pfree stubs.
 */
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;
void *
palloc0(Size size pg_attribute_unused())
{
	return NULL;
}
void
pfree(void *pointer pg_attribute_unused())
{}

/* fd.c VFD layer stubs. */
File
PathNameOpenFile(const char *fileName pg_attribute_unused(), int fileFlags pg_attribute_unused())
{
	return -1;
}
void
FileClose(File file pg_attribute_unused())
{}
int
FileRead(File f pg_attribute_unused(), void *b pg_attribute_unused(),
		 size_t a pg_attribute_unused(), off_t o pg_attribute_unused(),
		 uint32 w pg_attribute_unused())
{
	return 0;
}
int
FileWrite(File f pg_attribute_unused(), const void *b pg_attribute_unused(),
		  size_t a pg_attribute_unused(), off_t o pg_attribute_unused(),
		  uint32 w pg_attribute_unused())
{
	return 0;
}
int
FileSync(File f pg_attribute_unused(), uint32 w pg_attribute_unused())
{
	return 0;
}
off_t
FileSize(File f pg_attribute_unused())
{
	return 0;
}
int
FileTruncate(File f pg_attribute_unused(), off_t o pg_attribute_unused(),
			 uint32 w pg_attribute_unused())
{
	return 0;
}

char *
GetRelationPath(Oid dbOid pg_attribute_unused(), Oid spcOid pg_attribute_unused(),
				RelFileNumber relNumber pg_attribute_unused(), int backendId pg_attribute_unused(),
				ForkNumber forkNumber pg_attribute_unused())
{
	return NULL;
}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Compile-time anchors
 * ============================================================ */

UT_TEST(test_shared_fs_backend_max_constant)
{
	UT_ASSERT_EQ(CLUSTER_SHARED_FS_BACKEND_MAX, 16);
}


UT_TEST(test_shared_fs_backend_id_enum_frozen)
{
	UT_ASSERT_EQ((int)CLUSTER_SHARED_FS_BACKEND_STUB, 0);
	UT_ASSERT_EQ((int)CLUSTER_SHARED_FS_BACKEND_LOCAL, 1);
	UT_ASSERT_EQ((int)CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE, 2);
	UT_ASSERT_EQ((int)CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS, 3);
	UT_ASSERT_EQ((int)CLUSTER_SHARED_FS_BACKEND_RBD, 4);
	UT_ASSERT_EQ((int)CLUSTER_SHARED_FS_BACKEND_MULTI_ATTACH, 5);
}


UT_TEST(test_shared_fs_vtable_struct_nonempty)
{
	/*
	 * Anchor sizeof to "more than just one int" so an accidental
	 * structural change (member removed, int replaces a fp) is loud.
	 * Eleven function pointers + a string + an int lower-bound to
	 * ~80 bytes on 64-bit, ~50 on 32-bit; we test the conservative
	 * lower bound.
	 */
	UT_ASSERT(sizeof(ClusterSharedFsOps) >= sizeof(void *) * 11);
}


/* ============================================================
 * Built-in vtables
 * ============================================================ */

UT_TEST(test_stub_vtable_callbacks_nonnull)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_stub_ops;

	UT_ASSERT_EQ((int)ops->id, (int)CLUSTER_SHARED_FS_BACKEND_STUB);
	UT_ASSERT_NOT_NULL(ops->name);
	UT_ASSERT_STR_EQ(ops->name, "stub");

	UT_ASSERT_NOT_NULL((void *)ops->open);
	UT_ASSERT_NOT_NULL((void *)ops->close);
	UT_ASSERT_NOT_NULL((void *)ops->read);
	UT_ASSERT_NOT_NULL((void *)ops->write);
	UT_ASSERT_NOT_NULL((void *)ops->extend);
	UT_ASSERT_NOT_NULL((void *)ops->nblocks);
	UT_ASSERT_NOT_NULL((void *)ops->truncate);
	UT_ASSERT_NOT_NULL((void *)ops->immedsync);
	UT_ASSERT_NOT_NULL((void *)ops->unlink);
	UT_ASSERT_NOT_NULL((void *)ops->init);
	UT_ASSERT_NOT_NULL((void *)ops->shutdown);
}


UT_TEST(test_local_vtable_callbacks_nonnull)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_local_ops;

	UT_ASSERT_EQ((int)ops->id, (int)CLUSTER_SHARED_FS_BACKEND_LOCAL);
	UT_ASSERT_NOT_NULL(ops->name);
	UT_ASSERT_STR_EQ(ops->name, "local");

	UT_ASSERT_NOT_NULL((void *)ops->open);
	UT_ASSERT_NOT_NULL((void *)ops->close);
	UT_ASSERT_NOT_NULL((void *)ops->read);
	UT_ASSERT_NOT_NULL((void *)ops->write);
	UT_ASSERT_NOT_NULL((void *)ops->extend);
	UT_ASSERT_NOT_NULL((void *)ops->nblocks);
	UT_ASSERT_NOT_NULL((void *)ops->truncate);
	UT_ASSERT_NOT_NULL((void *)ops->immedsync);
	UT_ASSERT_NOT_NULL((void *)ops->unlink);
	UT_ASSERT_NOT_NULL((void *)ops->init);
	UT_ASSERT_NOT_NULL((void *)ops->shutdown);
}


UT_TEST(test_stub_and_local_distinct)
{
	UT_ASSERT_NE((int)cluster_shared_fs_stub_ops.id, (int)cluster_shared_fs_local_ops.id);
	UT_ASSERT((void *)cluster_shared_fs_stub_ops.open != (void *)cluster_shared_fs_local_ops.open);
}


/* ============================================================
 * Public symbol linkability
 *
 *	If any of these unresolves at link time, this test binary will
 *	fail to build.  Taking the address (cast to void *) is enough to
 *	pin link-time presence without invoking the body.
 * ============================================================ */

UT_TEST(test_lifecycle_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_init);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_shutdown);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_register_backend);
}


UT_TEST(test_accessor_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_get_active_ops);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_get_registered_count);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_get_backend_at);
}


UT_TEST(test_dispatch_wrappers_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_open);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_close);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_read);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_write);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_extend);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_nblocks);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_truncate);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_immedsync);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_unlink);
}


UT_TEST(test_get_backend_at_out_of_range)
{
	/*
	 * Accessor returns NULL for any out-of-range slot regardless of
	 * registry state, including pre-init (which is our state here).
	 */
	UT_ASSERT_NULL(cluster_shared_fs_get_backend_at(-1));
	UT_ASSERT_NULL(cluster_shared_fs_get_backend_at(CLUSTER_SHARED_FS_BACKEND_MAX));
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_shared_fs_backend_max_constant);
	UT_RUN(test_shared_fs_backend_id_enum_frozen);
	UT_RUN(test_shared_fs_vtable_struct_nonempty);
	UT_RUN(test_stub_vtable_callbacks_nonnull);
	UT_RUN(test_local_vtable_callbacks_nonnull);
	UT_RUN(test_stub_and_local_distinct);
	UT_RUN(test_lifecycle_symbols_linkable);
	UT_RUN(test_accessor_symbols_linkable);
	UT_RUN(test_dispatch_wrappers_linkable);
	UT_RUN(test_get_backend_at_out_of_range);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
