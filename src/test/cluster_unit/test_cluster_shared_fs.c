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
#include "port/pg_crc32c.h"
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
/* spec-4.5a: sharedfs backend GUC storages + node id (link-only). */
char *cluster_shared_data_dir = NULL;
char *cluster_shared_storage_uuid = NULL;
int cluster_node_id = 0;

/*
 * miscadmin global referenced by cluster_shared_fs_init's WARNING
 * branch (spec-1.7.2 codex round 3 P2 finding 1: !IsUnderPostmaster
 * guard).  Real backends set this; in this unit-test environment it
 * stays false so the guard short-circuits the WARNING (we never run
 * cluster_shared_fs_init() in the test anyway -- only take the
 * function address).
 */
bool IsUnderPostmaster = false;

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
int
FilePrefetch(File f pg_attribute_unused(), off_t o pg_attribute_unused(),
			 off_t a pg_attribute_unused(), uint32 w pg_attribute_unused())
{
	return 0;
}
void
FileWriteback(File f pg_attribute_unused(), off_t o pg_attribute_unused(),
			  off_t a pg_attribute_unused(), uint32 w pg_attribute_unused())
{}
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

/* ----------
 * spec-4.5a: additional stubs pulled in by cluster_shared_fs_sharedfs.o
 * (sentinel raw I/O + path building).  Link-only; never invoked here --
 * the runtime behaviour lives in test_cluster_shared_fs_sharedfs.c.
 * ----------
 */
char *
psprintf(const char *fmt pg_attribute_unused(), ...)
{
	return NULL;
}

char *
pstrdup(const char *in pg_attribute_unused())
{
	return NULL;
}

int pg_dir_create_mode = 0700;

int
pg_mkdir_p(char *path pg_attribute_unused(), int omode pg_attribute_unused())
{
	return -1;
}

int
OpenTransientFile(const char *fileName pg_attribute_unused(), int fileFlags pg_attribute_unused())
{
	return -1;
}

int
CloseTransientFile(int fd pg_attribute_unused())
{
	return 0;
}

int
pg_fsync(int fd pg_attribute_unused())
{
	return 0;
}

bool
pg_strong_random(void *buf pg_attribute_unused(), size_t len pg_attribute_unused())
{
	return false;
}

/* CRC32C symbol for the active platform variant (identity stub; the
 * sentinel paths that compute CRCs never run in this binary). */
extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len);

pg_crc32c
pg_comp_crc32c_sse42(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	return crc;
}

pg_crc32c
pg_comp_crc32c_armv8(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	return crc;
}

pg_crc32c (*pg_comp_crc32c)(pg_crc32c crc, const void *data, size_t len) = pg_comp_crc32c_sse42;


static const ClusterSharedFsCaps dummy_block_device_caps = {
	.supports_odirect = true,
	.required_io_alignment = 512,
	.supports_scsi3_pr = false,
	.durability_class = CLUSTER_DURABILITY_ODIRECT_BARRIER,
	.max_nodes = 128,
};

static bool
dummy_block_exists(RelFileLocator rlocator pg_attribute_unused(),
				   ForkNumber forknum pg_attribute_unused())
{
	return false;
}

static void
dummy_block_open(RelFileLocator rlocator pg_attribute_unused(),
				 ForkNumber forknum pg_attribute_unused(),
				 ClusterSharedFsHandle **out_handle pg_attribute_unused())
{}

static void
dummy_block_create(RelFileLocator rlocator pg_attribute_unused(),
				   ForkNumber forknum pg_attribute_unused(), bool isRedo pg_attribute_unused(),
				   ClusterSharedFsHandle **out_handle pg_attribute_unused())
{}

static void
dummy_block_close(ClusterSharedFsHandle *handle pg_attribute_unused())
{}

static int
dummy_block_read(ClusterSharedFsHandle *handle pg_attribute_unused(),
				 BlockNumber blocknum pg_attribute_unused(), char *buf pg_attribute_unused())
{
	return 0;
}

static int
dummy_block_write(ClusterSharedFsHandle *handle pg_attribute_unused(),
				  BlockNumber blocknum pg_attribute_unused(), const char *buf pg_attribute_unused())
{
	return 0;
}

static void
dummy_block_extend(ClusterSharedFsHandle *handle pg_attribute_unused(),
				   BlockNumber blocknum pg_attribute_unused())
{}

static BlockNumber
dummy_block_nblocks(ClusterSharedFsHandle *handle pg_attribute_unused())
{
	return 0;
}

static void
dummy_block_truncate(ClusterSharedFsHandle *handle pg_attribute_unused(),
					 BlockNumber nblocks pg_attribute_unused())
{}

static void
dummy_block_immedsync(ClusterSharedFsHandle *handle pg_attribute_unused())
{}

static void
dummy_block_unlink(RelFileLocator rlocator pg_attribute_unused(),
				   ForkNumber forknum pg_attribute_unused())
{}

static void
dummy_block_init(void)
{}

static void
dummy_block_shutdown(void)
{}

static int
dummy_block_barrier_sync(ClusterSharedFsHandle *handle pg_attribute_unused())
{
	return 0;
}

static int
dummy_block_register_fence_key(int node_id pg_attribute_unused())
{
	return 0;
}

static ClusterFenceCapability
dummy_block_fence_capability(void)
{
	return CLUSTER_FENCE_CAP_NONE;
}

static bool
dummy_block_prefetch(ClusterSharedFsHandle *handle pg_attribute_unused(),
					 BlockNumber blocknum pg_attribute_unused())
{
	return true;
}

static void
dummy_block_writeback(ClusterSharedFsHandle *handle pg_attribute_unused(),
					  BlockNumber blocknum pg_attribute_unused(),
					  BlockNumber nblocks pg_attribute_unused())
{}

const ClusterSharedFsOps cluster_shared_fs_block_device_ops = {
	.name = "block_device",
	.id = CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE,
	.caps = &dummy_block_device_caps,
	.exists = dummy_block_exists,
	.open_existing = dummy_block_open,
	.create = dummy_block_create,
	.close = dummy_block_close,
	.read = dummy_block_read,
	.write = dummy_block_write,
	.extend = dummy_block_extend,
	.nblocks = dummy_block_nblocks,
	.truncate = dummy_block_truncate,
	.immedsync = dummy_block_immedsync,
	.unlink = dummy_block_unlink,
	.init = dummy_block_init,
	.shutdown = dummy_block_shutdown,
	.barrier_sync = dummy_block_barrier_sync,
	.register_fence_key = dummy_block_register_fence_key,
	.fence_capability = dummy_block_fence_capability,
	.prefetch = dummy_block_prefetch,
	.writeback = dummy_block_writeback,
};

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
	 * Sprint A 2026-05-02: open split into exists / open_existing /
	 * create.  Spec-6.0a adds durability/fence/advisory callbacks.
	 */
	UT_ASSERT(sizeof(ClusterSharedFsOps) >= sizeof(void *) * 18);
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

	UT_ASSERT_NOT_NULL((void *)ops->exists);
	UT_ASSERT_NOT_NULL((void *)ops->open_existing);
	UT_ASSERT_NOT_NULL((void *)ops->create);
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
	UT_ASSERT_NOT_NULL((void *)ops->caps);
	UT_ASSERT_NOT_NULL((void *)ops->barrier_sync);
	UT_ASSERT_NOT_NULL((void *)ops->register_fence_key);
	UT_ASSERT_NOT_NULL((void *)ops->fence_capability);
	UT_ASSERT_NOT_NULL((void *)ops->prefetch);
	UT_ASSERT_NOT_NULL((void *)ops->writeback);
}


UT_TEST(test_local_vtable_callbacks_nonnull)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_local_ops;

	UT_ASSERT_EQ((int)ops->id, (int)CLUSTER_SHARED_FS_BACKEND_LOCAL);
	UT_ASSERT_NOT_NULL(ops->name);
	UT_ASSERT_STR_EQ(ops->name, "local");

	UT_ASSERT_NOT_NULL((void *)ops->exists);
	UT_ASSERT_NOT_NULL((void *)ops->open_existing);
	UT_ASSERT_NOT_NULL((void *)ops->create);
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
	UT_ASSERT_NOT_NULL((void *)ops->caps);
	UT_ASSERT_NOT_NULL((void *)ops->barrier_sync);
	UT_ASSERT_NOT_NULL((void *)ops->register_fence_key);
	UT_ASSERT_NOT_NULL((void *)ops->fence_capability);
	UT_ASSERT_NOT_NULL((void *)ops->prefetch);
	UT_ASSERT_NOT_NULL((void *)ops->writeback);
}


UT_TEST(test_stub_and_local_distinct)
{
	UT_ASSERT_NE((int)cluster_shared_fs_stub_ops.id, (int)cluster_shared_fs_local_ops.id);
	UT_ASSERT((void *)cluster_shared_fs_stub_ops.exists
			  != (void *)cluster_shared_fs_local_ops.exists);
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
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_get_active_caps);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_get_registered_count);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_get_backend_at);
}


UT_TEST(test_dispatch_wrappers_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_exists);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_open_existing);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_create);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_close);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_read);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_write);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_extend);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_nblocks);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_truncate);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_immedsync);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_unlink);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_barrier_sync);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_register_fence_key);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_fence_capability);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_prefetch);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_writeback);
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
 * Spec-1.7.2 F1 — create() vtable signature carries isRedo
 *
 *	The vtable create callback was extended in spec-1.7.2 to take a
 *	`bool isRedo` parameter so that local backend can use O_CREAT|
 *	O_EXCL (!isRedo) vs idempotent open (isRedo) per md.c mdcreate
 *	semantics.  Compile-time signature check below: if a future
 *	regression accidentally drops isRedo from the vtable signature,
 *	this test will fail to compile.
 *
 *	Behavior coverage (from TAP 019_smgr_cluster.pl):
 *	  - L3 CREATE TABLE / INSERT / SELECT exercises the !isRedo
 *	    path: non-redo create against a fresh RelFileLocator (no
 *	    existing file) succeeds via O_CREAT|O_EXCL.
 *	  - L7 pg_ctl restart triggers WAL redo replay, which calls
 *	    smgrcreate(isRedo=true) on already-existing relfilenode
 *	    files; idempotent path (existing file OK) is exercised
 *	    here.
 *	Together L3 + L7 cover spec-1.7.2 DoD #20's two required paths.
 * ============================================================ */

/*
 * Function pointer type matching the post-1.7.2 create signature.  If
 * the vtable signature drifts (e.g. isRedo accidentally removed in a
 * future amend) the assignment in test_create_signature_has_isRedo
 * below fails to compile, catching the regression at link time.
 */
typedef void (*PgracCreateCallbackSig)(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
									   ClusterSharedFsHandle **out_handle);


UT_TEST(test_create_signature_has_isRedo)
{
	/*
	 * Take the address of the dispatch wrapper WITHOUT a cast.  The
	 * compiler refuses to assign one function-pointer type to another
	 * with mismatched parameters under -Wincompatible-pointer-types
	 * (PG default), so a future drop of `bool isRedo` from cluster_
	 * shared_fs_create would surface as a build warning here.
	 *
	 * (Codex round 3 P3 finding 3 2026-05-03: the previous explicit
	 * cast suppressed the very type mismatch this test is designed
	 * to catch.)
	 */
	PgracCreateCallbackSig fn = cluster_shared_fs_create;

	UT_ASSERT_NOT_NULL((void *)fn);
}


UT_TEST(test_stub_create_signature_has_isRedo)
{
	/*
	 * Same compile-time signature check on the stub backend's vtable
	 * slot, ensuring built-in backends keep matching the new
	 * signature.  No cast, same rationale as
	 * test_create_signature_has_isRedo.
	 */
	PgracCreateCallbackSig fn = cluster_shared_fs_stub_ops.create;

	UT_ASSERT_NOT_NULL((void *)fn);
}


/* spec-4.5a D12: the sharedfs vtable joins the built-ins. */
UT_TEST(test_sharedfs_vtable_callbacks_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.exists);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.open_existing);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.create);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.close);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.read);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.write);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.extend);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.nblocks);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.truncate);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.immedsync);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.unlink);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.init);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.shutdown);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.caps);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.barrier_sync);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.register_fence_key);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.fence_capability);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.prefetch);
	UT_ASSERT_NOT_NULL((void *)cluster_shared_fs_sharedfs_ops.writeback);
}

UT_TEST(test_sharedfs_vtable_identity)
{
	UT_ASSERT_EQ(cluster_shared_fs_sharedfs_ops.id, CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);
	UT_ASSERT_STR_EQ(cluster_shared_fs_sharedfs_ops.name, "shared_fs");
	UT_ASSERT(cluster_shared_fs_sharedfs_ops.read != cluster_shared_fs_local_ops.read);
	UT_ASSERT(cluster_shared_fs_sharedfs_ops.write != cluster_shared_fs_local_ops.write);
}

UT_TEST(test_sharedfs_sentinel_symbols_linkable)
{
	void (*attach_fn)(void) = cluster_shared_fs_sentinel_attach;
	bool (*has_fn)(int) = cluster_shared_fs_sentinel_has_participant;

	UT_ASSERT_NOT_NULL((void *)attach_fn);
	UT_ASSERT_NOT_NULL((void *)has_fn);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(15);
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
	UT_RUN(test_create_signature_has_isRedo);
	UT_RUN(test_stub_create_signature_has_isRedo);
	UT_RUN(test_sharedfs_vtable_callbacks_nonnull);
	UT_RUN(test_sharedfs_vtable_identity);
	UT_RUN(test_sharedfs_sentinel_symbols_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
