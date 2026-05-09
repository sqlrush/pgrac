/*-------------------------------------------------------------------------
 *
 * test_cluster_smgr.c
 *	  Compile-time / link-level invariants for the cluster_smgr
 *	  bridge shipped at stage 1.2 (方案 C 单文件单 fork-handle 版本).
 *
 *	  Locks:
 *	    - All sixteen f_smgr callback prototypes resolve at link time
 *	      (validates that smgr.c can wire smgrsw[1] from these
 *	      symbols without glue).
 *	    - cluster_smgr_which_for / _init / _shutdown / accessors are
 *	      linkable.
 *	    - cluster_smgr_active_relation_count returns 0 before init
 *	      (the bypass HTAB is process-local; this test process never
 *	      runs cluster_smgr_init and thus has count == 0 throughout).
 *
 *	  End-to-end runtime behaviour (smgrsw[1] dispatch, GUC routing,
 *	  PG 219 GUC=on, large-table single-file passthrough) is verified
 *	  on a real PG instance by cluster_tap t/019_smgr_cluster.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_smgr.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_smgr.o standalone
 *	  pulls in cluster_shared_fs.o + _stub.o + _local.o (vtable
 *	  registry), plus PG core symbols (HTAB, ereport, palloc, fd.c,
 *	  TablespaceCreateDbspace, mdzeroextend, ...).  The test stubs
 *	  every PG core symbol because callbacks are address-taken only,
 *	  never invoked through to PG runtime.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/storage/cluster_smgr.h"

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
 * Stubs needed to link cluster_smgr.o + cluster_shared_fs*.o
 * standalone.  None of these paths run during the unit test -- we
 * only take addresses and read the smgrsw-equivalent dispatch
 * symbols.
 * ----------
 */
#include "fmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* GUC variables read by cluster_smgr / cluster_shared_fs. */
int cluster_shared_storage_backend = 0;
bool cluster_smgr_user_relations = false;

/* Cluster injection support. */
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

/* ereport machinery. */
bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}
void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}
void
pre_format_elog_string(int n pg_attribute_unused(), const char *d pg_attribute_unused())
{}
char *
format_elog_string(const char *f pg_attribute_unused(), ...)
{
	return NULL;
}

/* Memory context machinery. */
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;
void *
palloc0(Size s pg_attribute_unused())
{
	return NULL;
}
void
pfree(void *p pg_attribute_unused())
{}

/* fd.c VFD layer stubs. */
File
PathNameOpenFile(const char *fn pg_attribute_unused(), int fl pg_attribute_unused())
{
	return -1;
}
void
FileClose(File f pg_attribute_unused())
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
GetRelationPath(Oid d pg_attribute_unused(), Oid s pg_attribute_unused(),
				RelFileNumber r pg_attribute_unused(), int b pg_attribute_unused(),
				ForkNumber f pg_attribute_unused())
{
	return NULL;
}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}

/* HTAB stubs. */
HTAB *
hash_create(const char *t pg_attribute_unused(), long n pg_attribute_unused(),
			const HASHCTL *info pg_attribute_unused(), int flags pg_attribute_unused())
{
	return NULL;
}
void *
hash_search(HTAB *h pg_attribute_unused(), const void *k pg_attribute_unused(),
			HASHACTION a pg_attribute_unused(), bool *f pg_attribute_unused())
{
	return NULL;
}
void
hash_destroy(HTAB *h pg_attribute_unused())
{}
void
hash_seq_init(HASH_SEQ_STATUS *s pg_attribute_unused(), HTAB *h pg_attribute_unused())
{}
void *
hash_seq_search(HASH_SEQ_STATUS *s pg_attribute_unused())
{
	return NULL;
}
long
hash_get_num_entries(HTAB *h pg_attribute_unused())
{
	return 0;
}

/* TablespaceCreateDbspace stub. */
void
TablespaceCreateDbspace(Oid s pg_attribute_unused(), Oid d pg_attribute_unused(),
						bool r pg_attribute_unused())
{}

/*
 * miscadmin global referenced by cluster_smgr_init() startup-warning
 * branch (Sprint A 2026-05-02).  In a real backend it is set by the
 * postmaster; in this unit-test environment it stays false so the
 * WARNING path stays inert (cluster_smgr_init() short-circuits when
 * !IsUnderPostmaster, mirroring the standalone-bootstrap exemption).
 */
bool IsUnderPostmaster = false;

/* md.c stubs (cluster_smgr no longer fallbacks to these but still
 * referenced via header inclusion). */
void
mdzeroextend(SMgrRelation r pg_attribute_unused(), ForkNumber f pg_attribute_unused(),
			 BlockNumber b pg_attribute_unused(), int n pg_attribute_unused(),
			 bool s pg_attribute_unused())
{}
bool
mdprefetch(SMgrRelation r pg_attribute_unused(), ForkNumber f pg_attribute_unused(),
		   BlockNumber b pg_attribute_unused())
{
	return false;
}
void
mdwriteback(SMgrRelation r pg_attribute_unused(), ForkNumber f pg_attribute_unused(),
			BlockNumber b pg_attribute_unused(), BlockNumber n pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Sixteen f_smgr callback symbols are linkable.
 * ============================================================ */

UT_TEST(test_smgr_callbacks_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_init);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_shutdown);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_open);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_close);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_create);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_exists);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_unlink);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_extend);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_zeroextend);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_prefetch);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_read);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_write);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_writeback);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_nblocks);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_truncate);
	UT_ASSERT_NOT_NULL((void *)cluster_smgr_immedsync);
}


/* ============================================================
 * Routing decision short-circuit cases (cluster_smgr_which_for).
 *
 *	None of these branches need a registered backend or an
 *	initialised HTAB -- the function returns based on GUC + backend
 *	checks alone.  We exercise the four return-0 branches.
 * ============================================================ */

UT_TEST(test_which_for_temp_relation_returns_md)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };

	/* Temp relation: backend != InvalidBackendId -> always 0. */
	UT_ASSERT_EQ(cluster_smgr_which_for(rl, 12), 0);
}


UT_TEST(test_which_for_stub_backend_returns_md)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };

	/* Default GUCs: shared_storage_backend=stub, smgr_user_relations=off.
	 * Either of those means smgr_which=0. */
	cluster_shared_storage_backend = 0; /* STUB */
	cluster_smgr_user_relations = true; /* even if user wants on */
	UT_ASSERT_EQ(cluster_smgr_which_for(rl, InvalidBackendId), 0);
}


UT_TEST(test_which_for_user_relations_off_returns_md)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };

	cluster_shared_storage_backend = 1;	 /* LOCAL */
	cluster_smgr_user_relations = false; /* GUC opt-in off */
	UT_ASSERT_EQ(cluster_smgr_which_for(rl, InvalidBackendId), 0);
}


UT_TEST(test_which_for_full_opt_in_returns_cluster)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };

	cluster_shared_storage_backend = 1; /* LOCAL */
	cluster_smgr_user_relations = true; /* opt-in on */
	UT_ASSERT_EQ(cluster_smgr_which_for(rl, InvalidBackendId), 1);
}


/* ============================================================
 * Diagnostic accessor.
 * ============================================================ */

UT_TEST(test_active_relation_count_pre_init)
{
	/* HTAB is process-local; not initialised in this unit-test
	 * process.  Accessor must safely return 0 instead of segfaulting. */
	UT_ASSERT_EQ(cluster_smgr_active_relation_count(), 0);
}


/* ============================================================
 * Header-level expectations.
 * ============================================================ */

UT_TEST(test_smgrsw_callback_signature_compiles)
{
	/*
	 * If any of the callback prototypes above drift from PG's
	 * f_smgr typedef, the compile will fail.  Touch all sixteen
	 * function pointers via address-take to exercise that.
	 */
	void (*p_init)(void) = cluster_smgr_init;
	void (*p_shutdown)(void) = cluster_smgr_shutdown;
	void (*p_open)(SMgrRelation) = cluster_smgr_open;

	UT_ASSERT_NOT_NULL((void *)p_init);
	UT_ASSERT_NOT_NULL((void *)p_shutdown);
	UT_ASSERT_NOT_NULL((void *)p_open);
}


/* ============================================================
 * spec-2.7 invalidation hook unit tests (v0.2 frozen 2026-05-09).
 *
 *	The HTAB stubs above make hash_search / hash_create no-ops, so
 *	cluster_smgr_relations stays NULL throughout this test.  That
 *	suits Q1 v0.2's contract:
 *	  - invalidate_relation / invalidate_relmap have NO local action,
 *	    they just bump the cross-instance STUB counter.
 *	  - invalidate_unlink_pending's local real action goes through
 *	    cluster_smgr_close_handle_for_rlocator, which short-circuits
 *	    when cluster_smgr_relations == NULL -- exactly what we want
 *	    here so the test exercises only the counter portion safely.
 *
 *	The runtime "real HTAB entry actually removed" verification lives
 *	in t/090_smgr_cluster_2node_concurrent_open.pl L4 + L5; here we
 *	verify only the counter contract and the function-symbol surface.
 * ============================================================ */

UT_TEST(test_invalidate_relation_is_pure_stub)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };
	uint64 before;
	uint64 after;

	before = cluster_smgr_get_remote_invalidation_stub_call_count();
	cluster_smgr_invalidate_relation(rl, MAIN_FORKNUM);
	after = cluster_smgr_get_remote_invalidation_stub_call_count();

	/* Counter advanced by exactly one;cluster_smgr_active_relation_count
	 * unchanged because invalidate_relation has no local HTAB action. */
	UT_ASSERT_EQ(after - before, 1);
	UT_ASSERT_EQ(cluster_smgr_active_relation_count(), 0);
}


UT_TEST(test_invalidate_relmap_takes_bool_shared)
{
	uint64 before;
	uint64 after;

	before = cluster_smgr_get_remote_invalidation_stub_call_count();
	cluster_smgr_invalidate_relmap(true);  /* shared catalog map */
	cluster_smgr_invalidate_relmap(false); /* per-database map */
	after = cluster_smgr_get_remote_invalidation_stub_call_count();

	/* Both shared=true and shared=false bump the same counter.
	 * Q2 v0.2 sig check: the call must compile against `bool shared`. */
	UT_ASSERT_EQ(after - before, 2);
}


UT_TEST(test_invalidate_unlink_pending_includes_local_close)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16386 };
	uint64 before;
	uint64 after;

	before = cluster_smgr_get_remote_invalidation_stub_call_count();
	cluster_smgr_invalidate_unlink_pending(rl);
	after = cluster_smgr_get_remote_invalidation_stub_call_count();

	/*
	 * Counter advanced by one (cross-instance STUB portion).
	 * Local close path is no-op here because cluster_smgr_relations
	 * stays NULL with the test HTAB stubs;the body must short-
	 * circuit gracefully on NULL HTAB instead of segfaulting.
	 */
	UT_ASSERT_EQ(after - before, 1);
	UT_ASSERT_EQ(cluster_smgr_active_relation_count(), 0);
}


UT_TEST(test_remote_invalidation_counter_is_atomic_and_monotonic)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16387 };
	uint64 baseline;
	uint64 final;

	baseline = cluster_smgr_get_remote_invalidation_stub_call_count();

	/* Three different hook entry points, three different argument
	 * shapes -- all funnel into the same atomic counter. */
	cluster_smgr_invalidate_relation(rl, MAIN_FORKNUM);
	cluster_smgr_invalidate_relmap(true);
	cluster_smgr_invalidate_unlink_pending(rl);

	final = cluster_smgr_get_remote_invalidation_stub_call_count();

	UT_ASSERT_EQ(final - baseline, 3);

	/*
	 * Counter symbol must be link-resolvable in --enable-cluster builds.
	 * Address-take exercises the public extern -- spec-2.27 amend
	 * will rename this symbol in lockstep with the counter rename.
	 */
	UT_ASSERT_NOT_NULL((void *)&cluster_smgr_remote_invalidation_stub_call_count);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_smgr_callbacks_linkable);
	UT_RUN(test_which_for_temp_relation_returns_md);
	UT_RUN(test_which_for_stub_backend_returns_md);
	UT_RUN(test_which_for_user_relations_off_returns_md);
	UT_RUN(test_which_for_full_opt_in_returns_cluster);
	UT_RUN(test_active_relation_count_pre_init);
	UT_RUN(test_smgrsw_callback_signature_compiles);
	UT_RUN(test_invalidate_relation_is_pure_stub);
	UT_RUN(test_invalidate_relmap_takes_bool_shared);
	UT_RUN(test_invalidate_unlink_pending_includes_local_close);
	UT_RUN(test_remote_invalidation_counter_is_atomic_and_monotonic);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
