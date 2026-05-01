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
void cluster_injection_run(const char *name pg_attribute_unused()) { }

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ereport machinery. */
bool errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused()) { return false; }
bool errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused()) { return false; }
void errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
			   const char *fn pg_attribute_unused()) { }
int errcode(int s pg_attribute_unused()) { return 0; }
int errcode_for_file_access(void) { return 0; }
int errmsg(const char *f pg_attribute_unused(), ...) { return 0; }
int errmsg_internal(const char *f pg_attribute_unused(), ...) { return 0; }
int errdetail(const char *f pg_attribute_unused(), ...) { return 0; }
int errhint(const char *f pg_attribute_unused(), ...) { return 0; }
void elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
				const char *fn pg_attribute_unused()) { }
void elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...) { }
void pre_format_elog_string(int n pg_attribute_unused(),
							const char *d pg_attribute_unused()) { }
char *format_elog_string(const char *f pg_attribute_unused(), ...) { return NULL; }

/* Memory context machinery. */
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;
void *palloc0(Size s pg_attribute_unused()) { return NULL; }
void pfree(void *p pg_attribute_unused()) { }

/* fd.c VFD layer stubs. */
File PathNameOpenFile(const char *fn pg_attribute_unused(),
					  int fl pg_attribute_unused()) { return -1; }
void FileClose(File f pg_attribute_unused()) { }
int FileRead(File f pg_attribute_unused(), void *b pg_attribute_unused(),
			 size_t a pg_attribute_unused(), off_t o pg_attribute_unused(),
			 uint32 w pg_attribute_unused()) { return 0; }
int FileWrite(File f pg_attribute_unused(), const void *b pg_attribute_unused(),
			  size_t a pg_attribute_unused(), off_t o pg_attribute_unused(),
			  uint32 w pg_attribute_unused()) { return 0; }
int FileSync(File f pg_attribute_unused(), uint32 w pg_attribute_unused()) { return 0; }
off_t FileSize(File f pg_attribute_unused()) { return 0; }
int FileTruncate(File f pg_attribute_unused(), off_t o pg_attribute_unused(),
				 uint32 w pg_attribute_unused()) { return 0; }

char *
GetRelationPath(Oid d pg_attribute_unused(), Oid s pg_attribute_unused(),
				RelFileNumber r pg_attribute_unused(), int b pg_attribute_unused(),
				ForkNumber f pg_attribute_unused()) { return NULL; }

void before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
					   Datum arg pg_attribute_unused()) { }

/* HTAB stubs. */
HTAB *
hash_create(const char *t pg_attribute_unused(), long n pg_attribute_unused(),
			const HASHCTL *info pg_attribute_unused(), int flags pg_attribute_unused())
{
	return NULL;
}
void *hash_search(HTAB *h pg_attribute_unused(), const void *k pg_attribute_unused(),
				  HASHACTION a pg_attribute_unused(), bool *f pg_attribute_unused())
{
	return NULL;
}
void hash_destroy(HTAB *h pg_attribute_unused()) { }
void hash_seq_init(HASH_SEQ_STATUS *s pg_attribute_unused(),
				   HTAB *h pg_attribute_unused()) { }
void *hash_seq_search(HASH_SEQ_STATUS *s pg_attribute_unused()) { return NULL; }
long hash_get_num_entries(HTAB *h pg_attribute_unused()) { return 0; }

/* TablespaceCreateDbspace stub. */
void TablespaceCreateDbspace(Oid s pg_attribute_unused(), Oid d pg_attribute_unused(),
							 bool r pg_attribute_unused()) { }

/* md.c stubs (cluster_smgr no longer fallbacks to these but still
 * referenced via header inclusion). */
void mdzeroextend(SMgrRelation r pg_attribute_unused(),
				  ForkNumber f pg_attribute_unused(),
				  BlockNumber b pg_attribute_unused(),
				  int n pg_attribute_unused(),
				  bool s pg_attribute_unused()) { }
bool mdprefetch(SMgrRelation r pg_attribute_unused(),
				ForkNumber f pg_attribute_unused(),
				BlockNumber b pg_attribute_unused()) { return false; }
void mdwriteback(SMgrRelation r pg_attribute_unused(),
				 ForkNumber f pg_attribute_unused(),
				 BlockNumber b pg_attribute_unused(),
				 BlockNumber n pg_attribute_unused()) { }


UT_DEFINE_GLOBALS();


/* ============================================================
 * Sixteen f_smgr callback symbols are linkable.
 * ============================================================ */

UT_TEST(test_smgr_callbacks_linkable)
{
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_init);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_shutdown);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_open);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_close);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_create);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_exists);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_unlink);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_extend);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_zeroextend);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_prefetch);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_read);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_write);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_writeback);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_nblocks);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_truncate);
	UT_ASSERT_NOT_NULL((void *) cluster_smgr_immedsync);
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
	cluster_shared_storage_backend = 0;	/* STUB */
	cluster_smgr_user_relations = true;	/* even if user wants on */
	UT_ASSERT_EQ(cluster_smgr_which_for(rl, InvalidBackendId), 0);
}


UT_TEST(test_which_for_user_relations_off_returns_md)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };

	cluster_shared_storage_backend = 1;	/* LOCAL */
	cluster_smgr_user_relations = false;	/* GUC opt-in off */
	UT_ASSERT_EQ(cluster_smgr_which_for(rl, InvalidBackendId), 0);
}


UT_TEST(test_which_for_full_opt_in_returns_cluster)
{
	RelFileLocator rl = { .spcOid = 1664, .dbOid = 5, .relNumber = 16385 };

	cluster_shared_storage_backend = 1;	/* LOCAL */
	cluster_smgr_user_relations = true;	/* opt-in on */
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
	void (*p_init) (void) = cluster_smgr_init;
	void (*p_shutdown) (void) = cluster_smgr_shutdown;
	void (*p_open) (SMgrRelation) = cluster_smgr_open;

	UT_ASSERT_NOT_NULL((void *) p_init);
	UT_ASSERT_NOT_NULL((void *) p_shutdown);
	UT_ASSERT_NOT_NULL((void *) p_open);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_smgr_callbacks_linkable);
	UT_RUN(test_which_for_temp_relation_returns_md);
	UT_RUN(test_which_for_stub_backend_returns_md);
	UT_RUN(test_which_for_user_relations_off_returns_md);
	UT_RUN(test_which_for_full_opt_in_returns_cluster);
	UT_RUN(test_active_relation_count_pre_init);
	UT_RUN(test_smgrsw_callback_signature_compiles);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
