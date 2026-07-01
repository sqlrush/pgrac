/*-------------------------------------------------------------------------
 *
 * test_cluster_conf.c
 *	  Compile-time / link-level invariants for the cluster topology
 *	  configuration framework introduced in stage 0.19.
 *
 *	  Stage 0.19 ships the parser + shmem + SRF skeleton.  The parser
 *	  itself is exercised end-to-end on a real PG instance by
 *	  cluster_tap t/013_conf.pl (writes pgrac.conf, restarts server,
 *	  inspects pg_cluster_nodes view).  This unit test only locks the
 *	  shmem ABI (sizeof / magic / max nodes) and verifies that the
 *	  Stage 2+ symbol surface (cluster_conf_load / lookup_node /
 *	  role helpers / SRF) is linkable.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_conf.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_conf.o standalone
 *	  pulls in references to ereport / ShmemInitStruct / AllocateFile /
 *	  cluster_node_id / cluster_config_file; those are stubbed locally
 *	  below so the binary can run without the full PG backend.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_scn.h" /* spec-2.1 D5: SCN_NODE_ID_VALID range tests */

/*
 * postgres.h transitively pulls in port.h which redirects printf etc.
 * Standalone unit-test binaries do not link libpgport, so undo the
 * redirection before pulling in unit_test.h.
 */
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
 * Stubs needed to link cluster_conf.o standalone.  The unit tests below
 * only take function addresses and read constants; none of these stubs
 * is ever entered at runtime.
 * ----------
 */
#include "storage/shmem.h"
#include "storage/fd.h"
#include "utils/elog.h"

int cluster_node_id = -1;
char *cluster_config_file = (char *)"pgrac.conf";
bool cluster_allow_single_node = true; /* spec-2.1 D1; storage stub matches default */

/*
 * spec-2.1 Hardening v1.0.1 D-H7 -- F2 fix in cluster_conf.c::
 * cluster_conf_load adds an extern reference to cluster_enabled
 * (defensive guard).  Stub matches GUC default; tests do not
 * exercise the !cluster_enabled early-return path (verified at
 * TAP layer L11).  v1.0.2 D-I5 (codex review P2 post-Sprint B):
 * moved the multi-line note above the declaration so the
 * continuation lines use tabs (was inline with space-indented
 * continuation, breaking git diff --check / PG style).
 */
bool cluster_enabled = true;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	/* Stub: real impl in src/backend/storage/ipc/shmem.c */
	return NULL;
}

void
RequestAddinShmemSpace(Size size pg_attribute_unused())
{
	/* Stub: real impl in src/backend/storage/ipc/ipci.c */
}

FILE *
AllocateFile(const char *name pg_attribute_unused(), const char *mode pg_attribute_unused())
{
	/* Stub: real impl in src/backend/storage/file/fd.c */
	return NULL;
}

int
FreeFile(FILE *file pg_attribute_unused())
{
	/* Stub: real impl in src/backend/storage/file/fd.c */
	return 0;
}

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
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errcontext_msg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
set_errcontext_domain(const char *domain pg_attribute_unused())
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

/*
 * Funcapi / tuplestore / cstring_to_text stubs needed by the SRF body.
 * Same approach as test_cluster_views.c (stage 0.16).
 */
#include "funcapi.h"
#include "utils/builtins.h"

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/fmgr/funcapi.c */
}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/sort/tuplestore.c */
}

text *
cstring_to_text(const char *s pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/adt/varlena.c */
	return NULL;
}

/*
 * Stage 0.27 injection-framework symbols used by cluster_conf.o
 * (cluster_conf_load expands CLUSTER_INJECTION_POINT).
 * cluster_inject.o is not linked here; stub the symbols.
 */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Shmem ABI invariants (compile-time anchors).
 * ============================================================ */

UT_TEST(test_max_nodes_constant)
{
	UT_ASSERT_EQ(CLUSTER_MAX_NODES, 128);
}

UT_TEST(test_conf_magic_constant)
{
	UT_ASSERT_EQ(PGRAC_CLUSTER_CONF_MAGIC, (uint32)0x434F4E46);
}

UT_TEST(test_node_role_int32_sized)
{
	UT_ASSERT_EQ(sizeof(ClusterNodeRole), sizeof(int32));
}

UT_TEST(test_node_info_alignment)
{
	/*
	 * ClusterNodeInfo is part of the shmem ABI.  We do not lock the
	 * exact byte size (compilers may pad differently), but the array
	 * element must be a multiple of 8 bytes for the indexed access in
	 * Stage 2+ to remain branchless.
	 */
	UT_ASSERT_EQ(sizeof(ClusterNodeInfo) % 8, 0);
}

UT_TEST(test_rdma_node_field_budgets)
{
	UT_ASSERT_EQ(CLUSTER_NODE_RDMA_ADDR_LEN, 64);
	UT_ASSERT_EQ(CLUSTER_NODE_RDMA_GID_LEN, 40);
	UT_ASSERT(sizeof(ClusterConf) <= 65536);
}

UT_TEST(test_conf_size_under_64k)
{
	UT_ASSERT(sizeof(ClusterConf) <= 65536);
}


/* ============================================================
 * Role helpers: bidirectional mapping.
 * ============================================================ */

UT_TEST(test_role_to_string_primary)
{
	UT_ASSERT_STR_EQ(cluster_conf_role_to_string(CLUSTER_ROLE_PRIMARY), "primary");
}

UT_TEST(test_role_to_string_standby_arbiter)
{
	UT_ASSERT_STR_EQ(cluster_conf_role_to_string(CLUSTER_ROLE_STANDBY), "standby");
	UT_ASSERT_STR_EQ(cluster_conf_role_to_string(CLUSTER_ROLE_ARBITER), "arbiter");
}

UT_TEST(test_role_from_string_valid)
{
	ClusterNodeRole role;

	UT_ASSERT(cluster_conf_role_from_string("primary", &role));
	UT_ASSERT_EQ(role, CLUSTER_ROLE_PRIMARY);

	UT_ASSERT(cluster_conf_role_from_string("standby", &role));
	UT_ASSERT_EQ(role, CLUSTER_ROLE_STANDBY);

	UT_ASSERT(cluster_conf_role_from_string("arbiter", &role));
	UT_ASSERT_EQ(role, CLUSTER_ROLE_ARBITER);
}

UT_TEST(test_role_from_string_invalid)
{
	ClusterNodeRole role = CLUSTER_ROLE_PRIMARY;

	UT_ASSERT(!cluster_conf_role_from_string("master", &role));
	UT_ASSERT(!cluster_conf_role_from_string("", &role));
	UT_ASSERT(!cluster_conf_role_from_string(NULL, &role));
	UT_ASSERT(!cluster_conf_role_from_string("primary", NULL));
}


/* ============================================================
 * Symbol linkability -- Stage 2+ subsystems and SQL-level callers
 * will resolve to these.
 * ============================================================ */

UT_TEST(test_load_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_conf_load);
}

UT_TEST(test_lookup_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_conf_lookup_node);
}

UT_TEST(test_node_count_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_conf_node_count);
}

UT_TEST(test_get_nodes_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_nodes);
}


/* ============================================================
 * spec-2.1 D5: cluster.allow_single_node + SCN_NODE_ID_VALID range
 * ============================================================ */

UT_TEST(test_allow_single_node_default)
{
	/*
	 * Default storage value is true (spec-2.1 D1; Stage 2.1 backward-
	 * compat).  GUC registration in cluster_guc.c boot value matches.
	 */
	UT_ASSERT(cluster_allow_single_node);
}

UT_TEST(test_allow_single_node_size)
{
	UT_ASSERT_EQ(sizeof(cluster_allow_single_node), sizeof(bool));
}

UT_TEST(test_scn_node_id_valid_unset)
{
	/* -1 sentinel (unset) is invalid (spec-1.15 / spec-1.16 contract). */
	UT_ASSERT(!SCN_NODE_ID_VALID(-1));
}

UT_TEST(test_scn_node_id_valid_range)
{
	/* 0..127 valid (AD-012 example 10 + spec-1.15 §2.5). */
	UT_ASSERT(SCN_NODE_ID_VALID(0));
	UT_ASSERT(SCN_NODE_ID_VALID(63));
	UT_ASSERT(SCN_NODE_ID_VALID(SCN_MAX_VALID_NODE_ID));
}

UT_TEST(test_scn_node_id_valid_overflow)
{
	/* 128+ reserved (Stage 2+ extension space; spec-1.15 §2.5). */
	UT_ASSERT(!SCN_NODE_ID_VALID(128));
	UT_ASSERT(!SCN_NODE_ID_VALID(255));
	UT_ASSERT(!SCN_NODE_ID_VALID(1000));
}


/* ============================================================
 * spec-2.1 Hardening v1.0.1 D-H7: cluster_enabled extern linkable
 *
 * F2 fix in cluster_shmem.c (caller gate) and cluster_conf.c
 * (callee defensive guard) both reference cluster_enabled.  This
 * test verifies the extern symbol resolves at link time so future
 * regressions are caught at unit-test build, not at TAP runtime.
 * Cross-ref: lessons L48 meta (lesson SSOT enforce 不能假设单点
 * -- need compile-time + CI gate, not just docstring).
 * ============================================================ */

UT_TEST(test_cluster_enabled_linkable)
{
	/*
	 * Stub above sets cluster_enabled = true.  We are only checking
	 * that the symbol resolves; runtime behaviour of the F2 guard is
	 * verified at TAP layer (072 L11).
	 */
	UT_ASSERT(cluster_enabled);
}


int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_max_nodes_constant);
	UT_RUN(test_conf_magic_constant);
	UT_RUN(test_node_role_int32_sized);
	UT_RUN(test_node_info_alignment);
	UT_RUN(test_rdma_node_field_budgets);
	UT_RUN(test_conf_size_under_64k);
	UT_RUN(test_role_to_string_primary);
	UT_RUN(test_role_to_string_standby_arbiter);
	UT_RUN(test_role_from_string_valid);
	UT_RUN(test_role_from_string_invalid);
	UT_RUN(test_load_linkable);
	UT_RUN(test_lookup_linkable);
	UT_RUN(test_node_count_linkable);
	UT_RUN(test_get_nodes_srf_linkable);
	UT_RUN(test_allow_single_node_default);
	UT_RUN(test_allow_single_node_size);
	UT_RUN(test_scn_node_id_valid_unset);
	UT_RUN(test_scn_node_id_valid_range);
	UT_RUN(test_scn_node_id_valid_overflow);
	UT_RUN(test_cluster_enabled_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
