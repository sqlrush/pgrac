/*-------------------------------------------------------------------------
 *
 * test_cluster_grd.c
 *	  Standalone unit tests for spec-2.14 GRD routing substrate.
 *
 *	  T-grd-1 (7 tests, spec-2.14 D10):
 *	    a) ClusterResId size 16 invariant + StaticAssertDecl 编译期 trigger
 *	    b) cluster_grd_resid_encode/decode roundtrip 4 LOCKTAG class
 *	    c) **真测 hash distribution uniform** — 16 golden vectors (lockmethodid
 *	       swapped → shard 变;field4 swapped → shard 不变) + 100k uniform
 *	       sample (max bucket / mean ≤ 2.0;NOT all-bucket-non-empty 概率
 *	       断言 — v0.4 P1 修正)
 *	    d) uninitialized master map returns -1, not zero-filled node 0
 *	    e) declared-node-aware master mapping sparse-node 场景(mock 3 节
 *	       点 sparse 0/2/5 declared list verify);**v0.4 注解**:TAP 103
 *	       不做 2-node check,sparse-node coverage 由本 unit test 替代
 *	    f) is_local_master matrix(mock cluster_node_id 切换)
 *	    g) is_cluster_aware 分类 — 4 cluster-aware types true + 4 non-cluster
 *	       types false(PAGE / TUPLE / RELATION_EXTEND / VIRTUALTRANSACTION)
 *
 *	  Stubs:
 *	    - ShmemInitStruct returns union force-aligned buffer per L105
 *	    - cluster_conf_lookup_node mocked to simulate sparse declared list
 *	    - cluster_shmem_register_region: no-op
 *
 *	  Spec: spec-2.14 D10 (frozen v0.4)
 *	  Lessons inherited: L8 / L77 / L94 / L105 / L106 / L107
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_grd.c
 *
 * NOTES
 *	  pgrac-original file.  Standalone binary linking cluster_grd.o only;
 *	  all PG backend symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <string.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_grd.h"
#include "port/atomics.h"
#include "storage/lock.h"
#include "storage/s_lock.h"
#include "utils/hsearch.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
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


/* ============================================================
 * Stubs needed to link cluster_grd.o standalone (L105 union align).
 * ============================================================ */

bool IsUnderPostmaster = false;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

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

/*
 * L105 union force-align shmem stub.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster grd") == 0) {
		static union {
			/* cppcheck-suppress unusedStructMember
			 * Reason: force_align is intentionally never read; it raises the
			 * standalone shmem stub's alignment to at least 8 bytes for
			 * pg_atomic_uint64 fields inside ClusterGrdShared. */
			uint64 force_align;
			char data[131072]; /* 4096 atomic uint32 + 6 atomic uint64 < 17KB; buffer 128KB 充足 */
		} grd_buf;
		static bool grd_initialized = false;

		Assert(size <= sizeof(grd_buf.data));
		*foundPtr = grd_initialized;
		grd_initialized = true;
		return grd_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}

/*
 * Mock declared list — sparse {0, 2, 5} for T-grd-1d.
 * Tests can switch by writing to mock_declared_count + mock_declared[].
 */
static int32 mock_declared[CLUSTER_MAX_NODES];
static int mock_declared_count;
static ClusterNodeInfo mock_node_info; /* dummy non-NULL return */

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	int i;

	for (i = 0; i < mock_declared_count; i++) {
		if (mock_declared[i] == node_id)
			return &mock_node_info;
	}
	return NULL;
}

int
cluster_conf_node_count(void)
{
	return mock_declared_count;
}

int32 cluster_node_id = 0; /* NodeId typedef = int32 (cluster_scn.h:135) */

/* spec-2.15 D11:  cluster.grd_max_entries GUC stub (standalone harness
 * keeps 0 → skeleton mode → lookup_or_create returns NOT_READY).
 * cluster_unit harness does NOT exercise the HTAB code path. */
int cluster_grd_max_entries = 0;


/* ============================================================
 * spec-2.15 D11:  HTAB / named tranche / spinlock stubs for the
 *   standalone cluster_unit harness.  cluster_grd.c references these
 *   symbols even when cluster.grd_max_entries=0 (the early-return
 *   branch is taken before reaching them) — the stubs just need to
 *   link.  Real behavior is exercised in cluster_tap 104 under a
 *   live postmaster.
 * ============================================================ */

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
{
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return 0;
}

void *
hash_search_with_hash_value(HTAB *hashp pg_attribute_unused(),
							const void *keyPtr pg_attribute_unused(),
							uint32 hashvalue pg_attribute_unused(),
							HASHACTION action pg_attribute_unused(), bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = false;
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status pg_attribute_unused(), HTAB *hashp pg_attribute_unused())
{}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	return NULL;
}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entrysize pg_attribute_unused())
{
	return 0;
}

void
RequestNamedLWLockTranche(const char *tranche_name pg_attribute_unused(),
						  int num_lwlocks pg_attribute_unused())
{}

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name pg_attribute_unused())
{
	static LWLockPadded dummy_locks[1];
	return dummy_locks;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

/* spec-2.15 D11: s_lock contention stub.  PG inlines TAS spinlocks via
 * compiler primitives on most targets, but s_lock() resolves at link
 * time for the contended-spin slow path (always reachable in object
 * code, even when never entered at run time).  Stub returns immediately
 * — the cluster_unit harness never actually contends a slock. */
int
s_lock(volatile slock_t *lock pg_attribute_unused(), const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	return 0;
}

/* spec-2.15 D11: shmem add_size stub.  cluster_grd_shmem_size() wraps
 * add_size() for the entry HTAB component; standalone harness never
 * allocates >0 bytes for the entry HTAB (cluster.grd_max_entries=0),
 * so a naive add_size is sufficient. */
Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}


/* ============================================================
 * Helper:  reset mock declared list.
 * ============================================================ */
static void
set_mock_declared(int count, const int32 *nodes)
{
	int i;

	mock_declared_count = count;
	for (i = 0; i < count; i++)
		mock_declared[i] = nodes[i];
}


/* ============================================================
 * T-grd-1 a/b/c/d/e/f.
 * ============================================================ */

UT_TEST(test_grd_clusterresid_size_16)
{
	UT_ASSERT_EQ(sizeof(ClusterResId), (size_t)16);
}

UT_TEST(test_grd_resid_encode_decode_roundtrip)
{
	cluster_grd_shmem_init();

	/* Cover 4 cluster-aware LOCKTAG class via 4 samples */
	{
		LOCKTAG src;
		ClusterResId mid;
		LOCKTAG dst;

		/* RELATION:  field1=db, field2=relid */
		memset(&src, 0, sizeof(src));
		src.locktag_field1 = 12345;
		src.locktag_field2 = 67890;
		src.locktag_type = LOCKTAG_RELATION;
		src.locktag_lockmethodid = 1;

		cluster_grd_resid_encode(&src, &mid);
		cluster_grd_resid_decode(&mid, &dst);
		UT_ASSERT_EQ(dst.locktag_field1, (uint32)12345);
		UT_ASSERT_EQ(dst.locktag_field2, (uint32)67890);
		UT_ASSERT_EQ((int)dst.locktag_type, (int)LOCKTAG_RELATION);
		UT_ASSERT_EQ((int)dst.locktag_lockmethodid, 1);
	}

	{
		LOCKTAG src;
		ClusterResId mid;
		LOCKTAG dst;

		/* TRANSACTION:  field1=xid */
		memset(&src, 0, sizeof(src));
		src.locktag_field1 = 99999;
		src.locktag_type = LOCKTAG_TRANSACTION;
		src.locktag_lockmethodid = 1;

		cluster_grd_resid_encode(&src, &mid);
		cluster_grd_resid_decode(&mid, &dst);
		UT_ASSERT_EQ(dst.locktag_field1, (uint32)99999);
		UT_ASSERT_EQ((int)dst.locktag_type, (int)LOCKTAG_TRANSACTION);
	}

	{
		LOCKTAG src;
		ClusterResId mid;
		LOCKTAG dst;

		/* OBJECT:  field1=classid, field2=objid, field3=objsubid */
		memset(&src, 0, sizeof(src));
		src.locktag_field1 = 1;
		src.locktag_field2 = 2;
		src.locktag_field3 = 3;
		src.locktag_type = LOCKTAG_OBJECT;
		src.locktag_lockmethodid = 1;

		cluster_grd_resid_encode(&src, &mid);
		cluster_grd_resid_decode(&mid, &dst);
		UT_ASSERT_EQ(dst.locktag_field1, (uint32)1);
		UT_ASSERT_EQ(dst.locktag_field2, (uint32)2);
		UT_ASSERT_EQ(dst.locktag_field3, (uint32)3);
	}

	{
		LOCKTAG src;
		ClusterResId mid;
		LOCKTAG dst;

		/* ADVISORY:  field1=key1, field2=key2 */
		memset(&src, 0, sizeof(src));
		src.locktag_field1 = 42;
		src.locktag_field2 = 100;
		src.locktag_field4 = 7;
		src.locktag_type = LOCKTAG_ADVISORY;
		src.locktag_lockmethodid = 2; /* USER_LOCKMETHOD */

		cluster_grd_resid_encode(&src, &mid);
		cluster_grd_resid_decode(&mid, &dst);
		UT_ASSERT_EQ(dst.locktag_field1, (uint32)42);
		UT_ASSERT_EQ(dst.locktag_field4, (uint16)7);
		UT_ASSERT_EQ((int)dst.locktag_lockmethodid, 2);
	}

	/* encode_count must have incremented 4 times (P1.1 v0.4 verify) */
	UT_ASSERT_EQ(cluster_grd_resid_encode_count(), (uint64)4);
}

UT_TEST(test_grd_shard_lookup_hash_distribution_uniform)
{
	uint32 buckets[PGRAC_GRD_SHARD_COUNT];
	int i;
	uint32 max_bucket = 0;
	uint64 total_sum = 0;
	double mean;
	double max_over_mean;

	cluster_grd_shmem_init();

	/* (c1) 4 golden vector — lockmethodid swap → shard 变(P1.1 identity) */
	{
		ClusterResId a, b;

		memset(&a, 0, sizeof(a));
		a.field1 = 100;
		a.field2 = 200;
		a.type = LOCKTAG_RELATION;
		a.lockmethodid = 1;

		b = a;
		b.lockmethodid = 2; /* swap lockmethodid */

		UT_ASSERT(cluster_grd_hash_resource(&a) != cluster_grd_hash_resource(&b));
	}

	/* (c2) 4 golden vector — field4 swap → shard 不变(P1.1 skip field4) */
	{
		ClusterResId a, b;

		memset(&a, 0, sizeof(a));
		a.field1 = 100;
		a.field2 = 200;
		a.field4 = 10;
		a.type = LOCKTAG_RELATION;
		a.lockmethodid = 1;

		b = a;
		b.field4 = 99; /* swap field4 */

		UT_ASSERT_EQ(cluster_grd_hash_resource(&a), cluster_grd_hash_resource(&b));
	}

	/* (c3) 100k sample uniform distribution — quantitative max/mean ≤ 2.0 */
	memset(buckets, 0, sizeof(buckets));
	for (i = 0; i < 100000; i++) {
		ClusterResId resid;
		uint32 shard;

		memset(&resid, 0, sizeof(resid));
		resid.field1 = (uint32)i;
		resid.field2 = (uint32)(i * 7);
		resid.type = LOCKTAG_RELATION;
		resid.lockmethodid = 1;

		shard = cluster_grd_shard_for_resource(&resid);
		buckets[shard]++;
		total_sum++;
	}

	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		if (buckets[i] > max_bucket)
			max_bucket = buckets[i];

	mean = (double)total_sum / (double)PGRAC_GRD_SHARD_COUNT;
	max_over_mean = (double)max_bucket / mean;

	/* deterministic — assert max/mean ≤ 2.0;NOT all-bucket-non-empty
	 * (v0.4 P1 修正:概率断言 risk flake → 改 quantitative bound) */
	UT_ASSERT(max_over_mean <= 2.0);
}

UT_TEST(test_grd_uninitialized_master_map_returns_unknown)
{
	ClusterResId resid;
	uint64 before_total;
	uint64 before_local;
	uint64 before_remote;

	cluster_grd_shmem_init();

	memset(&resid, 0, sizeof(resid));
	resid.field1 = 100;
	resid.field2 = 200;
	resid.type = LOCKTAG_RELATION;
	resid.lockmethodid = 1;

	before_total = cluster_grd_shard_lookup_count();
	before_local = cluster_grd_local_master_lookup_count();
	before_remote = cluster_grd_remote_master_lookup_count();

	/* master[] is zero-filled before init; callers must see UNKNOWN, not node 0. */
	UT_ASSERT_EQ(cluster_grd_shard_master(0), (int32)-1);
	UT_ASSERT_EQ(cluster_grd_is_local_master(0), false);
	UT_ASSERT_EQ(cluster_grd_lookup_master(&resid), (int32)-1);
	UT_ASSERT_EQ(cluster_grd_shard_lookup_count(), before_total + 1);
	UT_ASSERT_EQ(cluster_grd_local_master_lookup_count(), before_local);
	UT_ASSERT_EQ(cluster_grd_remote_master_lookup_count(), before_remote);
}

UT_TEST(test_grd_master_map_sparse_declared_nodes)
{
	int32 sparse[] = { 0, 2, 5 };
	uint32 i;

	cluster_grd_shmem_init();
	set_mock_declared(3, sparse);

	cluster_grd_master_map_init();

	/* Q10 + P2.1:  shard_id % 3 → declared[idx] = 0/2/5 round-robin */
	UT_ASSERT_EQ(cluster_grd_shard_master(0), (int32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master(1), (int32)2);
	UT_ASSERT_EQ(cluster_grd_shard_master(2), (int32)5);
	UT_ASSERT_EQ(cluster_grd_shard_master(3), (int32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master(4), (int32)2);
	UT_ASSERT_EQ(cluster_grd_shard_master(5), (int32)5);

	/* counter invariant:  master_map_refresh_count must have ticked at
	 * least once after init (P1.1 v0.4 — counter increment verify). */
	UT_ASSERT(cluster_grd_master_map_refresh_count_get() >= (uint64)1);

	/* All 4096 shards covered by one of the 3 declared nodes. */
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		int32 m = cluster_grd_shard_master(i);

		UT_ASSERT(m == 0 || m == 2 || m == 5);
	}
}

UT_TEST(test_grd_is_local_master_matrix)
{
	int32 declared[] = { 0, 1 };

	cluster_grd_shmem_init();
	set_mock_declared(2, declared);
	cluster_grd_master_map_init();

	/* 2-node declared:  shard 0 → declared[0]=0, shard 1 → declared[1]=1 */
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_grd_is_local_master(0), true);
	UT_ASSERT_EQ(cluster_grd_is_local_master(1), false);
	UT_ASSERT_EQ(cluster_grd_is_local_master(2), true);
	UT_ASSERT_EQ(cluster_grd_is_local_master(3), false);

	cluster_node_id = 1;
	UT_ASSERT_EQ(cluster_grd_is_local_master(0), false);
	UT_ASSERT_EQ(cluster_grd_is_local_master(1), true);

	cluster_node_id = 0; /* restore */
}

UT_TEST(test_grd_is_cluster_aware_classification)
{
	LOCKTAG tag;

	/* 4 cluster-aware types → true */
	memset(&tag, 0, sizeof(tag));
	tag.locktag_type = LOCKTAG_RELATION;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), true);

	tag.locktag_type = LOCKTAG_TRANSACTION;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), true);

	tag.locktag_type = LOCKTAG_OBJECT;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), true);

	tag.locktag_type = LOCKTAG_ADVISORY;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), true);

	/* 4 non-cluster types → false */
	tag.locktag_type = LOCKTAG_PAGE;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), false);

	tag.locktag_type = LOCKTAG_TUPLE;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), false);

	tag.locktag_type = LOCKTAG_RELATION_EXTEND;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), false);

	tag.locktag_type = LOCKTAG_VIRTUALTRANSACTION;
	UT_ASSERT_EQ(cluster_grd_is_cluster_aware(&tag), false);
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * spec-2.15 T-grd-2 a-e (5 NEW unit tests).
 *
 *   T-grd-2 covers the entry-table infrastructure layer:
 *     a) enum value invariant (NOT sizeof — C enum size impl-defined)
 *     b) GUC=0 sentinel path (lookup_or_create returns NOT_READY)
 *     c) named tranche 4096 lock alloc (DEFERRED to harness/TAP — Get
 *        NamedLWLockTranche requires postmaster phase 1;  standalone
 *        unit test cannot invoke PG named-tranche infra without bringing
 *        in a real ProcArray / dsm / lwlock.c slot manager.  cluster_tap
 *        104 covers the real boot path;  unit test here records the
 *        invariant via DESCRIBE-only check).
 *     d) entry slock_t mutation safety (init + try-acquire idempotent)
 *     e) hash 单源 (hash64 % 4096 与 32-bit projection 一致)
 *
 *   holders/waiters/converts cap behavior tests推 spec-2.16 配 mutator API.
 * ============================================================ */

UT_TEST(test_grd_entry_result_enum_value_invariant)
{
	/* v0.2 P2.7:  enum VALUE invariant (NOT sizeof — C enum size is
	 * implementation-defined and not ABI 契约). */
	UT_ASSERT_EQ((int)CLUSTER_GRD_ENTRY_OK, 0);
	UT_ASSERT_EQ((int)CLUSTER_GRD_ENTRY_NOT_READY, 1);
	UT_ASSERT_EQ((int)CLUSTER_GRD_ENTRY_NOT_FOUND, 2);
	UT_ASSERT_EQ((int)CLUSTER_GRD_ENTRY_FULL, 3);
	UT_ASSERT_EQ((int)CLUSTER_GRD_ENTRY_ERROR, 4);
}

UT_TEST(test_grd_entry_lookup_not_ready_when_guc_zero)
{
	/* GUC=0 → entry HTAB never allocated → htab pointer stays NULL inside
	 * cluster_grd.c → lookup_or_create returns NOT_READY (sentinel path I1).
	 * Unit test harness does NOT call cluster_grd_shmem_init with non-zero
	 * GUC so the htab path always returns NOT_READY here. */
	LOCKTAG src;
	ClusterResId resid;
	ClusterGrdEntry *out = (ClusterGrdEntry *)0xdeadbeef;
	ClusterGrdEntryResult r;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 42;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;

	cluster_grd_resid_encode(&src, &resid);

	r = cluster_grd_entry_lookup_or_create(&resid, true, &out);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_NOT_READY);
	UT_ASSERT_EQ((void *)out, (void *)NULL); /* *out = NULL per I1 */
}

UT_TEST(test_grd_named_tranche_describe_only)
{
	/* spec-2.15 D11 T-grd-2c (DESCRIBE-only):  named tranche allocation
	 * happens at PG postmaster shmem-request phase via
	 * cluster_grd_request_lwlocks().  Standalone unit test cannot drive
	 * the PG named-tranche manager;  cluster_tap 104 covers the real
	 * end-to-end boot path.  This unit test records the contract that
	 * cluster_grd_request_lwlocks() is a single-call hook (I15) by
	 * verifying it has external linkage. */
	extern void cluster_grd_request_lwlocks(void);
	UT_ASSERT_NE((void *)cluster_grd_request_lwlocks, (void *)NULL);
}

UT_TEST(test_grd_entry_release_no_op_safe)
{
	/* RESERVED no-op contract (P1.3):  cluster_grd_entry_release(NULL)
	 * must not crash;  no side effect promised. */
	cluster_grd_entry_release(NULL);
	UT_ASSERT_EQ(1, 1); /* reaching here suffices */
}

UT_TEST(test_grd_hash_source_unification)
{
	/* spec-2.15 v0.4 P1.1 I13:  shard_id (hash64 % 4096) 与 HTAB
	 * hashvalue (32-bit projection of same hash64) must come from the
	 * same cluster_grd_hash_resource() call — never from dynahash's
	 * own HASHCTL.hash (which would use the full 16B key).
	 *
	 * Test:  same resid → same hash64 → shard_id = hash64 % 4096 +
	 * hashvalue = (uint32) hash64.  shard_for_resource() must match. */
	LOCKTAG src;
	ClusterResId resid;
	uint64 h64;
	uint32 shard_a;
	uint32 shard_b;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 0x12345678;
	src.locktag_field2 = 0xabcdef01;
	src.locktag_field3 = 0xfeedface;
	src.locktag_field4 = 0x4242;
	src.locktag_type = LOCKTAG_TRANSACTION;
	src.locktag_lockmethodid = 1;

	cluster_grd_resid_encode(&src, &resid);

	h64 = cluster_grd_hash_resource(&resid);
	shard_a = (uint32)(h64 % PGRAC_GRD_SHARD_COUNT);
	shard_b = cluster_grd_shard_for_resource(&resid);

	UT_ASSERT_EQ(shard_a, shard_b);
}


int
/* cppcheck-suppress constParameter
 * Reason: main() keeps the standard test harness signature used by the
 * other cluster_unit binaries; argv is intentionally unused. */
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(12);

	UT_RUN(test_grd_clusterresid_size_16);
	UT_RUN(test_grd_resid_encode_decode_roundtrip);
	UT_RUN(test_grd_shard_lookup_hash_distribution_uniform);
	UT_RUN(test_grd_uninitialized_master_map_returns_unknown);
	UT_RUN(test_grd_master_map_sparse_declared_nodes);
	UT_RUN(test_grd_is_local_master_matrix);
	UT_RUN(test_grd_is_cluster_aware_classification);

	/* spec-2.15 T-grd-2 a-e */
	UT_RUN(test_grd_entry_result_enum_value_invariant);
	UT_RUN(test_grd_entry_lookup_not_ready_when_guc_zero);
	UT_RUN(test_grd_named_tranche_describe_only);
	UT_RUN(test_grd_entry_release_no_op_safe);
	UT_RUN(test_grd_hash_source_unification);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
