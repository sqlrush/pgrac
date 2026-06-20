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
#include "cluster/cluster_gcs.h"	   /* spec-4.7 D2 (L238) — cluster_gcs_lookup_master proto */
#include "cluster/cluster_gcs_block.h" /* spec-4.7 D2 (L238) — block re-declare scan/send protos */
#include "cluster/cluster_ges_mode.h"  /* spec-5.1b — frozen matrix + convert classification */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_reconfig.h"		 /* spec-4.6 D1 — ReconfigEvent stub type */
#include "cluster/cluster_thread_recovery.h" /* spec-4.11 D3 (L238) — gate_unfreeze proto */
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
			char data[131072]; /* 3×4096 atomic uint32 arrays + counters < 50KB
								* (spec-4.6 adds master_generation[] + shard_phase[]);
								* buffer 128KB 充足 */
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
volatile sig_atomic_t cluster_ges_bast_pending = 0;
volatile sig_atomic_t cluster_ges_cancel_pending = 0;

/* spec-2.17 L104 stub:  MyProc reference for bast_handler.  Tests don't
 * exercise BAST handler runtime;  unit test only verifies symbol linkage. */
struct PGPROC *MyProc = NULL;

/* spec-2.16 D8 L104 stubs:  cluster_grd_lmon_tick_dead_sweep depends on
 * cluster_cssd_get_dead_generation + cluster_cssd_get_peer_state.  Mock
 * to default-ALIVE (no sweep triggered). */
uint64
cluster_cssd_get_dead_generation(void)
{
	return 0;
}

typedef enum { CSSD_PEER_ALIVE = 0, CSSD_PEER_SUSPECTED = 1, CSSD_PEER_DEAD = 2 } _stub_peer_state;
int /* ClusterCssdPeerState */
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return 0; /* CLUSTER_CSSD_PEER_ALIVE */
}

/* spec-2.15 D11:  cluster.grd_max_entries GUC stub.  Most tests keep 0
 * → skeleton mode → lookup_or_create returns NOT_READY; the soft-cap
 * regression test sets 1 and drives a tiny fake HTAB path. */
int cluster_grd_max_entries = 0;

/* spec-4.6 D2 stub:  cluster_grd_lookup_master_gen forwards the LMS
 * wire routing token verbatim (Q3-C).  Settable so the unit test can
 * assert verbatim pass-through. */
static uint64 mock_lms_shard_master_generation = 0;
uint64
cluster_lms_get_shard_master_generation(void)
{
	return mock_lms_shard_master_generation;
}

/* spec-4.6 D1 stubs:  the recovery tick (P0-P7) consumes reconfig
 * events / epoch reads / ProcArray walks / ProcSignal broadcast.
 * Standalone fixture pins them inert (cluster_enabled=false → the
 * tick early-returns;  end-to-end coverage is TAP t/249).  Types are
 * primitive-equivalent;  the real headers are not pulled in. */
bool cluster_enabled = false;
int cluster_grd_rebuild_timeout_ms = 5000;
volatile sig_atomic_t cluster_grd_redeclare_pending = false;
int MyProcPid = 0;

uint64
cluster_epoch_get_current(void)
{
	return 0;
}

/*
 * spec-4.7 D2 (L238) — cluster_grd.o's reconfig tick now references the block
 * re-declare scan/send (grd_block_redeclare_step / _cb).  This test exercises
 * only the GES/GRD remaster path, so stub them: scan_chunk returns start
 * unchanged (no buffers scanned → the callback is never invoked → send/lookup
 * are never reached in this test).
 */
int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return 0;
}
void
cluster_gcs_block_send_redeclare(BufferTag tag pg_attribute_unused(),
								 uint8 held_mode pg_attribute_unused(),
								 XLogRecPtr page_lsn pg_attribute_unused(),
								 uint64 cluster_epoch pg_attribute_unused(),
								 int master_node pg_attribute_unused())
{}
/* spec-4.7 D2/D7 (P0 fix) — controllable scan: fake_scan_nbuffers == 0 (default)
 * means "no buffers → instant done" (the no-op other tests expect);  a test
 * raises it to model a multi-tick scan so grd_block_redeclare_scan_complete
 * stays false until the cursor reaches it. */
static int fake_scan_nbuffers = 0;
int
cluster_bufmgr_redeclare_scan_chunk(int start_buf, int max_scan,
									ClusterGcsRedeclareCallback cb pg_attribute_unused(),
									void *arg pg_attribute_unused())
{
	int end;

	if (start_buf >= fake_scan_nbuffers)
		return start_buf; /* whole (fake) pool scanned — cursor unchanged = done */
	end = start_buf + max_scan;
	if (end > fake_scan_nbuffers)
		end = fake_scan_nbuffers;
	return end;
}

void
cluster_reconfig_get_last_event(ReconfigEvent *out)
{
	memset(out, 0, sizeof(*out));
}

/* spec-4.11 D3 stub:  cluster_grd.c's WAIT_CLUSTER->IDLE transition consults the
 * thread-recovery unfreeze gate before P7.  These tests drive the GES/GRD
 * remaster FSM, not online thread recovery, so the gate is out of scope -> no-op
 * false (P7 unfreeze proceeds exactly as before the gate was added). */
bool
cluster_thread_recovery_gate_unfreeze(const uint64 *dead_bitmap pg_attribute_unused(),
									  int nwords pg_attribute_unused())
{
	return false;
}

/* spec-4.11 3b-4b Part 3 stub:  cluster_grd.c's WAIT_CLUSTER tick now launches
 * the online thread-recovery executor for in-scope dead origins.  These tests
 * drive the GES/GRD remaster FSM, not online thread recovery, so the launch is
 * out of scope -> a no-op (no worker is registered). */
void
cluster_thread_recovery_launch_workers(const uint64 *dead pg_attribute_unused(),
									   int nwords pg_attribute_unused(),
									   uint64 episode_epoch pg_attribute_unused())
{}

struct PGPROC *
BackendIdGetProc(int backendID pg_attribute_unused())
{
	return NULL;
}

int
SendProcSignal(int pid pg_attribute_unused(), int reason pg_attribute_unused(),
			   int backendId pg_attribute_unused())
{
	return 0;
}

int64
GetCurrentTimestamp(void)
{
	return 0;
}

void
cluster_grd_redeclare_all_registered(void)
{}

/* spec-4.6 P0#3 stub:  REDECLARE_DONE broadcast enqueues to the outbound
 * ring.  Standalone fixture has no ring; no-op success.  (peer-state stub
 * for the dead-peer skip already exists above.) */
bool
cluster_grd_outbound_enqueue_backend_request(uint32 dest_node_id pg_attribute_unused(),
											 const void *payload pg_attribute_unused(),
											 uint32 payload_len pg_attribute_unused())
{
	return true;
}

#define FAKE_GRD_HTAB_MAX_ENTRIES 4
#define FAKE_GRD_HTAB_ENTRY_BYTES 4096

static int fake_grd_htab_token;
static int fake_grd_htab_count;
static int fake_grd_htab_seq_index;
static Size fake_grd_entrysize;
static union {
	uint64 force_align;
	char data[FAKE_GRD_HTAB_MAX_ENTRIES][FAKE_GRD_HTAB_ENTRY_BYTES];
} fake_grd_htab_entries;

static void
reset_fake_grd_htab(void)
{
	fake_grd_htab_count = 0;
	fake_grd_htab_seq_index = 0;
	fake_grd_entrysize = 0;
	memset(&fake_grd_htab_entries, 0, sizeof(fake_grd_htab_entries));
}


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
	Assert(infoP != NULL);
	Assert(infoP->entrysize <= FAKE_GRD_HTAB_ENTRY_BYTES);

	fake_grd_entrysize = infoP->entrysize;
	fake_grd_htab_count = 0;
	memset(&fake_grd_htab_entries, 0, sizeof(fake_grd_htab_entries));
	return (HTAB *)&fake_grd_htab_token;
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return fake_grd_htab_count;
}

void *
hash_search_with_hash_value(HTAB *hashp pg_attribute_unused(),
							const void *keyPtr pg_attribute_unused(),
							uint32 hashvalue pg_attribute_unused(),
							HASHACTION action pg_attribute_unused(), bool *foundPtr)
{
	int i;

	Assert(keyPtr != NULL);
	Assert(fake_grd_entrysize > 0);

	for (i = 0; i < fake_grd_htab_count; i++) {
		char *entry = fake_grd_htab_entries.data[i];

		if (memcmp(entry, keyPtr, sizeof(ClusterResId)) == 0) {
			if (action == HASH_REMOVE) {
				if (foundPtr != NULL)
					*foundPtr = true;
				if (i < fake_grd_htab_count - 1)
					memcpy(fake_grd_htab_entries.data[i],
						   fake_grd_htab_entries.data[fake_grd_htab_count - 1], fake_grd_entrysize);
				memset(fake_grd_htab_entries.data[fake_grd_htab_count - 1], 0, fake_grd_entrysize);
				fake_grd_htab_count--;
				return entry;
			}
			if (foundPtr != NULL)
				*foundPtr = true;
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;

	if (action == HASH_FIND)
		return NULL;

	if (action == HASH_ENTER_NULL) {
		char *entry;

		if (fake_grd_htab_count >= FAKE_GRD_HTAB_MAX_ENTRIES)
			return NULL;

		entry = fake_grd_htab_entries.data[fake_grd_htab_count++];
		memset(entry, 0, fake_grd_entrysize);
		memcpy(entry, keyPtr, sizeof(ClusterResId));
		return entry;
	}

	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status pg_attribute_unused(), HTAB *hashp pg_attribute_unused())
{
	fake_grd_htab_seq_index = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	if (fake_grd_htab_seq_index >= fake_grd_htab_count)
		return NULL;
	return fake_grd_htab_entries.data[fake_grd_htab_seq_index++];
}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entrysize pg_attribute_unused())
{
	if (num_entries <= 0 || entrysize == 0)
		return 0;
	return (Size)num_entries * entrysize + 1024;
}

void
RequestNamedLWLockTranche(const char *tranche_name pg_attribute_unused(),
						  int num_lwlocks pg_attribute_unused())
{}

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name pg_attribute_unused())
{
	static LWLockPadded dummy_locks[PGRAC_GRD_SHARD_COUNT];
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

/* spec-2.23 D6 stub: PG exported DoLockModesConflict — cluster_grd.c
 * uses this in enqueue_or_grant / release_and_pop_compatible_waiter.
 * Unit test never exercises conflict logic; return false so all
 * mode pairs read as compatible (skeleton scaffolding behavior). */
bool
DoLockModesConflict(int a pg_attribute_unused(), int b pg_attribute_unused())
{
	return false;
}

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

/* spec-2.24 D14/D16 stub audit. */
void
cluster_grd_outbound_enqueue_cleanup_release(uint32 d pg_attribute_unused(),
											 const void *p pg_attribute_unused(),
											 uint16 l pg_attribute_unused())
{}
void
cluster_lmd_cleanup_on_backend_exit_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cleanup_skip_other_owner_count_inc(uint64 d pg_attribute_unused())
{}
uint64
cluster_pcm_lock_clear_pending_x_for_node(int32 dead_node pg_attribute_unused())
{
	return 0;
}

/* PG runtime stubs needed by D8 cluster_grd_sweep_local_stale_procnos. */
LWLockPadded *MainLWLockArray = NULL;
int MaxBackends = 100;
typedef struct PROC_HDR_STUB {
	void *allProcs;
	int allProcCount;
} PROC_HDR_STUB;
static PROC_HDR_STUB stub_proc_global = { NULL, 0 };
void *ProcGlobal = &stub_proc_global;
void *
palloc0(Size sz)
{
	static char buf[256];
	(void)sz;
	memset(buf, 0, sizeof(buf));
	return buf;
}
void
pfree(void *p pg_attribute_unused())
{}

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
 * spec-2.15 T-grd-2 a-f (6 NEW unit tests).
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
 *     f) existing entry lookup survives soft cap; only new entries FULL
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
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;

	cluster_grd_resid_encode(&src, &resid);

	h64 = cluster_grd_hash_resource(&resid);
	shard_a = (uint32)(h64 % PGRAC_GRD_SHARD_COUNT);
	shard_b = cluster_grd_shard_for_resource(&resid);

	UT_ASSERT_EQ(shard_a, shard_b);
}

/* ============================================================
 * spec-2.26 T-grd-N..N+2 — LOCKTAG_TRANSACTION ClusterResId wrapper +
 * cleanup tests.
 *
 *	T-grd-N    (encode/decode round-trip)  — HC40 invariant
 *	T-grd-N+1  (encoder invalid node defense) — HC47
 *	T-grd-N+2  (cleanup_on_node_dead TRANSACTION entries) — HC44
 * ============================================================ */

UT_TEST(test_grd_resid_encode_transaction_roundtrip)
{
	/* spec-2.26 T-grd-N — HC40 round-trip invariant for LOCKTAG_TRANSACTION.
	 *
	 *  16-node × multi-xid boundary matrix.  Each iteration encodes a
	 *  PG-style LOCKTAG_TRANSACTION (field1 = xid, field2/3/4 = 0) plus
	 *  origin_node_id supplied through the local cluster_node_id global
	 *  (which the encoder substitutes into ClusterResId.field2 for the
	 *  TRANSACTION case per HC40).  The decoded LOCKTAG must restore
	 *  field2 = 0 (HC40 reverse contract: do not propagate origin back). */
	const uint32 xids[] = { 1u, 100u, 0x80000000u, 0xFFFFFFFFu };
	const size_t nxids = sizeof(xids) / sizeof(xids[0]);
	int saved_node = cluster_node_id;
	int32 n;
	size_t i;

	for (n = 0; n < 16; n++) {
		cluster_node_id = n;
		for (i = 0; i < nxids; i++) {
			LOCKTAG src;
			ClusterResId resid;
			LOCKTAG decoded;

			memset(&src, 0, sizeof(src));
			src.locktag_field1 = xids[i];
			src.locktag_type = LOCKTAG_TRANSACTION;
			src.locktag_lockmethodid = 1; /* DEFAULT_LOCKMETHOD */

			cluster_grd_resid_encode(&src, &resid);

			UT_ASSERT_EQ((int)resid.field1, (int)xids[i]);
			UT_ASSERT_EQ((int)resid.field2, n); /* HC40 wrapper */
			UT_ASSERT_EQ((int)resid.field3, 0);
			UT_ASSERT_EQ((int)resid.field4, 0);
			UT_ASSERT_EQ((int)resid.type, (int)LOCKTAG_TRANSACTION);
			UT_ASSERT_EQ((int)resid.lockmethodid, 1);

			cluster_grd_resid_decode(&resid, &decoded);
			UT_ASSERT_EQ((int)decoded.locktag_field1, (int)xids[i]);
			UT_ASSERT_EQ((int)decoded.locktag_field2, 0); /* HC40 reverse: no propagate */
			UT_ASSERT_EQ((int)decoded.locktag_field3, 0);
			UT_ASSERT_EQ((int)decoded.locktag_field4, 0);
			UT_ASSERT_EQ((int)decoded.locktag_type, (int)LOCKTAG_TRANSACTION);
			UT_ASSERT_EQ((int)decoded.locktag_lockmethodid, 1);
		}
	}

	cluster_node_id = saved_node;
}

UT_TEST(test_grd_resid_encode_transaction_invalid_node_fail_closed)
{
	/* spec-2.26 T-grd-N+1 — HC47 invalid cluster_node_id defence.
	 *
	 *	When cluster_node_id is -1 (bootstrap / uninitialized) or out of
	 *	range, cluster_grd_resid_encode MUST NOT silently cast -1 to
	 *	0xFFFFFFFF and write it to ClusterResId.field2 (R11 silent wrong-
	 *	master risk).  The encoder leaves field2 at the LOCKTAG-native
	 *	value (0 for TRANSACTION) — the caller-side gate
	 *	(cluster_lock_should_globalize) is the contractual fail-closed
	 *	point; this test exercises encoder defense-in-depth. */
	int saved_node = cluster_node_id;
	LOCKTAG src;
	ClusterResId resid;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 42;
	src.locktag_type = LOCKTAG_TRANSACTION;
	src.locktag_lockmethodid = 1;

	cluster_node_id = -1;
	cluster_grd_resid_encode(&src, &resid);
	UT_ASSERT_EQ((int)resid.field1, 42);
	UT_ASSERT_NE((int)resid.field2, -1);			  /* not silent cast */
	UT_ASSERT_NE((int)resid.field2, (int)0xFFFFFFFF); /* HC47 defence */
	UT_ASSERT_EQ((int)resid.field2, 0);

	cluster_node_id = 9999; /* out of range */
	cluster_grd_resid_encode(&src, &resid);
	UT_ASSERT_EQ((int)resid.field2, 0); /* HC47 defence */

	cluster_node_id = saved_node;
}

UT_TEST(test_grd_transaction_cleanup_on_node_dead_removes_entry)
{
	/* spec-2.26 T-grd-N+2 — HC44 cleanup_on_node_dead must remove
	 * TRANSACTION-class holders, waiters, and reservations owned by a
	 * dead origin node.  The fake HTAB now supports hash_seq_search and
	 * HASH_REMOVE so this is a true cleanup behavior test, not a link
	 * surface placeholder. */
	int saved_node = cluster_node_id;
	LOCKTAG src;
	ClusterResId resid;
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntry *after = (ClusterGrdEntry *)0xdeadbeef;
	ClusterGrdHolderId dead_holder;
	ClusterGrdEntryResult r;

	reset_fake_grd_htab();
	cluster_grd_max_entries = 4;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 4242;
	src.locktag_type = LOCKTAG_TRANSACTION;
	src.locktag_lockmethodid = 1;

	cluster_node_id = 5; /* origin node encoded into ClusterResId.field2 */
	cluster_grd_resid_encode(&src, &resid);
	cluster_node_id = 0; /* local cleanup executor */

	cluster_grd_shmem_init();
	r = cluster_grd_entry_lookup_or_create(&resid, true, &entry);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_NOT_NULL(entry);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	memset(&dead_holder, 0, sizeof(dead_holder));
	dead_holder.node_id = 5;
	dead_holder.procno = 17;
	dead_holder.cluster_epoch = 11;
	dead_holder.request_id = 100;

	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &dead_holder, ExclusiveLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_add_waiter(entry, &dead_holder, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_reservation_create(entry, &dead_holder, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_has_remote_holder(entry, 0), true);
	UT_ASSERT_EQ(cluster_grd_entry_has_pending_waiter(entry), true);

	cluster_grd_cleanup_on_node_dead(5);

	r = cluster_grd_entry_lookup_or_create(&resid, false, &after);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ((void *)after, (void *)NULL);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	cluster_node_id = saved_node;
	reset_fake_grd_htab();
}

UT_TEST(test_grd_entry_existing_hit_survives_soft_cap)
{
	LOCKTAG src;
	ClusterResId resid_a;
	ClusterResId resid_b;
	ClusterGrdEntry *first = NULL;
	ClusterGrdEntry *second = NULL;
	ClusterGrdEntry *third = (ClusterGrdEntry *)0xdeadbeef;
	ClusterGrdEntryResult r;

	/* Regression for the Step 5 review fix: soft cap must apply only to
	 * new entries.  A table at cap must still return the existing handle
	 * for the same resource. */
	reset_fake_grd_htab();
	cluster_grd_max_entries = 1;
	cluster_grd_shmem_init();

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 42;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, &resid_a);

	r = cluster_grd_entry_lookup_or_create(&resid_a, true, &first);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_NE((void *)first, (void *)NULL);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	r = cluster_grd_entry_lookup_or_create(&resid_a, true, &second);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((void *)second, (void *)first);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	src.locktag_field1 = 43;
	cluster_grd_resid_encode(&src, &resid_b);
	r = cluster_grd_entry_lookup_or_create(&resid_b, true, &third);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_FULL);
	UT_ASSERT_EQ((void *)third, (void *)NULL);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}


/* ============================================================
 * spec-4.6 D2 — failure-driven remaster unit suite
 *	(test_cluster_grd_remaster section;  lives in this binary because
 *	the mock-conf + shmem-stub harness is already here).
 * ============================================================ */

UT_TEST(test_grd_remaster_failure_driven_deterministic)
{
	int32 nodes3[] = { 0, 1, 2 };
	uint64 dead[2] = { 0, 0 };
	uint32 gen0_before;
	uint32 gen1_before;
	uint32 moved;
	uint32 moved_again;
	uint32 i;

	cluster_grd_shmem_init();
	set_mock_declared(3, nodes3);
	cluster_grd_master_map_init();

	/* Baseline: shard i → declared[i % 3]. */
	UT_ASSERT_EQ(cluster_grd_shard_master(0), (int32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master(1), (int32)1);
	UT_ASSERT_EQ(cluster_grd_shard_master(2), (int32)2);

	gen0_before = cluster_grd_shard_master_generation(0);
	gen1_before = cluster_grd_shard_master_generation(1);

	/* node0 dies (accepted dead bitmap bit 0). */
	dead[0] = (uint64)1 << 0;
	moved = cluster_grd_master_map_remaster(dead, /* reconfig_epoch */ 7);

	/* 4096 = 3*1365 + 1 → shards ≡ 0 (mod 3) count = 1366. */
	UT_ASSERT_EQ(moved, (uint32)1366);

	/* Survivors {1, 2}:  affected shard s → survivors[s % 2]. */
	UT_ASSERT_EQ(cluster_grd_shard_master(0), (int32)1);
	UT_ASSERT_EQ(cluster_grd_shard_master(3), (int32)2);
	UT_ASSERT_EQ(cluster_grd_shard_master(6), (int32)1);

	/* No shard is mastered by the dead node any more. */
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		UT_ASSERT_NE(cluster_grd_shard_master(i), (int32)0);

	/* Unaffected shards untouched (master + generation). */
	UT_ASSERT_EQ(cluster_grd_shard_master(1), (int32)1);
	UT_ASSERT_EQ(cluster_grd_shard_master(2), (int32)2);
	UT_ASSERT_EQ(cluster_grd_shard_master_generation(1), gen1_before);

	/* Moved shard generation bumped exactly once. */
	UT_ASSERT_EQ(cluster_grd_shard_master_generation(0), gen0_before + 1);
	UT_ASSERT_EQ(cluster_grd_shard_master_generation(3), cluster_grd_shard_master_generation(6));

	/* Idempotent:  same dead bitmap re-run is a no-op (affected masters
	 * are survivors now), generation does NOT bump again. */
	moved_again = cluster_grd_master_map_remaster(dead, 7);
	UT_ASSERT_EQ(moved_again, (uint32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master_generation(0), gen0_before + 1);
}

UT_TEST(test_grd_remaster_same_snapshot_same_result)
{
	/* Hard-gate #1:  every node computes the same map from the same
	 * accepted snapshot.  Simulate "another node" by re-running init +
	 * remaster with identical inputs and comparing the full map. */
	int32 nodes3[] = { 0, 1, 2 };
	uint64 dead[2] = { 0, 0 };
	static int32 first_run[PGRAC_GRD_SHARD_COUNT];
	uint32 i;

	cluster_grd_shmem_init();
	set_mock_declared(3, nodes3);
	cluster_grd_master_map_init();
	dead[0] = (uint64)1 << 1; /* node1 dies this time */
	(void)cluster_grd_master_map_remaster(dead, 11);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		first_run[i] = cluster_grd_shard_master(i);

	/* "Other node":  identical declared conf + identical dead bitmap. */
	cluster_grd_master_map_init();
	(void)cluster_grd_master_map_remaster(dead, 11);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		UT_ASSERT_EQ(cluster_grd_shard_master(i), first_run[i]);
}

UT_TEST(test_grd_remaster_multi_death_and_sparse)
{
	/* Sparse declared {0, 2, 5};  nodes 0 and 5 die in ONE reconfig →
	 * every affected shard lands on the lone survivor 2. */
	int32 sparse[] = { 0, 2, 5 };
	uint64 dead[2] = { 0, 0 };
	uint32 moved;
	uint32 i;

	cluster_grd_shmem_init();
	set_mock_declared(3, sparse);
	cluster_grd_master_map_init();

	dead[0] = ((uint64)1 << 0) | ((uint64)1 << 5);
	moved = cluster_grd_master_map_remaster(dead, 13);

	/* shards ≡0 (1366) + ≡2 (1365) mod 3 move. */
	UT_ASSERT_EQ(moved, (uint32)(1366 + 1365));
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		UT_ASSERT_EQ(cluster_grd_shard_master(i), (int32)2);
}

UT_TEST(test_grd_remaster_no_survivor_fail_closed)
{
	/* All declared dead → no reassignment (fail-closed upstream via
	 * quorum);  the map must be left untouched, return 0. */
	int32 nodes2[] = { 0, 1 };
	uint64 dead[2] = { 0, 0 };
	uint32 moved;

	cluster_grd_shmem_init();
	set_mock_declared(2, nodes2);
	cluster_grd_master_map_init();

	dead[0] = ((uint64)1 << 0) | ((uint64)1 << 1);
	moved = cluster_grd_master_map_remaster(dead, 17);
	UT_ASSERT_EQ(moved, (uint32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master(0), (int32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master(1), (int32)1);
}

UT_TEST(test_grd_lookup_master_gen_q3c_verbatim)
{
	/* Q3-C:  out_routing_generation = LMS wire token VERBATIM;  the
	 * per-shard remaster generation never rides the wire. */
	int32 nodes2[] = { 0, 1 };
	LOCKTAG src;
	ClusterResId resid;
	uint64 token = 0;
	int32 master;

	cluster_grd_shmem_init();
	set_mock_declared(2, nodes2);
	cluster_grd_master_map_init();

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 4960;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, &resid);

	mock_lms_shard_master_generation = (((uint64)21 << 32) | 0x000000aa);
	master = cluster_grd_lookup_master_gen(&resid, &token);
	UT_ASSERT_EQ(token, ((((uint64)21) << 32) | 0x000000aa));
	UT_ASSERT_EQ(master, cluster_grd_lookup_master(&resid));
	mock_lms_shard_master_generation = 0;
}

UT_TEST(test_grd_shard_phase_accessors)
{
	int32 nodes2[] = { 0, 1 };

	cluster_grd_shmem_init();
	set_mock_declared(2, nodes2);

	/* Default NORMAL;  LMON-set transitions read back;  out-of-range
	 * shard ids read as NORMAL and writes are ignored (defensive). */
	UT_ASSERT_EQ((int)cluster_grd_shard_phase(40), (int)GRD_SHARD_NORMAL);
	cluster_grd_shard_set_phase(40, GRD_SHARD_FROZEN);
	UT_ASSERT_EQ((int)cluster_grd_shard_phase(40), (int)GRD_SHARD_FROZEN);
	cluster_grd_shard_set_phase(40, GRD_SHARD_REBUILDING);
	UT_ASSERT_EQ((int)cluster_grd_shard_phase(40), (int)GRD_SHARD_REBUILDING);
	cluster_grd_shard_set_phase(40, GRD_SHARD_NORMAL);
	UT_ASSERT_EQ((int)cluster_grd_shard_phase(40), (int)GRD_SHARD_NORMAL);
	UT_ASSERT_EQ((int)cluster_grd_shard_phase(PGRAC_GRD_SHARD_COUNT), (int)GRD_SHARD_NORMAL);
	cluster_grd_shard_set_phase(PGRAC_GRD_SHARD_COUNT, GRD_SHARD_FROZEN);
	UT_ASSERT_EQ((int)cluster_grd_shard_phase(PGRAC_GRD_SHARD_COUNT), (int)GRD_SHARD_NORMAL);
}

/*
 * spec-4.7 D2/D7 (P0 code-review fix) — REDECLARE_DONE must not be announced
 * while the survivor block re-declare scan is incomplete.  Drive
 * grd_block_redeclare_step over a multi-tick fake pool and assert
 * grd_block_redeclare_scan_complete stays FALSE until the whole pool is swept,
 * and that a new episode epoch re-arms it to incomplete.  (Were the scan-
 * complete conjunct absent, a fast GES barrier would release the episode with
 * a held block un-re-declared → the new master serves it as cold → 8.A
 * double-grant.)
 */
UT_TEST(test_grd_d2_redeclare_scan_completion_gate)
{
	fake_scan_nbuffers = 600; /* > CHUNK (256) → needs several steps */

	grd_block_redeclare_step(10); /* cursor 0 -> 256 */
	UT_ASSERT(!grd_block_redeclare_scan_complete(10));
	grd_block_redeclare_step(10); /* 256 -> 512 */
	UT_ASSERT(!grd_block_redeclare_scan_complete(10));
	grd_block_redeclare_step(10); /* 512 -> 600 (== nbuffers, advance) */
	UT_ASSERT(!grd_block_redeclare_scan_complete(10));
	grd_block_redeclare_step(10); /* 600 -> 600 (no advance) -> done */
	UT_ASSERT(grd_block_redeclare_scan_complete(10));

	/* completion is per-episode: a different epoch is not "complete". */
	UT_ASSERT(!grd_block_redeclare_scan_complete(11));

	/* a fresh episode epoch re-arms the scan to incomplete. */
	grd_block_redeclare_step(11);
	UT_ASSERT(!grd_block_redeclare_scan_complete(11));

	fake_scan_nbuffers = 0; /* restore no-op for any later test */
}


/* ============================================================
 * spec-5.1b — GES grant/convert state machine (D11 unit suite U1-U11).
 *
 *	Convert-state-machine logic (D2/D4/D5/D6/D9) is LOGIC-only in
 *	spec-5.1b (no live cross-node producer — opcode-2 is explicitly
 *	FEATURE_NOT_SUPPORTED, see D3); these unit tests are the full
 *	correctness coverage of the partial-order classification, convert-
 *	priority drain, anti-starvation, self-exclusion, double-grant guard,
 *	and queue-full fail-closed paths.  Lives in this binary (rule 6.A
 *	low-risk deviation from the spec's test_cluster_grd_convert.c name)
 *	to reuse the existing shmem-stub + fake-HTAB + ges_mode link harness.
 * ============================================================ */

/* request_opcode tags (mirror cluster_ges.h GES_REQ_OPCODE_*; not included
 * here to avoid pulling the GES wire surface into the standalone binary). */
#define UT_GES_OPCODE_REQUEST 1
#define UT_GES_OPCODE_CONVERT 2

/* Internal sweep entry point (extern in cluster_grd.c, not in the public
 * header); declared locally so U12 can exercise the convert-queue sweep
 * predicates (spec-5.1b §3 clause 14) directly. */
extern int cluster_grd_entry_cleanup_guarded(ClusterGrdEntry *entry, int dead_procno,
											 int32 dead_node_id);

static ClusterGrdEntry *
convert_make_entry(int field1)
{
	LOCKTAG src;
	ClusterResId resid;
	ClusterGrdEntry *e = NULL;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = (uint32)field1;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, &resid);
	(void)cluster_grd_entry_lookup_or_create(&resid, true, &e);
	return e;
}

static void
convert_reset(void)
{
	/*
	 * The soft entry-cap counter (entry_current_count) lives in the shared
	 * struct and is only zero-initialised on the first-ever shmem_init
	 * (the fake ShmemInitStruct returns found=true thereafter), while
	 * reset_fake_grd_htab() only clears the fake HTAB storage.  The convert
	 * suite is not exercising the entry cap, so set a max far above the
	 * total entries the suite ever creates to keep lookup_or_create out of
	 * the FULL path.  At most ~2 entries are live per fake-HTAB reset, so
	 * the 4-slot fake storage never overflows.
	 */
	reset_fake_grd_htab();
	cluster_grd_max_entries = 1000000;
	cluster_grd_shmem_init();
}

static void
convert_teardown(void)
{
	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}

static void
convert_grant(ClusterGrdEntry *e, int32 node, uint32 procno, uint64 reqid, LOCKMODE mode)
{
	ClusterGrdHolderId h;

	memset(&h, 0, sizeof(h));
	h.node_id = (uint32)node;
	h.procno = procno;
	h.cluster_epoch = 0;
	h.request_id = reqid;
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(e, &h, mode), (int)CLUSTER_GRD_ENTRY_OK);
}

static ClusterGrdConvert
convert_req(int32 node, uint32 procno, LOCKMODE cur, LOCKMODE want, uint64 cvid)
{
	ClusterGrdConvert r;

	memset(&r, 0, sizeof(r));
	r.node_id = node;
	r.source_node_id = node;
	r.procno = procno;
	r.cluster_epoch = 0;
	r.current_mode = cur;
	r.requested_mode = want;
	r.convert_request_id = cvid;
	r.shard_master_generation = 0;
	r.request_opcode = UT_GES_OPCODE_CONVERT;
	r.wait_start = 0;
	return r;
}

/* U1 — the LIVE matrix-switch drives the grant path: for all 64 (held,
 * wanted) pairs, the second cross-node request is GRANTED iff the frozen
 * matrix says compatible, else ENQUEUED.  Plus independent anchors. */
UT_TEST(test_convert_u1_grant_path_matrix_switch_all_pairs)
{
	int saved = cluster_node_id;
	LOCKMODE held, wanted;

	cluster_node_id = 0;
	for (held = GES_MODE_FIRST; held <= GES_MODE_LAST; held++) {
		for (wanted = GES_MODE_FIRST; wanted <= GES_MODE_LAST; wanted++) {
			LOCKTAG src;
			ClusterResId resid;
			ClusterGrdHolderId h1, h2;
			ClusterGrdGrantAction action;

			convert_reset();
			memset(&src, 0, sizeof(src));
			src.locktag_field1 = (uint32)(1000 + held * 10 + wanted);
			src.locktag_type = LOCKTAG_RELATION;
			src.locktag_lockmethodid = 1;
			cluster_grd_resid_encode(&src, &resid);

			memset(&h1, 0, sizeof(h1));
			h1.node_id = 1;
			h1.procno = 100;
			h1.request_id = 1;
			action = cluster_grd_entry_enqueue_or_grant(&resid, &h1, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														held, NULL, NULL);
			UT_ASSERT_EQ((int)action, (int)CLUSTER_GRD_GRANT_NOW);

			memset(&h2, 0, sizeof(h2));
			h2.node_id = 2;
			h2.procno = 200;
			h2.request_id = 2;
			action = cluster_grd_entry_enqueue_or_grant(&resid, &h2, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														wanted, NULL, NULL);
			if (ges_modes_compatible(held, wanted))
				UT_ASSERT_EQ((int)action, (int)CLUSTER_GRD_GRANT_NOW);
			else
				UT_ASSERT_EQ((int)action, (int)CLUSTER_GRD_ENQUEUED_WAITER);
		}
	}
	/* Independent anchors (not via ges_modes_compatible). */
	UT_ASSERT(ges_modes_compatible(ShareLock, ShareLock));					/* S + S compatible */
	UT_ASSERT(!ges_modes_compatible(ShareLock, ExclusiveLock));				/* S + X conflict */
	UT_ASSERT(!ges_modes_compatible(AccessShareLock, AccessExclusiveLock)); /* AS + AE conflict */
	cluster_node_id = saved;
	convert_teardown();
}

/* U2 — SAME convert is an idempotent in-place no-op (no drain hint). */
UT_TEST(test_convert_u2_same_is_noop)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = true; /* must be cleared to false */
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(2001);
	convert_grant(e, 1, 100, 11, ShareLock);

	req = convert_req(1, 100, ShareLock, ShareLock, 50);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(e), 1);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock);
	UT_ASSERT_EQ((int)drain, (int)false);
	convert_teardown();
}

/* U3 — DOWNGRADE is always in-place and raises the drain hint. */
UT_TEST(test_convert_u3_downgrade_inplace_drain_hint)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(2002);
	convert_grant(e, 1, 100, 11, ExclusiveLock);
	convert_grant(e, 2, 200, 22, AccessShareLock); /* coexisting weaker holder */

	req = convert_req(1, 100, ExclusiveLock, ShareLock, 51); /* X -> S */
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT_EQ((int)drain, (int)true);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	convert_teardown();
}

/* U4 — UPGRADE: in-place when compatible with the other holders, else
 * enqueued (never silently granted). */
UT_TEST(test_convert_u4_upgrade_inplace_or_enqueue)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = true;
	LOCKMODE m = NoLock;

	/* Case A: lone holder S upgrades to X -> in-place. */
	convert_reset();
	e = convert_make_entry(2003);
	convert_grant(e, 1, 100, 11, ShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 52);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT_EQ((int)drain, (int)false);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ExclusiveLock);
	convert_teardown();

	/* Case B: S upgrade to X conflicts with a remote S holder -> enqueue. */
	convert_reset();
	e = convert_make_entry(2004);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, ShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 53);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock); /* unchanged: not silently upgraded */
	convert_teardown();
}

/* U5 — LATERAL (incomparable) and missing-holder both fail closed ILLEGAL. */
UT_TEST(test_convert_u5_lateral_and_no_holder_illegal)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;
	LOCKMODE m = NoLock;

	/* LATERAL: S <-> RowExclusive are incomparable (5.1a U6 canonical). */
	convert_reset();
	e = convert_make_entry(2005);
	convert_grant(e, 1, 100, 11, ShareLock);
	UT_ASSERT_EQ((int)ges_mode_convert_class(ShareLock, RowExclusiveLock),
				 (int)GES_CONVERT_LATERAL);
	req = convert_req(1, 100, ShareLock, RowExclusiveLock, 54);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ILLEGAL);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock); /* untouched */
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	convert_teardown();

	/* no holder matching the (node,procno,current_mode) locator -> ILLEGAL. */
	convert_reset();
	e = convert_make_entry(2006);
	convert_grant(e, 1, 100, 11, ShareLock);
	req = convert_req(9, 999, ShareLock, ExclusiveLock, 55); /* no such holder */
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ILLEGAL);
	convert_teardown();
}

/* U6 — self-exclusion: a holder in a self-conflicting mode can still
 * upgrade (its own slot must not be counted as a conflicting holder). */
UT_TEST(test_convert_u6_self_exclusion)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;
	LOCKMODE m = NoLock;

	/* SUE upgrades to E (E self-conflicts).  Lone holder -> in-place. */
	convert_reset();
	e = convert_make_entry(2007);
	convert_grant(e, 1, 100, 11, ShareUpdateExclusiveLock);
	req = convert_req(1, 100, ShareUpdateExclusiveLock, ExclusiveLock, 56);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ExclusiveLock);
	convert_teardown();

	/* E -> E is SAME (self-conflict short-circuited by SAME path). */
	convert_reset();
	e = convert_make_entry(2008);
	convert_grant(e, 1, 100, 11, ExclusiveLock);
	req = convert_req(1, 100, ExclusiveLock, ExclusiveLock, 57);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	convert_teardown();
}

/* U7 — anti-starvation: with a pending convert, a new request that
 * conflicts with the convert's target mode must be blocked (must wait);
 * one compatible with the target is not blocked. */
UT_TEST(test_convert_u7_new_request_blocked_by_pending_convert)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;

	convert_reset();
	e = convert_make_entry(2009);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, ShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 58); /* enqueues (conflict) */
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);

	/* pending convert wants X: a new S request conflicts with X -> blocked. */
	UT_ASSERT(cluster_grd_entry_request_blocked_by_pending_convert(e, ShareLock));
	/* AccessShare is compatible with X -> not blocked. */
	UT_ASSERT(!cluster_grd_entry_request_blocked_by_pending_convert(e, AccessShareLock));
	convert_teardown();
}

/* U8 — release drain order: pending convert is granted BEFORE a waiting
 * REQUEST, and the multi-grant return carries both (opcode-tagged). */
UT_TEST(test_convert_u8_drain_converts_before_waiters)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId h2, w3;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1];
	bool drain = false;
	int n;
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(2010);
	convert_grant(e, 1, 100, 11, ShareLock); /* convert source holder */
	convert_grant(e, 2, 200, 22, ShareLock); /* conflicts with the X convert */

	req = convert_req(1, 100, ShareLock, ExclusiveLock, 59);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	/* a pending REQUEST waiter for AccessShare (compatible with everything). */
	memset(&w3, 0, sizeof(w3));
	w3.node_id = 3;
	w3.procno = 300;
	w3.request_id = 33;
	UT_ASSERT_EQ((int)cluster_grd_entry_add_waiter(e, &w3, AccessShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);

	/* release the conflicting node-2 holder, then drain. */
	memset(&h2, 0, sizeof(h2));
	h2.node_id = 2;
	h2.procno = 200;
	h2.request_id = 22;
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h2), (int)CLUSTER_GRD_ENTRY_OK);

	n = cluster_grd_entry_drain_converts_then_waiters(e, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 2);
	/* convert first (opcode CONVERT, mode X), then the waiter (REQUEST, AS). */
	UT_ASSERT_EQ((int)granted[0].request_opcode, UT_GES_OPCODE_CONVERT);
	UT_ASSERT_EQ((int)granted[0].mode, (int)ExclusiveLock);
	UT_ASSERT_EQ((int)granted[1].request_opcode, UT_GES_OPCODE_REQUEST);
	UT_ASSERT_EQ((int)granted[1].mode, (int)AccessShareLock);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ExclusiveLock); /* converted in place */
	convert_teardown();
}

/* U9 — convert queue full fails closed with QUEUE_FULL + counter bump. */
UT_TEST(test_convert_u9_queue_full_fail_closed)
{
	ClusterGrdEntry *e;
	bool drain = false;
	uint64 before;
	int i;

	convert_reset();
	e = convert_make_entry(2011);
	convert_grant(e, 99, 999, 9, ShareLock); /* co-holder forcing RE conflict */
	for (i = 0; i < PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1; i++)
		convert_grant(e, (int32)(i + 1), (uint32)((i + 1) * 100), (uint64)(i + 1), RowShareLock);

	before = cluster_grd_convert_queue_full_count();
	for (i = 0; i < PGRAC_GRD_MAX_CONVERTS_PUBLIC; i++) {
		ClusterGrdConvert req = convert_req((int32)(i + 1), (uint32)((i + 1) * 100), RowShareLock,
											RowExclusiveLock, (uint64)(100 + i));
		UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
					 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	}
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), PGRAC_GRD_MAX_CONVERTS_PUBLIC);

	{
		ClusterGrdConvert req = convert_req(9, 900, RowShareLock, RowExclusiveLock, 999);
		UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
					 (int)CLUSTER_GRD_CONVERT_QUEUE_FULL);
	}
	UT_ASSERT_EQ(cluster_grd_convert_queue_full_count(), before + 1);
	convert_teardown();
}

/* U10 — double-grant guard: a conflicting in-place upgrade is refused
 * (enqueued), and a compatible one keeps the holder set conflict-free. */
UT_TEST(test_convert_u10_double_grant_guard)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;
	LOCKMODE m = NoLock;

	/* S + RS: upgrade S->X conflicts with RS -> enqueue, no double grant. */
	convert_reset();
	e = convert_make_entry(2012);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, RowShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 60);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock);
	convert_teardown();

	/* S + AS: upgrade S->X is compatible with AS -> in-place granted. */
	convert_reset();
	e = convert_make_entry(2013);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, AccessShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 61);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ExclusiveLock);
	UT_ASSERT(ges_modes_compatible(ExclusiveLock, AccessShareLock)); /* still conflict-free */
	convert_teardown();
}

/* U12 — convert-queue sweep on holder death (spec-5.1b §3 clause 14): a
 * pending convert is removed by BOTH the local-backend-death (dead_procno)
 * and the remote-node-death (dead_node_id) predicates of the GRD cleanup
 * sweep, exactly like holders/waiters. */
UT_TEST(test_convert_u12_sweep_on_holder_death)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;
	int saved = cluster_node_id;

	/* local backend death: dead_procno path (this node hosts the holder). */
	cluster_node_id = 1;
	convert_reset();
	e = convert_make_entry(2014);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, ShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 70);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);
	(void)cluster_grd_entry_cleanup_guarded(e, /* dead_procno */ 100, /* dead_node_id */ -1);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	convert_teardown();

	/* remote node death: dead_node_id path. */
	cluster_node_id = 0;
	convert_reset();
	e = convert_make_entry(2015);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, ShareLock);
	req = convert_req(1, 100, ShareLock, ExclusiveLock, 71);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);
	(void)cluster_grd_entry_cleanup_guarded(e, /* dead_procno */ -1, /* dead_node_id */ 1);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	cluster_node_id = saved;
	convert_teardown();
}

/* U11 — ClusterGrdConvert byte layout pinned (StaticAssert mirror + L45). */
UT_TEST(test_convert_u11_struct_layout)
{
	UT_ASSERT_EQ((int)sizeof(ClusterGrdConvert), 64);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, cluster_epoch), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, current_mode), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, requested_mode), 28);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, convert_request_id), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, wait_start), 56);
}


/* ============================================================
 * spec-5.3 — TM table-lock convert live consumer (GRD-level).
 *
 *	cluster_lock_decide_op (U13) needs LockMethodLocalHash + the cluster GUC
 *	and is covered by the 2-node cluster_tap e2e;  the wire-ABI 64B layout is
 *	compile-enforced by the StaticAssertDecl in cluster_ges.h.  These unit
 *	tests cover the NEW 5.3 GRD-level behaviour: request_id rebind, the
 *	locator-precision guard, release-and-drain ownership, the rollback helper,
 *	and the master-side convert wrappers.
 * ============================================================ */

/* helper: resid matching convert_make_entry(field1). */
static void
convert_resid(int field1, ClusterResId *resid)
{
	LOCKTAG src;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = (uint32)field1;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, resid);
}

/* U14 — UPGRADE compatible (no other holder): mode upgraded AND the holder's
 * request_id rebound to convert_request_id (§3.1a);  holder count unchanged.
 * Rebind is verified by release_holder: the NEW id matches, the OLD does not. */
UT_TEST(test_convert_5_3_u14_request_convert_rebinds_request_id)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId h;
	bool drain = false;
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(5301);
	convert_grant(e, 1, 100, /* R_old */ 11, ShareLock);

	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, /* R_new */ 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(e), 1); /* count unchanged */
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)AccessExclusiveLock); /* mode upgraded */

	/* request_id rebound to R_new=77:  release by R_new matches, R_old fails. */
	memset(&h, 0, sizeof(h));
	h.node_id = 1;
	h.procno = 100;
	h.cluster_epoch = 0;
	h.request_id = 11; /* R_old */
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	h.request_id = 77; /* R_new */
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_OK);
	convert_teardown();
}

/* U17 — locator precision: a convert whose current_mode does not match any
 * holder slot is ILLEGAL (fail-closed) and never mutates a different-mode slot
 * of the same (node,procno). */
UT_TEST(test_convert_5_3_u17_locator_current_mode_precision)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	bool drain = false;
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(5302);
	convert_grant(e, 1, 100, 11, ShareLock);

	/* claim current_mode=ShareUpdateExclusiveLock — no such slot for (1,100). */
	req = convert_req(1, 100, ShareUpdateExclusiveLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ILLEGAL);
	/* the real ShareLock slot is untouched. */
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock);
	convert_teardown();
}

/* U15 (drain ownership) — a convert enqueued behind a conflicting cluster
 * holder is granted (in place, rebound to R_new) by release_and_drain when the
 * conflicting holder releases;  the returned identity is tagged CONVERT. */
UT_TEST(test_convert_5_3_u15_release_and_drain_grants_convert)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId rel;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1];
	ClusterResId resid;
	bool drain = false;
	int n;
	LOCKMODE m = NoLock;

	convert_reset();
	convert_resid(5303, &resid);
	e = convert_make_entry(5303);
	convert_grant(e, 1, 100, 11, ShareLock); /* converting holder */
	convert_grant(e, 2, 200, 22, ShareLock); /* conflicting peer holder */

	/* node1 upgrades Share->AccessExclusive: conflicts with node2 -> enqueue. */
	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);

	/* node2 releases -> drain grants the convert in place + rebinds. */
	memset(&rel, 0, sizeof(rel));
	rel.node_id = 2;
	rel.procno = 200;
	rel.cluster_epoch = 0;
	rel.request_id = 22;
	n = cluster_grd_release_and_drain(&resid, &rel, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ((int)granted[0].request_opcode, (int)UT_GES_OPCODE_CONVERT);
	UT_ASSERT_EQ((int)granted[0].holder.request_id, 77); /* R_new */
	UT_ASSERT_EQ((int)granted[0].mode, (int)AccessExclusiveLock);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)AccessExclusiveLock);
	convert_teardown();
}

/* U22 — rollback helper restores BOTH the mode and the request_id of an
 * upgraded slot (the strict inverse of a convert — NOT a delete). */
UT_TEST(test_convert_5_3_u22_rollback_restores_mode_and_id)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId h;
	bool drain = false;
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(5304);
	convert_grant(e, 1, 100, /* R_old */ 11, ShareLock);

	/* upgrade Share(R_old=11) -> AccessExclusive(R_new=77). */
	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);

	/* rollback: restore (AccessExclusive,R_new) slot to (Share,R_old). */
	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock); /* mode restored */

	/* request_id restored to R_old=11. */
	memset(&h, 0, sizeof(h));
	h.node_id = 1;
	h.procno = 100;
	h.cluster_epoch = 0;
	h.request_id = 77; /* R_new no longer matches */
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	h.request_id = 11; /* R_old restored */
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_OK);

	/* rollback of a non-existent upgraded slot is a fail-closed no-op. */
	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11),
				 (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	convert_teardown();
}

/* U18 — master-side convert wrappers: convert_or_enqueue grants a lone holder
 * (mode + rebind) and convert_grant_by_backend commits a convert located by
 * (node,procno) alone (native-probe clear path). */
UT_TEST(test_convert_5_3_u18_master_wrappers)
{
	ClusterResId resid;
	ClusterGrdEntry *e;
	LOCKMODE m = NoLock;

	/* convert_or_enqueue: single holder -> GRANTED_INPLACE + rebind. */
	convert_reset();
	convert_resid(5305, &resid);
	e = convert_make_entry(5305);
	convert_grant(e, 1, 100, 11, ShareLock);
	UT_ASSERT_EQ((int)cluster_grd_convert_or_enqueue(&resid, 1, 100, 0, ShareLock,
													 AccessExclusiveLock, 77, 1, 0, NULL, NULL),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)AccessExclusiveLock);
	convert_teardown();

	/* convert_grant_by_backend (native-probe clear path): locates by the
	 * PRECISE (node,procno,current_mode) and upgrades that slot. */
	convert_reset();
	convert_resid(5306, &resid);
	e = convert_make_entry(5306);
	convert_grant(e, 1, 100, 11, ShareLock);
	UT_ASSERT_EQ((int)cluster_grd_convert_grant_by_backend(&resid, 1, 100, 0, ShareLock,
														   AccessExclusiveLock, 77, 1, 0),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	m = NoLock;
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)AccessExclusiveLock);
	convert_teardown();

	/* MULTI-OLD-HOLDER (review): the same backend holds TWO cluster modes on
	 * one resid (SHARE + SHARE UPDATE EXCLUSIVE — the latter is additive because
	 * it is numerically weaker, so decide_op does not convert it).  A convert
	 * whose current_mode matches NEITHER held slot must fail-closed ILLEGAL —
	 * the precise (node,procno,current_mode) locator must NOT grab an arbitrary
	 * (node,procno) holder (which the old (node,procno)-only derivation would,
	 * upgrading the wrong slot -> wrong-mode grant + leak). */
	convert_reset();
	convert_resid(5308, &resid);
	e = convert_make_entry(5308);
	convert_grant(e, 1, 100, /* R_share */ 11, ShareLock);
	convert_grant(e, 1, 100, /* R_suex  */ 12, ShareUpdateExclusiveLock);
	/* current_mode = ExclusiveLock is held by NEITHER slot -> ILLEGAL. */
	UT_ASSERT_EQ((int)cluster_grd_convert_grant_by_backend(&resid, 1, 100, 0, ExclusiveLock,
														   AccessExclusiveLock, 77, 1, 0),
				 (int)CLUSTER_GRD_CONVERT_ILLEGAL);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(e), 2); /* both slots untouched */
	{
		ClusterGrdHolderId h;

		memset(&h, 0, sizeof(h));
		h.node_id = 1;
		h.procno = 100;
		h.cluster_epoch = 0;
		h.request_id = 11; /* SHARE slot intact */
		UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_OK);
		h.request_id = 12; /* SUEX slot intact */
		UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_OK);
	}
	convert_teardown();
}

/* U23 (review P0-1) — CONVERT_ROLLBACK must CANCEL a still-QUEUED convert, so a
 * later release drain cannot resurrect it and grant a phantom strong mode to a
 * requester that has already timed out / gone away (8.A false-grant + leak). */
UT_TEST(test_convert_5_3_u23_rollback_cancels_queued_convert)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId rel;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1];
	ClusterResId resid;
	bool drain = false;
	int n;
	LOCKMODE m = NoLock;

	convert_reset();
	convert_resid(5307, &resid);
	e = convert_make_entry(5307);
	convert_grant(e, 1, 100, 11, ShareLock); /* converting holder */
	convert_grant(e, 2, 200, 22, ShareLock); /* conflicting peer */

	/* node1's upgrade conflicts with node2 -> ENQUEUED (still queued). */
	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);

	/* requester times out -> rollback must DEQUEUE the pending convert. */
	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);

	/* the peer now releases -> the drain must NOT resurrect the cancelled
	 * convert (no phantom AccessExclusive holder). */
	memset(&rel, 0, sizeof(rel));
	rel.node_id = 2;
	rel.procno = 200;
	rel.cluster_epoch = 0;
	rel.request_id = 22;
	n = cluster_grd_release_and_drain(&resid, &rel, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 0); /* nothing granted */
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock); /* converter still at the weak mode */
	convert_teardown();
}


/* ============================================================
 * spec-5.1c — BAST rewrite + self-conflict exclusion.
 * ============================================================ */

/* helper: build a resid from a tag id. */
static void
bast_resid(uint32 tagid, ClusterResId *resid)
{
	LOCKTAG src;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = tagid;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, resid);
}

/* helper: a holder identity. */
static ClusterGrdHolderId
bast_holder(int32 node, uint32 procno, uint64 reqid)
{
	ClusterGrdHolderId h;

	memset(&h, 0, sizeof(h));
	h.node_id = (uint32)node;
	h.procno = procno;
	h.request_id = reqid;
	return h;
}

/* spec-5.1c U9a — same backend, different mode: own prior hold is NOT a
 * conflict against itself; the request is granted (fix: cross-node
 * self-deadlock).  Without the exclusion the master would enqueue/BAST the
 * requester behind its own ShareLock slot. */
UT_TEST(test_5_1c_u9a_self_conflict_excluded)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int n_conflict = -1;

	cluster_node_id = 0;
	convert_reset();
	bast_resid(5100, &resid);

	h = bast_holder(1, 100, 1); /* same backend grabs ShareLock */
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ShareLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_GRANT_NOW);

	h = bast_holder(1, 100, 2); /* fresh request_id, conflicting ExclusiveLock */
	n_conflict = -1;
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 2, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ(n_conflict, 0); /* self excluded -> no conflict holders */

	cluster_node_id = saved;
	convert_teardown();
}

/* spec-5.1c U9b — same backend + a different backend both conflict: only the
 * other backend is reported as a conflict holder (and BAST'd). */
UT_TEST(test_5_1c_u9b_self_plus_other_keeps_other)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int n_conflict = -1;

	cluster_node_id = 0;
	convert_reset();
	bast_resid(5101, &resid);

	h = bast_holder(1, 100, 1); /* self ShareLock */
	(void)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST, ShareLock,
											 conflicts, &n_conflict);
	h = bast_holder(2, 200, 2); /* other backend ShareLock (S+S compatible) */
	(void)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST, ShareLock,
											 conflicts, &n_conflict);

	h = bast_holder(1, 100, 3); /* self requests ExclusiveLock */
	n_conflict = -1;
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 3, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ(n_conflict, 1); /* only the other backend */
	UT_ASSERT_EQ((int)conflicts[0].holder.node_id, 2);
	UT_ASSERT_EQ((int)conflicts[0].holder.procno, 200);

	cluster_node_id = saved;
	convert_teardown();
}

/* spec-5.1c U9c — different backend conflicts normally (self-exclusion does
 * not over-fire on other backends). */
UT_TEST(test_5_1c_u9c_different_backend_normal)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int n_conflict = -1;

	cluster_node_id = 0;
	convert_reset();
	bast_resid(5102, &resid);

	h = bast_holder(1, 100, 1); /* node 1 ShareLock */
	(void)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST, ShareLock,
											 conflicts, &n_conflict);

	h = bast_holder(2, 200, 2); /* node 2 requests ExclusiveLock -> conflict */
	n_conflict = -1;
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ(n_conflict, 1);
	UT_ASSERT_EQ((int)conflicts[0].holder.node_id, 1);

	cluster_node_id = saved;
	convert_teardown();
}

/* spec-5.1c U5 — bast_consume drains the pending convert before a waiter
 * (the spec-5.1b drain semantics through the BAST-side seam). */
UT_TEST(test_5_1c_u5_bast_consume_drains)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId h2, w3, released;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1];
	bool drain = false;
	int n;
	LOCKMODE m = NoLock;

	convert_reset();
	e = convert_make_entry(5103);
	convert_grant(e, 1, 100, 11, ShareLock); /* convert source */
	convert_grant(e, 2, 200, 22, ShareLock); /* conflicts the X convert */

	req = convert_req(1, 100, ShareLock, ExclusiveLock, 70);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	memset(&w3, 0, sizeof(w3));
	w3.node_id = 3;
	w3.procno = 300;
	w3.request_id = 33;
	UT_ASSERT_EQ((int)cluster_grd_entry_add_waiter(e, &w3, AccessShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);

	memset(&h2, 0, sizeof(h2));
	h2.node_id = 2;
	h2.procno = 200;
	h2.request_id = 22;
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h2), (int)CLUSTER_GRD_ENTRY_OK);

	released = bast_holder(2, 200, 22);
	n = cluster_grd_entry_bast_consume(e, &released, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT_EQ((int)granted[0].request_opcode, UT_GES_OPCODE_CONVERT);
	UT_ASSERT_EQ((int)granted[0].mode, (int)ExclusiveLock);
	UT_ASSERT_EQ((int)granted[1].request_opcode, UT_GES_OPCODE_REQUEST);
	UT_ASSERT_EQ((int)granted[1].mode, (int)AccessShareLock);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ExclusiveLock);
	convert_teardown();
}

/* spec-5.1c U6 — bast_consume with nothing pending returns 0; NULL / zero-cap
 * arguments fail closed to 0. */
UT_TEST(test_5_1c_u6_bast_consume_empty_and_guards)
{
	ClusterGrdEntry *e;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1];

	convert_reset();
	e = convert_make_entry(5104);
	convert_grant(e, 1, 100, 11, ShareLock); /* lone holder, no convert/waiter */

	UT_ASSERT_EQ(cluster_grd_entry_bast_consume(e, NULL, granted, lengthof(granted)), 0);
	UT_ASSERT_EQ(cluster_grd_entry_bast_consume(NULL, NULL, granted, lengthof(granted)), 0);
	UT_ASSERT_EQ(cluster_grd_entry_bast_consume(e, NULL, NULL, 4), 0);
	UT_ASSERT_EQ(cluster_grd_entry_bast_consume(e, NULL, granted, 0), 0);
	convert_teardown();
}

/* spec-5.1c U10 — best-effort local delivery guard predicate (the "纯函数化"
 * part Q9=B covers; the live SendProcSignal is e2e SKIP). */
UT_TEST(test_5_1c_u10_bast_deliver_ok_predicate)
{
	/* pass: in-range procno, current epoch, live backend. */
	UT_ASSERT(cluster_grd_bast_local_deliver_ok(5, 10, 7, 7, 1234, 3));
	/* procno out of range / zero proc_count. */
	UT_ASSERT(!cluster_grd_bast_local_deliver_ok(10, 10, 7, 7, 1234, 3));
	UT_ASSERT(!cluster_grd_bast_local_deliver_ok(5, 0, 7, 7, 1234, 3));
	/* stale (cross-epoch) holder. */
	UT_ASSERT(!cluster_grd_bast_local_deliver_ok(5, 10, 6, 7, 1234, 3));
	/* dead / not-yet-started backend: pid 0, InvalidBackendId (-1), or a
	 * backendId of 0 (recycled slot mid-startup -- BackendId must be > 0,
	 * else SendProcSignal would index psh_slot[-1]). */
	UT_ASSERT(!cluster_grd_bast_local_deliver_ok(5, 10, 7, 7, 0, 3));
	UT_ASSERT(!cluster_grd_bast_local_deliver_ok(5, 10, 7, 7, 1234, -1));
	UT_ASSERT(!cluster_grd_bast_local_deliver_ok(5, 10, 7, 7, 1234, 0));
}

/* spec-5.1c U11 — regression: the live release_and_pop path keeps single-pop
 * (granted[1]) semantics (spec-5.1c does not touch this signature). */
UT_TEST(test_5_1c_u11_release_and_pop_unchanged)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h, w;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	ClusterGrdWaiterIdentity granted[2];
	int n_conflict = -1;

	cluster_node_id = 0;
	convert_reset();
	bast_resid(5105, &resid);

	h = bast_holder(1, 100, 1); /* node 1 ExclusiveLock */
	(void)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
											 ExclusiveLock, conflicts, &n_conflict);
	memset(&w, 0, sizeof(w)); /* node 2 RowShare waits (RS conflicts with X) */
	w.node_id = 2;
	w.procno = 200;
	w.request_id = 2;
	n_conflict = -1;
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &w, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 RowShareLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	/* release the X holder -> exactly one compatible waiter popped. */
	UT_ASSERT_EQ(
		cluster_grd_entry_release_and_pop_compatible_waiter(&resid, &h, granted, lengthof(granted)),
		1);
	cluster_node_id = saved;
	convert_teardown();
}


int
/* cppcheck-suppress constParameter
 * Reason: main() keeps the standard test harness signature used by the
 * other cluster_unit binaries; argv is intentionally unused. */
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(42);

	UT_RUN(test_grd_clusterresid_size_16);
	UT_RUN(test_grd_resid_encode_decode_roundtrip);
	UT_RUN(test_grd_shard_lookup_hash_distribution_uniform);
	UT_RUN(test_grd_uninitialized_master_map_returns_unknown);
	UT_RUN(test_grd_master_map_sparse_declared_nodes);
	UT_RUN(test_grd_is_local_master_matrix);
	UT_RUN(test_grd_is_cluster_aware_classification);

	/* spec-2.15 T-grd-2 a-f */
	UT_RUN(test_grd_entry_result_enum_value_invariant);
	UT_RUN(test_grd_entry_lookup_not_ready_when_guc_zero);
	UT_RUN(test_grd_named_tranche_describe_only);
	UT_RUN(test_grd_entry_release_no_op_safe);
	UT_RUN(test_grd_hash_source_unification);

	/* spec-2.26 T-grd-N..N+2 */
	UT_RUN(test_grd_resid_encode_transaction_roundtrip);
	UT_RUN(test_grd_resid_encode_transaction_invalid_node_fail_closed);
	UT_RUN(test_grd_transaction_cleanup_on_node_dead_removes_entry);

	UT_RUN(test_grd_entry_existing_hit_survives_soft_cap);

	/* spec-4.6 D2 — failure-driven remaster suite. */
	UT_RUN(test_grd_remaster_failure_driven_deterministic);
	UT_RUN(test_grd_remaster_same_snapshot_same_result);
	UT_RUN(test_grd_remaster_multi_death_and_sparse);
	UT_RUN(test_grd_remaster_no_survivor_fail_closed);
	UT_RUN(test_grd_lookup_master_gen_q3c_verbatim);
	UT_RUN(test_grd_shard_phase_accessors);
	UT_RUN(test_grd_d2_redeclare_scan_completion_gate);

	/* spec-5.1b — GES grant/convert state machine (U1-U11). */
	UT_RUN(test_convert_u1_grant_path_matrix_switch_all_pairs);
	UT_RUN(test_convert_u2_same_is_noop);
	UT_RUN(test_convert_u3_downgrade_inplace_drain_hint);
	UT_RUN(test_convert_u4_upgrade_inplace_or_enqueue);
	UT_RUN(test_convert_u5_lateral_and_no_holder_illegal);
	UT_RUN(test_convert_u6_self_exclusion);
	UT_RUN(test_convert_u7_new_request_blocked_by_pending_convert);
	UT_RUN(test_convert_u8_drain_converts_before_waiters);
	UT_RUN(test_convert_u9_queue_full_fail_closed);
	UT_RUN(test_convert_u10_double_grant_guard);
	UT_RUN(test_convert_u11_struct_layout);
	UT_RUN(test_convert_u12_sweep_on_holder_death);

	/* spec-5.3 — TM convert live consumer (GRD-level). */
	UT_RUN(test_convert_5_3_u14_request_convert_rebinds_request_id);
	UT_RUN(test_convert_5_3_u17_locator_current_mode_precision);
	UT_RUN(test_convert_5_3_u15_release_and_drain_grants_convert);
	UT_RUN(test_convert_5_3_u22_rollback_restores_mode_and_id);
	UT_RUN(test_convert_5_3_u18_master_wrappers);
	UT_RUN(test_convert_5_3_u23_rollback_cancels_queued_convert);

	/* spec-5.1c — BAST rewrite + LIVE self-conflict exclusion. */
	UT_RUN(test_5_1c_u9a_self_conflict_excluded);
	UT_RUN(test_5_1c_u9b_self_plus_other_keeps_other);
	UT_RUN(test_5_1c_u9c_different_backend_normal);
	UT_RUN(test_5_1c_u5_bast_consume_drains);
	UT_RUN(test_5_1c_u6_bast_consume_empty_and_guards);
	UT_RUN(test_5_1c_u10_bast_deliver_ok_predicate);
	UT_RUN(test_5_1c_u11_release_and_pop_unchanged);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
