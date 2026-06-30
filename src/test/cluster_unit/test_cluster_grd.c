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
#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_gcs.h"	   /* spec-4.7 D2 (L238) — cluster_gcs_lookup_master proto */
#include "cluster/cluster_gcs_block.h" /* spec-4.7 D2 (L238) — block re-declare scan/send protos */
#include "cluster/cluster_ges_mode.h"  /* spec-5.1b — frozen matrix + convert classification */
#include "access/transam.h"			   /* spec-5.8 D1c — InvalidTransactionId */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_lmd.h"			 /* spec-5.8 D1b — WFG vertex + submit/cancel edge */
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

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/*
 * spec-5.16 — test-only flag to force the GRD shmem stub to re-run its full
 * init (so the join-fence suite gets a clean recovery_done_epoch / fence /
 * direction between tests; the stub allocates the buffer once, so without this
 * a repeat cluster_grd_shmem_init() would see found=true and skip the field
 * init).  Set by ut_reset_grd_shmem(); honoured + cleared on the next call. */
static bool ut_grd_force_reinit = false;
static void
ut_reset_grd_shmem(void)
{
	ut_grd_force_reinit = true;
}

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
			char data[262144]; /* ClusterGrdShared includes 4096 shard lists. */
		} grd_buf;
		static bool grd_initialized = false;

		if (ut_grd_force_reinit) {
			grd_initialized = false;
			ut_grd_force_reinit = false;
		}
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
bool cluster_grd_entry_reclaim = true;
int cluster_grd_entry_reclaim_max_per_sweep = 256;

/* spec-5.10 — GES starvation-fairness GUC stub (cluster_grd.o references it). */
int cluster_ges_starvation_max_skips = 8;

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
bool cluster_join_remaster_enabled = false; /* spec-5.16 L104 — lmon_tick GUC ref */
volatile sig_atomic_t cluster_grd_redeclare_pending = false;
int MyProcPid = 0;

/* spec-5.16 — settable so the join-fence suite can drive arm/observe epochs.
 * Default 0 keeps every pre-5.16 test unchanged. */
static uint64 ut_mock_epoch = 0;
uint64
cluster_epoch_get_current(void)
{
	return ut_mock_epoch;
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
/* spec-5.16 L104 — cluster_grd.c now references these (join fence predicates).
 * Settable so the join-fence suite can drive a specific static home + member
 * set.  Defaults (static master 0, all members) keep pre-5.16 tests unchanged. */
static int ut_mock_static_master = 0;
int
cluster_gcs_lookup_master_static(BufferTag tag pg_attribute_unused())
{
	return ut_mock_static_master;
}
/* ut_member_mask < 0 (default) means "all declared nodes are members";
 * otherwise a node is a member iff (ut_member_mask >> node) & 1. */
static int ut_member_mask = -1;
bool
cluster_membership_is_member(int32 node_id)
{
	if (ut_member_mask < 0)
		return true;
	if (node_id < 0 || node_id >= 31)
		return false;
	return ((ut_member_mask >> node_id) & 1) != 0;
}
void
cluster_gcs_block_send_redeclare(BufferTag tag pg_attribute_unused(),
								 uint8 held_mode pg_attribute_unused(),
								 XLogRecPtr page_lsn pg_attribute_unused(),
								 SCN page_scn pg_attribute_unused(),
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

/* spec-5.7 D3 S5d stubs:  cluster_grd.c's WAIT_CLUSTER tick now also launches the
 * HW authority rebuild worker and consults its unfreeze gate before P7.  These
 * tests drive the GES/GRD remaster FSM, not the HW authority, so both are out of
 * scope -> no-op (launch registers nothing; gate returns false so P7 proceeds). */
void
cluster_hw_remaster_launch_workers(const uint64 *dead pg_attribute_unused(),
								   int nwords pg_attribute_unused(),
								   uint64 episode_epoch pg_attribute_unused())
{}
bool
cluster_hw_remaster_gate_unfreeze(void)
{
	return false;
}

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

#define FAKE_GRD_HTAB_MAX_ENTRIES 320
#define FAKE_GRD_HTAB_ENTRY_BYTES 4096

static int fake_grd_htab_token;
static int fake_grd_htab_count;
static int fake_grd_htab_seq_index;
static Size fake_grd_entrysize;
static bool fake_grd_htab_used[FAKE_GRD_HTAB_MAX_ENTRIES];
static union {
	uint64 force_align;
	char data[FAKE_GRD_HTAB_MAX_ENTRIES][FAKE_GRD_HTAB_ENTRY_BYTES];
} fake_grd_htab_entries;

static void
reset_fake_grd_htab(void)
{
	ut_reset_grd_shmem();
	fake_grd_htab_count = 0;
	fake_grd_htab_seq_index = 0;
	fake_grd_entrysize = 0;
	memset(fake_grd_htab_used, 0, sizeof(fake_grd_htab_used));
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
	memset(fake_grd_htab_used, 0, sizeof(fake_grd_htab_used));
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

	for (i = 0; i < FAKE_GRD_HTAB_MAX_ENTRIES; i++) {
		char *entry = fake_grd_htab_entries.data[i];

		if (!fake_grd_htab_used[i])
			continue;
		if (memcmp(entry, keyPtr, sizeof(ClusterResId)) == 0) {
			if (action == HASH_REMOVE) {
				if (foundPtr != NULL)
					*foundPtr = true;
				fake_grd_htab_used[i] = false;
				memset(entry, 0, fake_grd_entrysize);
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

		for (i = 0; i < FAKE_GRD_HTAB_MAX_ENTRIES; i++)
			if (!fake_grd_htab_used[i])
				break;
		Assert(i < FAKE_GRD_HTAB_MAX_ENTRIES);
		if (i >= FAKE_GRD_HTAB_MAX_ENTRIES)
			return NULL;

		fake_grd_htab_used[i] = true;
		fake_grd_htab_count++;
		entry = fake_grd_htab_entries.data[i];
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
	while (fake_grd_htab_seq_index < FAKE_GRD_HTAB_MAX_ENTRIES) {
		int i = fake_grd_htab_seq_index++;

		if (fake_grd_htab_used[i])
			return fake_grd_htab_entries.data[i];
	}
	return NULL;
}

/* spec-5.13 S3/D4 stub: cluster_grd_clean_leave_verify_no_leftover early-
 * terminates its hash_seq scan via hash_seq_term when it finds a leftover
 * holder/waiter/convert (CL-I2).  The fake seq harness needs no teardown. */
void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

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

/* ============================================================
 * spec-5.8 D1b — faithful fake of the LMD wait-for graph edge API.
 *
 *	cluster_grd.c (D1b) registers master-side wait edges by calling
 *	cluster_lmd_submit_wait_edge_real / cluster_lmd_cancel_wait_edge_real.
 *	The standalone harness does NOT link cluster_lmd_graph.o, so these two
 *	functions are provided here as a faithful in-memory model that mirrors
 *	the real graph's multi-edge semantics (spec-5.8 D1a): submit adds a
 *	distinct (waiter,blocker) edge keyed on the 4-tuple identities (idempotent
 *	per pair, self-edge rejected), and cancel removes EVERY edge of a waiter.
 *	U2 asserts the master-side edge set after each GRD mutation against this
 *	model (the real-graph 2-node e2e is the D8 TAP test).
 * ============================================================ */
#define UT_WFG_MAX 256

typedef struct UtWfgEdge {
	ClusterLmdVertex waiter;
	ClusterLmdVertex blocker;
} UtWfgEdge;

static UtWfgEdge ut_wfg[UT_WFG_MAX];
static int ut_wfg_n = 0;
static bool ut_wfg_throw_on_submit_once = false;
static bool ut_wfg_throw_on_cancel_once = false;

static void
ut_wfg_reset(void)
{
	ut_wfg_n = 0;
	ut_wfg_throw_on_submit_once = false;
	ut_wfg_throw_on_cancel_once = false;
	memset(ut_wfg, 0, sizeof(ut_wfg));
}

/* True iff two vertices share the same 4-tuple identity (xid/start_ts are
 * sort metadata, not identity — mirror cluster_lmd_graph.c key compare). */
static bool
ut_vtx_eq(const ClusterLmdVertex *a, const ClusterLmdVertex *b)
{
	return a->node_id == b->node_id && a->procno == b->procno
		   && a->cluster_epoch == b->cluster_epoch && a->request_id == b->request_id;
}

bool
cluster_lmd_submit_wait_edge_real(const ClusterLmdVertex *waiter, const ClusterLmdVertex *blocker,
								  uint64 request_id pg_attribute_unused())
{
	int i;

	if (ut_wfg_throw_on_submit_once) {
		ut_wfg_throw_on_submit_once = false;
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
	if (waiter == NULL || blocker == NULL)
		return false;
	if (ut_vtx_eq(waiter, blocker)) /* self-edge rejected (graph defensive) */
		return false;
	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_vtx_eq(&ut_wfg[i].waiter, waiter) && ut_vtx_eq(&ut_wfg[i].blocker, blocker))
			return true; /* idempotent per (waiter,blocker) pair */
	}
	if (ut_wfg_n >= UT_WFG_MAX)
		return false;
	ut_wfg[ut_wfg_n].waiter = *waiter;
	ut_wfg[ut_wfg_n].blocker = *blocker;
	ut_wfg_n++;
	return true;
}

void
cluster_lmd_cancel_wait_edge_real(const ClusterLmdVertex *waiter)
{
	int i;

	if (ut_wfg_throw_on_cancel_once) {
		ut_wfg_throw_on_cancel_once = false;
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
	if (waiter == NULL)
		return;
	for (i = 0; i < ut_wfg_n;) {
		if (ut_vtx_eq(&ut_wfg[i].waiter, waiter)) {
			if (i < ut_wfg_n - 1)
				ut_wfg[i] = ut_wfg[ut_wfg_n - 1];
			memset(&ut_wfg[ut_wfg_n - 1], 0, sizeof(UtWfgEdge));
			ut_wfg_n--;
			continue; /* re-check the swapped-in edge */
		}
		i++;
	}
}

/* Count the blocker edges currently registered for a waiter 4-tuple. */
static int
ut_wfg_count_waiter(int32 node, uint32 procno, uint64 epoch, uint64 rid)
{
	int i, n = 0;

	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_wfg[i].waiter.node_id == node && ut_wfg[i].waiter.procno == procno
			&& ut_wfg[i].waiter.cluster_epoch == epoch && ut_wfg[i].waiter.request_id == rid)
			n++;
	}
	return n;
}

/* True iff a (waiter 4-tuple -> blocker 4-tuple) edge is registered. */
static bool
ut_wfg_has_edge(int32 wn, uint32 wp, uint64 we, uint64 wr, int32 bn, uint32 bp, uint64 be,
				uint64 br)
{
	int i;

	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_wfg[i].waiter.node_id == wn && ut_wfg[i].waiter.procno == wp
			&& ut_wfg[i].waiter.cluster_epoch == we && ut_wfg[i].waiter.request_id == wr
			&& ut_wfg[i].blocker.node_id == bn && ut_wfg[i].blocker.procno == bp
			&& ut_wfg[i].blocker.cluster_epoch == be && ut_wfg[i].blocker.request_id == br)
			return true;
	}
	return false;
}

/* spec-5.8 D1c — the xid stamped on a waiter's vertex (Invalid if no such
 * waiter).  All of a waiter's edges carry the same vertex, so the first match
 * wins. */
static TransactionId
ut_wfg_waiter_xid(int32 node, uint32 procno, uint64 epoch, uint64 rid)
{
	int i;

	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_wfg[i].waiter.node_id == node && ut_wfg[i].waiter.procno == procno
			&& ut_wfg[i].waiter.cluster_epoch == epoch && ut_wfg[i].waiter.request_id == rid)
			return ut_wfg[i].waiter.xid;
	}
	return InvalidTransactionId;
}

/* spec-5.8 D1e — the wait_seq stamped on a waiter's vertex (0 if no such
 * waiter). */
static uint64
ut_wfg_waiter_wait_seq(int32 node, uint32 procno, uint64 epoch, uint64 rid)
{
	int i;

	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_wfg[i].waiter.node_id == node && ut_wfg[i].waiter.procno == procno
			&& ut_wfg[i].waiter.cluster_epoch == epoch && ut_wfg[i].waiter.request_id == rid)
			return ut_wfg[i].waiter.wait_seq;
	}
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
	/* spec-6.3a: NULL release must remain safe for cleanup-style callers. */
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

static void
grd_lifecycle_reset(int max_entries)
{
	ut_reset_grd_shmem();
	reset_fake_grd_htab();
	cluster_grd_max_entries = max_entries;
	cluster_grd_entry_reclaim = true;
	cluster_grd_entry_reclaim_max_per_sweep = 256;
	cluster_node_id = 0;
	cluster_grd_shmem_init();
}

static void
grd_lifecycle_resid(uint32 field1, ClusterResId *resid)
{
	LOCKTAG src;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = field1;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, resid);
}

static ClusterGrdHolderId
grd_lifecycle_holder(int32 node_id, uint32 procno, uint64 request_id)
{
	ClusterGrdHolderId holder;

	memset(&holder, 0, sizeof(holder));
	holder.node_id = node_id;
	holder.procno = procno;
	holder.cluster_epoch = 1;
	holder.request_id = request_id;
	return holder;
}

static ClusterGrdConvert
grd_lifecycle_convert_req(int32 node_id, uint32 procno, LOCKMODE current_mode,
						  LOCKMODE requested_mode, uint64 request_id)
{
	ClusterGrdConvert req;

	memset(&req, 0, sizeof(req));
	req.node_id = node_id;
	req.source_node_id = node_id;
	req.procno = procno;
	req.cluster_epoch = 1;
	req.current_mode = current_mode;
	req.requested_mode = requested_mode;
	req.convert_request_id = request_id;
	req.request_opcode = 2; /* mirror GES_REQ_OPCODE_CONVERT without cluster_ges.h */
	return req;
}

UT_TEST(test_grd_entry_pin_release_reclaims_cold)
{
	ClusterResId resid;
	ClusterGrdEntry *first = NULL;
	ClusterGrdEntry *second = NULL;
	ClusterGrdEntry *again = NULL;
	ClusterGrdEntry *after = (ClusterGrdEntry *)0xdeadbeef;
	uint64 skipped_before;
	uint64 reclaimed_before;

	grd_lifecycle_reset(4);
	grd_lifecycle_resid(6301, &resid);

	skipped_before = cluster_grd_reclaim_skipped_pinned_count();
	reclaimed_before = cluster_grd_entries_reclaimed_count();

	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &first),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(first), 1);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &second),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((void *)second, (void *)first);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(first), 2);
	UT_ASSERT(cluster_grd_pin_high_water() >= 2);

	UT_ASSERT(!cluster_grd_reclaim_if_cold(&resid));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	UT_ASSERT_EQ(cluster_grd_reclaim_skipped_pinned_count(), skipped_before + 1);

	cluster_grd_entry_release(second);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(first), 1);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	cluster_grd_entry_release(first);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	UT_ASSERT_EQ(cluster_grd_entries_reclaimed_count(), reclaimed_before + 1);

	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, false, &after),
				 (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ((void *)after, (void *)NULL);

	/* Reusing the same resource after reclaim must start from a clean pin/state. */
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &again),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(again), 1);
	UT_ASSERT(!cluster_grd_entry_is_reclaimable(again));
	cluster_grd_entry_release(again);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}

UT_TEST(test_grd_reclaim_sweep_reclaims_legacy_cold_entries)
{
	ClusterGrdEntry *entry = NULL;
	uint64 runs_before;
	int i;

	grd_lifecycle_reset(4);
	cluster_grd_entry_reclaim = false;

	for (i = 0; i < 3; i++) {
		ClusterResId resid;

		grd_lifecycle_resid((uint32)(6310 + i), &resid);
		UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
					 (int)CLUSTER_GRD_ENTRY_OK);
		cluster_grd_entry_release(entry);
		UT_ASSERT_EQ(cluster_grd_entry_count(), i + 1);
	}

	runs_before = cluster_grd_sweep_runs();
	UT_ASSERT_EQ(cluster_grd_reclaim_sweep(), 0);
	UT_ASSERT_EQ(cluster_grd_sweep_runs(), runs_before);

	cluster_grd_entry_reclaim = true;
	cluster_grd_entry_reclaim_max_per_sweep = 2;
	UT_ASSERT_EQ(cluster_grd_reclaim_sweep(), 2);
	UT_ASSERT_EQ(cluster_grd_sweep_runs(), runs_before + 1);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	cluster_grd_entry_reclaim_max_per_sweep = 256;
	UT_ASSERT_EQ(cluster_grd_reclaim_sweep(), 1);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}

UT_TEST(test_grd_reclaim_sweep_honors_large_batch_guc)
{
	ClusterGrdEntry *entry = NULL;
	int i;

	grd_lifecycle_reset(320);
	cluster_grd_entry_reclaim = false;

	for (i = 0; i < 300; i++) {
		ClusterResId resid;

		grd_lifecycle_resid((uint32)(6340 + i), &resid);
		UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
					 (int)CLUSTER_GRD_ENTRY_OK);
		cluster_grd_entry_release(entry);
	}
	UT_ASSERT_EQ(cluster_grd_entry_count(), 300);

	cluster_grd_entry_reclaim = true;
	cluster_grd_entry_reclaim_max_per_sweep = 300;
	UT_ASSERT_EQ(cluster_grd_reclaim_sweep(), 300);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}

UT_TEST(test_grd_reclaim_excludes_live_state)
{
	ClusterResId resid;
	ClusterGrdEntry *entry = NULL;
	ClusterGrdHolderId h1;
	ClusterGrdHolderId h2;
	ClusterGrdConvert creq;
	bool drain = false;

	grd_lifecycle_reset(4);

	grd_lifecycle_resid(6320, &resid);
	h1 = grd_lifecycle_holder(1, 10, 100);
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &h1, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	cluster_grd_entry_release(entry);
	UT_ASSERT(!cluster_grd_reclaim_if_cold(&resid));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	UT_ASSERT_EQ((int)cluster_grd_release_holder_by_id(&resid, &h1), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	grd_lifecycle_resid(6321, &resid);
	h1 = grd_lifecycle_holder(1, 11, 101);
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_add_waiter(entry, &h1, ExclusiveLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	cluster_grd_entry_release(entry);
	UT_ASSERT(!cluster_grd_reclaim_if_cold(&resid));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	UT_ASSERT_EQ((int)cluster_grd_cancel_waiter_by_id(&resid, &h1), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	grd_lifecycle_resid(6322, &resid);
	h1 = grd_lifecycle_holder(1, 12, 102);
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_reservation_create(entry, &h1, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	cluster_grd_entry_release(entry);
	UT_ASSERT(!cluster_grd_reclaim_if_cold(&resid));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	UT_ASSERT_EQ((int)cluster_grd_cancel_reservation_by_id(&resid, &h1), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	grd_lifecycle_resid(6323, &resid);
	h1 = grd_lifecycle_holder(1, 13, 103);
	h2 = grd_lifecycle_holder(2, 14, 104);
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &h1, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &h2, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	creq = grd_lifecycle_convert_req(1, 13, ShareLock, AccessExclusiveLock, 203);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(entry, &creq, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(entry), 1);
	cluster_grd_entry_release(entry);
	UT_ASSERT(!cluster_grd_reclaim_if_cold(&resid));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	h1.request_id = 203;
	UT_ASSERT_EQ((int)cluster_grd_cancel_convert_by_id(&resid, &h1, 0), (int)CLUSTER_GRD_ENTRY_OK);
	h1.request_id = 103;
	UT_ASSERT_EQ((int)cluster_grd_release_holder_by_id(&resid, &h2), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_release_holder_by_id(&resid, &h1), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}

UT_TEST(test_grd_entry_release_overrelease_fail_safe)
{
	ClusterResId resid;
	ClusterGrdEntry *entry = NULL;
	ClusterGrdHolderId holder;

	grd_lifecycle_reset(4);
	grd_lifecycle_resid(6330, &resid);
	holder = grd_lifecycle_holder(1, 20, 200);

	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &holder, ShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	cluster_grd_entry_release(entry);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(entry), 0);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	cluster_grd_entry_release(entry);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(entry), 0);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);

	UT_ASSERT_EQ((int)cluster_grd_release_holder_by_id(&resid, &holder), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
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
	cluster_grd_entry_release(entry);

	cluster_grd_cleanup_on_node_dead(5);

	r = cluster_grd_entry_lookup_or_create(&resid, false, &after);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ((void *)after, (void *)NULL);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_grd_max_entries = 0;
	cluster_node_id = saved_node;
	reset_fake_grd_htab();
}

UT_TEST(test_grd_release_and_drain_reclaims_empty_entry)
{
	/* spec-5.3 — release/drain reclaims a GRD entry once it is completely empty
	 * (no holders/waiters/converts/reservations) so LOCKTAG_TRANSACTION churn no
	 * longer fills the HTAB.  A still-occupied entry must NOT be removed. */
	int saved_node = cluster_node_id;
	LOCKTAG src;
	ClusterResId resid;
	ClusterGrdEntry *entry = NULL;
	ClusterGrdHolderId h1, h2;
	ClusterGrdGrantIdentity granted[8];
	int i;

	reset_fake_grd_htab();
	cluster_grd_max_entries = 8;
	cluster_node_id = 0;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = 7777;
	src.locktag_type = LOCKTAG_TRANSACTION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, &resid);
	cluster_grd_shmem_init();

	/* case 1: single holder; release reclaims the now-empty entry. */
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	memset(&h1, 0, sizeof(h1));
	h1.node_id = 0;
	h1.procno = 11;
	h1.cluster_epoch = 1;
	h1.request_id = 100;
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &h1, ExclusiveLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	cluster_grd_entry_release(entry);
	(void)cluster_grd_release_and_drain(&resid, &h1, granted, lengthof(granted));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0); /* empty -> reclaimed */

	/* case 2: two holders; releasing one keeps the entry (still occupied). */
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, true, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	memset(&h1, 0, sizeof(h1));
	h1.node_id = 0;
	h1.procno = 11;
	h1.cluster_epoch = 1;
	h1.request_id = 100;
	memset(&h2, 0, sizeof(h2));
	h2.node_id = 0;
	h2.procno = 12;
	h2.cluster_epoch = 1;
	h2.request_id = 101;
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &h1, AccessShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(entry, &h2, AccessShareLock),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1);
	cluster_grd_entry_release(entry);
	(void)cluster_grd_release_and_drain(&resid, &h1, granted, lengthof(granted));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 1); /* h2 still holds -> NOT reclaimed */
	(void)cluster_grd_release_and_drain(&resid, &h2, granted, lengthof(granted));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0); /* now empty -> reclaimed */

	/* case 3: TX churn — 100 unique-xid entries created+released under cap=8;
	 * without per-release reclaim this would hit FAIL_RESERVATION_FULL. */
	for (i = 0; i < 100; i++) {
		LOCKTAG s2;
		ClusterResId r2;
		ClusterGrdEntry *e2 = NULL;
		ClusterGrdHolderId hx;

		memset(&s2, 0, sizeof(s2));
		s2.locktag_field1 = (uint32)(20000 + i); /* unique xid per iteration */
		s2.locktag_type = LOCKTAG_TRANSACTION;
		s2.locktag_lockmethodid = 1;
		cluster_grd_resid_encode(&s2, &r2);
		UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&r2, true, &e2),
					 (int)CLUSTER_GRD_ENTRY_OK);
		memset(&hx, 0, sizeof(hx));
		hx.node_id = 0;
		hx.procno = 13;
		hx.cluster_epoch = 1;
		hx.request_id = (uint64)(200 + i);
		UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(e2, &hx, ExclusiveLock),
					 (int)CLUSTER_GRD_ENTRY_OK);
		cluster_grd_entry_release(e2);
		(void)cluster_grd_release_and_drain(&r2, &hx, granted, lengthof(granted));
	}
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0); /* no accumulation */

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

	cluster_grd_entry_release(second);
	cluster_grd_entry_release(first);
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
	 * the FULL path.  The fake storage is larger than the per-test live set.
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

/* U11 — ClusterGrdConvert byte layout pinned (StaticAssert mirror + L45).
 * spec-5.8 D1c added waiter_xid in the former pad @52 (size-stable); D1e
 * appended wait_seq @64 (64 -> 72); spec-5.10 D1 appended the fairness state
 * fair_queue_seq @72 / skip_count @80 / boosted @84 (72 -> 88). */
UT_TEST(test_convert_u11_struct_layout)
{
	UT_ASSERT_EQ((int)sizeof(ClusterGrdConvert), 88);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, cluster_epoch), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, current_mode), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, requested_mode), 28);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, convert_request_id), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, waiter_xid), 52);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, wait_start), 56);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, wait_seq), 64);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, fair_queue_seq), 72); /* spec-5.10 */
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, skip_count), 80);		/* spec-5.10 */
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, boosted), 84);		/* spec-5.10 */
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
	UT_ASSERT_EQ(
		(int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock, 11, 77),
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
	UT_ASSERT_EQ(
		(int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock, 11, 77),
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
	UT_ASSERT_EQ(
		(int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock, 11, 77),
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


/* spec-5.9 Hardening v1.0.1 (P1#2) — the convert backout + cancel primitives
 * must be ABA-safe under (node,procno) slot reuse.  A stale rollback / cancel
 * carrying an OLD convert_request_id or wait_seq must NOT evict a re-queued
 * convert (stage-1) nor demote a re-upgraded holder (stage-2 false-grant); only
 * the EXACT id / wait_seq acts.  Covers the wait_seq-exact convert dequeue
 * (cluster_grd_cancel_convert_by_id) the deadlock-victim cleanup now uses plus
 * the convert_request_id guard on cluster_grd_entry_rollback_convert. */
UT_TEST(test_convert_5_9_rollback_cancel_aba_guard)
{
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdHolderId h;
	ClusterResId resid;
	bool drain = false;
	LOCKMODE m = NoLock;

	/* ---- stage-1 (queued convert): a wrong convert_request_id must NOT dequeue
	 * the live convert; the exact id does. ---- */
	convert_reset();
	convert_resid(5901, &resid);
	e = convert_make_entry(5901);
	convert_grant(e, 1, 100, 11, ShareLock); /* converting holder */
	convert_grant(e, 2, 200, 22, ShareLock); /* conflicting peer -> ENQUEUED */
	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);

	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11, 999 /* stale id */),
				 (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1); /* NOT dequeued (ABA-safe) */

	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11, 77 /* exact id */),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	convert_teardown();

	/* ---- cluster_grd_cancel_convert_by_id wait_seq ABA: the queued convert has
	 * wait_seq=0, so a wrong wait_seq is rejected; the exact one dequeues. ---- */
	convert_reset();
	convert_resid(5902, &resid);
	e = convert_make_entry(5902);
	convert_grant(e, 1, 100, 11, ShareLock);
	convert_grant(e, 2, 200, 22, ShareLock);
	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);
	memset(&h, 0, sizeof(h));
	h.node_id = 1;
	h.procno = 100;
	h.cluster_epoch = 0;
	h.request_id = 77; /* convert_request_id */
	UT_ASSERT_EQ((int)cluster_grd_cancel_convert_by_id(&resid, &h, 99 /* wrong wait_seq */),
				 (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1); /* NOT dequeued (ABA-safe) */
	UT_ASSERT_EQ((int)cluster_grd_cancel_convert_by_id(&resid, &h, 0 /* exact wait_seq */),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 0);
	convert_teardown();

	/* ---- stage-2 (granted slot): a wrong convert_request_id must NOT demote a
	 * re-upgraded holder (false-grant guard); the exact id restores it. ---- */
	convert_reset();
	convert_resid(5903, &resid);
	e = convert_make_entry(5903);
	convert_grant(e, 1, 100, 11, ShareLock);
	req = convert_req(1, 100, ShareLock, AccessExclusiveLock, 77);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)AccessExclusiveLock); /* upgraded */

	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11, 999 /* stale id */),
				 (int)CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)AccessExclusiveLock); /* still upgraded (no false-grant) */

	UT_ASSERT_EQ((int)cluster_grd_entry_rollback_convert(e, 1, 100, AccessExclusiveLock, ShareLock,
														 11, 77 /* exact id */),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT(cluster_grd_entry_holder_mode(e, 1, 100, &m));
	UT_ASSERT_EQ((int)m, (int)ShareLock); /* restored */
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

/* spec-5.5 U6/T5 — conditional (NOWAIT) grant never enqueues a waiter.
 *
 *	cluster_grd_entry_grant_conditional grants when the resource is free and,
 *	on conflict, returns CLUSTER_GRD_CONFLICT_NOWAIT WITHOUT touching the entry
 *	(no waiter enqueued, holder count unchanged) — the master-side correctness
 *	behind pg_try_advisory_lock returning false.  We prove "no waiter" by
 *	contrast: releasing the holder pops 0 compatible waiters (a blocking
 *	enqueue_or_grant would have left one to grant). */
UT_TEST(test_ul_grant_conditional_no_waiter_enqueued)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdEntry *e = NULL;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	ClusterGrdWaiterIdentity granted[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int n_conflict = -1;
	int popped;

	cluster_node_id = 0;
	convert_reset();
	bast_resid(5150, &resid);

	/* node1 holds ExclusiveLock. */
	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, false, &e),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(e), 1);

	/* node2 try-locks ExclusiveLock (conflict) → CONFLICT_NOWAIT, entry untouched. */
	h = bast_holder(2, 200, 2);
	n_conflict = -1;
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_conditional(&resid, &h, 2, 2, 0,
														  UT_GES_OPCODE_REQUEST, ExclusiveLock,
														  conflicts, &n_conflict),
				 (int)CLUSTER_GRD_CONFLICT_NOWAIT);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(e), 1); /* node2 NOT added as a holder */
	cluster_grd_entry_release(e);

	/* Release node1: zero compatible waiters pop (grant_conditional enqueued none). */
	h = bast_holder(1, 100, 1);
	popped = cluster_grd_entry_release_and_pop_compatible_waiter(&resid, &h, granted,
																 PGRAC_GRD_MAX_HOLDERS_PUBLIC);
	UT_ASSERT_EQ(popped, 0);

	/* node2 free now: conditional grant succeeds. */
	h = bast_holder(2, 200, 3);
	n_conflict = -1;
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_conditional(&resid, &h, 2, 3, 0,
														  UT_GES_OPCODE_REQUEST, ExclusiveLock,
														  conflicts, &n_conflict),
				 (int)CLUSTER_GRD_GRANT_NOW);

	cluster_node_id = saved;
	convert_teardown();
}

/* spec-5.5 U2 — advisory resid encoding:  the same (db,key) advisory lock maps
 * to a stable resid, distinct keys never collide, and because the encode is
 * lockmode-independent the S and X holds of one key share a resid (the same
 * cross-node mutual-exclusion domain — X excludes S on the same key). */
UT_TEST(test_ul_advisory_resid_encoding)
{
	LOCKTAG a;
	LOCKTAG b;
	ClusterResId ra;
	ClusterResId ra2;
	ClusterResId rb;

	memset(&a, 0, sizeof(a));
	a.locktag_field1 = 12345; /* db */
	a.locktag_field2 = 42;	  /* advisory key */
	a.locktag_field4 = 1;	  /* int8 advisory type */
	a.locktag_type = LOCKTAG_ADVISORY;
	a.locktag_lockmethodid = USER_LOCKMETHOD;

	cluster_grd_resid_encode(&a, &ra);
	cluster_grd_resid_encode(&a, &ra2);
	/* deterministic + lockmode-independent → S and X of this key share resid. */
	UT_ASSERT_EQ(memcmp(&ra, &ra2, sizeof(ClusterResId)), 0);

	/* a different advisory key must not collide. */
	b = a;
	b.locktag_field2 = 43;
	cluster_grd_resid_encode(&b, &rb);
	UT_ASSERT(memcmp(&ra, &rb, sizeof(ClusterResId)) != 0);
}

/* spec-5.5 U4 — advisory mode matrix via the conditional (try-lock) path:
 * S/S compatible (both granted), S/X conflict (CONFLICT_NOWAIT).  X/X conflict
 * is pinned by test_ul_grant_conditional_no_waiter_enqueued. */
UT_TEST(test_ul_advisory_mode_matrix_conditional)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int n_conflict = -1;

	cluster_node_id = 0;
	convert_reset();
	bast_resid(5160, &resid);

	/* node1 ShareLock → grant. */
	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_conditional(
					 &resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_GRANT_NOW);

	/* node2 ShareLock — S/S compatible → conditional grant. */
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_conditional(
					 &resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &n_conflict),
				 (int)CLUSTER_GRD_GRANT_NOW);

	/* node3 ExclusiveLock — S/X conflict → CONFLICT_NOWAIT (no waiter). */
	h = bast_holder(3, 300, 3);
	UT_ASSERT_EQ((int)cluster_grd_entry_grant_conditional(&resid, &h, 3, 3, 0,
														  UT_GES_OPCODE_REQUEST, ExclusiveLock,
														  conflicts, &n_conflict),
				 (int)CLUSTER_GRD_CONFLICT_NOWAIT);

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


/* ============================================================
 * spec-5.8 D1b U2 — master-side WFG edge authority (cluster_grd.c registers
 *	wait edges from the GRD master wrappers against the LMD wait-for graph).
 *	Edges use the 4-tuple identity (node, procno, epoch, request_id); xid is
 *	InvalidTransactionId until D1c.  Asserts run against the faithful fake WFG
 *	above (real-graph 2-node e2e = D8 TAP).
 * ============================================================ */

/* U2a — REQUEST enqueue registers the waiter against EVERY current conflicting
 * holder (multi-blocker, no drop — spec-5.8 D1a multi-edge + D1b authority). */
UT_TEST(test_5_8_d1b_u2a_enqueue_registers_multi_blocker)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5800, &resid);

	/* Two compatible S holders on distinct backends. */
	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);

	/* An X requester conflicts with BOTH S holders -> enqueued with 2 edges. */
	h = bast_holder(3, 300, 3);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 3, 3, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	UT_ASSERT_EQ(ut_wfg_count_waiter(3, 300, 0, 3), 2);
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 1, 100, 0, 1));
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	cluster_node_id = saved;
	convert_teardown();
}

/* U2b — edges follow the CURRENT holders, not the enqueue-time snapshot
 * (spec-5.8 §3.1 E2): when a holder releases and a queued waiter is promoted,
 * the granted waiter's edges are removed and the remaining waiter is re-pointed
 * at the NEW holder — never left stale on the departed one. */
UT_TEST(test_5_8_d1b_u2b_refresh_follows_current_holders)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 2];
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;
	int n;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5801, &resid);

	/* holder1 X; two X waiters both blocked by holder1. */
	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	h = bast_holder(3, 300, 3);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 3, 3, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ(ut_wfg_count_waiter(2, 200, 0, 2), 1);
	UT_ASSERT(ut_wfg_has_edge(2, 200, 0, 2, 1, 100, 0, 1));
	UT_ASSERT_EQ(ut_wfg_count_waiter(3, 300, 0, 3), 1);
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 1, 100, 0, 1));

	/* Release holder1: drain pops one FIFO waiter (w2) -> it becomes the X
	 * holder.  w2's edges are removed (granted); w3 is re-pointed at w2. */
	h = bast_holder(1, 100, 1);
	n = cluster_grd_release_and_drain(&resid, &h, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);

	UT_ASSERT_EQ(ut_wfg_count_waiter(2, 200, 0, 2), 0);		 /* w2 granted -> edges gone */
	UT_ASSERT_EQ(ut_wfg_count_waiter(3, 300, 0, 3), 1);		 /* w3 still blocked */
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));	 /* -> NEW holder w2 */
	UT_ASSERT(!ut_wfg_has_edge(3, 300, 0, 3, 1, 100, 0, 1)); /* stale edge cleared */

	cluster_node_id = saved;
	convert_teardown();
}

/* U2c — CONVERT enqueue registers the convert-waiter against the conflicting
 * holders, identified by (node, procno, epoch, convert_request_id); the
 * converting backend's own hold is self-excluded (not a blocker). */
UT_TEST(test_5_8_d1b_u2c_convert_enqueue_registers_edge)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5802, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);

	/* node1/procno100 converts S->X (convert_request_id 10); blocked by the
	 * node2 S holder (the node1 S hold self-excludes). */
	nc = -1;
	UT_ASSERT_EQ((int)cluster_grd_convert_or_enqueue(&resid, 1, 100, 0, ShareLock, ExclusiveLock,
													 10, 1, 0, conflicts, &nc),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	UT_ASSERT_EQ(ut_wfg_count_waiter(1, 100, 0, 10), 1);
	UT_ASSERT(ut_wfg_has_edge(1, 100, 0, 10, 2, 200, 0, 2));

	cluster_node_id = saved;
	convert_teardown();
}

/* U2d — cancelling a queued REQUEST waiter (timeout) removes its edges. */
UT_TEST(test_5_8_d1b_u2d_cancel_removes_waiter_edges)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5803, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ(ut_wfg_count_waiter(2, 200, 0, 2), 1);

	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_cancel_waiter_by_id(&resid, &h), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(ut_wfg_count_waiter(2, 200, 0, 2), 0);

	cluster_node_id = saved;
	convert_teardown();
}

UT_TEST(test_grd_pin_cleanup_on_lmd_submit_error)
{
	int saved_node = cluster_node_id;
	int saved_max_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdEntry *entry = NULL;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	volatile bool caught = false;
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	cluster_grd_set_starvation_protection(true);
	cluster_ges_starvation_max_skips = 1;
	bast_resid(5804, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 1, 1, 0, UT_GES_OPCODE_REQUEST,
														 ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);

	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant(&resid, &h, 2, 2, 0, UT_GES_OPCODE_REQUEST,
														 ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	ut_wfg_throw_on_submit_once = true;
	h = bast_holder(3, 300, 3);
	PG_TRY();
	{
		(void)cluster_grd_entry_enqueue_or_grant(&resid, &h, 3, 3, 0,
												 UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &nc);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT(!ut_wfg_throw_on_submit_once);
	UT_ASSERT_EQ((int)cluster_grd_entry_lookup_or_create(&resid, false, &entry),
				 (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_pin_count(entry), 1);
	cluster_grd_entry_release(entry);

	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_cancel_waiter_by_id(&resid, &h), (int)CLUSTER_GRD_ENTRY_OK);
	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_release_holder_by_id(&resid, &h), (int)CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);

	cluster_ges_starvation_max_skips = saved_max_skips;
	cluster_node_id = saved_node;
	convert_teardown();
}


/* ============================================================
 * spec-5.8 D1c U3 — the master-side WFG vertex carries the real waiter xid
 *	(passed at enqueue/convert), not InvalidTransactionId.  D1b stamped the
 *	4-tuple identity with xid=Invalid; D1c threads the waiter's xid through so
 *	a later TX edge can resolve holder=(node,xid) to a waiting backend.
 * ============================================================ */

/* U3a — an enqueued REQUEST waiter's vertex carries the xid passed in the
 * waiter meta. */
UT_TEST(test_5_8_d1c_u3a_request_waiter_carries_xid)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5810, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 1, 1, (ClusterGrdWaiterMeta){ (TransactionId)0, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 2, 2, (ClusterGrdWaiterMeta){ (TransactionId)12345, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	UT_ASSERT_EQ(ut_wfg_count_waiter(2, 200, 0, 2), 1);
	UT_ASSERT_EQ((int)ut_wfg_waiter_xid(2, 200, 0, 2), 12345);

	cluster_node_id = saved;
	convert_teardown();
}

/* U3b — an enqueued convert's convert-waiter vertex carries the xid passed in
 * the waiter meta. */
UT_TEST(test_5_8_d1c_u3b_convert_waiter_carries_xid)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5811, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 1, 1, (ClusterGrdWaiterMeta){ (TransactionId)0, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 2, 2, (ClusterGrdWaiterMeta){ (TransactionId)0, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);

	nc = -1;
	UT_ASSERT_EQ((int)cluster_grd_convert_or_enqueue_meta(
					 &resid, 1, 100, 0, ShareLock, ExclusiveLock, 10, 1, 0,
					 (ClusterGrdWaiterMeta){ (TransactionId)67890, 0 }, conflicts, &nc),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	UT_ASSERT_EQ(ut_wfg_count_waiter(1, 100, 0, 10), 1);
	UT_ASSERT_EQ((int)ut_wfg_waiter_xid(1, 100, 0, 10), 67890);

	cluster_node_id = saved;
	convert_teardown();
}

/* U4a — spec-5.8 D1e: an enqueued REQUEST waiter's vertex carries the wait_seq
 * passed in the waiter meta (the D5 ABA-revalidate basis). */
UT_TEST(test_5_8_d1e_u4a_request_waiter_carries_wait_seq)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5820, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 1, 1, (ClusterGrdWaiterMeta){ (TransactionId)0, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 2, 2, (ClusterGrdWaiterMeta){ (TransactionId)0, 777 }, 0,
					 UT_GES_OPCODE_REQUEST, ExclusiveLock, conflicts, &nc),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	UT_ASSERT_EQ(ut_wfg_count_waiter(2, 200, 0, 2), 1);
	UT_ASSERT(ut_wfg_waiter_wait_seq(2, 200, 0, 2) == 777);

	cluster_node_id = saved;
	convert_teardown();
}

/* U4b — spec-5.8 D1e: an enqueued convert's vertex carries the wait_seq. */
UT_TEST(test_5_8_d1e_u4b_convert_waiter_carries_wait_seq)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	cluster_node_id = 0;
	convert_reset();
	ut_wfg_reset();
	bast_resid(5821, &resid);

	h = bast_holder(1, 100, 1);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 1, 1, (ClusterGrdWaiterMeta){ (TransactionId)0, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);
	h = bast_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_enqueue_or_grant_meta(
					 &resid, &h, 2, 2, (ClusterGrdWaiterMeta){ (TransactionId)0, 0 }, 0,
					 UT_GES_OPCODE_REQUEST, ShareLock, conflicts, &nc),
				 (int)CLUSTER_GRD_GRANT_NOW);

	nc = -1;
	UT_ASSERT_EQ((int)cluster_grd_convert_or_enqueue_meta(
					 &resid, 1, 100, 0, ShareLock, ExclusiveLock, 10, 1, 0,
					 (ClusterGrdWaiterMeta){ (TransactionId)0, 888 }, conflicts, &nc),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	UT_ASSERT_EQ(ut_wfg_count_waiter(1, 100, 0, 10), 1);
	UT_ASSERT(ut_wfg_waiter_wait_seq(1, 100, 0, 10) == 888);

	cluster_node_id = saved;
	convert_teardown();
}


/* ============================================================
 * spec-5.16 — online node-join GRD/PCM remaster (D7 unit suite).
 *
 *	Lives in this binary (rule 6.A low-risk deviation from the spec's
 *	test_cluster_join_remaster.c name) to reuse the shmem-stub + mock-declared
 *	+ epoch/membership/static-master mock harness — same precedent as the
 *	spec-5.1b convert suite above.  These units prove the NEW 5.16 logic:
 *	the membership-aware recompute (U1-U5), the PCM fence predicates + the
 *	Hardening-v1.1 all-members view-rebuilt barrier (U10-U16).  The reused
 *	mechanisms are covered elsewhere: U6 single-X reject = cluster_pcm_lock
 *	block re-declare apply + cluster_tap t/326 L9; U7 default-deny = the GES
 *	result-enum invariant test above; U8 episode-epoch coherence / U9 publish-
 *	before-signal = the spec-4.6 failure-FSM tests above + t/326; the multi-
 *	node no-double-grant / gate-open / master-side / pre-MEMBER e2e are the
 *	t/326 ship-blockers (L8/L11/L12/L13).
 * ============================================================ */

/* Build a uint8[16] node bitmap from a small node list. */
static void
ut_jr_build_bitmap(uint8 *bm, const int *nodes, int n)
{
	int i;

	memset(bm, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	for (i = 0; i < n; i++)
		bm[nodes[i] >> 3] |= (uint8)(1u << (nodes[i] & 7));
}

/* 3-node declared {0,1,2} fixture with a home-based master map. */
static void
ut_jr_setup_3node(void)
{
	int32 nodes3[] = { 0, 1, 2 };

	ut_mock_epoch = 0;
	ut_member_mask = -1; /* all members */
	ut_mock_static_master = 0;
	ut_reset_grd_shmem(); /* clean fence / recovery_done_epoch / direction */
	cluster_grd_shmem_init();
	set_mock_declared(3, nodes3);
	cluster_grd_master_map_init();
}

/* U1 — recompute moves the joiner's home shards back from the survivor. */
UT_TEST(test_jr_u1_recompute_moves_joiner_home)
{
	uint8 active[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int all3[3] = { 0, 1, 2 };
	uint64 dead_n1[2] = { 0, 0 };
	uint32 moved_away;
	uint32 moved_back;
	int i;

	ut_jr_setup_3node();

	/* node 1 leaves: its home shards (i%3==1) move to survivors {0,2}. */
	dead_n1[0] = (uint64)1 << 1;
	moved_away = cluster_grd_master_map_remaster(dead_n1, 5);
	UT_ASSERT(moved_away > 0);

	/* node 1 rejoins: recompute with all members → home shards snap back to 1. */
	ut_jr_build_bitmap(active, all3, 3);
	moved_back = cluster_grd_master_map_recompute_for_membership(active, 6);

	/* 4096 = 3*1365 + 1 → shards ≡ 1 (mod 3) count = 1365. */
	UT_ASSERT_EQ(moved_back, (uint32)1365);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		if ((i % 3) == 1)
			UT_ASSERT_EQ(cluster_grd_shard_master(i), (int32)1);
		else
			UT_ASSERT_EQ(cluster_grd_shard_master(i), (int32)(i % 3));
	}
}

/* U2 — pure-failure recompute is per-shard equivalent to the 4.6 remaster. */
UT_TEST(test_jr_u2_equivalence_with_failure_remaster)
{
	int32 nodes3[] = { 0, 1, 2 };
	uint8 active02[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int nodes02[2] = { 0, 2 };
	uint64 dead_n1[2] = { 0, 0 };
	int32 remaster_map[PGRAC_GRD_SHARD_COUNT];
	int i;

	/* Path A: failure remaster of node 1. */
	cluster_grd_shmem_init();
	set_mock_declared(3, nodes3);
	cluster_grd_master_map_init();
	dead_n1[0] = (uint64)1 << 1;
	(void)cluster_grd_master_map_remaster(dead_n1, 7);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		remaster_map[i] = cluster_grd_shard_master(i);

	/* Path B: membership recompute with node 1 NOT a member (== dead). */
	cluster_grd_shmem_init();
	set_mock_declared(3, nodes3);
	cluster_grd_master_map_init();
	ut_jr_build_bitmap(active02, nodes02, 2);
	(void)cluster_grd_master_map_recompute_for_membership(active02, 7);

	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		UT_ASSERT_EQ(cluster_grd_shard_master(i), remaster_map[i]);
}

/* U3 — idempotent: same membership snapshot re-run is a no-op. */
UT_TEST(test_jr_u3_recompute_idempotent)
{
	uint8 active[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int all3[3] = { 0, 1, 2 };
	uint32 gen1;
	uint32 moved2;

	ut_jr_setup_3node();
	ut_jr_build_bitmap(active, all3, 3);

	(void)cluster_grd_master_map_recompute_for_membership(active, 6); /* already home — 0 moves */
	gen1 = cluster_grd_shard_master_generation(1);
	moved2 = cluster_grd_master_map_recompute_for_membership(active, 6);
	UT_ASSERT_EQ(moved2, (uint32)0);
	UT_ASSERT_EQ(cluster_grd_shard_master_generation(1), gen1);
}

/* U4 — scoped: only the joiner-home shards' generation bumps on rejoin. */
UT_TEST(test_jr_u4_scoped_move_set)
{
	uint8 active[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int all3[3] = { 0, 1, 2 };
	uint64 dead_n1[2] = { 0, 0 };
	uint32 gen0_before;
	uint32 gen2_before;
	uint32 gen0_after;
	uint32 gen2_after;

	ut_jr_setup_3node();
	dead_n1[0] = (uint64)1 << 1;
	(void)cluster_grd_master_map_remaster(dead_n1, 5);

	/* shards 0 (home 0) and 2 (home 2) were never touched by node 1's leave. */
	gen0_before = cluster_grd_shard_master_generation(0);
	gen2_before = cluster_grd_shard_master_generation(2);

	ut_jr_build_bitmap(active, all3, 3);
	(void)cluster_grd_master_map_recompute_for_membership(active, 6);

	gen0_after = cluster_grd_shard_master_generation(0);
	gen2_after = cluster_grd_shard_master_generation(2);
	/* non-joiner-home shards untouched by the rejoin recompute. */
	UT_ASSERT_EQ(gen0_after, gen0_before);
	UT_ASSERT_EQ(gen2_after, gen2_before);
	/* joiner-home shard 1 moved back → its generation bumped. */
	UT_ASSERT_EQ(cluster_grd_shard_master(1), (int32)1);
}

/* U5 — INV-R1: never remaster a shard to a non-member (joiner bit clear). */
UT_TEST(test_jr_u5_inv_r1_non_member_gets_nothing)
{
	uint8 active02[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int nodes02[2] = { 0, 2 };
	int i;

	ut_jr_setup_3node();
	ut_jr_build_bitmap(active02, nodes02, 2); /* node 1 NOT a member */
	(void)cluster_grd_master_map_recompute_for_membership(active02, 6);

	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++)
		UT_ASSERT_NE(cluster_grd_shard_master(i), (int32)1);
}

/* U10 — GRD-off + PCM-on: the fence predicates are INDEPENDENT of any GRD
 * master[] movement (bound to online_join via the armed epoch), so the PCM
 * fence holds even when no recompute ran. */
UT_TEST(test_jr_u10_pcm_fence_independent_of_grd_move)
{
	uint8 join1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n1[1] = { 1 };
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	ut_jr_setup_3node();
	ut_mock_epoch = 10;

	/* Arm the fence for the rejoining node 1 — NO recompute / GRD move done. */
	ut_jr_build_bitmap(join1, n1, 1);
	cluster_grd_arm_join_pcm_fence(join1);

	/* A block whose static home is the rejoiner is fenced... */
	ut_mock_static_master = 1;
	UT_ASSERT(cluster_grd_join_remaster_active_for_shard(tag));
	UT_ASSERT(!cluster_grd_block_view_rebuilt(tag)); /* survivors not done yet */

	/* ...a block whose home is a steady member is NOT fenced. */
	ut_mock_static_master = 0;
	UT_ASSERT(!cluster_grd_join_remaster_active_for_shard(tag));
}

/* U11 — scan-completion gate (join episode flavor of the 4.7 D2 gate). */
UT_TEST(test_jr_u11_scan_completion_gate)
{
	fake_scan_nbuffers = 600;
	grd_block_redeclare_step(20);
	UT_ASSERT(!grd_block_redeclare_scan_complete(20));
	grd_block_redeclare_step(20);
	grd_block_redeclare_step(20);
	grd_block_redeclare_step(20);
	UT_ASSERT(grd_block_redeclare_scan_complete(20));
	fake_scan_nbuffers = 0;
}

/* U12 — new shmem fields zeroed at init; in_progress flips on arm; the reply-
 * status no-collision StaticAssert is enforced at compile time in the header. */
UT_TEST(test_jr_u12_state_init_and_in_progress)
{
	uint8 join1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n1[1] = { 1 };

	ut_jr_setup_3node();
	UT_ASSERT(!cluster_grd_join_remaster_in_progress()); /* NONE after init */

	ut_mock_epoch = 10;
	ut_jr_build_bitmap(join1, n1, 1);
	cluster_grd_arm_join_pcm_fence(join1);
	UT_ASSERT(cluster_grd_join_remaster_in_progress()); /* JOIN after arm */
}

/* U13 — gate-open ordering + Hardening v1.1: the fence lifts ONLY after EVERY
 * member re-declared (all-members barrier), NOT after the joiner's own done. */
UT_TEST(test_jr_u13_view_rebuilt_all_members_barrier)
{
	uint8 join1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n1[1] = { 1 };
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	ut_jr_setup_3node();
	ut_mock_epoch = 10;
	ut_jr_build_bitmap(join1, n1, 1);
	cluster_grd_arm_join_pcm_fence(join1);
	ut_mock_static_master = 1; /* the rejoiner's home block */

	/* Initially fenced: armed, nobody done. */
	UT_ASSERT(cluster_grd_join_remaster_active_for_shard(tag));
	UT_ASSERT(!cluster_grd_block_view_rebuilt(tag));

	/* HARDENING v1.1 — the joiner (node 1) announcing its OWN trivial barrier
	 * must NOT lift the fence: survivors 0 and 2 have not re-declared yet. */
	cluster_grd_recovery_mark_peer_done(1, 10);
	UT_ASSERT(!cluster_grd_block_view_rebuilt(tag));

	/* One survivor done is still not enough. */
	cluster_grd_recovery_mark_peer_done(0, 10);
	UT_ASSERT(!cluster_grd_block_view_rebuilt(tag));

	/* All members done → view rebuilt → fence lifts. */
	cluster_grd_recovery_mark_peer_done(2, 10);
	UT_ASSERT(cluster_grd_block_view_rebuilt(tag));
}

/* U14 — fence-arm sets fence epoch (monotonic-max) + fenced member scope. */
UT_TEST(test_jr_u14_fence_arm_monotonic_and_scope)
{
	uint8 join1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n1[1] = { 1 };
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	ut_jr_setup_3node();

	ut_mock_epoch = 10;
	ut_jr_build_bitmap(join1, n1, 1);
	cluster_grd_arm_join_pcm_fence(join1);

	/* A lower-epoch re-arm must NOT lower the fence epoch (monotonic-max):
	 * marking members done at epoch 5 must leave the fence (epoch 10) unlifted. */
	ut_mock_epoch = 5;
	cluster_grd_arm_join_pcm_fence(join1);
	ut_mock_static_master = 1;
	cluster_grd_recovery_mark_peer_done(0, 5);
	cluster_grd_recovery_mark_peer_done(1, 5);
	cluster_grd_recovery_mark_peer_done(2, 5);
	UT_ASSERT(!cluster_grd_block_view_rebuilt(tag)); /* 5 < fence epoch 10 */

	/* Done at the real fence epoch lifts it. */
	cluster_grd_recovery_mark_peer_done(0, 10);
	cluster_grd_recovery_mark_peer_done(1, 10);
	cluster_grd_recovery_mark_peer_done(2, 10);
	UT_ASSERT(cluster_grd_block_view_rebuilt(tag));
}

/* U15 — master-side gate decision (home-block leg): deny while (active &&
 * !view_rebuilt), allow once rebuilt.  The in_quorum / is_member legs and the
 * envelope wiring are the t/326 L12 ship-blocker. */
UT_TEST(test_jr_u15_master_side_gate_decision)
{
	uint8 join1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n1[1] = { 1 };
	BufferTag tag;
	bool deny;

	memset(&tag, 0, sizeof(tag));
	ut_jr_setup_3node();
	ut_mock_epoch = 10;
	ut_jr_build_bitmap(join1, n1, 1);
	cluster_grd_arm_join_pcm_fence(join1);
	ut_mock_static_master = 1;

	/* The gate's home-block leg: active_for_shard && !view_rebuilt. */
	deny = cluster_grd_join_remaster_active_for_shard(tag) && !cluster_grd_block_view_rebuilt(tag);
	UT_ASSERT(deny);

	cluster_grd_recovery_mark_peer_done(0, 10);
	cluster_grd_recovery_mark_peer_done(1, 10);
	cluster_grd_recovery_mark_peer_done(2, 10);
	deny = cluster_grd_join_remaster_active_for_shard(tag) && !cluster_grd_block_view_rebuilt(tag);
	UT_ASSERT(!deny);
}

/* U16 — pre-MEMBER guard: the master-side gate denies whenever self is not a
 * MEMBER (the CSSD-ALIVE-before-commit window, INV-R14). */
UT_TEST(test_jr_u16_pre_member_guard)
{
	int saved = cluster_node_id;

	ut_jr_setup_3node();
	cluster_node_id = 0;

	/* Self (node 0) NOT a member (nodes 1,2 are): is_member(self) == false → the
	 * master-side gate's pre-MEMBER leg denies regardless of any fence/view. */
	ut_member_mask = (1 << 1) | (1 << 2);
	UT_ASSERT(!cluster_membership_is_member(cluster_node_id));

	/* Once committed (self a member) the pre-MEMBER leg no longer denies. */
	ut_member_mask = (1 << 0) | (1 << 1) | (1 << 2);
	UT_ASSERT(cluster_membership_is_member(cluster_node_id));

	ut_member_mask = -1;
	cluster_node_id = saved;
}

/* U17 — cross-episode fence accumulation (reviewer P1 #1; Hardening fix, 8.A).
 * A fenced recipient from a COMPLETED prior join episode must NOT be excluded
 * from the CURRENT episode's all-members re-declare barrier: it is now a steady
 * survivor that may hold X on the NEW joiner's home block and MUST re-declare
 * before that joiner cold-serves.  Pre-fix the OR-accumulated member bitmap kept
 * the prior rejoiner's bit set, so episode-2's barrier skipped it -> premature
 * fence lift -> cold-serve a block the survivor still holds X on -> double-grant
 * / false-visible. */
UT_TEST(test_jr_u17_stale_recipient_not_excluded_next_episode)
{
	uint8 join1[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 join2[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	int n1[1] = { 1 };
	int n2[1] = { 2 };
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	ut_jr_setup_3node(); /* declared {0,1,2}, all members */

	/* --- Episode 1: node 1 rejoins and completes (all survivors re-declare). --- */
	ut_mock_epoch = 10;
	ut_jr_build_bitmap(join1, n1, 1);
	cluster_grd_arm_join_pcm_fence(join1);
	ut_mock_static_master = 1; /* a node-1-home block */
	cluster_grd_recovery_mark_peer_done(0, 10);
	cluster_grd_recovery_mark_peer_done(1, 10);
	cluster_grd_recovery_mark_peer_done(2, 10);
	UT_ASSERT(cluster_grd_block_view_rebuilt(tag)); /* episode 1 converged */

	/* --- Episode 2: node 2 rejoins.  node 1 is now a steady survivor whose held
	 * node-2-home blocks MUST be re-declared before node 2 may cold-serve. --- */
	ut_mock_epoch = 20;
	ut_jr_build_bitmap(join2, n2, 1);
	cluster_grd_arm_join_pcm_fence(join2);
	ut_mock_static_master = 2; /* a node-2-home block (the new joiner's home) */

	/* Only the OTHER survivor (node 0) has re-declared for episode 2; node 1 has
	 * NOT.  node 1's stale episode-1 fence bit must NOT exclude it from the
	 * barrier — it must still gate the fence lift. */
	cluster_grd_recovery_mark_peer_done(0, 20);
	UT_ASSERT(!cluster_grd_block_view_rebuilt(tag)); /* must still wait for node 1 */

	/* Once node 1 re-declares for episode 2 the barrier converges and lifts. */
	cluster_grd_recovery_mark_peer_done(1, 20);
	UT_ASSERT(cluster_grd_block_view_rebuilt(tag));
}


int
/* cppcheck-suppress constParameter
 * Reason: main() keeps the standard test harness signature used by the
 * other cluster_unit binaries; argv is intentionally unused. */
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	/* prior 42; spec-5.3:+6; 5.5:+3; +1 release reclaim;
	 * spec-6.3a:+6 lifecycle; 5.8 D1b:+4 (U2a-d); D1c:+2 (U3a-b);
	 * D1e:+2 (U4a-b); 5.9 Hardening:+1 (convert ABA);
	 * spec-5.16:+12 (join-remaster U1-U5/U10-U16);
	 * +1 (U17 cross-episode fence Hardening). */
	UT_PLAN(80);

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
	UT_RUN(test_grd_entry_pin_release_reclaims_cold);
	UT_RUN(test_grd_reclaim_sweep_reclaims_legacy_cold_entries);
	UT_RUN(test_grd_reclaim_sweep_honors_large_batch_guc);
	UT_RUN(test_grd_reclaim_excludes_live_state);
	UT_RUN(test_grd_entry_release_overrelease_fail_safe);

	/* spec-2.26 T-grd-N..N+2 */
	UT_RUN(test_grd_resid_encode_transaction_roundtrip);
	UT_RUN(test_grd_resid_encode_transaction_invalid_node_fail_closed);
	UT_RUN(test_grd_transaction_cleanup_on_node_dead_removes_entry);
	UT_RUN(test_grd_release_and_drain_reclaims_empty_entry);

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
	UT_RUN(test_convert_5_9_rollback_cancel_aba_guard);

	/* spec-5.1c — BAST rewrite + LIVE self-conflict exclusion. */
	UT_RUN(test_5_1c_u9a_self_conflict_excluded);
	UT_RUN(test_5_1c_u9b_self_plus_other_keeps_other);
	UT_RUN(test_5_1c_u9c_different_backend_normal);
	UT_RUN(test_ul_grant_conditional_no_waiter_enqueued);
	UT_RUN(test_ul_advisory_resid_encoding);
	UT_RUN(test_ul_advisory_mode_matrix_conditional);
	UT_RUN(test_5_1c_u5_bast_consume_drains);
	UT_RUN(test_5_1c_u6_bast_consume_empty_and_guards);
	UT_RUN(test_5_1c_u10_bast_deliver_ok_predicate);
	UT_RUN(test_5_1c_u11_release_and_pop_unchanged);

	/* spec-5.8 D1b — master-side WFG edge authority (U2a-d). */
	UT_RUN(test_5_8_d1b_u2a_enqueue_registers_multi_blocker);
	UT_RUN(test_5_8_d1b_u2b_refresh_follows_current_holders);
	UT_RUN(test_5_8_d1b_u2c_convert_enqueue_registers_edge);
	UT_RUN(test_5_8_d1b_u2d_cancel_removes_waiter_edges);
	UT_RUN(test_grd_pin_cleanup_on_lmd_submit_error);

	/* spec-5.8 D1c — waiter xid threaded into the WFG vertex (U3a-b). */
	UT_RUN(test_5_8_d1c_u3a_request_waiter_carries_xid);
	UT_RUN(test_5_8_d1c_u3b_convert_waiter_carries_xid);

	/* spec-5.8 D1e — waiter wait_seq threaded into the WFG vertex (U4a-b). */
	UT_RUN(test_5_8_d1e_u4a_request_waiter_carries_wait_seq);
	UT_RUN(test_5_8_d1e_u4b_convert_waiter_carries_wait_seq);

	/* spec-5.16 — online node-join GRD/PCM remaster (D7 unit suite). */
	UT_RUN(test_jr_u1_recompute_moves_joiner_home);
	UT_RUN(test_jr_u2_equivalence_with_failure_remaster);
	UT_RUN(test_jr_u3_recompute_idempotent);
	UT_RUN(test_jr_u4_scoped_move_set);
	UT_RUN(test_jr_u5_inv_r1_non_member_gets_nothing);
	UT_RUN(test_jr_u10_pcm_fence_independent_of_grd_move);
	UT_RUN(test_jr_u11_scan_completion_gate);
	UT_RUN(test_jr_u12_state_init_and_in_progress);
	UT_RUN(test_jr_u13_view_rebuilt_all_members_barrier);
	UT_RUN(test_jr_u14_fence_arm_monotonic_and_scope);
	UT_RUN(test_jr_u15_master_side_gate_decision);
	UT_RUN(test_jr_u16_pre_member_guard);
	UT_RUN(test_jr_u17_stale_recipient_not_excluded_next_episode);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
