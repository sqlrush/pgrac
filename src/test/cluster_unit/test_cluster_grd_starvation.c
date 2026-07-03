/*-------------------------------------------------------------------------
 *
 * test_cluster_grd_starvation.c
 *	  Standalone unit tests for spec-5.10 GES enqueue lock-starvation
 *	  fairness protection (bounded skip-count barrier + master-autonomous
 *	  head-of-line boost + master-local fair_queue_seq ordering + two-phase
 *	  FAIRNESS_BARRIER edge publish reusing the spec-5.8-owned canonical WFG
 *	  vertex).
 *
 *	  U1-U21 (spec-5.10 §4.1):
 *	    U1-U3  scan-on-grant skip accounting + threshold boost
 *	    U4-U6  grant barrier decision (boosted conflict vs not)
 *	    U7     boost never bypasses the conflict matrix (Rule 8.A)
 *	    U8     NOWAIT hitting a barrier -> CONFLICT_NOWAIT (no enqueue)
 *	    U9-U10 fair_queue_seq monotonic + wrap-safe + mint-only-on-enqueue
 *	    U11    serve-by-min-fair_queue_seq
 *	    U12-U13 convert capping (narrowed) + partial order intact
 *	    U14-U16 two-phase barrier edge (capture/release/publish/revalidate)
 *	    U17    barrier W vertex == spec-5.8 canonical vertex (graph not split)
 *	    U18    pre-enqueue edge tentative / revalidate-fail retracts
 *	    U19    sweep (node-dead clear + runtime-off via shared flag)
 *	    U20    GUC (max_skips + protection flag fall-back to 5.1b)
 *	    U21    public/private layout + canonical wait identity ownership
 *
 *	  The standalone binary links cluster_grd.o + cluster_ges_mode.o only;
 *	  the spec-5.8 LMD wait-edge API is modelled by the same faithful in-memory
 *	  ut_wfg spy used by test_cluster_grd.c, so the master-side FAIRNESS_BARRIER
 *	  edge can be asserted directly (the real-graph cross-node cycle e2e is the
 *	  spec-5.10 D10 TAP test t/29N).
 *
 *	  Spec: spec-5.10-lock-starvation-fairness-protection.md (frozen v1.0)
 *	  Lessons inherited: L5 / L48 / L107 / L146 / L251 / L257 / R12
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_grd_starvation.c
 *
 * NOTES
 *	  pgrac-original file.  Standalone binary linking cluster_grd.o +
 *	  cluster_ges_mode.o; all PG backend symbols stubbed locally (shared
 *	  stub preamble mirrors test_cluster_grd.c).
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
#include "cluster/cluster_ges_handoff.h" /* spec-6.12e1 — verifier stub types */
#include "cluster/cluster_ges_mode.h"	 /* spec-5.1b — frozen matrix + convert classification */
#include "access/transam.h"				 /* spec-5.8 D1c — InvalidTransactionId */
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
 * init between scenarios.  The stub allocates the buffer once, so without this
 * a repeat cluster_grd_shmem_init() would see found=true and skip shared-state
 * field init, including the spec-6.3a shard entry lists.
 */
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
/* spec-5.16 L104 — cluster_grd.c now references these (join fence predicates). */
int
cluster_gcs_lookup_master_static(BufferTag tag pg_attribute_unused())
{
	return 0;
}
bool
cluster_membership_is_member(int32 node_id pg_attribute_unused())
{
	return true;
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

/* spec-5.13 S3/D4 stub: cluster_grd_clean_leave_verify_no_leftover (in
 * cluster_grd.o) early-terminates via hash_seq_term; fake harness no-op. */
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

/* spec-5.10 — fault injection for the barrier edge publish (U15): when set, the
 * next cluster_lmd_submit_wait_edge_real returns false (table-full equivalent)
 * so the grant path's publish-fail fallback can be exercised. */
static bool ut_wfg_fail_submit = false;

static void
ut_wfg_reset(void)
{
	ut_wfg_n = 0;
	ut_wfg_fail_submit = false;
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

	if (ut_wfg_fail_submit) /* spec-5.10 U15 — model a full wait-edge table */
		return false;

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


/* spec-5.10 D7 GUC stubs (defined in cluster_guc.c in the backend build). */
int cluster_ges_starvation_max_skips = 8;

/* spec-6.12e1 — handoff-verifier stubs (cluster_grd.o's release_and_drain
 * references them).  Both arming GUCs stay false here, so the snapshot
 * branch never fires and the two functions are never reached; the real
 * verifier is exercised by test_cluster_ges_handoff. */
bool cluster_ges_handoff = false;
bool cluster_xnode_profile_enabled = false;
ClusterGesHandoffVerdict
cluster_ges_handoff_verify(const ClusterGesHandoffSnapshot *snap pg_attribute_unused())
{
	abort();
}
void
cluster_ges_handoff_note_drain(int n_granted pg_attribute_unused(),
							   ClusterGesHandoffVerdict verdict pg_attribute_unused())
{
	abort();
}


/* ============================================================
 * spec-5.10 — test driving helpers (mirror test_cluster_grd.c).
 * ============================================================ */

/* request_opcode tags (mirror cluster_ges.h GES_REQ_OPCODE_*; not included
 * here to avoid pulling the GES wire surface into the standalone binary). */
#define UT_GES_OPCODE_REQUEST 1
#define UT_GES_OPCODE_CONVERT 2

/* helper: build a resid from a tag id. */
static void
starv_resid(uint32 tagid, ClusterResId *resid)
{
	LOCKTAG src;

	memset(&src, 0, sizeof(src));
	src.locktag_field1 = tagid;
	src.locktag_type = LOCKTAG_RELATION;
	src.locktag_lockmethodid = 1;
	cluster_grd_resid_encode(&src, resid);
}

/* helper: a holder / requester identity. */
static ClusterGrdHolderId
starv_holder(int32 node, uint32 procno, uint64 reqid)
{
	ClusterGrdHolderId h;

	memset(&h, 0, sizeof(h));
	h.node_id = (uint32)node;
	h.procno = procno;
	h.request_id = reqid;
	return h;
}

/* Enable the GRD entry path on a fresh fake HTAB (mirror convert_reset). */
static void
starv_reset(void)
{
	reset_fake_grd_htab();
	cluster_grd_max_entries = 1000000;
	cluster_grd_shmem_init();
	/* shmem_init only zero-inits on the first-ever fake ShmemInitStruct, so
	 * force the protection toggle back on for each test. */
	cluster_grd_set_starvation_protection(true);
	ut_wfg_reset();
}

static void
starv_teardown(void)
{
	cluster_grd_max_entries = 0;
	reset_fake_grd_htab();
}

/* Drive a blocking REQUEST through the master enqueue/grant path. */
static ClusterGrdGrantAction
starv_request(const ClusterResId *resid, int32 node, uint32 procno, uint64 reqid, LOCKMODE mode)
{
	ClusterGrdHolderId h = starv_holder(node, procno, reqid);
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	return cluster_grd_entry_enqueue_or_grant(resid, &h, node, reqid, 0, UT_GES_OPCODE_REQUEST,
											  mode, conflicts, &nc);
}

/* Drive a conditional (NOWAIT) try-lock through the master path. */
static ClusterGrdGrantAction
starv_request_nowait(const ClusterResId *resid, int32 node, uint32 procno, uint64 reqid,
					 LOCKMODE mode)
{
	ClusterGrdHolderId h = starv_holder(node, procno, reqid);
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nc = -1;

	return cluster_grd_entry_grant_conditional(resid, &h, node, reqid, 0, UT_GES_OPCODE_REQUEST,
											   mode, conflicts, &nc);
}

/* Drive a blocking REQUEST carrying a spec-5.8 canonical wait identity (xid +
 * wait_seq) so the FAIRNESS_BARRIER edge's blocker vertex can be checked
 * against it (U17). */
static ClusterGrdGrantAction
starv_request_meta(const ClusterResId *resid, int32 node, uint32 procno, uint64 reqid,
				   LOCKMODE mode, TransactionId xid, uint64 wait_seq)
{
	ClusterGrdHolderId h = starv_holder(node, procno, reqid);
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	ClusterGrdWaiterMeta meta = { xid, wait_seq };
	int nc = -1;

	return cluster_grd_entry_enqueue_or_grant_meta(resid, &h, node, reqid, meta, 0,
												   UT_GES_OPCODE_REQUEST, mode, conflicts, &nc);
}

/* The xid / wait_seq stamped on the BLOCKER vertex of waiter (wn,wp,we,wr)'s
 * first edge (0 if no such edge) — used to assert the FAIRNESS_BARRIER edge
 * points at the spec-5.8 canonical vertex, not a fabricated one. */
static TransactionId
ut_wfg_blocker_xid(int32 wn, uint32 wp, uint64 we, uint64 wr)
{
	int i;

	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_wfg[i].waiter.node_id == wn && ut_wfg[i].waiter.procno == wp
			&& ut_wfg[i].waiter.cluster_epoch == we && ut_wfg[i].waiter.request_id == wr)
			return ut_wfg[i].blocker.xid;
	}
	return InvalidTransactionId;
}

static uint64
ut_wfg_blocker_wait_seq(int32 wn, uint32 wp, uint64 we, uint64 wr)
{
	int i;

	for (i = 0; i < ut_wfg_n; i++) {
		if (ut_wfg[i].waiter.node_id == wn && ut_wfg[i].waiter.procno == wp
			&& ut_wfg[i].waiter.cluster_epoch == we && ut_wfg[i].waiter.request_id == wr)
			return ut_wfg[i].blocker.wait_seq;
	}
	return 0;
}


/* Entry-level helpers (mirror test_cluster_grd.c) for the convert-capping
 * tests (U12/U13), which need precise holder / waiter / convert state. */
static ClusterGrdEntry *
starv_entry(const ClusterResId *resid)
{
	ClusterGrdEntry *e = NULL;

	(void)cluster_grd_entry_lookup_or_create(resid, true, &e);
	return e;
}

static void
starv_grant_holder(ClusterGrdEntry *e, int32 node, uint32 procno, uint64 reqid, LOCKMODE mode)
{
	ClusterGrdHolderId h = starv_holder(node, procno, reqid);

	UT_ASSERT_EQ((int)cluster_grd_entry_grant_holder(e, &h, mode), (int)CLUSTER_GRD_ENTRY_OK);
}

static void
starv_add_waiter(ClusterGrdEntry *e, int32 node, uint32 procno, uint64 reqid, LOCKMODE mode)
{
	ClusterGrdHolderId h = starv_holder(node, procno, reqid);

	UT_ASSERT_EQ((int)cluster_grd_entry_add_waiter(e, &h, mode), (int)CLUSTER_GRD_ENTRY_OK);
}

static ClusterGrdConvert
starv_convert_req(int32 node, uint32 procno, LOCKMODE cur, LOCKMODE want, uint64 cvid)
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
	r.request_opcode = UT_GES_OPCODE_CONVERT;
	return r;
}


/* ============================================================
 * spec-5.10 — starvation fairness tests (U1-U21).
 *	(tests appended below as the TDD cycle progresses)
 * ============================================================ */

UT_DEFINE_GLOBALS();


/* U1 — scan-on-grant: granting a holder-compatible request that conflicts with
 * an earlier queued waiter increments that waiter's skip_count (spec-5.10
 * §3.1, P0#1-r2).  An S-flood past an enqueued X waiter must register skips;
 * the grant itself is unaffected (Rule 8.A — never blocks the grant). */
UT_TEST(test_starv_u1_scan_on_grant_counts_skips)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId w;
	uint32 skip = 999;
	bool boosted = true;
	uint64 fseq = 0;

	cluster_node_id = 0;
	starv_reset();
	starv_resid(5100, &resid);

	/* S holder on node1 (no conflict -> granted). */
	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);

	/* X waiter W on node2 conflicts with the S holder -> enqueued. */
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	/* W is freshly enqueued: skip_count 0, not boosted. */
	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 0);
	UT_ASSERT_EQ((int)boosted, 0);

	/* A compatible S request on node3 is granted, but jumps past W (its S
	 * conflicts with W's wanted X) -> W.skip_count == 1. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 1);

	/* Another compatible S grant -> skip_count == 2. */
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 2);

	cluster_node_id = saved;
	starv_teardown();
}

/* U2 — boost at threshold: once a waiter's skip_count reaches max_skips it is
 * boosted to head-of-line (spec-5.10 §3.1).  Sub-threshold skips leave it
 * unboosted; the threshold-crossing skip sets the flag.  (The grant action of
 * the boosting request is asserted by U4 once the barrier lands — here we only
 * pin the boost-flag progression.) */
UT_TEST(test_starv_u2_boost_at_threshold)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	uint32 skip = 0;
	bool boosted = true;
	uint64 fseq = 0;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 3;
	starv_reset();
	starv_resid(5101, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w = starv_holder(2, 200, 2);

	/* Two sub-threshold S grants -> 2 skips, not boosted. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 2);
	UT_ASSERT_EQ((int)boosted, 0);

	/* Third skip reaches the threshold -> boosted (ignore this grant's action;
	 * the barrier behaviour of the boosting request is U4). */
	(void)starv_request(&resid, 5, 500, 5, ShareLock);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 3);
	UT_ASSERT_EQ((int)boosted, 1);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U3 — protection off: with ges_starvation_protection disabled the grant path
 * falls straight back to spec-5.1b — no scan-on-grant, no skip accounting, no
 * boost (spec-5.10 §2.2 / Q9 / P1#1).  Re-enabling resumes counting. */
UT_TEST(test_starv_u3_protection_off_no_skip)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId w;
	uint32 skip = 999;
	bool boosted = true;
	uint64 fseq = 0;

	cluster_node_id = 0;
	starv_reset();
	starv_resid(5102, &resid);
	cluster_grd_set_starvation_protection(false);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w = starv_holder(2, 200, 2);

	/* Protection off -> grants do not accrue skips. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 0);
	UT_ASSERT_EQ((int)boosted, 0);

	/* Re-enable -> counting resumes. */
	cluster_grd_set_starvation_protection(true);
	UT_ASSERT_EQ((int)starv_request(&resid, 5, 500, 5, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, &fseq));
	UT_ASSERT_EQ((int)skip, 1);

	cluster_node_id = saved;
	starv_teardown();
}

/* U9 — fair_queue_seq is minted monotonically per entry at enqueue time: each
 * queued waiter gets a strictly increasing master-local order (spec-5.10 §3.3,
 * D4).  This is the serve-by-min-seq ordering key, NOT a wait identity (Q12). */
UT_TEST(test_starv_u9_fair_queue_seq_monotonic)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId w2, w3;
	uint64 s2 = 0, s3 = 0;

	cluster_node_id = 0;
	starv_reset();
	starv_resid(5109, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ExclusiveLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	w2 = starv_holder(2, 200, 2);
	w3 = starv_holder(3, 300, 3);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w2, NULL, NULL, &s2));
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w3, NULL, NULL, &s3));
	UT_ASSERT(s2 >= 1);
	UT_ASSERT(s3 > s2);

	cluster_node_id = saved;
	starv_teardown();
}

/* U10 — fair_queue_seq is minted ONLY when a request actually enqueues (P1c):
 * holder-compatible grants (and the scan-on-grant pass they trigger) never
 * consume a seq, so two enqueues straddling a burst of grants are adjacent. */
UT_TEST(test_starv_u10_mint_only_on_enqueue)
{
	int saved = cluster_node_id;
	ClusterResId resid;
	ClusterGrdHolderId w2, w6;
	uint64 sa = 0, sb = 0;

	cluster_node_id = 0;
	starv_reset();
	starv_resid(5110, &resid);

	/* S holder; an X waiter; then a burst of compatible S grants; then a 2nd
	 * X waiter.  The S grants must not advance the mint source. */
	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w2 = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w2, NULL, NULL, &sa));

	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 5, 500, 5, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);

	UT_ASSERT_EQ((int)starv_request(&resid, 6, 600, 6, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w6 = starv_holder(6, 600, 6);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w6, NULL, NULL, &sb));

	UT_ASSERT_EQ((int)(sb - sa), 1); /* adjacent — grants minted nothing */

	cluster_node_id = saved;
	starv_teardown();
}

/* U4 — grant barrier: a holder-compatible request that conflicts with an
 * earlier boosted waiter is held behind it (enqueued, not granted) — spec-5.10
 * §3.1 / D2.  The jumper that crosses the boost threshold and every later
 * conflicting jumper are barriered. */
UT_TEST(test_starv_u4_barrier_holds_jumper_behind_boosted)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	bool boosted = false;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 2;
	starv_reset();
	starv_resid(5104, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	/* First S jumper: 1 skip, below threshold (2) -> still granted. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);

	/* Second S jumper boosts W (skip reaches 2) and is itself barriered. */
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 1);

	/* Subsequent conflicting jumpers are also barriered. */
	UT_ASSERT_EQ((int)starv_request(&resid, 5, 500, 5, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U11 — serve-side barrier + serve-by-min-fair_queue_seq (spec-5.10 §3.3, D4 /
 * P1a).  A holder-compatible jumper that is barriered behind a boosted waiter
 * must NOT be promoted by the drain while that boosted waiter is still queued;
 * the boosted waiter is served first once it becomes grantable. */
UT_TEST(test_starv_u11_serve_side_barrier_min_seq)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId h;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 2];
	int n;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5111, &resid);

	/* Two S holders; an X waiter (becomes boosted); an S jumper barriered. */
	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 5, 500, 5, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	/* S jumper boosts W (skip reaches 1) and is itself barriered. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	/* Release one S holder.  W(X) still conflicts with the remaining S holder,
	 * and R(S) is barriered behind W -> NOTHING is promoted (R held back). */
	h = starv_holder(1, 100, 1);
	n = cluster_grd_release_and_drain(&resid, &h, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 0);

	/* Release the other S holder -> holders empty -> W(X) is served (min seq,
	 * not barriered); R(S) is still held behind W (now an X holder). */
	h = starv_holder(5, 500, 5);
	n = cluster_grd_release_and_drain(&resid, &h, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ((int)granted[0].holder.procno, 200); /* W (node2) served first */

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U5 — boosted but NON-conflicting: the barrier only holds a jumper that
 * actually conflicts with the boosted waiter (spec-5.10 §3.1).  A boosted
 * ShareLock waiter does not bar an AccessShareLock grant (compatible). */
UT_TEST(test_starv_u5_boosted_non_conflicting_grants)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	bool boosted = false;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 2;
	starv_reset();
	starv_resid(5105, &resid);

	/* RowExclusive holder; a Share waiter (conflicts -> enqueued); RowExclusive
	 * jumpers boost the Share waiter. */
	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, RowExclusiveLock),
				 (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, RowExclusiveLock),
				 (int)CLUSTER_GRD_GRANT_NOW);
	(void)starv_request(&resid, 4, 400, 4, RowExclusiveLock); /* boosts W */
	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 1);

	/* AccessShare is compatible with the boosted Share waiter -> granted. */
	UT_ASSERT_EQ((int)starv_request(&resid, 5, 500, 5, AccessShareLock),
				 (int)CLUSTER_GRD_GRANT_NOW);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U6 — a conflicting waiter that is NOT yet boosted does not trigger the
 * barrier: a compatible jumper still grants (the barrier requires a boost, not
 * merely a conflicting waiter) — spec-5.10 §3.1. */
UT_TEST(test_starv_u6_unboosted_conflict_still_grants)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 8; /* high threshold -> stays unboosted */
	starv_reset();
	starv_resid(5106, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	/* The X waiter is conflicting but unboosted -> S jumper still grants. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U7 — Rule 8.A: boost never bypasses the conflict matrix.  A request that
 * conflicts with a HOLDER is always enqueued, never granted, even when a
 * boosted waiter is present (the barrier only ever converts grant->enqueue). */
UT_TEST(test_starv_u7_boost_never_false_grants)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5107, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER); /* boosts W, barriered */

	/* Another ExclusiveLock conflicts with the S HOLDER -> enqueued (never
	 * granted) regardless of fairness machinery. */
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U8 — a NOWAIT (conditional) try-lock that hits the barrier returns
 * CONFLICT_NOWAIT and enqueues nothing (spec-5.10 §3.1, P0#1-r3): a NOWAIT
 * request can never wait, so the barrier is reported as a try-lock failure. */
UT_TEST(test_starv_u8_nowait_hits_barrier_conflict_nowait)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId probe;
	uint32 skip = 0;
	bool boosted = false;
	uint64 fseq = 0;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5108, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER); /* boosts W, barriered */

	/* NOWAIT S request hits the barrier -> CONFLICT_NOWAIT, no waiter added. */
	UT_ASSERT_EQ((int)starv_request_nowait(&resid, 9, 900, 9, ShareLock),
				 (int)CLUSTER_GRD_CONFLICT_NOWAIT);
	probe = starv_holder(9, 900, 9);
	UT_ASSERT(!cluster_grd_entry_describe_waiter(&resid, &probe, &skip, &boosted, &fseq));

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U14 — the FAIRNESS_BARRIER edge R -> W is registered on the master-side WFG
 * and SURVIVES the resync that the enqueue triggers (spec-5.10 §3.4, D5: the
 * edge is part of the refresh set, else the resync after the barrier-enqueue
 * would wipe it and a barrier deadlock would be missed — R2). */
UT_TEST(test_starv_u14_barrier_edge_registered_and_survives_resync)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5114, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER); /* W = node2 */
	/* node3 boosts W and is barriered behind it -> R = node3. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	/* The barrier edge R(node3) -> W(node2) is present AFTER the enqueue resync. */
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));
	/* R is holder-compatible (S vs S holder) so its ONLY WFG edge is the
	 * barrier edge -> exactly one out-edge. */
	UT_ASSERT_EQ(ut_wfg_count_waiter(3, 300, 0, 3), 1);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U15 — barrier edge publish failure falls back to the spec-5.1b decision
 * (Rule 8.A — fail closed to the safe path): the jumper is granted (it is
 * holder-compatible) and the publish-fail counter advances (spec-5.10 §3.4). */
UT_TEST(test_starv_u15_publish_fail_falls_back_to_grant)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	uint64 before;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5115, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);

	before = cluster_grd_starvation_barrier_publish_fail_count();
	ut_wfg_fail_submit = true; /* model a full wait-edge table */
	/* node3 boosts W, tries to barrier, publish fails -> fall back to grant. */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT(cluster_grd_starvation_barrier_publish_fail_count() > before);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U16 — observability counters: a boost advances starvation_boost_count and a
 * barrier-enqueue advances starvation_barrier_enqueued_count (spec-5.10 D7). */
UT_TEST(test_starv_u16_observability_counters)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	uint64 boost0, barr0;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5116, &resid);
	boost0 = cluster_grd_starvation_boost_count();
	barr0 = cluster_grd_starvation_barrier_enqueued_count();

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER); /* boost + barrier */

	UT_ASSERT_EQ((int)(cluster_grd_starvation_boost_count() - boost0), 1);
	UT_ASSERT_EQ((int)(cluster_grd_starvation_barrier_enqueued_count() - barr0), 1);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U17 — the FAIRNESS_BARRIER edge points at the spec-5.8-OWNED canonical vertex
 * (P0#3 — never fabricated): the blocker vertex carries the boosted waiter's
 * persisted waiter_xid + wait_seq, so the barrier vertex, the spec-5.8 detector
 * vertex, and the spec-5.9 cancel vertex are one and the same (graph not split,
 * L5). */
UT_TEST(test_starv_u17_barrier_edge_uses_canonical_vertex)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5117, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	/* W carries a real canonical wait identity (xid 12345, wait_seq 678). */
	UT_ASSERT_EQ((int)starv_request_meta(&resid, 2, 200, 2, ExclusiveLock, 12345, 678),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER); /* R barriered behind W */

	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));
	UT_ASSERT_EQ((int)ut_wfg_blocker_xid(3, 300, 0, 3), 12345);
	UT_ASSERT_EQ((int)ut_wfg_blocker_wait_seq(3, 300, 0, 3), 678);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U18 — the pre-enqueue barrier edge is TENTATIVE / retractable (P1#2): it is
 * conditional on the boost being live, so disabling protection retracts it on
 * the next refresh (a barrier edge never outlives the boost it represents). */
UT_TEST(test_starv_u18_barrier_edge_tentative_retracts)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5118, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	/* Disable protection, then drive a resync (an unrelated enqueue): the
	 * barrier edge is not re-emitted -> retracted. */
	cluster_grd_set_starvation_protection(false);
	UT_ASSERT_EQ((int)starv_request(&resid, 7, 700, 7, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT(!ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U12 — convert capping fires (spec-5.10 §3.2 / D3, narrowed P1c-r2): a BOOSTED
 * waiter that is already compatible with every holder but is held back only by
 * a pending convert's priority is served BEFORE the convert (the convert stays
 * queued one round).  W is boosted via convert-side scan-on-grant, then a
 * holder converts back so W is holder-compatible again with a pending convert
 * still blocking it. */
UT_TEST(test_starv_u12_convert_cap_serves_boosted_waiter)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 2];
	bool drain = false;
	int n;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5112, &resid);
	e = starv_entry(&resid);

	/* Two RowShare holders; a holder-compatible Share waiter W. */
	starv_grant_holder(e, 1, 100, 1, RowShareLock);
	starv_grant_holder(e, 2, 200, 2, RowShareLock);
	starv_add_waiter(e, 3, 300, 3, ShareLock); /* W: S compatible with RowShare */

	/* node2 RowShare->RowExclusive (granted in place) jumps W (S vs RowX) -> boost. */
	req = starv_convert_req(2, 200, RowShareLock, RowExclusiveLock, 20);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	/* node2 converts back to RowShare so W is holder-compatible again. */
	req = starv_convert_req(2, 200, RowExclusiveLock, RowShareLock, 21);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	/* node1 RowShare->Exclusive can't grant (conflicts node2 RowShare) -> pending
	 * convert that blocks W (S vs X). */
	req = starv_convert_req(1, 100, RowShareLock, ExclusiveLock, 10);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	/* Drain: the cap serves the boosted, holder-compatible W ahead of the
	 * pending convert (which stays queued). */
	n = cluster_grd_entry_drain_converts_then_waiters(e, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ((int)granted[0].request_opcode,
				 UT_GES_OPCODE_REQUEST);			  /* a waiter, not a convert */
	UT_ASSERT_EQ((int)granted[0].holder.procno, 300); /* W */
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);  /* convert still pending */

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U13 — convert capping does NOT fire when the boosted waiter is still blocked
 * by a HOLDER (not merely by convert priority); the convert priority order is
 * preserved (spec-5.10 §3.2 "boosted waiter 仍被 holder 挡 → 不暂停 convert" +
 * partial order intact). */
UT_TEST(test_starv_u13_convert_cap_skips_holder_blocked_waiter)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdEntry *e;
	ClusterGrdConvert req;
	ClusterGrdGrantIdentity granted[PGRAC_GRD_MAX_CONVERTS_PUBLIC + 2];
	ClusterGrdHolderId h;
	bool drain = false;
	int n;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5113, &resid);
	e = starv_entry(&resid);

	/* Two Share holders; an Exclusive waiter W (conflicts both -> holder-blocked). */
	starv_grant_holder(e, 1, 100, 1, ShareLock);
	starv_grant_holder(e, 2, 200, 2, ShareLock);
	starv_add_waiter(e, 3, 300, 3, ExclusiveLock); /* W: X conflicts with S holders */

	/* node2 Share->RowShare (granted in place) jumps W (X vs RowShare) -> boost. */
	req = starv_convert_req(2, 200, ShareLock, RowShareLock, 20);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_GRANTED_INPLACE);
	/* node1 Share->Exclusive can't grant (conflicts node2 RowShare) -> pending. */
	req = starv_convert_req(1, 100, ShareLock, ExclusiveLock, 10);
	UT_ASSERT_EQ((int)cluster_grd_entry_request_convert(e, &req, &drain),
				 (int)CLUSTER_GRD_CONVERT_ENQUEUED);

	/* Drain: W is boosted but still holder-blocked -> cap does NOT fire, and the
	 * convert cannot grant either (blocked by node2 RowShare) -> nothing served. */
	n = cluster_grd_entry_drain_converts_then_waiters(e, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 0);
	UT_ASSERT_EQ(cluster_grd_entry_nconverts(e), 1);

	/* Release node2: the pending convert now grants in place (partial order:
	 * convert before the still-conflicting waiter W). */
	h = starv_holder(2, 200, 2);
	UT_ASSERT_EQ((int)cluster_grd_entry_release_holder(e, &h), (int)CLUSTER_GRD_ENTRY_OK);
	n = cluster_grd_entry_drain_converts_then_waiters(e, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ((int)granted[0].request_opcode, UT_GES_OPCODE_CONVERT); /* convert, not W */

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U19 — sweep (spec-5.10 §3.7 / D8).  cluster_grd_clear_all_boosted (runtime-off
 * clean escape) clears every boost and retracts the FAIRNESS_BARRIER edges;
 * cluster_grd_clear_boosted_for_node clears only a departed node's boosts. */
UT_TEST(test_starv_u19_sweep_clears_boost_and_edges)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	bool boosted = false;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5119, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 1);
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	/* Runtime-off sweep: clears the boost and retracts the barrier edge. */
	UT_ASSERT_EQ((int)cluster_grd_clear_all_boosted(), 1);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 0);
	UT_ASSERT(!ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	/* Node-dead variant: re-boost, then sweep only the boosted node (node2). */
	(void)starv_request(&resid, 4, 400, 4, ShareLock); /* re-boosts W */
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 1);
	UT_ASSERT_EQ((int)cluster_grd_clear_boosted_for_node(2), 1);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 0);

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U20 — GUC cluster.ges_starvation_max_skips: 0 disables boosting entirely
 * (waiters still accrue skip counts for observability, but never boost, so a
 * jumper is never barriered) — spec-5.10 §2.3 / D7. */
UT_TEST(test_starv_u20_max_skips_zero_disables_boost)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	uint32 skip = 0;
	bool boosted = true;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 0; /* boosting disabled */
	starv_reset();
	starv_resid(5120, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	/* Many jumpers all grant (W accrues skips but never boosts -> no barrier). */
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 4, 400, 4, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 5, 500, 5, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);

	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, &skip, &boosted, NULL));
	UT_ASSERT_EQ((int)skip, 3);	   /* counted */
	UT_ASSERT_EQ((int)boosted, 0); /* but never boosted */

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U21 — public/private layout + canonical-wait-identity ownership (R12 + L5):
 * the public ClusterGrdConvert pins the spec-5.10 fairness fields at fixed
 * offsets WITHOUT moving the spec-5.8-owned canonical wait identity
 * (waiter_xid @52, wait_seq @64) — spec-5.10 only consumes it (Q12 / L5).  The
 * private fair_queue_next / ClusterGrdWaiter fields live in cluster_grd.c and
 * are exercised behaviourally by U1-U18. */
UT_TEST(test_starv_u21_layout_and_identity_ownership)
{
	UT_ASSERT_EQ((int)sizeof(ClusterGrdConvert), 88);
	/* spec-5.8-owned canonical wait identity NOT moved by spec-5.10. */
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, waiter_xid), 52);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, wait_seq), 64);
	/* spec-5.10 fairness fields appended after the spec-5.8 identity. */
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, fair_queue_seq), 72);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, skip_count), 80);
	UT_ASSERT_EQ((int)offsetof(ClusterGrdConvert, boosted), 84);
}

/* U22 — runtime-off LMON sweep wiring (fix-forward): turning protection off sets
 * the sweep-pending flag, and the LMON-tick sweep then clears boosted +
 * retracts the FAIRNESS_BARRIER edges (the sweep is now reachable from a
 * production path, not only the direct U19 helper call). */
UT_TEST(test_starv_u22_runtime_off_lmon_sweep)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	bool boosted = false;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5122, &resid);

	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 1);
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	/* Protection on + no request pending -> LMON sweep tick is a no-op. */
	UT_ASSERT_EQ((int)cluster_grd_lmon_tick_starvation_sweep(), 0);

	/* Turn protection OFF -> sets sweep-pending; the LMON tick then clears the
	 * boost + retracts the barrier edge (clean escape). */
	cluster_grd_set_starvation_protection(false);
	UT_ASSERT_EQ((int)cluster_grd_lmon_tick_starvation_sweep(), 1);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 0);
	UT_ASSERT(!ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	/* Pending consumed -> a second tick is a no-op. */
	UT_ASSERT_EQ((int)cluster_grd_lmon_tick_starvation_sweep(), 0);

	cluster_grd_set_starvation_protection(true);
	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}

/* U23 — node-dead sweep un-barriers live waiters (fix-forward order): clearing a
 * dead node's boosted W (before its slots are removed by the cleanup) re-derives
 * every live waiter without that boost, so a live request held behind the dead W
 * is un-barriered and its stale R->W edge is retracted. */
UT_TEST(test_starv_u23_node_dead_unbarriers_live_waiter)
{
	int saved = cluster_node_id;
	int saved_skips = cluster_ges_starvation_max_skips;
	ClusterResId resid;
	ClusterGrdHolderId w;
	bool boosted = false;

	cluster_node_id = 0;
	cluster_ges_starvation_max_skips = 1;
	starv_reset();
	starv_resid(5123, &resid);

	/* Holder on node1; boosted X waiter W on node2; live S jumper R on node3
	 * barriered behind W. */
	UT_ASSERT_EQ((int)starv_request(&resid, 1, 100, 1, ShareLock), (int)CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ((int)starv_request(&resid, 2, 200, 2, ExclusiveLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ((int)starv_request(&resid, 3, 300, 3, ShareLock),
				 (int)CLUSTER_GRD_ENQUEUED_WAITER);
	w = starv_holder(2, 200, 2);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 1);
	UT_ASSERT(ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2)); /* R barriered behind W */

	/* node2 (W's node) dies: clear its boost FIRST (the fix-forward order) ->
	 * W's boost cleared, live R re-derived without the barrier, R->W retracted. */
	UT_ASSERT_EQ((int)cluster_grd_clear_boosted_for_node(2), 1);
	UT_ASSERT(cluster_grd_entry_describe_waiter(&resid, &w, NULL, &boosted, NULL));
	UT_ASSERT_EQ((int)boosted, 0);
	UT_ASSERT(!ut_wfg_has_edge(3, 300, 0, 3, 2, 200, 0, 2));

	cluster_ges_starvation_max_skips = saved_skips;
	cluster_node_id = saved;
	starv_teardown();
}


int
/* cppcheck-suppress constParameter
 * Reason: main() keeps the standard test harness signature; argv unused. */
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(23);

	UT_RUN(test_starv_u1_scan_on_grant_counts_skips);
	UT_RUN(test_starv_u2_boost_at_threshold);
	UT_RUN(test_starv_u3_protection_off_no_skip);
	UT_RUN(test_starv_u9_fair_queue_seq_monotonic);
	UT_RUN(test_starv_u10_mint_only_on_enqueue);
	UT_RUN(test_starv_u4_barrier_holds_jumper_behind_boosted);
	UT_RUN(test_starv_u5_boosted_non_conflicting_grants);
	UT_RUN(test_starv_u6_unboosted_conflict_still_grants);
	UT_RUN(test_starv_u7_boost_never_false_grants);
	UT_RUN(test_starv_u8_nowait_hits_barrier_conflict_nowait);
	UT_RUN(test_starv_u11_serve_side_barrier_min_seq);
	UT_RUN(test_starv_u12_convert_cap_serves_boosted_waiter);
	UT_RUN(test_starv_u13_convert_cap_skips_holder_blocked_waiter);
	UT_RUN(test_starv_u14_barrier_edge_registered_and_survives_resync);
	UT_RUN(test_starv_u15_publish_fail_falls_back_to_grant);
	UT_RUN(test_starv_u16_observability_counters);
	UT_RUN(test_starv_u17_barrier_edge_uses_canonical_vertex);
	UT_RUN(test_starv_u18_barrier_edge_tentative_retracts);
	UT_RUN(test_starv_u19_sweep_clears_boost_and_edges);
	UT_RUN(test_starv_u20_max_skips_zero_disables_boost);
	UT_RUN(test_starv_u21_layout_and_identity_ownership);
	UT_RUN(test_starv_u22_runtime_off_lmon_sweep);
	UT_RUN(test_starv_u23_node_dead_unbarriers_live_waiter);

	return ut_failed_count == 0 ? 0 : 1;
}
