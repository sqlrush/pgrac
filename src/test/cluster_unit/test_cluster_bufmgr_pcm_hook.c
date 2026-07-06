/*-------------------------------------------------------------------------
 *
 * test_cluster_bufmgr_pcm_hook.c
 *	  Unit tests for the spec-2.31 bufmgr LockBuffer PCM hook structure.
 *
 *	  spec-2.31 inserts a PGRAC MODIFICATIONS block into bufmgr.c's
 *	  LockBuffer body: PCM acquire-before-LWLock (HC64), LWLockAcquire
 *	  wrapped in PG_TRY/PG_CATCH that releases PCM on error (HC66), and
 *	  PCM release-after-LWLock on UNLOCK (HC65 reverse order).  We can't
 *	  link the full PG bufmgr in a unit test, so this binary mirrors the
 *	  hook structure in a fake_LockBuffer wrapper and asserts the
 *	  ordering / gate / success-path / failure-path / local-buffer / two
 *	  disable-mode invariants (L1-L8).
 *
 *	  L1  cluster_pcm_is_active() truth table (enabled/node_id/peers/max_entries)
 *	  L2  PCM acquire fires before LWLock acquire (call_log order)
 *	  L3  PCM release fires after LWLock release (reverse order)
 *	  L4  success path:  buf->buffer_type = SCUR / XCUR; pcm_state = S / X
 *	  L5  ereport at LWLock acquire:  PCM released via PG_CATCH;
 *	       buf->buffer_type unchanged
 *	  L6  is_local=true:  hook skipped entirely
 *	  L7  cluster_enabled=false:  hook skipped (gate Layer 2)
 *	  L8  cluster_pcm_grd_max_entries=0:  cluster_pcm_is_active()=false → skipped
 *	  L9  unlock without prior PCM ownership skips PCM release
 *
 *	  Links cluster_pcm_lock.o so the real acquire/release behavior is
 *	  exercised;  PG-runtime stubs (ShmemInit*, LWLock*, hash_*, CV*) mirror
 *	  the same stub family used by test_cluster_pcm_lock.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_bufmgr_pcm_hook.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_buffer_desc.h"
#include "cluster/cluster_gcs_block.h" /* spec-4.7 D1 — ClusterGcsBlockPhase + phase_for_tag proto */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "storage/backendid.h" /* spec-6.14 D9 amend — MyBackendId stub */
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include <setjmp.h>

#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 * PG-runtime stubs + fake shared HTAB (mirrors test_cluster_pcm_lock.c).
 * ============================================================ */

int cluster_node_id = 0;
int NBuffers = 0;
int cluster_injection_armed_count = 0;
bool cluster_enabled = true; /* PGRAC: spec-2.31 D2 helper depends on this */

/* spec-4.7a D2 — the bufmgr PCM hook reads this GUC to decide hold-until-revoked
 * (preserve buf->pcm_state on content-lock unlock).  Stubbed ON (production
 * default) so these tests exercise the preserve path; that path is single-node
 * honest (local buf->pcm_state retention, no peer liveness needed). */
bool cluster_gcs_block_local_cache = true;

/* spec-6.12a stubs — the local-master S->X upgrade branch is wave-gated off
 * (cluster_read_scache default false), so it is inert here; provide inert
 * link-surface satisfaction.  Real coverage is t/352 (2-node). */
bool cluster_read_scache = false;

bool cluster_gcs_block_local_x_upgrade(BufferTag tag);
bool
cluster_gcs_block_local_x_upgrade(BufferTag tag pg_attribute_unused())
{
	return false;
}

/* spec-4.7a D4 — cluster_pcm_lock.o now references cluster_cssd_get_peer_state
 * (the other-live-holder gate).  This test links cluster_pcm_lock.o, so it
 * must provide the symbol; return ALIVE (this test does not exercise the D4
 * gate, only the bufmgr PCM hook).  Mirrors test_cluster_grd.c's stub. */
int /* ClusterCssdPeerState */
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return 0; /* CLUSTER_CSSD_PEER_ALIVE */
}

/*
 * spec-4.7 D1 (L238) — cluster_pcm_lock.o's acquire_buffer now opens with a
 * RECOVERING gate referencing cluster_gcs_block_phase_for_tag,
 * cluster_gcs_block_recovery_wait_ms and CHECK_FOR_INTERRUPTS.  This test
 * links cluster_pcm_lock.o but not cluster_gcs_block.o / cluster_guc.o /
 * postmaster core.  phase_for_tag → NORMAL keeps the gate a no-op so the
 * bufmgr PCM hook tests exercise the local hold-until-revoked path, not the
 * recovery path (covered e2e by t/251).
 */
volatile sig_atomic_t InterruptPending = false;
void ProcessInterrupts(void);
void
ProcessInterrupts(void)
{}
int cluster_gcs_block_recovery_wait_ms = 200;
ClusterGcsBlockPhase
cluster_gcs_block_phase_for_tag(BufferTag tag pg_attribute_unused())
{
	return GCS_BLOCK_NORMAL;
}

/* spec-4.7 D3 (L238) — rebuild fn's not-double-X branch references this 4.6
 * counter via cluster_pcm_lock.o;  stub no-op. */
void cluster_grd_inc_block_path_failclosed(void);
void
cluster_grd_inc_block_path_failclosed(void)
{}

static ClusterConf fake_cluster_conf = {
	.magic = PGRAC_CLUSTER_CONF_MAGIC,
	.node_count = 2,
};
ClusterConf *ClusterConfShmem = &fake_cluster_conf;

static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

#define FAKE_PCM_MAX_ENTRIES 8
#define FAKE_PCM_ENTRY_BYTES 1024

static union {
	uint64 force_align;
	char data[4096];
} fake_pcm_header;

static union {
	uint64 force_align;
	char data[FAKE_PCM_MAX_ENTRIES][FAKE_PCM_ENTRY_BYTES];
} fake_pcm_entries;

static char fake_pcm_htab_token;
static bool fake_pcm_header_found = false;
static long fake_pcm_entry_count = 0;
static long fake_pcm_entry_max = 0;
static Size fake_pcm_keysize = 0;
static Size fake_pcm_entrysize = 0;
static LWLock *fake_lwlock_held = NULL;
static LWLockMode fake_lwlock_mode = LW_EXCLUSIVE;

/* Order tracking — each call appends a tag to call_log so tests can
 * assert ordering of PCM-vs-LWLock acquire/release. */
#define CALL_LOG_MAX 32
static int call_log_len = 0;
static const char *call_log[CALL_LOG_MAX];

static void
call_log_push(const char *tag)
{
	if (call_log_len < CALL_LOG_MAX)
		call_log[call_log_len++] = tag;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 200;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	Assert(size <= sizeof(fake_pcm_header.data));
	*foundPtr = fake_pcm_header_found;
	fake_pcm_header_found = true;
	return fake_pcm_header.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size, HASHCTL *infoP, int hash_flags pg_attribute_unused())
{
	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->entrysize <= FAKE_PCM_ENTRY_BYTES);
	Assert(max_size <= FAKE_PCM_MAX_ENTRIES);
	fake_pcm_keysize = infoP->keysize;
	fake_pcm_entrysize = infoP->entrysize;
	fake_pcm_entry_max = max_size;
	fake_pcm_entry_count = 0;
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	return (HTAB *)&fake_pcm_htab_token;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr, HASHACTION action,
			bool *foundPtr)
{
	long i;

	Assert(fake_pcm_keysize > 0);

	for (i = 0; i < fake_pcm_entry_count; i++) {
		char *entry = fake_pcm_entries.data[i];

		if (memcmp(entry, keyPtr, fake_pcm_keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			return entry;
		}
	}

	if (action != HASH_ENTER && action != HASH_ENTER_NULL) {
		if (foundPtr != NULL)
			*foundPtr = false;
		return NULL;
	}

	if (fake_pcm_entry_count >= fake_pcm_entry_max) {
		if (action == HASH_ENTER_NULL) {
			if (foundPtr != NULL)
				*foundPtr = false;
			return NULL;
		}
		Assert(false);
	}

	memcpy(fake_pcm_entries.data[fake_pcm_entry_count], keyPtr, fake_pcm_keysize);
	if (foundPtr != NULL)
		*foundPtr = false;
	return fake_pcm_entries.data[fake_pcm_entry_count++];
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return fake_pcm_entry_count;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp pg_attribute_unused())
{
	status->curBucket = 0;
	status->hashp = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	if (status->curBucket >= (uint32)fake_pcm_entry_count)
		return NULL;
	return fake_pcm_entries.data[status->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	fake_lwlock_held = lock;
	fake_lwlock_mode = mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (fake_lwlock_held == lock)
		fake_lwlock_held = NULL;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return fake_lwlock_held == lock && fake_lwlock_mode == mode;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return (TimestampTz)0;
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

/* PGRAC spec-2.32 D5 stubs:  cluster_pcm_lock.c calls cluster_gcs helpers
 * from each mutation entry point.  Test fixture forces local path. */
int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return cluster_node_id;
}

void
cluster_gcs_send_transition_and_wait(BufferTag tag pg_attribute_unused(),
									 PcmLockTransition trans pg_attribute_unused(),
									 int master_node pg_attribute_unused())
{
	abort();
}

/* spec-2.33 D3 stub:  data-plane sender unreachable in this fixture. */
bool
cluster_gcs_send_block_request_and_wait(struct BufferDesc *buf pg_attribute_unused(),
										PcmLockTransition trans pg_attribute_unused(),
										int master_node pg_attribute_unused(),
										bool clean_eligible pg_attribute_unused())
{
	abort();
}

/* spec-5.2 D2 sub-case B stub: local-master read-image forward unreachable. */
bool
cluster_gcs_local_master_read_image_and_wait(struct BufferDesc *buf pg_attribute_unused(),
											 int32 holder_node pg_attribute_unused())
{
	abort();
}

/* spec-5.2 D11 stub: local-master X-transfer forward unreachable in this unit. */
bool
cluster_gcs_local_master_x_transfer_and_wait(struct BufferDesc *buf pg_attribute_unused(),
											 int32 holder_node pg_attribute_unused(),
											 bool clean_eligible pg_attribute_unused())
{
	abort();
}

/* spec-2.35 D3 stub for HC110 master_holder lifecycle counter bump. */
void
cluster_gcs_block_bump_master_holder_lifecycle(void)
{}

Size
hash_estimate_size(long num_entries, Size entry_size)
{
	return num_entries * entry_size;
}

void
ConditionVariableInit(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariableSleep(ConditionVariable *cv pg_attribute_unused(),
					   uint32 wait_event_info pg_attribute_unused())
{
	/* never reached in these tests (single-threaded; no contention) */
}

bool
ConditionVariableCancelSleep(void)
{
	return false;
}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariableSignal(ConditionVariable *cv pg_attribute_unused())
{}

void *
ShmemAllocUnlocked(Size size pg_attribute_unused())
{
	abort();
}

/* PG ereport stubs — cluster_pcm_lock.o references errstart/errfinish/etc.
 * for ERROR paths that none of these tests trigger (we control the gate
 * conditions explicitly). */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
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
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* spec-6.14 D9 amend: acquire_buffer's no-backend-identity guard reads
 * MyBackendId; a valid id (1) keeps the historical acquire paths open. */
BackendId MyBackendId = 1;

/* ============================================================
 * Minimal mock BufferDesc with just the fields the hook touches.
 *
 *	The real bufmgr.c LockBuffer reads buf->tag and writes
 *	buf->buffer_type / buf->pcm_state.  These are the only fields the
 *	spec-2.31 D3 hook touches.
 * ============================================================ */
typedef struct MockBufferDesc {
	BufferTag tag;
	uint8 buffer_type;
	uint8 pcm_state;
} MockBufferDesc;

#define BUFFER_LOCK_UNLOCK 0
#define BUFFER_LOCK_SHARE 1
#define BUFFER_LOCK_EXCLUSIVE 2

/* Test-controlled toggle to force the ereport branch at LWLock acquire. */
static bool fake_ereport_at_lwlock = false;

/* bufpage.h PageSetLSN merged-window hook globals (spec-4.5a §3.3b):
 * normally defined by cluster_recovery_merge.c, which this standalone
 * binary does not link.  Inactive stubs keep the inline's window gate
 * permanently false here. */
bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;

/* ============================================================
 * Fake LockBuffer wrapper — mirrors spec-2.31 D3 hook body verbatim.
 *
 *	Returns 0 on success, 1 on ereport caught.  Tests use 1 to verify
 *	HC66 PG_CATCH cleanup behavior without triggering real ereport
 *	(which would tear down the process).
 * ============================================================ */
static int
fake_LockBuffer(MockBufferDesc *buf, int mode, bool is_local)
{
	bool pcm_acquired = false;
	PcmLockMode pcm_mode = PCM_LOCK_MODE_N;

	if (is_local)
		return 0; /* HC68: local buffer skip */

	/* HC64: PCM acquire before LWLock acquire. */
	if (mode != BUFFER_LOCK_UNLOCK && cluster_pcm_is_active()) {
		pcm_mode = (mode == BUFFER_LOCK_SHARE) ? PCM_LOCK_MODE_S : PCM_LOCK_MODE_X;
		cluster_pcm_lock_acquire(buf->tag, pcm_mode);
		call_log_push("PCM-acquire");
		pcm_acquired = true;
	}

	if (mode == BUFFER_LOCK_UNLOCK) {
		call_log_push("LWLock-release");
	} else {
		/* Mimics the PG_TRY/PG_CATCH structure of the bufmgr hook;
		 * fake_ereport_at_lwlock=true simulates the LWLock ereport path. */
		if (fake_ereport_at_lwlock) {
			/* HC66: must release PCM before re-throw. */
			if (pcm_acquired) {
				cluster_pcm_lock_release(buf->tag);
				call_log_push("PCM-release-on-ereport");
			}
			return 1; /* simulated RE_THROW */
		}

		call_log_push("LWLock-acquire");
		if (pcm_acquired) {
			/* HC66 success path: update buffer_type + pcm_state. */
			cluster_buffer_desc_apply_pcm_ownership_fields(&buf->buffer_type, &buf->pcm_state,
														   pcm_mode);
		}
	}

	/* HC65: PCM release after LWLock release, on UNLOCK. */
	if (mode == BUFFER_LOCK_UNLOCK && cluster_pcm_is_active()
		&& buf->pcm_state != (uint8)PCM_STATE_N) {
		cluster_pcm_lock_release(buf->tag);
		call_log_push("PCM-release");
		buf->pcm_state = (uint8)cluster_pcm_lock_query(buf->tag);
	}

	return 0;
}

static void
reset_fixture(void)
{
	memset(&fake_pcm_header, 0, sizeof(fake_pcm_header));
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	fake_pcm_header_found = false;
	fake_pcm_entry_count = 0;
	fake_pcm_entry_max = 0;
	fake_pcm_keysize = 0;
	fake_pcm_entrysize = 0;
	fake_lwlock_held = NULL;
	ut_wait_event_info_storage = 0;
	call_log_len = 0;
	fake_ereport_at_lwlock = false;
	cluster_node_id = 0;
	cluster_enabled = true;
	fake_cluster_conf.node_count = 2;
	NBuffers = 4;
	cluster_pcm_grd_max_entries = 4;
	cluster_pcm_grd_init();
}


/* ============================================================
 * Tests L1-L9.
 * ============================================================ */

UT_TEST(test_L1_is_active_triple_gate_truth_table)
{
	/* (cluster_enabled, cluster_node_id, has_peers, max_entries)
	 * → cluster_pcm_is_active(). */
	cluster_enabled = true;
	cluster_node_id = 0;
	fake_cluster_conf.node_count = 2;
	cluster_pcm_grd_max_entries = 4;
	UT_ASSERT(cluster_pcm_is_active());

	cluster_enabled = false;
	UT_ASSERT(!cluster_pcm_is_active());

	cluster_enabled = true;
	cluster_node_id = -1; /* single-node fallback / pgrac.conf not loaded */
	UT_ASSERT(!cluster_pcm_is_active());

	cluster_node_id = 0;
	fake_cluster_conf.node_count = 1;
	UT_ASSERT(!cluster_pcm_is_active());

	fake_cluster_conf.node_count = 2;
	cluster_pcm_grd_max_entries = 0;
	UT_ASSERT(!cluster_pcm_is_active());

	cluster_enabled = false;
	cluster_node_id = -1;
	cluster_pcm_grd_max_entries = 0;
	UT_ASSERT(!cluster_pcm_is_active());
}


UT_TEST(test_L2_pcm_acquired_before_lwlock)
{
	MockBufferDesc buf;

	reset_fixture();
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(101);

	UT_ASSERT_EQ(fake_LockBuffer(&buf, BUFFER_LOCK_SHARE, false), 0);
	UT_ASSERT(call_log_len >= 2);
	UT_ASSERT(strcmp(call_log[0], "PCM-acquire") == 0);
	UT_ASSERT(strcmp(call_log[1], "LWLock-acquire") == 0);
}


UT_TEST(test_L3_lwlock_released_before_pcm)
{
	MockBufferDesc buf;

	reset_fixture();
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(102);

	fake_LockBuffer(&buf, BUFFER_LOCK_SHARE, false);
	call_log_len = 0;
	fake_LockBuffer(&buf, BUFFER_LOCK_UNLOCK, false);

	UT_ASSERT(call_log_len >= 2);
	UT_ASSERT(strcmp(call_log[0], "LWLock-release") == 0);
	UT_ASSERT(strcmp(call_log[1], "PCM-release") == 0);
}


UT_TEST(test_L4_success_path_sets_buffer_type_and_pcm_state)
{
	MockBufferDesc buf_s;
	MockBufferDesc buf_x;

	reset_fixture();
	memset(&buf_s, 0, sizeof(buf_s));
	memset(&buf_x, 0, sizeof(buf_x));
	buf_s.tag = make_tag(103);
	buf_x.tag = make_tag(104);

	fake_LockBuffer(&buf_s, BUFFER_LOCK_SHARE, false);
	UT_ASSERT_EQ((int)buf_s.buffer_type, (int)BUF_TYPE_SCUR);
	UT_ASSERT_EQ((int)buf_s.pcm_state, (int)PCM_STATE_S);

	fake_LockBuffer(&buf_x, BUFFER_LOCK_EXCLUSIVE, false);
	UT_ASSERT_EQ((int)buf_x.buffer_type, (int)BUF_TYPE_XCUR);
	UT_ASSERT_EQ((int)buf_x.pcm_state, (int)PCM_STATE_X);
}


UT_TEST(test_L5_ereport_at_lwlock_releases_pcm_and_keeps_buffer_type)
{
	MockBufferDesc buf;

	reset_fixture();
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(105);
	buf.buffer_type = (uint8)BUF_TYPE_CURRENT;
	buf.pcm_state = (uint8)PCM_STATE_N;

	fake_ereport_at_lwlock = true;
	UT_ASSERT_EQ(fake_LockBuffer(&buf, BUFFER_LOCK_SHARE, false), 1);

	/* HC66: PCM released on ereport, buffer_type NOT updated. */
	UT_ASSERT_EQ((int)buf.buffer_type, (int)BUF_TYPE_CURRENT);
	UT_ASSERT_EQ((int)buf.pcm_state, (int)PCM_STATE_N);

	/* call_log: PCM-acquire then PCM-release-on-ereport. */
	UT_ASSERT(call_log_len >= 2);
	UT_ASSERT(strcmp(call_log[0], "PCM-acquire") == 0);
	UT_ASSERT(strcmp(call_log[1], "PCM-release-on-ereport") == 0);

	/* Verify PCM state actually rolled back to N (entry not held by node). */
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(buf.tag), (int)PCM_LOCK_MODE_N);
}


UT_TEST(test_L6_local_buffer_skips_hook_entirely)
{
	MockBufferDesc buf;

	reset_fixture();
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(106);

	fake_LockBuffer(&buf, BUFFER_LOCK_SHARE, true /* is_local */);

	/* HC68: no PCM call, no LWLock call (PG-native early return). */
	UT_ASSERT_EQ(call_log_len, 0);
	UT_ASSERT_EQ((int)buf.buffer_type, (int)BUF_TYPE_CURRENT);
}


UT_TEST(test_L7_cluster_enabled_false_skips_hook)
{
	MockBufferDesc buf;

	reset_fixture();
	cluster_enabled = false;
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(107);

	fake_LockBuffer(&buf, BUFFER_LOCK_SHARE, false);

	/* Layer 2 gate: cluster_pcm_is_active() returns false, no PCM call. */
	UT_ASSERT_EQ(call_log_len, 1);
	UT_ASSERT(strcmp(call_log[0], "LWLock-acquire") == 0);
	UT_ASSERT_EQ((int)buf.buffer_type, (int)BUF_TYPE_CURRENT);
}


UT_TEST(test_L8_max_entries_zero_skips_hook)
{
	MockBufferDesc buf;

	reset_fixture();
	cluster_pcm_grd_max_entries = 0;
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(108);

	fake_LockBuffer(&buf, BUFFER_LOCK_SHARE, false);

	/* max_entries=0 gate: cluster_pcm_is_active() false. */
	UT_ASSERT_EQ(call_log_len, 1);
	UT_ASSERT(strcmp(call_log[0], "LWLock-acquire") == 0);
}


UT_TEST(test_L9_unlock_without_prior_pcm_ownership_skips_release)
{
	MockBufferDesc buf;

	reset_fixture();
	memset(&buf, 0, sizeof(buf));
	buf.tag = make_tag(109);
	buf.pcm_state = (uint8)PCM_STATE_N;

	fake_LockBuffer(&buf, BUFFER_LOCK_UNLOCK, false);

	UT_ASSERT_EQ(call_log_len, 1);
	UT_ASSERT(strcmp(call_log[0], "LWLock-release") == 0);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(buf.tag), (int)PCM_LOCK_MODE_N);
}


int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_L1_is_active_triple_gate_truth_table);
	UT_RUN(test_L2_pcm_acquired_before_lwlock);
	UT_RUN(test_L3_lwlock_released_before_pcm);
	UT_RUN(test_L4_success_path_sets_buffer_type_and_pcm_state);
	UT_RUN(test_L5_ereport_at_lwlock_releases_pcm_and_keeps_buffer_type);
	UT_RUN(test_L6_local_buffer_skips_hook_entirely);
	UT_RUN(test_L7_cluster_enabled_false_skips_hook);
	UT_RUN(test_L8_max_entries_zero_skips_hook);
	UT_RUN(test_L9_unlock_without_prior_pcm_ownership_skips_release);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
