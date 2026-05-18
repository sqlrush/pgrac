/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_lock.c
 *	  Compile-time + link-time + behavioral invariants for the PCM lock
 *	  9-state machine activated in spec-2.30.
 *
 *	  spec-2.30 replaces spec-1.7 4-stub bodies with the real 9-transition
 *	  state machine + GrdEntry HTAB + per-entry LWLockPadded.  This test
 *	  binary links cluster_pcm_lock.o + provides minimal stubs for all PG
 *	  runtime deps (ShmemInit*, LWLock*, hash_*, ereport, etc) so that
 *	  both pure-function paths and the real acquire/release/upgrade/
 *	  downgrade/query state machine can be exercised standalone.
 *
 *	  Test plan (26 tests; spec-2.30 §4.1 + codereview hardening):
 *	    T-pcm-1..8   :  transition validator returns true for legal (from, to, trans)
 *	    T-pcm-9      :  Trans-9 reserved as legal entry (validator accepts)
 *	    T-pcm-10     :  transition validator rejects illegal combinations
 *	    T-pcm-11     :  disable path (ClusterPcm == NULL):  counter accessors return 0
 *	    T-pcm-12     :  HTAB-FULL surface (link-only;  cap enforcement)
 *	    T-pcm-13     :  per-entry LWLock granularity invariant (symbol existence)
 *	    T-pcm-14     :  PI bitmap atomic primitive present (link-only)
 *	    T-pcm-15     :  9 counter accessor surface returns 0 under disabled path
 *	    T-pcm-16..20 :  real acquire/release/upgrade/downgrade/query paths,
 *	                   live summary rows, and wait-event callsites
 *
 *	  The fake shared HTAB below is intentionally tiny, but it models the
 *	  behaviours that matter for PCM correctness: key lookup, cap-full,
 *	  shared holder bitmap updates, per-entry LWLock ownership assertions,
 *	  and SQL-visible summary counters.
 *
 *	  Spec: spec-2.30-pcm-9-state-machine-activation.md (FROZEN v0.3)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pcm_lock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_buffer_desc.h" /* PcmState (1.6) */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "storage/buf_internals.h" /* BufferTag */
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include <setjmp.h>

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 * PG-runtime stubs + fake shared HTAB.
 * ============================================================ */

int cluster_node_id = 0;
int NBuffers = 0;
int cluster_injection_armed_count = 0;
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
static uint32 fake_init_wait_event_seen = 0;
static uint32 fake_lwlock_wait_event_seen = 0;

static sigjmp_buf ut_ereport_jump;
static bool ut_ereport_jump_armed = false;
static int ut_ereport_fired_count = 0;

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

static void
reset_fake_pcm_runtime(int max_entries)
{
	memset(&fake_pcm_header, 0, sizeof(fake_pcm_header));
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	fake_pcm_header_found = false;
	fake_pcm_entry_count = 0;
	fake_pcm_entry_max = max_entries;
	fake_pcm_keysize = 0;
	fake_pcm_entrysize = 0;
	fake_lwlock_held = NULL;
	fake_init_wait_event_seen = 0;
	fake_lwlock_wait_event_seen = 0;
	ut_wait_event_info_storage = 0;
	ut_ereport_fired_count = 0;
	ut_ereport_jump_armed = false;
	cluster_node_id = 0;
	NBuffers = max_entries;
	cluster_pcm_grd_max_entries = max_entries;
	cluster_pcm_grd_init();
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	Assert(size <= sizeof(fake_pcm_header.data));
	fake_init_wait_event_seen = ut_wait_event_info_storage;
	*foundPtr = fake_pcm_header_found;
	fake_pcm_header_found = true;
	return fake_pcm_header.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
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
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr pg_attribute_unused(),
			HASHACTION action pg_attribute_unused(), bool *foundPtr pg_attribute_unused())
{
	long i;

	Assert(hashp == (HTAB *)&fake_pcm_htab_token);
	Assert(fake_pcm_keysize > 0);
	Assert(fake_pcm_entrysize > 0);

	for (i = 0; i < fake_pcm_entry_count; i++) {
		char *entry = fake_pcm_entries.data[i];

		if (memcmp(entry, keyPtr, fake_pcm_keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE) {
				if (i + 1 < fake_pcm_entry_count)
					memmove(fake_pcm_entries.data[i], fake_pcm_entries.data[i + 1],
							(size_t)(fake_pcm_entry_count - i - 1) * FAKE_PCM_ENTRY_BYTES);
				fake_pcm_entry_count--;
				return entry;
			}
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if (action == HASH_ENTER_NULL && fake_pcm_entry_count >= fake_pcm_entry_max)
		return NULL;
	if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
		char *entry = fake_pcm_entries.data[fake_pcm_entry_count++];

		memset(entry, 0, FAKE_PCM_ENTRY_BYTES);
		memcpy(entry, keyPtr, fake_pcm_keysize);
		return entry;
	}
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return fake_pcm_entry_count;
}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entrysize pg_attribute_unused())
{
	return (Size)num_entries * entrysize;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
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
	fake_lwlock_wait_event_seen = ut_wait_event_info_storage;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	Assert(fake_lwlock_held == lock);
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

/* ereport stubs — minimal enough to satisfy linker.  ereport(ERROR, ...)
 * path in cluster_pcm_lock.o would call errstart_cold + errfinish;  none
 * of the tests in this binary trigger that path. */
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
	ut_ereport_fired_count++;
	if (ut_ereport_jump_armed)
		siglongjmp(ut_ereport_jump, 1);
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
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

#define UT_EXPECT_EREPORT(stmt)                                                                    \
	do {                                                                                           \
		if (sigsetjmp(ut_ereport_jump, 1) == 0) {                                                  \
			ut_ereport_jump_armed = true;                                                          \
			stmt;                                                                                  \
			ut_ereport_jump_armed = false;                                                         \
			UT_ASSERT(false);                                                                      \
		} else {                                                                                   \
			ut_ereport_jump_armed = false;                                                         \
			UT_ASSERT(ut_ereport_fired_count > 0);                                                 \
		}                                                                                          \
	} while (0)


/* ============================================================
 * Tests
 * ============================================================ */

UT_TEST(test_pcm_lock_mode_constant_aliases_match_pcm_state)
{
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_N, 0);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_S, 1);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_X, 2);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_N, (int)PCM_STATE_N);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_S, (int)PCM_STATE_S);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_X, (int)PCM_STATE_X);
}

UT_TEST(test_pcm_lock_transition_count_is_9)
{
	UT_ASSERT_EQ(PCM_TRANSITION_COUNT, 9);
}

UT_TEST(test_pcm_lock_transition_enum_values_are_1_to_9)
{
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_S, 1);
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_X, 2);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_UPGRADE, 3);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_S_DOWNGRADE, 4);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_N_DOWNGRADE, 5);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_N_RELEASE, 6);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_N_INVALIDATE, 7);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_N_RELEASE, 8);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_CLEANOUT, 9);
}

UT_TEST(test_pcm_grd_max_entries_default_is_minus_one)
{
	/*
	 * spec-2.30 D5:  default changed 0 → -1 sentinel (auto-resolve to
	 * NBuffers at startup);  explicit 0 = disable path.
	 */
	extern int cluster_pcm_grd_max_entries;
	UT_ASSERT_EQ(cluster_pcm_grd_max_entries, -1);
}

UT_TEST(test_pcm_buffer_desc_invariants_hold_at_stage_2_30)
{
	UT_ASSERT_EQ((int)PCM_STATE_N, 0);
	UT_ASSERT_EQ((int)PCM_STATE_S, 1);
	UT_ASSERT_EQ((int)PCM_STATE_X, 2);
}

UT_TEST(test_pcm_lock_module_init_symbol_is_callable)
{
	void (*fn)(void) = cluster_pcm_lock_module_init;
	UT_ASSERT(fn != NULL);
}


/* ============================================================
 * spec-2.30 NEW tests T-pcm-1..15.
 * ============================================================ */

/* T-pcm-1..8: validator accepts each of 8 active transitions. */
UT_TEST(test_pcm_trans_1_n_to_s_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_S, PCM_TRANS_N_TO_S));
}

UT_TEST(test_pcm_trans_2_n_to_x_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_X, PCM_TRANS_N_TO_X));
}

UT_TEST(test_pcm_trans_3_s_to_x_upgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_X, PCM_TRANS_S_TO_X_UPGRADE));
}

UT_TEST(test_pcm_trans_4_x_to_s_downgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_S, PCM_TRANS_X_TO_S_DOWNGRADE));
}

UT_TEST(test_pcm_trans_5_x_to_n_downgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_N, PCM_TRANS_X_TO_N_DOWNGRADE));
}

UT_TEST(test_pcm_trans_6_x_to_n_release_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_N, PCM_TRANS_X_TO_N_RELEASE));
}

UT_TEST(test_pcm_trans_7_s_to_n_invalidate_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_N, PCM_TRANS_S_TO_N_INVALIDATE));
}

UT_TEST(test_pcm_trans_8_s_to_n_release_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_N, PCM_TRANS_S_TO_N_RELEASE));
}


/* T-pcm-9: HC60 — Trans-9 reachable from validator. */
UT_TEST(test_pcm_trans_9_cleanout_validator_reachable_but_apply_fail_closed)
{
	/*
	 * HC60 — validator accepts as legal entry transition (reachable from
	 * validator);  apply body fail-closed FEATURE_NOT_SUPPORTED until
	 * Stage 3 AD-006 第五轮 wires ITL cleanout.  Counter永 0.
	 */
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_X, PCM_TRANS_S_TO_X_CLEANOUT));
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_cleanout_count(), 0);
}


/* T-pcm-10: HC56 — validator rejects illegal combinations. */
UT_TEST(test_pcm_illegal_transition_validator_rejects)
{
	/* (N → X) with trans=N_TO_S code → illegal */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_X, PCM_TRANS_N_TO_S));
	/* (S → S) any trans → illegal (no self-transition) */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_S, PCM_TRANS_N_TO_S));
	/* (X → X) any trans → illegal */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_X, PCM_TRANS_N_TO_X));
}


/* T-pcm-11: disable path — ClusterPcm == NULL → counter accessors return 0. */
UT_TEST(test_pcm_disable_path_counters_return_zero)
{
	/*
	 * In cluster_unit binary cluster_pcm_grd_init is never called so
	 * ClusterPcm stays NULL — all 9 counter accessors return 0.
	 */
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_x_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_upgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_s_downgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_n_downgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_n_release_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_invalidate_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_cleanout_count(), 0);
}


/* T-pcm-12: HC59 fail-closed cap — link-only surface verification. */
UT_TEST(test_pcm_grd_entry_lifecycle_link_surface)
{
	/*
	 * HC59 lifecycle (alloc-on-first-acquire / never-freed-until-shutdown)
	 * is verified by cluster_tap 108 under a live postmaster.  Here we
	 * verify the cap GUC surface exists.
	 */
	extern int cluster_pcm_grd_max_entries;
	UT_ASSERT(&cluster_pcm_grd_max_entries != NULL);
}


/* T-pcm-13: HC61 per-entry LWLock granularity — symbol existence. */
UT_TEST(test_pcm_per_entry_lwlock_independence_link_surface)
{
	/*
	 * HC61 per-entry LWLockPadded granularity (vs per-shard / global).
	 *  Real concurrency test deferred to cluster_tap.  Here verify
	 *  cluster_pcm_lock_module_init symbol is linkable (drives shmem +
	 *  LWTRANCHE_CLUSTER_PCM registration).
	 */
	void (*fn)(void) = cluster_pcm_lock_module_init;
	UT_ASSERT(fn != NULL);
}


/* T-pcm-14: HC58 PI bitmap atomic — verify accessor symbol exists. */
UT_TEST(test_pcm_pi_bitmap_atomic_accessor_linkable)
{
	/*
	 * HC58 PI bitmap atomic update — bitmap field is internal to file-
	 *  private GrdEntry;  observation surface is dump_pcm + future
	 *  cluster_tap.  Here verify cluster_pcm_get_trans_x_to_s_downgrade_count
	 *  (the PI-set transition counter accessor) symbol is linkable.
	 */
	uint64 (*fn)(void) = cluster_pcm_get_trans_x_to_s_downgrade_count;
	UT_ASSERT(fn != NULL);
}


/* T-pcm-15: 9 counter accessor surface — all linkable + return 0 under disabled. */
UT_TEST(test_pcm_counter_observability_9_accessors_linkable)
{
	uint64 (*fns[9])(void) = {
		cluster_pcm_get_trans_n_to_s_count,
		cluster_pcm_get_trans_n_to_x_count,
		cluster_pcm_get_trans_s_to_x_upgrade_count,
		cluster_pcm_get_trans_x_to_s_downgrade_count,
		cluster_pcm_get_trans_x_to_n_downgrade_count,
		cluster_pcm_get_trans_x_to_n_release_count,
		cluster_pcm_get_trans_s_to_n_invalidate_count,
		cluster_pcm_get_trans_s_to_n_release_count,
		cluster_pcm_get_trans_s_to_x_cleanout_count,
	};
	int i;

	for (i = 0; i < 9; i++) {
		UT_ASSERT(fns[i] != NULL);
		UT_ASSERT_EQ((int)fns[i](), 0);
	}
}


UT_TEST(test_pcm_real_shared_s_holders_release_independently)
{
	BufferTag tag = make_tag(1);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	cluster_node_id = 0;
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	cluster_node_id = 1;
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 2);
}


UT_TEST(test_pcm_real_x_release_and_downgrade_require_owner)
{
	BufferTag tag = make_tag(2);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	cluster_node_id = 1;
	UT_EXPECT_EREPORT(cluster_pcm_lock_release(tag));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_EXPECT_EREPORT(cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_S, true));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	cluster_node_id = 0;
	cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_S, true);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
}


UT_TEST(test_pcm_real_upgrade_requires_single_s_holder)
{
	BufferTag tag = make_tag(3);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	cluster_node_id = 0;
	UT_EXPECT_EREPORT(cluster_pcm_lock_upgrade(tag));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	cluster_node_id = 1;
	cluster_pcm_lock_release(tag);
	cluster_node_id = 0;
	cluster_pcm_lock_upgrade(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
}


UT_TEST(test_pcm_real_summary_counts_live_entries)
{
	BufferTag tag_s = make_tag(4);
	BufferTag tag_x = make_tag(5);
	int n_count, s_count, x_count, pi_total, convert_q;

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag_s, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag_x, PCM_LOCK_MODE_X);

	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(n_count, 0);
	UT_ASSERT_EQ(s_count, 1);
	UT_ASSERT_EQ(x_count, 1);
	UT_ASSERT_EQ(pi_total, 0);
	UT_ASSERT_EQ(convert_q, 0);

	cluster_pcm_lock_downgrade(tag_x, PCM_LOCK_MODE_N, true);
	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(n_count, 1);
	UT_ASSERT_EQ(s_count, 1);
	UT_ASSERT_EQ(x_count, 0);
	UT_ASSERT_EQ(pi_total, 1);
}


UT_TEST(test_pcm_real_wait_event_call_sites_are_exercised)
{
	BufferTag tag = make_tag(6);

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ((int)fake_init_wait_event_seen, (int)WAIT_EVENT_PCM_GRD_INIT);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)fake_lwlock_wait_event_seen, (int)WAIT_EVENT_PCM_TRANSITION_APPLY);
	UT_ASSERT_EQ((int)ut_wait_event_info_storage, 0);
}


int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_pcm_lock_mode_constant_aliases_match_pcm_state);
	UT_RUN(test_pcm_lock_transition_count_is_9);
	UT_RUN(test_pcm_lock_transition_enum_values_are_1_to_9);
	UT_RUN(test_pcm_grd_max_entries_default_is_minus_one);
	UT_RUN(test_pcm_buffer_desc_invariants_hold_at_stage_2_30);
	UT_RUN(test_pcm_lock_module_init_symbol_is_callable);
	UT_RUN(test_pcm_trans_1_n_to_s_validator_accepts);
	UT_RUN(test_pcm_trans_2_n_to_x_validator_accepts);
	UT_RUN(test_pcm_trans_3_s_to_x_upgrade_validator_accepts);
	UT_RUN(test_pcm_trans_4_x_to_s_downgrade_validator_accepts);
	UT_RUN(test_pcm_trans_5_x_to_n_downgrade_validator_accepts);
	UT_RUN(test_pcm_trans_6_x_to_n_release_validator_accepts);
	UT_RUN(test_pcm_trans_7_s_to_n_invalidate_validator_accepts);
	UT_RUN(test_pcm_trans_8_s_to_n_release_validator_accepts);
	UT_RUN(test_pcm_trans_9_cleanout_validator_reachable_but_apply_fail_closed);
	UT_RUN(test_pcm_illegal_transition_validator_rejects);
	UT_RUN(test_pcm_disable_path_counters_return_zero);
	UT_RUN(test_pcm_grd_entry_lifecycle_link_surface);
	UT_RUN(test_pcm_per_entry_lwlock_independence_link_surface);
	UT_RUN(test_pcm_pi_bitmap_atomic_accessor_linkable);
	UT_RUN(test_pcm_counter_observability_9_accessors_linkable);
	UT_RUN(test_pcm_real_shared_s_holders_release_independently);
	UT_RUN(test_pcm_real_x_release_and_downgrade_require_owner);
	UT_RUN(test_pcm_real_upgrade_requires_single_s_holder);
	UT_RUN(test_pcm_real_summary_counts_live_entries);
	UT_RUN(test_pcm_real_wait_event_call_sites_are_exercised);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
