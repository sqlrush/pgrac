/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_reply_wait.c
 *	  Standalone tests for cooperative reply-wait polling (Spec 8.4A D1).
 *
 * The real reply-wait object runs against a bounded fake dynahash.  Tests pin
 * the consumer-visible contract: pending entries stay installed, delivered
 * verdicts are copied before atomic removal, abandoned/missing entries have
 * distinct outcomes, and every byte of the five-field key participates in
 * correlation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_ges.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

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

UT_DEFINE_GLOBALS();


/* ============================================================
 * Minimal backend runtime and profiling stubs.
 * ============================================================ */

bool IsUnderPostmaster = false;
bool cluster_enabled = true;
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
int cluster_ges_reply_wait_max_entries = 1024;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
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
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}


/* ============================================================
 * One-lock fake dynahash.
 * ============================================================ */

#define FAKE_REPLY_WAIT_CAP 32

typedef struct FakeReplyWaitHash {
	char entries[FAKE_REPLY_WAIT_CAP][sizeof(GesReplyWaitEntry)];
	long count;
} FakeReplyWaitHash;

static FakeReplyWaitHash fake_hash;
static Size fake_keysize;
static Size fake_entrysize;
static long fake_init_size;
static long fake_max_size;
static int fake_lock_depth;
static LWLock *fake_held_lock;
static int fake_lock_acquires;
static int fake_lock_releases;
static int fake_cv_prepare_calls;
static int fake_cv_timed_sleep_calls;
static int fake_cv_cancel_calls;
static ConditionVariable *fake_cv_target;
static long fake_cv_timeout_ms;
static uint32 fake_cv_wait_event;
static bool fake_delivery_during_sleep;
static bool fake_error_during_sleep;
static int fake_cv_broadcast_calls;
static union {
	uint64 force_align;
	char data[4096];
} fake_shared;
static bool fake_shared_found;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	Assert(size <= sizeof(fake_shared.data));
	*foundPtr = fake_shared_found;
	fake_shared_found = true;
	return fake_shared.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP, int hash_flags)
{
	Assert(infoP != NULL);
	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->keysize == sizeof(GesReplyWaitKey));
	Assert(infoP->entrysize == sizeof(GesReplyWaitEntry));
	fake_keysize = infoP->keysize;
	fake_entrysize = infoP->entrysize;
	fake_init_size = init_size;
	fake_max_size = max_size;
	memset(&fake_hash, 0, sizeof(fake_hash));
	return (HTAB *)&fake_hash;
}

void *
hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr)
{
	FakeReplyWaitHash *hash = (FakeReplyWaitHash *)hashp;
	long i;

	Assert(fake_lock_depth == 1);
	Assert(hash == &fake_hash);
	Assert(keyPtr != NULL);

	for (i = 0; i < hash->count; i++) {
		char *entry = hash->entries[i];

		if (memcmp(entry, keyPtr, fake_keysize) != 0)
			continue;
		if (foundPtr != NULL)
			*foundPtr = true;
		if (action == HASH_REMOVE) {
			if (i + 1 < hash->count)
				memcpy(entry, hash->entries[hash->count - 1], fake_entrysize);
			memset(hash->entries[hash->count - 1], 0xA5, fake_entrysize);
			hash->count--;
		}
		return entry;
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if ((action == HASH_ENTER || action == HASH_ENTER_NULL) && hash->count < FAKE_REPLY_WAIT_CAP) {
		char *entry = hash->entries[hash->count++];

		memset(entry, 0, fake_entrysize);
		memcpy(entry, keyPtr, fake_keysize);
		return entry;
	}
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp)
{
	Assert(fake_lock_depth == 1);
	Assert(hashp == (HTAB *)&fake_hash);
	return fake_hash.count;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	Assert(fake_lock_depth == 1);
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeReplyWaitHash *hash = (FakeReplyWaitHash *)status->hashp;

	Assert(fake_lock_depth == 1);
	if (status->curBucket >= (uint32)hash->count)
		return NULL;
	return hash->entries[status->curBucket++];
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	Assert(fake_lock_depth == 0);
	Assert(mode == LW_EXCLUSIVE || mode == LW_SHARED);
	fake_lock_depth = 1;
	fake_held_lock = lock;
	fake_lock_acquires++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	Assert(fake_lock_depth == 1);
	fake_lock_depth = 0;
	fake_held_lock = NULL;
	fake_lock_releases++;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return fake_lock_depth != 0 && lock == fake_held_lock;
}

void
ConditionVariableInit(ConditionVariable *cv)
{
	memset(cv, 0, sizeof(*cv));
}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	Assert(fake_lock_depth == 0);
	fake_cv_broadcast_calls++;
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	Assert(fake_lock_depth == 1);
	fake_cv_prepare_calls++;
	fake_cv_target = cv;
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout_ms, uint32 wait_event)
{
	Assert(fake_lock_depth == 0);
	Assert(cv == fake_cv_target);
	fake_cv_timed_sleep_calls++;
	fake_cv_timeout_ms = timeout_ms;
	fake_cv_wait_event = wait_event;
	if (fake_delivery_during_sleep) {
		GesReplyWaitEntry *entry
			= (GesReplyWaitEntry *)((char *)cv - offsetof(GesReplyWaitEntry, cv));
		GesReplyWaitKey key = entry->key;

		/* Even a past-deadline live waiter cannot be removed by the sweeper. */
		UT_ASSERT_EQ(cluster_ges_reply_wait_sweep_timeout(INT64_MAX), 0);
		UT_ASSERT_EQ(cluster_ges_reply_wait_lookup(&key), entry);
		UT_ASSERT_EQ(
			cluster_ges_reply_wait_deliver(&key, GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE),
			GES_REPLY_DELIVER_WOKE);
		UT_ASSERT_EQ(cluster_ges_reply_wait_lookup(&key), entry);
		UT_ASSERT_EQ(fake_cv_target, cv);
	}
	if (fake_error_during_sleep)
		siglongjmp(*PG_exception_stack, 1);
	return true;
}

bool
ConditionVariableCancelSleep(void)
{
	Assert(fake_lock_depth == 0);
	fake_cv_cancel_calls++;
	fake_cv_target = NULL;
	return true;
}


/* ============================================================
 * Fixtures.
 * ============================================================ */

static GesReplyWaitKey
make_key(uint64 request_id, int32 source_node_id, int32 dest_node_id, uint32 request_opcode,
		 uint64 cluster_epoch)
{
	GesReplyWaitKey key;

	memset(&key, 0, sizeof(key));
	key.request_id = request_id;
	key.source_node_id = source_node_id;
	key.dest_node_id = dest_node_id;
	key.request_opcode = request_opcode;
	key.cluster_epoch = cluster_epoch;
	return key;
}

static void
reset_reply_wait_with_cap(int max_entries)
{
	bool under_postmaster = IsUnderPostmaster;
	IsUnderPostmaster = false; /* actual fresh-region initializer */
	memset(&fake_shared, 0, sizeof(fake_shared));
	fake_shared_found = false;
	fake_keysize = 0;
	fake_entrysize = 0;
	fake_init_size = 0;
	fake_max_size = 0;
	fake_lock_depth = 0;
	fake_lock_acquires = 0;
	fake_lock_releases = 0;
	fake_cv_prepare_calls = 0;
	fake_cv_timed_sleep_calls = 0;
	fake_cv_cancel_calls = 0;
	fake_cv_target = NULL;
	fake_cv_timeout_ms = 0;
	fake_cv_wait_event = 0;
	fake_delivery_during_sleep = false;
	fake_error_during_sleep = false;
	fake_cv_broadcast_calls = 0;
	cluster_ges_reply_wait_max_entries = max_entries;
	cluster_ges_reply_wait_shmem_init();
	IsUnderPostmaster = under_postmaster;
}

static void
reset_reply_wait(void)
{
	reset_reply_wait_with_cap(1024);
}


/* ============================================================
 * Tests.
 * ============================================================ */

UT_TEST(test_poll_pending_keeps_exact_entry)
{
	GesReplyWaitKey key = make_key(11, 1, 3, 7, 101);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };
	GesReplyWaitPollResult result;

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	result = cluster_ges_reply_wait_poll_consume(&key, &verdict);

	UT_ASSERT_EQ(result, GES_REPLY_WAIT_POLL_PENDING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_lookup(&key));
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_poll_delivered_copies_complete_verdict_then_consumes)
{
	GesReplyWaitKey key = make_key(12, 1, 3, 7, 102);
	GesReplyWaitVerdict verdict = { 0, 0 };
	GesReplyWaitPollResult result;

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	UT_ASSERT_EQ(cluster_ges_reply_wait_deliver(&key, 0x12345678U, 0x87654321U),
				 GES_REPLY_DELIVER_WOKE);
	result = cluster_ges_reply_wait_poll_consume(&key, &verdict);

	UT_ASSERT_EQ(result, GES_REPLY_WAIT_POLL_DELIVERED);
	UT_ASSERT_EQ(verdict.reply_opcode, 0x12345678U);
	UT_ASSERT_EQ(verdict.reject_reason, 0x87654321U);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_NULL(cluster_ges_reply_wait_lookup(&key));
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_poll_abandoned_is_explicit_and_preserves_tombstone)
{
	GesReplyWaitKey key = make_key(13, 1, 3, 7, 103);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };
	GesReplyWaitPollResult result;

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	UT_ASSERT(!cluster_ges_reply_wait_mark_abandoned(&key, 10000));
	result = cluster_ges_reply_wait_poll_consume(&key, &verdict);

	UT_ASSERT_EQ(result, GES_REPLY_WAIT_POLL_ABANDONED);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_lookup(&key));
}

UT_TEST(test_poll_missing_is_explicit_without_output_mutation)
{
	GesReplyWaitKey key = make_key(14, 1, 3, 7, 104);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };

	reset_reply_wait();
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&key, &verdict), GES_REPLY_WAIT_POLL_MISSING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
}

UT_TEST(test_poll_matches_all_five_key_fields)
{
	GesReplyWaitKey base = make_key(20, 1, 3, 7, 200);
	GesReplyWaitKey variants[5];
	GesReplyWaitVerdict verdict;
	int i;

	variants[0] = make_key(21, 1, 3, 7, 200);
	variants[1] = make_key(20, 2, 3, 7, 200);
	variants[2] = make_key(20, 1, 4, 7, 200);
	variants[3] = make_key(20, 1, 3, 8, 200);
	variants[4] = make_key(20, 1, 3, 7, 201);

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&base, 9000));
	for (i = 0; i < 5; i++) {
		UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&variants[i], 9000));
		UT_ASSERT_EQ(
			cluster_ges_reply_wait_deliver(&variants[i], (uint32)(100 + i), (uint32)(200 + i)),
			GES_REPLY_DELIVER_WOKE);
	}

	verdict.reply_opcode = 0xAAAAAAAAU;
	verdict.reject_reason = 0xBBBBBBBBU;
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&base, &verdict), GES_REPLY_WAIT_POLL_PENDING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);

	for (i = 0; i < 5; i++) {
		memset(&verdict, 0, sizeof(verdict));
		UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&variants[i], &verdict),
					 GES_REPLY_WAIT_POLL_DELIVERED);
		UT_ASSERT_EQ(verdict.reply_opcode, 100 + i);
		UT_ASSERT_EQ(verdict.reject_reason, 200 + i);
	}
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_lookup(&base));
}

UT_TEST(test_configured_cap_controls_shmem_and_live_admission)
{
	GesReplyWaitKey keys[3] = {
		make_key(31, 0, 1, 7, 300),
		make_key(32, 0, 1, 7, 300),
		make_key(33, 0, 1, 7, 300),
	};
	Size size_at_two;
	Size size_at_five;

	cluster_ges_reply_wait_max_entries = 2;
	size_at_two = cluster_ges_reply_wait_shmem_size();
	cluster_ges_reply_wait_max_entries = 5;
	size_at_five = cluster_ges_reply_wait_shmem_size();
	UT_ASSERT_EQ(size_at_five - size_at_two, (Size)(3 * sizeof(GesReplyWaitEntry)));

	reset_reply_wait_with_cap(2);
	UT_ASSERT_EQ(fake_init_size, 2);
	UT_ASSERT_EQ(fake_max_size, 2);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&keys[0], 9000));
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&keys[1], 9000));
	UT_ASSERT_NULL(cluster_ges_reply_wait_insert(&keys[2], 9000));
}

UT_TEST(test_sleep_exact_waits_without_consuming_pending_entry)
{
	GesReplyWaitKey key = make_key(41, 1, 3, 7, 401);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	UT_ASSERT(cluster_ges_reply_wait_sleep_exact(&key, 1, 0x1234U));
	UT_ASSERT_EQ(fake_cv_prepare_calls, 1);
	UT_ASSERT_EQ(fake_cv_timed_sleep_calls, 1);
	UT_ASSERT_EQ(fake_cv_cancel_calls, 1);
	UT_ASSERT_EQ(fake_cv_timeout_ms, 1);
	UT_ASSERT_EQ(fake_cv_wait_event, 0x1234U);
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&key, &verdict), GES_REPLY_WAIT_POLL_PENDING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_sleep_exact_refuses_missing_and_abandoned_keys)
{
	GesReplyWaitKey key = make_key(42, 1, 3, 7, 402);
	GesReplyWaitKey other = make_key(43, 1, 3, 7, 402);

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	UT_ASSERT(!cluster_ges_reply_wait_sleep_exact(&other, 1, 0x1234U));
	UT_ASSERT(!cluster_ges_reply_wait_mark_abandoned(&key, 10000));
	UT_ASSERT(!cluster_ges_reply_wait_sleep_exact(&key, 1, 0x1234U));
	UT_ASSERT_EQ(fake_cv_prepare_calls, 0);
	UT_ASSERT_EQ(fake_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(fake_cv_cancel_calls, 0);
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_registered_reply_wait_delivers_without_deleting_live_entry)
{
	GesReplyWaitKey key = make_key(90, 0, 1, GES_REQ_OPCODE_REQUEST, 3);
	GesReplyWaitVerdict verdict;

	reset_reply_wait();
	UT_ASSERT(cluster_ges_reply_wait_insert(&key, 1) != NULL);
	fake_delivery_during_sleep = true;
	UT_ASSERT(cluster_ges_reply_wait_sleep_exact(&key, 1, 0x1234U));
	UT_ASSERT_EQ(fake_cv_broadcast_calls, 1);
	UT_ASSERT_EQ(fake_cv_prepare_calls, 1);
	UT_ASSERT_EQ(fake_cv_cancel_calls, 1);
	UT_ASSERT(fake_cv_target == NULL);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), UINT64_C(1));
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&key, &verdict),
				 GES_REPLY_WAIT_POLL_DELIVERED);
	UT_ASSERT_EQ(verdict.reply_opcode, GES_REPLY_OPCODE_GRANT);
}

UT_TEST(test_reply_ready_before_enrollment_skips_sleep_but_requires_poll)
{
	GesReplyWaitKey key = make_key(91, 0, 1, GES_REQ_OPCODE_RELEASE, 3);
	GesReplyWaitVerdict verdict;

	reset_reply_wait();
	UT_ASSERT(cluster_ges_reply_wait_insert(&key, 10000) != NULL);
	UT_ASSERT_EQ(
		cluster_ges_reply_wait_deliver(&key, GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE),
		GES_REPLY_DELIVER_WOKE);
	UT_ASSERT(cluster_ges_reply_wait_sleep_exact(&key, 1, 0x1234U));
	UT_ASSERT_EQ(fake_cv_prepare_calls, 1);
	UT_ASSERT_EQ(fake_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(fake_cv_cancel_calls, 1);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), UINT64_C(1));
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&key, &verdict),
				 GES_REPLY_WAIT_POLL_DELIVERED);
}

UT_TEST(test_reply_wait_error_cancels_registration_before_owner_cleanup)
{
	GesReplyWaitKey key = make_key(92, 0, 1, GES_REQ_OPCODE_REQUEST, 3);
	volatile bool caught = false;

	reset_reply_wait();
	UT_ASSERT(cluster_ges_reply_wait_insert(&key, 10000) != NULL);
	fake_error_during_sleep = true;
	PG_TRY();
	{
		(void)cluster_ges_reply_wait_sleep_exact(&key, 1, 0x1234U);
	}
	PG_CATCH();
	{
		caught = true;
		UT_ASSERT(fake_cv_target == NULL);
		UT_ASSERT_EQ(fake_cv_cancel_calls, 1);
		UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), UINT64_C(1));
		UT_ASSERT(!cluster_ges_reply_wait_mark_abandoned(&key, 10001));
		UT_ASSERT_EQ(cluster_ges_reply_wait_sweep_timeout(10001), 1);
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_normal_stop_missing_then_actual_empty_table)
{
	const char *reason;
	GesReplyWaitKey key;
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(&key, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "GES_REPLY_UNINITIALIZED");
	reset_reply_wait();
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(&key, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(key.request_id, 0);
	IsUnderPostmaster = false;
}
UT_TEST(test_normal_stop_delivered_is_pending_until_real_consume)
{
	GesReplyWaitKey key = make_key(701, 1, 3, GES_REQ_OPCODE_REQUEST, 21), observed;
	GesReplyWaitVerdict verdict;
	const char *reason;
	FakeReplyWaitHash before;
	IsUnderPostmaster = true;
	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	before = fake_hash;
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(&observed, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(memcmp(&key, &observed, sizeof(key)), 0);
	UT_ASSERT_EQ(memcmp(&before, &fake_hash, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_ges_reply_wait_deliver(&key, GES_REPLY_OPCODE_GRANT, 0),
				 GES_REPLY_DELIVER_WOKE);
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&key, &verdict),
				 GES_REPLY_WAIT_POLL_DELIVERED);
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(&observed, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = false;
}
UT_TEST(test_normal_stop_abandoned_is_not_owner_cleanup)
{
	GesReplyWaitKey key = make_key(702, 1, 3, GES_REQ_OPCODE_REQUEST, 21);
	const char *reason;
	IsUnderPostmaster = true;
	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 1));
	UT_ASSERT_EQ(cluster_ges_reply_wait_sweep_timeout(50000), 0); /* live, even past deadline */
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(NULL, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!cluster_ges_reply_wait_mark_abandoned(&key, 9000));
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(NULL, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_ges_reply_wait_sweep_timeout(8999), 0);
	UT_ASSERT_EQ(cluster_ges_reply_wait_sweep_timeout(9000), 1);
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(NULL, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 10000));
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(NULL, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	cluster_ges_reply_wait_delete(&key);
	IsUnderPostmaster = false;
}
UT_TEST(test_normal_stop_late_bad_key_and_held_original_lock)
{
	GesReplyWaitKey valid = make_key(703, 1, 3, GES_REQ_OPCODE_REQUEST, 21);
	GesReplyWaitKey bad = make_key(0, 1, 3, GES_REQ_OPCODE_REQUEST, 21), observed;
	const char *reason;
	IsUnderPostmaster = true;
	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&valid, 9000));
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&bad, 9000)); /* malformed producer boundary */
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(&observed, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "GES_REPLY_KEY_INVALID");
	UT_ASSERT_EQ(memcmp(&observed, &bad, sizeof(bad)), 0);
	LWLockAcquire((LWLock *)fake_shared.data, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_ges_reply_wait_normal_stop_poll(NULL, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "GES_REPLY_LOCK_HELD");
	LWLockRelease((LWLock *)fake_shared.data);
	IsUnderPostmaster = false;
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_normal_stop_missing_then_actual_empty_table);
	UT_RUN(test_poll_pending_keeps_exact_entry);
	UT_RUN(test_poll_delivered_copies_complete_verdict_then_consumes);
	UT_RUN(test_poll_abandoned_is_explicit_and_preserves_tombstone);
	UT_RUN(test_poll_missing_is_explicit_without_output_mutation);
	UT_RUN(test_poll_matches_all_five_key_fields);
	UT_RUN(test_configured_cap_controls_shmem_and_live_admission);
	UT_RUN(test_sleep_exact_waits_without_consuming_pending_entry);
	UT_RUN(test_sleep_exact_refuses_missing_and_abandoned_keys);
	UT_RUN(test_registered_reply_wait_delivers_without_deleting_live_entry);
	UT_RUN(test_reply_ready_before_enrollment_skips_sleep_but_requires_poll);
	UT_RUN(test_reply_wait_error_cancels_registration_before_owner_cleanup);
	UT_RUN(test_normal_stop_delivered_is_pending_until_real_consume);
	UT_RUN(test_normal_stop_abandoned_is_not_owner_cleanup);
	UT_RUN(test_normal_stop_late_bad_key_and_held_original_lock);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
