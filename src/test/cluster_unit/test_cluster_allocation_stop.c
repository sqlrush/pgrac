/* Actual OID/SQ/HW claim, publish and consume bodies. Shmem/hash, locks,
 * scheduler, OID authority I/O and GES are boundary fixtures. READY here is
 * not proof of checkpoint durability, absent frontends or remote completion. */
#include "postgres.h"
#include "../../backend/cluster/cluster_oid_lease_shmem.c"
#include "../../backend/cluster/cluster_sequence_shmem.c"
#include "../../backend/cluster/cluster_hw_shmem.c"
#include "../../backend/cluster/cluster_hw.c"
#include "../../backend/cluster/cluster_hw_ic.c"
#include "../../backend/cluster/cluster_sequence.c"

/* Retain real OID carve/consume math; replace only its file I/O boundary. */
bool unused_oid_authority_read(Oid *out);
void unused_oid_authority_write(Oid value);
#define cluster_oid_authority_read unused_oid_authority_read
#define cluster_oid_authority_write unused_oid_authority_write
#include "../../backend/cluster/cluster_oid_lease.c"
#undef cluster_oid_authority_read
#undef cluster_oid_authority_write
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster;
BackendType MyBackendType = B_INVALID;
int MaxBackends = 16;
int cluster_ges_request_timeout_ms = 1000;
int cluster_oid_lease_size = 8;
int cluster_node_id = 0;
char *cluster_shared_data_dir;
static int fixture_node_count = 4;
int cluster_conf_node_count(void) { return fixture_node_count; }
PGPROC *MyProc;
PROC_HDR *ProcGlobal;
Latch *MyLatch;
volatile sig_atomic_t InterruptPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static PROC_HDR proc_header;
static PGPROC procs[16];
static void *shared_allocations[3];
static int allocation_count;
static LWLock *held_lock, *busy_lock;
static LWLock *outer_lock;
static bool fixture_throws;
static unsigned authority_reads, authority_writes, lock_releases, wakeups;
static bool fail_authority_write;
static Oid authority_hwm;
static bool stop_new_modifier_allowed = true;
static unsigned stop_new_modifier_calls, hw_wal_calls, hw_flush_calls, hw_send_calls;
static BlockNumber hw_wal_end;
static HwAllocReply hw_sent_reply;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(modifies_data);
	stop_new_modifier_calls++;
	return stop_new_modifier_allowed;
}
uint32 cluster_grd_shard_for_resource(const ClusterResId *resid) { return 0; }
uint32 cluster_grd_shard_master_generation(uint32 shard) { return 0; }
ClusterGrdShardPhase cluster_grd_shard_phase(uint32 shard) { return GRD_SHARD_NORMAL; }
XLogRecPtr
cluster_hw_emit_reserve(RelFileLocator rloc, ForkNumber fork, BlockNumber end, uint32 granted)
{
	Assert(held_lock == NULL);
	hw_wal_calls++;
	hw_wal_end = end;
	return 987;
}
void
XLogFlush(XLogRecPtr lsn)
{
	Assert(lsn == 987 && hw_wal_calls > hw_flush_calls && held_lock == NULL);
	hw_flush_calls++;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 length)
{
	Assert(type == PGRAC_IC_MSG_HW_ALLOC_REPLY && dest == 1);
	Assert(length == sizeof(hw_sent_reply));
	Assert(hw_flush_calls == hw_wal_calls && hw_flush_calls > hw_send_calls);
	Assert(held_lock == NULL);
	hw_send_calls++;
	memcpy(&hw_sent_reply, payload, length);
	return CLUSTER_IC_SEND_DONE;
}

static ClusterNormalStopPollResult
hw_poll(const char **domain, uint64 *key, const char **reason)
{
	return cluster_hw_normal_stop_poll(domain, key, NULL, reason);
}

typedef struct TestHash {
	Size key_size, entry_size;
	long capacity;
	unsigned char *bytes;
	bool *used;
	LWLock *lock;
} TestHash;
static TestHash hashes[3];
static int hash_count;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
void
pg_re_throw(void)
{
	Assert(PG_exception_stack != NULL);
	siglongjmp(*PG_exception_stack, 1);
}
bool
errstart(int level, const char *domain)
{
	return fixture_throws && level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return fixture_throws && level >= ERROR;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errhint(const char *fmt, ...)
{
	return 0;
}
int
errcode(int code)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	if (fixture_throws && PG_exception_stack != NULL)
		pg_re_throw();
	abort();
}
Size
add_size(Size a, Size b)
{
	return a + b;
}
Size
hash_estimate_size(long count, Size size)
{
	return count * size;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	Assert(allocation_count < 3);
	*found = false;
	shared_allocations[allocation_count] = calloc(1, size);
	Assert(shared_allocations[allocation_count] != NULL);
	return shared_allocations[allocation_count++];
}
HTAB *
ShmemInitHash(const char *name, long initial, long maximum, HASHCTL *info, int flags)
{
	TestHash *h;
	Assert(hash_count < 3);
	h = &hashes[hash_count++];
	h->key_size = info->keysize;
	h->entry_size = info->entrysize;
	h->capacity = maximum;
	h->bytes = calloc(maximum, info->entrysize);
	h->used = calloc(maximum, sizeof(bool));
	h->lock = hash_count == 1 ? &sq_state->lwlock : &hw_state->lwlock;
	Assert(h->bytes != NULL && h->used != NULL);
	return (HTAB *)h;
}
void *
hash_search(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	TestHash *h = (TestHash *)table;
	long first_free = -1;
	Assert(held_lock == h->lock);
	if (found != NULL)
		*found = false;
	for (long i = 0; i < h->capacity; i++) {
		void *entry = h->bytes + i * h->entry_size;
		if (!h->used[i]) {
			if (first_free < 0)
				first_free = i;
			continue;
		}
		if (memcmp(key, entry, h->key_size) != 0)
			continue;
		if (found != NULL)
			*found = true;
		if (action == HASH_REMOVE)
			h->used[i] = false;
		return entry;
	}
	if ((action == HASH_ENTER || action == HASH_ENTER_NULL) && first_free >= 0) {
		void *entry = h->bytes + first_free * h->entry_size;
		h->used[first_free] = true;
		memset(entry, 0, h->entry_size);
		memcpy(entry, key, h->key_size);
		return entry;
	}
	return NULL;
}
void
hash_seq_init(HASH_SEQ_STATUS *scan, HTAB *table)
{
	Assert(held_lock == ((TestHash *)table)->lock);
	memset(scan, 0, sizeof(*scan));
	scan->hashp = table;
}
void *
hash_seq_search(HASH_SEQ_STATUS *scan)
{
	TestHash *h = (TestHash *)scan->hashp;
	Assert(held_lock == h->lock);
	while (scan->curBucket < h->capacity) {
		unsigned i = scan->curBucket++;
		if (h->used[i])
			return h->bytes + i * h->entry_size;
	}
	return NULL;
}
void
hash_seq_term(HASH_SEQ_STATUS *scan)
{
	Assert(held_lock == ((TestHash *)scan->hashp)->lock);
}
void
LWLockInitialize(LWLock *lock, int tranche)
{
	memset(lock, 0, sizeof(*lock));
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (held_lock != NULL) {
		Assert(outer_lock == NULL && held_lock == &hw_state->snapshot_write_lock
			   && lock == &hw_state->lwlock);
		outer_lock = held_lock;
	}
	held_lock = lock;
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return lock == busy_lock ? false : LWLockAcquire(lock, mode);
}
void
LWLockRelease(LWLock *lock)
{
	Assert(held_lock == lock);
	held_lock = outer_lock;
	outer_lock = NULL;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == held_lock;
}
void
ConditionVariableInit(ConditionVariable *cv)
{
	memset(cv, 0, sizeof(*cv));
}
void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	abort();
}
void
ConditionVariableSleep(ConditionVariable *cv, uint32 event)
{
	abort();
}
bool
ConditionVariableCancelSleep(void)
{
	abort();
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	Assert(held_lock == NULL);
	wakeups++;
}
void
SetLatch(Latch *latch)
{
	Assert(held_lock == NULL);
	wakeups++;
}
void
ResetLatch(Latch *latch)
{
	abort();
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	abort();
}
void
ProcessInterrupts(void)
{
	abort();
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{
	Assert(held_lock == NULL);
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}
ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}
ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{
	Assert(held_lock == NULL);
	lock_releases++;
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}
bool
cluster_oid_authority_read(Oid *out)
{
	Assert(held_lock == NULL && oid_x_hold.held);
	authority_reads++;
	*out = authority_hwm;
	return true;
}
void
cluster_oid_authority_write(Oid value)
{
	BackendType saved = MyBackendType;
	Assert(held_lock == NULL && oid_x_hold.held);
	/* Interleave the observer while the real claimant is still in I/O. */
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_oid_lease_normal_stop_poll(NULL));
	MyBackendType = saved;
	if (fail_authority_write)
		pg_re_throw();
	authority_writes++;
	authority_hwm = value;
}

static void
reset_fixture(void)
{
	Assert(held_lock == NULL);
	fixture_throws = false;
	fixture_node_count = 4;
	cluster_shared_data_dir = NULL;
	stop_new_modifier_allowed = true;
	stop_new_modifier_calls = hw_wal_calls = hw_flush_calls = hw_send_calls = 0;
	hw_wal_end = 0;
	memset(&hw_sent_reply, 0, sizeof(hw_sent_reply));
	for (int i = 0; i < allocation_count; i++)
		free(shared_allocations[i]);
	for (int i = 0; i < hash_count; i++) {
		free(hashes[i].bytes);
		free(hashes[i].used);
	}
	allocation_count = hash_count = 0;
	memset(hashes, 0, sizeof(hashes));
	memset(&oid_x_hold, 0, sizeof(oid_x_hold));
	IsUnderPostmaster = false;
	cluster_oid_lease_shmem_init();
	cluster_sequence_shmem_init();
	cluster_hw_shmem_init();
	IsUnderPostmaster = true;
	MyBackendType = B_CHECKPOINTER;
	memset(&proc_header, 0, sizeof(proc_header));
	ProcGlobal = &proc_header;
	proc_header.allProcs = procs;
	proc_header.allProcCount = lengthof(procs);
	for (int i = 0; i < lengthof(procs); i++)
		procs[i].pgprocno = i;
	MyProc = &procs[0];
	MyLatch = &MyProc->procLatch;
	busy_lock = NULL;
	authority_reads = authority_writes = lock_releases = wakeups = 0;
	fail_authority_write = false;
	authority_hwm = FirstNormalObjectId;
}

UT_TEST(owner_and_missing_regions)
{
	reset_fixture();
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_oid_lease_normal_stop_poll(NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_sequence_normal_stop_poll(NULL, NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, cluster_oid_lease_normal_stop_poll(NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, cluster_sequence_normal_stop_poll(NULL, NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, hw_poll(NULL, NULL, NULL));
	MyBackendType = B_CHECKPOINTER;
	oid_state = NULL;
	sq_cache_htab = NULL;
	hw_reply_htab = NULL;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, cluster_oid_lease_normal_stop_poll(NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, cluster_sequence_normal_stop_poll(NULL, NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, hw_poll(NULL, NULL, NULL));
}
UT_TEST(oid_original_refill_success_and_unused_grant)
{
	Oid value, next, end;
	reset_fixture();
	MyBackendType = B_BACKEND;
	value = cluster_oid_lease_get_next();
	UT_ASSERT_EQ(FirstNormalObjectId, value);
	UT_ASSERT_EQ(1, authority_reads);
	UT_ASSERT_EQ(1, authority_writes);
	UT_ASSERT_EQ(1, lock_releases);
	UT_ASSERT(!oid_x_hold.held);
	MyBackendType = B_CHECKPOINTER;
	next = oid_state->next;
	end = oid_state->end;
	UT_ASSERT(next != end);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_oid_lease_normal_stop_poll(NULL));
	UT_ASSERT_EQ(next, oid_state->next);
	UT_ASSERT_EQ(end, oid_state->end);
	UT_ASSERT_EQ(1, authority_writes);
}
UT_TEST(oid_original_exception_closes_refill)
{
	volatile bool caught = false;
	reset_fixture();
	fail_authority_write = true;
	MyBackendType = B_BACKEND;
	PG_TRY();
	{
		(void)cluster_oid_lease_get_next();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!oid_x_hold.held);
	UT_ASSERT_EQ(1, lock_releases);
	UT_ASSERT_EQ(0, authority_writes);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_oid_lease_normal_stop_poll(NULL));
}
UT_TEST(sequence_real_claim_wait_publish_abort)
{
	ClusterResId key, observed;
	int64 value;
	reset_fixture();
	cluster_sq_resid_encode(5, 12345, 12345, &key);
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_CLAIMED,
				 cluster_sq_instance_cache_begin_refill(&key, key.field3, 1, &value));
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_WAIT,
				 cluster_sq_instance_cache_begin_refill(&key, key.field3, 1, &value));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_sequence_normal_stop_poll(&observed, NULL));
	UT_ASSERT_EQ(0, memcmp(&key, &observed, sizeof(key)));
	cluster_sq_instance_cache_publish_and_take(&key, key.field3, 1, 101, 108, &value);
	UT_ASSERT_EQ(101, value);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_sequence_normal_stop_poll(NULL, NULL));
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_SERVED,
				 cluster_sq_instance_cache_begin_refill(&key, key.field3, 1, &value));
	UT_ASSERT_EQ(102, value);
	cluster_sq_resid_encode(5, 12346, 12346, &key);
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_CLAIMED,
				 cluster_sq_instance_cache_begin_refill(&key, key.field3, -1, &value));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_sequence_normal_stop_poll(NULL, NULL));
	cluster_sq_instance_cache_abort_refill(&key);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_sequence_normal_stop_poll(NULL, NULL));
}
UT_TEST(sequence_full_table_last_claim_and_invalid_priority)
{
	ClusterResId key, observed;
	int64 value;
	reset_fixture();
	for (int i = 0; i < CLUSTER_SQ_INSTANCE_CACHE_MAX; i++) {
		cluster_sq_resid_encode(5, 10000 + i, 10000 + i, &key);
		UT_ASSERT_EQ(CLUSTER_SQ_REFILL_CLAIMED,
					 cluster_sq_instance_cache_begin_refill(&key, key.field3, 1, &value));
		if (i + 1 < CLUSTER_SQ_INSTANCE_CACHE_MAX)
			cluster_sq_instance_cache_abort_refill(&key);
	}
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_sequence_normal_stop_poll(&observed, NULL));
	UT_ASSERT_EQ(0, memcmp(&key, &observed, sizeof(key)));
	/* Real claim API can expose an inconsistent caller generation. */
	cluster_sq_instance_cache_abort_refill(&key);
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_CLAIMED,
				 cluster_sq_instance_cache_begin_refill(&key, key.field3 + 1, 1, &value));
	cluster_sq_resid_encode(5, 10000, 10000, &observed);
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_CLAIMED,
				 cluster_sq_instance_cache_begin_refill(&observed, observed.field3, 1, &value));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, cluster_sequence_normal_stop_poll(NULL, NULL));
	cluster_sq_instance_cache_abort_refill(&key);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_sequence_normal_stop_poll(NULL, NULL));
	cluster_sq_instance_cache_abort_refill(&observed);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_sequence_normal_stop_poll(NULL, NULL));
}
UT_TEST(hw_reply_is_pending_until_real_consumption)
{
	HwAllocReply reply = { 0 }, out = { 0 };
	const char *domain;
	uint64 request;
	int backend;
	reset_fixture();
	MyProc = &procs[15];
	cluster_hw_reply_slot_arm(941);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING,
				 cluster_hw_normal_stop_poll(&domain, &request, &backend, NULL));
	UT_ASSERT_EQ(941, request);
	UT_ASSERT_EQ(15, backend);
	reply.request_id = 941;
	reply.source_procno = 15;
	reply.status = HW_ALLOC_REPLY_OK;
	reply.first_block = 111;
	reply.granted = 4;
	cluster_hw_reply_slot_deliver(&reply);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, hw_poll(NULL, NULL, NULL));
	UT_ASSERT(cluster_hw_reply_slot_wait(941, 1000, &out));
	UT_ASSERT_EQ(111, out.first_block);
	UT_ASSERT_EQ(4, out.granted);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
	cluster_hw_reply_slot_deliver(&reply);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
}
UT_TEST(hw_original_remaster_status_and_snapshot_owner)
{
	reset_fixture();
	cluster_hw_remaster_set_launched(CLUSTER_MAX_NODES - 1, 5);
	cluster_hw_remaster_set_result(CLUSTER_MAX_NODES - 1, CLUSTER_HW_REMASTER_RUNNING);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, hw_poll(NULL, NULL, NULL));
	cluster_hw_remaster_set_result(CLUSTER_MAX_NODES - 1, CLUSTER_HW_REMASTER_DONE);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
	busy_lock = &hw_state->snapshot_write_lock;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, hw_poll(NULL, NULL, NULL));
	busy_lock = NULL;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
	cluster_hw_remaster_set_result(CLUSTER_MAX_NODES - 1, CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, hw_poll(NULL, NULL, NULL));
}
UT_TEST(hw_invalid_late_entry_overrides_pending)
{
	HwAllocReply reply = { 0 };
	reset_fixture();
	cluster_hw_reply_slot_arm(1);
	MyProc = &procs[15];
	cluster_hw_reply_slot_arm(2);
	reply.request_id = 2;
	reply.source_procno = 15;
	reply.status = 99;
	cluster_hw_reply_slot_deliver(&reply);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_INVALID, hw_poll(NULL, NULL, NULL));
}
UT_TEST(lmon_post_send_observes_original_shared_owners)
{
	ClusterResId key;
	int64 value;
	reset_fixture();
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_oid_lease_normal_stop_poll(NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_sequence_normal_stop_poll(NULL, NULL));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
	cluster_sq_resid_encode(5, 12400, 12400, &key);
	UT_ASSERT_EQ(CLUSTER_SQ_REFILL_CLAIMED,
				 cluster_sq_instance_cache_begin_refill(&key, key.field3, 1, &value));
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, cluster_sequence_normal_stop_poll(NULL, NULL));
	cluster_sq_instance_cache_abort_refill(&key);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, cluster_sequence_normal_stop_poll(NULL, NULL));
	cluster_hw_remaster_set_launched(3, 5);
	cluster_hw_remaster_set_result(3, CLUSTER_HW_REMASTER_RUNNING);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_PENDING, hw_poll(NULL, NULL, NULL));
	cluster_hw_remaster_set_result(3, CLUSTER_HW_REMASTER_DONE);
	UT_ASSERT_EQ(CLUSTER_NORMAL_STOP_READY, hw_poll(NULL, NULL, NULL));
	UT_ASSERT_EQ(authority_writes, 0);
}

UT_TEST(hw_remote_seal_precedes_original_advance_wal_and_reply)
{
	HwAllocRequest req = {0};
	ClusterICEnvelope env = {0};
	HwAllocReply accepted;
	ClusterResId resid;
	bool found;
	ClusterHwEntry *entry;

	reset_fixture();
	/* This test supplies an already running original recovery service. It
	 * does not certify normal startup; the cold suite runs the real producer. */
	cluster_shared_data_dir = "allocation-fixture";
	pg_atomic_write_u32(&hw_state->cold_boot_mode, CLUSTER_HW_BOOT_EXISTING_RECOVERY);
	req.request_id = 81;
	req.source_node = 1;
	req.source_procno = 7;
	req.spcOid = 1663;
	req.dbOid = 5;
	req.relNumber = 16384;
	req.fork = MAIN_FORKNUM;
	req.want = 2;
	req.seed_nblocks = 17003;
	env.source_node_id = 1;
	env.dest_node_id = 0;
	env.payload_length = sizeof(req);
	cluster_hw_resid_encode((RelFileLocator){1663, 5, 16384}, MAIN_FORKNUM, &resid);
	stop_new_modifier_allowed = false;
	cluster_hw_alloc_request_handler(&env, &req);
	UT_ASSERT_EQ(stop_new_modifier_calls, 1);
	UT_ASSERT_EQ(hw_wal_calls + hw_flush_calls + hw_send_calls, 0);
	LWLockAcquire(&hw_state->lwlock, LW_SHARED);
	entry = hash_search(hw_htab, &resid, HASH_FIND, &found);
	UT_ASSERT(!found && entry == NULL);
	LWLockRelease(&hw_state->lwlock);
	stop_new_modifier_allowed = true;
	cluster_hw_alloc_request_handler(&env, &req);
	UT_ASSERT_EQ(hw_send_calls, 1);
	UT_ASSERT_EQ(hw_wal_calls, 1);
	UT_ASSERT_EQ(hw_flush_calls, 1);
	UT_ASSERT_EQ(hw_wal_end, 17005);
	UT_ASSERT_EQ(hw_sent_reply.first_block, 17003);
	UT_ASSERT_EQ(hw_sent_reply.granted, 2);
	accepted = hw_sent_reply;
	stop_new_modifier_allowed = false;
	stop_new_modifier_calls = 0;
	cluster_hw_alloc_request_handler(&env, &req);
	UT_ASSERT_EQ(stop_new_modifier_calls, 1);
	UT_ASSERT_EQ(hw_send_calls, 1);
	UT_ASSERT_EQ(hw_wal_calls, 1);
	UT_ASSERT_EQ(memcmp(&accepted, &hw_sent_reply, sizeof(accepted)), 0);
}

UT_TEST(hw_local_backend_and_existing_reply_keep_original_paths)
{
	HwAllocRequest req = {0};
	HwAllocReply reply, received;
	ClusterICEnvelope env = {0};

	reset_fixture();
	cluster_shared_data_dir = "allocation-fixture";
	pg_atomic_write_u32(&hw_state->cold_boot_mode, CLUSTER_HW_BOOT_EXISTING_RECOVERY);
	req.request_id = 82;
	req.source_procno = 0;
	req.spcOid = 1663;
	req.dbOid = 5;
	req.relNumber = 16385;
	req.want = 1;
	req.seed_nblocks = 17005;
	stop_new_modifier_allowed = false;
	/* Actual local master body, not a forged service actor; frontend drain
	 * remains an independent native barrier. WAL/flush are boundary fixtures. */
	cluster_hw_master_process(&req, &reply);
	UT_ASSERT_EQ(stop_new_modifier_calls, 0);
	UT_ASSERT_EQ(reply.first_block, 17005);
	UT_ASSERT_EQ(hw_wal_end, 17006);
	cluster_hw_reply_slot_arm(req.request_id);
	cluster_hw_alloc_reply_handler(&env, &reply);
	UT_ASSERT_EQ(hw_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(cluster_hw_reply_slot_wait(req.request_id, 1000, &received));
	UT_ASSERT_EQ(memcmp(&reply, &received, sizeof(reply)), 0);
	UT_ASSERT_EQ(hw_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(stop_new_modifier_calls, 0);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(owner_and_missing_regions);
	UT_RUN(oid_original_refill_success_and_unused_grant);
	UT_RUN(oid_original_exception_closes_refill);
	UT_RUN(sequence_real_claim_wait_publish_abort);
	UT_RUN(sequence_full_table_last_claim_and_invalid_priority);
	UT_RUN(hw_reply_is_pending_until_real_consumption);
	UT_RUN(hw_original_remaster_status_and_snapshot_owner);
	UT_RUN(hw_invalid_late_entry_overrides_pending);
	UT_RUN(lmon_post_send_observes_original_shared_owners);
	UT_RUN(hw_remote_seal_precedes_original_advance_wal_and_reply);
	UT_RUN(hw_local_backend_and_existing_reply_keep_original_paths);
	UT_DONE();
	return ut_failed_count != 0;
}
