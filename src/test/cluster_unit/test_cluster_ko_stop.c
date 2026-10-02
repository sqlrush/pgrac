/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual KO SPSC admission/drain, with explicit storage and ACK-queue boundary
 * fixtures. A queue-empty observation alone does not certify flush completion. */
#include "postgres.h"
#include "cluster/cluster_hw_lease.h"
#include "cluster/storage/cluster_smgr.h"
#include "../../backend/cluster/cluster_ko_lock.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
AuxProcType MyAuxProcType = SinvalBcastProcess;
int cluster_node_id = 0;
bool cluster_enabled = true;
bool cluster_object_reuse_flush_enabled = false;
bool cluster_shared_config = false;
int cluster_space_affinity = CLUSTER_SPACE_AFFINITY_STATIC;
int cluster_injection_armed_count = 0;
static ClusterKoShared storage;
static ClusterHwLeaseShared leases;
static ClusterNodeInfo peer;
static SMgrRelationData relation;
static bool removal_ready = true;
static bool flushing;
static int flush_count, drop_count, ack_count, wake_count;
static KoFlushAckHeader last_ack;
static uint64 current_epoch;
static uint32 existing_forks, synced_forks;
static int sync_count, sync_fail_fork, epoch_change_sync_fork;
static bool epoch_change_flush, epoch_change_drop;
static bool shared_relation;
static uint64 recorded_batch;
static int record_count;
static bool barrier_active, barrier_complete, epoch_change_complete;
static int barrier_requests, barrier_waits, barrier_removes;
static KoFlushHeader last_request;
static bool gate_admit = true;
static unsigned gate_calls;
static unsigned liveness_calls;
int cluster_ges_request_timeout_ms = 100;
int cluster_sinval_ack_timeout_ms = 100;
Latch *MyLatch;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

bool RecoveryInProgress(void) { return false; }
TimestampTz GetCurrentTimestamp(void) { return 1; }
void
ResetLatch(Latch *latch)
{
	Assert(barrier_active);
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 wait_event)
{
	Assert(barrier_active && timeout > 0);
	barrier_waits++;
	current_epoch++;
	barrier_complete = true;
	return WL_LATCH_SET;
}
void
pg_re_throw(void)
{
	Assert(PG_exception_stack != NULL);
	siglongjmp(*PG_exception_stack, 1);
}
int errhint(const char *format, ...) { return 0; }
ClusterExtendEngage cluster_extend_liveness_engage(bool wait_for_lms)
{
	Assert(!wait_for_lms);
	liveness_calls++;
	return CLUSTER_EXTEND_ENGAGE_NATIVE;
}
void cluster_ko_resid_encode(RelFileLocator locator, ClusterResId *resid) { abort(); }
ClusterLockAcquireResult cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{ abort(); }
ClusterLockAcquireResult cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{ abort(); }
ClusterLockAcquireResult cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{ abort(); }
uint32 cluster_sinval_compute_alive_peer_mask(void) { abort(); }
uint64
cluster_sinval_ack_wait_alloc_batch_id(void)
{
	return 912;
}
bool
cluster_sinval_ack_wait_begin(uint64 batch, uint32 mask, TimestampTz deadline)
{
	Assert(batch == 912 && mask == (1u << 1) && !barrier_active);
	barrier_active = true;
	return true;
}
bool
cluster_sinval_ack_wait_is_complete(uint64 batch)
{
	Assert(barrier_active && batch == 912);
	if (epoch_change_complete)
		current_epoch++;
	return barrier_complete;
}
void
cluster_sinval_ack_wait_remove(uint64 batch)
{
	Assert(barrier_active && batch == 912);
	barrier_removes++;
	barrier_active = false;
}
void
cluster_sinval_ack_wait_record(uint64 batch, int32 acker)
{
	Assert(acker == 1);
	recorded_batch = batch;
	record_count++;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	return true;
}
void
LWLockRelease(LWLock *lock)
{}
void LWLockInitialize(LWLock *lock, int tranche) { abort(); }
void cluster_shmem_register_region(const ClusterShmemRegion *region) { abort(); }

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(modifies_data);
	gate_calls++;
	return gate_admit;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	Assert(size == MAXALIGN(sizeof(storage)));
	*found = false;
	return &storage;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	return node == 1 ? &peer : NULL;
}
uint64
cluster_epoch_get_current(void)
{
	return current_epoch;
}
void
cluster_sinval_set_proc_latch(void)
{
	wake_count++;
}
void
cluster_lmon_wakeup(void)
{
	wake_count++;
}
void
cluster_injection_run(const char *name)
{
	abort();
}
bool
cluster_injection_should_skip(const char *name)
{
	return false;
}
bool
cluster_ctrc_relation_removal_ready_shared(uint32 spc, uint32 db, uint32 rel)
{
	Assert(spc == 1663 && rel == 99);
	return removal_ready;
}
SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	Assert(locator.spcOid == 1663 && locator.relNumber == 99 && backend == InvalidBackendId);
	return &relation;
}
void
FlushRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
	const char *reason;
	uint32 slot;
	Assert(nrels == 1 && *smgrs == &relation);
	flushing = true;
	/* The last item has already left the ring. Its actual actor must stay
	 * active across this call; the read-only ring poll cannot sign that. */
	if (pg_atomic_read_u32(&storage.inbound_head) == pg_atomic_read_u32(&storage.inbound_tail))
		UT_ASSERT_EQ(cluster_ko_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	flush_count++;
	flushing = false;
	if (epoch_change_flush)
		current_epoch++;
}
void
DropRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
	Assert(!flushing && flush_count == drop_count + 1);
	if (cluster_shared_config && shared_relation)
		UT_ASSERT_EQ(synced_forks, existing_forks);
	drop_count++;
	if (epoch_change_drop)
		current_epoch++;
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	Assert(locator.spcOid == 1663 && locator.relNumber == 99 && backend == InvalidBackendId);
	return shared_relation ? 1 : 0;
}
bool
smgrexists(SMgrRelation smgr, ForkNumber fork)
{
	Assert(smgr == &relation && fork >= MAIN_FORKNUM && fork <= MAX_FORKNUM);
	return (existing_forks & (1u << fork)) != 0;
}
void
smgrimmedsync(SMgrRelation smgr, ForkNumber fork)
{
	Assert(smgr == &relation && !flushing && flush_count == drop_count + 1);
	Assert((existing_forks & (1u << fork)) != 0);
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(drop_count, 0);
	sync_count++;
	if (fork == sync_fail_fork)
		pg_re_throw();
	synced_forks |= 1u << fork;
	if (fork == epoch_change_sync_fork)
		current_epoch++;
}
bool
cluster_grd_outbound_enqueue_backend_msg(uint8 type, uint32 dest, const void *payload, uint16 len)
{
	if (type == PGRAC_IC_MSG_KO_FLUSH) {
		Assert(barrier_active && len == sizeof(last_request));
		UT_ASSERT_EQ(dest, 1);
		memcpy(&last_request, payload, len);
		barrier_requests++;
		return true;
	}
	Assert(type == PGRAC_IC_MSG_KO_FLUSH_ACK && dest == 1 && len == sizeof(last_ack));
	Assert(flush_count == drop_count && drop_count == ack_count + 1);
	/* Even if the file later regrows beyond block 4, the pre-RESET range
	 * cannot be consumed after this peer has acknowledged the barrier. */
	UT_ASSERT_EQ(cluster_hw_lease_next_block((RelFileLocator){1663, 0, 99}, MAIN_FORKNUM),
		InvalidBlockNumber);
	memcpy(&last_ack, payload, len);
	ack_count++;
	return true;
}
static ClusterNormalStopPollResult
poll_queue(void)
{
	uint32 slot;
	const char *reason;
	return cluster_ko_normal_stop_poll(&slot, &reason);
}
static void
reset_test(void)
{
	memset(&storage, 0, sizeof(storage));
	IsUnderPostmaster = false;
	cluster_ko_shmem_init();
	IsUnderPostmaster = true;
	MyAuxProcType = SinvalBcastProcess;
	flush_count = drop_count = ack_count = wake_count = 0;
	removal_ready = true;
	gate_admit = true;
	gate_calls = 0;
	liveness_calls = 0;
	cluster_shared_config = false;
	shared_relation = true;
	current_epoch = 1;
	existing_forks = (1u << MAIN_FORKNUM) | (1u << VISIBILITYMAP_FORKNUM) | (1u << SPACE_FORKNUM);
	synced_forks = 0;
	sync_count = 0;
	sync_fail_fork = epoch_change_sync_fork = -1;
	epoch_change_flush = epoch_change_drop = false;
	recorded_batch = 0;
	record_count = 0;
	barrier_active = false;
	barrier_complete = true;
	epoch_change_complete = false;
	barrier_requests = barrier_waits = barrier_removes = 0;
	memset(&last_ack, 0, sizeof(last_ack));
	memset(&last_request, 0, sizeof(last_request));
	memset(&leases, 0, sizeof(leases));
	ClusterHwLeaseCtl = &leases;
	for (int i = 0; i < CLUSTER_HW_LEASE_SLOTS; i++)
		leases.slots[i].fork = -1;
}
static void
enqueue_epoch(uint64 id, uint64 epoch)
{
	ClusterICEnvelope env = { 0 };
	KoFlushHeader request = { 0 };
	request.batch_id = id;
	request.epoch = epoch;
	request.source_node = 1;
	request.spc_oid = 1663;
	request.db_oid = 0; /* Shared relations have a valid zero database OID. */
	request.rel_number = 99;
	env.source_node_id = 1;
	env.payload_length = sizeof(request);
	cluster_ko_flush_request_handler(&env, &request);
}

static void
enqueue(uint64 id)
{
	enqueue_epoch(id, current_epoch);
}

UT_TEST(test_only_actual_consumer_can_observe)
{
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
	reset_test();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
	MyAuxProcType = SinvalBcastProcess;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_real_admission_flush_drop_ack_order)
{
	reset_test();
	cluster_hw_lease_install((RelFileLocator){1663, 0, 99}, MAIN_FORKNUM, 4, 4);
	enqueue(42);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(ack_count, 0);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(sync_count, 0); /* The original nonshared path is unchanged. */
	UT_ASSERT_EQ(drop_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 42);
	UT_ASSERT_EQ(last_ack.status, KO_FLUSH_ACK_DONE);
	UT_ASSERT_EQ(ack_count, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_consumed), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_orphan_zero), 4);
}

UT_TEST(test_origin_barrier_discards_lease_even_without_remote_work)
{
	RelFileLocator locator = {1663, 0, 99};

	for (int shared = 0; shared <= 1; shared++) {
		reset_test();
		cluster_shared_config = shared;
		cluster_hw_lease_install(locator, MAIN_FORKNUM, 4, 4);
		cluster_ko_flush_and_wait_ack(locator, RELPERSISTENCE_PERMANENT);
		UT_ASSERT_EQ(cluster_hw_lease_next_block(locator, MAIN_FORKNUM), InvalidBlockNumber);
		UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_consumed), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_orphan_zero), 4);
		UT_ASSERT_EQ(liveness_calls, shared); /* New profile cannot disable KO. */
	}
}
UT_TEST(test_full_ring_wrap_and_retained_stale_bytes)
{
	reset_test();
	for (int i = 1; i <= 64; i++)
		enqueue(i);
	UT_ASSERT_EQ(cluster_ko_inbound_full_count(), 1);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(ack_count, 63);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	enqueue(65);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(last_ack.batch_id, 65);
}
UT_TEST(test_later_invalid_slot_overrides_pending)
{
	uint32 slot;
	const char *reason;
	reset_test();
	enqueue(1);
	enqueue(2);
	storage.inbound[1].source_node = CLUSTER_MAX_NODES;
	UT_ASSERT_EQ(cluster_ko_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(slot, 1);
	storage.inbound[1].source_node = 1;
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u32(&storage.inbound_tail, CLUSTER_KO_INBOUND_CAPACITY);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_refused_removal_never_forges_apply_ack)
{
	reset_test();
	removal_ready = false;
	enqueue(7);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(flush_count, 0);
	/* The original refusal consumes the local request, not its remote ACK
	 * wait. The separate actual SI wait-table poll covers that responsibility. */
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_sealed_new_request_never_enqueues_or_acks)
{
	ClusterICEnvelope env = { 0 };
	KoFlushHeader request = { 0 };
	reset_test();
	gate_admit = false;
	cluster_ko_flush_request_handler(&env, &request);
	UT_ASSERT_EQ(gate_calls, 0); /* Original validation still first. */
	enqueue(80);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ack_count, 0);
	reset_test();
	enqueue(81);
	UT_ASSERT_EQ(gate_calls, 1);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(ack_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 81);
}

UT_TEST(test_shared_existing_forks_synced_before_drop_and_ack)
{
	for (int all_forks = 0; all_forks <= 1; all_forks++) {
		reset_test();
		cluster_shared_config = true;
		if (all_forks)
			existing_forks = (1u << (MAX_FORKNUM + 1)) - 1;
		enqueue(91);
		cluster_ko_drain_inbound_and_apply();
		UT_ASSERT_EQ(synced_forks, existing_forks);
		UT_ASSERT_EQ(sync_count, all_forks ? MAX_FORKNUM + 1 : 3);
		UT_ASSERT_EQ(drop_count, 1);
		UT_ASSERT_EQ(ack_count, 1);
		UT_ASSERT_EQ(last_ack.epoch, 1);
	}
	reset_test();
	cluster_shared_config = true;
	shared_relation = false;
	enqueue(92);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(sync_count, 0);
	UT_ASSERT_EQ(ack_count, 1);
}
UT_TEST(test_shared_sync_failure_preserves_buffers_and_never_acks)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		volatile bool caught = false;
		reset_test();
		cluster_shared_config = true;
		existing_forks = (1u << (MAX_FORKNUM + 1)) - 1;
		sync_fail_fork = fork;
		enqueue(93);
		PG_TRY();
		{
			cluster_ko_drain_inbound_and_apply();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(flush_count, 1);
		UT_ASSERT_EQ(sync_count, fork + 1);
		UT_ASSERT_EQ(drop_count, 0);
		UT_ASSERT_EQ(ack_count, 0);
		UT_ASSERT_EQ(cluster_ko_peer_apply_count(), 0);
		UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
		cluster_ko_drain_inbound_and_apply();
		UT_ASSERT_EQ(flush_count, 1); /* Original failed request is not retried. */
	}
}
UT_TEST(test_queued_old_epoch_refused_before_page_work)
{
	reset_test();
	enqueue(94);
	current_epoch++;
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(flush_count, 0);
	UT_ASSERT_EQ(drop_count, 0);
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_epoch_change_during_io_never_acks_new_epoch)
{
	for (int phase = 0; phase < 3; phase++) {
		reset_test();
		cluster_shared_config = true;
		epoch_change_flush = phase == 0;
		epoch_change_sync_fork = phase == 1 ? MAIN_FORKNUM : -1;
		epoch_change_drop = phase == 2;
		enqueue(95);
		cluster_ko_drain_inbound_and_apply();
		UT_ASSERT_EQ(current_epoch, 2);
		UT_ASSERT_EQ(flush_count, 1);
		UT_ASSERT_EQ(drop_count, phase == 2 ? 1 : 0);
		UT_ASSERT_EQ(ack_count, 0);
		UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	}
}
UT_TEST(test_request_requires_exact_nonzero_epoch)
{
	reset_test();
	current_epoch = 3;
	enqueue_epoch(96, 0);
	enqueue_epoch(96, 2);
	enqueue_epoch(96, 4);
	UT_ASSERT_EQ(gate_calls, 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	enqueue_epoch(97, 3);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(ack_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 97);
	UT_ASSERT_EQ(last_ack.epoch, 3);
	reset_test();
	current_epoch = 0;
	enqueue_epoch(98, 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_ack_requires_exact_nonzero_epoch)
{
	ClusterICEnvelope env = { 0 };
	KoFlushAckHeader ack = { 0 };
	reset_test();
	current_epoch = 3;
	env.source_node_id = 1;
	env.payload_length = sizeof(ack);
	ack.batch_id = 99;
	ack.acker_node = 1;
	ack.status = KO_FLUSH_ACK_DONE;
	for (int epoch = 0; epoch <= 4; epoch++) {
		if (epoch == 3)
			continue;
		ack.epoch = epoch;
		cluster_ko_flush_ack_handler(&env, &ack);
	}
	UT_ASSERT_EQ(record_count, 0);
	ack.epoch = 3;
	cluster_ko_flush_ack_handler(&env, &ack);
	UT_ASSERT_EQ(record_count, 1);
	UT_ASSERT_EQ(recorded_batch, 99);
	UT_ASSERT_EQ(cluster_ko_ack_received_count(), 1);
	current_epoch = ack.epoch = 0;
	cluster_ko_flush_ack_handler(&env, &ack);
	UT_ASSERT_EQ(record_count, 1);
}
UT_TEST(test_origin_completion_stays_in_request_epoch)
{
	RelFileLocator locator = { 1663, 0, 99 };
	for (int phase = 0; phase < 3; phase++) {
		reset_test();
		barrier_complete = phase != 1;
		epoch_change_complete = phase == 2;
		UT_ASSERT_EQ(ko_run_barrier(locator, 1u << 1), phase == 0);
		UT_ASSERT_EQ(barrier_requests, 1);
		UT_ASSERT_EQ(last_request.epoch, 1);
		UT_ASSERT_EQ(barrier_waits, phase == 1 ? 1 : 0);
		UT_ASSERT_EQ(barrier_removes, 1);
		UT_ASSERT(!barrier_active);
	}
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_only_actual_consumer_can_observe);
	UT_RUN(test_real_admission_flush_drop_ack_order);
	UT_RUN(test_origin_barrier_discards_lease_even_without_remote_work);
	UT_RUN(test_full_ring_wrap_and_retained_stale_bytes);
	UT_RUN(test_later_invalid_slot_overrides_pending);
	UT_RUN(test_refused_removal_never_forges_apply_ack);
	UT_RUN(test_sealed_new_request_never_enqueues_or_acks);
	UT_RUN(test_shared_existing_forks_synced_before_drop_and_ack);
	UT_RUN(test_shared_sync_failure_preserves_buffers_and_never_acks);
	UT_RUN(test_queued_old_epoch_refused_before_page_work);
	UT_RUN(test_epoch_change_during_io_never_acks_new_epoch);
	UT_RUN(test_request_requires_exact_nonzero_epoch);
	UT_RUN(test_ack_requires_exact_nonzero_epoch);
	UT_RUN(test_origin_completion_stays_in_request_epoch);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
