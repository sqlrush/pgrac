/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual KO SPSC admission/drain, with explicit storage and ACK-queue boundary
 * fixtures. A queue-empty observation alone does not certify flush completion. */
#include "postgres.h"
#include "../../backend/cluster/cluster_ko_lock.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
AuxProcType MyAuxProcType = SinvalBcastProcess;
int cluster_node_id = 0;
int cluster_injection_armed_count = 0;
static ClusterKoShared storage;
static ClusterNodeInfo peer;
static SMgrRelationData relation;
static bool removal_ready = true;
static bool flushing;
static int flush_count, drop_count, ack_count, wake_count;
static KoFlushAckHeader last_ack;
static bool gate_admit = true;
static unsigned gate_calls;

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
	return 1;
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
}
void
DropRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
	Assert(!flushing && flush_count == drop_count + 1);
	drop_count++;
}
bool
cluster_grd_outbound_enqueue_backend_msg(uint8 type, uint32 dest, const void *payload, uint16 len)
{
	Assert(type == PGRAC_IC_MSG_KO_FLUSH_ACK && dest == 1 && len == sizeof(last_ack));
	Assert(flush_count == drop_count && drop_count == ack_count + 1);
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
}
static void
enqueue(uint64 id)
{
	ClusterICEnvelope env = { 0 };
	KoFlushHeader request = { 0 };
	request.batch_id = id;
	request.epoch = 1;
	request.source_node = 1;
	request.spc_oid = 1663;
	request.db_oid = 0; /* Shared relations have a valid zero database OID. */
	request.rel_number = 99;
	env.source_node_id = 1;
	env.payload_length = sizeof(request);
	cluster_ko_flush_request_handler(&env, &request);
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
	enqueue(42);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(ack_count, 0);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(drop_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 42);
	UT_ASSERT_EQ(last_ack.status, KO_FLUSH_ACK_DONE);
	UT_ASSERT_EQ(ack_count, 1);
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

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_only_actual_consumer_can_observe);
	UT_RUN(test_real_admission_flush_drop_ack_order);
	UT_RUN(test_full_ring_wrap_and_retained_stale_bytes);
	UT_RUN(test_later_invalid_slot_overrides_pending);
	UT_RUN(test_refused_removal_never_forges_apply_ack);
	UT_RUN(test_sealed_new_request_never_enqueues_or_acks);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
