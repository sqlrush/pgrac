/*-------------------------------------------------------------------------
 * test_cluster_control_transport.c
 *    Actual outbound + registry + retirement service + TCP/FIFO + work queue.
 *    Envelope construction and master mutation are explicit boundaries;
 *    the real GRD atomic mutation has a separate production C suite.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define PGRAC_CONTROL_TRANSPORT_EMBEDDED
int control_tcp_fixture_main(void);
#define main control_tcp_fixture_main
#include "test_cluster_ic_tier1_partial.c"
#undef main

#include "cluster/cluster_control_retire.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_startup_phase.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "../../backend/cluster/cluster_grd_work_queue.c"
#include "../../backend/cluster/cluster_grd_outbound.c"

bool cluster_shared_config = true;
int cluster_lms_workers = 1;
int cluster_lmon_main_loop_interval = 1000;
int MaxBackends = 200;
int max_prepared_xacts = 0;

int
cluster_conf_declared_node_count_early(void)
{
	return 4;
}

Size
add_size(Size left, Size right)
{
	if (left > SIZE_MAX - right)
		abort();
	return left + right;
}

Size
mul_size(Size left, Size right)
{
	if (right != 0 && left > SIZE_MAX / right)
		abort();
	return left * right;
}

ProcessingMode Mode = NormalProcessing;
BackendType MyBackendType = B_LMON;
PROC_HDR *ProcGlobal;
static LWLockPadded work_lock, outbound_lock;
static LWLock *held_lock;
static ClusterControlRetireMessage ack_message;
static bool master_saw_acquisition;
static bool expect_acquisition;
static unsigned master_retire_count, ack_count;

int
LWLockNewTrancheId(void)
{
	return 201;
}
void
LWLockInitialize(LWLock *lock, int id)
{
	lock->tranche = id;
}
void
LWLockRegisterTranche(int id pg_attribute_unused(), const char *name pg_attribute_unused())
{}
LWLockPadded *
GetNamedLWLockTranche(const char *name)
{
	return strcmp(name, "ClusterGrdWorkQueue") == 0 ? &work_lock : &outbound_lock;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	Assert(held_lock == NULL);
	held_lock = lock;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	Assert(held_lock == lock);
	held_lock = NULL;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == held_lock;
}
void
SetLatch(Latch *latch pg_attribute_unused())
{}
void
cluster_lmon_wakeup(void)
{
	Assert(held_lock == NULL);
}
void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty pg_attribute_unused())
{}
void
cluster_grd_inc_ges_cleanup_deferred(void)
{}
void
cluster_grd_inc_ges_reply_deferred(void)
{}
void
cluster_grd_inc_ges_reply_dropped(void)
{}
uint64
cluster_lms_get_shard_master_generation(void)
{
	return 7;
}
int32
cluster_grd_lookup_master_gen(const ClusterResId *resid pg_attribute_unused(), uint64 *gen)
{
	*gen = 7;
	return UT_PEER_ID;
}
bool
cluster_authority_readiness_managed(void)
{
	return true;
}
bool
cluster_serving_ready_is_current(void)
{
	return true;
}
bool
cluster_recovery_transport_components_current(void)
{
	return true;
}
bool
cluster_recovery_transport_is_current(void)
{
	return true;
}
bool
cluster_qvotec_in_quorum(void)
{
	return true;
}
const ClusterICMsgTypeInfo *
cluster_ic_get_msg_type_info(uint8 type pg_attribute_unused())
{
	return NULL;
}
int
cluster_gcs_block_payload_shard(uint8 type pg_attribute_unused(),
								const void *bytes pg_attribute_unused(),
								uint16 len pg_attribute_unused(), int n pg_attribute_unused())
{
	return -1;
}
bool
cluster_lms_outbound_enqueue(int worker pg_attribute_unused(), uint8 type pg_attribute_unused(),
							 uint32 node pg_attribute_unused(),
							 const void *bytes pg_attribute_unused(),
							 uint16 len pg_attribute_unused())
{
	return false;
}
void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req pg_attribute_unused(),
												int32 dest pg_attribute_unused())
{}

/* This fixture checks ordering, not the separately tested GRD mutation. */
ClusterControlRetireVerb
cluster_ges_control_retire_at_master(const ClusterControlRetireMessage *message,
									 const ClusterControlRequestCut *cut)
{
	UT_ASSERT_EQ(message->key.holder.request_id, 7101);
	UT_ASSERT_EQ(cut->master, cluster_node_id);
	UT_ASSERT_EQ(master_saw_acquisition, expect_acquisition);
	master_saw_acquisition = false;
	master_retire_count++;
	return CLUSTER_CONTROL_RETIRED;
}

/* Only envelope construction is a boundary; admission, tail and FIFO are real. */
static ClusterICSendResult
send_through_tcp(uint8 type, int32 dest, const void *bytes, uint32 len)
{
	uint8
		frame[sizeof(ClusterICEnvelope) + sizeof(GesRequestPayload) + CLUSTER_CONTROL_RETIRE_BYTES];
	ClusterICEnvelope env = { 0 };
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GES_REQUEST);
	if (cluster_node_id == UT_PEER_ID) {
		UT_ASSERT_EQ(dest, 0);
		UT_ASSERT(cluster_control_retire_decode(bytes, len, &ack_message));
		ack_count++;
		return CLUSTER_IC_SEND_DONE;
	}
	UT_ASSERT_EQ(dest, UT_PEER_ID);
	UT_ASSERT(sizeof(env) + len <= sizeof(frame));
	env.msg_type = type;
	env.epoch = 1;
	env.source_node_id = 0;
	env.dest_node_id = UT_PEER_ID;
	env.payload_length = len;
	memcpy(frame, &env, sizeof(env));
	memcpy(frame + sizeof(env), bytes, len);
	return ClusterICOps_Tier1.send_bytes(dest, frame, sizeof(env) + len);
}

static void
receive_in_order(const char *bytes, Size len, unsigned expected)
{
	Size at = 0;
	unsigned seen = 0;
	ClusterGrdWorkItem item;
	cluster_node_id = UT_PEER_ID;
	while (at < len) {
		ClusterICEnvelope env;
		UT_ASSERT(len - at >= sizeof(env));
		memcpy(&env, bytes + at, sizeof(env));
		at += sizeof(env);
		UT_ASSERT(env.payload_length <= len - at);
		if (cluster_control_retire_is_frame(bytes + at, env.payload_length))
			cluster_control_retire_ingress(&env, bytes + at);
		else
			UT_ASSERT(
				cluster_grd_work_queue_enqueue(env.source_node_id, bytes + at, env.payload_length));
		at += env.payload_length;
		seen++;
	}
	UT_ASSERT_EQ(seen, expected);
	UT_ASSERT_EQ(master_retire_count, 0); /* ingress never executes mutation */
	while (cluster_grd_work_queue_dequeue(&item)) {
		if (cluster_control_retire_is_frame(item.payload, item.payload_len))
			cluster_control_retire_drain(&item);
		else {
			GesRequestPayload req;
			UT_ASSERT_EQ(item.payload_len, sizeof(req));
			memcpy(&req, item.payload, sizeof(req));
			UT_ASSERT_EQ(req.opcode, GES_REQ_OPCODE_REQUEST);
			UT_ASSERT_EQ(req.holder_request_id_lo, 7101);
			master_saw_acquisition = true;
		}
	}
	UT_ASSERT_EQ(master_retire_count, 1);
	UT_ASSERT_EQ(ack_count, 1);
	cluster_node_id = 0;
}

static void
control_tcp_case(bool disconnect)
{
	ClusterControlRequestKey key = { 0 };
	ClusterControlRequestOwner owner;
	ClusterControlRequestHandle handle;
	ClusterControlRequestCut cut = { 1, 7, UT_PEER_ID };
	ClusterControlRequestView before, after;
	GesRequestPayload req = { 0 };
	ClusterICEnvelope env = { 0 };
	uint8 ack[CLUSTER_CONTROL_RETIRE_BYTES];
	long got;
	Size req_size = sizeof(env) + sizeof(req);
	Size retire_size = sizeof(env) + CLUSTER_CONTROL_RETIRE_BYTES;

	cluster_node_id = 0;
	cluster_control_request_shmem_init();
	cluster_grd_work_queue_shmem_init();
	cluster_grd_outbound_shmem_register();
	ut_captured_region->init_fn();
	cluster_control_retire_lmon_start();
	ut_send_envelope_hook = send_through_tcp;
	master_saw_acquisition = false;
	expect_acquisition = !disconnect;
	master_retire_count = ack_count = 0;
	key.resid.type = CLUSTER_CF_RESID_TYPE;
	key.resid.lockmethodid = DEFAULT_LOCKMETHOD;
	key.holder.node_id = 0;
	key.holder.procno = 12;
	key.holder.cluster_epoch = 1;
	key.holder.request_id = 7101;
	UT_ASSERT(cluster_control_request_owner_init(12, 1012, &owner));
	UT_ASSERT(cluster_control_request_register(&key, ShareLock, &owner, 0, NoLock, &handle));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.lockmode = ShareLock;
	memcpy(req.resid, &key.resid, sizeof(key.resid));
	req.holder_procno = 12;
	req.holder_cluster_epoch_lo = 1;
	req.holder_request_id_lo = 7101;

	(void)ut_fill_until_eagain(ut_tx_fd);
	UT_ASSERT(cluster_grd_outbound_enqueue_backend_request(UT_PEER_ID, &req, sizeof(req)));
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 1);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	ut_now += 1000000;
	cluster_control_retire_lmon_tick();
	UT_ASSERT_EQ(tier1_outbound_fifo_frames[UT_PEER_ID], 1); /* RETIRE follows REQUEST */
	UT_ASSERT(cluster_control_request_snapshot(&handle, &before));
	UT_ASSERT(!cluster_control_request_forget(&handle, &owner, &cut));

	if (disconnect) {
		cluster_ic_tier1_close_peer(UT_PEER_ID, "control retirement TCP test");
		ut_release_backpressure(ut_tx_fd);
		(void)close(ut_rx_fd);
		UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
		UT_ASSERT_EQ(tier1_outbound_fifo_frames[UT_PEER_ID], 0);
		test_reconnect_after_close();
		ut_now += 1000000;
		cluster_control_retire_lmon_tick();
		UT_ASSERT(cluster_control_request_snapshot(&handle, &after));
		UT_ASSERT_EQ(before.message.exchange_id, after.message.exchange_id);
		got = ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, sizeof(ut_acc));
		UT_ASSERT_EQ(got, retire_size); /* no frame from the closed stream */
		receive_in_order(ut_acc, got, 1);
	} else {
		got = ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, sizeof(ut_acc));
		UT_ASSERT(got >= req_size + retire_size);
		receive_in_order(ut_acc + got - req_size - retire_size, req_size + retire_size, 2);
	}
	UT_ASSERT(cluster_control_retire_encode(&ack_message, ack));
	env.source_node_id = UT_PEER_ID;
	env.epoch = 1;
	env.payload_length = sizeof(ack);
	cluster_control_retire_ingress(&env, ack);
	UT_ASSERT(cluster_control_request_forget(&handle, &owner, &cut));
	/* A stale frame retained above transport cannot pass after slot reuse/free. */
	UT_ASSERT(cluster_grd_outbound_enqueue_backend_request(UT_PEER_ID, &req, sizeof(req)));
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 0);
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), 0);
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	ut_send_envelope_hook = NULL;
}

UT_TEST(accepted_request_precedes_retire_in_tcp_and_master_fifo)
{
	control_tcp_case(false);
}
UT_TEST(reconnect_drops_old_frames_and_retries_only_owned_retirement)
{
	control_tcp_case(true);
}

int
main(void)
{
	UT_PLAN(2);
	test_connect_registers_peer_fd();
	(void)ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, sizeof(ut_acc));
	UT_RUN(accepted_request_precedes_retire_in_tcp_and_master_fifo);
	UT_RUN(reconnect_drops_old_frames_and_retries_only_owned_retirement);
	cluster_ic_tier1_close_peer(UT_PEER_ID, "done");
	(void)close(ut_rx_fd);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
