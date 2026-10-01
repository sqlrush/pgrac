/* Real DATA/physical PI owner plus writeback codec, queues and endpoints.
 * The shared ROOT input scope and transport scheduling are explicit fixtures;
 * PAGE ancestry and file DATA write/fsync/read are the real implementations.
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_TEST_REAL_PI_WRITEBACK 1
int page_data_boundary_test_main(void);
#define main page_data_boundary_test_main
#define ShmemInitStruct page_fixture_ShmemInitStruct
#include "test_cluster_page_data.c"
#undef ShmemInitStruct
#undef main
extern void *ShmemInitStruct(const char *name, Size size, bool *found);

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_inputs.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/timestamp.h"

int MyProcPid = 100;
PGPROC *MyProc;
PROC_HDR *ProcGlobal;
static uint64 wb_epoch = 1, wb_nonce;
static uint64 wb_boots[3] = { 9, 9, 9 };
static bool wb_quorum = true, wb_stop, wb_input_current = true, wb_input_wait;
static ClusterWalSourceRef wb_sources[3];
static ClusterPcmPiStorageCutV1 wb_storage_cut;
static RfPageOnlinePlanV1 *wb_page_plan;
static unsigned wb_plan_builds, wb_input_releases, wb_sends;
static const ClusterShmemRegion *wb_region;
static uint8 wb_memory[3][65536] pg_attribute_aligned(MAXIMUM_ALIGNOF);
static uint8 wb_wire[CLUSTER_PI_WRITEBACK_MAX_BYTES];
static uint32 wb_length;
static int32 wb_destination;
static TimestampTz wb_now = 1000000;
static ResourceReleaseCallback wb_release_callback;
static ClusterICSendResult wb_send_result = CLUSTER_IC_SEND_DONE;
static bool wb_candidates;
static unsigned wb_master_completions;
static ClusterWalInputV1 wb_inputs[3];

uint32
cluster_pcm_lock_pi_candidates_v1(uint32 *cursor, uint32 budget, BufferTag *tags, uint32 capacity)
{
	UT_ASSERT_EQ(MyBackendType, B_CHECKPOINTER);
	UT_ASSERT(capacity > 0 && budget >= capacity);
	if (!wb_candidates)
		return 0;
	wb_candidates = false;
	tags[0] = wb_storage_cut.resource;
	return 1;
}
ClusterControlRootResult
cluster_pi_data_begin_v1(const ClusterSpaceIdentityKey *key, const ClusterPcmPiWriteCutV1 *cut,
						 ClusterPiDataV1 **out)
{
	/* This fixture exercises storage-state jobs, not the DATA transport. */
	UT_ASSERT(false);
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
ClusterControlRootResult
cluster_pi_data_poll_v1(ClusterPiDataV1 *job, ClusterPageDataReceiptV1 **out)
{
	UT_ASSERT(false);
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
void
cluster_pi_data_release_v1(ClusterPiDataV1 **job)
{
	UT_ASSERT(*job == NULL);
}
bool
cluster_pcm_lock_pi_write_complete_v1(const ClusterPageDataReceiptV1 *receipt,
									  const RfPageOnlinePlanV1 *plan,
									  const ClusterWalSourceRef *sources, uint32 count,
									  const ClusterPiPhysicalAckV1 *const *acks, uint32 ack_count,
									  uint32 *holders)
{
	UT_ASSERT(false);
	return false;
}
bool
cluster_pcm_lock_pi_storage_complete_v1(const ClusterPageDataReceiptV1 *receipt,
										const RfPageOnlinePlanV1 *plan,
										const ClusterWalSourceRef *sources, uint32 count,
										const ClusterPiPhysicalAckV1 *const *acks, uint32 ack_count,
										uint32 *holders)
{
	ClusterPcmPiStorageCutV1 cut;
	uint32 bitmap = 0;
	UT_ASSERT_EQ(MyBackendType, B_CHECKPOINTER);
	*holders = 0;
	if (!cluster_page_data_pi_storage_proof_v1(receipt, plan, sources, count, &cut)
		|| memcmp(&cut, &wb_storage_cut, sizeof(cut)) != 0)
		return false;
	for (uint32 i = 0; i < ack_count; i++) {
		int32 node;
		if (!cluster_page_data_pi_ack_read_v1(acks[i], receipt, &node) || node < 0 || node >= 3
			|| (bitmap & (1u << node)))
			return false;
		bitmap |= 1u << node;
	}
	if (bitmap != cut.pi_holders_bitmap)
		return false;
	wb_master_completions++;
	*holders = bitmap;
	wb_storage_cut.pi_holders_bitmap = 0;
	return true;
}

uint64
GetSystemIdentifier(void)
{
	return writer.claim.identity.system_identifier;
}
uint64
cluster_epoch_get_current(void)
{
	return wb_epoch;
}
bool
cluster_qvotec_in_quorum(void)
{
	return wb_quorum;
}
bool
cluster_normal_stop_requested(void)
{
	return wb_stop;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return false;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return wb_now;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return node >= 0 && node < 3 ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_ABSENT;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return node >= 0 && node < 3 ? wb_boots[node] : 0;
}
int32
cluster_gcs_lookup_master(BufferTag tag)
{
	return 0;
}
bool
cluster_pcm_lock_pi_write_snapshot_v1(BufferTag tag, ClusterPcmPiWriteCutV1 *out)
{
	return false;
}
bool
cluster_pcm_lock_pi_storage_snapshot_v1(BufferTag tag, ClusterPcmPiStorageCutV1 *out)
{
	if (!BufferTagsEqual(&tag, &wb_storage_cut.resource))
		return false;
	*out = wb_storage_cut;
	return true;
}
bool
pg_strong_random(void *ptr, size_t length)
{
	UT_ASSERT_EQ(length, sizeof(wb_nonce));
	++wb_nonce;
	memcpy(ptr, &wb_nonce, length);
	return true;
}
void
cluster_lmon_wakeup(void)
{}
void
SetLatch(Latch *latch)
{}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	wb_release_callback = callback;
}
void
before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{}
void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	wb_region = region;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (strcmp(name, "pgrac PI writeback") != 0)
		return page_fixture_ShmemInitStruct(name, size, found);
	UT_ASSERT(size <= sizeof(wb_memory[0]));
	*found = false;
	return wb_memory[cluster_node_id];
}
void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	UT_ASSERT_EQ(info->plane, CLUSTER_IC_PLANE_CONTROL);
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_PI_WRITEBACK);
	UT_ASSERT_EQ(MyBackendType, B_LMON);
	UT_ASSERT(length <= sizeof(wb_wire));
	memcpy(wb_wire, payload, length);
	wb_length = length;
	wb_destination = destination;
	wb_sends++;
	return wb_send_result;
}
ClusterControlRootResult
cluster_wal_inputs_begin_v1(const uint8 uuid[16], uint64 sysid, ClusterWalInputsV1 **out)
{
	UT_ASSERT(MyBackendType == B_BG_WRITER || MyBackendType == B_CHECKPOINTER);
	*out = (void *)1;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs)
{
	return 3;
}
const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	return index < 3 ? &wb_inputs[index] : NULL;
}
ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	return wb_input_current ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
ClusterControlRootResult
cluster_wal_inputs_contributions_v1(ClusterWalInputsV1 *inputs, bool space_active,
									ClusterThreadRecoveryFabricPlanV1 **out, uint64 *records,
									RfPageProofDetailV1 *detail)
{
	*out = NULL;
	*records = 0;
	*detail = RF_PAGE_PROOF_DETAIL_OK;
	if (wb_input_wait)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	wb_plan_builds++;
	*out = (void *)wb_page_plan;
	*records = 3;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	if (*inputs != NULL)
		wb_input_releases++;
	*inputs = NULL;
}
uint32
cluster_thread_recovery_fabric_participant_count_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	UT_ASSERT((const void *)plan == wb_page_plan);
	return 3;
}
const RfPageOnlinePlanV1 *
cluster_thread_recovery_fabric_page_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	UT_ASSERT((const void *)plan == wb_page_plan);
	return wb_page_plan;
}
void
cluster_thread_recovery_fabric_plan_destroy_v1(ClusterThreadRecoveryFabricPlanV1 **plan)
{
	*plan = NULL; /* This fixture retains the real PAGE plan until scenario end. */
}

static bool wb_allocation_error;
static void *
wb_palloc0(Size size)
{
	if (wb_allocation_error) {
		wb_allocation_error = false;
		pg_re_throw();
	}
	return palloc0(size);
}
#define palloc0 wb_palloc0
#include "../../backend/cluster/cluster_pi_writeback.c"
#undef palloc0

static void
wb_select(int node, BackendType role)
{
	cluster_node_id = node;
	MyBackendType = role;
	MyProcPid = 100 + node;
	writer = wb_sources[node];
	ack_boot = wb_boots[node];
	CurrentResourceOwner = node == 0 ? (void *)1 : (void *)2;
	wb_shared = (WritebackShared *)wb_memory[node];
}

static ClusterPageDataReceiptV1 *
wb_setup(void)
{
	ClusterPageDataReceiptV1 *data = NULL;
	wb_epoch = ack_writer_epoch = 1;
	wb_quorum = wb_input_current = true;
	wb_stop = wb_input_wait = false;
	wb_send_result = CLUSTER_IC_SEND_DONE;
	wb_plan_builds = wb_input_releases = wb_sends = 0;
	wb_master_completions = 0;
	wb_candidates = false;
	wb_page_plan = prepare_storage_observation(wb_sources, &wb_storage_cut);
	wb_storage_cut.master_session_incarnation = 9;
	UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &wb_storage_cut, wb_page_plan,
												   wb_sources, 3, &data));
	cluster_pi_writeback_shmem_register_v1();
	for (int node = 0; node < 3; node++) {
		wb_boots[node] = 9;
		memset(&wb_inputs[node], 0, sizeof(wb_inputs[node]));
		wb_inputs[node].kind = CLUSTER_WAL_INPUT_CHECKPOINT;
		wb_inputs[node].current = true;
		wb_inputs[node].source = wb_sources[node];
		wb_inputs[node].checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
		wb_select(node, B_LMON);
		wb_region->init_fn();
	}
	wb_select(0, B_CHECKPOINTER);
	wb_now += 1000000;
	return data;
}

static ClusterPiWritebackJobV1 *
wb_start(ClusterPageDataReceiptV1 *data)
{
	ClusterPiWritebackJobV1 *job = NULL;
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1,
											   &wb_sources[1], &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 1);
	return job;
}

static void
wb_deliver(int source, int dest, const uint8 *wire, uint32 length)
{
	ClusterICEnvelope env = { 0 };
	wb_select(dest, B_LMON);
	env.msg_type = PGRAC_IC_MSG_PI_WRITEBACK;
	env.source_node_id = source;
	env.dest_node_id = dest;
	env.epoch = wb_epoch;
	env.payload_length = length;
	cluster_pi_writeback_ingress_v1(&env, wire);
}

UT_TEST(remote_physical_ack_runs_actual_ancestry_and_disposal)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = NULL;
	ClusterPiPhysicalAckV1 *ack = NULL;
	uint8 request[CLUSTER_PI_WRITEBACK_MAX_BYTES];
	uint32 request_length;
	int32 node;
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1,
											   &wb_sources[1], &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 1);
	request_length = wb_length;
	memcpy(request, wb_wire, wb_length);
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, request, request_length);
	UT_ASSERT_EQ(pi_discards, 0);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_plan_builds, 1);
	UT_ASSERT_EQ(wb_input_releases, 1);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 0);
	wb_deliver(1, 0, wb_wire, wb_length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_page_data_pi_ack_import_v1(job, 0, data, &ack));
	UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, data, &node));
	UT_ASSERT_EQ(node, 1);
	cluster_page_data_pi_ack_free_v1(&ack);
	/* Duplicate requests can only repeat the qualified result, not rebuild
	 * another plan or dispose a replacement PI. */
	wb_deliver(0, 1, request, request_length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(!cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_plan_builds, 1);
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_codec_has_exact_bounded_authenticated_facts)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackMessageV1 m = { 0 }, decoded, bad;
	Size length;
	m.verb = CLUSTER_PI_WRITEBACK_NOTIFY;
	m.count = CLUSTER_PI_WRITEBACK_MAX;
	m.nonce = m.epoch = 1;
	m.peer = wb_sources[1];
	UT_ASSERT(cluster_page_data_pi_fact_v1(data, &m.facts[0]));
	for (uint32 i = 1; i < m.count; i++) {
		m.facts[i] = m.facts[0];
		m.facts[i].binding.identity.blockno += i;
		m.facts[i].storage_cut.resource.blockNum += i;
	}
	UT_ASSERT(!cluster_pi_writeback_encode_v1(&m, wb_wire, sizeof(wb_wire) - 1, &length));
	UT_ASSERT_EQ(length, 0);
	UT_ASSERT(cluster_pi_writeback_encode_v1(&m, wb_wire, sizeof(wb_wire), &length));
	UT_ASSERT_EQ(length, CLUSTER_PI_WRITEBACK_MAX_BYTES);
	UT_ASSERT(cluster_pi_writeback_decode_v1(wb_wire, length, &decoded));
	UT_ASSERT(memcmp(&m, &decoded, sizeof(m)) == 0);
	UT_ASSERT(!cluster_pi_writeback_decode_v1(wb_wire, length - 1, &decoded));
	for (unsigned i = 0; i < 4; i++) {
		Size offset = i == 0 ? 168 : i == 1 ? 74 : i == 2 ? 176 + 360 + 56 : 176 + 360 + 228;
		wb_wire[offset] = 1;
		UT_ASSERT(!cluster_pi_writeback_decode_v1(wb_wire, length, &decoded));
		wb_wire[offset] = 0;
	}
	bad = m;
	bad.count++;
	UT_ASSERT(!cluster_pi_writeback_encode_v1(&bad, wb_wire, sizeof(wb_wire), &length));
	bad = m;
	bad.facts[1] = bad.facts[0];
	UT_ASSERT(!cluster_pi_writeback_encode_v1(&bad, wb_wire, sizeof(wb_wire), &length));
	bad = m;
	bad.peer.claim.identity.origin_owner_incarnation = 0;
	UT_ASSERT(!cluster_pi_writeback_encode_v1(&bad, wb_wire, sizeof(wb_wire), &length));
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_rejects_foreign_sender_boot_and_changed_master_cut)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_start(data);
	uint8 request[CLUSTER_PI_WRITEBACK_MAX_BYTES];
	uint32 length = wb_length;
	memcpy(request, wb_wire, length);
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(2, 1, request, length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(!cluster_pi_writeback_bgwriter_tick_v1());
	wb_boots[1]++;
	wb_deliver(0, 1, request, length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(!cluster_pi_writeback_bgwriter_tick_v1());
	wb_boots[1]--;
	wb_deliver(0, 1, request, length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	wb_deliver(2, 0, wb_wire, wb_length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	wb_deliver(1, 0, wb_wire, wb_length);
	wb_storage_cut.authority.transition_count++;
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(pi_discards, 1); /* Physical success cannot clear a later cut. */
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_wait_keeps_one_scope_and_stop_cancels_actual_owner)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_start(data);
	const char *reason;
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, wb_wire, wb_length);
	wb_select(1, B_BG_WRITER);
	wb_input_wait = true;
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(wb_input_releases, 0);
	UT_ASSERT_EQ(pi_discards, 0);
	UT_ASSERT_EQ(cluster_pi_writeback_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_PENDING);
	wb_stop = true;
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(cluster_pi_writeback_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_PENDING);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(wb_input_releases, 1);
	UT_ASSERT_EQ(cluster_pi_writeback_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pi_discards, 0);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_never_acknowledges_missing_ancestry_or_busy_physical_pi)
{
	for (unsigned fault_case = 0; fault_case < 3; fault_case++) {
		ClusterPageDataReceiptV1 *data = wb_setup();
		ClusterPiWritebackJobV1 *job = wb_start(data);
		physical_pi_from_source(&wb_sources[1], fault_case == 0 ? 81 : 19);
		if (fault_case == 1)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		if (fault_case == 2)
			wb_input_current = false;
		wb_deliver(0, 1, wb_wire, wb_length);
		wb_select(1, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		UT_ASSERT_EQ(pi_discards, 0);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY);
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		cluster_pi_writeback_release_v1(&job);
		cluster_page_data_receipt_free_v1(&data);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_reply_is_retired_and_retries_only_transport_refusal)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_start(data);
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, wb_wire, wb_length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	wb_select(1, B_LMON);
	wb_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT(wb_shared->reply_pending);
	wb_now += WB_RETRY_US;
	wb_send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT(!wb_shared->reply_pending);
	UT_ASSERT_EQ(wb_sends, 3);
	wb_now += WB_RETRY_US;
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_sends, 3);
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(checkpointer_batch_reaches_actual_local_and_remote_physical_owners)
{
	ClusterPageDataReceiptV1 *previous = wb_setup();
	wb_storage_cut.pi_holders_bitmap = 3;
	wb_candidates = true;
	physical_pi_from_source(&wb_sources[0], 80);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_plan_builds, 1);
	UT_ASSERT_EQ(wb_master_completions, 0);
	UT_ASSERT_EQ(wb_input_releases, 0);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 1);
	/* Switch the physical fixture from A's discarded descriptor to B's
	 * independently resident PI; A's real invalidation cleared its tag. */
	InitBufferTag(&descriptors[1].bufferdesc.tag, &target.identity.locator, target.identity.forknum,
				  target.identity.blockno);
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, wb_wire, wb_length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 2);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	wb_deliver(1, 0, wb_wire, wb_length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
	UT_ASSERT_EQ(wb_plan_builds, 2);
	UT_ASSERT_EQ(wb_input_releases, 2);
	UT_ASSERT_EQ(wb_master_completions, 1);
	UT_ASSERT_EQ(wb_storage_cut.pi_holders_bitmap, 0);
	UT_ASSERT(!cluster_pi_writeback_checkpointer_tick_v1());
	cluster_page_data_receipt_free_v1(&previous);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(checkpointer_releases_pending_batch_before_native_checkpoint)
{
	ClusterPageDataReceiptV1 *previous = wb_setup();
	wb_storage_cut.pi_holders_bitmap = 3;
	wb_candidates = true;
	physical_pi_from_source(&wb_sources[0], 80);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
	UT_ASSERT_EQ(wb_input_releases, 0);
	cluster_pi_writeback_checkpointer_release_v1();
	UT_ASSERT_EQ(wb_input_releases, 1);
	UT_ASSERT_EQ(wb_master_completions, 0);
	UT_ASSERT_EQ(wb_storage_cut.pi_holders_bitmap, 3);
	UT_ASSERT_EQ(wb_shared->outbound_pid, 0);
	cluster_page_data_receipt_free_v1(&previous);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_initial_allocation_error_cannot_orphan_running_owner)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_start(data);
	volatile bool caught = false;
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, wb_wire, wb_length);
	wb_select(1, B_BG_WRITER);
	wb_allocation_error = true;
	PG_TRY();
	{
		(void)cluster_pi_writeback_bgwriter_tick_v1();
	}
	PG_CATCH();
	{
		caught = true;
		/* Original bgwriter ERROR recovery releases this ResourceOwner and
		 * resumes the same process, without invoking before_shmem_exit. */
		wb_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(wb_notice == NULL);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_REPLIED);
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(remote_physical_ack_runs_actual_ancestry_and_disposal);
	UT_RUN(writeback_codec_has_exact_bounded_authenticated_facts);
	UT_RUN(writeback_rejects_foreign_sender_boot_and_changed_master_cut);
	UT_RUN(writeback_wait_keeps_one_scope_and_stop_cancels_actual_owner);
	UT_RUN(writeback_never_acknowledges_missing_ancestry_or_busy_physical_pi);
	UT_RUN(writeback_reply_is_retired_and_retries_only_transport_refusal);
	UT_RUN(checkpointer_batch_reaches_actual_local_and_remote_physical_owners);
	UT_RUN(checkpointer_releases_pending_batch_before_native_checkpoint);
	UT_RUN(writeback_initial_allocation_error_cannot_orphan_running_owner);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
