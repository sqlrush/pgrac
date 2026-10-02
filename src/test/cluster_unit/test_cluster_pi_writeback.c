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
#include "cluster/cluster_pi_rebuild.h"
#include "storage/buffile.h"
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
static ClusterPcmPiStorageCutV1 wb_vm_cut;
static bool wb_multiple;
static RfPageOnlinePlanV1 *wb_page_plan;
static unsigned wb_plan_builds, wb_input_releases, wb_sends;
static unsigned wb_census_builds;
static bool wb_census_tail_bad;
static unsigned wb_log_calls[3];
static RfSideOnlineOperationKindV1 wb_side_kind;
static bool wb_space_valid;
static bool wb_side_mapped;
static uint32 wb_space_count;
static RfSideSpaceContributionV1 wb_space_contribution;
const RfSideOnlinePlanV1 *
cluster_thread_recovery_fabric_side_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return (const RfSideOnlinePlanV1 *)plan;
}
uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	return wb_side_kind == RF_SIDE_ONLINE_OPERATION_INVALID ? 0 : 1;
}
bool
rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
								 RfSideOnlineOperationV1 *out)
{
	if (index != 0 || wb_side_kind == RF_SIDE_ONLINE_OPERATION_INVALID)
		return false;
	memset(out, 0, sizeof(*out));
	out->kind = wb_side_kind;
	out->history_only = true; /* Retained predecessors also carry PI duties. */
	if (wb_space_valid || wb_side_mapped
		|| wb_side_kind == RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL) {
		RfPageOnlineTargetViewV1 view;
		UT_ASSERT(rf_page_online_plan_target_v1(wb_page_plan, 0, &view));
		for (uint32 i = 0; i < view.contributors->edge_count; i++)
			if (view.contributors->edges[i].participant_index == 2) {
				out->identity.participant_index = 2;
				out->identity.record = view.contributors->edges[i].record_identity;
				break;
			}
	}
	return true;
}
bool
rf_side_online_plan_contribution_owners_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
										   RfSideContributionOwnersV1 *out)
{
	if (index != 0
		|| (!wb_side_mapped && !wb_space_valid
			&& wb_side_kind != RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL))
		return false;
	out->space_locator_count = wb_space_valid ? wb_space_count : 0;
	out->owners = wb_space_valid ? RF_SIDE_CONTRIBUTION_SPACE : RF_SIDE_CONTRIBUTION_NATIVE_CONTROL;
	if (wb_side_kind == RF_SIDE_ONLINE_OPERATION_XACT)
		out->owners |= RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL;
	return true;
}
/* The SIDE planner is an explicit boundary in this endpoint fixture. Its
 * typed payload/history enumeration executes separately in side_xact. */
uint32
rf_side_online_plan_space_contribution_count_v1(const RfSideOnlinePlanV1 *plan, uint32 index)
{
	return wb_space_valid && index == 0 ? wb_space_count : UINT32_MAX;
}
bool
rf_side_online_plan_space_contribution_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
										  uint32 locator, RfSideSpaceContributionV1 *out)
{
	if (!wb_space_valid || index != 0 || locator >= wb_space_count)
		return false;
	*out = wb_space_contribution;
	out->result.key.locator.relNumber += locator;
	return true;
}
bool
cluster_thread_recovery_fabric_cut_v1(const ClusterThreadRecoveryFabricPlanV1 *plan, uint32 index,
									  RfContributorStreamCutV1 *out)
{
	RfPageOnlineTargetViewV1 view;
	if (!rf_page_online_plan_target_v1(wb_page_plan, 0, &view)
		|| index >= view.contributors->participant_count)
		return false;
	*out = view.contributors->cuts[index];
	return true;
}
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
static bool wb_inputs_pinned[3], wb_resume_wait;
static unsigned wb_resumes;
static unsigned wb_absorbs;
volatile sig_atomic_t ShutdownRequestPending;
static ClusterGrdPiRebuildCutV1 rebuild_cut;
static bool rebuild_needed, rebuild_apply_ok = true, rebuild_drift, rebuild_root_drift;
static uint32 rebuild_holders, rebuild_applies, rebuild_completions;
static unsigned rebuild_side_blocked, rebuild_logs;
static unsigned rebuild_apply_blocked, rebuild_fail_after = UINT32_MAX;
static unsigned rebuild_plan_blocked;
static RfPageProofDetailV1 rebuild_plan_failure;

void
cluster_grd_inc_pi_rebuild_side_blocked(void)
{
	rebuild_side_blocked++;
}

void
cluster_grd_inc_pi_rebuild_apply_blocked(void)
{
	rebuild_apply_blocked++;
}

void
cluster_grd_inc_pi_rebuild_plan_blocked(void)
{
	rebuild_plan_blocked++;
}

int
cluster_gcs_lookup_master_static(BufferTag tag)
{
	return 1;
}
int
cluster_grd_pi_rebuild_snapshot_v1(ClusterGrdPiRebuildCutV1 *out)
{
	*out = rebuild_cut;
	return rebuild_needed ? 1 : 0;
}
bool
cluster_grd_pi_rebuild_current_v1(const ClusterGrdPiRebuildCutV1 *cut)
{
	return rebuild_needed && memcmp(cut, &rebuild_cut, sizeof(*cut)) == 0;
}
bool
cluster_grd_pi_rebuild_gate_v1(void)
{
	return rebuild_needed;
}
bool
cluster_grd_pi_rebuild_complete_v1(const ClusterGrdPiRebuildCutV1 *cut)
{
	UT_ASSERT(wb_inputs_pinned[cluster_node_id]);
	if (!cluster_grd_pi_rebuild_current_v1(cut))
		return false;
	rebuild_completions++;
	rebuild_needed = false;
	return true;
}
bool
cluster_pcm_rebuild_pi_contributors_v1(const ClusterGrdPiRebuildCutV1 *cut, BufferTag tag,
									   uint32 holders, XLogRecPtr lsn, SCN scn)
{
	BufferTag expected;
	if (!cluster_grd_pi_rebuild_current_v1(cut))
		return false;
	UT_ASSERT_EQ(MyBackendType, B_BG_WRITER);
	UT_ASSERT(wb_inputs_pinned[cluster_node_id]);
	InitBufferTag(&expected, &target.identity.locator, tag.forkNum,
				  tag.forkNum == SPACE_FORKNUM ? tag.blockNum : target.identity.blockno);
	if (tag.forkNum == SPACE_FORKNUM) {
		UT_ASSERT(tag.relNumber >= expected.relNumber
				  && tag.relNumber - expected.relNumber < wb_space_count);
		expected.relNumber = tag.relNumber;
	}
	UT_ASSERT(BufferTagsEqual(&tag, &expected));
	UT_ASSERT_NE(lsn, 0);
	if (tag.forkNum == SPACE_FORKNUM) {
		UT_ASSERT(tag.blockNum <= 1);
		UT_ASSERT_EQ(scn, wb_space_contribution.result_token[tag.blockNum]);
		UT_ASSERT_EQ(holders, 4);
	} else {
		UT_ASSERT(tag.forkNum == MAIN_FORKNUM || tag.forkNum == VISIBILITYMAP_FORKNUM);
		UT_ASSERT_EQ(scn, tag.forkNum == MAIN_FORKNUM ? 80 : 7);
		UT_ASSERT_EQ(holders, tag.forkNum == MAIN_FORKNUM ? 7 : 4);
	}
	rebuild_applies++;
	if (rebuild_drift)
		rebuild_cut.routing_generation++;
	if (rebuild_root_drift)
		wb_input_current = false;
	if (!rebuild_apply_ok || rebuild_applies > rebuild_fail_after)
		return false;
	rebuild_holders |= holders;
	return true;
}

void
AbsorbSyncRequests(void)
{
	UT_ASSERT_EQ(MyBackendType, B_CHECKPOINTER);
	UT_ASSERT(!locks[0] && !locks[1]);
	wb_absorbs++;
}

uint32
cluster_pcm_lock_pi_candidates_v1(uint32 *cursor, uint32 budget, BufferTag *tags, uint32 capacity)
{
	UT_ASSERT_EQ(MyBackendType, B_CHECKPOINTER);
	UT_ASSERT(capacity > 0 && budget >= capacity);
	if (!wb_candidates)
		return 0;
	wb_candidates = false;
	tags[0] = wb_storage_cut.resource;
	if (wb_multiple)
		tags[1] = wb_vm_cut.resource;
	return wb_multiple ? 2 : 1;
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
	ClusterPcmPiStorageCutV1 *current;
	uint32 bitmap = 0;
	UT_ASSERT_EQ(MyBackendType, B_CHECKPOINTER);
	*holders = 0;
	if (!cluster_page_data_pi_storage_proof_v1(receipt, plan, sources, count, &cut))
		return false;
	current = wb_multiple && cut.resource.forkNum == VISIBILITYMAP_FORKNUM ? &wb_vm_cut
																		   : &wb_storage_cut;
	if (memcmp(&cut, current, sizeof(cut)) != 0)
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
	current->pi_holders_bitmap = 0;
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
	if (wb_multiple && BufferTagsEqual(&tag, &wb_vm_cut.resource)) {
		*out = wb_vm_cut;
		return true;
	}
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
	wb_inputs_pinned[cluster_node_id] = true;
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
	return wb_input_current && wb_inputs_pinned[cluster_node_id] ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
																 : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
ClusterControlRootResult
cluster_wal_inputs_suspend_v1(ClusterWalInputsV1 *inputs)
{
	wb_inputs_pinned[cluster_node_id] = false;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_wal_inputs_resume_v1(ClusterWalInputsV1 *inputs)
{
	wb_resumes++;
	if (wb_resume_wait)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	wb_inputs_pinned[cluster_node_id] = true;
	return cluster_wal_inputs_revalidate_v1(inputs);
}
ClusterControlRootResult
cluster_wal_inputs_wait_failed_v1(ClusterWalInputsV1 *inputs)
{
	wb_inputs_pinned[cluster_node_id] = false;
	return wb_input_current ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
ClusterControlRootResult
cluster_wal_inputs_contributions_v1(ClusterWalInputsV1 *inputs, bool space_active,
									ClusterThreadRecoveryFabricPlanV1 **out, uint64 *records,
									RfPageProofDetailV1 *detail)
{
	UT_ASSERT(wb_inputs_pinned[cluster_node_id]);
	*out = NULL;
	*records = 0;
	*detail = RF_PAGE_PROOF_DETAIL_OK;
	if (wb_input_wait)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	wb_plan_builds++;
	if (rebuild_plan_failure != RF_PAGE_PROOF_DETAIL_OK) {
		*detail = rebuild_plan_failure;
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	}
	*out = (void *)wb_page_plan;
	*records = 3;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
/* The physical scanner and typed decoder are separate boundaries here;
 * their real bytes/identity paths run in control_root and side_xact tests. */
static struct {
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blockno;
	uint64 token;
} census_component;

RfPageProofDetailV1
cluster_thread_recovery_record_census_v1(XLogReaderState *record, const ClusterWalSourceRef *source,
										 const RfContributorStreamCutV1 *cut,
										 ClusterRecoveryContributionVisitorV1 visitor, void *arg)
{
	if (source->claim.identity.origin_owner_incarnation == 0
		|| source->claim.identity.origin_owner_incarnation != cut->origin_owner_incarnation
		|| source->claim.identity.origin_thread_id != cut->failed_thread
		|| source->timeline != cut->timeline_id)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	return visitor(arg, &census_component.locator, census_component.forknum,
				   census_component.blockno, census_component.token)
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
}

ClusterControlRootResult
cluster_wal_inputs_census_v1(ClusterWalInputsV1 *inputs, ClusterWalCensusVisitorV1 visitor,
							 void *arg, uint64 *records, RfPageProofDetailV1 *detail)
{
	XLogReaderState reader = { 0 };
	UT_ASSERT(wb_inputs_pinned[cluster_node_id]);
	*records = 0;
	*detail = RF_PAGE_PROOF_DETAIL_OK;
	if (wb_input_wait)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	wb_plan_builds++;
	wb_census_builds++;
	*detail = rebuild_plan_failure;
	if (*detail != RF_PAGE_PROOF_DETAIL_OK)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	for (uint32 target_index = 0; target_index < rf_page_online_plan_target_count_v1(wb_page_plan);
		 target_index++) {
		RfPageOnlineTargetViewV1 view;
		UT_ASSERT(rf_page_online_plan_target_v1(wb_page_plan, target_index, &view));
		census_component.locator = view.page_identity.locator;
		census_component.forknum = view.page_identity.forknum;
		census_component.blockno = view.page_identity.blockno;
		for (uint32 i = 0; i < view.contributors->edge_count; i++) {
			const RfPageStableEdgeInputV1 *edge = &view.contributors->edges[i];
			ClusterWalSourceRef source;
			UT_ASSERT(
				rf_page_online_plan_source_v1(wb_page_plan, edge->participant_index, &source));
			reader.ReadRecPtr = edge->record_identity.read_rec_ptr;
			census_component.token = edge->result_token;
			*detail
				= visitor(&reader, &source, &view.contributors->cuts[edge->participant_index], arg);
			if (*detail != RF_PAGE_PROOF_DETAIL_OK)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
			(*records)++;
		}
	}
	if (wb_side_kind != RF_SIDE_ONLINE_OPERATION_INVALID) {
		RfSideContributionOwnersV1 owners;
		ClusterWalSourceRef source;
		RfContributorStreamCutV1 cut;
		if (!rf_side_online_plan_contribution_owners_v1((void *)wb_page_plan, 0, &owners)) {
			*detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		}
		UT_ASSERT(rf_page_online_plan_source_v1(wb_page_plan, 2, &source));
		UT_ASSERT(cluster_thread_recovery_fabric_cut_v1((void *)wb_page_plan, 2, &cut));
		for (uint32 i = 0; i < owners.space_locator_count; i++) {
			RfSideSpaceContributionV1 space = { 0 };
			UT_ASSERT(
				rf_side_online_plan_space_contribution_v1((void *)wb_page_plan, 0, i, &space));
			if (space.result.key.database_incarnation != source.claim.database_incarnation) {
				*detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
			}
			census_component.locator = space.result.key.locator;
			census_component.forknum = SPACE_FORKNUM;
			for (uint8 block = 0; block < 2; block++) {
				if (!(space.page_mask & (1u << block)))
					continue;
				census_component.blockno = block;
				census_component.token = space.result_token[block];
				*detail = visitor(&reader, &source, &cut, arg);
				if (*detail != RF_PAGE_PROOF_DETAIL_OK)
					return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
				(*records)++;
			}
		}
	}
	if (wb_census_tail_bad) {
		*detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* Native temporary file owner is replaced with a real FILE in this fixture. */
struct BufFile {
	FILE *file;
};
static unsigned census_file_open, census_file_reads;
BufFile *
BufFileCreateTemp(bool interXact)
{
	BufFile *file = malloc(sizeof(*file));
	UT_ASSERT(!interXact && file != NULL);
	file->file = tmpfile();
	UT_ASSERT(file->file != NULL);
	census_file_open++;
	return file;
}
void
BufFileClose(BufFile *file)
{
	UT_ASSERT_EQ(fclose(file->file), 0);
	census_file_open--;
	free(file);
}
void
BufFileWrite(BufFile *file, const void *ptr, size_t size)
{
	UT_ASSERT_EQ(fwrite(ptr, 1, size, file->file), size);
}
void
BufFileReadExact(BufFile *file, void *ptr, size_t size)
{
	UT_ASSERT_EQ(fread(ptr, 1, size, file->file), size);
	census_file_reads++;
}
int
BufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
{
	UT_ASSERT_EQ(fileno, 0);
	return fseeko(file->file, offset, whence);
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	wb_inputs_pinned[cluster_node_id] = false;
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
static void wb_elog(int level, const char *format, ...) pg_attribute_printf(2, 3);
static void
wb_elog(int level, const char *format, ...)
{
	char message[256];
	va_list args;
	va_start(args, format);
	vsnprintf(message, sizeof(message), format, args);
	va_end(args);
	UT_ASSERT_EQ(level, LOG);
	UT_ASSERT(strstr(message, "PGRAC_FAMILY=PI_WRITEBACK action=PAGE_DEFERRED stage=") != NULL);
	wb_log_calls[cluster_node_id]++;
}
#undef elog
#define elog wb_elog
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		if ((level) >= ERROR) {                                                                    \
			InterruptHoldoffCount = 0;                                                             \
			pg_re_throw();                                                                         \
		} else if ((level) == LOG)                                                                 \
			rebuild_logs++;                                                                        \
	} while (0)
#include "../../backend/cluster/cluster_pi_writeback.c"
#include "../../backend/cluster/cluster_pi_rebuild.c"
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
	wb_multiple = false;
	wb_epoch = ack_writer_epoch = 1;
	wb_quorum = wb_input_current = true;
	wb_stop = wb_input_wait = false;
	wb_send_result = CLUSTER_IC_SEND_DONE;
	wb_plan_builds = wb_input_releases = wb_sends = 0;
	memset(wb_log_calls, 0, sizeof(wb_log_calls));
	memset(wb_inputs_pinned, 0, sizeof(wb_inputs_pinned));
	wb_resumes = 0;
	wb_absorbs = 0;
	wb_resume_wait = false;
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

static ClusterPageDataReceiptV1 *
wb_second_page(void)
{
	ClusterPageDataTargetV1 vm_target = target;
	ClusterPageDataReceiptV1 *data = NULL;
	wb_multiple = true;
	wb_vm_cut = wb_storage_cut;
	wb_vm_cut.resource.forkNum = vm_target.identity.forknum = VISIBILITYMAP_FORKNUM;
	/* The existing sealed decoded record changes both MAIN and VM to token
	 * 7; the file-backed fixture contains those same header bytes. */
	UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&vm_target, &wb_vm_cut, wb_page_plan, wb_sources,
												   3, &data));
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
	ClusterWalWriterToken rejected;
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
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(!cluster_pi_writeback_ack_read_v1(job, 0, data, &rejected));
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
		ClusterWalWriterToken rejected;
		physical_pi_from_source(&wb_sources[1], fault_case == 0 ? 81 : 19);
		if (fault_case == 1)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		if (fault_case == 2)
			wb_input_current = false;
		wb_deliver(0, 1, wb_wire, wb_length);
		wb_select(1, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		UT_ASSERT_EQ(pi_discards, 0);
		UT_ASSERT_EQ(wb_shared->inbound_state, fault_case == 2 ? WB_EMPTY : WB_REPLIED);
		if (fault_case != 2) {
			UT_ASSERT_EQ(wb_shared->reply.count, 0);
			wb_select(1, B_LMON);
			cluster_pi_writeback_lmon_tick_v1();
			wb_deliver(1, 0, wb_wire, wb_length);
		}
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), fault_case == 2
															? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
															: CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(!cluster_pi_writeback_ack_read_v1(job, 0, data, &rejected));
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
	UT_ASSERT(wb_absorbs > 0);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 1);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT(!wb_inputs_pinned[0]);
	{
		unsigned before = wb_resumes;
		for (int i = 0; i < 100; i++) {
			UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
			UT_ASSERT(!wb_inputs_pinned[0]);
		}
		UT_ASSERT_EQ(wb_resumes, before);
		UT_ASSERT_EQ(wb_plan_builds, 1);
	}
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
	wb_resume_wait = true;
	UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
	UT_ASSERT_EQ(wb_master_completions, 0);
	UT_ASSERT(!wb_inputs_pinned[0]);
	wb_resume_wait = false;
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

UT_TEST(remote_batch_preserves_other_page_when_one_pi_retries)
{
	for (unsigned count = 1; count <= 2; count++) {
		ClusterPageDataReceiptV1 *data[2];
		ClusterPiWritebackJobV1 *job = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		data[0] = wb_setup();
		data[1] = wb_second_page();
		UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)data, count,
												   &wb_sources[1], &job),
					 0);
		wb_select(0, B_LMON);
		cluster_pi_writeback_lmon_tick_v1();
		physical_pi_from_source(&wb_sources[1], 19);
		pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		wb_deliver(0, 1, wb_wire, wb_length);
		wb_select(1, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_REPLIED);
		UT_ASSERT_EQ(wb_shared->reply.count, count - 1);
		{
			ClusterPiWritebackRejectionsV1 observed;
			UT_ASSERT(cluster_pi_writeback_rejections_v1(&observed));
			UT_ASSERT_EQ(observed.attempts[CLUSTER_PI_WRITEBACK_PEER_PHYSICAL], 1);
			UT_ASSERT_EQ(observed.log_events, 1);
			UT_ASSERT_EQ(wb_log_calls[1], 1);
			UT_ASSERT(BufferTagsEqual(&observed.last_resource, &wb_storage_cut.resource));
		}
		UT_ASSERT_EQ(pi_discards, 0);
		UT_ASSERT_EQ(wb_plan_builds, 1);
		wb_select(1, B_LMON);
		cluster_pi_writeback_lmon_tick_v1();
		if (count == 2) {
			uint8 valid[CLUSTER_PI_WRITEBACK_MAX_BYTES];
			Size valid_length = wb_length, wrong_length;
			ClusterPiWritebackMessageV1 reply, wrong;
			memcpy(valid, wb_wire, valid_length);
			UT_ASSERT(cluster_pi_writeback_decode_v1(valid, valid_length, &reply));
			for (unsigned fault = 0; fault < 3; fault++) {
				wrong = reply;
				if (fault == 0)
					wrong.nonce++;
				else if (fault == 1)
					wrong.facts[0].binding.record_crc++;
				else {
					wrong.count = 2;
					wrong.facts[1] = job->requested.facts[0];
				}
				UT_ASSERT(cluster_pi_writeback_encode_v1(&wrong, wb_wire, sizeof(wb_wire),
														 &wrong_length));
				wb_deliver(1, 0, wb_wire, wrong_length);
				wb_select(0, B_CHECKPOINTER);
				UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
			}
			memcpy(wb_wire, valid, valid_length);
			wb_length = valid_length;
		}
		wb_deliver(1, 0, wb_wire, wb_length);
		wb_select(0, B_CHECKPOINTER);
		/* A concurrent hot MAIN cut must not erase the VM's exact reply. */
		wb_storage_cut.authority.transition_count++;
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), 0);
		UT_ASSERT(!cluster_page_data_pi_ack_import_v1(job, 0, data[0], &ack));
		if (count == 2) {
			UT_ASSERT(cluster_page_data_pi_ack_import_v1(job, 1, data[1], &ack));
			cluster_page_data_pi_ack_free_v1(&ack);
		}
		cluster_pi_writeback_release_v1(&job);
		pg_atomic_fetch_sub_u32(&descriptors[1].bufferdesc.state, 1);
		cluster_page_data_receipt_free_v1(&data[0]);
		cluster_page_data_receipt_free_v1(&data[1]);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(checkpointer_batch_skips_local_retry_and_retires_other_page)
{
	for (unsigned remote = 0; remote < 2; remote++) {
		ClusterPageDataReceiptV1 *data = wb_setup(), *vm = wb_second_page();
		wb_storage_cut.pi_holders_bitmap = wb_vm_cut.pi_holders_bitmap = 3;
		wb_candidates = true;
		physical_pi_from_source(&wb_sources[0], 80);
		if (!remote)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
		UT_ASSERT(wb_batch != NULL);
		UT_ASSERT(!wb_inputs_pinned[0]);
		wb_select(0, B_LMON);
		cluster_pi_writeback_lmon_tick_v1();
		if (!remote)
			pg_atomic_fetch_sub_u32(&descriptors[1].bufferdesc.state, 1);
		/* Switch from A's actual discarded descriptor to B's resident PI. */
		InitBufferTag(&descriptors[1].bufferdesc.tag, &target.identity.locator,
					  target.identity.forknum, target.identity.blockno);
		physical_pi_from_source(&wb_sources[1], 19);
		if (remote)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		wb_deliver(0, 1, wb_wire, wb_length);
		wb_select(1, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		wb_select(1, B_LMON);
		cluster_pi_writeback_lmon_tick_v1();
		wb_deliver(1, 0, wb_wire, wb_length);
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT(cluster_pi_writeback_checkpointer_tick_v1());
		UT_ASSERT_EQ(wb_master_completions, 1);
		UT_ASSERT_EQ(wb_vm_cut.pi_holders_bitmap, 0);
		UT_ASSERT_EQ(wb_storage_cut.pi_holders_bitmap, 3);
		UT_ASSERT(!wb_inputs_pinned[0]);
		{
			ClusterPiWritebackRejectionsV1 observed;
			UT_ASSERT(cluster_pi_writeback_rejections_v1(&observed));
			UT_ASSERT_EQ(observed.attempts[remote ? CLUSTER_PI_WRITEBACK_REMOTE_ACK
												  : CLUSTER_PI_WRITEBACK_LOCAL_ACK],
						 1);
			UT_ASSERT_EQ(observed.log_events, 1);
			UT_ASSERT_EQ(wb_log_calls[0], 1);
			UT_ASSERT(BufferTagsEqual(&observed.last_resource, &wb_storage_cut.resource));
		}
		if (remote)
			pg_atomic_fetch_sub_u32(&descriptors[1].bufferdesc.state, 1);
		cluster_pi_writeback_checkpointer_release_v1();
		cluster_page_data_receipt_free_v1(&data);
		cluster_page_data_receipt_free_v1(&vm);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_rejections_survive_batches_and_throttle_per_boot_epoch)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackRejectionsV1 observed;
	BufferTag other = wb_storage_cut.resource;
	other.blockNum++;
	/* Actual per-page callers are covered above. Here two background roles
	 * share diagnostic state across jobs; neither nonce nor tag resets LOG. */
	wb_rejected(CLUSTER_PI_WRITEBACK_LOCAL_ACK, &wb_storage_cut.resource, 0, 1, 9);
	wb_select(0, B_BG_WRITER);
	wb_rejected(CLUSTER_PI_WRITEBACK_LOCAL_ACK, &other, 0, 1, 9);
	UT_ASSERT(cluster_pi_writeback_rejections_v1(&observed));
	UT_ASSERT_EQ(observed.attempts[CLUSTER_PI_WRITEBACK_LOCAL_ACK], 2);
	UT_ASSERT_EQ(observed.log_events, 1);
	UT_ASSERT(BufferTagsEqual(&observed.last_resource, &other));
	wb_rejected(CLUSTER_PI_WRITEBACK_LOCAL_ACK, &other, 0, 2, 9);
	wb_rejected(CLUSTER_PI_WRITEBACK_LOCAL_ACK, &other, 0, 2, 10);
	wb_rejected(CLUSTER_PI_WRITEBACK_REMOTE_ACK, &other, 1, 2, 10);
	UT_ASSERT(cluster_pi_writeback_rejections_v1(&observed));
	UT_ASSERT_EQ(observed.attempts[CLUSTER_PI_WRITEBACK_LOCAL_ACK], 4);
	UT_ASSERT_EQ(observed.attempts[CLUSTER_PI_WRITEBACK_REMOTE_ACK], 1);
	UT_ASSERT_EQ(observed.log_events, 4);
	UT_ASSERT_EQ(wb_log_calls[0], 4);
	UT_ASSERT_EQ(observed.last_peer, 1);
	UT_ASSERT_EQ(observed.last_reason, CLUSTER_PI_WRITEBACK_REMOTE_ACK);
	/* Diagnostic overflow must not silently look like a fresh zero count. */
	wb_shared->rejections.attempts[CLUSTER_PI_WRITEBACK_LOCAL_ACK] = UINT64_MAX;
	wb_rejected(CLUSTER_PI_WRITEBACK_LOCAL_ACK, &other, 0, 2, 10);
	UT_ASSERT(cluster_pi_writeback_rejections_v1(&observed));
	UT_ASSERT_EQ(observed.attempts[CLUSTER_PI_WRITEBACK_LOCAL_ACK], UINT64_MAX);
	UT_ASSERT_EQ(observed.log_events, 4);
	cluster_page_data_receipt_free_v1(&data);
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

static ClusterPageDataReceiptV1 *
rebuild_setup(void)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	wb_side_kind = RF_SIDE_ONLINE_OPERATION_INVALID;
	wb_space_valid = false;
	wb_side_mapped = false;
	wb_space_count = 1;
	memset(&wb_space_contribution, 0, sizeof(wb_space_contribution));
	wb_select(0, B_BG_WRITER);
	memset(&rebuild_cut, 0, sizeof(rebuild_cut));
	rebuild_cut.epoch = 1;
	rebuild_cut.self_boot = 9;
	rebuild_cut.affected[0] = 2;
	rebuild_needed = rebuild_apply_ok = true;
	rebuild_drift = rebuild_root_drift = false;
	rebuild_holders = rebuild_applies = rebuild_completions = 0;
	rebuild_side_blocked = rebuild_logs = 0;
	rebuild_apply_blocked = 0;
	rebuild_plan_blocked = 0;
	rebuild_plan_failure = RF_PAGE_PROOF_DETAIL_OK;
	rebuild_fail_after = UINT32_MAX;
	pi_rebuild_logged = false;
	return data;
}

UT_TEST(retained_rebuild_never_completes_with_unmapped_side_contributions)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	for (unsigned kind = RF_SIDE_ONLINE_OPERATION_XACT; kind <= RF_SIDE_ONLINE_OPERATION_SPACE;
		 kind++) {
		unsigned builds = wb_plan_builds;
		wb_side_kind = kind;
		UT_ASSERT(cluster_pi_rebuild_bgwriter_tick_v1());
		for (int i = 0; i < 4; i++)
			UT_ASSERT(cluster_pi_rebuild_bgwriter_tick_v1());
		UT_ASSERT_EQ(wb_plan_builds, builds + 1);
		UT_ASSERT_EQ(rebuild_side_blocked, 1);
		UT_ASSERT_EQ(rebuild_logs, 1);
		UT_ASSERT_EQ(rebuild_completions, 0);
		UT_ASSERT_EQ(rebuild_applies, 0);
		UT_ASSERT(!wb_inputs_pinned[0]);
		pi_rebuild_release();
	}
	wb_side_kind = RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_completions, 1);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_census_does_not_materialize_history_and_checks_tail_first)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	wb_census_builds = 0;
	wb_census_tail_bad = true;
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_WAIT);
	UT_ASSERT_EQ(wb_census_builds, 1);
	UT_ASSERT_EQ(rebuild_applies, 0);
	UT_ASSERT_EQ(rebuild_completions, 0);
	UT_ASSERT(!wb_inputs_pinned[0]);
	pi_rebuild_release();
	wb_census_tail_bad = false;
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_IDLE);
	UT_ASSERT_EQ(wb_census_builds, 2);
	UT_ASSERT_EQ(rebuild_completions, 1);
	UT_ASSERT_EQ(rebuild_holders, 7);
	UT_ASSERT(sizeof(PiRebuildJob) < 16384);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_rebuild_restores_all_original_writers_before_complete)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_holders, 7); /* A -> B -> C, including nonterminal writers. */
	UT_ASSERT_EQ(rebuild_applies, 2);
	UT_ASSERT_EQ(rebuild_completions, 1);
	UT_ASSERT_EQ(wb_input_releases, 1);
	UT_ASSERT(!wb_inputs_pinned[0]);
	UT_ASSERT(pi_rebuild_job == NULL);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_rebuild_maps_both_space_pages_under_original_source)
{
	for (unsigned invalid = 0; invalid < 2; invalid++) {
		ClusterPageDataReceiptV1 *data = rebuild_setup();
		wb_side_kind = RF_SIDE_ONLINE_OPERATION_SPACE;
		wb_space_valid = true;
		wb_space_contribution.result = identity;
		wb_space_contribution.result.key.database_incarnation += invalid;
		wb_space_contribution.page_mask = 3;
		wb_space_contribution.result_token[0] = 53;
		wb_space_contribution.result_token[1] = 41;
		UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), invalid != 0);
		UT_ASSERT_EQ(rebuild_applies,
					 invalid ? 0 : 4); /* All input checks precede GRD additions. */
		UT_ASSERT_EQ(rebuild_completions, invalid ? 0 : 1);
		UT_ASSERT(!wb_inputs_pinned[0]);
		UT_ASSERT_EQ(pi_rebuild_job == NULL, invalid == 0);
		pi_rebuild_release();
		cluster_page_data_receipt_free_v1(&data);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(retained_rebuild_maps_commit_drop_and_separate_non_pcm_owners)
{
	for (unsigned drop_count = 0; drop_count <= 40; drop_count += 20) {
		ClusterPageDataReceiptV1 *data = rebuild_setup();
		bool pending;
		wb_side_kind = RF_SIDE_ONLINE_OPERATION_XACT;
		wb_side_mapped = true;
		wb_space_valid = drop_count != 0;
		wb_space_count = drop_count;
		wb_space_contribution.result = identity;
		wb_space_contribution.page_mask = 3;
		wb_space_contribution.result_token[0] = 53;
		wb_space_contribution.result_token[1] = 41;
		pending = cluster_pi_rebuild_bgwriter_tick_v1();
		UT_ASSERT_EQ(pending, drop_count == 40);
		if (drop_count == 40) {
			UT_ASSERT_EQ(rebuild_applies, 64);
			UT_ASSERT_EQ(rebuild_completions, 0);
			UT_ASSERT(!wb_inputs_pinned[0]);
			UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
		}
		UT_ASSERT_EQ(rebuild_applies, 2 + 2 * drop_count);
		UT_ASSERT_EQ(rebuild_completions, 1);
		UT_ASSERT_EQ(rebuild_side_blocked, 0);
		UT_ASSERT_EQ(rebuild_logs, 0);
		pi_rebuild_release();
		cluster_page_data_receipt_free_v1(&data);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(retained_rebuild_waits_unpinned_and_rejects_incomplete_or_changed_cut)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	wb_input_wait = true;
	UT_ASSERT(cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT(!wb_inputs_pinned[0]);
	UT_ASSERT_EQ(rebuild_applies, 0);
	UT_ASSERT_EQ(rebuild_completions, 0);
	wb_input_wait = false;
	rebuild_drift = true;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_holders, 7); /* Additions survive, but never open service. */
	UT_ASSERT_EQ(rebuild_completions, 0);
	rebuild_drift = false;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_completions, 1);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_rebuild_root_change_or_apply_failure_never_completes)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	rebuild_apply_ok = false;
	UT_ASSERT(cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_completions, 0);
	UT_ASSERT_EQ(rebuild_holders, 0);
	rebuild_apply_ok = true;
	wb_input_current = false;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_applies, 1);
	UT_ASSERT_EQ(rebuild_completions, 0);
	UT_ASSERT(!wb_inputs_pinned[0]);
	UT_ASSERT(pi_rebuild_job == NULL);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_rebuild_apply_failure_retries_original_target_without_rescan)
{
	for (unsigned space = 0; space < 2; space++) {
		ClusterPageDataReceiptV1 *data = rebuild_setup();
		rebuild_cut.epoch = 80 + space;
		if (space) {
			wb_side_kind = RF_SIDE_ONLINE_OPERATION_SPACE;
			wb_space_valid = true;
			wb_space_contribution.result = identity;
			wb_space_contribution.page_mask = 3;
			wb_space_contribution.result_token[0] = 53;
			wb_space_contribution.result_token[1] = 41;
		}
		rebuild_fail_after = 2 * space;
		for (unsigned tick = 0; tick < 5; tick++) {
			UT_ASSERT(cluster_pi_rebuild_bgwriter_tick_v1());
			UT_ASSERT(!wb_inputs_pinned[0]);
			UT_ASSERT_EQ(rebuild_completions, 0);
			UT_ASSERT_EQ(wb_plan_builds, 1);
			UT_ASSERT_EQ(rebuild_apply_blocked, 1);
			UT_ASSERT_EQ(rebuild_logs, 1);
		}
		rebuild_fail_after = UINT32_MAX;
		UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
		UT_ASSERT_EQ(rebuild_completions, 1);
		UT_ASSERT_EQ(rebuild_applies, 7 + 2 * space);
		UT_ASSERT_EQ(wb_plan_builds, 1);
		UT_ASSERT(!wb_inputs_pinned[0]);
		pi_rebuild_release();
		cluster_page_data_receipt_free_v1(&data);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(retained_rebuild_plan_failure_waits_for_changed_root_without_rescan)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	rebuild_cut.epoch = 93;
	rebuild_plan_failure = RF_PAGE_PROOF_DETAIL_CAPACITY;
	for (unsigned tick = 0; tick < 4; tick++) {
		UT_ASSERT(cluster_pi_rebuild_bgwriter_tick_v1());
		UT_ASSERT_EQ(wb_plan_builds, 1);
		UT_ASSERT_EQ(rebuild_plan_blocked, 1);
		UT_ASSERT_EQ(rebuild_logs, 1);
		UT_ASSERT_EQ(rebuild_completions, 0);
		UT_ASSERT(!wb_inputs_pinned[0]);
	}
	/* A changed ROOT can shorten the retained window. Discard the failed
	 * scope; a fresh scope, never the failed object, must rebuild it. */
	wb_input_current = false;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT(pi_rebuild_job == NULL);
	wb_input_current = true;
	rebuild_plan_failure = RF_PAGE_PROOF_DETAIL_OK;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(wb_plan_builds, 2);
	UT_ASSERT_EQ(rebuild_completions, 1);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_rebuild_progress_continues_without_bgwriter_delay)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	wb_side_kind = RF_SIDE_ONLINE_OPERATION_XACT;
	wb_side_mapped = wb_space_valid = true;
	wb_space_count = 80;
	wb_space_contribution.result = identity;
	wb_space_contribution.page_mask = 3;
	wb_space_contribution.result_token[0] = 53;
	wb_space_contribution.result_token[1] = 41;
	/* 2 is runnable work, distinct from a peer/capacity wait (1). */
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_MORE);
	UT_ASSERT_EQ(rebuild_applies, 64);
	UT_ASSERT(!wb_inputs_pinned[0]);
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_MORE);
	UT_ASSERT_EQ(rebuild_applies, 128);
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_applies, 162);
	UT_ASSERT_EQ(rebuild_completions, 1);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_census_spill_retries_exact_pending_without_reading_or_rescanning)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	wb_side_kind = RF_SIDE_ONLINE_OPERATION_XACT;
	wb_side_mapped = wb_space_valid = true;
	wb_space_count = 80;
	wb_space_contribution.result = identity;
	wb_space_contribution.page_mask = 3;
	wb_space_contribution.result_token[0] = 53;
	wb_space_contribution.result_token[1] = 41;
	census_file_reads = 0;
	rebuild_fail_after = 64;
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_MORE);
	UT_ASSERT_EQ(census_file_open, 1);
	for (int i = 0; i < 4; i++) {
		UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_WAIT);
		UT_ASSERT_EQ(census_file_reads, 65);
		UT_ASSERT_EQ(wb_plan_builds, 1);
		UT_ASSERT_EQ(pi_rebuild_job->cursor, 64);
		UT_ASSERT(!wb_inputs_pinned[0]);
	}
	rebuild_fail_after = UINT32_MAX;
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_MORE);
	UT_ASSERT_EQ(cluster_pi_rebuild_bgwriter_tick_v1(), CLUSTER_PI_REBUILD_IDLE);
	UT_ASSERT_EQ(census_file_reads, 162);
	UT_ASSERT_EQ(census_file_open, 0);
	UT_ASSERT_EQ(rebuild_completions, 1);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(retained_rebuild_error_cleanup_and_postapply_root_check)
{
	ClusterPageDataReceiptV1 *data = rebuild_setup();
	volatile bool caught = false;
	wb_allocation_error = true;
	PG_TRY();
	{
		(void)cluster_pi_rebuild_bgwriter_tick_v1();
	}
	PG_CATCH();
	{
		caught = true;
		pi_rebuild_owner_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(pi_rebuild_job == NULL);
	UT_ASSERT(!wb_inputs_pinned[0]);
	rebuild_root_drift = true;
	UT_ASSERT(!cluster_pi_rebuild_bgwriter_tick_v1());
	UT_ASSERT_EQ(rebuild_holders, 7);
	UT_ASSERT_EQ(rebuild_completions, 0);
	UT_ASSERT_EQ(wb_input_releases, 1);
	UT_ASSERT(!wb_inputs_pinned[0]);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

int
main(void)
{
	UT_PLAN(24);
	UT_RUN(remote_physical_ack_runs_actual_ancestry_and_disposal);
	UT_RUN(writeback_codec_has_exact_bounded_authenticated_facts);
	UT_RUN(writeback_rejects_foreign_sender_boot_and_changed_master_cut);
	UT_RUN(writeback_wait_keeps_one_scope_and_stop_cancels_actual_owner);
	UT_RUN(writeback_never_acknowledges_missing_ancestry_or_busy_physical_pi);
	UT_RUN(writeback_reply_is_retired_and_retries_only_transport_refusal);
	UT_RUN(checkpointer_batch_reaches_actual_local_and_remote_physical_owners);
	UT_RUN(checkpointer_releases_pending_batch_before_native_checkpoint);
	UT_RUN(remote_batch_preserves_other_page_when_one_pi_retries);
	UT_RUN(checkpointer_batch_skips_local_retry_and_retires_other_page);
	UT_RUN(writeback_rejections_survive_batches_and_throttle_per_boot_epoch);
	UT_RUN(writeback_initial_allocation_error_cannot_orphan_running_owner);
	UT_RUN(retained_rebuild_never_completes_with_unmapped_side_contributions);
	UT_RUN(retained_census_does_not_materialize_history_and_checks_tail_first);
	UT_RUN(retained_rebuild_restores_all_original_writers_before_complete);
	UT_RUN(retained_rebuild_maps_both_space_pages_under_original_source);
	UT_RUN(retained_rebuild_maps_commit_drop_and_separate_non_pcm_owners);
	UT_RUN(retained_rebuild_waits_unpinned_and_rejects_incomplete_or_changed_cut);
	UT_RUN(retained_rebuild_root_change_or_apply_failure_never_completes);
	UT_RUN(retained_rebuild_apply_failure_retries_original_target_without_rescan);
	UT_RUN(retained_rebuild_plan_failure_waits_for_changed_root_without_rescan);
	UT_RUN(retained_rebuild_progress_continues_without_bgwriter_delay);
	UT_RUN(retained_census_spill_retries_exact_pending_without_reading_or_rescanning);
	UT_RUN(retained_rebuild_error_cleanup_and_postapply_root_check);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
