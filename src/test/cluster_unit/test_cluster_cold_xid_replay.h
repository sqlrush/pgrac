/* Real Startup adapter and accumulator; checkpoint/reader/decoder/drain
 * and completion owners are boundaries. This fixture never applies WAL,
 * publishes a ROOT, or mutates storage. */
#include <setjmp.h>
#include <stdarg.h>
#include "access/xlogreader.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_write_fence.h"

static ClusterColdTypedV1 replay_typed;
static ClusterWalRecoveryCheckpointV1 replay_checkpoints[3];
static uint64 replay_counts[3];
static uint32 replay_cursor[3];
static XLogReaderState replay_readers[3];
static DecodedXLogRecord replay_headers[3];
static ClusterColdRecordV1 replay_record;
static ClusterColdXidInputV1 replay_input;
static bool replay_checkpoint_ok, replay_decode_ok, replay_drain_ok;
static bool replay_decode_foreign;
static unsigned replay_drain_calls;
static uint8 replay_fence;
static unsigned replay_barrier_calls, replay_endpoint_calls;
static bool replay_endpoint_ok, replay_barrier_ok;
static bool replay_effects_ok;
static unsigned replay_effects_calls;

/* The original publish function must stop before handing off any failed cut. */
static bool replay_ready_ok, replay_fatal_armed;
static unsigned replay_ready_calls, replay_complete_calls, replay_external_calls;
static unsigned replay_fatal_calls, replay_log_calls;
static int replay_error_level, replay_sqlstate;
static ClusterRecoveryFenceCompleteV1 replay_complete_result;
static jmp_buf replay_fatal;
static char replay_events[16], replay_message[256];
static unsigned replay_event_count;

static void
replay_event(char event)
{
	if (replay_event_count + 1 >= sizeof(replay_events))
		abort();
	replay_events[replay_event_count++] = event;
	replay_events[replay_event_count] = '\0';
}

bool
errstart(int elevel, const char *domain)
{
	(void)domain;
	replay_error_level = elevel;
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int sqlerrcode)
{
	replay_sqlstate = sqlerrcode;
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(replay_message, sizeof(replay_message), fmt, args);
	va_end(args);
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

int
errdetail_log(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

int
errhint(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
	(void)filename;
	(void)lineno;
	(void)funcname;
	if (replay_error_level >= ERROR) {
		replay_fatal_calls++;
		if (!replay_fatal_armed)
			abort();
		longjmp(replay_fatal, 1);
	}
	replay_log_calls++;
}

uint16
cluster_recovery_merge_fence_plan_origin_count(const ClusterRecoveryFencePlan *plan)
{
	UT_ASSERT(plan == (const ClusterRecoveryFencePlan *)&replay_fence);
	return 2;
}

bool
cluster_cold_typed_effects_complete_v1(ClusterColdTypedV1 *typed,
									  const ClusterColdEffectsProofV1 *proof,
									  const ClusterColdReplayResultV1 *result)
{
	UT_ASSERT(typed == &replay_typed && proof != NULL && result != NULL);
	replay_event('E');
	replay_effects_calls++;
	return replay_effects_ok;
}

bool
cluster_cold_completion_ready_v1(const ClusterColdTypedV1 *typed,
								 const ClusterRecoveryFencePlan *fence,
								 const ClusterColdReplayResultV1 *result,
								 ClusterColdTouchedV1 *touched, uint16 *thread)
{
	UT_ASSERT(typed == &replay_typed);
	UT_ASSERT(fence == (const ClusterRecoveryFencePlan *)&replay_fence);
	UT_ASSERT(result != NULL && touched != NULL && thread != NULL);
	replay_event('C');
	replay_ready_calls++;
	*thread = 3;
	return replay_ready_ok;
}

ClusterRecoveryFenceCompleteV1
cluster_recovery_merge_fence_plan_complete_v1(ClusterRecoveryFencePlan *plan, uint16 *failed_thread,
											  int *failed_detail)
{
	UT_ASSERT(plan == (ClusterRecoveryFencePlan *)&replay_fence);
	UT_ASSERT(failed_thread != NULL && failed_detail != NULL);
	replay_event('H');
	replay_complete_calls++;
	*failed_thread = 3;
	*failed_detail = 17;
	return replay_complete_result;
}

void
cluster_write_fence_note_external_publish_gate_blocked(void)
{
	replay_event('F');
	replay_external_calls++;
}

bool
cluster_cold_durable_barrier_v1(const ClusterColdTouchedV1 *touched)
{
	UT_ASSERT(touched != NULL);
	replay_event('B');
	replay_barrier_calls++;
	return replay_barrier_ok;
}

bool
cluster_cold_typed_xid_endpoints_v1(ClusterColdTypedV1 *typed,
									struct ClusterRecoveryFencePlan *fence,
									const ClusterColdReplayResultV1 *result,
									const ClusterColdXidProgressV1 *progress)
{
	UT_ASSERT(typed == &replay_typed);
	UT_ASSERT(fence == (struct ClusterRecoveryFencePlan *)&replay_fence);
	UT_ASSERT(result != NULL && progress != NULL);
	UT_ASSERT_EQ(replay_barrier_calls, 1);
	replay_event('X');
	replay_endpoint_calls++;
	return replay_endpoint_ok;
}

static void
HandleStartupProcInterrupts(void)
{
}

bool
cluster_cold_typed_checkpoint_v1(const ClusterColdTypedV1 *typed, uint32 participant,
								 ClusterControlRootSnapshot *root,
								 ClusterWalRecoveryCheckpointV1 *checkpoint)
{
	(void)typed;
	memset(root, 0, sizeof(*root));
	memset(checkpoint, 0, sizeof(*checkpoint));
	if (!replay_checkpoint_ok || participant >= 3)
		return false;
	*checkpoint = replay_checkpoints[participant];
	return true;
}

uint64
cluster_cold_plan_replay_record_count_v1(const ClusterColdPlanV1 *plan, uint32 participant)
{
	(void)plan;
	return replay_counts[participant];
}

XLogReaderState *
cluster_cold_reader_next_v1(ClusterColdReaderV1 *reader, char **errormsg)
{
	uint32 i = (uint32)((uintptr_t)reader - 1);
	(void)errormsg;
	if (replay_cursor[i]++ >= replay_counts[i])
		return NULL;
	return &replay_readers[i];
}

ClusterColdDetailV1
cluster_cold_recovery_decode_v1(XLogReaderState *reader, uint64 system_identifier,
							   const uint8 storage_uuid[16], bool allow_zero_scn, bool foreign,
							   ClusterColdDecodedV1 *decoded)
{
	(void)system_identifier;
	(void)storage_uuid;
	(void)allow_zero_scn;
	replay_decode_foreign = foreign;
	memset(decoded, 0, sizeof(*decoded));
	decoded->record = replay_record;
	decoded->record.read_rec_ptr = reader->ReadRecPtr;
	decoded->record.end_rec_ptr = reader->EndRecPtr;
	decoded->xid_input = replay_input;
	return replay_decode_ok ? CLUSTER_COLD_OK : CLUSTER_COLD_OPCODE_UNSUPPORTED;
}

bool
cluster_cold_typed_source_drained_v1(ClusterColdTypedV1 *typed,
									 struct ClusterRecoveryFencePlan *fence, uint32 participant,
									 const ClusterColdReplayResultV1 *progress, uint64 records)
{
	(void)typed;
	(void)fence;
	(void)participant;
	(void)progress;
	(void)records;
	replay_drain_calls++;
	return replay_drain_ok;
}

#include "test_cluster_cold_xid_replay.inc"

static ClusterColdTypedReplay replay_adapter;
static struct ClusterRecoveryFencePlan *replay_fence_plan;

static void
replay_xid_reset(int count)
{
	memset(&replay_typed, 0, sizeof(replay_typed));
	memset(&replay_adapter, 0, sizeof(replay_adapter));
	memset(replay_cursor, 0, sizeof(replay_cursor));
	replay_typed.participant_count = 3;
	replay_typed.replay_count = 2;
	replay_typed.plan = (ClusterColdPlanV1 *)&replay_fence;
	replay_adapter.typed = &replay_typed;
	replay_adapter.fence_plan = &replay_fence_plan;
	replay_fence_plan = (struct ClusterRecoveryFencePlan *)&replay_fence;
	for (uint32 i = 0; i < 3; i++) {
		replay_typed.participants[i] = source(count);
		replay_typed.participants[i].thread_id = i + 1;
		replay_checkpoints[i].checkpoint.redo = 128;
		replay_checkpoints[i].checkpoint.nextXid = FullTransactionIdFromU64(full(8 + i, 100));
		replay_counts[i] = count;
		replay_adapter.readers[i] = (ClusterColdReaderV1 *)(uintptr_t)(i + 1);
		replay_readers[i].record = &replay_headers[i];
		replay_readers[i].ReadRecPtr = 128;
		replay_readers[i].EndRecPtr = 160;
		replay_headers[i].header.xl_crc = 17;
	}
	replay_record = record(0);
	memset(&replay_input, 0, sizeof(replay_input));
	replay_input.complete = true;
	replay_input.record_xid = 110;
	replay_input.payload_xid = 114;
	replay_checkpoint_ok = replay_decode_ok = replay_drain_ok = true;
	replay_drain_calls = 0;

	replay_barrier_calls = replay_endpoint_calls = 0;
	replay_effects_calls = 0;
	replay_effects_ok = true;
	replay_ready_calls = replay_complete_calls = replay_external_calls = 0;
	replay_fatal_calls = replay_log_calls = 0;
	replay_error_level = replay_sqlstate = 0;
	replay_ready_ok = replay_barrier_ok = replay_endpoint_ok = true;
	replay_fatal_armed = false;
	replay_complete_result = CLUSTER_RECOVERY_FENCE_COMPLETE_OK;
	replay_event_count = 0;
	replay_events[0] = replay_message[0] = '\0';
}

static ClusterColdReplayResultV1
replay_xid_consumed(uint32 participant)
{
	ClusterColdReplayResultV1 progress = { 0 };
	progress.last_read[participant] = 128;
	progress.last_end[participant] = 160;
	progress.last_crc[participant] = 17;
	return progress;
}

UT_TEST(test_replay_xid_original_seeds_and_reset)
{
	replay_xid_reset(1);
	replay_typed.xid_next_at_drain[0] = 99;
	replay_typed.effects_receipt.plan = replay_typed.plan;
	UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
	UT_ASSERT_NULL(replay_typed.effects_receipt.plan);
	for (uint32 i = 0; i < 3; i++) {
		UT_ASSERT_EQ(replay_adapter.xid_progress[i].next_xid, full(8 + i, 100));
		UT_ASSERT_EQ(replay_adapter.xid_progress[i].expected_records, 1);
		UT_ASSERT_EQ(replay_typed.xid_next_at_drain[i], 0);
	}
}

UT_TEST(test_replay_xid_native_read_fold_and_drain)
{
	replay_xid_reset(1);
	UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
	for (uint32 i = 0; i < 2; i++) {
		ClusterColdReplayRecordV1 identity = { 0 };
		ClusterColdReplayResultV1 progress = replay_xid_consumed(i);
		UT_ASSERT(cluster_typed_replay_next(&replay_adapter, i, &identity));
		UT_ASSERT_EQ(replay_decode_foreign, i != 0);
		UT_ASSERT_EQ(identity.record_crc, 17);
		UT_ASSERT_EQ(replay_adapter.xid_progress[i].next_xid, full(8 + i, 115));
		UT_ASSERT_EQ(replay_typed.xid_next_at_drain[i], 0);
		UT_ASSERT(cluster_typed_replay_source_drained(&replay_adapter, i, &progress, 1));
		UT_ASSERT_EQ(replay_typed.xid_next_at_drain[i], full(8 + i, 115));
	}
}

UT_TEST(test_replay_xid_no_endpoint_without_original_drain)
{
	ClusterColdReplayRecordV1 identity = { 0 };
	ClusterColdReplayResultV1 progress = replay_xid_consumed(0);
	replay_xid_reset(1);
	UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
	UT_ASSERT(cluster_typed_replay_next(&replay_adapter, 0, &identity));
	replay_drain_ok = false;
	UT_ASSERT(!cluster_typed_replay_source_drained(&replay_adapter, 0, &progress, 1));
	UT_ASSERT_EQ(replay_drain_calls, 1);
	UT_ASSERT_EQ(replay_typed.xid_next_at_drain[0], 0);
}

UT_TEST(test_replay_xid_count_and_consumed_identity)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterColdReplayRecordV1 identity = { 0 };
		ClusterColdReplayResultV1 progress = replay_xid_consumed(0);
		replay_xid_reset(1);
		UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
		if (fault != 0)
			UT_ASSERT(cluster_typed_replay_next(&replay_adapter, 0, &identity));
		if (fault == 1) progress.last_read[0]++;
		if (fault == 2) progress.last_end[0]++;
		UT_ASSERT(!cluster_typed_replay_source_drained(&replay_adapter, 0, &progress,
													 fault == 3 ? 0 : 1));
		UT_ASSERT_EQ(replay_typed.xid_next_at_drain[0], 0);
	}
}

UT_TEST(test_replay_xid_bad_record_cannot_publish)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterColdReplayRecordV1 identity = { 0 };
		ClusterColdReplayResultV1 progress = replay_xid_consumed(0);
		replay_xid_reset(1);
		UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
		if (fault == 0) replay_decode_ok = false;
		if (fault == 1) replay_input.complete = false;
		if (fault == 2) replay_typed.participants[0].owner_incarnation++;
		UT_ASSERT(!cluster_typed_replay_next(&replay_adapter, 0, &identity));
		UT_ASSERT(!cluster_typed_replay_source_drained(&replay_adapter, 0, &progress, 1));
		UT_ASSERT_EQ(replay_typed.xid_next_at_drain[0], 0);
	}
}

UT_TEST(test_replay_xid_history_cut_is_not_checkpoint_coverage)
{
	ClusterColdReplayResultV1 progress = { 0 };
	replay_xid_reset(0);
	replay_typed.participants[2].native_redo = 160;
	replay_typed.participants[2].tail_end = 160;
	UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
	UT_ASSERT(!replay_adapter.xid_progress[2].begun);
	UT_ASSERT(cluster_typed_replay_source_drained(&replay_adapter, 2, &progress, 0));
	UT_ASSERT_EQ(replay_typed.xid_next_at_drain[2], 0);
}

UT_TEST(test_replay_xid_empty_exact_checkpoint_cut)
{
	ClusterColdReplayResultV1 progress = { 0 };
	replay_xid_reset(0);
	UT_ASSERT(cluster_typed_replay_xid_begin(&replay_adapter));
	/* The real sequencer leaves result cursors zero when it consumes no
	 * records; only its private cursor starts at the source's redo cut. */
	UT_ASSERT(cluster_typed_replay_source_drained(&replay_adapter, 2, &progress, 0));
	UT_ASSERT_EQ(replay_typed.xid_next_at_drain[2], full(10, 100));
	progress.last_end[2] = 128; /* A fabricated consumed-record cursor refuses. */
	UT_ASSERT(!cluster_typed_replay_source_drained(&replay_adapter, 2, &progress, 0));
	UT_ASSERT_EQ(replay_typed.xid_next_at_drain[2], 0);
}

UT_TEST(test_replay_xid_bad_checkpoint_or_source)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		replay_xid_reset(1);
		replay_typed.xid_next_at_drain[1] = 99;
		if (fault == 0) replay_checkpoint_ok = false;
		if (fault == 1) replay_checkpoints[1].checkpoint.nextXid = FullTransactionIdFromU64(0);
		if (fault == 2) replay_checkpoints[1].checkpoint.redo++;
		UT_ASSERT(!cluster_typed_replay_xid_begin(&replay_adapter));
		UT_ASSERT_EQ(replay_typed.xid_next_at_drain[1], 0);
	}
}

UT_TEST(test_replay_endpoints_follow_durability_and_propagate_refusal)
{
	for (int barrier_ok = 0; barrier_ok <= 1; barrier_ok++)
		for (int endpoint_ok = 0; endpoint_ok <= 1; endpoint_ok++) {
			ClusterColdReplayResultV1 result = { 0 };
			replay_xid_reset(1);
			replay_barrier_calls = replay_endpoint_calls = 0;
			replay_barrier_ok = barrier_ok;
			replay_endpoint_ok = endpoint_ok;
			UT_ASSERT_EQ(cluster_typed_replay_durable_endpoints(&replay_adapter, &result),
						 barrier_ok && endpoint_ok);
			UT_ASSERT_EQ(replay_barrier_calls, 1);
			UT_ASSERT_EQ(replay_endpoint_calls, barrier_ok);
		}
}


UT_TEST(test_startup_publish_refuses_missing_effects_before_completion)
{
	ClusterColdReplayResultV1 result = { .detail = CLUSTER_COLD_REPLAY_OK };

	replay_xid_reset(1);
	replay_effects_ok = false;
	replay_fatal_armed = true;
	if (setjmp(replay_fatal) == 0) {
		cluster_typed_replay_publish(&replay_adapter, &result);
		UT_ASSERT(false);
	}
	replay_fatal_armed = false;
	UT_ASSERT_EQ(replay_error_level, FATAL);
	UT_ASSERT_EQ(replay_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(replay_effects_calls, 1);
	UT_ASSERT_EQ(replay_ready_calls, 0);
	UT_ASSERT_EQ(replay_barrier_calls, 0);
	UT_ASSERT_EQ(replay_endpoint_calls, 0);
	UT_ASSERT_EQ(replay_complete_calls, 0);
	UT_ASSERT(strcmp(replay_events, "E") == 0);
}

UT_TEST(test_startup_publish_stops_before_handoff_on_incomplete_inputs)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterColdReplayResultV1 result = { .detail = CLUSTER_COLD_REPLAY_OK };
		replay_xid_reset(1);
		if (fault == 0)
			replay_ready_ok = false;
		if (fault == 1)
			replay_barrier_ok = false;
		if (fault == 2)
			replay_endpoint_ok = false;
		replay_fatal_armed = true;
		if (setjmp(replay_fatal) == 0) {
			cluster_typed_replay_publish(&replay_adapter, &result);
			UT_ASSERT(false);
		}
		replay_fatal_armed = false;
		UT_ASSERT_EQ(replay_error_level, FATAL);
		UT_ASSERT_EQ(replay_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(replay_fatal_calls, 1);
		UT_ASSERT_EQ(replay_log_calls, 0);
		UT_ASSERT_EQ(replay_ready_calls, 1);
		UT_ASSERT_EQ(replay_barrier_calls, fault > 0);
		UT_ASSERT_EQ(replay_endpoint_calls, fault > 1);
		UT_ASSERT_EQ(replay_complete_calls, 0);
		UT_ASSERT_EQ(replay_external_calls, 0);
		UT_ASSERT(strcmp(replay_events, fault == 0 ? "EC" : fault == 1 ? "ECB" : "ECBX") == 0);
		UT_ASSERT(strstr(replay_message, "could not publish thread 3 recovered") != NULL);
	}
}

UT_TEST(test_startup_publish_propagates_all_completion_refusals)
{
	/* Every original completion failure, including unknown results, is fatal. */
	for (int failure = CLUSTER_RECOVERY_FENCE_COMPLETE_INVALID;
		 failure <= CLUSTER_RECOVERY_FENCE_COMPLETE_PIN_RELEASE + 1; failure++) {
		ClusterColdReplayResultV1 result = { .detail = CLUSTER_COLD_REPLAY_OK };
		replay_xid_reset(1);
		replay_complete_result = (ClusterRecoveryFenceCompleteV1)failure;
		replay_fatal_armed = true;
		if (setjmp(replay_fatal) == 0) {
			cluster_typed_replay_publish(&replay_adapter, &result);
			UT_ASSERT(false);
		}
		replay_fatal_armed = false;
		UT_ASSERT_EQ(replay_error_level, FATAL);
		UT_ASSERT_EQ(replay_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(replay_fatal_calls, 1);
		UT_ASSERT_EQ(replay_log_calls, 0);
		UT_ASSERT_EQ(replay_complete_calls, 1);
		UT_ASSERT_EQ(replay_external_calls, failure == CLUSTER_RECOVERY_FENCE_COMPLETE_AUTHORITY);
		UT_ASSERT(strcmp(replay_events,
						 failure == CLUSTER_RECOVERY_FENCE_COMPLETE_AUTHORITY ? "ECBXHF" : "ECBXH")
				  == 0);
		if (failure == CLUSTER_RECOVERY_FENCE_COMPLETE_IR_RELEASE)
			UT_ASSERT(strstr(replay_message, "serialization release was not confirmed") != NULL);
		else
			UT_ASSERT(strstr(replay_message, "could not publish thread 3 recovered") != NULL);
	}
}

UT_TEST(test_startup_publish_hands_off_only_after_native_durability_and_endpoints)
{
	ClusterColdReplayResultV1 result = { .detail = CLUSTER_COLD_REPLAY_OK };
	replay_xid_reset(1);
	cluster_typed_replay_publish(&replay_adapter, &result);
	UT_ASSERT_EQ(replay_ready_calls, 1);
	UT_ASSERT_EQ(replay_barrier_calls, 1);
	UT_ASSERT_EQ(replay_endpoint_calls, 1);
	UT_ASSERT_EQ(replay_complete_calls, 1);
	UT_ASSERT_EQ(replay_external_calls, 0);
	UT_ASSERT_EQ(replay_fatal_calls, 0);
	UT_ASSERT_EQ(replay_log_calls, 1);
	UT_ASSERT_EQ(replay_effects_calls, 1);
	UT_ASSERT(strcmp(replay_events, "ECBXH") == 0);
	UT_ASSERT(strstr(replay_message, "2 fenced writer generations published recovered") != NULL);
}
