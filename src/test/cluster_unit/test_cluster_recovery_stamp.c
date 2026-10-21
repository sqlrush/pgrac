/* Author: SqlRush <sqlrush@gmail.com>
 * Original shared slot/state machine. External authority and WAL/IO completion
 * are fixture boundaries; this is not a recovery or checkpoint field test. */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "common/cryptohash.h"
#include "../../backend/cluster/cluster_recovery_plan.c"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
/* Single-thread fixture: a contended slot means an illegal retained lock. */
int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	fprintf(stderr, "unexpected slot lock at %s:%d %s\n", file, line, func);
	abort();
}

bool IsUnderPostmaster = true;
bool cluster_enabled = true, cluster_shared_config = true;
BackendType MyBackendType = B_STARTUP;
int MyProcPid = 11;
volatile uint32 CritSectionCount;
static ClusterRecoveryPlanShmem region;
static bool found_region;
static ClusterRecoveryStampOwnerV1 owner;
static uint8 payload[CLUSTER_RECOVERY_TT_ABORT_BYTES];
static unsigned hash_calls;

/* Actual native accessor, with controlled process-local insertion leaves.
 * This does not execute XLogInsert or supply full raw-record/payload proof. */
XLogRecPtr ProcLastRecPtr, XactLastRecEnd;
static XLogRecPtr last_insert_record_end;
static bool begininsert_called;
static char *hdr_scratch;
static XLogRecord native_header;
static bool insert_allowed;
static TransactionId insert_xid;
static unsigned native_calls, native_stage, native_fault, native_throw;
static uint8 registered_payload[320];
static sigjmp_buf insert_error_boundary;

bool
XLogInsertAllowed(void)
{
	return insert_allowed;
}
TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return insert_xid;
}
static void
native_insert_boundary(unsigned stage)
{
	UT_ASSERT(CritSectionCount > 0);
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTING);
	for (unsigned i = 0; i <= CLUSTER_RECOVERY_STAMP_SLOTS; i++)
		UT_ASSERT(SpinLockFree(&region.thread_replay[i].stamp.lock));
	UT_ASSERT_EQ(native_stage + 1, stage);
	native_stage = stage;
	native_calls++;
	if (native_throw == stage)
		siglongjmp(insert_error_boundary, 1);
}
void
XLogBeginInsert(void)
{
	native_insert_boundary(1);
	UT_ASSERT(insert_allowed);
	UT_ASSERT(!begininsert_called);
	begininsert_called = true;
}
void
XLogRegisterData(char *data, uint32 length)
{
	native_insert_boundary(2);
	UT_ASSERT(begininsert_called);
	UT_ASSERT_EQ(length, 320);
	UT_ASSERT(memcmp(data, region.thread_replay[2].stamp.handle.payload, 320) == 0);
	memcpy(registered_payload, data, 320);
}
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	native_insert_boundary(3);
	UT_ASSERT(begininsert_called);
	UT_ASSERT_EQ(rmid, RM_CLUSTER_UNDO_ID);
	UT_ASSERT_EQ(info, 0xD0);
	memset(&native_header, 0, sizeof(native_header));
	native_header.xl_tot_len = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 320;
	native_header.xl_rmid = rmid;
	native_header.xl_info = info;
	hdr_scratch = (char *)&native_header;
	begininsert_called = false;
	ProcLastRecPtr = 0x2000018;
	XactLastRecEnd = last_insert_record_end = 0x2000413;
	switch (native_fault) {
	case 1: native_header.xl_rmid = RM_XLOG_ID; break;
	case 2: native_header.xl_xid = 100; break;
	case 3: return InvalidXLogRecPtr;
	case 4: last_insert_record_end++; break;
	case 5: ProcLastRecPtr = 0x1FFFFFF; break;
	case 6: native_header.xl_tot_len++; break;
	case 7: native_header.xl_info ^= 0x10; break;
	}
	return XactLastRecEnd;
}
#include "test_cluster_recovery_stamp_insert.inc"

/* Use the actual codec/provider. Allocation must be outside every original
 * slot lock and the insertion critical section, including observer calls. */
static pg_cryptohash_ctx *
stamp_hash_create(pg_cryptohash_type type)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	for (unsigned i = 0; i <= CLUSTER_RECOVERY_STAMP_SLOTS; i++)
		UT_ASSERT(SpinLockFree(&region.thread_replay[i].stamp.lock));
	hash_calls++;
	return pg_cryptohash_create(type);
}
#define pg_cryptohash_create stamp_hash_create
#include "../../backend/cluster/cluster_recovery_ctrc_record.c"
#undef pg_cryptohash_create

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s %s:%d\n", condition, file, line);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT(strcmp(name, "pgrac recovery plan") == 0);
	UT_ASSERT_EQ(size, MAXALIGN(sizeof(region)));
	*found = found_region;
	found_region = true;
	return &region;
}

static void
fresh(void)
{
	ClusterRecoveryTTAbortV1 record = { 0 };
	found_region = false;
	cluster_recovery_plan_shmem_init();
	MyBackendType = B_STARTUP;
	MyProcPid = 11;
	CritSectionCount = 0;
	IsUnderPostmaster = cluster_enabled = cluster_shared_config = true;
	ProcLastRecPtr = XactLastRecEnd = last_insert_record_end = InvalidXLogRecPtr;
	begininsert_called = false;
	hdr_scratch = NULL;
	memset(&native_header, 0, sizeof(native_header));
	insert_allowed = true;
	insert_xid = InvalidTransactionId;
	native_calls = native_stage = native_fault = native_throw = 0;
	memset(registered_payload, 0, sizeof(registered_payload));
	memset(&owner, 0, sizeof(owner));
	owner.writer.ref.claim.identity.system_identifier = 99;
	memset(owner.writer.ref.claim.identity.storage_uuid, 1, 16);
	memset(owner.writer.ref.claim.identity.authority_uuid, 2, 16);
	owner.writer.ref.claim.identity.origin_thread_id = 1;
	owner.writer.ref.claim.identity.origin_node_id = 0;
	owner.writer.ref.claim.identity.thread_claim_created_at = 100;
	owner.writer.ref.claim.identity.origin_owner_incarnation = 20;
	owner.writer.ref.claim.identity.root_lineage_seq = 2;
	owner.writer.ref.claim.database_incarnation = 8;
	owner.writer.ref.claim.max_config_generation = 3;
	memset(owner.writer.ref.claim.claim_sha256, 3, 32);
	owner.writer.ref.timeline = 1;
	owner.writer.epoch = 7;
	owner.writer.startup_first_lsn = 0x2000000;
	owner.target = owner.writer.ref.claim;
	owner.target.identity.origin_thread_id = 2;
	owner.target.identity.origin_node_id = 1;
	owner.target.identity.origin_owner_incarnation = 10;
	owner.target.identity.root_lineage_seq = 1;
	memset(owner.target.claim_sha256, 4, 32);
	owner.executor_pid = MyProcPid;
	owner.executor_boot = 20;
	owner.recovery_serial = 30;
	record.version = 1;
	record.length = sizeof(payload);
	record.flags = 1;
	record.target_thread = 2;
	record.writer_thread = 1;
	record.target_incarnation = 10;
	record.writer_incarnation = 20;
	record.recovery_serial = 30;
	record.target_epoch = 5;
	record.target_full_xid = record.top_full_xid = 100;
	memcpy(record.target_claim_sha256, owner.target.claim_sha256, 32);
	record.exact = (xl_undo_tt_slot_abort_exact){ .segment_id = 257,
												  .segment_generation = 0,
												  .xid = 100,
												  .slot_offset = 1,
												  .wrap = 1,
												  .instance = 2,
												  .format_version = 1 };
	record.before.xid = 100;
	record.before.wrap = 1;
	record.before.status = TT_SLOT_ACTIVE;
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&record, payload, sizeof(payload)));
}
static bool
begin(ClusterRecoveryStampHandleV1 *h)
{
	bool ok = cluster_recovery_stamp_prepare_v1(&owner, CLUSTER_RECOVERY_STAMP_C0, payload,
												sizeof(payload), h);
	UT_ASSERT(ok);
	return ok;
}
static void
insert(ClusterRecoveryStampHandleV1 *h)
{
	CritSectionCount = 1;
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(h));
	UT_ASSERT(cluster_recovery_stamp_inserted_v1(h, 0x2000010, 0x20000c0));
	CritSectionCount = 0;
}
UT_TEST(exact_lifecycle_and_owned_payload)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampCaptureV1 c;
	fresh();
	if (!begin(&h))
		return;
	UT_ASSERT_EQ(h.key.target_thread, 2);
	UT_ASSERT_EQ(h.key.slot_generation, 1);
	UT_ASSERT_EQ(h.key.window_seq, 1);
	UT_ASSERT_EQ(sizeof(region.thread_replay[2].stamp.handle.payload), 320);
	UT_ASSERT(memcmp(region.thread_replay[2].stamp.handle.payload, payload, sizeof(payload)) == 0);
	payload[0] = 0; /* caller buffer does not own the published descriptor */
	UT_ASSERT_EQ(region.thread_replay[2].stamp.handle.payload[0], 1);
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	UT_ASSERT_EQ(c.count, 1);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_PENDING);
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&h)); /* no critical section */
	UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
	insert(&h);
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT(cluster_recovery_stamp_durable_v1(&h));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_PENDING);
	UT_ASSERT(cluster_recovery_stamp_clear_v1(&h));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_COMPLETE);
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
}
UT_TEST(preinsert_cancel_capture_and_new_sequence)
{
	ClusterRecoveryStampHandleV1 h, next;
	ClusterRecoveryStampCaptureV1 c, later;
	fresh();
	if (!begin(&h))
		return;
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &next));
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
	if (!begin(&next))
		return;
	UT_ASSERT_EQ(next.key.slot_generation, h.key.slot_generation);
	UT_ASSERT_EQ(next.key.window_seq, h.key.window_seq + 1);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_COMPLETE);
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&later));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&later), CLUSTER_RECOVERY_STAMP_PENDING);
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&next));
	owner.executor_pid = MyProcPid = 12;
	if (!begin(&next))
		return;
	UT_ASSERT_EQ(next.key.slot_generation, h.key.slot_generation + 1);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&later), CLUSTER_RECOVERY_STAMP_COMPLETE);
}
UT_TEST(inserting_failure_exit_and_replay_reset_cannot_retire)
{
	ClusterRecoveryStampHandleV1 h, next;
	ClusterRecoveryStampCaptureV1 c;
	fresh();
	if (!begin(&h))
		return;
	CritSectionCount = 1;
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
	CritSectionCount = 0;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_stamp_inserted_v1(&h, 0x2000010, 0x20000c0));
	CritSectionCount = 0;
	owner.executor_pid = MyProcPid = 12; /* exit/replacement is not adoption */
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &next));
	pg_atomic_write_u32(&region.thread_replay[2].state, CLUSTER_THREADREC_REPLAY_IDLE);
	pg_atomic_write_u64(&region.thread_replay[2].episode_epoch, 999);
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_INVALID);
	cluster_recovery_plan_shmem_init(); /* attaching never resets responsibility */
	UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTING);
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(identity_role_kind_and_payload_refusal)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampOwnerV1 saved;
	fresh();
	saved = owner;
	for (int i = 0; i < 13; i++) {
		owner = saved;
		switch (i) {
		case 0:
			owner.executor_pid++;
			break;
		case 1:
			owner.executor_boot++;
			break;
		case 2:
			owner.recovery_serial++;
			break;
		case 3:
			owner.target.claim_sha256[31]++;
			break;
		case 4:
			owner.target.identity.origin_owner_incarnation++;
			break;
		case 5:
			owner.writer.ref.claim.identity.origin_owner_incarnation++;
			break;
		case 6:
			owner.target.database_incarnation++;
			break;
		case 7:
			owner.target.identity.storage_uuid[15]++;
			break;
		case 8:
			owner.target.identity.system_identifier++;
			break;
		case 9:
			owner.writer.epoch = 0;
			break;
		case 10:
			owner.writer.startup_first_lsn = 0;
			break;
		case 11:
			owner.target.identity.reserved42 = 1;
			break;
		case 12:
			owner.target.identity.authority_uuid[15]++;
			break;
		}
		memset(&h, 0x55, sizeof(h));
		UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &h));
		UT_ASSERT_EQ(h.key.window_seq, 0);
	}
	owner = saved;
	for (int kind = 0; kind < 5; kind++)
		if (kind != 1)
			UT_ASSERT(
				!cluster_recovery_stamp_prepare_v1(&owner, kind, payload, sizeof(payload), &h));
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload) - 1, &h));
	payload[0] = 99;
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &h));
	payload[0] = 1;
	MyBackendType = B_BACKEND;
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &h));
	MyBackendType = B_BG_WORKER;
	if (!begin(&h))
		return;
	MyProcPid++;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	MyProcPid--;
	h.owner.writer.ref.claim.claim_sha256[31]++;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
}
UT_TEST(lsn_boundaries_and_sticky_postinsert_failure)
{
	ClusterRecoveryStampHandleV1 h;
	fresh();
	if (!begin(&h))
		return;
	CritSectionCount = 1;
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_inserted_v1(&h, 0, 1));
	UT_ASSERT(!cluster_recovery_stamp_inserted_v1(&h, 1, 2));
	UT_ASSERT(!cluster_recovery_stamp_inserted_v1(&h, 0x2000010, 0x2000010));
	UT_ASSERT(cluster_recovery_stamp_inserted_v1(&h, 0x2000010, 0x20000c0));
	CritSectionCount = 0;
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
}
UT_TEST(capture_shape_overflow_and_corruption_fail_closed)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampCaptureV1 c;
	fresh();
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	UT_ASSERT_EQ(c.count, 0);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_COMPLETE);
	if (!begin(&h))
		return;
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	c.keys[0].window_seq++;
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_INVALID);
	c.keys[0] = h.key;
	c.count = 129;
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_INVALID);
	c.count = 2;
	c.keys[1] = h.key;
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_INVALID);
	c.captured = false;
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_INVALID);
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
	region.thread_replay[2].stamp.handle.key.window_seq = UINT64_MAX;
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &h));
	fresh();
	if (!begin(&h))
		return;
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
	region.thread_replay[2].stamp.handle.key.slot_generation = UINT64_MAX;
	owner.executor_pid = MyProcPid = 12;
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &h));
	region.thread_replay[128].stamp.phase = 99;
	UT_ASSERT(!cluster_recovery_stamp_capture_v1(&c));
	UT_ASSERT(!c.captured);
	cluster_recovery_plan_shmem = NULL;
	UT_ASSERT(!cluster_recovery_stamp_capture_v1(&c));
}
UT_TEST(one_serial_executor_and_all_original_slots)
{
	ClusterRecoveryStampHandleV1 handles[128], extra;
	ClusterRecoveryStampCaptureV1 c;
	ClusterRecoveryTTAbortV1 record;
	fresh();
	if (!begin(&handles[0]))
		return;
	UT_ASSERT(cluster_recovery_tt_abort_decode_v1(payload, sizeof(payload), &record));
	record.target_thread = record.exact.instance = 3;
	record.exact.segment_id = 513;
	owner.target.identity.origin_thread_id = 3;
	owner.target.identity.origin_node_id = 2;
	UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&record, payload, sizeof(payload)));
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &extra));
	fresh();
	UT_ASSERT(cluster_recovery_tt_abort_decode_v1(payload, sizeof(payload), &record));
	for (uint16 thread = 1; thread <= 128; thread++) {
		MyProcPid = owner.executor_pid = 100 + thread;
		record.target_thread = record.exact.instance = thread;
		record.exact.segment_id = (thread - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		owner.target.identity.origin_thread_id = thread;
		owner.target.identity.origin_node_id = thread - 1;
		UT_ASSERT(cluster_recovery_tt_abort_encode_v1(&record, payload, sizeof(payload)));
		if (!begin(&handles[thread - 1]))
			return;
	}
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	UT_ASSERT_EQ(c.count, 128);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_PENDING);
	for (uint16 thread = 1; thread <= 128; thread++) {
		MyProcPid = 100 + thread;
		UT_ASSERT(cluster_recovery_stamp_cancel_v1(&handles[thread - 1]));
	}
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_COMPLETE);
	fresh();
	if (!begin(&extra))
		return;
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&extra));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&extra));
	CritSectionCount = 0;
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&extra));
}
UT_TEST(normal_stop_observes_windows_separately)
{
	ClusterRecoveryStampHandleV1 h;
	const char *domain, *reason;
	uint64 key;
	fresh();
	if (!begin(&h))
		return;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(key, 2);
	UT_ASSERT(strcmp(domain, "RECOVERY_STAMP") == 0);
	MyBackendType = B_STARTUP;
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}
static bool
begin_d0(ClusterRecoveryStampHandleV1 *h, uint8 terminal)
{
	ClusterRecoveryCTRCReleaseV2 r = { 0 };
	uint8 bytes[CLUSTER_RECOVERY_CTRC_BYTES];
	r.version = CLUSTER_RECOVERY_CTRC_VERSION;
	r.length = sizeof(bytes);
	r.flags = CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN;
	r.root_id = 100;
	r.root_generation = 200;
	r.formation_epoch = owner.writer.epoch;
	r.recovery_serial = owner.recovery_serial;
	r.database_incarnation = owner.target.database_incarnation;
	r.writer_boot = owner.executor_boot;
	r.covered_history = 4;
	r.highest_fpi_end = 0x10000;
	r.cohort_bitmap = 3;
	r.origin_node_id = owner.target.identity.origin_node_id;
	r.writer_thread = owner.writer.ref.claim.identity.origin_thread_id;
	r.target_owner = owner.target.identity.origin_thread_id;
	r.segment_id = (r.target_owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	r.slot_offset = 1;
	r.wrap = 1;
	r.xid = 100;
	r.terminal_outcome = terminal;
	r.before = (TTSlot){ .xid = r.xid,
						 .wrap = r.wrap,
						 .status = terminal,
						 .commit_scn = terminal == TT_SLOT_COMMITTED ? 11 : 0 };
	r.after = r.before;
	r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	memset(r.graph_sha256, 5, 32);
	memset(r.input_sha256, 6, 32);
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&r, owner.writer.ref.claim.identity.system_identifier,
											  bytes, sizeof(bytes)));
	return cluster_recovery_stamp_prepare_v1(&owner, CLUSTER_RECOVERY_STAMP_D0, bytes,
											 sizeof(bytes), h);
}

UT_TEST(d0_commit_abort_lifecycle_and_later_window)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h, next;
		ClusterRecoveryStampCaptureV1 c;
		unsigned before;
		fresh();
		if (!begin_d0(&h, outcome)) {
			UT_ASSERT(false);
			return;
		}
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_PENDING);
		before = hash_calls;
		insert(&h);
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT_EQ(hash_calls, before);
		UT_ASSERT(begin(&next));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_COMPLETE);
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_cancel_v1(&next));
	}
}

UT_TEST(d0_sticky_failure_and_normal_stop)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampCaptureV1 c;
	fresh();
	if (!begin_d0(&h, TT_SLOT_ABORTED)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&c));
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	MyBackendType = B_STARTUP;
	insert(&h);
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&c), CLUSTER_RECOVERY_STAMP_INVALID);
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(d0_owner_correspondence_and_preinsert_cancel)
{
	ClusterRecoveryStampHandleV1 h, refused;
	ClusterRecoveryStampOwnerV1 changed;
	uint8 bytes[CLUSTER_RECOVERY_CTRC_BYTES];
	fresh();
	if (!begin_d0(&h, TT_SLOT_COMMITTED)) {
		UT_ASSERT(false);
		return;
	}
	memcpy(bytes, h.payload, sizeof(bytes));
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
	for (unsigned i = 0; i < 7; i++) {
		changed = owner;
		switch (i) {
		case 0:
			changed.writer.ref.claim.identity.origin_thread_id = 3;
			changed.writer.ref.claim.identity.origin_node_id = 2;
			break;
		case 1:
			changed.writer.ref.claim.identity.origin_owner_incarnation++;
			changed.executor_boot++;
			break;
		case 2:
			changed.target.identity.origin_thread_id = 3;
			changed.target.identity.origin_node_id = 2;
			break;
		case 3:
			changed.target.database_incarnation++;
			changed.writer.ref.claim.database_incarnation++;
			break;
		case 4:
			changed.writer.epoch++;
			break;
		case 5:
			changed.recovery_serial++;
			break;
		case 6:
			changed.writer.ref.claim.identity.system_identifier++;
			changed.target.identity.system_identifier++;
			break;
		}
		UT_ASSERT(stamp_owner_valid(&changed));
		memset(&refused, 0xa5, sizeof(refused));
		UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&changed, CLUSTER_RECOVERY_STAMP_D0, bytes,
													 sizeof(bytes), &refused));
		for (unsigned j = 0; j < sizeof(refused); j++)
			UT_ASSERT_EQ(((uint8 *)&refused)[j], 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	}
	UT_ASSERT(cluster_recovery_stamp_prepare_v1(&owner, CLUSTER_RECOVERY_STAMP_D0, bytes,
												sizeof(bytes), &h));
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
}

UT_TEST(prepared_operation_is_exact_and_observers_validate_unlocked)
{
	ClusterRecoveryStampHandleV1 h, altered;
	ClusterRecoveryStampCaptureV1 capture, refused;
	ClusterRecoveryStampSlotV1 *slot;
	unsigned before;
	fresh();
	if (!begin_d0(&h, TT_SLOT_ABORTED)) {
		UT_ASSERT(false);
		return;
	}
	slot = &region.thread_replay[2].stamp;
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
	before = hash_calls;
	CritSectionCount = 1;
	for (unsigned i = 0; i < sizeof(h.payload); i++) {
		altered = h;
		altered.payload[i] ^= 1;
		UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&altered));
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
	}
	altered = h;
	altered.kind = CLUSTER_RECOVERY_STAMP_FPI;
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&altered));
	altered = h;
	altered.payload_length--;
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&altered));
	UT_ASSERT_EQ(hash_calls, before);
	/* Observer misuse in a critical section refuses before allocating SHA. */
	UT_ASSERT(!cluster_recovery_stamp_capture_v1(&refused));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_INVALID);
	UT_ASSERT_EQ(hash_calls, before);
	CritSectionCount = 0;
	slot->handle.payload[100] ^= 1;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_capture_v1(&refused));
	UT_ASSERT(!refused.captured && refused.count == 0);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_INVALID);
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_recovery_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	MyBackendType = B_STARTUP;
	slot->handle.payload[100] ^= 1;
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT(begin(&h));
	altered = h;
	altered.payload[sizeof(altered.payload) - 1] = 1;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&altered));
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&h));
}

static void
native_d0_insert(XLogRecPtr start, XLogRecPtr end)
{
	memset(&native_header, 0, sizeof(native_header));
	native_header.xl_tot_len
		= SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES;
	native_header.xl_rmid = RM_CLUSTER_UNDO_ID;
	native_header.xl_info = CLUSTER_RECOVERY_CTRC_WAL_INFO;
	hdr_scratch = (char *)&native_header;
	begininsert_called = false;
	cluster_shared_config = true;
	ProcLastRecPtr = start;
	XactLastRecEnd = last_insert_record_end = end;
}

UT_TEST(d0_native_insert_actual_positions_and_outcomes)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampSlotV1 *slot;
		unsigned before;
		fresh();
		if (!begin_d0(&h, outcome)) {
			UT_ASSERT(false);
			return;
		}
		slot = &region.thread_replay[2].stamp;
		/* Deliberately not start+raw_length: WAL page headers/alignment are
		 * the original inserter's concern, never guessed by the adapter. */
		native_d0_insert(0x2000018, 0x2000413);
		before = hash_calls;
		CritSectionCount = 1;
		UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(slot->record_start, ProcLastRecPtr);
		UT_ASSERT_EQ(slot->record_end, XactLastRecEnd);
		UT_ASSERT(memcmp(&slot->handle, &h, sizeof(h)) == 0);
		UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
		CritSectionCount = 0;
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT_EQ(hash_calls, before);
	}
}

UT_TEST(d0_native_insert_context_and_position_refusal)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampSlotV1 *slot;
	ClusterRecoveryStampSlotV1 saved;
	unsigned before;
	fresh();
	if (!begin_d0(&h, TT_SLOT_ABORTED)) {
		UT_ASSERT(false);
		return;
	}
	slot = &region.thread_replay[2].stamp;
	native_d0_insert(0x2000018, 0x2000413);
	UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
	saved = *slot;
	before = hash_calls;
	for (unsigned i = 0; i < 11; i++) {
		XLogRecPtr end = 0x2000413;
		native_d0_insert(0x2000018, end);
		CritSectionCount = 1;
		switch (i) {
		case 0:
			CritSectionCount = 0;
			break;
		case 1:
			cluster_shared_config = false;
			break;
		case 2:
			begininsert_called = true;
			break;
		case 3:
			hdr_scratch = NULL;
			break;
		case 4:
			end = InvalidXLogRecPtr;
			break;
		case 5:
			last_insert_record_end++;
			break;
		case 6:
			XactLastRecEnd++;
			break;
		case 7:
			ProcLastRecPtr = InvalidXLogRecPtr;
			break;
		case 8:
			ProcLastRecPtr = end;
			break;
		case 9:
			ProcLastRecPtr = end + 1;
			break;
		case 10:
			ProcLastRecPtr = owner.writer.startup_first_lsn - 1;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&h, end));
		UT_ASSERT(memcmp(slot, &saved, sizeof(saved)) == 0);
	}
	UT_ASSERT_EQ(hash_calls, before);
	CritSectionCount = 0;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
}

UT_TEST(d0_native_insert_exact_header_refusal)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampSlotV1 *slot;
	ClusterRecoveryStampSlotV1 saved;
	unsigned before;
	fresh();
	if (!begin_d0(&h, TT_SLOT_COMMITTED)) {
		UT_ASSERT(false);
		return;
	}
	slot = &region.thread_replay[2].stamp;
	CritSectionCount = 1;
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
	saved = *slot;
	before = hash_calls;
	for (unsigned i = 0; i < 7; i++) {
		native_d0_insert(0x2000018, 0x2000413);
		switch (i) {
		case 0:
			native_header.xl_rmid = RM_XLOG_ID;
			break;
		case 1:
			native_header.xl_info ^= 0x10;
			break;
		case 2:
			native_header.xl_info |= XLR_SPECIAL_REL_UPDATE;
			break;
		case 3:
			native_header.xl_xid = 100;
			break;
		case 4:
			native_header.xl_tot_len--;
			break;
		case 5:
			native_header.xl_tot_len++;
			break;
		case 6:
			native_header.xl_tot_len = 0;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
		UT_ASSERT(memcmp(slot, &saved, sizeof(saved)) == 0);
	}
	native_d0_insert(0x2000018, 0x2000413);
	UT_ASSERT(cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
	UT_ASSERT_EQ(hash_calls, before);
	CritSectionCount = 0;
}

UT_TEST(d0_native_insert_exact_window_and_sticky_refusal)
{
	ClusterRecoveryStampHandleV1 h, changed;
	ClusterRecoveryStampSlotV1 *slot;
	ClusterRecoveryStampSlotV1 saved;
	unsigned before;
	fresh();
	if (!begin_d0(&h, TT_SLOT_ABORTED)) {
		UT_ASSERT(false);
		return;
	}
	slot = &region.thread_replay[2].stamp;
	CritSectionCount = 1;
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
	native_d0_insert(0x2000018, 0x2000413);
	saved = *slot;
	before = hash_calls;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(NULL, XactLastRecEnd));
	for (unsigned i = 0; i < 7; i++) {
		changed = h;
		switch (i) {
		case 0:
			changed.kind = CLUSTER_RECOVERY_STAMP_C0;
			break;
		case 1:
			changed.payload_length--;
			break;
		case 2:
			changed.key.window_seq++;
			break;
		case 3:
			changed.owner.recovery_serial++;
			break;
		case 4:
			changed.payload[319] ^= 1;
			break;
		case 5:
			MyProcPid++;
			break;
		case 6:
			MyBackendType = B_LMON;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&changed, XactLastRecEnd));
		UT_ASSERT(memcmp(slot, &saved, sizeof(saved)) == 0);
		MyProcPid = owner.executor_pid;
		MyBackendType = B_STARTUP;
	}
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	saved = *slot;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_inserted_v2(&h, XactLastRecEnd));
	UT_ASSERT(memcmp(slot, &saved, sizeof(saved)) == 0);
	UT_ASSERT_EQ(hash_calls, before);
	CritSectionCount = 0;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
}

/* The original adapter/codec/window/native accessor are real. The three
 * native insertion leaves below the adapter are controlled, not disk WAL. */
UT_TEST(d0_emitter_exact_payload_positions_and_single_insert)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampSlotV1 *slot;
		unsigned before;
		fresh();
		UT_ASSERT(begin_d0(&h, outcome));
		slot = &region.thread_replay[2].stamp;
		if (outcome == TT_SLOT_ABORTED)
			MyBackendType = B_BG_WORKER;
		before = hash_calls;
		CritSectionCount = 1;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_v2(&h), 0x2000413);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(native_stage, 3);
		UT_ASSERT(memcmp(registered_payload, h.payload, 320) == 0);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(slot->record_start, 0x2000018);
		UT_ASSERT_EQ(slot->record_end, 0x2000413);
		UT_ASSERT(!slot->failed);
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_v2(&h), InvalidXLogRecPtr);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(hash_calls, before);
		CritSectionCount = 0;
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_clear_v1(&h));
	}
}

UT_TEST(d0_emitter_preinsert_refusal_is_atomic)
{
	for (unsigned i = 0; i < 12; i++) {
		ClusterRecoveryStampHandleV1 h, changed;
		ClusterRecoveryStampSlotV1 saved;
		unsigned before;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_ABORTED));
		changed = h;
		CritSectionCount = 1;
		switch (i) {
		case 0: CritSectionCount = 0; break;
		case 1: changed.kind = CLUSTER_RECOVERY_STAMP_C0; break;
		case 2: changed.payload_length--; break;
		case 3: changed.payload[319] ^= 1; break;
		case 4: changed.key.window_seq++; break;
		case 5: changed.owner.recovery_serial++; break;
		case 6: MyProcPid++; break;
		case 7: MyBackendType = B_LMON; break;
		case 8: insert_allowed = false; break;
		case 9: insert_xid = 100; break;
		case 10: UT_ASSERT(cluster_recovery_stamp_fail_v1(&h)); break;
		case 11: cluster_shared_config = false; break;
		}
		saved = region.thread_replay[2].stamp;
		before = hash_calls;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_v2(&changed), InvalidXLogRecPtr);
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_v2(NULL), InvalidXLogRecPtr);
		UT_ASSERT_EQ(native_calls, 0);
		UT_ASSERT_EQ(hash_calls, before);
		UT_ASSERT(memcmp(&region.thread_replay[2].stamp, &saved, sizeof(saved)) == 0);
	}
	CritSectionCount = 0;
}

UT_TEST(d0_emitter_postinsert_refusal_retains_failed_window)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampCaptureV1 capture;
		ClusterRecoveryStampSlotV1 *slot;
		unsigned before;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_COMMITTED));
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		slot = &region.thread_replay[2].stamp;
		native_fault = fault;
		before = hash_calls;
		CritSectionCount = 1;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_v2(&h), InvalidXLogRecPtr);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTING);
		UT_ASSERT(slot->failed);
		UT_ASSERT(memcmp(&slot->handle, &h, sizeof(h)) == 0);
		UT_ASSERT_EQ(hash_calls, before);
		CritSectionCount = 0;
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_INVALID);
	}
}

UT_TEST(d0_emitter_native_error_cannot_cancel_or_complete)
{
	for (unsigned stage = 1; stage <= 3; stage++) {
		ClusterRecoveryStampHandleV1 h;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_ABORTED));
		native_throw = stage;
		CritSectionCount = 1;
		if (sigsetjmp(insert_error_boundary, 0) == 0) {
			(void)cluster_recovery_stamp_ctrc_insert_v2(&h);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTING);
			UT_ASSERT(memcmp(&region.thread_replay[2].stamp.handle, &h, sizeof(h)) == 0);
			CritSectionCount = 0;
			UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
			UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
			UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		}
	}
	CritSectionCount = 0;
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(exact_lifecycle_and_owned_payload);
	UT_RUN(preinsert_cancel_capture_and_new_sequence);
	UT_RUN(inserting_failure_exit_and_replay_reset_cannot_retire);
	UT_RUN(identity_role_kind_and_payload_refusal);
	UT_RUN(lsn_boundaries_and_sticky_postinsert_failure);
	UT_RUN(capture_shape_overflow_and_corruption_fail_closed);
	UT_RUN(one_serial_executor_and_all_original_slots);
	UT_RUN(normal_stop_observes_windows_separately);
	UT_RUN(d0_commit_abort_lifecycle_and_later_window);
	UT_RUN(d0_sticky_failure_and_normal_stop);
	UT_RUN(d0_owner_correspondence_and_preinsert_cancel);
	UT_RUN(prepared_operation_is_exact_and_observers_validate_unlocked);
	UT_RUN(d0_native_insert_actual_positions_and_outcomes);
	UT_RUN(d0_native_insert_context_and_position_refusal);
	UT_RUN(d0_native_insert_exact_header_refusal);
	UT_RUN(d0_native_insert_exact_window_and_sticky_refusal);
	UT_RUN(d0_emitter_exact_payload_positions_and_single_insert);
	UT_RUN(d0_emitter_preinsert_refusal_is_atomic);
	UT_RUN(d0_emitter_postinsert_refusal_retains_failed_window);
	UT_RUN(d0_emitter_native_error_cannot_cancel_or_complete);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
