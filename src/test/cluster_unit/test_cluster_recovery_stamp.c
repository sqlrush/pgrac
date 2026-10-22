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
#include "access/xlog_internal.h"
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
static union {
	uint64 align;
	char bytes[SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong];
} native_scratch;
static XLogRecData hdr_rdt, native_data[3];
static int max_rdatas = 3, max_registered_buffers;
static bool insert_allowed;
static TransactionId insert_xid;
static unsigned native_calls, native_stage, native_fault, native_throw;
static uint8 registered_payload[320];
static sigjmp_buf insert_error_boundary;
static unsigned flush_calls, writer_checks, cover_calls, writer_reject_on, flush_fault;
static XLogRecPtr flush_seen, covered_end;
static TimeLineID covered_timeline;
static bool cover_enabled;
/* Storage is an external leaf here; the real held-FD entry is independently
 * exercised against native temporary undo files in recovery_tt_io. */
static void flush_unlocked(void);
static unsigned target_calls, target_fault;
static uint8 target_payload[320];
static ClusterUndoSmgrRecoveryStampV1 target_stamp = { .token = 37 };
static const uint8 *apply_raw, *apply_start;

static void
apply_outputs_unpublished(void)
{
	XLogRecPtr start;
	if (apply_raw == NULL)
		return;
	for (unsigned i = 0; i < SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 320; i++)
		UT_ASSERT_EQ(apply_raw[i], 0xA5);
	memcpy(&start, apply_start, sizeof(start));
	UT_ASSERT_EQ(start, 91);
}

bool
cluster_undo_smgr_recovery_ctrc_write_bound_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
											   const uint8 *bytes, size_t length)
{
	ClusterRecoveryStampSlotV1 *slot = &region.thread_replay[2].stamp;
	flush_unlocked();
	apply_outputs_unpublished();
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(cover_calls, 1);
	UT_ASSERT_EQ(writer_checks, 2);
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT_EQ(length, sizeof(target_payload));
	UT_ASSERT(memcmp(bytes, target_payload, sizeof(target_payload)) == 0);
	target_calls++;
	if (stamp.token != target_stamp.token)
		return false;
	switch (target_fault) {
	case 1:
		return false;
	case 2:
		siglongjmp(insert_error_boundary, 1);
		break;
	case 3:
		slot->record_end++;
		break;
	case 4:
		slot->record_start++;
		break;
	case 5:
		slot->handle.payload[319] ^= 1;
		break;
	case 6:
		slot->handle.key.window_seq++;
		break;
	case 7:
		insert_allowed = false;
		break;
	case 8:
		writer_reject_on = writer_checks + 1;
		break;
	case 9:
		covered_end--;
		break;
	case 10:
		slot->failed = true;
		break;
	}
	return true;
}

static void
flush_unlocked(void)
{
	UT_ASSERT(CritSectionCount > 0);
	for (unsigned i = 0; i <= CLUSTER_RECOVERY_STAMP_SLOTS; i++)
		UT_ASSERT(SpinLockFree(&region.thread_replay[i].stamp.lock));
}

/* These are original writer/native WAL leaves, not graph or IO authority.
 * The real startup accessor is separately exercised in its existing suite. */
ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *writer)
{
	flush_unlocked();
	writer_checks++;
	UT_ASSERT(memcmp(writer, &owner.writer, sizeof(*writer)) == 0);
	return writer_checks == writer_reject_on ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
											 : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
XLogFlush(XLogRecPtr end)
{
	ClusterRecoveryStampSlotV1 *slot = &region.thread_replay[2].stamp;
	flush_unlocked();
	apply_outputs_unpublished();
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT_EQ(end, slot->record_end);
	flush_calls++;
	flush_seen = end;
	switch (flush_fault) {
	case 1:
		siglongjmp(insert_error_boundary, 1);
		break;
	case 2:
		slot->record_end++;
		break;
	case 3:
		slot->record_start++;
		break;
	case 4:
		slot->handle.payload[319] ^= 1;
		break;
	case 5:
		slot->handle.key.window_seq++;
		break;
	}
}

bool
ClusterXLogStartupFlushCovers(XLogRecPtr end, TimeLineID timeline)
{
	flush_unlocked();
	UT_ASSERT_EQ(MyBackendType, B_STARTUP);
	UT_ASSERT_EQ(end, flush_seen);
	UT_ASSERT_EQ(timeline, owner.writer.ref.timeline);
	cover_calls++;
	return cover_enabled && covered_end >= end && covered_timeline == timeline;
}

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
	hdr_scratch = native_scratch.bytes;
	memcpy(hdr_scratch, &native_header, SizeOfXLogRecord);
	((uint8 *) hdr_scratch)[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_LONG;
	memcpy(hdr_scratch + SizeOfXLogRecord + 1, &((uint32){320}), sizeof(uint32));
	hdr_rdt = (XLogRecData){ .data = hdr_scratch, .len = sizeof(native_scratch.bytes),
		.next = &native_data[0] };
	native_data[0] = (XLogRecData){ .data = (char *)registered_payload, .len = 320 };
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
	case 8: registered_payload[319] ^= 1; break;
	case 9: ((uint8 *) hdr_scratch)[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_SHORT; break;
	case 10: hdr_scratch[SizeOfXLogRecord + 1] ^= 1; break;
	case 11: hdr_rdt.next = NULL; break;
	case 12: native_data[0].next = &native_data[0]; break;
	case 13: region.thread_replay[2].stamp.handle.key.window_seq++; break;
	}
	native_header.xl_prev = 0x1FFFFF0;
	INIT_CRC32C(native_header.xl_crc);
	COMP_CRC32C(native_header.xl_crc, hdr_scratch + SizeOfXLogRecord,
		SizeOfXLogRecordDataHeaderLong);
	COMP_CRC32C(native_header.xl_crc, registered_payload, sizeof(registered_payload));
	COMP_CRC32C(native_header.xl_crc, &native_header, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(native_header.xl_crc);
	memcpy(hdr_scratch, &native_header, SizeOfXLogRecord);
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
	memset(&native_scratch, 0, sizeof(native_scratch));
	memset(&hdr_rdt, 0, sizeof(hdr_rdt));
	memset(native_data, 0, sizeof(native_data));
	max_rdatas = 3;
	max_registered_buffers = 0;
	insert_allowed = true;
	insert_xid = InvalidTransactionId;
	native_calls = native_stage = native_fault = native_throw = 0;
	flush_calls = writer_checks = cover_calls = writer_reject_on = flush_fault = 0;
	flush_seen = InvalidXLogRecPtr;
	covered_end = 0x2000413;
	covered_timeline = 1;
	cover_enabled = true;
	target_calls = target_fault = 0;
	apply_raw = apply_start = NULL;
	memset(target_payload, 0, sizeof(target_payload));
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

static bool
flush_window(ClusterRecoveryStampHandleV1 *h, uint8 outcome)
{
	fresh();
	if (!begin_d0(h, outcome))
		return false;
	CritSectionCount = 1;
	return cluster_recovery_stamp_ctrc_insert_v2(h) == 0x2000413;
}

UT_TEST(d0_flush_exact_end_preserves_target_duty)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampSlotV1 saved;
		ClusterRecoveryStampCaptureV1 capture;
		unsigned before;
		UT_ASSERT(flush_window(&h, outcome));
		saved = region.thread_replay[2].stamp;
		before = hash_calls;
		UT_ASSERT(cluster_recovery_stamp_ctrc_flush_v2(&h));
		UT_ASSERT_EQ(flush_seen, 0x2000413);
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(writer_checks, 2);
		UT_ASSERT_EQ(cover_calls, 1);
		UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
		UT_ASSERT(cluster_recovery_stamp_ctrc_flush_v2(&h));
		UT_ASSERT_EQ(flush_calls, 2);
		UT_ASSERT_EQ(native_calls, 3); /* Never inserts a second D0. */
		UT_ASSERT_EQ(hash_calls, before);
		CritSectionCount = 0;
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
	}
}

UT_TEST(d0_flush_wrong_handle_and_context_do_no_io)
{
	ClusterRecoveryStampHandleV1 h, changed;
	ClusterRecoveryStampSlotV1 saved;
	UT_ASSERT(flush_window(&h, TT_SLOT_ABORTED));
	saved = region.thread_replay[2].stamp;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(NULL));
	for (unsigned i = 0; i < 332; i++) {
		changed = h;
		switch (i) {
		case 320:
			changed.key.window_seq++;
			break;
		case 321:
			changed.key.slot_generation++;
			break;
		case 322:
			changed.key.target_thread = 3;
			break;
		case 323:
			changed.owner.recovery_serial++;
			break;
		case 324:
			changed.owner.writer.ref.timeline++;
			break;
		case 325:
			changed.kind = CLUSTER_RECOVERY_STAMP_C0;
			break;
		case 326:
			changed.payload_length--;
			break;
		case 327:
			CritSectionCount = 0;
			break;
		case 328:
			MyBackendType = B_BG_WORKER;
			break;
		case 329:
			MyProcPid++;
			break;
		case 330:
			cluster_enabled = false;
			break;
		case 331:
			IsUnderPostmaster = false;
			break;
		default:
			changed.payload[i] ^= 1;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&changed));
		UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
		CritSectionCount = 1;
		MyBackendType = B_STARTUP;
		MyProcPid = owner.executor_pid;
		cluster_enabled = IsUnderPostmaster = true;
	}
	UT_ASSERT_EQ(flush_calls + writer_checks + cover_calls, 0);
	CritSectionCount = 0;
}

UT_TEST(d0_flush_noninserted_and_failed_windows_refuse)
{
	ClusterRecoveryStampHandleV1 h;
	ClusterRecoveryStampSlotV1 saved;
	fresh();
	UT_ASSERT(begin_d0(&h, TT_SLOT_COMMITTED));
	CritSectionCount = 1;
	saved = region.thread_replay[2].stamp;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
	UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&h));
	saved = region.thread_replay[2].stamp;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
	UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
	UT_ASSERT(flush_window(&h, TT_SLOT_COMMITTED));
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&h));
	saved = region.thread_replay[2].stamp;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
	UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
	UT_ASSERT_EQ(flush_calls + writer_checks + cover_calls, 0);
	UT_ASSERT(flush_window(&h, TT_SLOT_COMMITTED));
	UT_ASSERT(cluster_recovery_stamp_durable_v1(&h)); /* Fixture-only metadata transition. */
	saved = region.thread_replay[2].stamp;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
	UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
	UT_ASSERT_EQ(flush_calls + writer_checks + cover_calls, 0);
	CritSectionCount = 0;
}

UT_TEST(d0_flush_refusal_retains_sticky_inserted_responsibility)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampSlotV1 *slot;
		UT_ASSERT(flush_window(&h, TT_SLOT_ABORTED));
		slot = &region.thread_replay[2].stamp;
		switch (fault) {
		case 0:
			writer_reject_on = 1;
			break;
		case 1:
			insert_allowed = false;
			break;
		case 2:
			covered_end--;
			break;
		case 3:
			covered_timeline++;
			break;
		case 4:
			writer_reject_on = 2;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
		UT_ASSERT_EQ(flush_calls, fault < 2 ? 0 : 1);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(slot->record_end, 0x2000413);
		UT_ASSERT(slot->failed);
		UT_ASSERT(memcmp(&slot->handle, &h, sizeof(h)) == 0);
		UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
		UT_ASSERT_EQ(flush_calls, fault < 2 ? 0 : 1);
		CritSectionCount = 0;
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	}
}

UT_TEST(d0_flush_revalidates_window_after_native_wait)
{
	for (unsigned fault = 2; fault <= 5; fault++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampSlotV1 *slot;
		UT_ASSERT(flush_window(&h, TT_SLOT_COMMITTED));
		slot = &region.thread_replay[2].stamp;
		flush_fault = fault;
		UT_ASSERT(!cluster_recovery_stamp_ctrc_flush_v2(&h));
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		/* Exact old handles must never poison a changed operation/key. */
		UT_ASSERT_EQ(slot->failed, fault < 4);
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		CritSectionCount = 0;
	}
}

UT_TEST(d0_flush_native_error_does_not_retire_window)
{
	ClusterRecoveryStampHandleV1 h;
	UT_ASSERT(flush_window(&h, TT_SLOT_ABORTED));
	flush_fault = 1;
	if (sigsetjmp(insert_error_boundary, 0) == 0) {
		(void)cluster_recovery_stamp_ctrc_flush_v2(&h);
		UT_ASSERT(false);
	} else {
		ClusterRecoveryStampSlotV1 *slot = &region.thread_replay[2].stamp;
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(cover_calls, 0);
		UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(slot->record_end, 0x2000413);
		UT_ASSERT(memcmp(&slot->handle, &h, sizeof(h)) == 0);
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
	}
	/* Synthetic unwind is not real critical-section PANIC process death. */
	CritSectionCount = 0;
}

static bool
finish_window(ClusterRecoveryStampHandleV1 *h, uint8 outcome)
{
	if (!flush_window(h, outcome))
		return false;
	memcpy(target_payload, h->payload, sizeof(target_payload));
	return true;
}

UT_TEST(d0_finish_flushes_bound_target_before_clear)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterRecoveryStampCaptureV1 capture;
		unsigned before;
		UT_ASSERT(finish_window(&h, outcome));
		CritSectionCount = 0;
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		CritSectionCount = 1;
		before = hash_calls;
		UT_ASSERT(cluster_recovery_stamp_ctrc_finish_v2(&h, target_stamp));
		UT_ASSERT_EQ(target_calls, 1);
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(writer_checks, 3);
		UT_ASSERT_EQ(cover_calls, 2);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(hash_calls, before);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		UT_ASSERT(!region.thread_replay[2].stamp.failed);
		UT_ASSERT(!cluster_recovery_stamp_ctrc_finish_v2(&h, target_stamp));
		UT_ASSERT_EQ(target_calls, 1);
		CritSectionCount = 0;
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_COMPLETE);
	}
}
UT_TEST(d0_finish_wrong_operation_and_context_do_no_io)
{
	ClusterRecoveryStampHandleV1 h, changed;
	ClusterRecoveryStampSlotV1 saved;
	UT_ASSERT(finish_window(&h, TT_SLOT_ABORTED));
	saved = region.thread_replay[2].stamp;
	UT_ASSERT(!cluster_recovery_stamp_ctrc_finish_v2(NULL, target_stamp));
	for (unsigned i = 0; i < 329; i++) {
		changed = h;
		switch (i) {
		case 320:
			changed.key.window_seq++;
			break;
		case 321:
			changed.key.slot_generation++;
			break;
		case 322:
			changed.owner.recovery_serial++;
			break;
		case 323:
			changed.kind = CLUSTER_RECOVERY_STAMP_C0;
			break;
		case 324:
			changed.payload_length--;
			break;
		case 325:
			CritSectionCount = 0;
			break;
		case 326:
			MyBackendType = B_BG_WORKER;
			break;
		case 327:
			MyProcPid++;
			break;
		case 328:
			cluster_enabled = false;
			break;
		default:
			changed.payload[i] ^= 1;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_finish_v2(&changed, target_stamp));
		UT_ASSERT(memcmp(&saved, &region.thread_replay[2].stamp, sizeof(saved)) == 0);
		CritSectionCount = 1;
		MyBackendType = B_STARTUP;
		MyProcPid = owner.executor_pid;
		cluster_enabled = true;
	}
	UT_ASSERT_EQ(flush_calls + writer_checks + cover_calls + target_calls, 0);
	CritSectionCount = 0;
}
UT_TEST(d0_finish_refusal_retains_exact_inserted_duty)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterUndoSmgrRecoveryStampV1 token = target_stamp;
		UT_ASSERT(finish_window(&h, TT_SLOT_COMMITTED));
		switch (mode) {
		case 0:
			token.token = 0;
			break;
		case 1:
			token.token++;
			break;
		case 2:
			target_fault = 1;
			break;
		case 3:
			writer_reject_on = 1;
			break;
		case 4:
			covered_end--;
			break;
		case 5:
			writer_reject_on = 2;
			break;
		}
		UT_ASSERT(!cluster_recovery_stamp_ctrc_finish_v2(&h, token));
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT(region.thread_replay[2].stamp.failed);
		UT_ASSERT(memcmp(&region.thread_replay[2].stamp.handle, &h, sizeof(h)) == 0);
		UT_ASSERT_EQ(target_calls, mode == 1 || mode == 2 ? 1 : 0);
		UT_ASSERT(!cluster_recovery_stamp_ctrc_finish_v2(&h, target_stamp));
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		CritSectionCount = 0;
	}
}
UT_TEST(d0_finish_rechecks_writer_coverage_and_same_window_after_target)
{
	for (unsigned fault = 3; fault <= 10; fault++) {
		ClusterRecoveryStampHandleV1 h;
		UT_ASSERT(finish_window(&h, TT_SLOT_ABORTED));
		target_fault = fault;
		UT_ASSERT(!cluster_recovery_stamp_ctrc_finish_v2(&h, target_stamp));
		UT_ASSERT_EQ(target_calls, 1);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.failed, fault != 5 && fault != 6);
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		CritSectionCount = 0;
	}
}
UT_TEST(d0_finish_native_error_never_retires_inserted_window)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterRecoveryStampHandleV1 h;
		UT_ASSERT(finish_window(&h, TT_SLOT_ABORTED));
		if (mode == 0)
			flush_fault = 1;
		else
			target_fault = 2;
		if (sigsetjmp(insert_error_boundary, 0) == 0) {
			(void)cluster_recovery_stamp_ctrc_finish_v2(&h, target_stamp);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
			UT_ASSERT_EQ(target_calls, mode);
			UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
			UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		}
		/* Controlled unwind does not prove real PANIC process death. */
		CritSectionCount = 0;
	}
}


#define D0_RAW_BYTES (SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 320)

/* Native insertion/flush/storage remain controlled leaves. The chain copy,
 * stamp owner and CRC/SHA implementation are actual production functions. */
static void
snapshot_insert(ClusterRecoveryStampHandleV1 *h)
{
	fresh();
	UT_ASSERT(begin_d0(h, TT_SLOT_COMMITTED));
	CritSectionCount = 1;
	UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_v2(h), 0x2000413);
}

UT_TEST(d0_snapshot_exact_final_raw_survives_native_reuse)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 raw[D0_RAW_BYTES], saved[D0_RAW_BYTES], digest[32], again[32];
		XLogRecord copied;
		XLogRecPtr start = 0, end;
		unsigned before;
		fresh();
		UT_ASSERT(begin_d0(&h, outcome));
		before = hash_calls;
		CritSectionCount = 1;
		end = cluster_recovery_stamp_ctrc_insert_record_v2(&h, raw, sizeof(raw), &start);
		UT_ASSERT_EQ(end, 0x2000413);
		UT_ASSERT_EQ(start, 0x2000018);
		if (end == 0) { CritSectionCount = 0; continue; }
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(flush_calls + target_calls, 0);
		UT_ASSERT_EQ(hash_calls, before);
		memcpy(&copied, raw, SizeOfXLogRecord);
		UT_ASSERT_EQ(copied.xl_prev, native_header.xl_prev);
		UT_ASSERT_EQ(copied.xl_crc, native_header.xl_crc);
		UT_ASSERT(memcmp(raw, native_scratch.bytes, sizeof(native_scratch.bytes)) == 0);
		UT_ASSERT(memcmp(raw + sizeof(native_scratch.bytes), h.payload, 320) == 0);
		memcpy(saved, raw, sizeof(raw));
		memcpy(target_payload, h.payload, 320);
		UT_ASSERT(cluster_recovery_stamp_ctrc_finish_v2(&h, target_stamp));
		UT_ASSERT_EQ(hash_calls, before);
		CritSectionCount = 0;
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(h.owner.target.claim_sha256,
			start, end, raw, sizeof(raw), digest));
		/* Construction storage is no longer a durable reference after reuse. */
		memset(&native_scratch, 0xCC, sizeof(native_scratch));
		memset(registered_payload, 0xDD, sizeof(registered_payload));
		last_insert_record_end = InvalidXLogRecPtr;
		UT_ASSERT(memcmp(raw, saved, sizeof(raw)) == 0);
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(h.owner.target.claim_sha256,
			start, end, raw, sizeof(raw), again));
		UT_ASSERT(memcmp(digest, again, 32) == 0);
		raw[sizeof(raw)-1] ^= 1;
		UT_ASSERT(!cluster_recovery_ctrc_record_hash_v2(h.owner.target.claim_sha256,
			start, end, raw, sizeof(raw), again));
	}
}

UT_TEST(d0_snapshot_preinsert_refusal_is_atomic)
{
	for (unsigned mode = 0; mode < 9; mode++) {
		ClusterRecoveryStampHandleV1 h, saved;
		ClusterRecoveryStampSlotV1 slot;
		uint8 raw[D0_RAW_BYTES], before[D0_RAW_BYTES];
		void *out = raw;
		XLogRecPtr start = 91, *start_out = &start;
		Size len = sizeof(raw);
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_COMMITTED));
		CritSectionCount = 1;
		memset(raw, 0xA5, sizeof(raw));
		memcpy(before, raw, sizeof(raw));
		switch (mode) {
		case 0: out = NULL; break;
		case 1: len--; break;
		case 2: len++; break;
		case 3: start_out = NULL; break;
		case 4: start_out = (XLogRecPtr *)raw; break;
		case 5: out = &h; break;
		case 6: start_out = (XLogRecPtr *)&h; break;
		case 7: CritSectionCount = 0; break;
		case 8: out = &region.thread_replay[2].stamp; break;
		}
		saved = h;
		stamp_snapshot(&region.thread_replay[2].stamp, &slot);
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_record_v2(&h, out, len, start_out), 0);
		UT_ASSERT_EQ(native_calls + flush_calls + target_calls, 0);
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
		UT_ASSERT(memcmp(&h, &saved, sizeof(h)) == 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, slot.phase);
		UT_ASSERT(!region.thread_replay[2].stamp.failed);
		CritSectionCount = 0;
	}
}

UT_TEST(d0_snapshot_postinsert_refusal_is_sticky_and_atomic)
{
	for (unsigned fault = 1; fault <= 13; fault++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 raw[D0_RAW_BYTES], before[D0_RAW_BYTES];
		XLogRecPtr start = 91;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_ABORTED));
		memset(raw, 0xA5, sizeof(raw));
		memcpy(before, raw, sizeof(raw));
		native_fault = fault;
		CritSectionCount = 1;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_insert_record_v2(&h, raw, sizeof(raw), &start), 0);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(flush_calls + target_calls, 0);
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_durable_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		if (fault == 13)
			UT_ASSERT(!region.thread_replay[2].stamp.failed);
		else
			UT_ASSERT(region.thread_replay[2].stamp.failed);
		CritSectionCount = 0;
	}
}

UT_TEST(native_record_copy_exact_split_zero_fragments_unaligned)
{
	ClusterRecoveryStampHandleV1 h;
	uint8 output[D0_RAW_BYTES + 2], expected[D0_RAW_BYTES];
	XLogRecPtr start = 0;
	snapshot_insert(&h);
	memcpy(expected, native_scratch.bytes, sizeof(native_scratch.bytes));
	memcpy(expected + sizeof(native_scratch.bytes), registered_payload, 320);
	native_data[0].len = 127;
	native_data[0].next = &native_data[1];
	native_data[1] = (XLogRecData){ .data = NULL, .len = 0, .next = &native_data[2] };
	native_data[2] = (XLogRecData){ .data = (char *)registered_payload + 127, .len = 193 };
	memset(output, 0xA5, sizeof(output));
	UT_ASSERT(XLogCopyLastInsertRecord(XactLastRecEnd, output + 1, D0_RAW_BYTES, &start));
	UT_ASSERT_EQ(start, ProcLastRecPtr);
	UT_ASSERT(memcmp(output + 1, expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(output[0], 0xA5);
	UT_ASSERT_EQ(output[sizeof(output)-1], 0xA5);
	CritSectionCount = 0;
}

UT_TEST(native_record_copy_rejects_incomplete_extra_and_cyclic_chains)
{
	for (unsigned mode = 0; mode < 11; mode++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 output[D0_RAW_BYTES], before[D0_RAW_BYTES];
		XLogRecPtr start = 91;
		snapshot_insert(&h);
		memset(output, 0xA5, sizeof(output));
		memcpy(before, output, sizeof(output));
		switch (mode) {
		case 0: hdr_rdt.next = NULL; break;
		case 1: native_data[0].len--; break;
		case 2: native_data[0].len++; break;
		case 3: native_data[0].data = NULL; break;
		case 4: native_data[0].next = &native_data[0]; break;
		case 5: hdr_rdt.len = SizeOfXLogRecord - 1; break;
		case 6: hdr_rdt.data++; break;
		case 7: max_rdatas = 0; break;
		case 8: native_data[0].next = &native_data[1];
			native_data[1].len = 1; native_data[1].data = (char *)output; break;
		case 9: ((XLogRecord *)hdr_scratch)->xl_tot_len--; break;
		case 10: native_data[0].next = &native_data[1];
			native_data[1].next = &native_data[1]; break;
		}
		UT_ASSERT(!XLogCopyLastInsertRecord(XactLastRecEnd, output, sizeof(output), &start));
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(output, before, sizeof(output)) == 0);
		CritSectionCount = 0;
	}
}

UT_TEST(native_record_copy_refuses_stale_construction_and_lengths)
{
	for (unsigned mode = 0; mode < 12; mode++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 output[D0_RAW_BYTES], before[D0_RAW_BYTES];
		XLogRecPtr start = 91, end;
		Size length = sizeof(output);
		snapshot_insert(&h);
		end = XactLastRecEnd;
		memset(output, 0xA5, sizeof(output));
		memcpy(before, output, sizeof(output));
		switch (mode) {
		case 0: begininsert_called = true; break;
		case 1: last_insert_record_end = 0; break;
		case 2: end++; break;
		case 3: XactLastRecEnd++; break;
		case 4: ProcLastRecPtr = 0; break;
		case 5: ProcLastRecPtr = end; break;
		case 6: cluster_shared_config = false; break;
		case 7: hdr_scratch = NULL; break;
		case 8: length--; break;
		case 9: length++; break;
		case 10: length = SizeOfXLogRecord - 1; break;
		case 11: length = XLogRecordMaxSize + (Size)1; break;
		}
		UT_ASSERT(!XLogCopyLastInsertRecord(end, output, length, &start));
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(output, before, sizeof(output)) == 0);
		CritSectionCount = 0;
	}
}

UT_TEST(native_record_copy_refuses_input_and_output_aliases)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 output[D0_RAW_BYTES], before[D0_RAW_BYTES], source[320];
		XLogRecData nodes[4];
		XLogRecPtr start = 91, *start_out = &start;
		void *raw = output;
		snapshot_insert(&h);
		memset(output, 0xA5, sizeof(output));
		memcpy(before, output, sizeof(output));
		memcpy(source, registered_payload, sizeof(source));
		nodes[0] = hdr_rdt;
		memcpy(nodes + 1, native_data, sizeof(native_data));
		switch (mode) {
		case 0: raw = hdr_scratch; break;
		case 1: raw = registered_payload; break;
		case 2: raw = &native_data[0]; break;
		case 3: start_out = (XLogRecPtr *)output; break;
		case 4: start_out = (XLogRecPtr *)registered_payload; break;
		case 5: start_out = (XLogRecPtr *)&native_data[0]; break;
		case 6: raw = NULL; break;
		case 7: start_out = NULL; break;
		}
		UT_ASSERT(!XLogCopyLastInsertRecord(XactLastRecEnd, raw, sizeof(output), start_out));
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(output, before, sizeof(output)) == 0);
		UT_ASSERT(memcmp(source, registered_payload, sizeof(source)) == 0);
		UT_ASSERT(memcmp(&nodes[0], &hdr_rdt, sizeof(hdr_rdt)) == 0);
		UT_ASSERT(memcmp(nodes + 1, native_data, sizeof(native_data)) == 0);
		CritSectionCount = 0;
	}
}

UT_TEST(d0_apply_publishes_only_after_exact_finish)
{
	for (unsigned outcome = TT_SLOT_COMMITTED; outcome <= TT_SLOT_ABORTED; outcome++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 raw[D0_RAW_BYTES + 2], saved[D0_RAW_BYTES], address[sizeof(XLogRecPtr) + 2];
		uint8 digest[32];
		XLogRecPtr start = 91, end;
		unsigned before;
		fresh();
		UT_ASSERT(begin_d0(&h, outcome));
		memcpy(target_payload, h.payload, sizeof(target_payload));
		memset(raw, 0xA5, sizeof(raw));
		memset(address, 0xA5, sizeof(address));
		memcpy(address + 1, &start, sizeof(start));
		apply_raw = raw + 1;
		apply_start = address + 1;
		before = hash_calls;
		CritSectionCount = 2;
		end = cluster_recovery_stamp_ctrc_apply_v2(&h, target_stamp, raw + 1,
			D0_RAW_BYTES, (XLogRecPtr *)(address + 1));
		UT_ASSERT_EQ(end, 0x2000413);
		UT_ASSERT_EQ(CritSectionCount, 2);
		if (end == 0) { CritSectionCount = 0; continue; }
		memcpy(&start, address + 1, sizeof(start));
		UT_ASSERT_EQ(start, 0x2000018);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(target_calls, 1);
		UT_ASSERT_EQ(writer_checks, 3);
		UT_ASSERT_EQ(cover_calls, 2);
		UT_ASSERT_EQ(hash_calls, before);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		UT_ASSERT(!region.thread_replay[2].stamp.failed);
		UT_ASSERT(memcmp(raw + 1, native_scratch.bytes, sizeof(native_scratch.bytes)) == 0);
		UT_ASSERT(memcmp(raw + 1 + sizeof(native_scratch.bytes), h.payload, 320) == 0);
		UT_ASSERT_EQ(raw[0], 0xA5);
		UT_ASSERT_EQ(raw[sizeof(raw)-1], 0xA5);
		UT_ASSERT_EQ(address[0], 0xA5);
		UT_ASSERT_EQ(address[sizeof(address)-1], 0xA5);
		memcpy(saved, raw + 1, sizeof(saved));
		/* An already completed handle cannot insert again or touch outputs. */
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_apply_v2(&h, target_stamp, raw + 1,
			D0_RAW_BYTES, (XLogRecPtr *)(address + 1)), 0);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(flush_calls + target_calls, 2);
		UT_ASSERT(memcmp(saved, raw + 1, sizeof(saved)) == 0);
		CritSectionCount = 0;
		UT_ASSERT(cluster_recovery_ctrc_record_hash_v2(h.owner.target.claim_sha256,
			start, end, raw + 1, D0_RAW_BYTES, digest));
		memset(&native_scratch, 0xCC, sizeof(native_scratch));
		memset(registered_payload, 0xDD, sizeof(registered_payload));
		UT_ASSERT(memcmp(saved, raw + 1, sizeof(saved)) == 0);
	}
	apply_raw = apply_start = NULL;
}

UT_TEST(d0_apply_preinsert_refusal_preserves_outputs_and_window)
{
	for (unsigned mode = 0; mode < 19; mode++) {
		ClusterRecoveryStampHandleV1 h, saved;
		const ClusterRecoveryStampHandleV1 *input = &h;
		ClusterUndoSmgrRecoveryStampV1 token = target_stamp;
		uint8 raw[D0_RAW_BYTES], before[D0_RAW_BYTES];
		void *out = raw;
		XLogRecPtr start = 91, *start_out = &start;
		Size length = sizeof(raw);
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_COMMITTED));
		memset(raw, 0xA5, sizeof(raw));
		memcpy(before, raw, sizeof(raw));
		CritSectionCount = 1;
		switch (mode) {
		case 0: input = NULL; break;
		case 1: out = NULL; break;
		case 2: start_out = NULL; break;
		case 3: length--; break;
		case 4: length++; break;
		case 5: start_out = (XLogRecPtr *)raw; break;
		case 6: out = &h; break;
		case 7: start_out = (XLogRecPtr *)&h; break;
		case 8: out = &region.thread_replay[2].stamp; break;
		case 9: start_out = (XLogRecPtr *)&region.thread_replay[2].stamp; break;
		case 10: token.token = 0; break;
		case 11: CritSectionCount = 0; break;
		case 12: MyBackendType = B_BACKEND; break;
		case 13: MyProcPid++; break;
		case 14: cluster_enabled = false; break;
		case 15: insert_allowed = false; break;
		case 16: insert_xid = 100; break;
		case 17: h.key.target_thread = 0; break;
		case 18: cluster_shared_config = false; break;
		}
		saved = h;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_apply_v2(input, token, out, length, start_out), 0);
		UT_ASSERT_EQ(native_calls + flush_calls + target_calls, 0);
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
		UT_ASSERT(memcmp(&saved, &h, sizeof(h)) == 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
		UT_ASSERT(!region.thread_replay[2].stamp.failed);
		CritSectionCount = 0;
	}
}

UT_TEST(d0_apply_snapshot_refusal_never_flushes_or_publishes)
{
	const unsigned faults[] = { 1, 8, 12, 13 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 raw[D0_RAW_BYTES], before[D0_RAW_BYTES];
		XLogRecPtr start = 91;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_ABORTED));
		memset(raw, 0xA5, sizeof(raw));
		memcpy(before, raw, sizeof(raw));
		native_fault = faults[i];
		CritSectionCount = 1;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_apply_v2(&h, target_stamp, raw, sizeof(raw), &start), 0);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(flush_calls + target_calls, 0);
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		UT_ASSERT_EQ(region.thread_replay[2].stamp.failed, faults[i] != 13);
		CritSectionCount = 0;
	}
}

UT_TEST(d0_apply_finish_refusal_retains_duty_and_private_snapshot)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		ClusterRecoveryStampHandleV1 h;
		ClusterUndoSmgrRecoveryStampV1 token = target_stamp;
		uint8 raw[D0_RAW_BYTES], before[D0_RAW_BYTES];
		XLogRecPtr start = 91;
		unsigned calls;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_COMMITTED));
		memcpy(target_payload, h.payload, sizeof(target_payload));
		memset(raw, 0xA5, sizeof(raw));
		memcpy(before, raw, sizeof(raw));
		apply_raw = raw;
		apply_start = (uint8 *)&start;
		switch (mode) {
		case 0: token.token++; break;
		case 1: writer_reject_on = 1; break;
		case 2: covered_end--; break;
		case 3: flush_fault = 2; break;
		case 4: target_fault = 1; break;
		case 5: target_fault = 6; break;
		case 6: target_fault = 8; break;
		case 7: target_fault = 9; break;
		}
		CritSectionCount = 1;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_apply_v2(&h, token, raw, sizeof(raw), &start), 0);
		UT_ASSERT_EQ(native_calls, 3);
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(region.thread_replay[2].stamp.failed, mode != 5);
		UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
		UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		calls = native_calls + flush_calls + target_calls;
		UT_ASSERT_EQ(cluster_recovery_stamp_ctrc_apply_v2(&h, token, raw, sizeof(raw), &start), 0);
		UT_ASSERT_EQ(native_calls + flush_calls + target_calls, calls);
		UT_ASSERT_EQ(start, 91);
		UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
		CritSectionCount = 0;
	}
	apply_raw = apply_start = NULL;
}

UT_TEST(d0_apply_native_errors_propagate_without_retirement)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		ClusterRecoveryStampHandleV1 h;
		uint8 raw[D0_RAW_BYTES], before[D0_RAW_BYTES];
		XLogRecPtr start = 91;
		fresh();
		UT_ASSERT(begin_d0(&h, TT_SLOT_ABORTED));
		memcpy(target_payload, h.payload, sizeof(target_payload));
		memset(raw, 0xA5, sizeof(raw));
		memcpy(before, raw, sizeof(raw));
		apply_raw = raw;
		apply_start = (uint8 *)&start;
		if (mode < 3) native_throw = mode + 1;
		else if (mode == 3) flush_fault = 1;
		else target_fault = 2;
		CritSectionCount = 1;
		if (sigsetjmp(insert_error_boundary, 1) == 0) {
			(void)cluster_recovery_stamp_ctrc_apply_v2(&h, target_stamp, raw, sizeof(raw), &start);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(CritSectionCount, 1);
			UT_ASSERT_EQ(region.thread_replay[2].stamp.phase,
				mode < 3 ? CLUSTER_RECOVERY_STAMP_INSERTING : CLUSTER_RECOVERY_STAMP_INSERTED);
			UT_ASSERT_EQ(start, 91);
			UT_ASSERT(memcmp(raw, before, sizeof(raw)) == 0);
			UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&h));
			UT_ASSERT(!cluster_recovery_stamp_clear_v1(&h));
		}
		/* Controlled unwind checks ownership, not real PANIC process death. */
		CritSectionCount = 0;
	}
	apply_raw = apply_start = NULL;
}

int
main(void)
{
	UT_PLAN(43);
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
	UT_RUN(d0_flush_exact_end_preserves_target_duty);
	UT_RUN(d0_flush_wrong_handle_and_context_do_no_io);
	UT_RUN(d0_flush_noninserted_and_failed_windows_refuse);
	UT_RUN(d0_flush_refusal_retains_sticky_inserted_responsibility);
	UT_RUN(d0_flush_revalidates_window_after_native_wait);
	UT_RUN(d0_flush_native_error_does_not_retire_window);
	UT_RUN(d0_finish_flushes_bound_target_before_clear);
	UT_RUN(d0_finish_wrong_operation_and_context_do_no_io);
	UT_RUN(d0_finish_refusal_retains_exact_inserted_duty);
	UT_RUN(d0_finish_rechecks_writer_coverage_and_same_window_after_target);
	UT_RUN(d0_finish_native_error_never_retires_inserted_window);
	UT_RUN(d0_snapshot_exact_final_raw_survives_native_reuse);
	UT_RUN(d0_snapshot_preinsert_refusal_is_atomic);
	UT_RUN(d0_snapshot_postinsert_refusal_is_sticky_and_atomic);
	UT_RUN(native_record_copy_exact_split_zero_fragments_unaligned);
	UT_RUN(native_record_copy_rejects_incomplete_extra_and_cyclic_chains);
	UT_RUN(native_record_copy_refuses_stale_construction_and_lengths);
	UT_RUN(native_record_copy_refuses_input_and_output_aliases);
	UT_RUN(d0_apply_publishes_only_after_exact_finish);
	UT_RUN(d0_apply_preinsert_refusal_preserves_outputs_and_window);
	UT_RUN(d0_apply_snapshot_refusal_never_flushes_or_publishes);
	UT_RUN(d0_apply_finish_refusal_retains_duty_and_private_snapshot);
	UT_RUN(d0_apply_native_errors_propagate_without_retirement);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
