/* Author: SqlRush <sqlrush@gmail.com>
 * Original shared slot/state machine. External authority and WAL/IO completion
 * are fixture boundaries; this is not a recovery or checkpoint field test. */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_thread_recovery.h"
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
static unsigned stamp_hash_calls;

/* Observe the actual D0 codec's provider allocation, rather than replacing
 * its decode result. All original slot locks must already be released. */
static pg_cryptohash_ctx *
stamp_hash_create(pg_cryptohash_type type)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	for (unsigned i = 0; i <= CLUSTER_RECOVERY_STAMP_SLOTS; i++)
		UT_ASSERT(SpinLockFree(&region.thread_replay[i].stamp.lock));
	stamp_hash_calls++;
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
	stamp_hash_calls = 0;
	IsUnderPostmaster = cluster_enabled = cluster_shared_config = true;
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
	/* A shared BGW now needs its original validated launch registration. */
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&owner, 1, payload, sizeof(payload), &h));
	pg_atomic_write_u64(&region.thread_replay[2].episode_epoch, 77);
	pg_atomic_write_u32(&region.thread_replay[2].state, CLUSTER_THREADREC_REPLAY_REPLAYING);
	UT_ASSERT(cluster_recovery_stamp_worker_begin_v1(2, 77));
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
UT_TEST(immutable_operation_survives_worker_stop)
{
	ClusterRecoveryStampHandleV1 handle, changed;
	ClusterRecoveryStampCaptureV1 capture;
	ClusterRecoveryStampSlotV1 *slot;
	fresh();
	MyBackendType = B_BG_WORKER;
	pg_atomic_write_u64(&region.thread_replay[2].episode_epoch, 77);
	pg_atomic_write_u32(&region.thread_replay[2].state, CLUSTER_THREADREC_REPLAY_REPLAYING);
	UT_ASSERT(cluster_recovery_stamp_worker_begin_v1(2, 77));
	if (!begin(&handle))
		return;
	slot = &region.thread_replay[2].stamp;
	UT_ASSERT_EQ(handle.kind, CLUSTER_RECOVERY_STAMP_C0);
	UT_ASSERT_EQ(handle.payload_length, sizeof(payload));
	UT_ASSERT(memcmp(handle.payload, payload, sizeof(payload)) == 0);
	changed = handle;
	changed.payload[sizeof(changed.payload) - 1] ^= 1;
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&changed));
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&handle));
	CritSectionCount = 0;
	MyBackendType = B_LMON;
	UT_ASSERT(!cluster_recovery_stamp_worker_stopped_v1(2, 78));
	UT_ASSERT(cluster_recovery_stamp_worker_stopped_v1(2, 77));
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
	UT_ASSERT_EQ(slot->adopt_from, CLUSTER_RECOVERY_STAMP_INSERTING);
	UT_ASSERT_EQ(slot->executor_attempt_stamp, 77);
	UT_ASSERT_EQ(slot->bound_worker_attempt, 0);
	UT_ASSERT(memcmp(slot->handle.payload, handle.payload, sizeof(handle.payload)) == 0);
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
	MyBackendType = B_BG_WORKER;
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&handle));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_stamp_inserted_v1(&handle, 0x2000010, 0x20000c0));
	CritSectionCount = 0;
}

UT_TEST(d0_codec_is_unlocked_and_orphan_remains_pending)
{
	ClusterRecoveryCTRCReleaseV2 record = { 0 };
	uint8 d0[CLUSTER_RECOVERY_CTRC_BYTES];
	ClusterRecoveryStampHandleV1 handle, changed;
	ClusterRecoveryStampCaptureV1 capture;
	ClusterRecoveryStampSlotV1 *slot;
	unsigned hashes;
	fresh();
	record.version = CLUSTER_RECOVERY_CTRC_VERSION;
	record.length = sizeof(d0);
	record.flags = CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN;
	record.root_id = 1;
	record.root_generation = 2;
	record.formation_epoch = owner.writer.epoch;
	record.recovery_serial = owner.recovery_serial;
	record.database_incarnation = owner.target.database_incarnation;
	record.writer_boot = owner.executor_boot;
	record.cohort_bitmap = 3;
	record.origin_node_id = owner.target.identity.origin_node_id;
	record.writer_thread = owner.writer.ref.claim.identity.origin_thread_id;
	record.target_owner = owner.target.identity.origin_thread_id;
	record.segment_id = 257;
	record.slot_offset = 1;
	record.wrap = 1;
	record.xid = 100;
	record.terminal_outcome = TT_SLOT_ABORTED;
	record.before.xid = record.xid;
	record.before.wrap = record.wrap;
	record.before.status = record.terminal_outcome;
	record.after = record.before;
	record.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	memset(record.graph_sha256, 3, sizeof(record.graph_sha256));
	memset(record.input_sha256, 4, sizeof(record.input_sha256));
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&record, 99, d0, sizeof(d0)));
	MyBackendType = B_BG_WORKER;
	pg_atomic_write_u64(&region.thread_replay[2].episode_epoch, 79);
	pg_atomic_write_u32(&region.thread_replay[2].state, CLUSTER_THREADREC_REPLAY_REPLAYING);
	UT_ASSERT(cluster_recovery_stamp_worker_begin_v1(2, 79));
	UT_ASSERT(cluster_recovery_stamp_prepare_v1(&owner, CLUSTER_RECOVERY_STAMP_D0,
												 d0, sizeof(d0), &handle));
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
	UT_ASSERT(stamp_hash_calls > 0);
	hashes = stamp_hash_calls;
	changed = handle;
	changed.payload[319] ^= 1;
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&changed));
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&handle));
	UT_ASSERT(cluster_recovery_stamp_inserted_v1(&handle, 0x2000010, 0x2000200));
	CritSectionCount = 0;
	UT_ASSERT_EQ(stamp_hash_calls, hashes);
	slot = &region.thread_replay[2].stamp;
	/* Preserve an original failed INSERTED obligation at exact native STOPPED. */
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&handle));
	MyBackendType = B_LMON;
	UT_ASSERT(cluster_recovery_stamp_worker_stopped_v1(2, 79));
	UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_ADOPT_PENDING);
	UT_ASSERT_EQ(slot->adopt_from, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(slot->failed);
	UT_ASSERT_EQ(slot->executor_attempt_stamp, 79);
	UT_ASSERT(memcmp(&slot->handle, &handle, sizeof(handle)) == 0);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
	MyBackendType = B_BG_WORKER;
	UT_ASSERT(!cluster_recovery_stamp_durable_v1(&handle));
	UT_ASSERT(!cluster_recovery_stamp_clear_v1(&handle));
	/* Lock-free observers still authenticate the immutable payload. */
	slot->handle.payload[319] ^= 1;
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_INVALID);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(exact_lifecycle_and_owned_payload);
	UT_RUN(preinsert_cancel_capture_and_new_sequence);
	UT_RUN(inserting_failure_exit_and_replay_reset_cannot_retire);
	UT_RUN(identity_role_kind_and_payload_refusal);
	UT_RUN(lsn_boundaries_and_sticky_postinsert_failure);
	UT_RUN(capture_shape_overflow_and_corruption_fail_closed);
	UT_RUN(one_serial_executor_and_all_original_slots);
	UT_RUN(normal_stop_observes_windows_separately);
	UT_RUN(immutable_operation_survives_worker_stop);
	UT_RUN(d0_codec_is_unlocked_and_orphan_remains_pending);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
