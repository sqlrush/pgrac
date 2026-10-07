/*-------------------------------------------------------------------------
 * test_cluster_xid_stripe_boot.c
 *    Production stripe admission ordering, before and after local recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Recovery state, counters, shared publication and the WAL sink are dependency
 * inputs. All gate/seed staging/WAL emission bodies are extracted unchanged.
 * This is not a live formation, durable-voting or recovery completion proof.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_stripe_xlog.h"
#include "cluster/cluster_mxid_stripe.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_xid_striping = true;
int cluster_node_id = 1;
int cluster_xid_herding_slack = 4194304;
static bool recovery, wal_allowed;
static uint64 next_xid;
static uint32 next_mxid;
static unsigned xid_reads, mxid_reads, wal_records, wal_begins;
static xl_cluster_xid_stripe_join wal_payload;

bool
RecoveryInProgress(void)
{
	return recovery;
}
bool
XLogInsertAllowed(void)
{
	return wal_allowed;
}
FullTransactionId
ReadNextFullTransactionId(void)
{
	xid_reads++;
	return FullTransactionIdFromU64(next_xid);
}
MultiXactId
ReadNextMultiXactId(void)
{
	mxid_reads++;
	return next_mxid;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	return true;
}
void
LWLockRelease(LWLock *lock)
{}
void
XLogBeginInsert(void)
{
	wal_begins++;
}
void
XLogRegisterData(char *data, uint32 len)
{
	UT_ASSERT_EQ(len, sizeof(wal_payload));
	memcpy(&wal_payload, data, len);
}
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(rmid, RM_CLUSTER_XID_STRIPE_ID);
	UT_ASSERT_EQ(info, XLOG_CLUSTER_XID_STRIPE_JOIN);
	wal_records++;
	return UINT64_C(8192);
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
#undef errstart
#undef errstart_cold
bool
errstart(int level, const char *domain)
{
	if (level >= ERROR)
		abort();
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}

#include "test_cluster_xid_stripe_boot.inc"

static ClusterXidStripeBootShmem boot;
static void
reset_boot(void)
{
	memset(&boot, 0, sizeof(boot));
	StripeBootShmem = &boot;
	pg_atomic_init_u64(&boot.req_seq, 0);
	pg_atomic_init_u64(&boot.done_seq, 0);
	cluster_enabled = cluster_xid_striping = true;
	recovery = true;
	wal_allowed = false;
	next_xid = 1025;
	next_mxid = 17;
	xid_reads = mxid_reads = wal_records = wal_begins = 0;
}

UT_TEST(test_recovery_holds_every_activation_state_without_side_effects)
{
	reset_boot();
	for (int state = CLUSTER_XID_STRIPE_DISK_UNKNOWN; state <= CLUSTER_XID_STRIPE_DISK_CORRUPT;
		 state++) {
		boot.disk_state = state;
		boot.slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
		UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_WAIT_EVIDENCE);
		UT_ASSERT_EQ(pg_atomic_read_u64(&boot.req_seq), 0);
	}
	UT_ASSERT_EQ(xid_reads, 0);
	UT_ASSERT_EQ(mxid_reads, 0);
	UT_ASSERT_EQ(wal_begins, 0);
}

UT_TEST(test_seed_samples_recovery_end_not_the_pre_recovery_counter)
{
	reset_boot();
	boot.disk_state = CLUSTER_XID_STRIPE_DISK_ABSENT;
	UT_ASSERT_EQ(cluster_xid_stripe_join_gate(true), CLUSTER_XID_STRIPE_JOIN_HOLD);
	next_xid = 9003;
	next_mxid = 219;
	recovery = false;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(false), STRIPE_JOIN_WAIT_EVIDENCE);
	UT_ASSERT_EQ(xid_reads, 0);
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_WAIT_EVIDENCE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&boot.req_seq), 1);
	UT_ASSERT_EQ(boot.op, STRIPE_OP_SEED);
	UT_ASSERT_EQ(boot.seed_floor_full, UINT64_C(9008) + 4194304);
	UT_ASSERT_EQ(boot.seed_mxid_floor, 240);
	UT_ASSERT_EQ(xid_reads, 1);
	UT_ASSERT_EQ(mxid_reads, 1);
	UT_ASSERT_EQ(wal_records, 0);
	/* A busy mailbox neither replaces the seed nor resamples its floor. */
	next_xid = 17001;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_WAIT_EVIDENCE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&boot.req_seq), 1);
	UT_ASSERT_EQ(xid_reads, 1);
}

UT_TEST(test_post_recovery_unknown_corrupt_and_retired_stay_closed)
{
	reset_boot();
	recovery = false;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_WAIT_EVIDENCE);
	boot.disk_state = CLUSTER_XID_STRIPE_DISK_CORRUPT;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_REFUSE);
	boot.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_RETIRED;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_REFUSE);
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_CORRUPT;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(true), STRIPE_JOIN_REFUSE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&boot.req_seq), 0);
	UT_ASSERT_EQ(wal_records, 0);
}

UT_TEST(test_claim_requires_exact_pending_self_owner)
{
	reset_boot();
	recovery = false;
	boot.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_ABSENT;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(false), STRIPE_JOIN_CLAIM_OWNED);
	UT_ASSERT_EQ(boot.op, STRIPE_OP_CLAIM);
	UT_ASSERT_EQ(boot.op_target_node, cluster_node_id);
	UT_ASSERT_EQ(cluster_xid_stripe_join_gate(false), CLUSTER_XID_STRIPE_JOIN_HOLD);
	UT_ASSERT_EQ(pg_atomic_read_u64(&boot.req_seq), 1);
	boot.op_target_node = 0;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(false), STRIPE_JOIN_WAIT_EVIDENCE);
	boot.op_target_node = cluster_node_id;
	boot.op = STRIPE_OP_RETIRE;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(false), STRIPE_JOIN_WAIT_EVIDENCE);
	UT_ASSERT_EQ(wal_records, 0);
}

/* Keep success last: the real emitter's per-process one-shot flag is not
 * reset by a testing API. A new process, not memset, represents a new boot. */
UT_TEST(test_owned_claim_waits_for_original_join_wal_before_proceed)
{
	reset_boot();
	recovery = false;
	boot.disk_state = CLUSTER_XID_STRIPE_DISK_PUBLISHED;
	boot.slot_state = CLUSTER_XID_STRIPE_SLOT_MINE;
	boot.floor_full = UINT64_C(4203312);
	boot.epoch = 1;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(false), STRIPE_JOIN_CLAIM_OWNED);
	UT_ASSERT_EQ(wal_records, 0);
	wal_allowed = true;
	UT_ASSERT_EQ(cluster_xid_stripe_join_progress(false), STRIPE_JOIN_PROCEED);
	UT_ASSERT_EQ(wal_records, 1);
	UT_ASSERT_EQ(wal_payload.activated_floor_full, boot.floor_full);
	UT_ASSERT_EQ(wal_payload.stride_mode_epoch, 1);
	UT_ASSERT_EQ(wal_payload.slot, cluster_node_id);
	UT_ASSERT_EQ(cluster_xid_stripe_join_gate(false), CLUSTER_XID_STRIPE_JOIN_PROCEED);
	UT_ASSERT_EQ(wal_records, 1);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_recovery_holds_every_activation_state_without_side_effects);
	UT_RUN(test_seed_samples_recovery_end_not_the_pre_recovery_counter);
	UT_RUN(test_post_recovery_unknown_corrupt_and_retired_stay_closed);
	UT_RUN(test_claim_requires_exact_pending_self_owner);
	UT_RUN(test_owned_claim_waits_for_original_join_wal_before_proceed);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
