/* Original scoped recursive caller. Storage/WAL/receipt tables are controlled
 * leaves; this is not SQL or durable WAL evidence. */
#define HISTORY_NATIVE_RECURSION_FIXTURE
#define GetCurrentTimestamp recursive_forbidden_timestamp
#define TransactionIdIsCurrentTransactionId recursive_forbidden_own_xid
#include "test_cluster_mx_history_two_hop.c"
#undef main
#undef HISTORY_NATIVE_RECURSION_FIXTURE
#undef GetCurrentTimestamp
#undef TransactionIdIsCurrentTransactionId

#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/storage/cluster_smgr.h"
#include "../../backend/access/heap/heapam_r4_private.h"
#include "test_cluster_mx_history_native_recursive_leaves.inc"

TimestampTz GetCurrentTimestamp(void);
bool TransactionIdIsCurrentTransactionId(TransactionId xid);
TimestampTz GetCurrentTimestamp(void) { return 1000000; }
bool TransactionIdIsCurrentTransactionId(TransactionId xid) { return xid == 145; }

bool cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	UT_ASSERT(!content_locked && xid == 129);
	*out = canonical_source;
	return canonical_source.xid == xid && publication_source == CLUSTER_CANONICAL_TXN_PUBLISHED;
}
static void native_lock(Buffer buffer, int mode)
{
	if (mode == BUFFER_LOCK_SHARE) {
		UT_ASSERT_EQ(buffer, 1); UT_ASSERT(!content_locked);
		content_locked = true; exclusive = false;
	} else auth_lock(buffer, mode);
}
static int native_call(const ClusterHeapSuccessorProof *next)
{
	volatile int result = -1;
	error_code = 0;
	PG_TRY(); { result = mx_history_native_recursive(&relation, &next->tid, next); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY();
	return result;
}
static FormData_pg_class native_class;
static Snapshot native_setup(ClusterHeapSuccessorProof *next)
{
	MxHeapReadout out;
	Snapshot s = two_setup();

	memset(&native_class, 0, sizeof(native_class));
	native_class.relpersistence = RELPERSISTENCE_PERMANENT;
	relation.rd_rel = &native_class; native_snapshot = s;
	native_reads = native_releases = native_visibility = native_reserves = 0;
	native_prepared_checks = native_fault = native_pins = 0;
	native_retry_limit = native_requalifications = 0; publication_A_grant_revoked = false;
	UT_ASSERT_EQ(writer_scoped_call(&out), 2);
	UT_ASSERT(two_publish(&out, &tuple, 0, next));
	bridge_lock_override = native_lock;
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	return s;
}
/* The original ERROR ResourceOwner owns these I/O leaves, not the native
 * per-walk proof bracket. This fixture only releases its controlled pin. */
static void native_clean(Snapshot s)
{
	CurrentResourceOwner = (ResourceOwner)1;
	if (content_locked) LockBuffer(1, BUFFER_LOCK_UNLOCK);
	if (native_pins) ReleaseBuffer(1);
	UT_ASSERT_EQ(native_pins, 0);
	writer_clean(s, true);
}
UT_TEST(native_recursive_reservation_relock_preserves_consumed_B)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = native_setup(&next);
	UT_ASSERT_EQ(native_call(&next), -1);
	printf("# native error=%d reserves=%u prepared_checks=%u reads=%u requests=%u\n",
		error_code, native_reserves, native_prepared_checks, native_reads, finish_requests);
	UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(native_reserves, 1); UT_ASSERT_EQ(native_prepared_checks, 1);
	UT_ASSERT_EQ(native_reads, 1); UT_ASSERT_EQ(native_visibility, 1);
	UT_ASSERT_EQ(finish_requests, 13); UT_ASSERT_EQ(finish_reads, 13);
	for (unsigned i = 0; i < 6; i++) UT_ASSERT_EQ(two_records[i].reads, 2);
	UT_ASSERT_EQ(two_records[6].reads, 1);
	UT_ASSERT_EQ(publication_applies, 2); /* First controlled hop only. */
	UT_ASSERT_EQ(publication_cancels, 0);
	native_clean(s);
}
UT_TEST(native_recursive_relock_rejects_late_page_PCM_scope_and_grant_drift)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		ClusterHeapSuccessorProof next;
		Snapshot s = native_setup(&next);
		uint64 epoch = s->read_epoch;

		native_fault = fault;
		UT_ASSERT_EQ(native_call(&next), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(native_reserves, 1); UT_ASSERT_EQ(native_prepared_checks, 0);
		UT_ASSERT_EQ(finish_requests, 13); /* No recollection after refused relock. */
		UT_ASSERT_EQ(publication_applies, 2); UT_ASSERT_EQ(publication_cancels, 0);
		s->read_epoch = epoch; native_clean(s);
	}
}
UT_TEST(native_recursive_new_invocation_cannot_replay_consumed_B)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = native_setup(&next);

	UT_ASSERT_EQ(native_call(&next), -1); UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(native_prepared_checks, 1);
	/* Controlled ERROR pin cleanup does not reset scope/B/transition state. */
	if (content_locked) LockBuffer(1, BUFFER_LOCK_UNLOCK);
	if (native_pins) ReleaseBuffer(1);
	UT_ASSERT_EQ(native_call(&next), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(native_prepared_checks, 1); UT_ASSERT_EQ(native_reserves, 1);
	UT_ASSERT_EQ(finish_requests, 13); UT_ASSERT_EQ(publication_applies, 2);
	native_clean(s);
}
UT_TEST(native_recursive_missing_proved_B_cannot_return_vacuum_success)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = native_setup(&next);

	ItemIdSetUnused(PageGetItemId(BufferBlocks, 2));
	UT_ASSERT_EQ(native_call(&next), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(native_reserves, 0); UT_ASSERT_EQ(native_prepared_checks, 0);
	UT_ASSERT_EQ(native_visibility, 0); UT_ASSERT_EQ(native_pins, 0);
	UT_ASSERT_EQ(finish_requests, 6); UT_ASSERT_EQ(publication_applies, 2);
	native_clean(s);
}
UT_TEST(native_recursive_same_hop_repeated_prepare_relocks_do_not_reread)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = native_setup(&next);
	native_retry_limit = 3;
	UT_ASSERT_EQ(native_call(&next), -1);
	printf("# repeated error=%d requalifications=%u requests=%u reads=%u\n",
		error_code, native_requalifications, finish_requests, finish_reads);
	UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(native_prepared_checks, 3); UT_ASSERT_EQ(native_requalifications, 2);
	UT_ASSERT_EQ(native_reserves, 1);
	UT_ASSERT_EQ(finish_requests, 13); UT_ASSERT_EQ(finish_reads, 13);
	for (unsigned i = 0; i < 6; i++) UT_ASSERT_EQ(two_records[i].reads, 2);
	UT_ASSERT_EQ(two_records[6].reads, 1);
	UT_ASSERT_EQ(publication_applies, 2); native_clean(s);
}
UT_TEST(native_recursive_A_grant_revocation_invalidates_complete_pair)
{
	ClusterHeapSuccessorProof next;
	Snapshot s = native_setup(&next);
	native_fault = 8; /* B capability 55 stays fresh; revoke only A 21. */
	UT_ASSERT_EQ(native_call(&next), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(native_reserves, 1); UT_ASSERT_EQ(native_prepared_checks, 0);
	UT_ASSERT_EQ(finish_requests, 13); UT_ASSERT_EQ(publication_applies, 2);
	native_clean(s);
}
UT_TEST(native_recursive_zero_effect_attempt_restart_drops_original_pair)
{
	for (unsigned change = 1; change <= 2; change++) {
		ClusterHeapSuccessorProof next;
		Snapshot s = native_setup(&next);
		volatile int result = -1;
		native_retry_limit = 2; native_fault = 8 + change;
		error_code = 0;
		PG_TRY(); { result = mx_history_native_attempt(&relation, &next.tid, &next); }
		PG_CATCH(); { result = -1; }
		PG_END_TRY();
		UT_ASSERT_EQ(result, -1); UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(native_prepared_checks, 2); UT_ASSERT_EQ(native_requalifications, 1);
		/* Two distinct attempts: the second acquires its own complete pair. */
		UT_ASSERT_EQ(finish_requests, 20); UT_ASSERT_EQ(finish_reads, 20);
		UT_ASSERT_EQ(publication_applies, 2); native_clean(s);
	}
}
int main(void)
{
	setbuf(stdout, NULL); history_fixture_init(); UT_PLAN(7);
	UT_RUN(native_recursive_reservation_relock_preserves_consumed_B);
	UT_RUN(native_recursive_relock_rejects_late_page_PCM_scope_and_grant_drift);
	UT_RUN(native_recursive_new_invocation_cannot_replay_consumed_B);
	UT_RUN(native_recursive_missing_proved_B_cannot_return_vacuum_success);
	UT_RUN(native_recursive_same_hop_repeated_prepare_relocks_do_not_reread);
	UT_RUN(native_recursive_A_grant_revocation_invalidates_complete_pair);
	UT_RUN(native_recursive_zero_effect_attempt_restart_drops_original_pair);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
