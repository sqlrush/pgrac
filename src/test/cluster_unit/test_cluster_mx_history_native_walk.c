/* Original HOT/latest-TID loops over actual Snapshot/history/typed successor
 * consumers. Visibility/SSI, buffer I/O and admission enter/leave are leaves;
 * this is a directed native-body test, not native SQL/GCS/TT40 evidence.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_WALKER_FIXTURE
#define cluster_semantic_activation_leave walker_fixture_leave_base
#include "test_cluster_mx_history_hot_caller.c"
#undef main
#undef cluster_semantic_activation_leave
#undef HISTORY_WALKER_FIXTURE
#include "../../backend/cluster/cluster_visibility_verdict.c"

static Snapshot walk_snapshot;
static unsigned walk_visible[3], walk_conflicts, walk_predicates, walk_reads, walk_releases;
static unsigned walk_late_aba;
static bool walk_next_visible;
static FormData_pg_class walk_class;
void cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token);
void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	walker_fixture_leave_base(token);
	/* The proof was delivered, and the next native consumer must still
	 * reject changed full keys before its visibility leaf sees the tuple. */
	if (walk_late_aba == 1) HeapTupleHeaderSetXmin(hot_candidate().t_data, 112);
	if (walk_late_aba == 2)
		ClusterPageGetItlSlots(BufferBlocks)[2].undo_segment_head = uba_encode(1, 7, 3, 0);
	if (walk_late_aba == 3) ClusterPageGetItlSlots(BufferBlocks)[2].xid = 112;
	walk_late_aba = 0;
}
bool
HeapTupleSatisfiesVisibility(HeapTuple t, Snapshot snapshot, Buffer buffer)
{
	unsigned off = ItemPointerGetOffsetNumber(&t->t_self);
	UT_ASSERT(snapshot == walk_snapshot); UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(content_locked && !exclusive); UT_ASSERT(off == 1 || off == 2);
	walk_visible[off]++;
	return off == 2 && walk_next_visible;
}
bool
CheckForSerializableConflictOutNeeded(Relation rel, Snapshot snapshot)
{
	UT_ASSERT(snapshot == walk_snapshot); UT_ASSERT(content_locked && !exclusive);
	walk_conflicts++;
	return false; /* Original native SSI front door; non-serializable statement. */
}
void CheckForSerializableConflictOut(Relation rel, TransactionId xid, Snapshot s) { abort(); }
HTSV_Result HeapTupleSatisfiesVacuum(HeapTuple t, TransactionId xmin, Buffer b) { abort(); }
TransactionId SubTransGetTopmostTransaction(TransactionId xid) { abort(); }
bool TransactionIdFollowsOrEquals(TransactionId a, TransactionId b) { abort(); }
void
PredicateLockTID(Relation rel, ItemPointer tid, Snapshot snapshot, TransactionId xmin)
{
	UT_ASSERT(snapshot == walk_snapshot); UT_ASSERT_EQ(ItemPointerGetOffsetNumber(tid), 2);
	UT_ASSERT_EQ(xmin, 96); walk_predicates++;
}
GlobalVisState *GlobalVisTestFor(Relation rel) { return (GlobalVisState *)1; }
bool HeapTupleIsSurelyDead(HeapTuple tuple, GlobalVisState *state) { return false; }
Buffer
ReadBuffer(Relation rel, BlockNumber block)
{
	UT_ASSERT(rel == &relation); UT_ASSERT_EQ(block, 3); UT_ASSERT(!content_locked);
	walk_reads++; return 1;
}
void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(content_locked); LockBuffer(buffer, BUFFER_LOCK_UNLOCK); walk_releases++;
}
void TestForOldSnapshot_impl(Snapshot snapshot, Relation rel) { abort(); }
TransactionId HeapTupleGetUpdateXid(HeapTupleHeader tuple) { abort(); }
bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple) { abort(); }
static void
walk_lock(Buffer buffer, int mode)
{
	/* Latest-TID's initial SHARE precedes the descriptor capture. Inject
	 * descriptor drift only at the subsequent descriptor RPC relock. */
	if (mode == BUFFER_LOCK_SHARE && walk_reads != 0 && bridge_unlocks == 0) {
		unsigned saved = auth_descriptor_fault;
		auth_descriptor_fault = 0; hot_lock(buffer, mode); auth_descriptor_fault = saved;
	} else hot_lock(buffer, mode);
}
static Snapshot
walk_setup(bool scur)
{
	Snapshot s = read_setup(scur);
	memset(&walk_class, 0, sizeof(walk_class));
	walk_class.relpersistence = RELPERSISTENCE_PERMANENT;
	relation.rd_rel = &walk_class; /* Native all_dead path reads persistence. */
	tuple.t_data->t_infomask2 |= HEAP_HOT_UPDATED;
	hot_candidate().t_data->t_infomask2 |= HEAP_ONLY_TUPLE;
	hot_candidate().t_data->t_infomask |= HEAP_XMAX_INVALID;
	/* The original native peer-mode branch, without a test-only selector. */
	RecentXmin = 3;
	walk_snapshot = s; memset(walk_visible, 0, sizeof(walk_visible));
	walk_conflicts = walk_predicates = walk_reads = walk_releases = walk_late_aba = 0;
	walk_next_visible = true;
	bridge_lock_override = walk_lock;
	return s;
}
static int
walk_call(Snapshot actual, bool latest, bool first, bool *all_dead, ItemPointer tid)
{
	volatile int result = -1; error_code = 0; walk_snapshot = actual;
	if (latest) content_locked = false; /* The original walker acquires its pin/SHARE. */
	PG_TRY();
	{ result = mx_history_native_hot_walk(&relation, 1, actual, tid, latest, first, all_dead) ? 1 : 0; }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static void
walk_balanced(bool latest)
{
	read_balanced(); UT_ASSERT_EQ(hot_promotions, 0); UT_ASSERT_EQ(auth_waits, 0);
	UT_ASSERT_EQ(walk_reads, latest ? 1 : 0); UT_ASSERT_EQ(walk_releases, latest ? 1 : 0);
	UT_ASSERT_EQ(content_locked, !latest);
}
UT_TEST(native_walkers_consume_history_successor_under_original_share)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned scur = 0; scur < 2; ++scur) {
			Snapshot s = walk_setup(scur); ItemPointerData tid = tuple.t_self; bool dead = true;
			UT_ASSERT_EQ(walk_call(s, latest, true, &dead, &tid), 1);
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), 2);
			UT_ASSERT_EQ(walk_visible[1], 1); UT_ASSERT_EQ(walk_visible[2], 1);
			UT_ASSERT_EQ(walk_conflicts, 2); UT_ASSERT_EQ(walk_predicates, latest ? 0 : 1);
			UT_ASSERT_EQ(auth_describes, 1); UT_ASSERT_EQ(finish_reads, 3);
			walk_balanced(latest); if (!latest) UT_ASSERT(!dead); auth_clean(s);
		}
}
UT_TEST(native_walkers_restart_original_root_with_same_snapshot)
{
	for (unsigned latest = 0; latest < 2; ++latest) {
		Snapshot s = walk_setup(true); ItemPointerData tid = tuple.t_self;
		auth_descriptor_fault = 1;
		UT_ASSERT_EQ(walk_call(s, latest, true, NULL, &tid), 1);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), 2);
		UT_ASSERT_EQ(auth_describes, 2); UT_ASSERT_EQ(walk_visible[1], 2);
		UT_ASSERT_EQ(walk_visible[2], 1); UT_ASSERT_EQ(caller_enters, 2);
		UT_ASSERT_EQ(caller_leaves, 2); UT_ASSERT_EQ(caller_inflight, 0);
		UT_ASSERT_EQ(remembered, 1); auth_clean(s);
	}
}
UT_TEST(native_walkers_reject_late_successor_ABA_before_visibility)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned fault = 1; fault <= 3; ++fault) {
			Snapshot s = walk_setup(true); ItemPointerData tid = tuple.t_self;
			walk_late_aba = fault;
			UT_ASSERT_EQ(walk_call(s, latest, true, NULL, &tid), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			UT_ASSERT_EQ(walk_visible[2], 0); UT_ASSERT_EQ(walk_predicates, 0);
			read_balanced(); UT_ASSERT_EQ(hot_promotions, 0); auth_clean(s);
		}
}
UT_TEST(native_walkers_do_not_guess_snapshot_authority)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned fault = 0; fault < 3; ++fault) {
			Snapshot s = walk_setup(true), input = s; SnapshotData forged = *s;
			ItemPointerData tid = tuple.t_self;
			if (fault == 0) input = &forged;
			if (fault == 1) s->read_epoch++;
			if (fault == 2) CurrentResourceOwner = NULL;
			UT_ASSERT_EQ(walk_call(input, latest, true, NULL, &tid), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(caller_enters, 0);
			UT_ASSERT_EQ(walk_visible[2], 0); CurrentResourceOwner = (ResourceOwner)1; auth_clean(s);
		}
}
UT_TEST(native_walkers_ERROR_preserves_outer_evaluator_and_releases_admission)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned fault = 0; fault < 2; ++fault) {
			Snapshot s = walk_setup(true), actual; SCN floor; const char *reason;
			ClusterSnapshotReadScopeV1 outer; ItemPointerData tid = tuple.t_self;
			cluster_snapshot_read_enter_v1(&outer, s);
			auth_throw_describe = fault == 0;
			if (fault == 1) { finish_pending = true; history_wait_fault = 1; }
			UT_ASSERT_EQ(walk_call(s, latest, true, NULL, &tid), -1);
			read_balanced(); UT_ASSERT_EQ(walk_visible[2], 0);
			UT_ASSERT(cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason));
			UT_ASSERT(actual == s); cluster_snapshot_read_exit_v1(&outer); auth_clean(s);
		}
}
UT_TEST(native_walkers_cannot_follow_noncommitted_or_discarded_updater)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned fault = 0; fault < 3; ++fault) {
			Snapshot s = walk_setup(true); ItemPointerData tid = tuple.t_self;
			if (fault < 2) finish_outcome = fault ? CLUSTER_TX_ABORTED : CLUSTER_TX_IN_PROGRESS;
			if (fault == 2) {
				/* V1 top records always mean LIVE. Use real operation-tail
				 * bytes for a DISCARDED negative, never a fictitious V1 flag. */
				finish_native_top_records = false; finish_all_discarded = true;
			}
			UT_ASSERT_EQ(walk_call(s, latest, true, NULL, &tid), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			UT_ASSERT_EQ(walk_visible[2], 0); UT_ASSERT_EQ(auth_waits, 0);
			read_balanced(); auth_clean(s);
		}
}
UT_TEST(native_hot_second_call_skips_only_original_root)
{
	Snapshot s = walk_setup(true); ItemPointerData tid = tuple.t_self; bool dead = true;
	UT_ASSERT_EQ(walk_call(s, false, false, &dead, &tid), 1);
	UT_ASSERT_EQ(walk_visible[1], 0); UT_ASSERT_EQ(walk_visible[2], 1);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), 2); UT_ASSERT(!dead);
	walk_balanced(false); auth_clean(s);
}
UT_TEST(native_walkers_preserve_tid_when_no_version_is_visible)
{
	for (unsigned latest = 0; latest < 2; ++latest) {
		Snapshot s = walk_setup(true); ItemPointerData tid = tuple.t_self;
		walk_next_visible = false;
		UT_ASSERT_EQ(walk_call(s, latest, true, NULL, &tid), 0);
		UT_ASSERT(ItemPointerEquals(&tid, &tuple.t_self));
		UT_ASSERT_EQ(walk_visible[2], 1); UT_ASSERT_EQ(walk_predicates, 0);
		walk_balanced(latest); auth_clean(s);
	}
}
static Snapshot
walk_child_setup(bool scur)
{
	Snapshot s = walk_setup(scur);
	finish_top = 80; finish_native_top_records = false; finish_live_child_successor = true;
	for (unsigned i = 0; i < 3; ++i) ClusterPageGetItlSlots(BufferBlocks)[i].xid = 80;
	return s;
}
UT_TEST(native_walkers_follow_child_canonical_history)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned scur = 0; scur < 2; ++scur) {
			Snapshot s = walk_child_setup(scur); ItemPointerData tid = tuple.t_self; bool dead = true;
			UT_ASSERT_EQ(walk_call(s, latest, true, &dead, &tid), 1);
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), 2);
			UT_ASSERT_EQ(walk_visible[2], 1); UT_ASSERT_EQ(finish_reads, 3);
			walk_balanced(latest); auth_clean(s);
		}
}
UT_TEST(native_walkers_reject_child_late_ABA_before_visibility)
{
	for (unsigned latest = 0; latest < 2; ++latest)
		for (unsigned fault = 1; fault <= 3; ++fault) {
			Snapshot s = walk_child_setup(true); ItemPointerData tid = tuple.t_self;
			walk_late_aba = fault;
			UT_ASSERT_EQ(walk_call(s, latest, true, NULL, &tid), -1);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			UT_ASSERT_EQ(walk_visible[2], 0); UT_ASSERT_EQ(finish_reads, 3);
			read_balanced(); auth_clean(s);
		}
}
#ifdef HISTORY_FULL_FIXTURE
int history_full_unused_walk_main(void);
#define main history_full_unused_walk_main
#endif
int main(void)
{
	history_fixture_init(); UT_PLAN(10);
	UT_RUN(native_walkers_consume_history_successor_under_original_share);
	UT_RUN(native_walkers_restart_original_root_with_same_snapshot);
	UT_RUN(native_walkers_reject_late_successor_ABA_before_visibility);
	UT_RUN(native_walkers_do_not_guess_snapshot_authority);
	UT_RUN(native_walkers_ERROR_preserves_outer_evaluator_and_releases_admission);
	UT_RUN(native_walkers_cannot_follow_noncommitted_or_discarded_updater);
	UT_RUN(native_hot_second_call_skips_only_original_root);
	UT_RUN(native_walkers_preserve_tid_when_no_version_is_visible);
	UT_RUN(native_walkers_follow_child_canonical_history);
	UT_RUN(native_walkers_reject_child_late_ABA_before_visibility);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
