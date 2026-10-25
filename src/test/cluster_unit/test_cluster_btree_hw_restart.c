/*-------------------------------------------------------------------------
 *
 * test_cluster_btree_hw_restart.c
 *    Execute the insertion owner's allocation restart and wait cleanup.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_btree_hw_restart.c
 *
 * NOTES
 *    Search, uniqueness, buffer and GES boundaries are controlled fixtures.
 *    The insertion owner is extracted from the production source unchanged.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_content_trace.h"
#include "access/nbtree.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_tx_enqueue.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
bool cluster_enabled = true, cluster_shared_config = true;
bool cluster_tx_enqueue_wait_enabled = true;
bool cluster_trace_tx_identity;
void cluster_content_trace_wait_set(ClusterContentTraceWait kind, const ClusterWaitTrace *wait)
{ (void)kind; (void)wait; }
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
int cluster_ges_request_timeout_ms = 1000;
int NBuffers = 2, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile sig_atomic_t InterruptPending;
static PGAlignedBlock pages[2];
static RelationData relation;
static FormData_pg_class form;
static SMgrRelationData storage;
static IndexTupleData item;
static unsigned searches, unique_checks, locations, inserts, hw_acquires, hw_releases;
static unsigned ko_begins, ko_ends, ko_aborts, waits, ko_waits, freestacks, unlock_all;
static unsigned content, pins, mode, fault, ko_retries;
static bool full, held, ko_live, waited, caught;

void ExceptionalCondition(const char *c, const char *f, int n) { printf("# %s %s:%d\n", c, f, n); abort(); }
void pg_re_throw(void) { if (PG_exception_stack) siglongjmp(*PG_exception_stack, 1); abort(); }
bool errstart(int level, const char *domain) { (void)domain; return level >= ERROR; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errmsg(const char *fmt, ...) { (void)fmt; return 0; }
int errdetail(const char *fmt, ...) { (void)fmt; return 0; }
int errhint(const char *fmt, ...) { (void)fmt; return 0; }
int errcode(int code) { (void)code; return 0; }
void errfinish(const char *file, int line, const char *func) { (void)file; (void)line; (void)func; pg_re_throw(); }
void *palloc(Size n) { return malloc(n); }
void *palloc0(Size n) { return calloc(1, n); }
void pfree(void *p) { free(p); }
bool RecoveryInProgress(void) { return false; }
int cluster_smgr_which_for(RelFileLocator loc, BackendId backend) { (void)loc; (void)backend; return 1; }
void ProcessInterrupts(void) { pg_re_throw(); }
void LWLockReleaseAll(void) { content = 0; unlock_all++; }
void UnlockBuffers(void) { UT_ASSERT_EQ(content, 0); }
SMgrRelation smgropen(RelFileLocator loc, BackendId backend) { (void)loc; (void)backend; return &storage; }
void smgrsetowner(SMgrRelation *owner, SMgrRelation rel) { *owner = rel; }

void
_bt_get_version_identity(Relation rel, ClusterSpaceIdentity *out)
{
	UT_ASSERT_EQ(content, 0);
	UT_ASSERT(!held);
	memset(out, 0, sizeof(*out));
	out->key.locator = rel->rd_locator;
	out->state = CLUSTER_SPACE_IDENTITY_LIVE;
}
BTScanInsert
_bt_mkscankey(Relation rel, IndexTuple tuple)
{
	BTScanInsert key = palloc0(sizeof(BTScanInsertData));
	(void)rel;
	key->heapkeyspace = true;
	key->scantid = &tuple->t_tid;
	return key;
}
void
cluster_hw_resid_encode(RelFileLocator loc, ForkNumber fork, ClusterResId *out)
{
	UT_ASSERT(RelFileLocatorEquals(loc, relation.rd_locator));
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	memset(out, 0, sizeof(*out));
}
bool
cluster_hw_lock(const ClusterResId *resid, HwLock *lock)
{
	(void)resid;
	UT_ASSERT(ko_live && !held && content == 0 && pins == 0);
	hw_acquires++;
	if (fault == 1)
		pg_re_throw();
	if (fault == 4)
		return false;
	lock->held = lock->coordinated = held = true;
	return true;
}
void
cluster_hw_unlock(HwLock *lock)
{
	if (!lock->held)
		return;
	UT_ASSERT(held && ko_live && content == 0);
	hw_releases++;
	lock->held = lock->coordinated = held = false;
}
bool
cluster_ko_extend_begin_v3(RelFileLocator loc, ForkNumber fork, uint64 *serial, bool *retry)
{
	(void)loc;
	UT_ASSERT(fork == MAIN_FORKNUM && !held && !ko_live && content == 0 && pins == 0);
	ko_begins++;
	*serial = 0;
	if (fault == 3) { *retry = false; return false; }
	*retry = ko_retries != 0;
	if (ko_retries != 0) { ko_retries--; return false; }
	*serial = 42;
	ko_live = true;
	return true;
}
void
cluster_ko_peer_admission_wait(long timeout)
{
	UT_ASSERT(timeout > 0 && !held && !ko_live && content == 0 && pins == 0);
	ko_waits++;
}
bool
cluster_ko_extend_end_v3(uint64 serial)
{
	UT_ASSERT(serial == 42 && ko_live && !held && content == 0 && pins == 0);
	ko_ends++;
	ko_live = false;
	return true;
}
void
cluster_ko_extend_abort_v3(uint64 serial)
{
	if (serial == 0)
		return;
	UT_ASSERT(serial == 42 && ko_live && content == 0 && !held);
	ko_aborts++;
	/* The real reference remains until ResourceOwner AFTER_LOCKS. */
}
void
_bt_relbuf(Relation rel, Buffer buf)
{
	(void)rel;
	UT_ASSERT(BufferIsValid(buf) && content > 0 && pins > 0);
	content--; pins--;
}
BlockNumber BufferGetBlockNumber(Buffer buf) { return buf; }
void _bt_freestack(BTStack stack) { freestacks++; pfree(stack); }
void CheckForSerializableConflictIn(Relation rel, ItemPointer tid, BlockNumber block) { (void)rel; (void)tid; (void)block; }
static BTStack
_bt_search_insert(Relation rel, Relation heap, BTInsertState state)
{
	BTStack stack = palloc0(sizeof(BTStackData));
	(void)rel; (void)heap;
	UT_ASSERT(content == 0 && pins == 0 && !state->bounds_valid && state->postingoff == 0);
	UT_ASSERT(state->itup_key->scantid == NULL);
	searches++;
	state->buf = searches == 1 ? 1 : 2;
	content++; pins++;
	return stack;
}
static TransactionId
_bt_check_unique(Relation rel, BTInsertState state, Relation heap, IndexUniqueCheck check,
				 bool *unique, uint32 *speculative, bool *barrier, ItemPointer tid,
				 bool *remote, ClusterTxLocator *locator)
{
	(void)rel; (void)heap; (void)check; (void)locator;
	UT_ASSERT(content == 1 && pins == 1 && state->itup_key->scantid == NULL);
	unique_checks++;
	*unique = true; *speculative = 0;
	state->bounds_valid = true;
	state->postingoff = 3;
	if (mode && held && !waited) {
		waited = true;
		if (mode == 4) { *barrier = true; ItemPointerSet(tid, 3, 1); }
		else if (mode == 3) *remote = true;
		else { *speculative = mode == 2 ? 7 : 0; return 99; }
	}
	return InvalidTransactionId;
}
static void wait_boundary(void) { UT_ASSERT(content == 0 && pins == 0 && !held && !ko_live); waits++; }
void XactLockTableWait(TransactionId xid, Relation rel, ItemPointer tid, XLTW_Oper op) { (void)xid; (void)rel; (void)tid; (void)op; wait_boundary(); }
void SpeculativeInsertionWait(TransactionId xid, uint32 token) { (void)xid; (void)token; wait_boundary(); }
ClusterTxwResult cluster_tx_enqueue_wait_exact(const ClusterTxLocator *loc, int ms, ClusterTxResolveReason *reason) { (void)loc; (void)ms; (void)reason; wait_boundary(); return CLUSTER_TXW_RESOLVED; }
const char *cluster_tx_resolve_reason_name(ClusterTxResolveReason reason) { (void)reason; return "fixture"; }
static TableIndexFetchTupleResult
fixture_warm(Relation rel, ItemPointer tid, Snapshot snap, ...)
{
	(void)rel; (void)tid; (void)snap; wait_boundary();
	return TABLE_INDEX_FETCH_FOUND;
}
#define table_index_fetch_tuple_check_barrier_aware fixture_warm
static OffsetNumber
_bt_findinsertloc(Relation rel, BTInsertState state, bool unique, bool unchanged, BTStack stack, Relation heap)
{
	(void)rel; (void)unique; (void)unchanged; (void)stack; (void)heap;
	locations++;
	if (fault == 2 && held)
		pg_re_throw();
	return state->buf == 1 ? 1 : 5;
}
static void
_bt_insertonpg(Relation rel, Relation heap, BTScanInsert key, Buffer buf, Buffer child,
			   BTStack stack, IndexTuple tuple, Size size, OffsetNumber off, int posting,
			   bool only, ...)
{
	(void)heap; (void)key; (void)child; (void)stack; (void)tuple; (void)size; (void)posting; (void)only;
	if (full) {
		UT_ASSERT(held && ko_live);
		UT_ASSERT(buf == 2 && off == 5);
	}
	inserts++;
	_bt_relbuf(rel, buf);
}
#include "test_cluster_btree_hw_owner.inc"

static void
reset_case(void)
{
	memset(&relation, 0, sizeof(relation)); memset(&form, 0, sizeof(form));
	memset(&item, 0, sizeof(item)); memset(pages, 0, sizeof(pages));
	relation.rd_rel = &form; relation.rd_backend = InvalidBackendId;
	relation.rd_locator = (RelFileLocator){1663, 5, 900};
	form.relpersistence = RELPERSISTENCE_PERMANENT; form.relkind = RELKIND_INDEX;
	item.t_info = MAXALIGN(sizeof(item)); ItemPointerSet(&item.t_tid, 1, 1);
	for (int i = 0; i < 2; i++) ((PageHeader)pages[i].data)->pd_lower = SizeOfPageHeaderData;
	/* Both searched pages force the same allocation preflight. */
	for (int i = 0; i < 2; i++) ((PageHeader)pages[i].data)->pd_upper = ((PageHeader)pages[i].data)->pd_lower;
	BufferBlocks = pages[0].data;
	searches = unique_checks = locations = inserts = hw_acquires = hw_releases = 0;
	ko_begins = ko_ends = ko_aborts = waits = ko_waits = freestacks = unlock_all = 0;
	content = pins = mode = fault = ko_retries = 0;
	full = held = ko_live = waited = caught = false;
	cluster_shared_config = true;
}
static void run_owner(void) { (void)_bt_doinsert(&relation, &item, UNIQUE_CHECK_YES, false, &relation); }
UT_TEST(test_split_researches_and_rechecks_unique)
{
	reset_case(); full = true; run_owner();
	UT_ASSERT(searches == 2 && unique_checks == 2 && locations == 2 && inserts == 1);
	UT_ASSERT(hw_acquires == 1 && hw_releases == 1 && ko_begins == 1 && ko_ends == 1);
	UT_ASSERT(content == 0 && pins == 0 && freestacks == 2);
}
UT_TEST(test_no_allocation_has_no_enqueue)
{
	reset_case();
	for (int i = 0; i < 2; i++) ((PageHeader)pages[i].data)->pd_upper = BLCKSZ - MAXALIGN(sizeof(BTPageOpaqueData));
	run_owner();
	UT_ASSERT(searches == 1 && inserts == 1 && hw_acquires == 0 && ko_begins == 0);
}
UT_TEST(test_tx_and_barrier_waits_release_allocation_owner)
{
	for (unsigned wait_mode = 1; wait_mode <= 4; wait_mode++) {
		reset_case(); full = true; mode = wait_mode; run_owner();
		UT_ASSERT(searches == 3 && unique_checks == 3 && waits == 1 && inserts == 1);
		UT_ASSERT(hw_acquires == 2 && hw_releases == 2 && ko_begins == 2 && ko_ends == 2);
		UT_ASSERT(content == 0 && pins == 0 && freestacks == 3);
	}
}
UT_TEST(test_admission_retries_outside_hw_and_content)
{
	reset_case(); full = true; ko_retries = 2; run_owner();
	UT_ASSERT(ko_begins == 3 && ko_waits == 2 && hw_acquires == 1 && ko_ends == 1);
}
UT_TEST(test_native_profile_keeps_no_allocation_enqueue)
{
	reset_case(); cluster_shared_config = false; run_owner();
	UT_ASSERT(searches == 1 && inserts == 1 && hw_acquires == 0 && ko_begins == 0);
	reset_case(); cluster_interconnect_tier = CLUSTER_IC_TIER_2; run_owner();
	UT_ASSERT(searches == 1 && inserts == 1 && hw_acquires == 0 && ko_begins == 0);
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
}
UT_TEST(test_unprovable_admission_or_hw_never_mutates)
{
	for (unsigned point = 3; point <= 4; point++) {
		reset_case(); full = true; fault = point;
		PG_TRY(); { run_owner(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught && !held && content == 0 && pins == 0 && inserts == 0);
		UT_ASSERT(ko_ends == 0 && ko_waits == 0 && hw_releases == 0);
		UT_ASSERT_EQ(ko_aborts, point == 4 ? 1 : 0);
	}
}
UT_TEST(test_error_unwinds_before_original_ko_recovery)
{
	for (unsigned point = 1; point <= 2; point++) {
		reset_case(); full = true; fault = point;
		PG_TRY(); { run_owner(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught && !held && content == 0 && ko_live && ko_aborts == 1 && ko_ends == 0);
		UT_ASSERT_EQ(hw_releases, point == 2 ? 1 : 0);
	}
}
int main(void)
{
	UT_PLAN(7);
	UT_RUN(test_split_researches_and_rechecks_unique);
	UT_RUN(test_no_allocation_has_no_enqueue);
	UT_RUN(test_tx_and_barrier_waits_release_allocation_owner);
	UT_RUN(test_admission_retries_outside_hw_and_content);
	UT_RUN(test_native_profile_keeps_no_allocation_enqueue);
	UT_RUN(test_unprovable_admission_or_hw_never_mutates);
	UT_RUN(test_error_unwinds_before_original_ko_recovery);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
