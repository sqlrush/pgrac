/* PGRAC: execute actual precommit/publication callbacks with transport fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <setjmp.h>
#include "../../backend/utils/cache/inval.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true;
bool cluster_shared_catalog;
int cluster_node_id = 0;
Oid MyDatabaseId = 5;
static ClusterConf conf = { .node_count = 2 };
ClusterConf *ClusterConfShmem = &conf;
static int observed_error, send_calls, prepare_calls, clear_calls, sync_calls;
static ClusterSinvalAckResult send_result;
static jmp_buf error_jump;
static int native_callbacks;
MemoryContext TopTransactionContext;
static int local_sends;
static SharedInvalidationMessage published[8];
static int published_count;
volatile uint32 CritSectionCount;

int GetCurrentTransactionNestLevel(void) { return 1; }
void AssertCouldGetRelation(void) { }
void *MemoryContextAlloc(MemoryContext context, Size size) { return malloc(size); }
void *MemoryContextAllocZero(MemoryContext context, Size size) { return calloc(1, size); }
void *repalloc(void *pointer, Size size) { return realloc(pointer, size); }
void RelationCacheInitFilePreInvalidate(void) { native_callbacks++; }
void RelationCacheInitFilePostInvalidate(void) { native_callbacks++; }
void SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n) { local_sends++; }

void InvalidateCatalogSnapshot(void) { native_callbacks++; }
void SysCacheInvalidate(int cacheId, uint32 hashValue) { native_callbacks++; }
void CatalogCacheFlushCatalog(Oid catId) { native_callbacks++; }
void RelationCacheInvalidate(bool debug_discard) { native_callbacks++; }
void RelationCacheInvalidateEntry(Oid relid) { native_callbacks++; }
void smgrcloserellocator(RelFileLocatorBackend rlocator) { native_callbacks++; }
void RelationMapInvalidate(bool shared) { native_callbacks++; }

UT_TEST(test_native_si_ignores_versioned_ko_commit_marker)
{
	SharedInvalidationMessage msg = { 0 };

	/* The operation owner is the authenticated publication consumer. Native
	 * replay, command-end and abort cache processing must never retire it. */
	msg.ko.id = SHAREDINVALKO_COMMIT_ID;
	msg.ko.version = SHAREDINVALKO_COMMIT_VERSION;
	msg.ko.batch_lo = 17;
	observed_error = native_callbacks = 0;
	if (setjmp(error_jump) == 0)
		LocalExecuteInvalidationMessage(&msg);
	UT_ASSERT_EQ(observed_error, 0);
	UT_ASSERT_EQ(native_callbacks, 0);
}

UT_TEST(test_native_si_rejects_malformed_ko_commit_marker)
{
	for (int variant = 0; variant < 6; variant++) {
		SharedInvalidationMessage msg = { 0 };
		msg.ko.id = SHAREDINVALKO_COMMIT_ID;
		msg.ko.version = SHAREDINVALKO_COMMIT_VERSION;
		msg.ko.batch_lo = 17;
		switch (variant) {
			case 0: msg.ko.version++; break;
			case 1: msg.ko.flags = 1; break;
			case 2: msg.ko.reserved = 1; break;
			case 3: msg.ko.batch_lo = 0; break;
			case 4: msg.ko.batch_lo = msg.ko.batch_hi = UINT32_MAX; break;
			case 5: msg.id = -7; break;
		}
		observed_error = native_callbacks = 0;
		if (setjmp(error_jump) == 0)
			LocalExecuteInvalidationMessage(&msg);
		UT_ASSERT_EQ(observed_error, FATAL);
		UT_ASSERT_EQ(native_callbacks, 0);
	}
}

bool
errstart(int level, const char *domain)
{
	observed_error = level;
	if (level >= ERROR)
		longjmp(error_jump, 1);
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}
int
errmsg(const char *format, ...)
{
	return 0;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
void
ForceSyncCommit(void)
{
	sync_calls++;
}
void
cluster_sinval_prepare_commit(void)
{
	prepare_calls++;
}
void
cluster_sinval_clear_commit(void)
{
	clear_calls++;
}
ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	Assert(n > 0);
	send_calls++;
	Assert(n <= lengthof(published));
	published_count = n;
	memcpy(published, msgs, n * sizeof(*msgs));
	return send_result;
}

UT_TEST(test_ko_marker_uses_transaction_commit_publication_only)
{
	for (int commit = 0; commit <= 1; commit++) {
		TransInvalidationInfo info = { 0 };
		SharedInvalidationMessage storage[8] = { 0 };
		uint64 batch = UINT64_C(0x1020304050607080);

		info.my_level = 1;
		transInvalInfo = &info;
		InvalMessageArrays[RelCacheMsgs].msgs = storage;
		InvalMessageArrays[RelCacheMsgs].maxmsgs = lengthof(storage);
		cluster_shared_catalog = true;
		send_result = CLUSTER_SINVAL_ACK_DONE;
		send_calls = local_sends = published_count = native_callbacks = 0;
		observed_error = 0;
		CacheInvalidateKoCommit(batch);
		UT_ASSERT_EQ(send_calls, 0);
		UT_ASSERT_EQ(local_sends, 0);
		UT_ASSERT_EQ(NumMessagesInGroup(&info.ii.CurrentCmdInvalidMsgs), 1);
		/* Exercise the actual local command-end callback before termination. */
		ProcessInvalidationMessages(&info.ii.CurrentCmdInvalidMsgs,
									LocalExecuteInvalidationMessage);
		AppendInvalidationMessages(&info.PriorCmdInvalidMsgs, &info.ii.CurrentCmdInvalidMsgs);
		PreCommit_ClusterInval();
		AtEOXact_Inval(commit != 0);
		UT_ASSERT_EQ(send_calls, commit);
		UT_ASSERT_EQ(local_sends, commit);
		UT_ASSERT_EQ(native_callbacks, 0);
		UT_ASSERT_EQ(observed_error, 0);
		UT_ASSERT(transInvalInfo == NULL);
		if (commit) {
			UT_ASSERT_EQ(published_count, 1);
			UT_ASSERT(SharedInvalKoCommitIsValid(&published[0].ko));
			UT_ASSERT_EQ(SharedInvalKoCommitBatchId(&published[0].ko), batch);
		}
		InvalMessageArrays[RelCacheMsgs].msgs = NULL;
		InvalMessageArrays[RelCacheMsgs].maxmsgs = 0;
	}
}

UT_TEST(test_postcommit_error_policy_matches_publication_mode)
{
	SharedInvalidationMessage msg = { 0 };
	cluster_shared_catalog = false;
	send_result = CLUSTER_SINVAL_ACK_TIMEOUT;
	if (setjmp(error_jump) == 0)
		cluster_aware_send_shared_invalid_messages(&msg, 1);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(observed_error, 0);
	cluster_shared_catalog = true;
	if (setjmp(error_jump) == 0)
		cluster_aware_send_shared_invalid_messages(&msg, 1);
	UT_ASSERT_EQ(observed_error, PANIC);
	observed_error = 0;
	send_result = CLUSTER_SINVAL_ACK_DONE;
	if (setjmp(error_jump) == 0)
		cluster_aware_send_shared_invalid_messages(&msg, 1);
	UT_ASSERT_EQ(observed_error, 0);
}

UT_TEST(test_precommit_snapshots_only_shared_pending_catalog_changes)
{
	TransInvalidationInfo info = { 0 };
	transInvalInfo = &info;
	cluster_shared_catalog = true;
	PreCommit_ClusterInval();
	UT_ASSERT_EQ(prepare_calls, 0);
	info.ii.CurrentCmdInvalidMsgs.nextmsg[CatCacheMsgs] = 1;
	PreCommit_ClusterInval();
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(sync_calls, 1);
	cluster_shared_catalog = false;
	PreCommit_ClusterInval();
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(clear_calls, 3);
	transInvalInfo = NULL;
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_postcommit_error_policy_matches_publication_mode);
	UT_RUN(test_precommit_snapshots_only_shared_pending_catalog_changes);
	UT_RUN(test_native_si_ignores_versioned_ko_commit_marker);
	UT_RUN(test_native_si_rejects_malformed_ko_commit_marker);
	UT_RUN(test_ko_marker_uses_transaction_commit_publication_only);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
