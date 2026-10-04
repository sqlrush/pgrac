/*-------------------------------------------------------------------------
 * test_cluster_smgr_drop.c
 *    Drive the original smgr DROP caller across its owner and I/O boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_smgr_drop.c
 *
 * NOTES
 *    PGRAC-original caller tests. The production smgr and structure codec run
 *    here; KO and physical-result boundaries are supplied by the fixture.
 *    test_cluster_shared_fs_drop separately exercises real files and syscalls.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_smgr.h"
#include "utils/memutils.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_shared_config = true;
bool cluster_shared_catalog = true;
bool IsBinaryUpgrade = false;
volatile uint32 InterruptHoldoffCount;
volatile uint32 QueryCancelHoldoffCount;
volatile uint32 CritSectionCount;
MemoryContext CurrentMemoryContext;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static int error_level, warnings, rethrows;
static ErrorData copied_error;
static bool owner_present, observation_ok, physical_ok, setter_ok;
static int throw_at;
static bool primitive_finished;
static int owner_reads, physical_calls, setter_calls, legacy_unlinks, legacy_truncates;
static int closed_handles, forget_count, unlink_requests;
static char trace[128];
static unsigned trace_length;
static ClusterSpaceStructureChange change;
static ClusterPageWalBindingV1 binding;
static uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
static int owner_storage;
static ClusterSharedFsOps active_ops;

/* The API is supplied by the original KO owner, never by a production stub. */
bool cluster_ko_shared_observe_drop_v2(ClusterKoCompletionV2 *completion);

static void
event(char c)
{
	if (trace_length + 1 >= sizeof(trace))
		abort();
	trace[trace_length++] = c;
	trace[trace_length] = '\0';
}

static void
raise_error(void)
{
	if (PG_exception_stack == NULL)
		abort();
	/* PostgreSQL ERROR unwinding resets these before entering PG_CATCH. */
	InterruptHoldoffCount = QueryCancelHoldoffCount = CritSectionCount = 0;
	siglongjmp(*PG_exception_stack, 1);
}

bool errstart(int level, const char *domain pg_attribute_unused())
{
	error_level = level;
	return true;
}
bool errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
	const char *function pg_attribute_unused())
{
	if (error_level >= ERROR)
		raise_error();
	if (error_level == WARNING)
		warnings++;
}
int errcode(int value pg_attribute_unused()) { return 0; }
int errcode_for_file_access(void) { return 0; }
int errmsg(const char *format pg_attribute_unused(), ...) { return 0; }
int errmsg_internal(const char *format pg_attribute_unused(), ...) { return 0; }
int errdetail(const char *format pg_attribute_unused(), ...) { return 0; }
int errhint(const char *format pg_attribute_unused(), ...) { return 0; }
ErrorData *CopyErrorData(void) { return &copied_error; }
void FlushErrorState(void) {}
void ThrowErrorData(ErrorData *error)
{
	if (error->elevel != WARNING)
		abort();
	warnings++;
}
void FreeErrorData(ErrorData *error pg_attribute_unused()) {}
void pg_re_throw(void)
{
	rethrows++;
	raise_error();
	abort();
}
void ExceptionalCondition(const char *condition pg_attribute_unused(),
	const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

void cluster_write_fence_reject_if_fenced(const char *op pg_attribute_unused()) {}
void cluster_mrp_standby_shared_write_gate(const char *op pg_attribute_unused()) {}

const ClusterSharedFsOps *cluster_shared_fs_get_active_ops(void) { return &active_ops; }
bool cluster_shared_fs_exists(RelFileLocator locator pg_attribute_unused(),
	ForkNumber forknum pg_attribute_unused()) { return true; }
void cluster_shared_fs_open_existing(RelFileLocator locator pg_attribute_unused(),
	ForkNumber forknum pg_attribute_unused(), ClusterSharedFsHandle **out)
{
	*out = (ClusterSharedFsHandle *)&owner_storage;
}
void cluster_shared_fs_close(ClusterSharedFsHandle *handle pg_attribute_unused())
{
	closed_handles++;
	event('C');
}
void cluster_shared_fs_truncate(ClusterSharedFsHandle *handle pg_attribute_unused(),
	BlockNumber blocks)
{
	UT_ASSERT_EQ(blocks, 0);
	legacy_truncates++;
}
void cluster_shared_fs_unlink(RelFileLocator locator pg_attribute_unused(),
	ForkNumber forknum pg_attribute_unused())
{
	legacy_unlinks++;
}
bool RegisterSyncRequest(const FileTag *tag, SyncRequestType type, bool retry)
{
	UT_ASSERT(retry);
	UT_ASSERT_EQ(tag->handler, SYNC_HANDLER_CLUSTER_SHARED);
	if (type == SYNC_FORGET_REQUEST) {
		forget_count++;
		event('F');
		if (throw_at == 3)
			raise_error();
	} else if (type == SYNC_UNLINK_REQUEST) {
		UT_ASSERT_EQ(tag->forknum, MAIN_FORKNUM);
		unlink_requests++;
		event('U');
	} else
		abort();
	return true;
}

bool cluster_ko_shared_pending_drop_v2(RelFileLocator locator, ClusterKoCompletionV2 **out)
{
	owner_reads++;
	UT_ASSERT(*out == NULL);
	UT_ASSERT(RelFileLocatorEquals(locator, change.identity.result.key.locator));
	event('B');
	if (throw_at == 1)
		raise_error();
	if (!owner_present)
		return false;
	*out = (ClusterKoCompletionV2 *)&owner_storage;
	return true;
}
bool cluster_ko_shared_space_observation_v2(const ClusterKoCompletionV2 *completion,
	struct ClusterPageWalBindingV1 *terminal, void *bytes, Size length)
{
	UT_ASSERT(completion == (ClusterKoCompletionV2 *)&owner_storage);
	UT_ASSERT_EQ(length, sizeof(wal));
	event('O');
	if (throw_at == 2)
		raise_error();
	if (!observation_ok)
		return false;
	*terminal = binding;
	memcpy(bytes, wal, sizeof(wal));
	return true;
}
bool cluster_shared_fs_sharedfs_drop_durable(const ClusterSpaceIdentity *identity, uint64 token)
{
	physical_calls++;
	UT_ASSERT(memcmp(identity, &change.identity.result, sizeof(*identity)) == 0);
	UT_ASSERT_EQ(token, change.identity.result_token);
	UT_ASSERT_EQ(forget_count, MAX_FORKNUM);
	UT_ASSERT_EQ(setter_calls, 0);
	UT_ASSERT_EQ(unlink_requests, 0);
	event('P');
	if (throw_at == 4)
		raise_error();
	primitive_finished = physical_ok;
	return physical_ok;
}
bool cluster_ko_shared_observe_drop_v2(ClusterKoCompletionV2 *completion)
{
	setter_calls++;
	UT_ASSERT(completion == (ClusterKoCompletionV2 *)&owner_storage);
	UT_ASSERT(primitive_finished);
	UT_ASSERT_EQ(unlink_requests, 0);
	event('S');
	if (throw_at == 5)
		raise_error();
	return setter_ok;
}

#include "../../backend/cluster/storage/cluster_smgr.c"

static ClusterSmgrRelationState open_state;
void *hash_search(HTAB *table pg_attribute_unused(), const void *key pg_attribute_unused(),
	HASHACTION action, bool *found)
{
	if (found != NULL)
		*found = true;
	if (action == HASH_FIND)
		return &open_state;
	if (action != HASH_REMOVE)
		abort();
	cluster_smgr_relations = NULL;
	return NULL;
}

static void
reset_drop(bool tombstone)
{
	memset(&change, 0, sizeof(change));
	change.identity.expected.key.system_identifier = 7;
	change.identity.expected.key.database_incarnation = 9;
	memset(change.identity.expected.key.storage_uuid, 0xa7, 16);
	change.identity.expected.key.locator = (RelFileLocator){DEFAULTTABLESPACE_OID, 5, 16384};
	memset(change.identity.expected.incarnation, 0x31, 16);
	change.identity.expected.sequence = 1;
	change.identity.expected.operation = 10;
	change.identity.expected.state = CLUSTER_SPACE_IDENTITY_LIVE;
	change.identity.result = change.identity.expected;
	change.identity.result.sequence++;
	change.identity.result.operation = change.identity.result_token = 20;
	change.identity.before_token = 10;
	change.identity.action = tombstone ? CLUSTER_SPACE_WAL_TOMBSTONE : CLUSTER_SPACE_WAL_TRUNCATE;
	change.identity.nblocks = tombstone ? InvalidBlockNumber : 4;
	if (tombstone)
		change.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	else
		change.identity.result.incarnation[0]++;
	change.reservation.action = tombstone ? CLUSTER_SPACE_RESERVATION_TOMBSTONE : CLUSTER_SPACE_RESERVATION_RESET;
	change.reservation.before.identity = change.identity.expected;
	change.reservation.before.next_block = 8;
	change.reservation.before_token = 10;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result.next_block = tombstone ? 8 : 4;
	change.reservation.first_block = tombstone ? 0 : 4;
	change.reservation.result_token = 20;
	if (!cluster_space_structure_wal_encode(&change, wal, sizeof(wal)))
		abort();
	memset(&binding, 0, sizeof(binding));
	binding.identity.system_identifier = change.identity.result.key.system_identifier;
	memcpy(binding.identity.storage_uuid, change.identity.result.key.storage_uuid, 16);
	binding.identity.locator = change.identity.result.key.locator;
	binding.identity.forknum = SPACE_FORKNUM;
	binding.source.claim.database_incarnation = change.identity.result.key.database_incarnation;
	memcpy(binding.version.segment_incarnation, change.identity.result.incarnation, 16);
	binding.version.mutation_token = 20;
	binding.rmid = RM_XACT_ID;
	binding.info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	binding.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	cluster_shared_config = cluster_shared_catalog = true;
	IsBinaryUpgrade = false;
	owner_present = observation_ok = physical_ok = setter_ok = true;
	active_ops.id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	throw_at = error_level = warnings = rethrows = 0;
	owner_reads = physical_calls = setter_calls = legacy_unlinks = legacy_truncates = 0;
	closed_handles = forget_count = unlink_requests = 0;
	trace_length = 0;
	trace[0] = '\0';
	primitive_finished = false;
	cluster_smgr_relations = NULL;
	memset(&open_state, 0, sizeof(open_state));
	InterruptHoldoffCount = 2;
	QueryCancelHoldoffCount = 1;
	CritSectionCount = 0;
	PG_exception_stack = NULL;
}

static RelFileLocatorBackend
locator(void)
{
	return (RelFileLocatorBackend){change.identity.expected.key.locator, InvalidBackendId};
}

static void
run_unlink(RelFileLocatorBackend target, ForkNumber forknum, bool redo)
{
	volatile bool escaped = false;

	PG_TRY();
	{
		cluster_smgr_unlink(target, forknum, redo);
	}
	PG_CATCH();
	{
		escaped = true;
		FlushErrorState();
	}
	PG_END_TRY();
	UT_ASSERT(!escaped);
}

UT_TEST(test_committed_drop_reports_only_after_exact_physical_result)
{
	reset_drop(true);
	cluster_smgr_relations = (HTAB *)&owner_storage;
	for (ForkNumber f = 0; f <= MAX_FORKNUM; f++)
		open_state.fork_handles[f] = (ClusterSharedFsHandle *)&owner_storage;
	run_unlink(locator(), InvalidForkNumber, false);
	UT_ASSERT_EQ(closed_handles, MAX_FORKNUM + 1);
	UT_ASSERT_EQ(owner_reads, 1);
	UT_ASSERT_EQ(physical_calls, 1);
	UT_ASSERT_EQ(setter_calls, 1);
	UT_ASSERT_EQ(unlink_requests, 1);
	UT_ASSERT_EQ(legacy_unlinks + legacy_truncates, 0);
	UT_ASSERT_EQ(warnings, 0);
	UT_ASSERT(strstr(trace, "B") != NULL && strrchr(trace, 'C') != NULL
		&& strstr(trace, "B") > strrchr(trace, 'C'));
	UT_ASSERT(strstr(trace, "PSU") != NULL);
	UT_ASSERT(cluster_smgr_relations == NULL);
}

UT_TEST(test_failed_physical_drop_never_reports_or_falls_back)
{
	reset_drop(true);
	physical_ok = false;
	run_unlink(locator(), InvalidForkNumber, false);
	UT_ASSERT_EQ(physical_calls, 1);
	UT_ASSERT_EQ(setter_calls, 0);
	UT_ASSERT_EQ(unlink_requests, 0);
	UT_ASSERT_EQ(legacy_unlinks + legacy_truncates, 0);
	UT_ASSERT_EQ(warnings, 1);
}

UT_TEST(test_rejected_owner_result_keeps_main_reserved)
{
	reset_drop(true);
	setter_ok = false;
	run_unlink(locator(), InvalidForkNumber, false);
	UT_ASSERT_EQ(physical_calls, 1);
	UT_ASSERT_EQ(setter_calls, 1);
	UT_ASSERT_EQ(unlink_requests, 0);
	UT_ASSERT_EQ(legacy_unlinks + legacy_truncates, 0);
	UT_ASSERT_EQ(warnings, 1);
}

UT_TEST(test_unproven_drop_input_never_starts_physical_removal)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		reset_drop(fault != 2);
		switch (fault) {
		case 0: observation_ok = false; break;
		case 1: wal[0] ^= 1; break;
		case 2: break; /* A valid TRUNCATE is not a DROP. */
		case 3: binding.identity.locator.relNumber++; break;
		case 4: binding.identity.system_identifier++; break;
		case 5: binding.identity.storage_uuid[0]++; break;
		case 6: binding.source.claim.database_incarnation++; break;
		case 7: binding.version.segment_incarnation[0]++; break;
		case 8: binding.version.mutation_token++; break;
		case 9: active_ops.id = CLUSTER_SHARED_FS_BACKEND_LOCAL; break;
		}
		run_unlink(locator(), InvalidForkNumber, false);
		UT_ASSERT_EQ(physical_calls + setter_calls + unlink_requests, 0);
		UT_ASSERT_EQ(legacy_unlinks + legacy_truncates, 0);
		UT_ASSERT_EQ(warnings, 1);
	}
}

UT_TEST(test_postcommit_error_keeps_holdoffs_and_never_reports_completion)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		reset_drop(true);
		throw_at = fault;
		run_unlink(locator(), InvalidForkNumber, false);
		UT_ASSERT_EQ(InterruptHoldoffCount, 2);
		UT_ASSERT_EQ(QueryCancelHoldoffCount, 1);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(warnings, 1);
		UT_ASSERT_EQ(rethrows, 0);
		UT_ASSERT_EQ(unlink_requests, 0);
		UT_ASSERT_EQ(legacy_unlinks + legacy_truncates, 0);
		UT_ASSERT_EQ(setter_calls, fault == 5 ? 1 : 0);
		UT_ASSERT(PG_exception_stack == NULL);
	}
}

UT_TEST(test_no_committed_owner_retains_original_abort_cleanup)
{
	reset_drop(true);
	owner_present = false;
	run_unlink(locator(), InvalidForkNumber, false);
	UT_ASSERT_EQ(owner_reads, 1);
	UT_ASSERT_EQ(physical_calls + setter_calls, 0);
	UT_ASSERT_EQ(legacy_truncates, 1);
	UT_ASSERT_EQ(legacy_unlinks, MAX_FORKNUM);
	UT_ASSERT_EQ(unlink_requests, 1);
	UT_ASSERT_EQ(warnings, 0);
}

UT_TEST(test_redo_temp_nonshared_and_partial_remain_original_paths)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		RelFileLocatorBackend target;
		reset_drop(true);
		target = locator();
		if (mode == 0)
			cluster_shared_config = false;
		if (mode == 2)
			target.backend = 7;
		if (mode == 4)
			IsBinaryUpgrade = true;
		run_unlink(target, mode == 3 ? VISIBILITYMAP_FORKNUM : InvalidForkNumber,
			mode == 1);
		UT_ASSERT_EQ(owner_reads + physical_calls + setter_calls, 0);
		UT_ASSERT_EQ(legacy_truncates, mode <= 1 ? 1 : 0);
		UT_ASSERT_EQ(legacy_unlinks, mode == 3 ? 1 : MAX_FORKNUM + (mode >= 2 ? 1 : 0));
		UT_ASSERT_EQ(unlink_requests, mode == 0 ? 1 : 0);
		UT_ASSERT_EQ(warnings, 0);
	}
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_committed_drop_reports_only_after_exact_physical_result);
	UT_RUN(test_failed_physical_drop_never_reports_or_falls_back);
	UT_RUN(test_rejected_owner_result_keeps_main_reserved);
	UT_RUN(test_unproven_drop_input_never_starts_physical_removal);
	UT_RUN(test_postcommit_error_keeps_holdoffs_and_never_reports_completion);
	UT_RUN(test_no_committed_owner_retains_original_abort_cleanup);
	UT_RUN(test_redo_temp_nonshared_and_partial_remain_original_paths);
	UT_DONE();
	return ut_failed_count != 0;
}
