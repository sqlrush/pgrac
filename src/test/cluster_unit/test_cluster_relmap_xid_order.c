/*-------------------------------------------------------------------------
 * test_cluster_relmap_xid_order.c
 *    Run the real relmap producer across explicit backend boundaries.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_xid_order.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

/* Keep the producer, committed-image validation and merge unchanged. */
#include "../../backend/utils/cache/relmapper.c"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_shared_catalog = true;
int cluster_node_id = 3;
int cluster_injection_armed_count;
bool allowSystemTableMods;
Oid MyDatabaseId = 12345;
Oid MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
char *DatabasePath = "base/12345";
ProcessingMode Mode = NormalProcessing;
volatile uint32 CritSectionCount;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;

static sigjmp_buf error_target;
static bool error_expected;
static int error_level;
static bool authority_held, authority_available, lock_available, local_held;
static bool fail_assignment;
static uint64 pending_generation;
static FullTransactionId assigned_xid, next_xid;
static unsigned int sequence, xid_order, lock_order, wal_order, stage_order;
static unsigned int full_requests, raw_requests, assignments;
static unsigned int xid_under_lock, xid_in_crit, local_acquires;
static unsigned int begins, inserts, flushes, stages, preserves, sync_commits;
static unsigned int registered;
static TransactionId xid_at_lock, xid_at_wal[2];
static xl_relmap_update wal_header;
static RelMapFile committed, updates, wal_image, staged_image[2];
static ClusterRelmapOwner staged_owner[2];
static bool staged_shared[2];
static Oid staged_dbid[2];

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected Assert: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel < ERROR)
		return false;
	if (!error_expected)
		abort();
	error_level = elevel;
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int code pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	siglongjmp(error_target, 1);
}

/* The allocator is a fixture; observations test the real caller's ordering. */
static FullTransactionId
assign_top(void)
{
	xid_order = ++sequence;
	xid_under_lock += local_held;
	xid_in_crit += CritSectionCount != 0;
	if (fail_assignment)
		ereport(ERROR, (errmsg("top xid assignment failed")));
	if (!FullTransactionIdIsValid(assigned_xid))
	{
		assigned_xid = next_xid;
		assignments++;
	}
	return assigned_xid;
}

FullTransactionId
GetTopFullTransactionId(void)
{
	full_requests++;
	return assign_top();
}

TransactionId
GetTopTransactionId(void)
{
	raw_requests++;
	return XidFromFullTransactionId(assign_top());
}

bool
cluster_relmap_authority_x_held(void)
{
	return authority_held;
}

bool
cluster_relmap_authority_x_lock(void)
{
	authority_held = lock_available;
	return authority_held;
}

bool
cluster_relmap_authority_read_header(bool shared pg_attribute_unused(),
									   Oid dbid pg_attribute_unused(),
									   ClusterRelmapAuthorityHeader *out)
{
	UT_ASSERT(authority_held);
	memset(out, 0, sizeof(*out));
	out->committed_generation = 7;
	out->pending_generation = pending_generation;
	return authority_available;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == RelationMappingLock && mode == LW_EXCLUSIVE);
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	local_held = true;
	local_acquires++;
	lock_order = ++sequence;
	xid_at_lock = XidFromFullTransactionId(assigned_xid);
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == RelationMappingLock && local_held);
	local_held = false;
}

bool
cluster_relmap_authority_read_committed(bool shared, Oid dbid,
										  char *out, uint32 *len)
{
	UT_ASSERT(authority_held && local_held);
	UT_ASSERT_EQ(dbid, shared ? InvalidOid : MyDatabaseId);
	memcpy(out, &committed, sizeof(committed));
	*len = sizeof(committed);
	return true;
}

/* The cluster path must never fall back to a node-local map file. */
int
OpenTransientFile(const char *name pg_attribute_unused(), int flags pg_attribute_unused())
{
	abort();
}

int
CloseTransientFile(int fd pg_attribute_unused())
{
	abort();
}

void
XLogBeginInsert(void)
{
	UT_ASSERT(local_held && CritSectionCount == 1);
	begins++;
	registered = 0;
}

void
XLogRegisterData(char *data, uint32 len)
{
	if (registered++ == 0)
	{
		UT_ASSERT_EQ(len, MinSizeOfRelmapUpdate);
		memcpy(&wal_header, data, len);
	}
	else
	{
		UT_ASSERT_EQ(registered, 2);
		UT_ASSERT_EQ(len, sizeof(wal_image));
		memcpy(&wal_image, data, len);
	}
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(rmid, RM_RELMAP_ID);
	UT_ASSERT_EQ(info, XLOG_RELMAP_UPDATE);
	UT_ASSERT_EQ(registered, 2);
	UT_ASSERT(local_held && CritSectionCount == 1 && inserts < 2);
	xid_at_wal[inserts] = XidFromFullTransactionId(assigned_xid);
	wal_order = ++sequence;
	inserts++;
	return (XLogRecPtr) (1000 + inserts);
}

void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, 1000 + inserts);
	UT_ASSERT(local_held && CritSectionCount == 1);
	flushes++;
}

uint64
cluster_reconfig_get_observed_epoch(int32 node)
{
	UT_ASSERT_EQ(node, cluster_node_id);
	return 909;
}

void
cluster_relmap_authority_write_pending(bool shared, Oid dbid, const char *image,
										 uint32 len, uint64 generation,
										 const ClusterRelmapOwner *owner)
{
	UT_ASSERT(authority_held && local_held && CritSectionCount == 0 && stages < 2);
	UT_ASSERT_EQ(flushes, inserts);
	UT_ASSERT_EQ(generation, 8);
	UT_ASSERT_EQ(len, sizeof(RelMapFile));
	UT_ASSERT(memcmp(image, &wal_image, len) == 0);
	UT_ASSERT_EQ(wal_header.dbid, dbid);
	UT_ASSERT_EQ(wal_header.tsid, shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
	UT_ASSERT_EQ(wal_header.nbytes, len);
	staged_shared[stages] = shared;
	staged_dbid[stages] = dbid;
	staged_owner[stages] = *owner;
	memcpy(&staged_image[stages], image, len);
	stage_order = ++sequence;
	stages++;
}

void
RelationPreserveStorage(RelFileLocator locator, bool istemp)
{
	UT_ASSERT(local_held && !istemp);
	UT_ASSERT_EQ(locator.relNumber, updates.mappings[0].mapfilenumber);
	preserves++;
}

void
ForceSyncCommit(void)
{
	UT_ASSERT(!local_held && CritSectionCount == 0);
	sync_commits++;
}

void
cluster_injection_run(const char *name pg_attribute_unused())
{
	abort();
}

bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

static void
reset(bool preassigned)
{
	authority_held = local_held = fail_assignment = error_expected = false;
	authority_available = lock_available = true;
	pending_generation = 0;
	error_level = 0;
	sequence = xid_order = lock_order = wal_order = stage_order = 0;
	full_requests = raw_requests = assignments = 0;
	xid_under_lock = xid_in_crit = local_acquires = 0;
	begins = inserts = flushes = stages = preserves = sync_commits = 0;
	CritSectionCount = 0;
	memset(cluster_relmap_publish_arms, 0, sizeof(cluster_relmap_publish_arms));
	memset(&shared_map, 0, sizeof(shared_map));
	memset(&local_map, 0, sizeof(local_map));
	memset(xid_at_wal, 0, sizeof(xid_at_wal));
	next_xid = FullTransactionIdFromEpochAndXid(7, 100);
	assigned_xid = preassigned ? next_xid : InvalidFullTransactionId;
	memset(&committed, 0, sizeof(committed));
	committed.magic = RELMAPPER_FILEMAGIC;
	committed.num_mappings = 1;
	committed.mappings[0].mapoid = 1259;
	committed.mappings[0].mapfilenumber = 500;
	INIT_CRC32C(committed.crc);
	COMP_CRC32C(committed.crc, (char *) &committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(committed.crc);
	updates = committed;
	updates.mappings[0].mapfilenumber = 501;
}

static void
check_success(bool shared, bool preassigned)
{
	reset(preassigned);
	cluster_perform_relmap_update(shared, &updates);
	UT_ASSERT_EQ(full_requests, 1);
	UT_ASSERT_EQ(raw_requests, 0);
	UT_ASSERT_EQ(assignments, preassigned ? 0 : 1);
	UT_ASSERT_EQ(xid_under_lock, 0);
	UT_ASSERT_EQ(xid_in_crit, 0);
	UT_ASSERT(xid_order < lock_order && lock_order < wal_order && wal_order < stage_order);
	UT_ASSERT_EQ(xid_at_lock, XidFromFullTransactionId(next_xid));
	UT_ASSERT_EQ(xid_at_wal[0], XidFromFullTransactionId(next_xid));
	UT_ASSERT_EQ(U64FromFullTransactionId(assigned_xid), U64FromFullTransactionId(next_xid));
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(stages, 1);
	UT_ASSERT_EQ(preserves, 1);
	UT_ASSERT_EQ(sync_commits, 1);
	UT_ASSERT_EQ(staged_shared[0], shared);
	UT_ASSERT_EQ(staged_dbid[0], shared ? InvalidOid : MyDatabaseId);
	UT_ASSERT_EQ(staged_owner[0].owner_node, cluster_node_id);
	UT_ASSERT_EQ(staged_owner[0].owner_xid, xid_at_wal[0]);
	UT_ASSERT_EQ(staged_owner[0].owner_epoch, 909);
	UT_ASSERT_EQ(staged_owner[0].relmap_lsn, 1001);
	UT_ASSERT_EQ(staged_image[0].mappings[0].mapfilenumber, 501);
	UT_ASSERT(memcmp(shared ? &shared_map : &local_map,
					 &staged_image[0], sizeof(RelMapFile)) == 0);
	UT_ASSERT(cluster_relmap_publish_arms[shared ? 0 : 1].armed);
	UT_ASSERT(!local_held && CritSectionCount == 0);
}

static void
check_error(bool shared, bool assignment_reached)
{
	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
	{
		cluster_perform_relmap_update(shared, &updates);
		UT_ASSERT(false);
	}
	error_expected = false;
	UT_ASSERT_EQ(error_level, ERROR);
	UT_ASSERT_EQ(full_requests, assignment_reached ? 1 : 0);
	UT_ASSERT_EQ(raw_requests, 0);
	UT_ASSERT_EQ(assignments, 0);
	UT_ASSERT_EQ(local_acquires, 0);
	UT_ASSERT_EQ(begins, 0);
	UT_ASSERT_EQ(inserts, 0);
	UT_ASSERT_EQ(flushes, 0);
	UT_ASSERT_EQ(stages, 0);
	UT_ASSERT_EQ(preserves, 0);
	UT_ASSERT_EQ(sync_commits, 0);
	UT_ASSERT(!local_held && CritSectionCount == 0);
	UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
}

UT_TEST(shared_assigns_before_lock_and_wal) { check_success(true, false); }
UT_TEST(database_assigns_before_lock_and_wal) { check_success(false, false); }
UT_TEST(shared_reuses_existing_full_xid) { check_success(true, true); }
UT_TEST(database_reuses_existing_full_xid) { check_success(false, true); }

UT_TEST(shared_assignment_error_has_no_local_effects)
{
	reset(false);
	fail_assignment = true;
	check_error(true, true);
}

UT_TEST(database_assignment_error_has_no_local_effects)
{
	reset(false);
	fail_assignment = true;
	check_error(false, true);
}

UT_TEST(unresolved_pending_rejects_before_assignment)
{
	reset(false);
	pending_generation = 8;
	check_error(true, false);
}

UT_TEST(unavailable_header_rejects_before_assignment)
{
	reset(false);
	authority_available = false;
	check_error(false, false);
}

UT_TEST(unavailable_authority_lock_rejects_before_assignment)
{
	reset(false);
	lock_available = false;
	check_error(true, false);
}

UT_TEST(both_namespaces_share_one_transaction_birth)
{
	reset(false);
	cluster_perform_relmap_update(true, &updates);
	cluster_perform_relmap_update(false, &updates);
	UT_ASSERT_EQ(full_requests, 2);
	UT_ASSERT_EQ(raw_requests, 0);
	UT_ASSERT_EQ(assignments, 1);
	UT_ASSERT_EQ(xid_under_lock, 0);
	UT_ASSERT_EQ(xid_in_crit, 0);
	UT_ASSERT_EQ(xid_at_wal[0], XidFromFullTransactionId(next_xid));
	UT_ASSERT_EQ(xid_at_wal[1], xid_at_wal[0]);
	UT_ASSERT_EQ(staged_owner[0].owner_xid, xid_at_wal[0]);
	UT_ASSERT_EQ(staged_owner[1].owner_xid, xid_at_wal[1]);
	UT_ASSERT_EQ(stages, 2);
	UT_ASSERT(cluster_relmap_publish_arms[0].armed && cluster_relmap_publish_arms[1].armed);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(shared_assigns_before_lock_and_wal);
	UT_RUN(database_assigns_before_lock_and_wal);
	UT_RUN(shared_reuses_existing_full_xid);
	UT_RUN(database_reuses_existing_full_xid);
	UT_RUN(shared_assignment_error_has_no_local_effects);
	UT_RUN(database_assignment_error_has_no_local_effects);
	UT_RUN(unresolved_pending_rejects_before_assignment);
	UT_RUN(unavailable_header_rejects_before_assignment);
	UT_RUN(unavailable_authority_lock_rejects_before_assignment);
	UT_RUN(both_namespaces_share_one_transaction_birth);
	UT_DONE();
	return ut_failed_count != 0;
}
