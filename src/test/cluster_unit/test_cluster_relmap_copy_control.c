/*-------------------------------------------------------------------------
 * test_cluster_relmap_copy_control.c
 *    Original cross-database copy admission and control-lock ordering.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_copy_control.c
 * NOTES
 *    Executes the original entry and control wrapper. Storage/control/lock
 *    leaves count attempted effects; this is not native file durability.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_control.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_relmap_lock.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config = true, cluster_shared_catalog = true;
ProcessingMode Mode = NormalProcessing;
volatile uint32 CritSectionCount, InterruptHoldoffCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool x_held, cf_held, mapping_held, xidgen_held, control_held;
static unsigned x_acquires, x_releases, cf_acquires, cf_releases, formats;
static unsigned reads, writes, mapping_acquires, mapping_releases, violations;
static unsigned fault, version;
static LOCKMODE cf_mode;
static int error_level;
static RelMapFile source, destination;
static Oid read_db, read_ts, write_db, write_ts;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "Assert: %s at %s:%d\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	error_level = level;
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool
cluster_relmap_authority_x_held(void)
{
	return x_held;
}
bool
cluster_relmap_authority_x_lock(void)
{
	x_acquires++;
	violations += cf_held || mapping_held || xidgen_held || control_held;
	if (fault == 1)
		return false;
	x_held = true;
	return true;
}
void
cluster_relmap_authority_x_unlock(void)
{
	violations += !x_held || cf_held || mapping_held;
	x_releases++;
	x_held = false;
}
bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return cf_held && mode == cf_mode;
}
bool
cluster_cf_lock(LOCKMODE mode)
{
	cf_acquires++;
	violations += !x_held || cf_held || mapping_held || mode != ShareLock;
	if (fault == 2)
		return false;
	if (fault == 3)
		ereport(ERROR, (errmsg("control acquire ERROR")));
	cf_held = true;
	cf_mode = mode;
	return true;
}
ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	cf_releases++;
	violations += !cf_held || mapping_held || mode != ShareLock;
	cf_held = false;
	if (fault == 9)
		ereport(ERROR, (errmsg("unconfirmed release ERROR")));
	return fault == 8 ? CLUSTER_CF_RELEASE_UNCONFIRMED : CLUSTER_CF_RELEASE_CONFIRMED;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return (lock == RelationMappingLock && mapping_held) || (lock == XidGenLock && xidgen_held)
		   || (lock == ControlFileLock && control_held);
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	violations += lock != RelationMappingLock || mode != LW_EXCLUSIVE || mapping_held;
	if (cluster_shared_config && !IsBootstrapProcessingMode())
		violations += !x_held || !cf_held;
	mapping_held = true;
	mapping_acquires++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	violations += lock != RelationMappingLock || !mapping_held;
	if ((fault == 6 || fault == 7) && !InterruptHoldoffCount)
		violations++;
	mapping_held = false;
	mapping_releases++;
}
ClusterControlRootResult
cluster_control_root_relmap_format_locked_v2(Size budget, ClusterControlRelmapFormatV2 *out)
{
	formats++;
	violations += !x_held || !cf_held || mapping_held || !budget;
	if (fault == 4)
		ereport(ERROR, (errmsg("format read ERROR")));
	memset(out, 0, sizeof(*out));
	out->root_version = version;
	if (fault == 5 && formats == 2)
		out->root_sha256[0]++;
	return fault == 10 ? CLUSTER_CONTROL_ROOT_STALE_TOKEN : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
static void
read_relmap_file(RelMapFile *out, char *path, bool held, int level, Oid db, Oid ts)
{
	reads++;
	violations += strcmp(path, "source") != 0 || held || level != ERROR;
	read_db = db;
	read_ts = ts;
	if (fault == 6) {
		/* The original reader can ERROR while its own MappingLock is held. */
		mapping_held = true;
		ereport(ERROR, (errmsg("source read ERROR")));
	}
	*out = source;
}
static void
write_relmap_file(RelMapFile *map, bool wal, bool sinval, bool preserve, Oid db, Oid ts, char *path)
{
	writes++;
	violations += !mapping_held || !wal || sinval || preserve || strcmp(path, "destination") != 0;
	write_db = db;
	write_ts = ts;
	destination = *map;
	if (fault == 7)
		ereport(ERROR, (errmsg("target write ERROR")));
}
static bool relmap_writer_cf_enter_v2(bool *owned);
static void cluster_copy_relmap_from_control(Oid, Oid, char *, char *, Oid, Oid);
#include "test_cluster_relmap_copy_control_entries.inc"

static void
setup(unsigned selected)
{
	x_held = cf_held = mapping_held = xidgen_held = control_held = false;
	x_acquires = x_releases = cf_acquires = cf_releases = formats = 0;
	reads = writes = mapping_acquires = mapping_releases = violations = 0;
	fault = 0;
	cf_mode = ShareLock;
	version = selected;
	error_level = 0;
	CritSectionCount = InterruptHoldoffCount = 0;
	cluster_shared_config = cluster_shared_catalog = true;
	Mode = NormalProcessing;
	memset(&source, 0, sizeof(source));
	memset(&destination, 0xa5, sizeof(destination));
	source.magic = RELMAPPER_FILEMAGIC;
	source.num_mappings = 1;
	source.mappings[0].mapoid = 1259;
	source.mappings[0].mapfilenumber = 900;
}
static bool
copy_call(void)
{
	volatile bool failed = false;
	PG_TRY();
	{
		RelationMapCopy(200, 1663, "source", "destination", 100, 1663);
	}
	PG_CATCH();
	{
		failed = true;
	}
	PG_END_TRY();
	return !failed;
}
static void
no_effects(void)
{
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(mapping_acquires, 0);
	UT_ASSERT_EQ(violations, 0);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}
UT_TEST(managed_copy_refuses_before_source_and_target_effects)
{
	setup(4);
	UT_ASSERT(!copy_call());
	no_effects();
	UT_ASSERT_EQ(error_level, ERROR);
	UT_ASSERT_EQ(formats, 1);
	UT_ASSERT(x_held && !cf_held && !mapping_held);
	UT_ASSERT_EQ(cf_releases, 1);
	UT_ASSERT_EQ(x_releases, 0);
}
UT_TEST(unknown_or_failed_control_never_chooses_legacy)
{
	for (unsigned i = 0; i < 4; i++) {
		setup(i == 3 ? 3 : i);
		if (i == 3)
			fault = 10;
		UT_ASSERT(!copy_call());
		no_effects();
		UT_ASSERT(x_held && !cf_held);
	}
}
UT_TEST(exact_legacy_copy_keeps_source_destination_and_lock_order)
{
	setup(3);
	UT_ASSERT(copy_call());
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(formats, 2);
	UT_ASSERT_EQ(read_db, 100);
	UT_ASSERT_EQ(read_ts, 1663);
	UT_ASSERT_EQ(write_db, 200);
	UT_ASSERT_EQ(write_ts, 1663);
	UT_ASSERT_EQ(memcmp(&source, &destination, sizeof(source)), 0);
	UT_ASSERT_EQ(violations, 0);
	UT_ASSERT_EQ(cf_releases, 1);
	UT_ASSERT_EQ(x_releases, 1);
	UT_ASSERT(!cf_held && !x_held && !mapping_held);
}
UT_TEST(borrowed_original_locks_are_neither_reentered_nor_released)
{
	for (unsigned selected = 3; selected <= 4; selected++) {
		for (unsigned exclusive = 0; exclusive <= 1; exclusive++) {
			setup(selected);
			x_held = cf_held = true;
			cf_mode = exclusive ? ExclusiveLock : ShareLock;
			UT_ASSERT_EQ(copy_call(), selected == 3);
			UT_ASSERT(x_held && cf_held && !mapping_held);
			UT_ASSERT_EQ(x_acquires + cf_acquires + x_releases + cf_releases, 0);
			UT_ASSERT_EQ(violations, 0);
		}
	}
}
UT_TEST(inverted_or_local_lock_entry_refuses_without_releasing_caller_locks)
{
	for (unsigned i = 0; i < 5; i++) {
		setup(4);
		if (i == 0)
			cf_held = true;
		if (i == 1)
			mapping_held = true;
		if (i == 2)
			xidgen_held = true;
		if (i == 3)
			control_held = true;
		if (i == 4)
			CritSectionCount = 1;
		UT_ASSERT(!copy_call());
		no_effects();
		UT_ASSERT_EQ(x_acquires + cf_acquires + formats + cf_releases + x_releases, 0);
		UT_ASSERT_EQ(cf_held, i == 0);
		UT_ASSERT_EQ(mapping_held, i == 1);
	}
}
UT_TEST(acquisition_and_format_errors_keep_original_abort_duty)
{
	for (unsigned i = 1; i <= 4; i++) {
		setup(4);
		fault = i;
		UT_ASSERT(!copy_call());
		no_effects();
		UT_ASSERT_EQ(x_held, i != 1);
		UT_ASSERT(!cf_held);
		UT_ASSERT_EQ(cf_releases, i == 4);
		UT_ASSERT_EQ(x_releases, 0);
	}
}
UT_TEST(late_failure_unwinds_owned_local_locks_but_keeps_creation_debt)
{
	for (unsigned i = 5; i <= 9; i++) {
		setup(3);
		fault = i;
		UT_ASSERT(!copy_call());
		UT_ASSERT(x_held && !cf_held && !mapping_held);
		UT_ASSERT_EQ(x_releases, 0);
		UT_ASSERT_EQ(cf_releases, 1);
		UT_ASSERT_EQ(violations, 0);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(writes, i == 6 ? 0 : 1);
	}
	/* Cleanup can itself ERROR: balance interrupt holdoff and never retry
	 * that CF release or drop the original singleton/creation obligation. */
	setup(4);
	fault = 9;
	UT_ASSERT(!copy_call());
	no_effects();
	UT_ASSERT(x_held && !cf_held);
	UT_ASSERT_EQ(cf_releases, 1);
}
UT_TEST(off_and_bootstrap_copy_preserve_the_original_route)
{
	for (unsigned i = 0; i < 2; i++) {
		setup(4);
		if (i == 0)
			cluster_shared_config = false;
		else
			Mode = BootstrapProcessing;
		UT_ASSERT(copy_call());
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(formats + x_acquires + cf_acquires, 0);
		UT_ASSERT_EQ(violations, 0);
	}
}
UT_TEST(managed_control_cannot_be_bypassed_by_catalog_guc)
{
	setup(4);
	cluster_shared_catalog = false;
	UT_ASSERT(!copy_call());
	no_effects();
}
int
main(void)
{
	UT_PLAN(9);
	UT_RUN(managed_copy_refuses_before_source_and_target_effects);
	UT_RUN(unknown_or_failed_control_never_chooses_legacy);
	UT_RUN(exact_legacy_copy_keeps_source_destination_and_lock_order);
	UT_RUN(borrowed_original_locks_are_neither_reentered_nor_released);
	UT_RUN(inverted_or_local_lock_entry_refuses_without_releasing_caller_locks);
	UT_RUN(acquisition_and_format_errors_keep_original_abort_duty);
	UT_RUN(late_failure_unwinds_owned_local_locks_but_keeps_creation_debt);
	UT_RUN(off_and_bootstrap_copy_preserve_the_original_route);
	UT_RUN(managed_control_cannot_be_bypassed_by_catalog_guc);
	UT_DONE();
	return ut_failed_count != 0;
}
