/* Actual Startup preparation and one-shot replay handoff. Original readonly
 * preflight, claim and typed-plan construction remain controlled owner leaves;
 * this fixture grants no source/fence, WAL, storage or recovery authority. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <setjmp.h>
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "utils/resowner.h"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
AuxProcType MyAuxProcType;
int MyProcPid;
bool InRecovery, cluster_shared_config, ArchiveRecoveryRequested, StandbyMode;
static bool cluster_backup_recovery_merge_required;
ResourceOwner CurrentResourceOwner;
static XLogRecPtr CheckPointLoc, RedoStartLSN;
static TimeLineID CheckPointTLI, RedoStartTLI;
static bool mrp, begin_refuses;
static unsigned preflights, claims, begins, releases, fatals, invalidate_during_begin;
static ClusterMergeEngage decision, cluster_engage;
static ClusterRecoveryFencePlan *cluster_fence_plan;
static ClusterColdTypedV1 *cluster_typed;
static char fence_storage, typed_storage, owner_storage, other_owner;
static jmp_buf fatal_jump;

#undef ereport
#define ereport(level, args) do { if ((level) >= ERROR) { fatals++; longjmp(fatal_jump, 1); } } while (0)

uint16 cluster_wal_thread_id(void) { return 7; }
bool cluster_mrp_should_start(void) { return mrp; }
ClusterMergeEngage
cluster_recovery_merge_preflight_readonly(uint16 thread, XLogRecPtr redo,
										  ClusterRecoveryFencePlan **out)
{
	if (thread != 7 || redo != RedoStartLSN || claims != 0 || begins != 0)
		abort();
	preflights++;
	*out = decision == CLUSTER_MERGE_ENGAGE
		? (ClusterRecoveryFencePlan *) &fence_storage : NULL;
	return decision;
}
void cluster_recovery_merge_claim_acquire_blocking(void)
{
	if (preflights != 1 || begins != 0) abort();
	claims++;
}
static ClusterColdTypedV1 *
cluster_recovery_typed_begin(ClusterRecoveryFencePlan **plan)
{
	if (claims != 1 || *plan != (ClusterRecoveryFencePlan *) &fence_storage) abort();
	begins++;
	if (begin_refuses)
		ereport(FATAL, (errmsg("fixture original typed owner refused")));
	if (invalidate_during_begin == 1) CheckPointLoc++;
	if (invalidate_during_begin == 2) RedoStartLSN++;
	if (invalidate_during_begin == 3) CurrentResourceOwner = (ResourceOwner) &other_owner;
	if (invalidate_during_begin == 4) MyProcPid++;
	if (invalidate_during_begin == 5) CheckPointTLI++;
	if (invalidate_during_begin == 6) RedoStartTLI++;
	return (ClusterColdTypedV1 *) &typed_storage;
}
static void
cluster_recovery_typed_fail(ClusterRecoveryFencePlan **plan, ClusterColdTypedV1 **typed,
						   const char *message, const char *detail)
{
	(void)message; (void)detail;
	if (*plan != (ClusterRecoveryFencePlan *) &fence_storage
		|| *typed != (ClusterColdTypedV1 *) &typed_storage) abort();
	releases++;
	*plan = NULL;
	*typed = NULL;
	ereport(FATAL, (errmsg("fixture original cleanup then refusal")));
}
#include "test_cluster_cold_start_route.inc"
#include "test_cluster_cold_start_prepare_types.inc"
static ClusterColdStartupPreparationV1 cluster_recovery_cold_preparation;
#include "test_cluster_cold_start_prepare_owner.inc"

static bool
prepare_entry(void)
{
	if (setjmp(fatal_jump)) return false;
	ClusterPrepareColdWalRecovery();
	return true;
}
static bool
replay_entry(void)
{
	cluster_engage = CLUSTER_MERGE_NO_DISABLED;
	cluster_fence_plan = NULL;
	cluster_typed = NULL;
	if (setjmp(fatal_jump)) return false;
#include "test_cluster_cold_start_replay_entry.inc"
	return true;
}
static void
reset(void)
{
	memset(&cluster_recovery_cold_preparation, 0, sizeof(cluster_recovery_cold_preparation));
	MyAuxProcType = StartupProcess;
	MyProcPid = 71;
	InRecovery = cluster_shared_config = true;
	ArchiveRecoveryRequested = StandbyMode = cluster_backup_recovery_merge_required = false;
	mrp = begin_refuses = false;
	CurrentResourceOwner = (ResourceOwner) &owner_storage;
	CheckPointLoc = 200;
	RedoStartLSN = 100;
	CheckPointTLI = RedoStartTLI = 3;
	preflights = claims = begins = releases = fatals = invalidate_during_begin = 0;
	decision = CLUSTER_MERGE_ENGAGE;
}
static void
invalidate_context(unsigned which)
{
	switch (which) {
		case 1: cluster_shared_config = false; break;
		case 2: MyAuxProcType = NotAnAuxProcess; break;
		case 3: InRecovery = false; break;
		case 4: ArchiveRecoveryRequested = true; break;
		case 5: StandbyMode = true; break;
		case 6: cluster_backup_recovery_merge_required = true; break;
		case 7: mrp = true; break;
		case 8: CurrentResourceOwner = NULL; break;
		case 9: MyProcPid = 0; break;
		case 10: CheckPointLoc = 0; break;
		case 11: RedoStartLSN = 0; break;
		case 12: CheckPointTLI = 0; break;
		case 13: RedoStartTLI = 0; break;
		default: abort();
	}
}
UT_TEST(preparation_keeps_exact_original_plan_for_one_replay)
{
	reset();
	UT_ASSERT(prepare_entry());
	UT_ASSERT_EQ(preflights, 1);
	UT_ASSERT_EQ(claims, 1);
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT(cluster_recovery_cold_preparation.ready);
	UT_ASSERT(replay_entry());
	UT_ASSERT_EQ(cluster_engage, CLUSTER_MERGE_ENGAGE);
	UT_ASSERT(cluster_fence_plan == (ClusterRecoveryFencePlan *) &fence_storage);
	UT_ASSERT(cluster_typed == (ClusterColdTypedV1 *) &typed_storage);
	UT_ASSERT_EQ(preflights + claims + begins, 3);
	UT_ASSERT(!cluster_recovery_cold_preparation.ready);
	UT_ASSERT(cluster_recovery_cold_preparation.typed == NULL);
	UT_ASSERT(cluster_recovery_cold_preparation.fence == NULL);
	UT_ASSERT_EQ(releases + fatals, 0);
	UT_ASSERT(!replay_entry());
	UT_ASSERT(cluster_typed == NULL && cluster_fence_plan == NULL);
	UT_ASSERT(!prepare_entry());
	UT_ASSERT_EQ(preflights + claims + begins, 3);
}
UT_TEST(shared_replay_without_preparation_never_runs_native_preflight)
{
	reset();
	UT_ASSERT(!replay_entry());
	UT_ASSERT_EQ(preflights + claims + begins, 0);
	UT_ASSERT(cluster_typed == NULL && cluster_fence_plan == NULL);
}
UT_TEST(unsupported_prepare_context_is_rejected_before_owners)
{
	for (unsigned i = 1; i <= 13; i++) {
		reset(); invalidate_context(i);
		UT_ASSERT(!prepare_entry());
		UT_ASSERT_EQ(preflights + claims + begins, 0);
		UT_ASSERT(!cluster_recovery_cold_preparation.ready);
	}
}
UT_TEST(replay_rejects_changed_context_without_native_fallback)
{
	for (unsigned i = 1; i <= 13; i++) {
		reset(); UT_ASSERT(prepare_entry()); invalidate_context(i);
		UT_ASSERT(!replay_entry());
		UT_ASSERT_EQ(preflights + claims + begins, 3);
		UT_ASSERT(cluster_typed == NULL && cluster_fence_plan == NULL);
	}
}
UT_TEST(replay_rejects_changed_selected_identity)
{
	for (unsigned i = 1; i <= 6; i++) {
		reset(); UT_ASSERT(prepare_entry());
		switch (i) {
			case 1: CheckPointLoc++; break;
			case 2: RedoStartLSN++; break;
			case 3: CheckPointTLI++; break;
			case 4: RedoStartTLI++; break;
			case 5: CurrentResourceOwner = (ResourceOwner) &other_owner; break;
			case 6: MyProcPid++; break;
		}
		UT_ASSERT(!replay_entry());
		UT_ASSERT_EQ(preflights + claims + begins, 3);
		UT_ASSERT(cluster_typed == NULL && cluster_fence_plan == NULL);
	}
}
UT_TEST(preflight_refusals_never_claim_or_prepare)
{
	reset(); decision = CLUSTER_MERGE_REFUSE_UNSHARED;
	UT_ASSERT(!prepare_entry());
	UT_ASSERT_EQ(preflights, 1);
	UT_ASSERT_EQ(claims + begins, 0);
	UT_ASSERT(!cluster_recovery_cold_preparation.ready);
	reset(); decision = CLUSTER_MERGE_NO_NOT_COLD;
	UT_ASSERT(!prepare_entry());
	UT_ASSERT_EQ(preflights, 1);
	UT_ASSERT_EQ(claims + begins, 0);
	UT_ASSERT(!cluster_recovery_cold_preparation.ready);
}
UT_TEST(typed_owner_refusal_cannot_publish_partial_preparation)
{
	reset(); begin_refuses = true;
	UT_ASSERT(!prepare_entry());
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT(!cluster_recovery_cold_preparation.ready);
	UT_ASSERT(!replay_entry());
	UT_ASSERT(!prepare_entry());
	UT_ASSERT_EQ(preflights + claims + begins, 3);
}
UT_TEST(identity_change_during_preparation_releases_original_plan)
{
	for (unsigned i = 1; i <= 6; i++) {
		reset(); invalidate_during_begin = i;
		UT_ASSERT(!prepare_entry());
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT(!cluster_recovery_cold_preparation.ready);
		UT_ASSERT(!replay_entry());
		UT_ASSERT_EQ(preflights + claims + begins, 3);
	}
}
UT_TEST(nonshared_plain_recovery_keeps_original_preflight_and_claim)
{
	reset(); cluster_shared_config = false;
	decision = CLUSTER_MERGE_NO_NO_CANDIDATES;
	UT_ASSERT(replay_entry());
	UT_ASSERT_EQ(preflights, 1);
	UT_ASSERT_EQ(claims, 1);
	UT_ASSERT_EQ(begins, 0);
	UT_ASSERT(cluster_typed == NULL && cluster_fence_plan == NULL);
	UT_ASSERT(!cluster_recovery_cold_preparation.started);
}
UT_TEST(nonshared_archive_and_standby_keep_native_route)
{
	for (unsigned i = 1; i <= 4; i++) {
		reset(); cluster_shared_config = false;
		if (i == 1) ArchiveRecoveryRequested = true;
		if (i == 2) StandbyMode = true;
		if (i == 3) cluster_backup_recovery_merge_required = true;
		if (i == 4) mrp = true;
		UT_ASSERT(replay_entry());
		UT_ASSERT_EQ(preflights + claims + begins, 0);
		UT_ASSERT(cluster_typed == NULL && cluster_fence_plan == NULL);
	}
}
int main(void)
{
	UT_PLAN(10);
	UT_RUN(preparation_keeps_exact_original_plan_for_one_replay);
	UT_RUN(shared_replay_without_preparation_never_runs_native_preflight);
	UT_RUN(unsupported_prepare_context_is_rejected_before_owners);
	UT_RUN(replay_rejects_changed_context_without_native_fallback);
	UT_RUN(replay_rejects_changed_selected_identity);
	UT_RUN(preflight_refusals_never_claim_or_prepare);
	UT_RUN(typed_owner_refusal_cannot_publish_partial_preparation);
	UT_RUN(identity_change_during_preparation_releases_original_plan);
	UT_RUN(nonshared_plain_recovery_keeps_original_preflight_and_claim);
	UT_RUN(nonshared_archive_and_standby_keep_native_route);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
