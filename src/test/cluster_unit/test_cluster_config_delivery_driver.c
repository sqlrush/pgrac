/*-------------------------------------------------------------------------
 *
 * test_cluster_config_delivery_driver.c
 *    Real LMON delivery driver; native reload and CF proofs tested separately.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_delivery_driver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <signal.h>
#include "miscadmin.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_reconfig.h"
#include "utils/timestamp.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true, cluster_enabled = true;
int cluster_node_id = 0;
bool IsUnderPostmaster = true;
BackendType MyBackendType = B_LMON;
pid_t PostmasterPid = 120;
static ClusterSharedConfigProcess actual;
static ClusterSharedConfigRef published;
static TimestampTz now = 1000000;
static unsigned polls, releases, sends, cancels, writes, member_polls, member_cancels;
static bool stop, leaving, busy, publication, publication_error, signal_error;
static bool members_available, actual_available;
static uint64 selected_generation;
static ClusterControlRootResult selection_result;
static ClusterR4MembershipSnapshot members;

void
cluster_config_members_poll(const ClusterSharedConfigRef *ref,
							const ClusterR4MembershipSnapshot *cut)
{
	UT_ASSERT(memcmp(ref, &published, sizeof(*ref)) == 0);
	UT_ASSERT_EQ(cut->formation_epoch, members.formation_epoch);
	member_polls++;
}
void
cluster_config_members_cancel(void)
{
	member_cancels++;
}
bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	if (!members_available)
		return false;
	*out = members;
	return true;
}
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
TimestampTz
GetCurrentTimestamp(void)
{
	return now;
}
bool
cluster_normal_stop_requested(void)
{
	return stop;
}
bool
cluster_clean_leave_node_refuses_writes(void)
{
	return leaving;
}
bool
cluster_shared_config_process_observe(ClusterSharedConfigProcess *out)
{
	*out = actual;
	return actual_available;
}
void
cluster_control_root_config_cancel(void)
{
	cancels++;
}
ClusterControlRootResult
cluster_control_root_config_poll(const ClusterSharedConfigRef *prior,
								 ClusterSharedConfigSelected *selected,
								 ClusterSharedConfigImage *image)
{
	polls++;
	UT_ASSERT_EQ(prior->identity.generation, actual.ref.identity.generation);
	memset(image, 0, sizeof(*image));
	if (busy)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (selection_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return selection_result;
	memset(selected, 0, sizeof(*selected));
	selected->ref = actual.ref;
	selected->ref.identity.generation = selected_generation;
	image->bytes = malloc(2);
	image->bytes[0] = 'x';
	image->bytes[1] = '\0';
	image->len = 1;
	return selection_result;
}
void
cluster_shared_config_free(ClusterSharedConfigImage *image)
{
	if (image->bytes) {
		free(image->bytes);
		releases++;
	}
	memset(image, 0, sizeof(*image));
}
bool
cluster_shared_config_delivery_publish(const ClusterSharedConfigRef *ref,
									   const ClusterSharedConfigImage *image)
{
	UT_ASSERT(image->len == 1 && image->bytes[0] == 'x');
	writes++;
	if (publication_error)
		siglongjmp(*PG_exception_stack, 1);
	if (publication)
		published = *ref;
	return publication;
}
int
kill(pid_t pid, int signal)
{
	UT_ASSERT_EQ(pid, PostmasterPid);
	UT_ASSERT_EQ(signal, SIGHUP);
	UT_ASSERT_EQ(published.identity.generation, selected_generation);
	sends++;
	return signal_error ? -1 : 0;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# %s %s:%d\n", condition, file, line);
	abort();
}
static void
fixture(void)
{
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	cluster_shared_config_delivery_lmon_cancel();
	memset(&actual, 0, sizeof(actual));
	memset(&published, 0, sizeof(published));
	memset(&members, 0, sizeof(members));
	members.formation_epoch = 7;
	members.admitted_members_lo = 3;
	members.admitted_incarnation[0] = 100;
	members.admitted_incarnation[1] = 101;
	members.local_self_boot_incarnation = 100;
	actual.ref.identity.generation = 1;
	actual.ref.identity.configured[0] = 3;
	actual.node_id = cluster_node_id = 0;
	PostmasterPid = 120;
	selected_generation = 2;
	selection_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	cluster_shared_config = cluster_enabled = true;
	members_available = actual_available = publication = true;
	stop = leaving = busy = publication_error = signal_error = false;
	polls = releases = sends = cancels = writes = member_polls = member_cancels = 0;
	now += 1000000;
}
static void
next_tick(void)
{
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
}

UT_TEST(select_and_deliver)
{
	fixture();
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 1 && releases == 1 && writes == 1 && sends == 1);
	UT_ASSERT_EQ(member_polls, 1);
}
UT_TEST(notification_is_not_application)
{
	fixture();
	cluster_shared_config_delivery_lmon_tick();
	next_tick();
	/* No process changes its application state in this test. */
	UT_ASSERT_EQ(actual.ref.identity.generation, 1);
	UT_ASSERT(polls == 2 && writes == 1 && sends == 1 && member_polls == 2);
}
UT_TEST(failed_signal_retried)
{
	fixture();
	signal_error = true;
	cluster_shared_config_delivery_lmon_tick();
	signal_error = false;
	next_tick();
	UT_ASSERT(writes == 2 && sends == 2);
	next_tick();
	UT_ASSERT(writes == 2 && sends == 2);
}
UT_TEST(missing_parent_retried)
{
	fixture();
	PostmasterPid = 0;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(writes == 1 && sends == 0);
	PostmasterPid = 120;
	next_tick();
	UT_ASSERT(writes == 2 && sends == 1);
}
UT_TEST(new_object_notified)
{
	fixture();
	cluster_shared_config_delivery_lmon_tick();
	selected_generation++;
	next_tick();
	UT_ASSERT(writes == 2 && sends == 2);
	/* Byte identity, not generation alone, is used for deduplication. */
	actual.ref.identity.configured[1] = 1;
	next_tick();
	UT_ASSERT(writes == 3 && sends == 3);
}
UT_TEST(pending_not_paced)
{
	fixture();
	busy = true;
	cluster_shared_config_delivery_lmon_tick();
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 2 && writes == 0);
	busy = false;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 3 && writes == 1);
}
UT_TEST(stop_and_leave_cancel)
{
	fixture();
	stop = true;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(cancels == 1 && member_cancels == 1 && polls == 0);
	stop = false;
	leaving = true;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(cancels == 2 && member_cancels == 2 && polls == 0);
}
UT_TEST(refused_delivery)
{
	fixture();
	publication = false;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(writes == 1 && sends == 0 && member_polls == 0 && releases == 1);
	publication = true;
	next_tick();
	UT_ASSERT(writes == 2 && sends == 1);
}
UT_TEST(publication_error_cleanup)
{
	fixture();
	publication_error = true;
	PG_TRY();
	{
		cluster_shared_config_delivery_lmon_tick();
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		UT_ASSERT(releases == 1 && cancels == 1 && member_cancels == 1);
	}
	PG_END_TRY();
	publication_error = false;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(writes == 2 && sends == 1 && releases == 2);
}
UT_TEST(reload_without_online_membership)
{
	fixture();
	members_available = false;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(writes == 1 && sends == 1 && member_polls == 0 && member_cancels == 1);
}
UT_TEST(member_change_not_delivery_cut)
{
	fixture();
	busy = true;
	cluster_shared_config_delivery_lmon_tick();
	members.formation_epoch++;
	members.admitted_incarnation[1]++;
	busy = false;
	cluster_shared_config_delivery_lmon_tick();
	/* Exact CF validation belongs to the root owner, not a second cut. */
	UT_ASSERT(writes == 1 && sends == 1 && member_polls == 1);
}
UT_TEST(wrong_local_node_refuses)
{
	fixture();
	actual.node_id = 1;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 0 && writes == 0 && cancels == 1);
}
UT_TEST(root_refusal_cannot_publish)
{
	fixture();
	selection_result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 1 && writes == 0 && sends == 0 && member_cancels == 1);
}
UT_TEST(backward_clock_does_not_strand)
{
	fixture();
	cluster_shared_config_delivery_lmon_tick();
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(polls, 1);
	now -= 2000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 2 && writes == 1);
}
UT_TEST(non_lmon_cannot_deliver)
{
	fixture();
	MyBackendType = B_BACKEND;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(polls, 0);
	MyBackendType = B_LMON;
	IsUnderPostmaster = false;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(polls, 0);
}
UT_TEST(no_child_census_for_notification)
{
	fixture();
	cluster_shared_config_delivery_lmon_tick();
	/* No child registration or census fixtures are linked here. */
	next_tick();
	UT_ASSERT(sends == 1 && writes == 1);
}
UT_TEST(new_driver_redispatches)
{
	fixture();
	cluster_shared_config_delivery_lmon_tick();
	cluster_shared_config_delivery_lmon_cancel();
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(writes == 2 && sends == 2);
}
UT_TEST(missing_actual_cancels)
{
	fixture();
	actual_available = false;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 0 && writes == 0 && cancels == 1);
}
int
main(void)
{
	UT_PLAN(18);
	UT_RUN(select_and_deliver);
	UT_RUN(notification_is_not_application);
	UT_RUN(failed_signal_retried);
	UT_RUN(missing_parent_retried);
	UT_RUN(new_object_notified);
	UT_RUN(pending_not_paced);
	UT_RUN(stop_and_leave_cancel);
	UT_RUN(refused_delivery);
	UT_RUN(publication_error_cleanup);
	UT_RUN(reload_without_online_membership);
	UT_RUN(member_change_not_delivery_cut);
	UT_RUN(wrong_local_node_refuses);
	UT_RUN(root_refusal_cannot_publish);
	UT_RUN(backward_clock_does_not_strand);
	UT_RUN(non_lmon_cannot_deliver);
	UT_RUN(no_child_census_for_notification);
	UT_RUN(new_driver_redispatches);
	UT_RUN(missing_actual_cancels);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
