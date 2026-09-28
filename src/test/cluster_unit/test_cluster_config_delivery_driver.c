/*-------------------------------------------------------------------------
 * test_cluster_config_delivery_driver.c
 *    Actual background driver with CF selection/native registration fixtures.
 *    CF identity/retirement and real native delivery have separate tests.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <signal.h>
#include "miscadmin.h"
#include "storage/proc.h"
#include "postmaster/syslogger.h"
#include "cluster/cluster_shared_config.h"
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
bool Logging_collector = true;
static PGPROC processes[2];
static PROC_HDR procs;
PROC_HDR *ProcGlobal = &procs;
static ClusterSharedConfigProcess actual;
static ClusterSharedConfigRegistration logger;
static ClusterSharedConfigRef published;
static TimestampTz now = 1000000;
static unsigned polls, releases, sends, cancels, writes;
static bool stop, leaving, publication = true, busy, publication_error;
static ClusterR4MembershipSnapshot members;
static bool members_available = true;
static int change_during_poll;

bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	if (!members_available)
		return false;
	*out = members;
	return true;
}

static void
seed_members(void)
{
	memset(&members, 0, sizeof(members));
	members.formation_epoch = 7;
	members.admitted_members_lo = 3;
	members.admitted_incarnation[0] = 100;
	members.admitted_incarnation[1] = 101;
	members.observed_generation[0] = 20;
	members.observed_generation[1] = 21;
	members.local_self_boot_incarnation = 100;
}

bool
ProcConfigSnapshotPids(int32 *pids, uint32 capacity)
{
	if (capacity != lengthof(processes))
		return false;
	for (uint32 i = 0; i < capacity; ++i)
		pids[i] = processes[i].pid;
	return true;
}

void *
palloc(Size size)
{
	void *ptr = malloc(size);
	if (!ptr)
		abort();
	return ptr;
}
void
pfree(void *ptr)
{
	free(ptr);
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
	return true;
}
bool
cluster_shared_config_registration_read(ClusterSharedConfigSlot *slot,
										ClusterSharedConfigRegistration *out)
{
	*out = slot->value;
	return true;
}
bool
cluster_shared_config_delivery_logger_snapshot(ClusterSharedConfigRegistration *out,
											   uint64 *sequence)
{
	*out = logger;
	*sequence = 2;
	return logger.pid != 0;
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
	if (change_during_poll == 1)
		members.formation_epoch++;
	if (change_during_poll == 2)
		members.observed_generation[1]++;
	UT_ASSERT_EQ(prior->identity.generation, actual.ref.identity.generation);
	memset(image, 0, sizeof(*image));
	if (busy)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	memset(selected, 0, sizeof(*selected));
	selected->ref = actual.ref;
	selected->ref.identity.generation = 2;
	image->bytes = malloc(2);
	image->bytes[0] = 'x';
	image->bytes[1] = '\0';
	image->len = 1;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
	sends++;
	return 0;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# %s %s:%d\n", condition, file, line);
	abort();
}

static void
target(ClusterSharedConfigRegistration *value, int pid)
{
	memset(value, 0, sizeof(*value));
	value->pid = pid;
	value->registration = 2;
	value->observed = true;
	value->process = actual;
	value->process.ref = published;
	value->active.version = CLUSTER_SHARED_CONFIG_ACTIVE_VERSION;
	value->active.static_entries = 1;
	value->active.dynamic_entries = 1;
}

UT_TEST(select_and_deliver)
{
	procs.allProcs = processes;
	procs.allProcCount = lengthof(processes);
	actual.ref.identity.generation = 1;
	actual.ref.identity.configured[0] = 3;
	actual.applier_pid = PostmasterPid;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 1 && releases == 1 && writes == 1 && sends == 1);
}
UT_TEST(lost_notification)
{
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(polls, 1);
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(polls == 2 && sends == 2);
}
UT_TEST(parent_does_not_prove_children)
{
	target(&procs.cluster_config_postmaster.value, PostmasterPid);
	target(&processes[0].cluster_config.value, 130);
	processes[0].pid = 130;
	processes[0].cluster_config.value.process.ref.identity.generation = 1;
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(sends, 3);
	target(&processes[0].cluster_config.value, 130);
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(sends, 4);
	target(&logger, 121);
	logger.role = B_LOGGER;
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(sends, 4);
}
UT_TEST(failed_and_parallel_not_applied)
{
	processes[0].cluster_config.value.process.failed = true;
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(sends, 5);
	processes[0].cluster_config.value.process.failed = false;
	processes[0].cluster_config.value.process.parallel_snapshot = true;
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(sends, 6);
}
UT_TEST(pending_not_paced)
{
	unsigned before = polls;
	busy = true;
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(polls, before + 2);
}
UT_TEST(stop_and_leave_cancel)
{
	unsigned before_cancel = cancels, before = polls;
	stop = true;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(cancels > before_cancel && polls == before);
	stop = false;
	leaving = true;
	before_cancel = cancels;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT(cancels > before_cancel && polls == before);
}
UT_TEST(refused_delivery)
{
	unsigned before = sends;
	leaving = busy = false;
	publication = false;
	cluster_shared_config_delivery_lmon_cancel();
	now += 1000000;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(sends, before);
}
UT_TEST(publication_error_cleanup)
{
	unsigned before = releases, before_cancel = cancels;
	publication_error = true;
	now += 1000000;
	PG_TRY();
	{
		cluster_shared_config_delivery_lmon_tick();
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		UT_ASSERT(releases == before + 1 && cancels > before_cancel);
	}
	PG_END_TRY();
}

static void
member_fixture(void)
{
	cluster_shared_config_delivery_lmon_cancel();
	seed_members();
	members_available = publication = true;
	stop = leaving = busy = publication_error = false;
	change_during_poll = 0;
	cluster_node_id = 0;
	actual.node_id = 0;
	actual.ref.identity.configured[0] = 3;
	actual.ref.identity.configured[1] = 0;
	now += 1000000;
}

UT_TEST(no_membership_no_delivery)
{
	unsigned before;
	member_fixture();
	members_available = false;
	before = writes;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(writes, before);
}

UT_TEST(member_change_retires_pending_selection)
{
	for (int change = 0; change < 4; ++change) {
		unsigned before, before_cancel, before_poll;
		member_fixture();
		busy = true;
		cluster_shared_config_delivery_lmon_tick();
		before = writes;
		before_poll = polls;
		before_cancel = cancels;
		if (change == 0)
			members.formation_epoch++;
		if (change == 1) {
			members.admitted_members_lo = 1;
			members.admitted_incarnation[1] = 0;
			members.observed_generation[1] = 0;
		}
		if (change == 2) {
			members.local_self_boot_incarnation++;
			members.admitted_incarnation[0]++;
		}
		if (change == 3)
			members.admitted_incarnation[1]++;
		busy = false;
		cluster_shared_config_delivery_lmon_tick();
		UT_ASSERT(writes == before && polls == before_poll && cancels > before_cancel);
		/* A fresh attempt, not the old one relabeled, may progress. */
		cluster_shared_config_delivery_lmon_tick();
		UT_ASSERT_EQ(writes, before + 1);
	}
}

UT_TEST(changed_completion_cannot_publish)
{
	unsigned before;
	member_fixture();
	change_during_poll = 1;
	before = writes;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(writes, before);
}

UT_TEST(undeclared_member_or_wrong_local_node)
{
	unsigned before;
	member_fixture();
	members.admitted_members_hi = 1;
	members.admitted_incarnation[64] = 164;
	members.observed_generation[64] = 80;
	before = writes;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(writes, before);
	member_fixture();
	actual.node_id = 1;
	before = writes;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(writes, before);
}

UT_TEST(observation_refresh_is_not_member_change)
{
	unsigned before;
	member_fixture();
	busy = true;
	cluster_shared_config_delivery_lmon_tick();
	members.observed_generation[0]++;
	members.observed_generation[1]++;
	change_during_poll = 2;
	busy = false;
	before = writes;
	cluster_shared_config_delivery_lmon_tick();
	UT_ASSERT_EQ(writes, before + 1);
}
int
main(void)
{
	seed_members();
	UT_PLAN(13);
	UT_RUN(select_and_deliver);
	UT_RUN(lost_notification);
	UT_RUN(parent_does_not_prove_children);
	UT_RUN(failed_and_parallel_not_applied);
	UT_RUN(pending_not_paced);
	UT_RUN(stop_and_leave_cancel);
	UT_RUN(refused_delivery);
	UT_RUN(publication_error_cleanup);
	UT_RUN(no_membership_no_delivery);
	UT_RUN(member_change_retires_pending_selection);
	UT_RUN(changed_completion_cannot_publish);
	UT_RUN(undeclared_member_or_wrong_local_node);
	UT_RUN(observation_refresh_is_not_member_change);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
