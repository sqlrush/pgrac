/*-------------------------------------------------------------------------
 *
 * test_cluster_config_census.c
 *    PGRAC production census with native registration boundary fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_census.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "postmaster/syslogger.h"
#include "cluster/cluster_shared_config.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

pid_t PostmasterPid = 120;
bool Logging_collector = true;
static PROC_HDR procs;
static PGPROC processes[3];
PROC_HDR *ProcGlobal = &procs;
static ClusterSharedConfigSlot logger;
static ClusterSharedConfigRef target;
static unsigned reads, allocations;
static int mutate;
static bool logger_missing;
static bool allocated[3], snapshot_unavailable;

/* Native allocation is a boundary here; t/019 runs the real freelists. */
bool
ProcConfigSnapshotPids(int32 *pids, uint32 capacity)
{
	if (snapshot_unavailable || capacity != lengthof(processes))
		return false;
	for (uint32 i = 0; i < capacity; ++i)
		pids[i] = allocated[i] ? (processes[i].pid > 0 ? processes[i].pid : -1) : 0;
	return true;
}

void *
palloc(Size size)
{
	void *ptr = malloc(size);
	if (!ptr)
		abort();
	allocations++;
	return ptr;
}
void *
palloc0(Size size)
{
	void *ptr = palloc(size);
	memset(ptr, 0, size);
	return ptr;
}
void
pfree(void *ptr)
{
	if (!ptr || !allocations)
		abort();
	allocations--;
	free(ptr);
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# %s %s:%d\n", condition, file, line);
	abort();
}
bool
cluster_shared_config_registration_read(ClusterSharedConfigSlot *slot,
										ClusterSharedConfigRegistration *out)
{
	reads++;
	if (pg_atomic_read_u64(&slot->sequence) & 1)
		return false;
	*out = slot->value;
	/* Last ordinary slot: mutate an already captured participant, as a real
	 * concurrent exit/reload/PID reuse would. Production must recheck all. */
	if (slot == &processes[2].cluster_config) {
		if (mutate == 1)
			pg_atomic_fetch_add_u64(&processes[0].cluster_config.sequence, 2);
		if (mutate == 2)
			processes[0].pid++;
		if (mutate == 3)
			pg_atomic_fetch_add_u64(&procs.cluster_config_postmaster.sequence, 2);
		if (mutate == 4)
			pg_atomic_fetch_add_u64(&logger.sequence, 2);
		if (mutate == 5)
			allocated[2] = true;
	}
	return true;
}
bool
cluster_shared_config_delivery_logger_snapshot(ClusterSharedConfigRegistration *out,
											   uint64 *sequence)
{
	if (logger_missing || (pg_atomic_read_u64(&logger.sequence) & 1))
		return false;
	*out = logger.value;
	*sequence = pg_atomic_read_u64(&logger.sequence);
	return true;
}
static void
registration(ClusterSharedConfigSlot *slot, int pid, BackendType role)
{
	memset(slot, 0, sizeof(*slot));
	pg_atomic_init_u64(&slot->sequence, 2);
	slot->value.pid = pid;
	slot->value.role = role;
	slot->value.registration = 2;
	slot->value.observed = true;
	slot->value.process.ref = target;
	slot->value.process.applier_pid = PostmasterPid;
	slot->value.active.version = CLUSTER_SHARED_CONFIG_ACTIVE_VERSION;
	slot->value.active.static_entries = 10;
	slot->value.active.dynamic_entries = 20;
	memset(slot->value.active.static_sha256, 1, 32);
	memset(slot->value.active.dynamic_sha256, 2, 32);
}
static void
setup(void)
{
	UT_ASSERT_EQ(allocations, 0);
	memset(&procs, 0, sizeof(procs));
	memset(processes, 0, sizeof(processes));
	memset(allocated, 0, sizeof(allocated));
	snapshot_unavailable = false;
	memset(&target, 1, sizeof(target));
	target.identity.configured[0] = 1;
	target.identity.configured[1] = 0;
	target.identity.generation = 2;
	procs.allProcs = processes;
	procs.allProcCount = lengthof(processes);
	ProcGlobal = &procs;
	registration(&procs.cluster_config_postmaster, PostmasterPid, B_INVALID);
	registration(&logger, 121, B_LOGGER);
	for (int i = 0; i < 2; ++i) {
		allocated[i] = true;
		processes[i].pid = 130 + i;
		registration(&processes[i].cluster_config, 130 + i, B_BACKEND);
	}
	pg_atomic_init_u64(&processes[2].cluster_config.sequence, 0);
	reads = 0;
	mutate = 0;
	logger_missing = false;
	Logging_collector = true;
}
static void
refused(void)
{
	ClusterSharedConfigCensus out, zero = { 0 };
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT_EQ(allocations, 0);
}
UT_TEST(all_actual_participants)
{
	ClusterSharedConfigCensus out;
	setup();
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.participants, 4);
	UT_ASSERT_EQ(out.current_processes, 4);
	UT_ASSERT_EQ(out.waiting_processes + out.failed_processes + out.parallel_processes, 0);
	UT_ASSERT_EQ(memcmp(&out.ref, &target, sizeof(target)), 0);
	UT_ASSERT_EQ(allocations, 0);
}
UT_TEST(parent_cannot_prove_old_child)
{
	ClusterSharedConfigCensus out;
	setup();
	processes[0].cluster_config.value.process.ref.identity.generation--;
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.current_processes, 3);
	UT_ASSERT_EQ(out.waiting_processes, 1);
}
UT_TEST(failure_and_parallel_are_distinct)
{
	ClusterSharedConfigCensus out;
	setup();
	processes[0].cluster_config.value.process.ref.identity.generation--;
	processes[0].cluster_config.value.process.failed = true;
	processes[1].cluster_config.value.process.parallel_snapshot = true;
	memset(&processes[1].cluster_config.value.process.ref, 0, sizeof(target));
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.current_processes, 2);
	UT_ASSERT_EQ(out.failed_processes, 1);
	UT_ASSERT_EQ(out.parallel_processes, 1);
	UT_ASSERT_EQ(out.waiting_processes, 0);
}
UT_TEST(cumulative_obligations_not_active_values)
{
	ClusterSharedConfigCensus out;
	setup();
	for (int i = 0; i < 2; ++i) {
		processes[i].cluster_config.value.process.pending_restart_total = UINT32_MAX;
		processes[i].cluster_config.value.process.deferred_total = UINT32_MAX;
	}
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.current_processes, 4);
	UT_ASSERT_EQ(out.pending_processes, 2);
	UT_ASSERT_EQ(out.deferred_processes, 2);
	UT_ASSERT_EQ(out.pending_entries, UINT64CONST(2) * UINT32_MAX);
	UT_ASSERT_EQ(out.deferred_entries, UINT64CONST(2) * UINT32_MAX);
}
UT_TEST(unobserved_wrong_node_or_applier_waits)
{
	for (int i = 0; i < 4; ++i) {
		ClusterSharedConfigCensus out;
		setup();
		if (i == 0)
			processes[0].cluster_config.value.observed = false;
		if (i == 1)
			processes[0].cluster_config.value.process.node_id = 1;
		if (i == 2)
			processes[0].cluster_config.value.process.applier_pid = 0;
		if (i == 3)
			processes[0].cluster_config.value.process.ref.sha256[0] ^= 1;
		UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
		UT_ASSERT_EQ(out.waiting_processes, 1);
	}
}
UT_TEST(birth_exit_and_reuse_refuse_whole_scan)
{
	for (int i = 1; i <= 5; ++i) {
		setup();
		mutate = i;
		refused();
	}
	setup();
	processes[0].pid = 0;
	refused();
	setup();
	allocated[2] = true;
	processes[2].pid = 201;
	refused();
}
UT_TEST(native_free_slot_retains_stale_pid)
{
	ClusterSharedConfigCensus out;
	setup();
	/* ProcKill returns this slot to its native freelist without clearing PID. */
	processes[2].pid = 201;
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.participants, 4);
	UT_ASSERT_EQ(out.current_processes, 4);
}
UT_TEST(allocated_before_native_attach_is_not_free)
{
	setup();
	allocated[2] = true;
	/* InitProcess removed a fresh slot from its list but has not set PID. */
	refused();
	setup();
	snapshot_unavailable = true;
	refused();
}
UT_TEST(busy_parent_and_logger)
{
	setup();
	pg_atomic_write_u64(&procs.cluster_config_postmaster.sequence, 3);
	refused();
	setup();
	logger_missing = true;
	refused();
	setup();
	logger.value.role = B_BACKEND;
	refused();
	setup();
	procs.cluster_config_postmaster.value.pid++;
	refused();
}
UT_TEST(empty_logger_when_disabled)
{
	ClusterSharedConfigCensus out;
	setup();
	Logging_collector = false;
	logger_missing = true;
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.participants, 3);
}
UT_TEST(invalid_and_alias_inputs)
{
	ClusterSharedConfigCensus out, before;
	setup();
	ProcGlobal = NULL;
	refused();
	ProcGlobal = &procs;
	UT_ASSERT(!cluster_shared_config_node_census(NULL, 0, &out));
	UT_ASSERT(!cluster_shared_config_node_census(&target, 128, &out));
	UT_ASSERT(!cluster_shared_config_node_census(&target, 1, &out));
	memset(&out, 0xa5, sizeof(out));
	out.ref = target;
	before = out;
	UT_ASSERT(!cluster_shared_config_node_census(&out.ref, 0, &out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
}
UT_TEST(active_profiles_are_independent_of_consumed_ref)
{
	ClusterSharedConfigCensus out;
	setup();
	processes[0].cluster_config.value.active.static_sha256[3] ^= 1;
	processes[1].cluster_config.value.active.dynamic_sha256[3] ^= 1;
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.current_processes, 4);
	UT_ASSERT_EQ(out.active.version, CLUSTER_SHARED_CONFIG_ACTIVE_VERSION);
	UT_ASSERT_EQ(out.active_missing_processes, 0);
	UT_ASSERT_EQ(out.static_mismatch_processes, 1);
	UT_ASSERT_EQ(out.dynamic_mismatch_processes, 1);
}
UT_TEST(invalidation_and_parent_unavailable_are_not_equality)
{
	ClusterSharedConfigCensus out;
	setup();
	memset(&processes[0].cluster_config.value.active, 0, sizeof(out.active));
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.current_processes, 4);
	UT_ASSERT_EQ(out.active_missing_processes, 1);
	memset(&procs.cluster_config_postmaster.value.active, 0, sizeof(out.active));
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.active.version, 0);
	UT_ASSERT_EQ(out.active_missing_processes, 2);
	UT_ASSERT_EQ(out.static_mismatch_processes + out.dynamic_mismatch_processes, 0);
}
UT_TEST(profile_schema_and_counts_must_match)
{
	ClusterSharedConfigCensus out;
	setup();
	processes[0].cluster_config.value.active.version++;
	processes[1].cluster_config.value.active.static_entries++;
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.active_missing_processes, 1);
	UT_ASSERT_EQ(out.static_mismatch_processes, 1);
	processes[1].cluster_config.value.process.failed = true;
	UT_ASSERT(cluster_shared_config_node_census(&target, 0, &out));
	UT_ASSERT_EQ(out.failed_processes, 1);
	UT_ASSERT_EQ(out.static_mismatch_processes, 0);
}
int
main(void)
{
	UT_PLAN(14);
	UT_RUN(all_actual_participants);
	UT_RUN(parent_cannot_prove_old_child);
	UT_RUN(failure_and_parallel_are_distinct);
	UT_RUN(cumulative_obligations_not_active_values);
	UT_RUN(unobserved_wrong_node_or_applier_waits);
	UT_RUN(birth_exit_and_reuse_refuse_whole_scan);
	UT_RUN(native_free_slot_retains_stale_pid);
	UT_RUN(allocated_before_native_attach_is_not_free);
	UT_RUN(busy_parent_and_logger);
	UT_RUN(empty_logger_when_disabled);
	UT_RUN(invalid_and_alias_inputs);
	UT_RUN(active_profiles_are_independent_of_consumed_ref);
	UT_RUN(invalidation_and_parent_unavailable_are_not_equality);
	UT_RUN(profile_schema_and_counts_must_match);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
