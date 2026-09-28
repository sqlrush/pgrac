/*-------------------------------------------------------------------------
 *
 * test_cluster_config_producers.c
 *    Actual local producer controller and atomics; explicit native boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_producers.c
 * NOTES
 *    Native family/registration/member/census calls are fixtures. The real
 *    controller and gate operations execute; this is not distributed ACK or
 *    installed LMON evidence. Separate native TAP exercises original owners.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_producers.h"
#include "cluster/cluster_config_channels.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "../../backend/cluster/cluster_config_producers.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true, IsUnderPostmaster = true;
int cluster_node_id, MyProcPid = 42;
BackendType MyBackendType = B_LMON;
AuxProcType MyAuxProcType = LmonProcess;
static PGPROC proc;
static PROC_HDR proc_hdr;
PROC_HDR *ProcGlobal = &proc_hdr;
PGPROC *MyProc = &proc;
static ClusterConfigUseGate gates[CLUSTER_CONFIG_PRODUCERS_COUNT];
static ClusterConfigUseTarget targets[CLUSTER_CONFIG_PRODUCERS_COUNT];
static bool failed[CLUSTER_CONFIG_PRODUCERS_COUNT];
static ClusterConfigChannelsBoard channels;
static ClusterSharedConfigRegistration actual;
static ClusterR4MembershipSnapshot members;
static ClusterConfigMembersKey key;
static ClusterSharedConfigCensus census;
static uint8 episode[16];
static bool family_ok, members_ok, census_ok, register_ok, stopping;
static bool change_member_in_census, change_register_on_read;
static int32 lmon_pid;
static unsigned census_reads;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

ClusterConfigUseGate *
cluster_shared_config_delivery_native_gate(void)
{
	return family_ok ? &gates[0] : NULL;
}
ClusterConfigUseTarget *
cluster_shared_config_delivery_native_target(void)
{
	return family_ok ? &targets[0] : NULL;
}
ClusterConfigUseGate *
cluster_shared_config_delivery_cleaner_gate(bool *bad)
{
	*bad = !family_ok || failed[1];
	return family_ok ? &gates[1] : NULL;
}
ClusterConfigUseTarget *
cluster_shared_config_delivery_cleaner_target(void)
{
	return family_ok ? &targets[1] : NULL;
}
ClusterConfigUseGate *
cluster_shared_config_delivery_background_gate(ClusterConfigBackgroundKind kind, bool *bad)
{
	if (kind < 0 || kind >= CLUSTER_CONFIG_BACKGROUND_COUNT)
		abort();
	*bad = !family_ok || failed[2 + kind];
	return family_ok ? &gates[2 + kind] : NULL;
}
ClusterConfigUseTarget *
cluster_shared_config_delivery_background_target(ClusterConfigBackgroundKind kind)
{
	return family_ok ? &targets[2 + kind] : NULL;
}
ClusterConfigChannelsBoard *
cluster_shared_config_delivery_channels(int32 *pid)
{
	*pid = lmon_pid;
	return family_ok ? &channels : NULL;
}
bool
cluster_shared_config_registration_read(ClusterSharedConfigSlot *slot,
										ClusterSharedConfigRegistration *out)
{
	UT_ASSERT(slot == &proc.cluster_config);
	*out = actual;
	if (change_register_on_read)
		pg_atomic_fetch_add_u64(&slot->sequence, 2);
	return register_ok;
}
bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	*out = members;
	return members_ok;
}
bool
cluster_config_members_make_key(const ClusterSharedConfigRef *ref,
								const ClusterR4MembershipSnapshot *snapshot,
								ClusterConfigMembersKey *out)
{
	*out = key;
	out->ref = *ref;
	out->epoch = snapshot->formation_epoch;
	out->required[0] = snapshot->admitted_members_lo;
	out->required[1] = snapshot->admitted_members_hi;
	out->members_sha256[0] = snapshot->admitted_incarnation[0];
	return ref->identity.generation != 0 && members_ok;
}
bool
cluster_shared_config_node_common_census(const ClusterSharedConfigRef *ref, int node,
										 ClusterSharedConfigCensus *out)
{
	UT_ASSERT(node == 0);
	UT_ASSERT_EQ(ref->identity.generation, key.ref.identity.generation);
	census_reads++;
	*out = census;
	if (change_member_in_census)
		members.formation_epoch++;
	return census_ok;
}
bool
cluster_normal_stop_requested(void)
{
	return stopping;
}

static void
reset(void)
{
	memset(&producer_cut, 0, sizeof(producer_cut));
	memset(gates, 0, sizeof(gates));
	memset(targets, 0, sizeof(targets));
	memset(failed, 0, sizeof(failed));
	for (int i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i)
		cluster_config_use_gate_init(&gates[i]);
	memset(&proc, 0, sizeof(proc));
	proc.pid = MyProcPid = 42;
	proc.pgprocno = 0;
	pg_atomic_init_u64(&proc.cluster_config.sequence, 2);
	proc_hdr.allProcs = &proc;
	proc_hdr.allProcCount = 1;
	MyProc = &proc;
	ProcGlobal = &proc_hdr;
	MyBackendType = B_LMON;
	MyAuxProcType = LmonProcess;
	IsUnderPostmaster = cluster_enabled = cluster_shared_config = true;
	cluster_node_id = 0;
	family_ok = members_ok = census_ok = register_ok = true;
	stopping = change_member_in_census = change_register_on_read = false;
	lmon_pid = MyProcPid;
	census_reads = 0;
	memset(&actual, 0, sizeof(actual));
	actual.pid = MyProcPid;
	actual.role = MyBackendType;
	actual.aux_type = MyAuxProcType;
	actual.observed = true;
	actual.registration = 9;
	memset(&members, 0, sizeof(members));
	members.formation_epoch = 7;
	members.admitted_members_lo = 3;
	members.admitted_incarnation[0] = 31;
	memset(&key, 0, sizeof(key));
	key.ref.identity.system_identifier = 11;
	key.ref.identity.database_incarnation = 12;
	key.ref.identity.generation = 2;
	key.ref.identity.configured[0] = 3;
	key.ref.identity.storage_uuid[0] = 1;
	key.ref.identity.authority_uuid[0] = 2;
	key.ref.sha256[0] = 3;
	key.epoch = 7;
	key.required[0] = 3;
	key.members_sha256[0] = 31;
	memset(episode, 1, sizeof(episode));
	memset(&census, 0, sizeof(census));
	census.ref = key.ref;
	census.participants = census.current_processes = 10;
	census.active.version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	census.active.static_entries = census.active.dynamic_entries = 1;
	census.active.static_sha256[0] = 4;
	census.active.dynamic_sha256[0] = 5;
}

static ClusterConfigProducerResult
hold(int stage)
{
	return cluster_config_producers_hold(&key.ref, episode, stage);
}

static void
quiet(void)
{
	UT_ASSERT_EQ(hold(CLUSTER_CONFIG_PRODUCERS_FRONT), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT_EQ(hold(CLUSTER_CONFIG_PRODUCERS_STORAGE), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT_EQ(hold(CLUSTER_CONFIG_PRODUCERS_QUIET), CLUSTER_CONFIG_PRODUCERS_READY);
}

UT_TEST(front_owners_retire_before_background_is_held)
{
	uint32 epoch;
	ClusterConfigProducerCensus out;
	reset();
	UT_ASSERT(cluster_config_use_gate_enter(&gates[0], false, &epoch));
	UT_ASSERT(cluster_config_use_gate_enter(&gates[1], false, &epoch));
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_PENDING);
	UT_ASSERT(!cluster_config_use_gate_enter(&gates[0], false, &epoch));
	UT_ASSERT_EQ(hold(2), CLUSTER_CONFIG_PRODUCERS_PENDING);
	UT_ASSERT(!cluster_config_use_gate_read(&gates[2]).closed);
	UT_ASSERT(cluster_config_use_gate_enter(&gates[4], false, &epoch));
	UT_ASSERT(cluster_config_use_gate_leave(&gates[0]));
	UT_ASSERT(cluster_config_use_gate_leave(&gates[1]));
	UT_ASSERT_EQ(hold(2), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT(!cluster_config_use_gate_read(&gates[4]).closed);
	UT_ASSERT_EQ(hold(3), CLUSTER_CONFIG_PRODUCERS_PENDING);
	UT_ASSERT_EQ(cluster_config_producers_observe(&out), CLUSTER_CONFIG_PRODUCERS_PENDING);
	UT_ASSERT_EQ(out.pending, 1u << 4);
	UT_ASSERT_EQ(out.held, (1u << CLUSTER_CONFIG_PRODUCERS_COUNT) - 1);
	UT_ASSERT(cluster_config_use_gate_leave(&gates[4]));
	UT_ASSERT_EQ(cluster_config_producers_observe(&out), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT(!out.bound && !cluster_config_producers_release());
}

UT_TEST(stage_and_episode_are_not_caller_reset_tokens)
{
	uint8 other[16];
	reset();
	UT_ASSERT_EQ(hold(2), CLUSTER_CONFIG_PRODUCERS_INVALID);
	UT_ASSERT_EQ(hold(0), CLUSTER_CONFIG_PRODUCERS_INVALID);
	UT_ASSERT_EQ(hold(4), CLUSTER_CONFIG_PRODUCERS_INVALID);
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT_EQ(hold(3), CLUSTER_CONFIG_PRODUCERS_INVALID);
	memcpy(other, episode, 16);
	other[0]++;
	UT_ASSERT_EQ(cluster_config_producers_hold(&key.ref, other, 1),
				 CLUSTER_CONFIG_PRODUCERS_INVALID);
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT_EQ(hold(2), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_INVALID);
}

UT_TEST(unowned_closed_gate_and_partial_failure_stay_closed)
{
	uint32 cut;
	reset();
	UT_ASSERT(cluster_config_use_gate_close(&gates[1], &cut));
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_INVALID);
	UT_ASSERT(cluster_config_use_gate_read(&gates[0]).closed);
	UT_ASSERT(cluster_config_use_gate_read(&gates[1]).closed);
	UT_ASSERT(!cluster_config_producers_release());
	reset();
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_READY);
	failed[3] = true;
	UT_ASSERT_EQ(hold(2), CLUSTER_CONFIG_PRODUCERS_INVALID);
	UT_ASSERT(cluster_config_use_gate_read(&gates[2]).closed);
	failed[3] = false;
	UT_ASSERT_EQ(hold(2), CLUSTER_CONFIG_PRODUCERS_INVALID);
}

UT_TEST(actual_registered_lmon_and_member_identity_required)
{
	for (int bad = 0; bad < 13; ++bad) {
		reset();
		switch (bad) {
		case 0:
			MyBackendType = B_LMS;
			break;
		case 1:
			MyAuxProcType = LmsProcess;
			break;
		case 2:
			lmon_pid++;
			break;
		case 3:
			proc.pid++;
			break;
		case 4:
			actual.pid++;
			break;
		case 5:
			actual.aux_type = LmsProcess;
			break;
		case 6:
			actual.registration = 0;
			break;
		case 7:
			register_ok = false;
			break;
		case 8:
			change_register_on_read = true;
			break;
		case 9:
			family_ok = false;
			break;
		case 10:
			members_ok = false;
			break;
		case 11:
			MyProc = NULL;
			break;
		case 12:
			stopping = true;
			break;
		}
		UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_INVALID);
		UT_ASSERT(!cluster_config_use_gate_read(&gates[0]).closed);
	}
}

UT_TEST(identity_change_after_hold_cannot_be_undone_by_clock_or_reappearance)
{
	for (int bad = 0; bad < 4; ++bad) {
		ClusterConfigProducerCensus out;
		reset();
		quiet();
		switch (bad) {
		case 0:
			members.formation_epoch++;
			break;
		case 1:
			members.admitted_incarnation[0]++;
			break;
		case 2:
			actual.registration++;
			break;
		case 3:
			stopping = true;
			break;
		}
		UT_ASSERT_EQ(cluster_config_producers_observe(&out), CLUSTER_CONFIG_PRODUCERS_INVALID);
		members.formation_epoch = 7;
		members.admitted_incarnation[0] = 31;
		actual.registration = 9;
		stopping = false;
		UT_ASSERT_EQ(cluster_config_producers_bind(), CLUSTER_CONFIG_PRODUCERS_INVALID);
		UT_ASSERT(!cluster_config_producers_release());
		for (int i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i)
			UT_ASSERT(cluster_config_use_gate_read(&gates[i]).closed);
	}
}

UT_TEST(binding_needs_actual_complete_common_census)
{
	for (int bad = 0; bad < 12; ++bad) {
		reset();
		quiet();
		switch (bad) {
		case 0:
			census_ok = false;
			break;
		case 1:
			census.waiting_processes = 1;
			break;
		case 2:
			census.failed_processes = 1;
			break;
		case 3:
			census.parallel_processes = 1;
			break;
		case 4:
			census.active_missing_processes = 1;
			break;
		case 5:
			census.static_mismatch_processes = 1;
			break;
		case 6:
			census.dynamic_mismatch_processes = 1;
			break;
		case 7:
			census.current_processes--;
			break;
		case 8:
			census.participants = census.current_processes = 0;
			break;
		case 9:
			census.node_id = 1;
			break;
		case 10:
			census.ref.sha256[0]++;
			break;
		case 11:
			census.active.version = CLUSTER_SHARED_CONFIG_ACTIVE_VERSION;
			break;
		}
		UT_ASSERT(cluster_config_producers_bind() != CLUSTER_CONFIG_PRODUCERS_READY);
		UT_ASSERT(!cluster_config_producers_release());
		for (int i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i)
			UT_ASSERT_EQ(targets[i].epoch, 0);
	}
}

UT_TEST(bind_actual_values_not_pending_counts_and_release_exact_cut)
{
	ClusterConfigProducerCensus out;
	reset();
	quiet();
	census.pending_processes = census.deferred_processes = 1;
	census.pending_entries = census.deferred_entries = 2;
	UT_ASSERT_EQ(cluster_config_producers_bind(), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT_EQ(cluster_config_producers_observe(&out), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT(out.bound);
	for (int i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i) {
		UT_ASSERT(cluster_config_use_gate_read(&gates[i]).closed);
		UT_ASSERT_EQ(targets[i].epoch, 2);
		UT_ASSERT(memcmp(&targets[i].common, &census.active, sizeof(census.active)) == 0);
	}
	UT_ASSERT_EQ(cluster_config_producers_bind(), CLUSTER_CONFIG_PRODUCERS_READY);
	UT_ASSERT(cluster_config_producers_release());
	for (int i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i)
		UT_ASSERT(!cluster_config_use_gate_read(&gates[i]).closed);
	UT_ASSERT(!cluster_config_producers_release());
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_INVALID); /* No episode reuse. */
	episode[0]++;
	UT_ASSERT_EQ(hold(1), CLUSTER_CONFIG_PRODUCERS_READY);
}

UT_TEST(bound_values_or_membership_cannot_change_before_release)
{
	for (int bad = 0; bad < 4; ++bad) {
		reset();
		quiet();
		UT_ASSERT_EQ(cluster_config_producers_bind(), CLUSTER_CONFIG_PRODUCERS_READY);
		switch (bad) {
		case 0:
			census.active.dynamic_sha256[0]++;
			break;
		case 1:
			change_member_in_census = true;
			break;
		case 2:
			failed[1] = true;
			break;
		case 3:
			targets[5].ref.sha256[0]++;
			break;
		}
		UT_ASSERT(!cluster_config_producers_release());
		for (int i = 0; i < CLUSTER_CONFIG_PRODUCERS_COUNT; ++i)
			UT_ASSERT(cluster_config_use_gate_read(&gates[i]).closed);
	}
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(front_owners_retire_before_background_is_held);
	UT_RUN(stage_and_episode_are_not_caller_reset_tokens);
	UT_RUN(unowned_closed_gate_and_partial_failure_stay_closed);
	UT_RUN(actual_registered_lmon_and_member_identity_required);
	UT_RUN(identity_change_after_hold_cannot_be_undone_by_clock_or_reappearance);
	UT_RUN(binding_needs_actual_complete_common_census);
	UT_RUN(bind_actual_values_not_pending_counts_and_release_exact_cut);
	UT_RUN(bound_values_or_membership_cannot_change_before_release);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
