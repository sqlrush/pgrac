/*-------------------------------------------------------------------------
 *
 * test_cluster_config_use_gate.c
 *    Execute the native configuration cut with competing real processes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_use_gate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include "cluster/cluster_config_use_gate.h"
#include "cluster/cluster_shared_config.h"
#include "miscadmin.h"
#include "portability/mem.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static ClusterConfigUseGate gate;

static ClusterSharedConfigRegistration
actual_config(void)
{
	ClusterSharedConfigRegistration r = { 0 };
	r.pid = r.process.applier_pid = 100;
	r.role = B_BACKEND;
	r.registration = 2;
	r.observed = true;
	r.process.ref.identity.system_identifier = 1234;
	r.process.ref.identity.database_incarnation = 9;
	r.process.ref.identity.generation = 20;
	r.process.ref.identity.configured[0] = 3;
	memset(r.process.ref.identity.storage_uuid, 1, 16);
	memset(r.process.ref.identity.authority_uuid, 2, 16);
	memset(r.process.ref.sha256, 3, 32);
	r.common.version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	r.common.static_entries = 5;
	r.common.dynamic_entries = 7;
	memset(r.common.static_sha256, 4, 32);
	memset(r.common.dynamic_sha256, 5, 32);
	return r;
}

UT_TEST(bind_only_exact_empty_cut)
{
	ClusterConfigUseTarget target = { 0 }, before;
	ClusterSharedConfigRegistration r = actual_config();
	uint32 epoch, cut;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, 1, 0, &r.process.ref, &r.common));
	UT_ASSERT(cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT(
		!cluster_config_use_target_bind(&gate, &target, cut - 1, 0, &r.process.ref, &r.common));
	UT_ASSERT(cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	before = target;
	UT_ASSERT(cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	r.common.dynamic_sha256[0]++;
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	UT_ASSERT(memcmp(&before, &target, sizeof(target)) == 0);
	UT_ASSERT(cluster_config_use_gate_open(&gate, cut));
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
}

UT_TEST(binding_requires_complete_actual_profile)
{
	ClusterConfigUseTarget target = { 0 };
	ClusterSharedConfigRegistration r = actual_config(), bad;
	uint32 cut;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, cut, 2, &r.process.ref, &r.common));
	for (int kind = 0; kind < 7; kind++) {
		bad = r;
		switch (kind) {
		case 0:
			bad.process.ref.identity.generation = 0;
			break;
		case 1:
			memset(bad.process.ref.sha256, 0, 32);
			break;
		case 2:
			bad.common.version = CLUSTER_SHARED_CONFIG_ACTIVE_VERSION;
			break;
		case 3:
			bad.common.static_entries = 0;
			break;
		case 4:
			bad.common.dynamic_entries = 0;
			break;
		case 5:
			memset(bad.common.dynamic_sha256, 0, 32);
			break;
		case 6:
			bad.common.static_entries = CLUSTER_SHARED_CONFIG_MAX_ENTRIES;
			break;
		}
		UT_ASSERT(
			!cluster_config_use_target_bind(&gate, &target, cut, 0, &bad.process.ref, &bad.common));
		UT_ASSERT_EQ(target.epoch, 0);
	}
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, cut, 0, &target.ref, &r.common));
	UT_ASSERT(
		!cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &target.common));
}

static ClusterConfigUseTarget
bound_target(void)
{
	ClusterSharedConfigRegistration r = actual_config();
	ClusterConfigUseTarget target = { 0 };
	uint32 cut;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT(cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	UT_ASSERT(cluster_config_use_gate_open(&gate, cut));
	return target;
}

UT_TEST(common_use_requires_actual_registration)
{
	ClusterConfigUseTarget target = bound_target();
	ClusterSharedConfigRegistration r = actual_config(), bad;
	UT_ASSERT(cluster_config_use_target_matches(&target, target.epoch, &r));
	UT_ASSERT(!cluster_config_use_target_matches(&target, target.epoch - 1, &r));
	for (int kind = 0; kind < 8; kind++) {
		bad = r;
		switch (kind) {
		case 0:
			bad.observed = false;
			break;
		case 1:
			bad.registration = 0;
			break;
		case 2:
			bad.pid = 0;
			break;
		case 3:
			bad.process.applier_pid = 0;
			break;
		case 4:
			bad.process.failed = true;
			break;
		case 5:
			bad.process.parallel_snapshot = true;
			break;
		case 6:
			bad.process.node_id = 1;
			break;
		case 7:
			bad.common.version = 0;
			break;
		}
		UT_ASSERT(!cluster_config_use_target_matches(&target, target.epoch, &bad));
	}
}

UT_TEST(common_use_namespace_and_reference)
{
	ClusterConfigUseTarget target = bound_target();
	ClusterSharedConfigRegistration r = actual_config(), bad;
	for (int kind = 0; kind < 8; kind++) {
		bad = r;
		switch (kind) {
		case 0:
			bad.process.ref.identity.system_identifier++;
			break;
		case 1:
			bad.process.ref.identity.database_incarnation++;
			break;
		case 2:
			bad.process.ref.identity.storage_uuid[0]++;
			break;
		case 3:
			bad.process.ref.identity.authority_uuid[0]++;
			break;
		case 4:
			bad.process.ref.identity.configured[0]++;
			break;
		case 5:
			bad.process.ref.identity.generation--;
			break;
		case 6:
			bad.process.ref.sha256[0]++;
			break;
		case 7:
			memset(bad.process.ref.sha256, 0, 32);
			bad.process.ref.identity.generation++;
			break;
		}
		UT_ASSERT(!cluster_config_use_target_matches(&target, target.epoch, &bad));
	}
}

UT_TEST(common_use_compares_actual_not_pending_or_session_values)
{
	ClusterConfigUseTarget target = bound_target();
	ClusterSharedConfigRegistration r = actual_config(), bad;
	/* A real later defaults/pending outcome is not a new common profile. */
	r.process.ref.identity.generation++;
	r.process.ref.sha256[0]++;
	r.process.pending_restart_total = 2;
	r.process.deferred_total = 1;
	memset(&r.active, 0xa5, sizeof(r.active));
	UT_ASSERT(cluster_config_use_target_matches(&target, target.epoch, &r));
	for (int kind = 0; kind < 4; kind++) {
		bad = r;
		switch (kind) {
		case 0:
			bad.common.static_entries++;
			break;
		case 1:
			bad.common.dynamic_entries++;
			break;
		case 2:
			bad.common.static_sha256[0]++;
			break;
		case 3:
			bad.common.dynamic_sha256[0]++;
			break;
		}
		UT_ASSERT(!cluster_config_use_target_matches(&target, target.epoch, &bad));
	}
}

UT_TEST(bound_reader_prevents_target_replacement)
{
	ClusterConfigUseTarget target = bound_target(), before = target;
	ClusterSharedConfigRegistration r = actual_config();
	uint32 epoch, cut;
	UT_ASSERT(cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	r.process.ref.identity.generation++;
	UT_ASSERT(!cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	UT_ASSERT(memcmp(&before, &target, sizeof(target)) == 0);
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT(cluster_config_use_target_bind(&gate, &target, cut, 0, &r.process.ref, &r.common));
	UT_ASSERT_EQ(target.epoch, cut);
	UT_ASSERT(!cluster_config_use_target_matches(&target, epoch, &r));
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

UT_TEST(independent_owners)
{
	uint32 epoch, other;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT(cluster_config_use_gate_enter(&gate, false, &other));
	UT_ASSERT_EQ(epoch, 1);
	UT_ASSERT_EQ(other, epoch);
	UT_ASSERT_EQ(cluster_config_use_gate_read(&gate).owners, 2);
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT(!cluster_config_use_gate_leave(&gate));
}

UT_TEST(held_cut_and_exact_open)
{
	uint32 epoch, cut;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT_EQ(cut, epoch + 1);
	UT_ASSERT(!cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT_EQ(epoch, 0);
	UT_ASSERT(!cluster_config_use_gate_open(&gate, cut));
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT(!cluster_config_use_gate_open(&gate, cut - 1));
	UT_ASSERT(cluster_config_use_gate_read(&gate).closed);
	UT_ASSERT(cluster_config_use_gate_open(&gate, cut));
	UT_ASSERT(!cluster_config_use_gate_open(&gate, cut));
}

UT_TEST(repeated_close_does_not_replace_cut)
{
	uint32 first, again;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(cluster_config_use_gate_close(&gate, &first));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &again));
	UT_ASSERT_EQ(first, again);
	UT_ASSERT(cluster_config_use_gate_open(&gate, first));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &again));
	UT_ASSERT_EQ(again, first + 1);
	UT_ASSERT(!cluster_config_use_gate_open(&gate, first));
}

UT_TEST(continuation_is_only_a_counted_reservation)
{
	uint32 epoch, continuation, cut;
	cluster_config_use_gate_init(&gate);
	UT_ASSERT(cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT(cluster_config_use_gate_enter(&gate, true, &continuation));
	UT_ASSERT_EQ(epoch, continuation);
	UT_ASSERT_EQ(cluster_config_use_gate_read(&gate).owners, 2);
	/* Native binding must still prove its exact leader; refusal drops only
	 * this provisional owner, never the original parent's reservation. */
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT_EQ(cluster_config_use_gate_read(&gate).owners, 1);
	UT_ASSERT(cluster_config_use_gate_leave(&gate));
	UT_ASSERT(!cluster_config_use_gate_enter(&gate, true, &continuation));
	UT_ASSERT_EQ(continuation, 0);
	UT_ASSERT(cluster_config_use_gate_open(&gate, cut));
}

UT_TEST(no_count_or_generation_wrap)
{
	uint32 epoch, cut;
	cluster_config_use_gate_init(&gate);
	pg_atomic_write_u64(&gate.state, UINT64CONST(0x00000001ffffffff));
	UT_ASSERT(!cluster_config_use_gate_enter(&gate, false, &epoch));
	UT_ASSERT(cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT_EQ(cluster_config_use_gate_read(&gate).owners, PG_UINT32_MAX);
	pg_atomic_write_u64(&gate.state, UINT64CONST(0x7fffffff00000000));
	UT_ASSERT(!cluster_config_use_gate_close(&gate, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(cluster_config_use_gate_read(&gate).closed);
	UT_ASSERT(!cluster_config_use_gate_open(&gate, 0x7fffffff));
	UT_ASSERT(!cluster_config_use_gate_enter(&gate, false, &epoch));
}

UT_TEST(competing_process_entry_and_close)
{
	ClusterConfigUseGate *shared
		= mmap(NULL, sizeof(*shared), PROT_READ | PROT_WRITE, PG_MMAP_FLAGS, -1, 0);
	UT_ASSERT(shared != MAP_FAILED);
	if (shared == MAP_FAILED)
		return;
	for (int i = 0; i < 64; ++i) {
		int start[2], result[2], retire[2], status;
		uint32 cut;
		char byte = 1;
		pid_t child;
		cluster_config_use_gate_init(shared);
		if (pipe(start) || pipe(result) || pipe(retire))
			abort();
		fflush(NULL);
		child = fork();
		if (child == 0) {
			uint32 epoch;
			bool entered;
			if (read(start[0], &byte, 1) != 1)
				_exit(2);
			entered = cluster_config_use_gate_enter(shared, false, &epoch);
			byte = entered;
			if (write(result[1], &byte, 1) != 1 || read(retire[0], &byte, 1) != 1)
				_exit(3);
			if (entered && !cluster_config_use_gate_leave(shared))
				_exit(4);
			_exit(0);
		}
		if (child < 0)
			abort();
		UT_ASSERT_EQ(write(start[1], &byte, 1), 1);
		UT_ASSERT(cluster_config_use_gate_close(shared, &cut));
		UT_ASSERT_EQ(read(result[0], &byte, 1), 1);
		UT_ASSERT_EQ(cluster_config_use_gate_read(shared).owners, (unsigned)byte);
		if (byte)
			UT_ASSERT(!cluster_config_use_gate_open(shared, cut));
		UT_ASSERT_EQ(write(retire[1], &byte, 1), 1);
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		UT_ASSERT_EQ(cluster_config_use_gate_read(shared).owners, 0);
		UT_ASSERT(cluster_config_use_gate_open(shared, cut));
		close(start[0]);
		close(start[1]);
		close(result[0]);
		close(result[1]);
		close(retire[0]);
		close(retire[1]);
	}
	UT_ASSERT_EQ(munmap(shared, sizeof(*shared)), 0);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(independent_owners);
	UT_RUN(held_cut_and_exact_open);
	UT_RUN(repeated_close_does_not_replace_cut);
	UT_RUN(continuation_is_only_a_counted_reservation);
	UT_RUN(no_count_or_generation_wrap);
	UT_RUN(competing_process_entry_and_close);
	UT_RUN(bind_only_exact_empty_cut);
	UT_RUN(binding_requires_complete_actual_profile);
	UT_RUN(common_use_requires_actual_registration);
	UT_RUN(common_use_namespace_and_reference);
	UT_RUN(common_use_compares_actual_not_pending_or_session_values);
	UT_RUN(bound_reader_prevents_target_replacement);
	UT_DONE();
	return ut_failed_count != 0;
}
