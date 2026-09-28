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
#include "portability/mem.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static ClusterConfigUseGate gate;

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
	UT_PLAN(6);
	UT_RUN(independent_owners);
	UT_RUN(held_cut_and_exact_open);
	UT_RUN(repeated_close_does_not_replace_cut);
	UT_RUN(continuation_is_only_a_counted_reservation);
	UT_RUN(no_count_or_generation_wrap);
	UT_RUN(competing_process_entry_and_close);
	UT_DONE();
	return ut_failed_count != 0;
}
