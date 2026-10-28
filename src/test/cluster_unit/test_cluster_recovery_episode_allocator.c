/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_allocator.c
 *     Capture original allocator FullXid and native FULL CLOG WAL.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_allocator.c
 * NOTES
 *     Private fixture names only; all original providers and bodies remain.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_recovery_episode_allocator_namespace.h"
#include "unit_test.h"
#undef UT_DEFINE_GLOBALS
#define UT_DEFINE_GLOBALS()
#define TEST_CLUSTER_XID_NATIVE_CLOG
#define TEST_CLUSTER_EPISODE_ALLOCATOR
int d3_episode_allocator_main(void);
#include "test_cluster_xid_allocation.c"
#include "../../../src/backend/cluster/cluster_xid_stripe.c"
#include "test_cluster_recovery_episode_allocator.h"

bool
test_episode_allocator_produce(bool full, bool fail,
	TestEpisodeIssuedClogV1 *out, TestEpisodeAllocatorProbeV1 *probe)
{
	volatile uint64 first = 0, query = 0;
	volatile bool caught = false;
	bool ok;

	memset(out, 0, sizeof(*out));
	memset(probe, 0, sizeof(*probe));
	native_clog_begin(UINT64_C(0x3003ffffe), fail);
	cluster_node_id = 0;
	/* Bootstrap counter/lease/transaction boundaries are controlled. Every
	 * subsequent candidate, gap and counter advance comes from the original
	 * allocator. No numeric FullXid is passed to its production entry. */
	if (setjmp(full_clog_fixture.error) == 0) {
		while (U64FromFullTransactionId(variables.nextXid) <= UINT64_C(0x3004006a0)) {
			uint64 issued;
			/* Separate fixture transactions; keep the allocator's actual next. */
			memset(&proc, 0, sizeof(proc));
			memset(visible_xids, 0, sizeof(visible_xids));
			full_clog_fixture.count = 0;
			issued = U64FromFullTransactionId(full ? GetNewTransactionIdFullCLOG(false)
				: GetNewTransactionId(false));
			if (first == 0) first = issued;
			if (issued == UINT64_C(0x300400340)) query = issued;
			++probe->allocations;
			UT_ASSERT(!held && lock_errors == 0 && full_clog_fixture.early_publication == 0);
		}
	} else {
		caught = true;
	}
	probe->next_full_xid = U64FromFullTransactionId(variables.nextXid);
	probe->published_xid = visible_xids[0];
	probe->allocator_lock_held = held;
	test_xid_native_clog_state(&probe->native);
	ok = !caught && query != 0 && probe->native.inserts == 1
		&& probe->native.length == sizeof(xl_clog_zeropage_full)
		&& probe->native.info == CLOG_ZEROPAGE_FULL;
	if (ok) {
		out->first_issued = first;
		out->queried_full_xid = query;
		out->next_full_xid = probe->next_full_xid;
		out->native = probe->native;
	}
	/* Capture ERROR duty BEFORE the original fixture caller releases locks.
	 * No retry, cache repair, candidate publication or borrowed episode. */
	native_clog_end();
	return ok;
}
