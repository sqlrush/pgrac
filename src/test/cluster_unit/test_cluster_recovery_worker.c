/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_worker.c
 *	  pgrac spec-4.4 D7 — cluster_unit tests for the recovery-worker
 *	  pure helpers (cluster_recovery_worker.h, header-only inline).
 *
 *	  15 tests covering:
 *	    T1   striping cap >= N: one candidate per slot (1:1)
 *	    T2   striping cap < N: round-robin, pairwise disjoint, full
 *	         cover (union == candidate set)
 *	    T3   cap = 0 -> 0 workers
 *	    T4   no candidates -> 0 workers
 *	    T5   cap > MAX_SLOTS clamps to 16
 *	    T6   conservation at scale: 128 candidates / cap 16 -> every
 *	         slot gets exactly 8 threads, disjoint cover
 *	    T7   page check: good header -> OK
 *	    T8   page check: bad magic -> SUSPECT (zeroed tail shape)
 *	    T9   page check: wrong pageaddr -> SUSPECT (recycled stale
 *	         content carries the OLD segment's pageaddr; P0)
 *	    T10  page check: wrong thread stamp -> SUSPECT
 *	    T11  page check: LEGACY 0 stamp -> OK (mixed segments legal)
 *	    T12  enum value locks (verdicts stored as uint8 in shmem;
 *	         slot states stored in pg_atomic_uint32)
 *	    T13  target-page math: mid-segment + EXACT SEGMENT BOUNDARY
 *	         (highest_lsn at seg start -> last page of PREVIOUS seg)
 *	    T14  target-page math: highest_lsn == 0 -> false
 *	    T15  target pageaddr is page-aligned and contains target byte
 *
 *	  Linkage mirrors test_cluster_wal_state: header-only inclusion +
 *	  libpgcommon/libpgport.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.4-recovery-worker-skeleton.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_recovery_worker.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* cassert builds pull libpgport objects that reference this. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#define SEGSZ (16 * 1024 * 1024)

static void
set_candidate(uint64 bitmap[2], uint16 tid)
{
	bitmap[(tid - 1) / 64] |= ((uint64)1 << ((tid - 1) % 64));
}

static bool
assigned_has(const uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2], int slot, uint16 tid)
{
	return (assigned[slot][(tid - 1) / 64] & ((uint64)1 << ((tid - 1) % 64))) != 0;
}

UT_TEST(test_striping_cap_ge_n)
{
	uint64 cands[2] = { 0, 0 };
	uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];
	int n;

	set_candidate(cands, 2);
	set_candidate(cands, 9);
	set_candidate(cands, 100);
	n = cluster_recovery_worker_assign(cands, 3, 4, assigned);
	UT_ASSERT_EQ(n, 3);
	UT_ASSERT(assigned_has(assigned, 0, 2));
	UT_ASSERT(assigned_has(assigned, 1, 9));
	UT_ASSERT(assigned_has(assigned, 2, 100));
	UT_ASSERT(!assigned_has(assigned, 0, 9) && !assigned_has(assigned, 0, 100));
}

UT_TEST(test_striping_cap_lt_n_roundrobin)
{
	uint64 cands[2] = { 0, 0 };
	uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];
	int n;

	/* candidates 3,5,7,11,13 with cap 2: slots get 3,7,13 / 5,11 */
	set_candidate(cands, 3);
	set_candidate(cands, 5);
	set_candidate(cands, 7);
	set_candidate(cands, 11);
	set_candidate(cands, 13);
	n = cluster_recovery_worker_assign(cands, 5, 2, assigned);
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT(assigned_has(assigned, 0, 3));
	UT_ASSERT(assigned_has(assigned, 1, 5));
	UT_ASSERT(assigned_has(assigned, 0, 7));
	UT_ASSERT(assigned_has(assigned, 1, 11));
	UT_ASSERT(assigned_has(assigned, 0, 13));
	/* disjoint */
	UT_ASSERT((assigned[0][0] & assigned[1][0]) == 0 && (assigned[0][1] & assigned[1][1]) == 0);
	/* full cover */
	UT_ASSERT((assigned[0][0] | assigned[1][0]) == cands[0]
			  && (assigned[0][1] | assigned[1][1]) == cands[1]);
}

UT_TEST(test_cap_zero_no_workers)
{
	uint64 cands[2] = { 0, 0 };
	uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];

	set_candidate(cands, 2);
	UT_ASSERT_EQ(cluster_recovery_worker_assign(cands, 1, 0, assigned), 0);
}

UT_TEST(test_no_candidates_no_workers)
{
	uint64 cands[2] = { 0, 0 };
	uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];

	UT_ASSERT_EQ(cluster_recovery_worker_assign(cands, 0, 4, assigned), 0);
}

UT_TEST(test_cap_clamps_to_max_slots)
{
	uint64 cands[2] = { 0, 0 };
	uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];
	uint16 tid;
	int n;

	for (tid = 1; tid <= 32; tid++)
		set_candidate(cands, tid);
	n = cluster_recovery_worker_assign(cands, 32, 999, assigned);
	UT_ASSERT_EQ(n, CLUSTER_RECOVERY_WORKER_MAX_SLOTS);
}

UT_TEST(test_conservation_at_scale)
{
	uint64 cands[2] = { 0, 0 };
	uint64 assigned[CLUSTER_RECOVERY_WORKER_MAX_SLOTS][2];
	uint64 cover[2] = { 0, 0 };
	uint16 tid;
	int slot;
	int n;

	for (tid = 1; tid <= 128; tid++)
		set_candidate(cands, tid);
	n = cluster_recovery_worker_assign(cands, 128, 16, assigned);
	UT_ASSERT_EQ(n, 16);
	for (slot = 0; slot < n; slot++) {
		int per_slot = 0;

		for (tid = 1; tid <= 128; tid++)
			if (assigned_has(assigned, slot, tid))
				per_slot++;
		UT_ASSERT_EQ(per_slot, 8);
		/* disjointness: no bit already covered */
		UT_ASSERT((cover[0] & assigned[slot][0]) == 0 && (cover[1] & assigned[slot][1]) == 0);
		cover[0] |= assigned[slot][0];
		cover[1] |= assigned[slot][1];
	}
	UT_ASSERT(cover[0] == cands[0] && cover[1] == cands[1]);
}

static void
fill_page_header(char *page, uint64 pageaddr, uint16 thread)
{
	XLogPageHeaderData *hdr = (XLogPageHeaderData *)page;

	memset(page, 0, XLOG_BLCKSZ);
	hdr->xlp_magic = XLOG_PAGE_MAGIC;
	hdr->xlp_info = 0;
	hdr->xlp_tli = 1;
	hdr->xlp_pageaddr = (XLogRecPtr)pageaddr;
	hdr->xlp_thread_id = thread;
}

UT_TEST(test_page_check_good)
{
	char page[XLOG_BLCKSZ];

	fill_page_header(page, 0x1A2B0000, 7);
	UT_ASSERT_EQ((int)cluster_recovery_stream_page_check(page, 0x1A2B0000, 7),
				 (int)CLUSTER_RECOVERY_STREAM_OK);
}

UT_TEST(test_page_check_bad_magic)
{
	char page[XLOG_BLCKSZ];

	memset(page, 0, sizeof(page)); /* zeroed tail shape: magic 0 */
	UT_ASSERT_EQ((int)cluster_recovery_stream_page_check(page, 0x1A2B0000, 7),
				 (int)CLUSTER_RECOVERY_STREAM_SUSPECT);
}

UT_TEST(test_page_check_recycled_pageaddr)
{
	char page[XLOG_BLCKSZ];

	/* recycled segment: valid header but the OLD address */
	fill_page_header(page, 0x0A0B0000, 7);
	UT_ASSERT_EQ((int)cluster_recovery_stream_page_check(page, 0x1A2B0000, 7),
				 (int)CLUSTER_RECOVERY_STREAM_SUSPECT);
}

UT_TEST(test_page_check_wrong_thread)
{
	char page[XLOG_BLCKSZ];

	fill_page_header(page, 0x1A2B0000, 9);
	UT_ASSERT_EQ((int)cluster_recovery_stream_page_check(page, 0x1A2B0000, 7),
				 (int)CLUSTER_RECOVERY_STREAM_SUSPECT);
}

UT_TEST(test_page_check_legacy_stamp_ok)
{
	char page[XLOG_BLCKSZ];

	fill_page_header(page, 0x1A2B0000, XLP_THREAD_ID_LEGACY);
	UT_ASSERT_EQ((int)cluster_recovery_stream_page_check(page, 0x1A2B0000, 7),
				 (int)CLUSTER_RECOVERY_STREAM_OK);
}

UT_TEST(test_enum_value_locks)
{
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_WORKER_UNUSED, 0);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_WORKER_REQUESTED, 1);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_WORKER_RUNNING, 2);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_WORKER_DONE, 3);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_WORKER_FAILED, 4);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_STREAM_NONE, 0);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_STREAM_OK, 1);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_STREAM_SUSPECT, 2);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_STREAM_UNREADABLE, 3);
	UT_ASSERT_EQ((int)CLUSTER_RECOVERY_STREAM_SKIPPED, 4);
}

UT_TEST(test_target_page_math)
{
	uint64 segno;
	uint32 page_off;
	uint64 pageaddr;

	/* mid-segment: lsn 0x1000000 + 0x5234 -> page at 0x5000 */
	UT_ASSERT(cluster_recovery_worker_target_page((uint64)SEGSZ + 0x5234, SEGSZ, &segno, &page_off,
												  &pageaddr));
	UT_ASSERT_EQ((long long)segno, 1LL);
	UT_ASSERT_EQ((long long)page_off, 0x4000LL); /* (0x5234-1) & ~(8192-1) */
	UT_ASSERT_EQ((long long)pageaddr, (long long)SEGSZ + 0x4000);

	/* EXACT segment boundary: highest_lsn == start of segment 2 ->
	 * target is the LAST page of segment 1 (never unwritten space) */
	UT_ASSERT(cluster_recovery_worker_target_page((uint64)2 * SEGSZ, SEGSZ, &segno, &page_off,
												  &pageaddr));
	UT_ASSERT_EQ((long long)segno, 1LL);
	UT_ASSERT_EQ((long long)page_off, (long long)(SEGSZ - XLOG_BLCKSZ));
	UT_ASSERT_EQ((long long)pageaddr, (long long)(2 * SEGSZ - XLOG_BLCKSZ));
}

UT_TEST(test_target_page_zero_lsn)
{
	uint64 segno;
	uint32 page_off;
	uint64 pageaddr;

	UT_ASSERT(!cluster_recovery_worker_target_page(0, SEGSZ, &segno, &page_off, &pageaddr));
}

UT_TEST(test_target_pageaddr_contains_target)
{
	uint64 segno;
	uint32 page_off;
	uint64 pageaddr;
	uint64 hl = (uint64)5 * SEGSZ + 12345;

	UT_ASSERT(cluster_recovery_worker_target_page(hl, SEGSZ, &segno, &page_off, &pageaddr));
	UT_ASSERT(pageaddr % XLOG_BLCKSZ == 0);
	UT_ASSERT(pageaddr <= hl - 1 && hl - 1 < pageaddr + XLOG_BLCKSZ);
}


int
main(int argc, char **argv)
{
	UT_PLAN(15);

	UT_RUN(test_striping_cap_ge_n);
	UT_RUN(test_striping_cap_lt_n_roundrobin);
	UT_RUN(test_cap_zero_no_workers);
	UT_RUN(test_no_candidates_no_workers);
	UT_RUN(test_cap_clamps_to_max_slots);
	UT_RUN(test_conservation_at_scale);
	UT_RUN(test_page_check_good);
	UT_RUN(test_page_check_bad_magic);
	UT_RUN(test_page_check_recycled_pageaddr);
	UT_RUN(test_page_check_wrong_thread);
	UT_RUN(test_page_check_legacy_stamp_ok);
	UT_RUN(test_enum_value_locks);
	UT_RUN(test_target_page_math);
	UT_RUN(test_target_page_zero_lsn);
	UT_RUN(test_target_pageaddr_contains_target);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
