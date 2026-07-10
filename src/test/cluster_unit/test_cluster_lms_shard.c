/*-------------------------------------------------------------------------
 *
 * test_cluster_lms_shard.c
 *	  Unit tests for the LMS worker-pool shard map pure layer (spec-7.3 D1).
 *
 *	  cluster_lms_shard_for_tag() is the single (BufferTag -> worker)
 *	  mapping that keeps per-tag FIFO ordering intact after the DATA
 *	  plane is split N ways (spec-7.3 §3.1, 8.A load-bearing invariant).
 *	  These tests pin the truth table the spec's D0-① made mandatory:
 *
 *	    - the shard is in [0, n_workers) for every tag / N,
 *	    - N == 1 always maps to worker 0 (spec-7.2 topology identity;
 *	      worker 0 is a real LMS, not a sentinel -- L449),
 *	    - the shard depends on the BufferTag ALONE (never request_id /
 *	      backend / direction), so a same-tag REQUEST and its later ACK
 *	      ride the same worker stream (D0-① WATCH),
 *	    - every BufferTag field feeds the hash (no degenerate map), and
 *	    - the mapping is deterministic + reasonably balanced.
 *
 *	  The real 2-node cross-worker routing (counter deltas prove tags
 *	  land on distinct workers) is the D9 TAP; this file pins the pure
 *	  shard math that both ends of a connection must agree on.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lms_shard.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.3-lms-worker-pool.md (D1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/relpath.h" /* ForkNumber names */
#include "storage/block.h"
#include "cluster/cluster_lms_shard.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* Build a BufferTag from its five flat fields (spec-7.3 shard-key domain). */
static BufferTag
make_tag(Oid spc, Oid db, RelFileNumber rel, ForkNumber fork, BlockNumber blk)
{
	BufferTag tag;

	tag.spcOid = spc;
	tag.dbOid = db;
	tag.relNumber = rel;
	tag.forkNum = fork;
	tag.blockNum = blk;
	return tag;
}

/* ======================================================================
 * U1 -- shard is always in [0, n_workers) for every tag and every N.
 * ====================================================================== */
UT_TEST(test_shard_in_range)
{
	int n;
	BlockNumber blk;

	for (n = 1; n <= CLUSTER_LMS_MAX_WORKERS; n++) {
		for (blk = 0; blk < 200; blk++) {
			BufferTag tag = make_tag(1663, 5, 16384 + (blk % 7), MAIN_FORKNUM, blk);
			int s = cluster_lms_shard_for_tag(&tag, n);

			UT_ASSERT(s >= 0);
			UT_ASSERT(s < n);
		}
	}
}

/* ======================================================================
 * U2 -- N == 1 degenerate: every tag maps to worker 0 (7.2 identity;
 *		 worker 0 is a live LMS, not a sentinel -- L449).
 * ====================================================================== */
UT_TEST(test_shard_n1_degenerate_zero)
{
	BlockNumber blk;

	for (blk = 0; blk < 500; blk++) {
		BufferTag tag = make_tag(1663 + (blk % 3), 5 + (blk % 4), 16384 + blk,
								 (ForkNumber)(blk % (MAX_FORKNUM + 1)), blk * 13);

		UT_ASSERT_EQ(cluster_lms_shard_for_tag(&tag, 1), 0);
	}
}

/* ======================================================================
 * U3 -- deterministic: same (tag, N) always yields the same shard.
 * ====================================================================== */
UT_TEST(test_shard_deterministic)
{
	int n;
	int i;

	for (i = 0; i < 100; i++) {
		BufferTag tag = make_tag(1663, 5, 16384 + i, FSM_FORKNUM, i * 7 + 1);

		for (n = 1; n <= CLUSTER_LMS_MAX_WORKERS; n++) {
			int a = cluster_lms_shard_for_tag(&tag, n);
			int b = cluster_lms_shard_for_tag(&tag, n);

			UT_ASSERT_EQ(a, b);
		}
	}
}

/* ======================================================================
 * U4 -- depends on the BufferTag ALONE (D0-① load-bearing invariant).
 *
 *		The function takes no request_id / backend / direction, so its
 *		result must be a pure function of the tag bytes.  Two
 *		independently constructed byte-identical tags must map alike, and
 *		intervening calls for other tags must not perturb the mapping (no
 *		hidden per-call state).
 * ====================================================================== */
UT_TEST(test_shard_depends_only_on_tag)
{
	BufferTag t1 = make_tag(1663, 5, 16384, MAIN_FORKNUM, 42);
	BufferTag t2 = make_tag(1663, 5, 16384, MAIN_FORKNUM, 42);
	int ref = cluster_lms_shard_for_tag(&t1, CLUSTER_LMS_MAX_WORKERS);
	int i;

	/* Byte-identical tag -> identical shard. */
	UT_ASSERT_EQ(cluster_lms_shard_for_tag(&t2, CLUSTER_LMS_MAX_WORKERS), ref);

	/* Interleave unrelated tags; ref must stay stable (no hidden state). */
	for (i = 0; i < 50; i++) {
		BufferTag noise = make_tag(2000 + i, 9, 30000 + i, VISIBILITYMAP_FORKNUM, i);

		(void)cluster_lms_shard_for_tag(&noise, CLUSTER_LMS_MAX_WORKERS);
		UT_ASSERT_EQ(cluster_lms_shard_for_tag(&t1, CLUSTER_LMS_MAX_WORKERS), ref);
	}
}

/* ======================================================================
 * U5 -- same tag routes identically for every block-family message type
 *		 (D0-① WATCH: the same-tag INVALIDATE-ACK vs re-REQUEST wire-FIFO
 *		 dependency survives the split iff all five msg types of a tag
 *		 share one worker).  msg_type is NOT a shard input, so the shard
 *		 is invariant across the whole family for a given tag.
 * ====================================================================== */
UT_TEST(test_shard_same_tag_all_msg_types)
{
	/* Represents REQUEST / REPLY / FORWARD / INVALIDATE / ACK -- none is
	 * a shard input, so all must hit the same worker for a given tag. */
	int n_msg_types = 5;
	int i;

	for (i = 0; i < 64; i++) {
		BufferTag tag = make_tag(1663, 5, 16384 + (i % 11), MAIN_FORKNUM, i);
		int ref = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);
		int m;

		for (m = 0; m < n_msg_types; m++) {
			/* The msg type is deliberately not passed: the shard must be
			 * the tag's shard regardless of which message carries it. */
			UT_ASSERT_EQ(cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS), ref);
		}
	}
}

/*
 * Helper: does varying one field (holding the others fixed) ever change
 * the shard?  Returns true if at least two distinct shards are observed.
 */
static bool
field_moves_shard(int which_field)
{
	int seen_mask = 0;
	int i;

	for (i = 0; i < 256; i++) {
		Oid spc = 1663;
		Oid db = 5;
		RelFileNumber rel = 16384;
		ForkNumber fork = MAIN_FORKNUM;
		BlockNumber blk = 100;
		BufferTag tag;
		int s;

		switch (which_field) {
		case 0:
			spc = 1000 + (Oid)i;
			break;
		case 1:
			db = 1000 + (Oid)i;
			break;
		case 2:
			rel = 16384 + (RelFileNumber)i;
			break;
		case 3:
			fork = (ForkNumber)(i % (MAX_FORKNUM + 1));
			break;
		case 4:
			blk = (BlockNumber)i;
			break;
		default:
			break;
		}

		tag = make_tag(spc, db, rel, fork, blk);
		s = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);
		seen_mask |= (1 << s);
	}

	/* More than one distinct shard bit set == this field feeds the hash. */
	return (seen_mask & (seen_mask - 1)) != 0;
}

/* ======================================================================
 * U6 -- every BufferTag field feeds the hash (no degenerate map).  A
 *		 constant / partial hash would fail this (guards against a shard
 *		 fn that ignores blockNum, fork, etc.).
 * ====================================================================== */
UT_TEST(test_shard_field_sensitivity)
{
	UT_ASSERT(field_moves_shard(0)); /* spcOid */
	UT_ASSERT(field_moves_shard(1)); /* dbOid */
	UT_ASSERT(field_moves_shard(2)); /* relNumber */
	UT_ASSERT(field_moves_shard(3)); /* forkNum */
	UT_ASSERT(field_moves_shard(4)); /* blockNum */
}

/* ======================================================================
 * U7 -- distribution: over many distinct tags every worker gets traffic
 *		 and the map is roughly balanced (guards against degeneracy).
 * ====================================================================== */
UT_TEST(test_shard_distribution)
{
	int n_list[3] = { 2, 4, 8 };
	int total = 4096;
	int ni;

	for (ni = 0; ni < 3; ni++) {
		int n = n_list[ni];
		int buckets[CLUSTER_LMS_MAX_WORKERS];
		int min_bucket;
		int b;
		int i;

		for (b = 0; b < CLUSTER_LMS_MAX_WORKERS; b++)
			buckets[b] = 0;

		for (i = 0; i < total; i++) {
			BufferTag tag = make_tag(1663, 5, 16384 + (i / 512), MAIN_FORKNUM,
									 (BlockNumber)(i % 512) + (i / 512) * 1000);
			int s = cluster_lms_shard_for_tag(&tag, n);

			buckets[s]++;
		}

		/* Every worker must get at least one tag (catches constant maps). */
		min_bucket = total;
		for (b = 0; b < n; b++) {
			UT_ASSERT(buckets[b] > 0);
			if (buckets[b] < min_bucket)
				min_bucket = buckets[b];
		}
		/* Loose balance: no bucket below mean/4 (mean = total/n).  Safe for
		 * a decent hash over 4096 items; still catches a lopsided map. */
		UT_ASSERT(min_bucket >= (total / n) / 4);
	}
}

/* ======================================================================
 * U8 -- boundary tags: InvalidOid / all-max / block 0 / every fork all
 *		 produce in-range shards; N==1 stays 0.
 * ====================================================================== */
UT_TEST(test_shard_boundary_tags)
{
	BufferTag tags[6];
	int n_tags = 6;
	int n;
	int i;

	tags[0] = make_tag(InvalidOid, InvalidOid, InvalidRelFileNumber, MAIN_FORKNUM, 0);
	tags[1] = make_tag(0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu, INIT_FORKNUM, 0xFFFFFFFFu);
	tags[2] = make_tag(1663, 5, 16384, FSM_FORKNUM, 0);
	tags[3] = make_tag(1663, 5, 16384, VISIBILITYMAP_FORKNUM, 0);
	tags[4] = make_tag(1663, 5, 16384, INIT_FORKNUM, 0);
	tags[5] = make_tag(1663, 5, 16384, MAIN_FORKNUM, 0xFFFFFFFFu);

	for (i = 0; i < n_tags; i++) {
		for (n = 1; n <= CLUSTER_LMS_MAX_WORKERS; n++) {
			int s = cluster_lms_shard_for_tag(&tags[i], n);

			UT_ASSERT(s >= 0);
			UT_ASSERT(s < n);
		}
		UT_ASSERT_EQ(cluster_lms_shard_for_tag(&tags[i], 1), 0);
	}
}

/* ======================================================================
 * U9 -- golden characterization: the exact (tag -> shard) mapping is
 *		 pinned so any accidental change to the field packing or hash
 *		 (which would silently diverge the two ends of a connection --
 *		 R1) fails loudly.  Values captured on the reference build; the
 *		 map is a stable cluster-wide contract, not an implementation
 *		 detail.  If BufferTag layout or hash_bytes_extended changes, this
 *		 is expected to fail and the wire compatibility must be reasoned
 *		 through before re-blessing.
 * ====================================================================== */
UT_TEST(test_shard_golden)
{
	struct {
		BufferTag tag;
		int n2;
		int n4;
		int n8;
	} cases[6] = {
		{ { 1663, 5, 16384, MAIN_FORKNUM, 0 }, 1, 1, 1 },
		{ { 1663, 5, 16384, MAIN_FORKNUM, 1 }, 0, 2, 2 },
		{ { 1663, 5, 16384, MAIN_FORKNUM, 42 }, 0, 0, 0 },
		{ { 1663, 5, 16385, MAIN_FORKNUM, 0 }, 0, 0, 0 },
		{ { 1663, 12345, 16384, FSM_FORKNUM, 7 }, 1, 3, 3 },
		{ { 1700, 99, 20000, VISIBILITYMAP_FORKNUM, 123 }, 1, 1, 5 },
	};
	int i;

	for (i = 0; i < 6; i++) {
		UT_ASSERT_EQ(cluster_lms_shard_for_tag(&cases[i].tag, 2), cases[i].n2);
		UT_ASSERT_EQ(cluster_lms_shard_for_tag(&cases[i].tag, 4), cases[i].n4);
		UT_ASSERT_EQ(cluster_lms_shard_for_tag(&cases[i].tag, 8), cases[i].n8);
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_shard_in_range);
	UT_RUN(test_shard_n1_degenerate_zero);
	UT_RUN(test_shard_deterministic);
	UT_RUN(test_shard_depends_only_on_tag);
	UT_RUN(test_shard_same_tag_all_msg_types);
	UT_RUN(test_shard_field_sensitivity);
	UT_RUN(test_shard_distribution);
	UT_RUN(test_shard_boundary_tags);
	UT_RUN(test_shard_golden);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
