/*-------------------------------------------------------------------------
 *
 * test_cluster_remote_xact.c
 *	  Pure-logic unit tests for the per-origin materialized transaction
 *	  outcome store (spec-4.5a G5, deliverable D12).
 *
 *	  The SLRU I/O, the merged-replay divert, and the CR resolver are
 *	  integration-tested on a real instance (TAP t/248); this binary
 *	  covers the dependency-free invariants the on-disk + fail-closed
 *	  contracts rest on:
 *
 *	    - {origin,xid} page partitioning: distinct origins NEVER share a
 *	      page, so a wrapped same-valued xid of a DIFFERENT origin lands
 *	      in a different partition (F2 -- the property that lets the store
 *	      answer a remote xid without the local pg_xact's cross-instance
 *	      aliasing);
 *	    - the P1-1 side-effect predicate: a foreign commit record may
 *	      materialize ONLY as a pure outcome; any cross-instance side
 *	      effect blocks the merge (fail-closed);
 *	    - the entry width ABI.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_remote_xact.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D12)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_remote_xact.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Mirror the production xact.h bit values used by the predicate (kept local
 * so the test does not pull access/xact.h's heavy include chain). */
#define UT_XACT_XINFO_HAS_TWOPHASE 0x10
#define UT_XACT_XINFO_HAS_AE_LOCKS 0x40


/* ============================================================
 * Entry ABI + page partitioning (F2)
 * ============================================================ */

UT_TEST(test_remote_xact_entry_width)
{
	UT_ASSERT_EQ(CLUSTER_REMOTE_XACT_ENTRY_BYTES, 32);
	UT_ASSERT_EQ((int)CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE, BLCKSZ / 32);
}

UT_TEST(test_remote_xact_origin_partition_disjoint)
{
	/*
	 * The SAME xid value under DIFFERENT origins must map to DIFFERENT
	 * pages -- this is the wrap-defence (F2): origin 1's xid 100 and
	 * origin 2's xid 100 are distinct transactions and must never collide
	 * on one entry.
	 */
	TransactionId xid = 100;
	int p0 = cluster_remote_xact_pageno(0, xid);
	int p1 = cluster_remote_xact_pageno(1, xid);
	int p2 = cluster_remote_xact_pageno(2, xid);

	UT_ASSERT(p0 != p1);
	UT_ASSERT(p1 != p2);
	UT_ASSERT(p0 != p2);

	/* Same (origin,xid) is stable. */
	UT_ASSERT_EQ(cluster_remote_xact_pageno(1, xid), p1);

	/* Entry index is the in-page slot, independent of origin. */
	UT_ASSERT_EQ(cluster_remote_xact_entryno(xid),
				 (int)(xid % CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE));
}

UT_TEST(test_remote_xact_pending_binding_is_byte_exact_and_fail_closed)
{
	ClusterRemoteXactEntryV2 entry;
	uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];
	uint8 changed[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];

	memset(digest, 0x4d, sizeof(digest));
	memcpy(changed, digest, sizeof(changed));
	changed[sizeof(changed) - 1]++;
	UT_ASSERT(cluster_remote_xact_entry_encode_pending_v2(&entry, digest));
	UT_ASSERT_EQ(entry.status, CLUSTER_REMOTE_XACT_STORED_PREPARED);
	UT_ASSERT(cluster_remote_xact_entry_pending_matches_v2(&entry, digest));
	UT_ASSERT(!cluster_remote_xact_entry_pending_matches_v2(&entry, changed));
	entry.format_version++;
	UT_ASSERT(!cluster_remote_xact_entry_pending_matches_v2(&entry, digest));
}

UT_TEST(test_remote_xact_terminal_codec_carries_commit_ts_and_wrap)
{
	ClusterRemoteXactEntryV2 entry;
	ClusterRemoteXactEntryDecodedV2 decoded;

	UT_ASSERT(cluster_remote_xact_entry_encode_terminal_v2(
		&entry, CLUSTER_REMOTE_XACT_COMMITTED, UINT64_C(9123), INT64_C(77112233), true, 17));
	UT_ASSERT(cluster_remote_xact_entry_decode_terminal_v2(&entry, &decoded));
	UT_ASSERT_EQ(decoded.outcome, CLUSTER_REMOTE_XACT_COMMITTED);
	UT_ASSERT_EQ(decoded.commit_scn, UINT64_C(9123));
	UT_ASSERT_EQ(decoded.commit_timestamp, INT64_C(77112233));
	UT_ASSERT(decoded.wrap_valid);
	UT_ASSERT_EQ(decoded.wrap, 17);

	entry.payload[18] = 1;
	UT_ASSERT(!cluster_remote_xact_entry_decode_terminal_v2(&entry, &decoded));
	UT_ASSERT(cluster_remote_xact_entry_encode_terminal_v2(&entry, CLUSTER_REMOTE_XACT_ABORTED,
														   InvalidScn, 0, false, 0));
	UT_ASSERT(cluster_remote_xact_entry_decode_terminal_v2(&entry, &decoded));
	UT_ASSERT_EQ(decoded.outcome, CLUSTER_REMOTE_XACT_ABORTED);
	UT_ASSERT_EQ(decoded.commit_timestamp, 0);
}

UT_TEST(test_remote_xact_prepare_transition_is_idempotent_and_conflict_closed)
{
	ClusterRemoteXactEntryV2 current;
	ClusterRemoteXactEntryV2 next;
	uint8 first[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];
	uint8 second[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];

	memset(&current, 0, sizeof(current));
	memset(first, 0x31, sizeof(first));
	memset(second, 0x32, sizeof(second));
	UT_ASSERT_EQ(cluster_remote_xact_entry_prepare_transition_v2(&current, first, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_WRITE);
	current = next;
	UT_ASSERT_EQ(cluster_remote_xact_entry_prepare_transition_v2(&current, first, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_NOOP);
	UT_ASSERT_EQ(cluster_remote_xact_entry_prepare_transition_v2(&current, second, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_CONFLICT);
	UT_ASSERT(cluster_remote_xact_entry_encode_terminal_v2(&current, CLUSTER_REMOTE_XACT_ABORTED,
														   InvalidScn, 0, false, 0));
	UT_ASSERT_EQ(cluster_remote_xact_entry_prepare_transition_v2(&current, first, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_CONFLICT);
}

UT_TEST(test_remote_xact_prepared_terminal_requires_pending_or_exact_result)
{
	ClusterRemoteXactEntryV2 current;
	ClusterRemoteXactEntryV2 next;
	uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];
	uint8 wrong_digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];

	memset(&current, 0, sizeof(current));
	memset(digest, 0x77, sizeof(digest));
	memset(wrong_digest, 0x78, sizeof(wrong_digest));
	UT_ASSERT_EQ(cluster_remote_xact_entry_terminal_transition_v2(
					 &current, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, UINT64_C(91),
					 INT64_C(1234), true, 7, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_CONFLICT);
	UT_ASSERT(cluster_remote_xact_entry_encode_pending_v2(&current, digest));
	UT_ASSERT_EQ(cluster_remote_xact_entry_terminal_transition_v2(
					 &current, true, wrong_digest, CLUSTER_REMOTE_XACT_COMMITTED, UINT64_C(91),
					 INT64_C(1234), true, 7, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_CONFLICT);
	UT_ASSERT_EQ(cluster_remote_xact_entry_terminal_transition_v2(
					 &current, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, UINT64_C(91),
					 INT64_C(1234), true, 7, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_WRITE);
	current = next;
	UT_ASSERT_EQ(cluster_remote_xact_entry_terminal_transition_v2(
					 &current, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, UINT64_C(91),
					 INT64_C(1234), true, 7, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_NOOP);
	UT_ASSERT_EQ(cluster_remote_xact_entry_terminal_transition_v2(&current, true, digest,
																  CLUSTER_REMOTE_XACT_ABORTED,
																  InvalidScn, 0, false, 0, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_CONFLICT);
}

UT_TEST(test_remote_xact_projection_reset_is_idempotent_invalidation)
{
	ClusterRemoteXactEntryV2 current;
	ClusterRemoteXactEntryV2 next;

	memset(&current, 0, sizeof(current));
	UT_ASSERT_EQ(cluster_remote_xact_entry_reset_transition_v2(&current, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_NOOP);
	UT_ASSERT(cluster_remote_xact_entry_is_empty_v2(&next));
	UT_ASSERT(cluster_remote_xact_entry_encode_terminal_v2(&current, CLUSTER_REMOTE_XACT_COMMITTED,
														   UINT64_C(991), INT64_C(881), true, 7));
	UT_ASSERT_EQ(cluster_remote_xact_entry_reset_transition_v2(&current, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_WRITE);
	UT_ASSERT(cluster_remote_xact_entry_is_empty_v2(&next));
	/* Projection corruption is invalidated, never promoted to authority. */
	memset(&current, 0xee, sizeof(current));
	UT_ASSERT_EQ(cluster_remote_xact_entry_reset_transition_v2(&current, &next),
				 CLUSTER_REMOTE_XACT_ENTRY_WRITE);
	UT_ASSERT(cluster_remote_xact_entry_is_empty_v2(&next));
}

UT_TEST(test_remote_xact_projection_reset_range_is_bounded)
{
	UT_ASSERT(cluster_remote_xact_reset_range_valid_v2(0, 0, 1));
	UT_ASSERT(cluster_remote_xact_reset_range_valid_v2(127, UINT32_MAX - 32767, 32768));
	UT_ASSERT(!cluster_remote_xact_reset_range_valid_v2(-1, 0, 1));
	UT_ASSERT(!cluster_remote_xact_reset_range_valid_v2(128, 0, 1));
	UT_ASSERT(!cluster_remote_xact_reset_range_valid_v2(0, 0, 0));
	UT_ASSERT(!cluster_remote_xact_reset_range_valid_v2(0, 0, 32769));
	UT_ASSERT(!cluster_remote_xact_reset_range_valid_v2(0, UINT32_MAX - 2, 4));
}

UT_TEST(test_remote_xact_origin_no_cross_partition_overlap)
{
	/*
	 * The highest xid-page within origin N must still precede the first
	 * page of origin N+1 -- otherwise a high xid of one origin could fall
	 * into the next origin's partition.  Highest xid-page index is
	 * (2^32 - 1) / ENTRIES_PER_PAGE; with ENTRIES_PER_PAGE = 512 that is
	 * 2^24 - 1, exactly one less than the origin stride (1 << 24).
	 */
	int max_xid_page = (int)(0xFFFFFFFFU / CLUSTER_REMOTE_XACT_ENTRIES_PER_PAGE);
	int origin_stride = 1 << CLUSTER_REMOTE_XACT_ORIGIN_PAGE_SHIFT;

	UT_ASSERT(max_xid_page < origin_stride);

	/* Concretely: origin 0's last page < origin 1's first page. */
	UT_ASSERT(cluster_remote_xact_pageno(0, 0xFFFFFFFFU) < cluster_remote_xact_pageno(1, 0));
}


/* ============================================================
 * P1-1 side-effect predicate (fail-closed parse)
 * ============================================================ */

UT_TEST(test_remote_xact_pure_outcome_allowed)
{
	/* No side effects -> mergeable (not blocked). */
	UT_ASSERT(!cluster_remote_xact_commit_blocked(0, 0, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
												  UT_XACT_XINFO_HAS_AE_LOCKS));
}

UT_TEST(test_remote_xact_side_effects_blocked)
{
	/* Each individual cross-instance side effect blocks the merge. */
	UT_ASSERT(cluster_remote_xact_commit_blocked(1, 0, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_AE_LOCKS)); /* relfile drop */
	UT_ASSERT(cluster_remote_xact_commit_blocked(0, 1, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_AE_LOCKS)); /* invalidation */
	UT_ASSERT(cluster_remote_xact_commit_blocked(0, 0, 1, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_AE_LOCKS)); /* stats drop */
	UT_ASSERT(cluster_remote_xact_commit_blocked(0, 0, 0, 1, 0, UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_AE_LOCKS)); /* subxacts */
	UT_ASSERT(cluster_remote_xact_commit_blocked(0, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_AE_LOCKS)); /* 2PC */
	UT_ASSERT(cluster_remote_xact_commit_blocked(0, 0, 0, 0, UT_XACT_XINFO_HAS_AE_LOCKS,
												 UT_XACT_XINFO_HAS_TWOPHASE,
												 UT_XACT_XINFO_HAS_AE_LOCKS)); /* AE locks */
}

UT_TEST(test_remote_xact_prepared_commit_allows_2pc_lock_bits)
{
	/* COMMIT PREPARED must carry the 2PC bit, but that bit alone is not a
	 * side effect; the prepared outcome is keyed by the parsed twophase_xid.
	 * Access-exclusive lock release is likewise not durable state after PITR. */
	UT_ASSERT(!cluster_remote_xact_commit_prepared_blocked(0, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
														   UT_XACT_XINFO_HAS_TWOPHASE,
														   UT_XACT_XINFO_HAS_AE_LOCKS));
	UT_ASSERT(!cluster_remote_xact_commit_prepared_blocked(
		0, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE | UT_XACT_XINFO_HAS_AE_LOCKS,
		UT_XACT_XINFO_HAS_TWOPHASE, UT_XACT_XINFO_HAS_AE_LOCKS));
	UT_ASSERT(cluster_remote_xact_commit_prepared_blocked(1, 0, 0, 0, UT_XACT_XINFO_HAS_TWOPHASE,
														  UT_XACT_XINFO_HAS_TWOPHASE,
														  UT_XACT_XINFO_HAS_AE_LOCKS));
}

UT_TEST(test_remote_xact_unrelated_xinfo_not_blocked)
{
	/* An xinfo bit OUTSIDE the {2PC, AE locks} mask does not block. */
	UT_ASSERT(!cluster_remote_xact_commit_blocked(0, 0, 0, 0, 0x01, UT_XACT_XINFO_HAS_TWOPHASE,
												  UT_XACT_XINFO_HAS_AE_LOCKS));
}


/* ============================================================
 * spec-6.14 D9: shared-catalog terminal predicate
 * ============================================================ */

UT_TEST(test_remote_xact_shared_catalog_terminal_predicate)
{
	/* Under cluster.shared_catalog the relfile / inval / stats side effects
	 * execute for real, so they are no longer inputs to the predicate at all:
	 * only subxacts and a caller-declared malformed xinfo bit block. */
	UT_ASSERT(
		!cluster_remote_xact_terminal_blocked_shared_catalog(0, 0, UT_XACT_XINFO_HAS_TWOPHASE));
	/* subxact outcomes still fail closed (no per-subxact durable wrap proof) */
	UT_ASSERT(
		cluster_remote_xact_terminal_blocked_shared_catalog(1, 0, UT_XACT_XINFO_HAS_TWOPHASE));
	/* 2PC bit on a PLAIN record arm (disallowed mask carries it) blocks */
	UT_ASSERT(cluster_remote_xact_terminal_blocked_shared_catalog(0, UT_XACT_XINFO_HAS_TWOPHASE,
																  UT_XACT_XINFO_HAS_TWOPHASE));
	/* the *_PREPARED arms pass disallowed=0: the expected 2PC bit is admitted */
	UT_ASSERT(
		!cluster_remote_xact_terminal_blocked_shared_catalog(0, UT_XACT_XINFO_HAS_TWOPHASE, 0));
	/* AE-lock bits are consumed (standby machinery), never blocking */
	UT_ASSERT(!cluster_remote_xact_terminal_blocked_shared_catalog(0, UT_XACT_XINFO_HAS_AE_LOCKS,
																   UT_XACT_XINFO_HAS_TWOPHASE));
}


/* ============================================================
 * Outcome enum contract
 * ============================================================ */

UT_TEST(test_remote_xact_indoubt_is_zero)
{
	/* INDOUBT must be 0 so a zeroed SLRU entry reads as fail-closed. */
	UT_ASSERT_EQ((int)CLUSTER_REMOTE_XACT_INDOUBT, 0);
	UT_ASSERT(CLUSTER_REMOTE_XACT_COMMITTED != CLUSTER_REMOTE_XACT_INDOUBT);
	UT_ASSERT(CLUSTER_REMOTE_XACT_ABORTED != CLUSTER_REMOTE_XACT_INDOUBT);
}


/* ============================================================
 * R13 online-blocked elevel boundary (spec-4.11 3b-2)
 * ============================================================ */

UT_TEST(test_remote_xact_blocked_elevel_cold_is_fatal)
{
	/* Cold merged replay (online=false) keeps the pre-spec-4.11 FATAL: a clean
	 * re-merge on the next startup. */
	UT_ASSERT_EQ(cluster_remote_xact_blocked_elevel(false), FATAL);
}

UT_TEST(test_remote_xact_blocked_elevel_online_is_catchable)
{
	/* Online thread recovery (online=true) raises a CATCHABLE ERROR so the R13
	 * harness can demote it to BLOCKED and the survivor keeps running.  The
	 * producer (ERROR) must sit strictly below the consumer's rethrow boundary
	 * (cluster_thread_recovery_should_rethrow == elevel >= FATAL), or an online
	 * block could masquerade as a crash. */
	UT_ASSERT_EQ(cluster_remote_xact_blocked_elevel(true), ERROR);
	UT_ASSERT(cluster_remote_xact_blocked_elevel(true) < FATAL);
	UT_ASSERT(cluster_remote_xact_blocked_elevel(false) >= FATAL);
}


/* ============================================================
 * R14 materialization-writer admission (spec-4.11 3b-2)
 * ============================================================ */

UT_TEST(test_remote_xact_writer_allowed_startup)
{
	/* The startup process is always a legitimate writer (cold merged replay),
	 * with or without an online scope. */
	UT_ASSERT(cluster_remote_xact_writer_allowed(true, 0));
	UT_ASSERT(cluster_remote_xact_writer_allowed(true, 1));
}

UT_TEST(test_remote_xact_writer_allowed_online_scope)
{
	/* A non-startup process (the recovery-apply bgworker) is admitted ONLY
	 * inside an episode-fenced online-writer scope (depth > 0). */
	UT_ASSERT(cluster_remote_xact_writer_allowed(false, 1));
	UT_ASSERT(cluster_remote_xact_writer_allowed(false, 3)); /* re-entrant */
}

UT_TEST(test_remote_xact_writer_denied_outside_scope)
{
	/* Not startup AND no online scope -> illegal context: fail closed (the
	 * assert must trip rather than let a stray writer corrupt the store). */
	UT_ASSERT(!cluster_remote_xact_writer_allowed(false, 0));
}


int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_remote_xact_entry_width);
	UT_RUN(test_remote_xact_origin_partition_disjoint);
	UT_RUN(test_remote_xact_origin_no_cross_partition_overlap);
	UT_RUN(test_remote_xact_pending_binding_is_byte_exact_and_fail_closed);
	UT_RUN(test_remote_xact_terminal_codec_carries_commit_ts_and_wrap);
	UT_RUN(test_remote_xact_prepare_transition_is_idempotent_and_conflict_closed);
	UT_RUN(test_remote_xact_prepared_terminal_requires_pending_or_exact_result);
	UT_RUN(test_remote_xact_projection_reset_is_idempotent_invalidation);
	UT_RUN(test_remote_xact_projection_reset_range_is_bounded);
	UT_RUN(test_remote_xact_pure_outcome_allowed);
	UT_RUN(test_remote_xact_side_effects_blocked);
	UT_RUN(test_remote_xact_prepared_commit_allows_2pc_lock_bits);
	UT_RUN(test_remote_xact_unrelated_xinfo_not_blocked);
	UT_RUN(test_remote_xact_shared_catalog_terminal_predicate);
	UT_RUN(test_remote_xact_indoubt_is_zero);

	UT_RUN(test_remote_xact_blocked_elevel_cold_is_fatal);
	UT_RUN(test_remote_xact_blocked_elevel_online_is_catchable);
	UT_RUN(test_remote_xact_writer_allowed_startup);
	UT_RUN(test_remote_xact_writer_allowed_online_scope);
	UT_RUN(test_remote_xact_writer_denied_outside_scope);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
