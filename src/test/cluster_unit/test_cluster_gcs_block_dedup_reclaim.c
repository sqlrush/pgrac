/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_dedup_reclaim.c
 *	  Compile-time + pure-logic invariants for the spec-7.2a GCS block dedup
 *	  capacity + eager-GC hardening (review r1).
 *
 *	  Covers the two pure reclaim-safety decision primitives that carry the
 *	  Rule 8.A correctness contract:
 *	    - GcsBlockReplyStatusIsReclaimIdempotent()  in-window reclaim whitelist
 *	    - GcsBlockDedupEntryIsReclaimSafe()          per-entry reclaim verdict
 *
 *	  Header-only — no linking of cluster_gcs_block_dedup.o.  The behavioral
 *	  reclaim path (cap-full -> reclaim -> register, evict_count accounting,
 *	  retransmit-dedup correctness under drop-reply injection) lives in
 *	  cluster_tap t/366_gcs_block_dedup_capacity_2node.pl, which exercises a real
 *	  2-node ClusterPair.
 *
 *	  Tests in this binary:
 *	    R1  whitelist EMPTY: every status 0..19 is in-window UNSAFE (8.A)
 *	    R2  payload GRANTED never in-window reclaimable
 *	    R3  storage-fallback never in-window (:3305 N->S / :4089 S->X_UPGRADE)
 *	    R4  DENIED_PENDING_X never in-window (:4071 broadcast_invalidate)
 *	    R5  DENIED_LOST_WRITE never in-window (:4393 counter side-effect)
 *	    R6  READ_IMAGE_FROM_XHOLDER never in-window (payload)
 *	    R7  *_FROM_HOLDER forward markers never in-window
 *	    R8  in-flight entry (completed_at_ts == 0) never reclaim-safe
 *	    R9  completed entry aged past the out-of-window (2x) is safe for
 *	        EVERY status, including payload GRANTED (§3.1 theorem)
 *	    R10 completed payload GRANTED still in-window is NOT reclaim-safe
 *	    R11 out-of-window boundary is strict (age == window -> in-window path)
 *	    R12 in-window verdict tracks the idempotent whitelist (empty -> unsafe)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_dedup_reclaim.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.2a-gcs-block-dedup-capacity-gc.md (APPROVED 2026-07-09)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Assert hook — this header-only test pulls pg_strfromd from libpgport_srv.a
 * (via the snprintf family), which references ExceptionalCondition under
 * cassert.  Provide the standard local stub (matching the sibling unit tests).
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * Build a completed dedup entry with the given reply status whose reply was
 * produced `age_us` microseconds before `now`.
 */
static GcsBlockDedupEntry
make_completed_entry(GcsBlockReplyStatus status, TimestampTz now, int64 age_us)
{
	GcsBlockDedupEntry e;

	memset(&e, 0, sizeof(e));
	e.status = (uint8)status;
	e.registered_at_ts = now - age_us - 1;
	e.completed_at_ts = now - age_us;
	return e;
}

/* -------- in-window reclaim whitelist (8.A) -------- */

UT_TEST(test_r1_whitelist_empty_all_statuses_in_window_unsafe)
{
	int s;

	/* Every reply status in the enum range must be fail-closed UNSAFE for
	 * in-window reclaim until its install sites are proven idempotent. */
	for (s = GCS_BLOCK_REPLY_GRANTED; s <= GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT; s++)
		UT_ASSERT_EQ(false, GcsBlockReplyStatusIsReclaimIdempotent((GcsBlockReplyStatus)s));
}

UT_TEST(test_r2_payload_granted_never_in_window)
{
	UT_ASSERT_EQ(false, GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_GRANTED));
}

UT_TEST(test_r3_storage_fallback_never_in_window)
{
	/* :3305 applies N->S, :4089 applies S->X_UPGRADE before returning
	 * storage-fallback — not integrally idempotent (spec-7.2a §3.2). */
	UT_ASSERT_EQ(false,
				 GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK));
}

UT_TEST(test_r4_pending_x_never_in_window)
{
	/* :4071 broadcast_invalidate + clear_pending_x before deny. */
	UT_ASSERT_EQ(false, GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_DENIED_PENDING_X));
}

UT_TEST(test_r5_lost_write_never_in_window)
{
	/* :4393 lost_write_detected_count++ side effect. */
	UT_ASSERT_EQ(false, GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_DENIED_LOST_WRITE));
}

UT_TEST(test_r6_read_image_never_in_window)
{
	UT_ASSERT_EQ(false,
				 GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER));
}

UT_TEST(test_r7_forward_markers_never_in_window)
{
	UT_ASSERT_EQ(false,
				 GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER));
	UT_ASSERT_EQ(false,
				 GcsBlockReplyStatusIsReclaimIdempotent(GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER));
}

/* -------- per-entry reclaim verdict -------- */

UT_TEST(test_r8_in_flight_entry_never_reclaim_safe)
{
	GcsBlockDedupEntry e;
	TimestampTz now = 1000000;

	memset(&e, 0, sizeof(e));
	e.status = (uint8)GCS_BLOCK_REPLY_GRANTED;
	e.registered_at_ts = now - 5000;
	e.completed_at_ts = 0; /* in-flight: original request still processing */

	UT_ASSERT_EQ(false, GcsBlockDedupEntryIsReclaimSafe(&e, now, 1000));
}

UT_TEST(test_r9_aged_out_safe_for_every_status)
{
	TimestampTz now = 1000000;
	int64 window = 1000;
	GcsBlockDedupEntry granted = make_completed_entry(GCS_BLOCK_REPLY_GRANTED, now, 2000);
	GcsBlockDedupEntry sfb
		= make_completed_entry(GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK, now, 2000);
	GcsBlockDedupEntry lost = make_completed_entry(GCS_BLOCK_REPLY_DENIED_LOST_WRITE, now, 2000);

	/* age 2000us > window 1000us: out-of-window, safe for ALL (2x theorem). */
	UT_ASSERT_EQ(true, GcsBlockDedupEntryIsReclaimSafe(&granted, now, window));
	UT_ASSERT_EQ(true, GcsBlockDedupEntryIsReclaimSafe(&sfb, now, window));
	UT_ASSERT_EQ(true, GcsBlockDedupEntryIsReclaimSafe(&lost, now, window));
}

UT_TEST(test_r10_in_window_payload_granted_not_safe)
{
	TimestampTz now = 1000000;
	int64 window = 5000;
	GcsBlockDedupEntry granted = make_completed_entry(GCS_BLOCK_REPLY_GRANTED, now, 500);

	/* age 500us <= window 5000us: in-window; GRANTED is payload -> unsafe. */
	UT_ASSERT_EQ(false, GcsBlockDedupEntryIsReclaimSafe(&granted, now, window));
}

UT_TEST(test_r11_out_of_window_boundary_is_strict)
{
	TimestampTz now = 1000000;
	int64 window = 1000;
	GcsBlockDedupEntry at_boundary = make_completed_entry(GCS_BLOCK_REPLY_GRANTED, now, 1000);

	/* age == window: NOT strictly greater -> still in-window -> GRANTED unsafe. */
	UT_ASSERT_EQ(false, GcsBlockDedupEntryIsReclaimSafe(&at_boundary, now, window));
}

UT_TEST(test_r12_in_window_verdict_tracks_whitelist)
{
	TimestampTz now = 1000000;
	int64 window = 5000;
	int s;

	/* In-window (age 100 << window): verdict must equal the whitelist for
	 * every status.  Whitelist is empty -> all in-window verdicts false. */
	for (s = GCS_BLOCK_REPLY_GRANTED; s <= GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT; s++) {
		GcsBlockDedupEntry e = make_completed_entry((GcsBlockReplyStatus)s, now, 100);

		UT_ASSERT_EQ(GcsBlockReplyStatusIsReclaimIdempotent((GcsBlockReplyStatus)s),
					 GcsBlockDedupEntryIsReclaimSafe(&e, now, window));
	}
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_r1_whitelist_empty_all_statuses_in_window_unsafe);
	UT_RUN(test_r2_payload_granted_never_in_window);
	UT_RUN(test_r3_storage_fallback_never_in_window);
	UT_RUN(test_r4_pending_x_never_in_window);
	UT_RUN(test_r5_lost_write_never_in_window);
	UT_RUN(test_r6_read_image_never_in_window);
	UT_RUN(test_r7_forward_markers_never_in_window);
	UT_RUN(test_r8_in_flight_entry_never_reclaim_safe);
	UT_RUN(test_r9_aged_out_safe_for_every_status);
	UT_RUN(test_r10_in_window_payload_granted_not_safe);
	UT_RUN(test_r11_out_of_window_boundary_is_strict);
	UT_RUN(test_r12_in_window_verdict_tracks_whitelist);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
