/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retained_cut_records_cases.h
 *	  Cases of test_cluster_wal_retained_cut_records.c.  Self is thread 1
 *	  (history [0x1000, 0x3000), tail 0x4000); the peer is thread 2
 *	  (history [0x5000, 0x6000), tail 0x7000).
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wal_retained_cut_records_cases.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_CASES_H
#define TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_CASES_H

#define REC_SELF_REDO 0x3000
#define REC_PEER_REDO 0x6000

static void
rec_two_writers(uint32 *self, uint32 *peer)
{
	records_reset();
	*self = add_writer(1, 0x1000, REC_SELF_REDO, 0x4000);
	*peer = add_writer(2, 0x5000, REC_PEER_REDO, 0x7000);
}

static void
expect_cut(uint32 source, XLogRecPtr lower, ClusterWalRetainedPinV1 pin, uint32 side)
{
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	UT_ASSERT_EQ(compute_for(source, &cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cut.lower, lower);
	UT_ASSERT_EQ(cut.pin, pin);
	UT_ASSERT_EQ(cut.side_classes, side);
}

/* A COMMIT whose typed tombstones the SIDE decoder accepts: the DROP ends an
 * incarnation, so in history it holds its source (CR20).  The same COMMIT
 * still an obligation, or a plain COMMIT in history, holds nothing. */
UT_TEST(test_records_commit_tombstone_pins_its_source)
{
	static const RelFileNumber drops[] = { 16384, 16390 };

	for (int variant = 0; variant < 3; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		if (variant == 0)
			rec_commit(self, 0x1800, 0x1900, 801, drops, 2, true, 0);
		else if (variant == 1)
			rec_commit(self, 0x3200, 0x3300, 801, drops, 2, true, 0);
		else
			rec_commit(self, 0x1800, 0x1900, 801, NULL, 0, false, 0);
		expect_cut(
			self, variant == 0 ? 0x1800 : REC_SELF_REDO,
			variant == 0 ? CLUSTER_WAL_RETAINED_PIN_STRUCTURE : CLUSTER_WAL_RETAINED_PIN_NONE,
			variant == 1 ? RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL : 0);
		if (ut_current_failed)
			printf("# tombstone variant %d\n", variant);
	}
}

/* The SIDE decoder does not own a COMMIT with invalidation messages yet.
 * The census classifies it instead of giving up: its SIDE classes follow
 * the class rule; with relations to drop but no typed tombstones it holds
 * its source like a DROP. */
UT_TEST(test_records_commit_with_invalidations_is_classified)
{
	static const RelFileNumber drops[] = { 16384 };

	for (int variant = 0; variant < 5; variant++) {
		uint32 self, peer;
		XLogRecPtr lower = REC_SELF_REDO;
		ClusterWalRetainedPinV1 pin = CLUSTER_WAL_RETAINED_PIN_NONE;
		uint32 side = 0;

		rec_two_writers(&self, &peer);
		if (variant == 3) { /* a standalone invalidation record */
			RealRecord *r
				= record_new(self, RM_XACT_ID, XLOG_XACT_INVALIDATIONS, 801, 0x1800, 0x1900);
			int nmsgs = 0;

			record_append(r, &nmsgs, sizeof(nmsgs));
		} else if (variant == 4) { /* a subtransaction XID assignment */
			RealRecord *r = record_new(self, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, 801, 0x1800, 0x1900);
			xl_xact_assignment assignment = { 801, 1 };
			TransactionId sub = 802;

			record_append(r, &assignment, MinSizeOfXactAssignment);
			record_append(r, &sub, sizeof(sub));
		} else
			rec_commit(self, 0x1800, 0x1900, 801, variant == 2 ? drops : NULL, variant == 2 ? 1 : 0,
					   false, 3);
		if (variant == 1) { /* a peer obligation of the same class */
			rec_commit(peer, 0x6100, 0x6200, 902, NULL, 0, false, 0);
			lower = 0x1800;
			pin = CLUSTER_WAL_RETAINED_PIN_SIDE;
			side = RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL;
		} else if (variant == 2) {
			lower = 0x1800;
			pin = CLUSTER_WAL_RETAINED_PIN_STRUCTURE;
		}
		expect_cut(self, lower, pin, side);
		if (ut_current_failed)
			printf("# invalidation variant %d\n", variant);
	}
}

/* ABORT that drops the relations its transaction created ends their
 * incarnation: held in history.  A plain ABORT holds nothing. */
UT_TEST(test_records_abort_with_relations_pins_its_source)
{
	static const RelFileNumber rels[] = { 16400 };

	for (int variant = 0; variant < 2; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		rec_abort(self, 0x2000, 0x2100, 803, rels, variant == 0 ? 1 : 0);
		expect_cut(
			self, variant == 0 ? 0x2000 : REC_SELF_REDO,
			variant == 0 ? CLUSTER_WAL_RETAINED_PIN_STRUCTURE : CLUSTER_WAL_RETAINED_PIN_NONE, 0);
		if (ut_current_failed)
			printf("# abort variant %d\n", variant);
	}
}

/* Native SMGR CREATE (every CREATE TABLE) needs no older record; a native
 * SMGR TRUNCATE ends an incarnation. */
UT_TEST(test_records_native_smgr_create_and_truncate)
{
	for (int variant = 0; variant < 2; variant++) {
		uint32 self, peer;

		rec_two_writers(&self, &peer);
		rec_smgr(self, 0x1400, 0x1440, false, 16500);
		if (variant == 1)
			rec_smgr(self, 0x2400, 0x2440, true, 16500);
		expect_cut(
			self, variant == 0 ? REC_SELF_REDO : 0x2400,
			variant == 0 ? CLUSTER_WAL_RETAINED_PIN_NONE : CLUSTER_WAL_RETAINED_PIN_STRUCTURE, 0);
		if (ut_current_failed)
			printf("# smgr variant %d\n", variant);
	}
}

/* A record of another node's source is classified the same way and bounds
 * that source only. */
UT_TEST(test_records_foreign_source_pins_only_itself)
{
	static const RelFileNumber rels[] = { 16600 };
	uint32 self, peer;

	rec_two_writers(&self, &peer);
	rec_smgr(peer, 0x5100, 0x5140, false, 16600);
	rec_abort(peer, 0x5800, 0x5900, 905, rels, 1);
	rec_commit(peer, 0x5a00, 0x5b00, 906, NULL, 0, false, 2);
	expect_cut(self, REC_SELF_REDO, CLUSTER_WAL_RETAINED_PIN_NONE, 0);
	expect_cut(peer, 0x5800, CLUSTER_WAL_RETAINED_PIN_STRUCTURE, 0);
}

/* The fallback is narrow: a typed SPACE record the decoder refuses, a
 * transaction end too short to parse, one with a block reference, or one
 * the decoder rejects for its identity, still refuses the whole census. */
UT_TEST(test_records_unknown_native_record_still_refuses)
{
	for (int variant = 0; variant < 5; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		RealRecord *r;
		uint8 garbage[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };

		rec_two_writers(&self, &peer);
		if (variant == 0) {
			r = record_new(self, RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, InvalidTransactionId, 0x1800,
						   0x1900);
			record_append(r, garbage, sizeof(garbage));
		} else if (variant == 1) {
			r = record_new(self, RM_XACT_ID, XLOG_XACT_ABORT, 807, 0x1800, 0x1900);
			record_append(r, garbage, 4);
		} else if (variant == 2) {
			r = rec_commit(self, 0x1800, 0x1900, 808, NULL, 0, false, 2);
			r->u.decoded.max_block_id = 0;
			r->u.decoded.blocks[0].in_use = true;
		} else if (variant == 3) {
			r = record_new(self, RM_XACT_ID, XLOG_XACT_COMMIT, 810, 0x1800, 0x1900);
			record_append(r, garbage, 4); /* shorter than xl_xact_commit */
		} else {
			static const RelFileNumber drops[] = { 16384 };

			/* Its tombstone names another database incarnation. */
			rec_drop_dbinc = REC_DBINC + 1;
			r = rec_commit(self, 0x1800, 0x1900, 809, drops, 1, true, 0);
		}
		UT_ASSERT(compute_for(self, &cut, &detail) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cut.lower, 0);
		if (ut_current_failed)
			printf("# refusal variant %d\n", variant);
	}
}

/* A integration reproducer (A hold 16c44655e9), extended: the typed decoder
 * rejects a transaction end whose sections do not lie within its data, and
 * the native fallback must reject it too -- a damaged record refuses the
 * census instead of becoming retention evidence.  Truncated xinfo, count
 * arrays longer than the data, trailing bytes and a section an ABORT cannot
 * carry, for ABORT and COMMIT alike. */
UT_TEST(test_records_short_abort_does_not_advance_retention)
{
	for (int variant = 0; variant < 7; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		bool commit = variant >= 5;
		RealRecord *record;
		xl_xact_abort abort_record = { 0 };
		xl_xact_commit commit_record = { 0 };
		xl_xact_xinfo xinfo = { 0 };
		RelFileLocator locator = rec_locator(16400);
		TransactionId sub = 804;
		int count = 1000;
		uint8 junk[4] = { 9, 9, 9, 9 };

		rec_two_writers(&self, &peer);
		record = record_new(self, RM_XACT_ID,
							(commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT) | XLOG_XACT_HAS_INFO, 803,
							0x2000, 0x2100);
		if (commit)
			record_append(record, &commit_record, MinSizeOfXactCommit);
		else
			record_append(record, &abort_record, MinSizeOfXactAbort);
		if (variant == 0) /* A's case: no xinfo at all */
			;
		else if (variant == 1 || variant == 5) /* xinfo cut short */
			record_append(record, junk, 2);
		else if (variant == 2) { /* relation count beyond the data */
			xinfo.xinfo = XACT_XINFO_HAS_RELFILELOCATORS;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &count, sizeof(count));
			record_append(record, &locator, sizeof(locator));
		} else if (variant == 3) { /* bytes after the last section */
			xinfo.xinfo = XACT_XINFO_HAS_RELFILELOCATORS;
			count = 1;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &count, sizeof(count));
			record_append(record, &locator, sizeof(locator));
			record_append(record, junk, sizeof(junk));
		} else if (variant == 4) { /* a section only a COMMIT carries */
			xl_xact_tt_commit tt = { 0 };

			xinfo.xinfo = XACT_XINFO_HAS_TT_COMMIT;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &tt, sizeof(tt));
		} else { /* variant 6: subtransaction count beyond the data */
			xinfo.xinfo = XACT_XINFO_HAS_SUBXACTS;
			record_append(record, &xinfo, sizeof(xinfo));
			record_append(record, &count, sizeof(count));
			record_append(record, &sub, sizeof(sub));
		}
		UT_ASSERT(compute_for(self, &cut, &detail) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cut.lower, InvalidXLogRecPtr);
		if (variant == 0) /* reaches the native fallback, which refuses it */
			UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE);
		if (ut_current_failed)
			printf("# damaged transaction end variant %d\n", variant);
	}
}

#endif /* TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_CASES_H */
