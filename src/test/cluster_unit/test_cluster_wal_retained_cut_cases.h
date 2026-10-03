/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retained_cut_cases.h
 *	  Cases of test_cluster_wal_retained_cut.c, included once after its
 *	  fixtures.  Self is thread 1 (OPEN, native redo 0x3000); peers are
 *	  threads 2 and 3.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wal_retained_cut_cases.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_WAL_RETAINED_CUT_CASES_H
#define TEST_CLUSTER_WAL_RETAINED_CUT_CASES_H

#define SELF_LOWER 0x1000
#define SELF_REDO 0x3000
#define SELF_TAIL 0x4000

/* Self OPEN with history [lower, redo); a peer OPEN completed at 0x6000. */
static void
two_writers(uint32 *self, uint32 *peer)
{
	reset();
	*self
		= add_item(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, SELF_LOWER, SELF_REDO, SELF_TAIL, true);
	*peer = add_item(2, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, 0x5000, 0x6000, 0x7000, true);
}

UT_TEST(test_retained_cut_moves_to_native_redo_without_obligations)
{
	uint32 self, peer;
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	two_writers(&self, &peer);
	add_record(self, 0x1000, 0x1100, 100, 1, 2);
	add_record(self, 0x2000, 0x2100, 101, 2, 3);
	add_record(peer, 0x5100, 0x5200, 102, 3, 4); /* peer history */
	add_record(self, 0x2f00, 0x3000, 104, 5, 6); /* ends exactly at the redo */
	add_record(self, 0x3000, 0x3100, 103, 4, 5); /* self obligation, own page */
	UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cut.lower, SELF_REDO);
	UT_ASSERT_EQ(cut.old_lower, SELF_LOWER);
	UT_ASSERT_EQ(cut.native_redo, SELF_REDO);
	UT_ASSERT_EQ(cut.pin, CLUSTER_WAL_RETAINED_PIN_NONE);
	UT_ASSERT_EQ(cut.records, 5);
	UT_ASSERT_EQ(cut.history_edges, 4);
	UT_ASSERT_EQ(cut.retained_edges, 0);
	UT_ASSERT_EQ(cut.root_token.file_txn_seq, 77);
	UT_ASSERT_EQ(spool_creates, 0);
}

/* PU-D-7: on a page with an obligation, only history edges after the
 * obligation's before-version are kept (they prove the disk version
 * descends from it); its predecessors and other pages are released. */
UT_TEST(test_retained_cut_peer_obligation_keeps_successors_on_its_page)
{
	uint32 self, peer;
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	two_writers(&self, &peer);
	add_record(self, 0x1000, 0x1100, 100, 1, 2);
	add_record(self, 0x1400, 0x1500, 200, 1, 2); /* predecessor: released */
	add_record(self, 0x1800, 0x1900, 200, 4, 5); /* successor of the obligation */
	add_record(self, 0x2000, 0x2100, 200, 5, 6);
	add_record(peer, 0x6000, 0x6100, 200, 3, 4); /* peer obligation 3 -> 4 */
	UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cut.lower, 0x1800);
	UT_ASSERT_EQ(cut.pin, CLUSTER_WAL_RETAINED_PIN_PAGE);
	UT_ASSERT_EQ(cut.retained_edges, 2);
}

/* F-D-26 closed by PU-D-7: a page that keeps changing no longer holds the
 * lower; only an edge newer than the earliest obligation's base would. */
UT_TEST(test_retained_cut_hot_page_releases_predecessors)
{
	for (int variant = 0; variant < 2; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;

		two_writers(&self, &peer);
		add_record(self, 0x1000, 0x1100, 300, 1, 2);
		add_record(self, 0x2000, 0x2100, 300, 2, 3);
		add_record(self, 0x3000, 0x3100, 300, 3, 4); /* own obligation, same page */
		if (variant == 1)							 /* an older peer obligation base */
			add_record(peer, 0x6000, 0x6100, 300, 1, 7);
		UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cut.lower, variant == 0 ? SELF_REDO : 0x1000);
		UT_ASSERT_EQ(cut.pin,
					 variant == 0 ? CLUSTER_WAL_RETAINED_PIN_NONE : CLUSTER_WAL_RETAINED_PIN_PAGE);
		if (ut_current_failed)
			printf("# hot variant %d\n", variant);
	}
}

/* An edge of another segment incarnation than every obligation on the block
 * is kept whatever its version; a chain start (no present before-version)
 * keeps every history edge of its incarnation. */
UT_TEST(test_retained_cut_other_incarnation_is_kept)
{
	for (int variant = 0; variant < 4; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		FixtureRecord *history, *obligation;

		two_writers(&self, &peer);
		add_record(self, 0x1000, 0x1100, 100, 1, 2);
		history = add_record(self, 0x1800, 0x1900, 400, 1, 2);
		obligation = add_record(peer, 0x6000, 0x6100, 400, 3, 4);
		if (variant == 1)
			history->inc = 2; /* the obligation is on incarnation 1 */
		else if (variant == 2) {
			obligation->before_inc = 2; /* the obligation spans incarnations */
			obligation->inc = 1;
			history->inc = 2;
		} else if (variant == 3)
			obligation->before_kind[0] = RF_PAGE_STATE_UNFORMATTED;
		UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cut.lower, variant == 0 ? SELF_REDO : 0x1800);
		if (ut_current_failed)
			printf("# incarnation variant %d\n", variant);
	}
}

/* Every edge must strictly advance its page in SCN total order (local part,
 * then node id); otherwise nothing proves which edges precede the base. */
UT_TEST(test_retained_cut_refuses_a_chain_that_does_not_advance)
{
	for (int variant = 0; variant < 4; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		FixtureRecord *r;
		bool accepted = variant >= 2;

		two_writers(&self, &peer);
		add_record(self, 0x1000, 0x1100, 100, 1, 2);
		r = add_record(variant % 2 == 0 ? self : peer, variant % 2 == 0 ? 0x1800 : 0x6000,
					   variant % 2 == 0 ? 0x1900 : 0x6100, 500, 5, 5);
		if (variant == 1)
			r->result_token = 4; /* backwards */
		else if (variant == 2) {
			/* A raw integer comparison would call this backwards. */
			r->before[0] = scn_encode(3, 10);
			r->result_token = scn_encode(1, 11);
		} else if (variant == 3) {
			r->before[0] = scn_encode(1, 10); /* same local, later node */
			r->result_token = scn_encode(2, 10);
		}
		UT_ASSERT_EQ(compute(&cut, &detail), accepted ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
													  : CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT_EQ(detail,
					 accepted ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION);
		if (ut_current_failed)
			printf("# order variant %d\n", variant);
	}
}

/* A keyless SIDE class with any obligation keeps the earliest history record
 * of that class; native control records never do. */
UT_TEST(test_retained_cut_keyless_side_classes)
{
	for (int variant = 0; variant < 3; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		uint32 owner = variant == 0	  ? RF_SIDE_CONTRIBUTION_UNDO_HEADER
					   : variant == 1 ? RF_SIDE_CONTRIBUTION_NATIVE_CONTROL
									  : RF_SIDE_CONTRIBUTION_MULTIXACT;

		two_writers(&self, &peer);
		add_record(self, 0x1000, 0x1100, 100, 1, 2);
		add_record(self, 0x1400, 0x1500, InvalidOid, 0, 2)->owners = owner;
		add_record(self, 0x2000, 0x2100, InvalidOid, 0, 3)->owners = owner;
		add_record(peer, 0x6000, 0x6100, InvalidOid, 0, 4)->owners = owner;
		UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cut.lower, variant == 1 ? SELF_REDO : 0x1400);
		UT_ASSERT_EQ(cut.pin,
					 variant == 1 ? CLUSTER_WAL_RETAINED_PIN_NONE : CLUSTER_WAL_RETAINED_PIN_SIDE);
		UT_ASSERT_EQ(cut.side_classes, variant == 1 ? 0 : owner);
		if (ut_current_failed)
			printf("# side variant %d\n", variant);
	}
}

/* A SPACE contribution is keyed by its block: a peer obligation on the
 * relation's reservation block keeps self's history reservation edge. */
UT_TEST(test_retained_cut_space_obligation_keeps_space_history)
{
	uint32 self, peer;
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	two_writers(&self, &peer);
	add_record(self, 0x1000, 0x1100, 100, 1, 2);
	add_record(self, 0x1c00, 0x1d00, InvalidOid, 0, 3)->space_rel = 500;
	add_record(self, 0x2000, 0x2100, InvalidOid, 0, 3)->space_rel = 501;
	add_record(peer, 0x6000, 0x6100, InvalidOid, 0, 4)->space_rel = 500;
	UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cut.lower, 0x1c00);
	UT_ASSERT_EQ(cut.pin, CLUSTER_WAL_RETAINED_PIN_PAGE);
}

/* Until structural PI retirement is durable (CR20), a history record that
 * ends a relation incarnation (SPACE block 0: TRUNCATE, a DROP tombstone, a
 * COMMIT's drops) holds the lower at itself with no obligation at all; a
 * CREATE or a reservation does not, nor a structure change that is still
 * an obligation.  The earliest reason wins; a tie reports STRUCTURE. */
UT_TEST(test_retained_cut_structure_change_pins_history)
{
	for (int variant = 0; variant < 7; variant++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		FixtureRecord *r;
		XLogRecPtr expected = 0x1c00;
		ClusterWalRetainedPinV1 pin = CLUSTER_WAL_RETAINED_PIN_STRUCTURE;

		two_writers(&self, &peer);
		add_record(self, 0x1000, 0x1100, 100, 1, 2);
		r = add_record(self, 0x1c00, 0x1d00, InvalidOid, 0, 3);
		r->space_rel = 900;
		r->space_mask = 3;
		if (variant == 0) { /* a later structure change does not move it */
			r = add_record(self, 0x2400, 0x2500, InvalidOid, 0, 4);
			r->space_rel = 902;
			r->space_mask = 3;
		} else if (variant == 1) /* the drops of a COMMIT */
			r->owners = RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL;
		else if (variant == 2) { /* a reservation is not a structure change */
			r->space_mask = 2;
			expected = SELF_REDO;
			pin = CLUSTER_WAL_RETAINED_PIN_NONE;
		} else if (variant == 3) { /* still an obligation: released later */
			r->read = SELF_REDO;   /* starts exactly at the native redo */
			r->end = SELF_REDO + 0x100;
			expected = SELF_REDO;
			pin = CLUSTER_WAL_RETAINED_PIN_NONE;
		} else if (variant == 4) {	  /* an earlier page pin is the bound */
			records[0].before[0] = 5; /* a successor of the peer obligation */
			records[0].result_token = 6;
			add_record(peer, 0x6000, 0x6100, 100, 4, 5);
			expected = 0x1000;
			pin = CLUSTER_WAL_RETAINED_PIN_PAGE;
		} else if (variant == 6) { /* a CREATE starts an incarnation */
			r->space_create = true;
			expected = SELF_REDO;
			pin = CLUSTER_WAL_RETAINED_PIN_NONE;
		} else if (variant == 5) /* same record also pinned by its page */
			add_record(peer, 0x6000, 0x6100, InvalidOid, 0, 5)->space_rel = 900;
		UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cut.lower, expected);
		UT_ASSERT_EQ(cut.pin, pin);
		if (ut_current_failed)
			printf("# structure variant %d\n", variant);
	}
}

/* RECOVERY_COMPLETE and CLOSED inputs are history to their tails and never
 * pin; RECOVERY_REQUIRED completes at its native redo; a terminal input is
 * all obligations and pins the pages it touched. */
UT_TEST(test_retained_cut_completion_by_lifecycle)
{
	for (int variant = 0; variant < 4; variant++) {
		uint32 self, other;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		static const uint8 lifecycle[] = { CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE,
										   CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED,
										   CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED,
										   CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN };

		reset();
		self = add_item(1, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN, SELF_LOWER, SELF_REDO, SELF_TAIL,
						true);
		other = add_item(3, lifecycle[variant], 0x5000, 0x6000, 0x7000, false);
		if (variant == 3) {
			items[other].kind = CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL;
			items[other].first_segment = 0x5000 - SizeOfXLogLongPHD;
		}
		add_record(self, 0x1000, 0x1100, 100, 1, 2);
		add_record(self, 0x2000, 0x2100, 601, 5, 6);  /* after the other's 601 edge */
		add_record(other, 0x5000, 0x5100, 600, 3, 4); /* before the other's redo */
		add_record(other, 0x6800, 0x6900, 601, 4, 5); /* after its redo, before its tail */
		UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cut.lower, variant >= 2 ? 0x2000 : SELF_REDO);
		if (ut_current_failed)
			printf("# lifecycle variant %d\n", variant);
	}
}

/* A bound below the published lower keeps the published lower. */
UT_TEST(test_retained_cut_never_moves_back)
{
	uint32 self, peer;
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	two_writers(&self, &peer);
	items[self].checkpoint.checkpoint_lower_lsn = 0x1800;
	add_record(self, 0x1000, 0x1100, 700, 3, 4); /* older than the published lower */
	add_record(peer, 0x6000, 0x6100, 700, 2, 3);
	UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cut.lower, 0x1800);
	UT_ASSERT_EQ(cut.old_lower, 0x1800);
}

UT_TEST(test_retained_cut_refusals_zero_the_output)
{
	for (int fault = 0; fault < 9; fault++) {
		uint32 self, peer;
		ClusterWalRetainedCutV1 cut;
		RfPageProofDetailV1 detail;
		ClusterControlRootResult expected = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		RfPageProofDetailV1 expected_detail = RF_PAGE_PROOF_DETAIL_OK;

		two_writers(&self, &peer);
		add_record(self, 0x1000, 0x1100, 100, 1, 2);
		add_record(peer, 0x6000, 0x6100, 100, 2, 3);
		if (fault == 0) {
			add_record(self, 0x2f00, 0x3100, 800, 3, 4); /* crosses the native redo */
			expected_detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		} else if (fault == 1) {
			items[self].current = false; /* no current writer generation */
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		} else if (fault == 2) {
			items[peer].source = items[self].source; /* two current writers */
			items[peer].checkpoint.identity = items[self].checkpoint.identity;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		} else if (fault == 3)
			expected = census_result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		else if (fault == 4)
			expected = revalidate_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else if (fault == 5)
			expected = begin_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else if (fault == 6) {
			items[peer].checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		} else if (fault == 7)
			expected = token_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else {
			records[1].source = 3; /* a record of an unselected source */
			items[3] = items[peer];
			items[3].source.claim.identity.origin_thread_id = 4;
			expected_detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		}
		memset(&cut, 0xa5, sizeof(cut));
		UT_ASSERT_EQ(compute(&cut, &detail), expected);
		UT_ASSERT_EQ(detail, expected_detail);
		UT_ASSERT(v_zero(&cut, sizeof(cut)));
		if (ut_current_failed)
			printf("# refusal fault %d\n", fault);
	}
}

/* More history edges than one batch go through the temporary file; the
 * needed edge is found wherever it was spooled. */
UT_TEST(test_retained_cut_spills_history_to_a_temp_file)
{
	uint32 self, peer;
	ClusterWalRetainedCutV1 cut;
	RfPageProofDetailV1 detail;

	two_writers(&self, &peer);
	for (uint32 i = 0; i < 600; i++)
		add_record(self, 0x1000 + i * 8, 0x1000 + i * 8 + 8, 1000 + i, 1, 2);
	add_record(peer, 0x6000, 0x6100, 1000 + 450, 1, 3); /* base older than edge 450 */
	UT_ASSERT_EQ(compute(&cut, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(spool_creates, 1);
	UT_ASSERT_EQ(cut.history_edges, 600);
	UT_ASSERT_EQ(cut.lower, 0x1000 + 450 * 8);
	UT_ASSERT(cut.retained_edges >= 1);
}

/* Each bucket keeps the earliest base and the incarnation range of every
 * key hashed to it: a key's answer is never less conservative than its own
 * obligations, and a key with no obligation in either bucket keeps nothing. */
UT_TEST(test_retained_sketch_is_conservative)
{
	RetainedBucket *sketch = palloc(sizeof(RetainedBucket) * RETAINED_SKETCH_BUCKETS);
	uint64 a = UINT64CONST(0x0000000500000003), b = UINT64CONST(0x0000000700000003);
	uint64 twin = a | (UINT64CONST(1) << 20) | (UINT64CONST(1) << 52); /* both buckets of a */
	uint64 c = UINT64CONST(0x0000000900000011);
	uint8 one[16] = { 1 }, two[16] = { 2 };

	retained_sketch_init(sketch);
	UT_ASSERT(!retained_edge_needed(sketch, a, 100, one));
	retained_sketch_add(sketch, a, 50, one, one);
	retained_sketch_add(sketch, a, 40, one, one);
	UT_ASSERT(!retained_edge_needed(sketch, a, 40, one)); /* produced the base */
	UT_ASSERT(retained_edge_needed(sketch, a, 41, one));
	UT_ASSERT(retained_edge_needed(sketch, a, 1, two));	   /* another incarnation */
	UT_ASSERT(!retained_edge_needed(sketch, b, 100, one)); /* one bucket is empty */
	UT_ASSERT(!retained_edge_needed(sketch, c, 100, one));
	retained_sketch_add(sketch, b, 10, one, one);		  /* shares only a's first bucket */
	UT_ASSERT(!retained_edge_needed(sketch, a, 20, one)); /* a's own bucket bounds it */
	retained_sketch_add(sketch, twin, 10, one, one);
	UT_ASSERT(retained_edge_needed(sketch, a, 20, one)); /* the floor only drops */
	retained_sketch_add(sketch, twin, 60, two, two);
	UT_ASSERT(retained_edge_needed(sketch, a, 1, one)); /* mixed incarnations */
	pfree(sketch);
}

/* The driver publishes only an advance, with the census token, native redo
 * and bound; refusals keep the lower and log once per reason. */
UT_TEST(test_retained_cut_driver_publishes_only_an_advance)
{
	uint32 self, peer;

	two_writers(&self, &peer);
	add_record(self, 0x1000, 0x1100, 100, 1, 2);
	cluster_wal_retained_cut_after_checkpoint_v1();
	UT_ASSERT_EQ(publish_calls, 1);
	UT_ASSERT_EQ(publish_token.file_txn_seq, 77);
	UT_ASSERT_EQ(publish_native, SELF_REDO);
	UT_ASSERT_EQ(publish_lower, SELF_REDO);
	UT_ASSERT_EQ(log_count, 0);
	/* Held at the published lower: no publication, one LOG. */
	add_record(peer, 0x6000, 0x6100, 100, 1, 3); /* keeps the self edge 1 -> 2 */
	cluster_wal_retained_cut_after_checkpoint_v1();
	cluster_wal_retained_cut_after_checkpoint_v1();
	UT_ASSERT_EQ(publish_calls, 1);
	UT_ASSERT_EQ(log_count, 1);
	/* A refused publication is logged once. */
	nrecords = 1;
	publish_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	cluster_wal_retained_cut_after_checkpoint_v1();
	cluster_wal_retained_cut_after_checkpoint_v1();
	UT_ASSERT_EQ(publish_calls, 3);
	UT_ASSERT_EQ(log_count, 2);
	/* A retained structure change holds the lower: one LOG for it. */
	publish_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	{
		FixtureRecord *r = add_record(self, 0x1000, 0x1080, InvalidOid, 0, 9);

		r->space_rel = 901;
		r->space_mask = 3;
	}
	cluster_wal_retained_cut_after_checkpoint_v1();
	cluster_wal_retained_cut_after_checkpoint_v1();
	UT_ASSERT_EQ(publish_calls, 3);
	UT_ASSERT_EQ(log_count, 3);
	UT_ASSERT_EQ(retained_structure_pins, 2);
	nrecords = 1;
	/* Not the checkpointer, or shutting down: nothing at all. */
	MyAuxProcType = NotAnAuxProcess;
	cluster_wal_retained_cut_after_checkpoint_v1();
	MyAuxProcType = CheckpointerProcess;
	ShutdownRequestPending = true;
	cluster_wal_retained_cut_after_checkpoint_v1();
	ShutdownRequestPending = false;
	UT_ASSERT_EQ(publish_calls, 3);
	UT_ASSERT_EQ(scopes_open, 0);
}

#endif /* TEST_CLUSTER_WAL_RETAINED_CUT_CASES_H */
