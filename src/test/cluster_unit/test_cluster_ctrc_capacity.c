/*-------------------------------------------------------------------------
 * test_cluster_ctrc_capacity.c
 *    Actual CTRC capacity proof with SCUR and native-status boundary inputs.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ctrc_capacity.c
 *
 * The shared selectors and terminal sampler are real product C. Boundary
 * inputs model a resident block and independently sampled native outcomes;
 * no fixture returns a precomputed terminal/release verdict.
 *-------------------------------------------------------------------------
 */
int ctrc_fixture_main(void);
#define main ctrc_fixture_main
#include "test_cluster_ctrc_cleaner.c"
#undef main

int cluster_ges_request_timeout_ms = 3000;
static UndoSegmentHeaderData sample_header;
static bool sample_commit, sample_abort, sample_active, sample_admitted;
static bool sample_recheck;
static unsigned sample_enters, sample_leaves, sample_holds, sample_releases;
static uint32 sample_current_segment = 1;
static bool sample_native_drift;
static unsigned sample_native_calls;
static void (*sample_release_hook)(void);
static PGAlignedBlock certificate_resident;
static bool certificate_current, certificate_pin, certificate_error_after_flush;
static unsigned certificate_wal_calls, certificate_flush_calls;
static bool sample_copy_error;

int
cluster_conf_node_count(void)
{
	return 4;
}

uint64
cluster_epoch_get_current(void)
{
	return test_cluster_epoch;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return (scn_local(a) > scn_local(b)) - (scn_local(a) < scn_local(b));
}

uint64
GetSystemIdentifier(void)
{
	return 42;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 1101;
}

uint32
cluster_tt_slot_current_segment(int node pg_attribute_unused())
{
	return sample_current_segment;
}

bool
TransactionIdDidCommit(TransactionId xid pg_attribute_unused())
{
	sample_native_calls++;
	return sample_native_drift && sample_native_calls > 1 ? !sample_commit : sample_commit;
}

bool
TransactionIdDidAbort(TransactionId xid pg_attribute_unused())
{
	return sample_abort;
}

bool
TransactionIdIsInProgress(TransactionId xid pg_attribute_unused())
{
	return sample_active;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(ClusterSemanticAdmissionToken *token)
{
	if (held_count != 0)
		abort();
	MemSet(token, 0, sizeof(*token));
	token->formation_epoch = 23;
	token->record_generation = 29;
	if (!sample_admitted)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	sample_enters++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token pg_attribute_unused())
{
	sample_leaves++;
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token pg_attribute_unused())
{
	return sample_recheck;
}

bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token pg_attribute_unused(), ClusterUndoPathIntent intent,
	uint32 owner pg_attribute_unused(), uint32 segment pg_attribute_unused(),
	ClusterUndoBlock0ResolvedRoot *out)
{
	MemSet(out, 0, sizeof(*out));
	out->intent = intent;
	out->root_id = 37;
	out->root_generation = 43;
	return true;
}

bool
cluster_undo_block0_root_matches(const ClusterUndoBlock0ResolvedRoot *a,
								 const ClusterUndoBlock0ResolvedRoot *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *key pg_attribute_unused(), ClusterUndoBlock0CurrentMode mode,
	int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused(),
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	if (mode != CLUSTER_UNDO_BLOCK0_SCUR || held_count != 0)
		abort();
	sample_holds++;
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
										 ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

bool
cluster_undo_block0_current_wait_reply(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
									   ClusterUndoBlock0ReplyWaitSite site pg_attribute_unused())
{
	return true;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	sample_releases++;
	certificate_current = false;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(ClusterUndoBlock0CurrentGuard *guard
											  pg_attribute_unused(),
										  ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	sample_releases++;
	certificate_current = false;
	if (sample_release_hook != NULL) {
		void (*hook)(void) = sample_release_hook;
		sample_release_hook = NULL;
		hook();
	}
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
										 ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root pg_attribute_unused(),
	ClusterUndoBlock0Generation *out)
{
	out->known = true;
	out->value = sample_header.wrap_count;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_ctrc_release(
	const ClusterUndoBlock0LogicalKey *key pg_attribute_unused(),
	int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused(),
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	if (held_count != 0 || certificate_current)
		abort();
	MemSet(certificate_resident.data, 0, BLCKSZ);
	memcpy(certificate_resident.data, &sample_header, sizeof(sample_header));
	certificate_current = true;
	sample_holds++;
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation_exclusive(ClusterUndoBlock0CurrentGuard *guard,
														const ClusterUndoBlock0ResolvedRoot *root,
														ClusterUndoBlock0Generation *out)
{
	if (!certificate_current || certificate_pin)
		abort();
	return cluster_undo_block0_current_sample_generation(guard, root, out);
}

ClusterUndoBlock0Result
cluster_undo_block0_current_pin_exclusive(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root pg_attribute_unused(),
	const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Pin *pin, char **page)
{
	if (!certificate_current || certificate_pin)
		abort();
	certificate_pin = true;
	pin->observed_generation = *expected;
	*page = certificate_resident.data;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_recheck_exclusive(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused())
{
	return certificate_current ? CLUSTER_UNDO_BLOCK0_OK : CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
}

bool
cluster_undo_block0_generation_matches(const ClusterUndoBlock0Generation *a,
									   const ClusterUndoBlock0Generation *b)
{
	return a->known && b->known && a->value == b->value;
}

void
cluster_undo_block0_unpin(ClusterUndoBlock0Pin *pin pg_attribute_unused())
{
	if (!certificate_pin || !certificate_current || held_count != 0)
		abort();
	certificate_pin = false;
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							 uint32 segment pg_attribute_unused(),
							 uint8 owner pg_attribute_unused(), uint32 block, char *out)
{
	if (block != 0 || !certificate_current || held_count != 0)
		abort();
	MemSet(out, 0, BLCKSZ);
	memcpy(out, &sample_header, sizeof(sample_header));
	return true;
}

XLogRecPtr
cluster_undo_xlog_insert_tt_ctrc_release(const xl_undo_tt_slot_ctrc_release_v1 *record)
{
	if (!certificate_current || !certificate_pin || held_count != 0
		|| record->segment_id != sample_header.segment_id)
		abort();
	certificate_wal_calls++;
	return 101;
}

void
cluster_undo_block0_flush_sync(ClusterUndoBlock0Pin *pin pg_attribute_unused(),
							   const char *successor, XLogRecPtr lsn, bool fsync_parent)
{
	if (!certificate_current || !certificate_pin || held_count != 0 || lsn != 101 || fsync_parent)
		abort();
	memcpy(certificate_resident.data, successor, BLCKSZ);
	memcpy(&sample_header, successor, sizeof(sample_header));
	certificate_flush_calls++;
	if (certificate_error_after_flush)
		pg_re_throw();
}

ClusterUndoBlock0Result
cluster_undo_block0_current_copy_resident(
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *root pg_attribute_unused(),
	const ClusterUndoBlock0Generation *generation pg_attribute_unused(), char page[BLCKSZ])
{
	if (sample_copy_error)
		pg_re_throw();
	MemSet(page, 0, BLCKSZ);
	memcpy(page, &sample_header, sizeof(sample_header));
	return CLUSTER_UNDO_BLOCK0_OK;
}

static ClusterCtrcTxnKeyV1
reset_capacity_sample(void)
{
	ClusterCtrcOriginEntry *origin;

	reset_fixture();
	origin = seed_origin(0, 0);
	(void)ctrc_origin_dispatchable_locked(origin);
	MemSet(&sample_header, 0, sizeof(sample_header));
	sample_header.segment_id = sample_current_segment = 1;
	sample_header.owner_instance = 1;
	sample_header.wrap_count = 1;
	sample_header.tt_slots_count = TT_SLOTS_PER_SEGMENT;
	sample_header.tt_slots[0].xid = origin->key.xid;
	sample_header.tt_slots[0].wrap = origin->key.slot_wrap;
	sample_header.tt_slots[0].status = TT_SLOT_COMMITTED;
	sample_header.tt_slots[0].commit_scn = 1000;
	sample_commit = sample_admitted = sample_recheck = true;
	sample_abort = sample_active = sample_native_drift = false;
	sample_enters = sample_leaves = sample_holds = sample_releases = sample_native_calls = 0;
	sample_release_hook = NULL;
	certificate_current = certificate_pin = certificate_error_after_flush = false;
	certificate_wal_calls = certificate_flush_calls = 0;
	sample_copy_error = false;
	return origin->key;
}

UT_TEST(test_sampler_error_releases_current_and_admission_under_optimization)
{
	test_cluster_epoch = 23;
	for (unsigned release_sampler = 0; release_sampler < 2; release_sampler++) {
		ClusterCtrcTxnKeyV1 key = reset_capacity_sample();
		volatile bool caught = false;
		sample_copy_error = true;
		PG_TRY();
		{
			if (release_sampler)
				(void)cluster_ctrc_terminal_release_sample_exact(
					1, 0, key.xid, key.slot_wrap, TT_SLOT_COMMITTED, 1000, test_cluster_epoch);
			else
				(void)ctrc_cleaner_terminal_sample_exact(&key, NULL, NULL);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(sample_holds, 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
	test_cluster_epoch = 13;
}

UT_TEST(test_actual_certificate_error_after_flush_retains_then_replays_handoff)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginCertificateSnapshot snapshot;
	ClusterCtrcOriginEntry before;
	volatile bool caught = false;

	test_cluster_epoch = 23;
	key = reset_capacity_sample();
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	before = ctrc_origin_entries()[0];
	certificate_error_after_flush = true;
	PG_TRY();
	{
		(void)ctrc_cleaner_publish_certificate(&snapshot);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!certificate_current && !certificate_pin);
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT_EQ(sample_header.tt_slots[0].flags, TT_SLOT_FLAG_CTRC_RELEASE_PROVEN);
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
	UT_ASSERT(!cluster_ctrc_terminal_release_sample_exact(
		1, 0, key.xid, key.slot_wrap, TT_SLOT_COMMITTED, 1000, test_cluster_epoch));
	UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, INVALID_TT_SLOT_OFFSET));
	certificate_error_after_flush = false;
	UT_ASSERT(ctrc_cleaner_publish_certificate(&snapshot));
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT(cluster_ctrc_terminal_release_sample_exact(
		1, 0, key.xid, key.slot_wrap, TT_SLOT_COMMITTED, 1000, test_cluster_epoch));
	UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header, INVALID_TT_SLOT_OFFSET));
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	test_cluster_epoch = 13;
}

UT_TEST(test_handoff_exact_identity_and_all_slot_scan_never_mutate)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;
	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	UT_ASSERT(cluster_ctrc_origin_certificate_commit_entry(&ctrc_origin_entries()[0], &snapshot));
	/* RELEASE_PROVEN, including notification in flight, is accepted.
	 * This predicate alone says nothing about the header's release flag. */
	UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header, 0));
	for (int fault = 0; fault < 11; fault++) {
		UndoSegmentHeaderData saved_header = sample_header;
		ClusterCtrcOriginEntry saved = ctrc_origin_entries()[0];
		ClusterCtrcOriginEntry observed;
		switch (fault) {
		case 0:
			ctrc_origin_entries()[0].state = CTRC_ORIGIN_CERTIFYING;
			break;
		case 1:
			ctrc_origin_entries()[0].state = CTRC_ORIGIN_BLOCKED;
			break;
		case 2:
			ctrc_origin_entries()[0].state = CTRC_ORIGIN_EMPTY;
			break;
		case 3:
			ctrc_origin_entries()[0].key.xid++;
			break;
		case 4:
			ctrc_origin_entries()[0].key.slot_wrap++;
			break;
		case 5:
			ctrc_origin_entries()[0].key.segment_generation++;
			break;
		case 6:
			ctrc_origin_entries()[0].key.slot_offset++;
			break;
		case 7:
			ctrc_origin_entries()[0].key.root_id = 0;
			break;
		case 8:
			sample_header.owner_instance++;
			break;
		case 9:
			sample_header.tt_slots_count--;
			break;
		case 10:
			sample_header.wrap_count = UINT32_MAX;
			break;
		}
		observed = ctrc_origin_entries()[0];
		UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, 0));
		UT_ASSERT_EQ(memcmp(&observed, &ctrc_origin_entries()[0], sizeof(observed)), 0);
		ctrc_origin_entries()[0] = saved;
		sample_header = saved_header;
	}
	UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(NULL, 0));
	UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, TT_SLOTS_PER_SEGMENT));
	(void)seed_origin(TT_SLOTS_PER_SEGMENT - 1, 0);
	UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header, 0));
	UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, INVALID_TT_SLOT_OFFSET));
}

static void
finish_certificate_after_sample(void)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;
	if (held_count != 0 || !ctrc_origin_certificate_snapshot_index_locked(0, &snapshot))
		abort();
	sample_header.tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	if (!ctrc_origin_certificate_commit_shared(&snapshot))
		abort();
}

UT_TEST(test_capacity_completion_between_sample_and_origin_recheck_is_not_error)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	(void)reset_capacity_sample();
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	sample_release_hook = finish_certificate_after_sample;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	UT_ASSERT(!ctrc_bytes_zero(&continuation, sizeof(continuation)));
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_slot_reuse_waits_for_shared_certificate_handoff)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginCertificateSnapshot snapshot;
	ClusterCtrcOriginEntry before;

	test_cluster_epoch = 23;
	key = reset_capacity_sample();
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	sample_header.tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	before = ctrc_origin_entries()[0];
	/* A successful durable publication alone must not let another cleaner
	 * expose FREE while the original publisher still owns CERTIFYING.
	 * This also covers ERROR after flush but before shared handoff. */
	UT_ASSERT(!cluster_ctrc_terminal_release_sample_exact(
		1, 0, key.xid, key.slot_wrap, TT_SLOT_COMMITTED, 1000, test_cluster_epoch));
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
	UT_ASSERT(ctrc_origin_certificate_commit_shared(&snapshot));
	UT_ASSERT(cluster_ctrc_terminal_release_sample_exact(
		1, 0, key.xid, key.slot_wrap, TT_SLOT_COMMITTED, 1000, test_cluster_epoch));
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	test_cluster_epoch = 13;
}

UT_TEST(test_capacity_continues_after_release_without_weakening_seal)
{
	ClusterCtrcTxnKeyV1 key = reset_capacity_sample();
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	ClusterCtrcOriginEntry before = ctrc_origin_entries()[0];

	UT_ASSERT(ctrc_cleaner_terminal_sample_exact(&key, NULL, NULL));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&key, &continuation, sizeof(key)), 0);
	sample_header.tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT(!ctrc_cleaner_terminal_sample_exact(&key, NULL, NULL));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	MemSet(&ctrc_origin_entries()[0], 0, sizeof(before));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(before)));
	sample_current_segment = 2;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_RETRY);
	UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_capacity_proof_negative_matrix_retains_all_bytes)
{
	for (int fault = 0; fault < 15; fault++) {
		ClusterCtrcTxnKeyV1 key = reset_capacity_sample();
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterCtrcOriginEntry before;
		UndoSegmentHeaderData header_before;
		SCN horizon = 2000;
		uint64 epoch = test_cluster_epoch;

		switch (fault) {
		case 0:
			sample_active = true;
			break;
		case 1:
			sample_commit = false;
			break;
		case 2:
			sample_abort = true;
			break;
		case 3:
			sample_native_drift = true;
			break;
		case 4:
			sample_header.tt_slots[0].wrap++;
			break;
		case 5:
			sample_header.tt_slots[0].xid++;
			break;
		case 6:
			sample_header.wrap_count++;
			break;
		case 7:
			sample_header.tt_slots[0].flags = 0x80;
			break;
		case 8:
			((uint8 *)&sample_header.tt_slots[0].first_undo_block)[0] = 1;
			break;
		case 9:
			sample_header.tt_slots[0].commit_scn = 3000;
			break;
		case 10:
			sample_admitted = false;
			break;
		case 11:
			sample_recheck = false;
			break;
		case 12:
			horizon = InvalidScn;
			break;
		case 13:
			epoch++;
			break;
		case 14:
			ctrc_origin_entries()[0].key.root_id++;
			break;
		}
		before = ctrc_origin_entries()[0];
		header_before = sample_header;
		UT_ASSERT_EQ(
			cluster_ctrc_capacity_probe_current(key.segment_id, horizon, epoch, &continuation),
			CLUSTER_CTRC_CAPACITY_REFUSE);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(sample_header)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

UT_TEST(test_capacity_abort_and_witness_loss_are_not_guessed)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };

	(void)reset_capacity_sample();
	sample_header.tt_slots[0].status = TT_SLOT_ABORTED;
	sample_header.tt_slots[0].commit_scn = InvalidScn;
	sample_commit = false;
	sample_abort = true;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	MemSet(&ctrc_origin_entries()[0], 0, sizeof(ClusterCtrcOriginEntry));
	/* No certificate work AND no durable release flag is not progress. */
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 2000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_REFUSE);
	UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_sampler_error_releases_current_and_admission_under_optimization);
	UT_RUN(test_actual_certificate_error_after_flush_retains_then_replays_handoff);
	UT_RUN(test_handoff_exact_identity_and_all_slot_scan_never_mutate);
	UT_RUN(test_capacity_completion_between_sample_and_origin_recheck_is_not_error);
	UT_RUN(test_slot_reuse_waits_for_shared_certificate_handoff);
	UT_RUN(test_capacity_continues_after_release_without_weakening_seal);
	UT_RUN(test_capacity_proof_negative_matrix_retains_all_bytes);
	UT_RUN(test_capacity_abort_and_witness_loss_are_not_guessed);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count != 0;
}
