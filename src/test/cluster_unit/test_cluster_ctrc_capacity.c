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
#include "test_cluster_capacity_pool_compat.h"

#ifdef CLUSTER_CAPACITY_POOL_TEST_API_ABSENT
static ClusterCapacityReasonV1
capacity_pool_api_absent(uint32 segment pg_attribute_unused(), SCN horizon pg_attribute_unused(),
						 uint64 epoch pg_attribute_unused(), ClusterCapacityObservationV1 *out)
{
	if (out != NULL) {
		MemSet(out, 0, sizeof(*out));
		out->reason = CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
	}
	return CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
}
#define cluster_ctrc_capacity_probe_pool_v1 capacity_pool_api_absent
extern bool cluster_undo_cleaner_capacity_owner_snapshot(unsigned worker, uint64 *coordinator,
														 uint64 *generation);
#endif

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
static bool pool_owner_available;
static uint64 pool_owner_generation, pool_coordinator_generation;
static unsigned pool_owner_calls;

/* Original owner identity is a boundary here; its actual producer is exercised
 * by test_cluster_undo_cleaner_capacity. No PID is treated as liveness proof. */
bool
cluster_undo_cleaner_capacity_owner_snapshot(unsigned worker, uint64 *coordinator,
											 uint64 *generation)
{
	if (held_count != 0 || worker >= CLUSTER_CTRC_CLEANER_WORKERS)
		abort();
	pool_owner_calls++;
	if (!pool_owner_available)
		return false;
	*coordinator = pool_coordinator_generation;
	*generation = pool_owner_generation;
	return true;
}

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
	pool_owner_available = true;
	pool_owner_generation = 71;
	pool_coordinator_generation = 83;
	pool_owner_calls = 0;
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


/* Seed the original pool and a separately supplied canonical block. CURRENT
 * stays at1 while the candidate can be any old segment/slot in the real pool. */
static ClusterCtrcTxnKeyV1
reset_pool_sample(unsigned index)
{
	ClusterCtrcOriginEntry *origin;

	(void)reset_capacity_sample();
	MemSet(&ctrc_origin_entries()[0], 0, sizeof(ClusterCtrcOriginEntry));
	origin = seed_origin(index, 0);
	(void)ctrc_origin_dispatchable_locked(origin);
	MemSet(&sample_header, 0, sizeof(sample_header));
	sample_header.segment_id = origin->key.segment_id;
	sample_header.owner_instance = 1;
	sample_header.wrap_count = 1;
	sample_header.tt_slots_count = TT_SLOTS_PER_SEGMENT;
	sample_header.tt_slots[origin->key.slot_offset].xid = origin->key.xid;
	sample_header.tt_slots[origin->key.slot_offset].wrap = origin->key.slot_wrap;
	sample_header.tt_slots[origin->key.slot_offset].status = TT_SLOT_COMMITTED;
	sample_header.tt_slots[origin->key.slot_offset].commit_scn = 1000;
	return origin->key;
}

UT_TEST(test_pool_observes_last_old_segment_before_original_cv)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	ClusterCtrcTxnKeyV1 key = reset_pool_sample(index);
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginEntry before = ctrc_origin_entries()[index];
	UndoSegmentHeaderData header_before = sample_header;

	cluster_ctrc_stat_bump(CTRC_STAT_SEMANTIC_PROGRESS);
	/* H is below the commit. Certificate work can precede H advancement. */
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(observation.reason, CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
	UT_ASSERT_EQ(observation.pool_positions, CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT_EQ(observation.owner_worker, 7);
	UT_ASSERT_EQ(observation.owner_generation, 71);
	UT_ASSERT_EQ(observation.coordinator_generation, 83);
	UT_ASSERT_EQ(observation.observed_change_sequence,
				 cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS));
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
	UT_ASSERT_EQ(sample_holds, 1);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_missing_phase_ack_identity_native_and_owner_stays_unproven)
{
	for (unsigned fault = 0; fault < 20; fault++) {
		unsigned index = TT_SLOTS_PER_SEGMENT * 3;
		ClusterCtrcTxnKeyV1 key = reset_pool_sample(index);
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
		ClusterCtrcOriginEntry before;
		UndoSegmentHeaderData header_before;
		SCN horizon = 2000;
		uint64 epoch = test_cluster_epoch;

		switch (fault) {
		case 0:
			origin->state = CTRC_ORIGIN_CLEANING;
			break;
		case 1:
			origin->touched_bitmap = 1;
			origin->touched_count = 1;
			break;
		case 2:
			sample_active = true;
			break;
		case 3:
			sample_commit = false;
			break;
		case 4:
			sample_abort = true;
			break;
		case 5:
			sample_native_drift = true;
			break;
		case 6:
			sample_header.tt_slots[0].status = TT_SLOT_ACTIVE;
			break;
		case 7:
			sample_header.tt_slots[0].wrap++;
			break;
		case 8:
			sample_header.tt_slots[0].xid++;
			break;
		case 9:
			sample_header.wrap_count++;
			break;
		case 10:
			sample_header.tt_slots[0].flags = 0x80;
			break;
		case 11:
			((uint8 *)&sample_header.tt_slots[0].first_undo_block)[0] = 1;
			break;
		case 12:
			sample_admitted = false;
			break;
		case 13:
			sample_recheck = false;
			break;
		case 14:
			origin->key.origin_boot_incarnation++;
			break;
		case 15:
			origin->key.root_id++;
			break;
		case 16:
			pool_owner_available = false;
			break;
		case 17:
			horizon = InvalidScn;
			break;
		case 18:
			epoch++;
			break;
		case 19:
			origin->key.system_identifier++;
			break;
		}
		before = *origin;
		header_before = sample_header;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, horizon, epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT(observation.unproven_reasons != 0);
		UT_ASSERT_EQ(memcmp(&before, origin, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
		(void)key;
	}
}

UT_TEST(test_pool_bad_candidate_does_not_block_independent_original_work)
{
	ClusterCtrcTxnKeyV1 key = reset_pool_sample(2);
	ClusterCtrcOriginEntry *bad = seed_origin(1, 0);
	ClusterCtrcOriginEntry before;
	ClusterCapacityObservationV1 observation;
	(void)ctrc_origin_dispatchable_locked(bad);
	bad->touched_bitmap = 1;
	bad->touched_count = 1;
	before = *bad;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 2000, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(memcmp(&before, bad, sizeof(before)), 0);
}

UT_TEST(test_pool_certificate_batch_limit_advances_original_cursor)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;

	/* Original selector calibrates its original next position to1. No test
	 * writes a product cursor or expands a batch to reach the good row. */
	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(17);
	for (unsigned index = 1; index <= 16; index++)
		(void)ctrc_origin_dispatchable_locked(seed_origin(index, 0));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 2000, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_EQ(observation.pool_positions, CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(sample_holds, 16);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 2000, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(sample_holds, 17);
}

static unsigned pool_drift_kind;
static void
pool_change_during_scur_release(void)
{
	sample_release_hook = NULL;
	switch (pool_drift_kind) {
	case 0:
		ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT * 3].seal_generation++;
		break;
	case 1:
		ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT * 3].state = CTRC_ORIGIN_RELEASE_PROVEN;
		break;
	case 2:
		sample_current_segment = 2;
		break;
	case 3:
		pool_owner_generation++;
		break;
	case 4:
		pool_coordinator_generation++;
		break;
	}
}

UT_TEST(test_pool_rechecks_exact_phase_current_and_owner_after_physical_sample)
{
	for (pool_drift_kind = 0; pool_drift_kind < 5; pool_drift_kind++) {
		ClusterCapacityObservationV1 observation;
		(void)reset_pool_sample(TT_SLOTS_PER_SEGMENT * 3);
		sample_release_hook = pool_change_during_scur_release;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 2000, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(observation.reason, CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_never_turns_publisher_pending_or_horizon_into_reuse_authority)
{
	ClusterCtrcTxnKeyV1 key = reset_pool_sample(TT_SLOTS_PER_SEGMENT * 3);
	ClusterCapacityObservationV1 observation;
	sample_header.tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, key.slot_offset));
	UT_ASSERT(!cluster_ctrc_terminal_release_sample_exact(key.segment_id, key.slot_offset, key.xid,
														  key.slot_wrap, TT_SLOT_COMMITTED, 1000,
														  test_cluster_epoch));
	UT_ASSERT_EQ(ctrc_origin_entries()[TT_SLOTS_PER_SEGMENT * 3].state, CTRC_ORIGIN_CERTIFYING);
}

UT_TEST(test_pool_errors_keep_original_scur_and_admission_cleanup)
{
	volatile bool caught = false;
	ClusterCapacityObservationV1 observation;
	(void)reset_pool_sample(TT_SLOTS_PER_SEGMENT * 3);
	sample_copy_error = true;
	PG_TRY();
	{
		(void)cluster_ctrc_capacity_probe_pool_v1(1, 2000, test_cluster_epoch, &observation);
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
	UT_ASSERT_EQ(held_count, 0);
}


UT_TEST(test_pool_uses_original_required_ack_bytes_and_retains_corruption)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcOriginCertificateSnapshot certificate;
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcLocalReleaseAckV1 *ack;
	uint64 participant, receipt, ack_index;

	(void)reset_pool_sample(index);
	MemSet(&ctrc_origin_entries()[index], 0, sizeof(ClusterCtrcOriginEntry));
	UT_ASSERT(seed_frozen_certificate(index, &dispatch, &participant, &receipt));
	origin = &ctrc_origin_entries()[index];
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 501));
	UT_ASSERT(
		cluster_ctrc_origin_ack_land_shared(501, &ctrc_participant_ack_entries()[participant]));
	(void)ctrc_origin_dispatchable_locked(origin);
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT(ctrc_origin_ack_index(&origin->key, 0, &ack_index));
	ack = &ctrc_origin_ack_entries()[ack_index];
	ack->crc32c ^= 1;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_REQUIRED_ACK) != 0);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT_EQ(ack->crc32c, certificate.acks[0].crc32c ^ 1);
	UT_ASSERT_EQ(held_count, 0);
}

static bool
pool_interrupt_after_original_lock_span(LWLock *lock, LWLockMode mode pg_attribute_unused(),
										bool acquiring)
{
	if (acquiring || lock != &CtrcShared->origin_lock)
		return false;
	if (held_count != 1)
		abort();
	test_lock_leave(lock);
	external_lock_hook = NULL;
	InterruptPending = 1;
	return true;
}

UT_TEST(test_pool_census_cancellation_runs_after_original_locks_are_released)
{
	ClusterCapacityObservationV1 observation;
	volatile bool caught = false;
	(void)reset_pool_sample(CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1);
	external_lock_hook = pool_interrupt_after_original_lock_span;
	PG_TRY();
	{
		(void)cluster_ctrc_capacity_probe_pool_v1(1, 2000, test_cluster_epoch, &observation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	InterruptPending = 0;
	external_lock_hook = NULL;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(sample_holds, 0);
	UT_ASSERT_EQ(sample_enters, 0);
}

/* Publish through the original ACK and WAL/block-0 path. No phase or
 * capacity verdict is supplied by this fixture. Other pool rows survive. */
static bool
seed_pool_notification(unsigned index)
{
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcOriginCertificateSnapshot certificate;
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	uint64 participant, receipt;

	MemSet(origin, 0, sizeof(*origin));
	if (!seed_frozen_certificate(index, &dispatch, &participant, &receipt)
		|| !cluster_ctrc_origin_arm_close_entry(origin, 0, 501)
		|| !cluster_ctrc_origin_ack_land_shared(501, &ctrc_participant_ack_entries()[participant]))
		return false;
	(void)ctrc_origin_dispatchable_locked(origin);
	MemSet(&sample_header, 0, sizeof(sample_header));
	sample_header.segment_id = origin->key.segment_id;
	sample_header.owner_instance = 1;
	sample_header.wrap_count = origin->key.segment_generation;
	sample_header.tt_slots_count = TT_SLOTS_PER_SEGMENT;
	sample_header.tt_slots[origin->key.slot_offset].xid = origin->key.xid;
	sample_header.tt_slots[origin->key.slot_offset].wrap = origin->key.slot_wrap;
	sample_header.tt_slots[origin->key.slot_offset].status = TT_SLOT_COMMITTED;
	sample_header.tt_slots[origin->key.slot_offset].commit_scn = 1000;
	return ctrc_origin_certificate_snapshot_index_locked(index, &certificate)
		   && ctrc_cleaner_publish_certificate(&certificate)
		   && origin->state == CTRC_ORIGIN_RELEASE_PROVEN && origin->touched_bitmap == 1
		   && origin->close_dispatched_bitmap == 0 && origin->close_confirmed_bitmap == 0
		   && sample_header.tt_slots[origin->key.slot_offset].flags
				  == TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
}

UT_TEST(test_pool_observes_actual_release_notification_without_granting_supply)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginEntry before;
	ClusterCtrcLocalReleaseAckV1 ack_before;
	UndoSegmentHeaderData header_before;
	uint64 ack_index;

	(void)reset_pool_sample(index);
	UT_ASSERT(seed_pool_notification(index));
	before = ctrc_origin_entries()[index];
	UT_ASSERT(ctrc_origin_ack_index(&before.key, 0, &ack_index));
	ack_before = ctrc_origin_ack_entries()[ack_index];
	header_before = sample_header;
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
	UT_ASSERT_EQ(observation.pool_positions, CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(memcmp(&observation.key, &before.key, sizeof(before.key)), 0);
	UT_ASSERT_EQ(observation.owner_worker, 7);
	UT_ASSERT_EQ(observation.owner_generation, 71);
	UT_ASSERT_EQ(observation.coordinator_generation, 83);
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&ack_before, &ctrc_origin_ack_entries()[ack_index], sizeof(ack_before)), 0);
	UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT_EQ(sample_holds, 2);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header, before.key.slot_offset));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_notification_dispatch_and_real_reply_retire_original_work)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcSealReplyResult result;
	ClusterCtrcOriginEntry *origin;
	uint64 ack_index;

	(void)reset_pool_sample(index);
	UT_ASSERT(seed_pool_notification(index));
	origin = &ctrc_origin_entries()[index];
	UT_ASSERT(ctrc_origin_ack_index(&origin->key, 0, &ack_index));
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 0, 901));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	MemSet(&dispatch, 0, sizeof(dispatch));
	dispatch.key = origin->key;
	dispatch.participant = origin->touched[0];
	dispatch.grant_generation = origin->grant_generation;
	dispatch.seal_generation = origin->seal_generation;
	dispatch.request_id = 901;
	dispatch.suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(&dispatch, 1, &result));
	UT_ASSERT_EQ(result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(901, 0, result));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_ack_entries()[ack_index],
							  sizeof(ClusterCtrcLocalReleaseAckV1)));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_notification_missing_evidence_retains_original_bytes)
{
	for (unsigned fault = 0; fault < 21; fault++) {
		unsigned index = TT_SLOTS_PER_SEGMENT * 3;
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry *origin;
		ClusterCtrcOriginEntry before;
		ClusterCtrcLocalReleaseAckV1 ack_before;
		UndoSegmentHeaderData header_before;
		uint64 ack_index;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_notification(index));
		origin = &ctrc_origin_entries()[index];
		UT_ASSERT(ctrc_origin_ack_index(&origin->key, 0, &ack_index));
		switch (fault) {
		case 0:
			sample_header.tt_slots[0].flags = 0;
			break;
		case 1:
			ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
			break;
		case 2:
			sample_active = true;
			break;
		case 3:
			sample_commit = false;
			break;
		case 4:
			sample_admitted = false;
			break;
		case 5:
			sample_recheck = false;
			break;
		case 6:
			pool_owner_available = false;
			break;
		case 7:
			origin->close_dispatched_bitmap = 2;
			break;
		case 8:
			origin->close_confirmed_bitmap = 1;
			break;
		case 9:
			origin->close_request_id[0] = 901;
			break;
		case 10:
			origin->close_dispatched_bitmap = 1;
			break;
		case 11:
			origin->close_request_id[1] = 901;
			break;
		case 12:
			UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 0, 901));
			UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_entry(
				origin, 0, 901, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
			break;
		case 13:
			origin->ack_bitmap = 0;
			break;
		case 14:
			origin->grant_generation = 0;
			break;
		case 15:
			origin->seal_generation = UINT64_MAX;
			break;
		case 16:
			origin->key.xid++;
			break;
		case 17:
			sample_header.tt_slots[0].status = TT_SLOT_ACTIVE;
			break;
		case 18:
			origin->state = CTRC_ORIGIN_BLOCKED;
			break;
		case 19:
			MemSet(&ctrc_origin_ack_entries()[ack_index], 0, sizeof(ClusterCtrcLocalReleaseAckV1));
			break;
		case 20:
			origin->touched_bitmap = 0;
			break;
		}
		before = *origin;
		ack_before = ctrc_origin_ack_entries()[ack_index];
		header_before = sample_header;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT(observation.unproven_reasons != 0);
		UT_ASSERT_EQ(memcmp(&before, origin, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&ack_before, &ctrc_origin_ack_entries()[ack_index], sizeof(ack_before)),
					 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(certificate_wal_calls, 1);
		UT_ASSERT_EQ(certificate_flush_calls, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_notification_does_not_broaden_certificate_publication_contract)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCtrcOriginCertificateSnapshot snapshot;
	ClusterCtrcLocalReleaseAckV1 acks[CLUSTER_CTRC_MAX_PARTICIPANTS] = { 0 };
	ClusterCtrcOriginEntry *origin;
	uint64 ack_index;

	(void)reset_pool_sample(index);
	UT_ASSERT(seed_pool_notification(index));
	origin = &ctrc_origin_entries()[index];
	UT_ASSERT(ctrc_origin_ack_index(&origin->key, 0, &ack_index));
	acks[0] = ctrc_origin_ack_entries()[ack_index];
	UT_ASSERT(!cluster_ctrc_origin_certificate_snapshot_entry(origin, acks, index, &snapshot));
	UT_ASSERT(ctrc_bytes_zero(&snapshot, sizeof(snapshot)));
	UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &snapshot));
	UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header, origin->key.slot_offset));
}

static unsigned notification_drift_kind;
static void
notification_change_during_scur_release(void)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	uint64 ack_index;

	if (held_count != 0 || !ctrc_origin_ack_index(&origin->key, 0, &ack_index))
		abort();
	switch (notification_drift_kind) {
	case 0:
		origin->seal_generation++;
		break;
	case 1:
		origin->state = CTRC_ORIGIN_CERTIFYING;
		break;
	case 2:
		ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
		break;
	case 3:
		origin->key.xid++;
		break;
	case 4:
		if (!cluster_ctrc_origin_arm_certificate_entry(origin, 0, 901))
			abort();
		break;
	case 5:
		if (!cluster_ctrc_origin_note_certificate_reply_shared(
				901, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED))
			abort();
		break;
	case 6:
		sample_current_segment = 2;
		break;
	case 7:
		pool_owner_generation++;
		break;
	case 8:
		pool_coordinator_generation++;
		break;
	}
}

UT_TEST(test_pool_notification_rechecks_work_ack_and_owner_after_canonical_sample)
{
	for (notification_drift_kind = 0; notification_drift_kind < 9; notification_drift_kind++) {
		unsigned index = TT_SLOTS_PER_SEGMENT * 3;
		ClusterCapacityObservationV1 observation;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_notification(index));
		if (notification_drift_kind == 5)
			UT_ASSERT(
				cluster_ctrc_origin_arm_certificate_entry(&ctrc_origin_entries()[index], 0, 901));
		sample_release_hook = notification_change_during_scur_release;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_notification_and_certifying_share_one_fair_physical_budget)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	UndoSegmentHeaderData good_header;
	unsigned holds_before;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	(void)reset_pool_sample(17);
	UT_ASSERT(seed_pool_notification(17));
	key = ctrc_origin_entries()[17].key;
	good_header = sample_header;
	for (unsigned index = 1; index <= 16; index++) {
		if (index % 2 != 0)
			UT_ASSERT(seed_pool_notification(index));
		else
			(void)ctrc_origin_dispatchable_locked(seed_origin(index, 0));
	}
	sample_header = good_header;
	holds_before = sample_holds;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_EQ(sample_holds - holds_before, 16);
	UT_ASSERT_EQ(observation.pool_positions, CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT_EQ(sample_holds - holds_before, 17);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_invalid_notification_does_not_hide_independent_certifying_work)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	UndoSegmentHeaderData good_header;
	uint64 ack_index;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(2);
	good_header = sample_header;
	UT_ASSERT(seed_pool_notification(1));
	UT_ASSERT(ctrc_origin_ack_index(&ctrc_origin_entries()[1].key, 0, &ack_index));
	ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
	sample_header = good_header;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(ctrc_origin_entries()[1].state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_notification_error_keeps_original_sampler_cleanup)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCapacityObservationV1 observation;
	volatile bool caught = false;

	(void)reset_pool_sample(index);
	UT_ASSERT(seed_pool_notification(index));
	sample_copy_error = true;
	PG_TRY();
	{
		(void)cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(sample_holds, 2);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(ctrc_origin_entries()[index].state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT_EQ(held_count, 0);
}

/* Stop after an actual seal or ACK landing, before the original dispatch
 * continuation. Keep other rows for mixed-phase pool tests. */
static bool
seed_pool_seal_ready(unsigned index, bool empty_required)
{
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	ClusterCtrcCloseDispatch dispatch;
	uint64 participant, receipt;

	MemSet(origin, 0, sizeof(*origin));
	if (empty_required)
		return seed_origin(index, 0)->state == CTRC_ORIGIN_SEALING;
	return seed_frozen_certificate(index, &dispatch, &participant, &receipt)
		   && cluster_ctrc_origin_arm_close_entry(origin, 0, 501)
		   && cluster_ctrc_origin_ack_land_shared(501, &ctrc_participant_ack_entries()[participant])
		   && origin->state == CTRC_ORIGIN_SEALED && origin->ack_bitmap == 1
		   && origin->close_dispatched_bitmap == 1 && origin->close_confirmed_bitmap == 1;
}

UT_TEST(test_pool_observes_original_seal_completion_before_certificate_transition)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	for (unsigned scenario = 0; scenario < 4; scenario++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;
		ClusterCtrcLocalReleaseAckV1 ack_before;
		UndoSegmentHeaderData header_before;
		uint64 ack_index, progress;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_seal_ready(index, scenario % 2 == 0));
		before = ctrc_origin_entries()[index];
		UT_ASSERT(ctrc_origin_ack_index(&before.key, 0, &ack_index));
		ack_before = ctrc_origin_ack_entries()[ack_index];
		if (scenario >= 2) {
			sample_header.tt_slots[before.key.slot_offset].status = TT_SLOT_ABORTED;
			sample_header.tt_slots[before.key.slot_offset].commit_scn = InvalidScn;
			sample_commit = false;
			sample_abort = true;
		}
		header_before = sample_header;
		progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT_EQ(observation.phase, before.state);
		UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
		UT_ASSERT_EQ(observation.pool_positions,
					 CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
		UT_ASSERT_EQ(memcmp(&observation.key, &before.key, sizeof(before.key)), 0);
		UT_ASSERT_EQ(observation.owner_worker, 7);
		UT_ASSERT_EQ(observation.owner_generation, 71);
		UT_ASSERT_EQ(observation.coordinator_generation, 83);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&ack_before, &ctrc_origin_ack_entries()[ack_index], sizeof(ack_before)),
					 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(certificate_flush_calls, 0);
		UT_ASSERT_EQ(sample_holds, 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_seal_observation_preserves_original_transition_and_release_gates)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginCertificateSnapshot certificate;
		ClusterCtrcOriginEntry *origin;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_seal_ready(index, empty != 0));
		origin = &ctrc_origin_entries()[index];
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
		/* A durable flag alone still cannot bypass the shared publisher. */
		sample_header.tt_slots[origin->key.slot_offset].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, origin->key.slot_offset));
		sample_header.tt_slots[origin->key.slot_offset].flags = 0;
		(void)ctrc_origin_dispatchable_locked(origin);
		UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
		UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&certificate));
		UT_ASSERT(ctrc_cleaner_publish_certificate(&certificate));
		UT_ASSERT_EQ(certificate_wal_calls, 1);
		UT_ASSERT_EQ(certificate_flush_calls, 1);
		UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header,
													  certificate.origin.key.slot_offset));
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static void
invalidate_pool_seal_ready(unsigned fault, ClusterCtrcOriginEntry *origin, uint64 ack_index)
{
	switch (fault) {
	case 0:
		ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
		break;
	case 1:
		origin->ack_bitmap = 0;
		break;
	case 2:
		MemSet(&ctrc_origin_ack_entries()[ack_index], 0, sizeof(ClusterCtrcLocalReleaseAckV1));
		break;
	case 3:
		origin->close_dispatched_bitmap = 0;
		break;
	case 4:
		origin->close_confirmed_bitmap = 0;
		break;
	case 5:
		origin->close_confirmed_bitmap = 3;
		break;
	case 6:
		origin->close_request_id[0] = 0;
		break;
	case 7:
		origin->close_request_id[1] = 501;
		break;
	case 8:
		origin->touched_count++;
		break;
	case 9:
		origin->touched[0].boot_incarnation++;
		break;
	case 10:
		origin->grant_generation++;
		break;
	case 11:
		origin->seal_generation = 0;
		break;
	case 12:
		origin->seal_generation = UINT64_MAX;
		break;
	case 13:
		origin->reserved8[0] = 1;
		break;
	case 14:
		origin->key.xid++;
		break;
	case 15:
		pool_owner_available = false;
		break;
	case 16:
		sample_commit = false;
		break;
	case 17:
		sample_active = true;
		break;
	case 18:
		sample_admitted = false;
		break;
	case 19:
		sample_recheck = false;
		break;
	case 20:
		origin->state = CTRC_ORIGIN_SEALING;
		break;
	case 21:
		origin->state = CTRC_ORIGIN_BLOCKED;
		break;
	case 22:
		sample_header.wrap_count++;
		break;
	case 23:
		sample_native_drift = true;
		break;
	}
}

UT_TEST(test_pool_seal_requires_actual_ack_identity_native_and_owner)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned fault = 0; fault < 24; fault++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry *origin, before;
		ClusterCtrcLocalReleaseAckV1 ack_before;
		UndoSegmentHeaderData header_before;
		uint64 ack_index;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_seal_ready(index, false));
		origin = &ctrc_origin_entries()[index];
		UT_ASSERT(ctrc_origin_ack_index(&origin->key, 0, &ack_index));
		invalidate_pool_seal_ready(fault, origin, ack_index);
		before = *origin;
		ack_before = ctrc_origin_ack_entries()[ack_index];
		header_before = sample_header;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT(observation.unproven_reasons != 0);
		UT_ASSERT_EQ(memcmp(&before, origin, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&ack_before, &ctrc_origin_ack_entries()[ack_index], sizeof(ack_before)),
					 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_empty_seal_and_close_reply_cannot_substitute_for_required_ack)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginEntry *origin;
	for (unsigned fault = 0; fault < 6; fault++) {
		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_seal_ready(index, true));
		origin = &ctrc_origin_entries()[index];
		switch (fault) {
		case 0:
			origin->close_request_id[0] = 501;
			break;
		case 1:
			origin->close_dispatched_bitmap = 1;
			break;
		case 2:
			origin->close_confirmed_bitmap = 1;
			break;
		case 3:
			origin->seal_generation = 0;
			break;
		case 4:
			origin->touched_count = 1;
			break;
		case 5:
			ctrc_origin_ack_entries()[index * 4].crc32c = 1;
			break;
		}
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT_EQ(sample_holds, 0);
	}
	(void)reset_pool_sample(index);
	MemSet(&ctrc_origin_entries()[index], 0, sizeof(ClusterCtrcOriginEntry));
	origin = seed_origin(index, 1);
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 501));
	UT_ASSERT(
		cluster_ctrc_origin_note_close_reply_entry(origin, 0, 501, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_SEALED);
	UT_ASSERT_EQ(origin->ack_bitmap, 0);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT(cluster_ctrc_origin_begin_cleaning_entry(origin));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CLEANING);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CLEANING);
	UT_ASSERT_EQ(origin->ack_bitmap, 0);
	UT_ASSERT_EQ(sample_holds, 0);
	UT_ASSERT_EQ(held_count, 0);
}

static unsigned seal_ready_drift_kind;
static void
seal_ready_change_during_scur_release(void)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	uint64 ack_index;

	if (held_count != 0 || !ctrc_origin_ack_index(&origin->key, 0, &ack_index))
		abort();
	switch (seal_ready_drift_kind) {
	case 0:
		(void)ctrc_origin_dispatchable_locked(origin);
		break;
	case 1:
		ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
		break;
	case 2:
		origin->seal_generation++;
		break;
	case 3:
		origin->key.xid++;
		break;
	case 4:
		pool_owner_generation++;
		break;
	case 5:
		pool_coordinator_generation++;
		break;
	case 6:
		sample_current_segment = 2;
		break;
	case 7:
		origin->close_request_id[0]++;
		break;
	}
}

UT_TEST(test_pool_seal_rechecks_original_transition_ack_and_owner_after_sample)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned empty = 0; empty < 2; empty++) {
		for (seal_ready_drift_kind = 0; seal_ready_drift_kind < 8; seal_ready_drift_kind++) {
			ClusterCapacityObservationV1 observation;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_seal_ready(index, empty != 0));
			sample_release_hook = seal_ready_change_during_scur_release;
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				CLUSTER_CAPACITY_SUPPLY_RECHECK);
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

UT_TEST(test_pool_seals_and_certificates_share_original_cursor_and_total_budget)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	UndoSegmentHeaderData good_header;
	unsigned holds_before;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	(void)reset_pool_sample(17);
	UT_ASSERT(seed_pool_seal_ready(17, false));
	key = ctrc_origin_entries()[17].key;
	good_header = sample_header;
	for (unsigned index = 1; index <= 16; index++) {
		switch (index % 4) {
		case 0:
			(void)ctrc_origin_dispatchable_locked(seed_origin(index, 0));
			break;
		case 1:
			UT_ASSERT(seed_pool_seal_ready(index, false));
			break;
		case 2:
			UT_ASSERT(seed_pool_seal_ready(index, true));
			break;
		case 3:
			UT_ASSERT(seed_pool_notification(index));
			break;
		}
	}
	sample_header = good_header;
	holds_before = sample_holds;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_EQ(sample_holds - holds_before, 16);
	UT_ASSERT_EQ(observation.pool_positions, CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_SEALED);
	UT_ASSERT_EQ(sample_holds - holds_before, 17);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_bad_seal_does_not_hide_independent_certificate_work)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginEntry bad_before;
	uint64 ack_index;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(2);
	UT_ASSERT(seed_pool_seal_ready(1, false));
	UT_ASSERT(ctrc_origin_ack_index(&ctrc_origin_entries()[1].key, 0, &ack_index));
	ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
	bad_before = ctrc_origin_entries()[1];
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(memcmp(&bad_before, &ctrc_origin_entries()[1], sizeof(bad_before)), 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_seal_error_releases_original_sampler_holdings)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;
		volatile bool caught = false;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_seal_ready(index, empty != 0));
		before = ctrc_origin_entries()[index];
		sample_copy_error = true;
		PG_TRY();
		{
			(void)cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
		UT_ASSERT_EQ(sample_holds, 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

int
main(void)
{
	UT_PLAN(33);
	UT_RUN(test_pool_observes_original_seal_completion_before_certificate_transition);
	UT_RUN(test_pool_seal_observation_preserves_original_transition_and_release_gates);
	UT_RUN(test_pool_seal_requires_actual_ack_identity_native_and_owner);
	UT_RUN(test_pool_empty_seal_and_close_reply_cannot_substitute_for_required_ack);
	UT_RUN(test_pool_seal_rechecks_original_transition_ack_and_owner_after_sample);
	UT_RUN(test_pool_seals_and_certificates_share_original_cursor_and_total_budget);
	UT_RUN(test_pool_bad_seal_does_not_hide_independent_certificate_work);
	UT_RUN(test_pool_seal_error_releases_original_sampler_holdings);
	UT_RUN(test_pool_observes_actual_release_notification_without_granting_supply);
	UT_RUN(test_pool_notification_dispatch_and_real_reply_retire_original_work);
	UT_RUN(test_pool_notification_missing_evidence_retains_original_bytes);
	UT_RUN(test_pool_notification_does_not_broaden_certificate_publication_contract);
	UT_RUN(test_pool_notification_rechecks_work_ack_and_owner_after_canonical_sample);
	UT_RUN(test_pool_notification_and_certifying_share_one_fair_physical_budget);
	UT_RUN(test_pool_invalid_notification_does_not_hide_independent_certifying_work);
	UT_RUN(test_pool_notification_error_keeps_original_sampler_cleanup);
	UT_RUN(test_pool_uses_original_required_ack_bytes_and_retains_corruption);
	UT_RUN(test_pool_census_cancellation_runs_after_original_locks_are_released);
	UT_RUN(test_pool_observes_last_old_segment_before_original_cv);
	UT_RUN(test_pool_missing_phase_ack_identity_native_and_owner_stays_unproven);
	UT_RUN(test_pool_bad_candidate_does_not_block_independent_original_work);
	UT_RUN(test_pool_certificate_batch_limit_advances_original_cursor);
	UT_RUN(test_pool_rechecks_exact_phase_current_and_owner_after_physical_sample);
	UT_RUN(test_pool_never_turns_publisher_pending_or_horizon_into_reuse_authority);
	UT_RUN(test_pool_errors_keep_original_scur_and_admission_cleanup);

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
