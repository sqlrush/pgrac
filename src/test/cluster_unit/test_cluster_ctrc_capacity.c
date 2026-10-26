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
#include "cluster/cluster_undo_record_api.h"

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

#ifndef CLUSTER_CTRC_CAPACITY_POOL_CONTINUATION_V1
/* Exercise the parent observer unchanged before continuation support exists. */
static ClusterCapacityReasonV1
capacity_pool_without_continuation(uint32 segment, SCN horizon, uint64 epoch,
								   const ClusterCtrcTxnKeyV1 *hint pg_attribute_unused(),
								   ClusterCapacityObservationV1 *out)
{
	return cluster_ctrc_capacity_probe_pool_v1(segment, horizon, epoch, out);
}
#define cluster_ctrc_capacity_probe_pool_continuation_v1 capacity_pool_without_continuation
#endif

#ifndef CLUSTER_CTRC_CAPACITY_SEGMENT_V1
#define cluster_ctrc_capacity_probe_pool_segment_v1 cluster_ctrc_capacity_probe_pool_continuation_v1
#endif

#ifndef CLUSTER_UNDO_RETAIN_REASONS_V1
typedef enum ClusterUndoRetainReason {
	CLUSTER_UNDO_RETAIN_ACTIVE_OR_PREPARED = 1 << 0,
	CLUSTER_UNDO_RETAIN_RECORD_WRITER = 1 << 1,
	CLUSTER_UNDO_RETAIN_SEAL_OR_DRAIN = 1 << 2,
	CLUSTER_UNDO_RETAIN_HORIZON_MISSING = 1 << 3,
	CLUSTER_UNDO_RETAIN_HISTORY_PIN = 1 << 4,
	CLUSTER_UNDO_RETAIN_REFERENCE_PENDING = 1 << 5,
	CLUSTER_UNDO_RETAIN_RELEASE_NOT_DURABLE = 1 << 6,
	CLUSTER_UNDO_RETAIN_PUBLISHER_PENDING = 1 << 7,
	CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO = 1 << 8
} ClusterUndoRetainReason;

/* Parent's actual bool runs unchanged; it has no diagnostic output yet. */
static bool
retention_without_reasons(const UndoSegmentHeaderData *header, SCN horizon, bool peer,
						  uint32 *reasons)
{
	if (reasons != NULL)
		*reasons = 0;
	return cluster_undo_segment_recyclable_for_mode(header, horizon, peer);
}
#define cluster_undo_segment_recyclable_for_mode_with_reasons retention_without_reasons
#endif

static uint32
observed_retain_reasons(const ClusterCapacityObservationV1 *observation)
{
#ifdef CLUSTER_CTRC_CAPACITY_RETAIN_REASONS_V1
	return observation->retain_reasons;
#else
	(void)observation;
	return 0;
#endif
}

int cluster_ges_request_timeout_ms = 3000;
static UndoSegmentHeaderData sample_header;
static bool sample_commit, sample_abort, sample_active, sample_admitted;
static bool sample_recheck;
static unsigned sample_enters, sample_leaves, sample_holds, sample_releases;
static unsigned sample_admission_debt;
bool cluster_undo_cleaner_enabled = true;
bool cluster_enabled = true;
bool cluster_undo_retention_horizon_enabled = true;
static bool sample_maintenance_cut;
static uint32 sample_record_segment = 1;
static uint32 sample_current_segment = 1;
static void (*sample_current_read_hook)(void);
static bool sample_native_drift;
static unsigned sample_native_calls;
static TransactionId sample_native_order[64];
static void (*sample_release_hook)(void);
static PGAlignedBlock certificate_resident;
static bool certificate_current, certificate_pin, certificate_error_after_flush;
static unsigned certificate_wal_calls, certificate_flush_calls;
static bool sample_copy_error;
static bool sample_release_failure;
static bool pool_owner_available;
static uint64 pool_owner_generation, pool_coordinator_generation;
static unsigned pool_owner_calls;
static ClusterCtrcTxnKeyV1 gc_keys[TT_SLOTS_PER_SEGMENT];
static ClusterTTSlotCurrentOwner gc_owners[TT_SLOTS_PER_SEGMENT];
static uint64 gc_bindings[TT_SLOTS_PER_SEGMENT];
static bool gc_available[TT_SLOTS_PER_SEGMENT];
static unsigned gc_snapshot_calls;
static bool segment_inputs, segment_root_drift, segment_generation_drift;
static uint32 segment_physical_id, segment_requested_id;
static uint64 segment_current_binding;
static unsigned segment_root_calls, segment_generation_calls;
static unsigned segment_reads[CLUSTER_UNDO_SEGS_PER_INSTANCE];
static TransactionId segment_native_xids[TT_SLOTS_PER_SEGMENT];
static uint8 segment_native_states[TT_SLOTS_PER_SEGMENT];

/* Original allocator bytes are a boundary in this physical-reader fixture.
 * Their real publication/lifetime is tested by test_cluster_tt_active_owner. */
extern bool cluster_tt_slot_current_origin_snapshot(uint32 segment, uint16 offset,
													ClusterCtrcTxnKeyV1 *key,
													ClusterTTSlotCurrentOwner *owner,
													uint64 *binding);
bool
cluster_tt_slot_current_origin_snapshot(uint32 segment, uint16 offset, ClusterCtrcTxnKeyV1 *key,
										ClusterTTSlotCurrentOwner *owner, uint64 *binding)
{
	if (held_count != 0 || key == NULL || owner == NULL || binding == NULL)
		abort();
	MemSet(key, 0, sizeof(*key));
	MemSet(owner, 0, sizeof(*owner));
	*binding = 0;
	gc_snapshot_calls++;
	if (segment != sample_current_segment || offset >= TT_SLOTS_PER_SEGMENT
		|| !gc_available[offset])
		return false;
	*key = gc_keys[offset];
	*owner = gc_owners[offset];
	*binding = gc_bindings[offset];
	return true;
}

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

/* Existing optional-GC policy and record cursor are independent inputs.
 * No boundary supplies the original segment predicate's result. */
bool
cluster_normal_stop_maintenance_cut(void)
{
	if (held_count != 0)
		abort();
	return sample_maintenance_cut;
}

uint32
cluster_undo_record_active_segment_id(void)
{
	if (held_count != 0)
		abort();
	return sample_record_segment;
}

uint32
cluster_tt_slot_current_segment(int node pg_attribute_unused())
{
	if (sample_current_read_hook != NULL)
		sample_current_read_hook();
	return sample_current_segment;
}

bool
cluster_tt_slot_current_snapshot(int node, ClusterTTSlotCurrentSnapshot *out)
{
	if (held_count != 0 || out == NULL)
		abort();
	MemSet(out, 0, sizeof(*out));
	out->segment_id = cluster_tt_slot_current_segment(node);
	out->binding_generation = segment_current_binding;
	return true;
}

static bool
segment_native_is(TransactionId xid, uint8 status)
{
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		if (segment_native_xids[i] == xid)
			return segment_native_states[i] == status;
	return false;
}

bool
TransactionIdDidCommit(TransactionId xid)
{
	if (sample_native_calls < lengthof(sample_native_order))
		sample_native_order[sample_native_calls] = xid;
	sample_native_calls++;
	if (segment_inputs) {
		bool committed = segment_native_is(xid, TT_SLOT_COMMITTED);
		return sample_native_drift && sample_native_calls > 1 ? !committed : committed;
	}
	return sample_native_drift && sample_native_calls > 1 ? !sample_commit : sample_commit;
}

bool
TransactionIdDidAbort(TransactionId xid)
{
	if (segment_inputs)
		return segment_native_is(xid, TT_SLOT_ABORTED);
	return sample_abort;
}

bool
TransactionIdIsInProgress(TransactionId xid)
{
	if (segment_inputs)
		return segment_native_is(xid, TT_SLOT_ACTIVE);
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
	token->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	token->side = CLUSTER_SEMANTIC_TARGET_SIDE;
	token->entered = true;
	sample_admission_debt++;
	sample_enters++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	/* The real leave retires debt, then destroys the complete entered token. */
	if (token == NULL || !token->entered)
		return;
	if (sample_admission_debt == 0)
		abort();
	sample_admission_debt--;
	sample_leaves++;
	MemSet(token, 0, sizeof(*token));
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
	if (segment_inputs) {
		segment_root_calls++;
		if (segment_root_drift && segment_root_calls > 1)
			out->root_generation++;
	}
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
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
	int timeout_ms pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused(),
	ClusterUndoBlock0CurrentGuard *guard pg_attribute_unused(),
	ClusterUndoBlock0Result *failure pg_attribute_unused())
{
	if (mode != CLUSTER_UNDO_BLOCK0_SCUR || held_count != 0)
		abort();
	sample_holds++;
	if (segment_inputs) {
		segment_requested_id = key->segment_id;
		segment_reads[(key->segment_id - 1) % CLUSTER_UNDO_SEGS_PER_INSTANCE]++;
	}
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
	return sample_release_failure ? CLUSTER_UNDO_BLOCK0_CURRENT_FAILED
								  : CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
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
	if (segment_inputs && segment_generation_drift && ++segment_generation_calls > 1)
		out->value++;
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
	if (segment_inputs && segment_requested_id != segment_physical_id)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
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
	MemSet(sample_native_order, 0, sizeof(sample_native_order));
	sample_release_hook = NULL;
	sample_current_read_hook = NULL;
	sample_record_segment = 1;
	sample_maintenance_cut = false;
	cluster_undo_cleaner_enabled = cluster_undo_retention_horizon_enabled = true;
	certificate_current = certificate_pin = certificate_error_after_flush = false;
	certificate_wal_calls = certificate_flush_calls = 0;
	sample_copy_error = false;
	sample_release_failure = false;
	pool_owner_available = true;
	pool_owner_generation = 71;
	pool_coordinator_generation = 83;
	pool_owner_calls = 0;
	MemSet(gc_keys, 0, sizeof(gc_keys));
	MemSet(gc_owners, 0, sizeof(gc_owners));
	MemSet(gc_bindings, 0, sizeof(gc_bindings));
	MemSet(gc_available, 0, sizeof(gc_available));
	gc_snapshot_calls = 0;
	segment_inputs = segment_root_drift = segment_generation_drift = false;
	segment_current_binding = 9;
	segment_root_calls = segment_generation_calls = 0;
	MemSet(segment_reads, 0, sizeof(segment_reads));
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
	ClusterCtrcOriginCertificateSnapshot certificate;
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
	UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT(cluster_ctrc_origin_begin_cleaning_entry(origin));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CLEANING);
	UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CLEANING);
	UT_ASSERT_EQ(origin->ack_bitmap, 0);
	UT_ASSERT_EQ(sample_holds, 2);
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

/* Build OPEN through the real producer, stopping before the cleaner seal.
 * Reuse only the existing fixture's key construction, never its later phase. */
static bool
seed_pool_open(unsigned index, unsigned participants)
{
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity identity = { 0 };
	uint32 grant;

	MemSet(origin, 0, sizeof(*origin));
	key = seed_origin(index, 0)->key;
	MemSet(origin, 0, sizeof(*origin));
	if (cluster_ctrc_origin_open_active(origin, &key, 7) != CLUSTER_CTRC_ORIGIN_OPENED)
		return false;
	origin->original_full_xid = FullTransactionIdFromEpochAndXid(7, key.xid);
	for (unsigned node = 0; node < participants; node++) {
		identity.node_id = node;
		identity.boot_incarnation = 1101 + node;
		identity.capability_record_generation = test_capability_generation;
		identity.formation_epoch = 23;
		identity.admission_record_generation = 29;
		if (cluster_ctrc_origin_record_touched(origin, &identity, CTRC_PROOF_ACTIVE, &grant)
			!= CLUSTER_CTRC_TOUCH_RECORDED)
			return false;
	}
	return origin->state == CTRC_ORIGIN_OPEN && origin->seal_generation == 0;
}

UT_TEST(test_pool_observes_original_open_terminal_before_seal)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	for (unsigned scenario = 0; scenario < 4; scenario++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;
		ClusterCtrcLocalReleaseAckV1 acks_before[4];
		UndoSegmentHeaderData header_before;
		uint64 stats_before[CTRC_STAT_COUNT];
		uint64 seal_generation;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_open(index, scenario % 2 == 0 ? 0 : 4));
		before = ctrc_origin_entries()[index];
		if (scenario % 2 != 0)
			sample_header.tt_slots[before.key.slot_offset].first_undo_block.raw[0] = 1;
		if (scenario >= 2) {
			sample_header.tt_slots[before.key.slot_offset].status = TT_SLOT_ABORTED;
			sample_header.tt_slots[before.key.slot_offset].commit_scn = InvalidScn;
			sample_commit = false;
			sample_abort = true;
		}
		header_before = sample_header;
		memcpy(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before));
		seal_generation = CtrcShared->global_seal_generation;
		for (int stat = 0; stat < CTRC_STAT_COUNT; stat++)
			stats_before[stat] = cluster_ctrc_stat_get(stat);
		/* H is below commit; the existing seal edge does not wait for H. */
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_OPEN);
		UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
		UT_ASSERT_EQ(memcmp(&observation.key, &before.key, sizeof(before.key)), 0);
		UT_ASSERT_EQ(observation.pool_positions,
					 CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
		UT_ASSERT_EQ(observation.owner_worker, 7);
		UT_ASSERT_EQ(observation.owner_generation, 71);
		UT_ASSERT_EQ(observation.coordinator_generation, 83);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
		UT_ASSERT_EQ(
			memcmp(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before)), 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(CtrcShared->global_seal_generation, seal_generation);
		for (int stat = 0; stat < CTRC_STAT_COUNT; stat++)
			UT_ASSERT_EQ(cluster_ctrc_stat_get(stat), stats_before[stat]);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(certificate_flush_calls, 0);
		UT_ASSERT_EQ(sample_holds, 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_open_observation_cannot_seal_publish_or_grant_reuse)
{
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginCertificateSnapshot certificate;
	ClusterCtrcTxnKeyV1 selected;
	ClusterCtrcOriginEntry *origin;
	unsigned index = 0;

	(void)reset_pool_sample(index);
	UT_ASSERT(seed_pool_open(index, 0));
	origin = &ctrc_origin_entries()[index];
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SLOT);
	UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
	UT_ASSERT(!ctrc_cleaner_publish_certificate(&certificate));
	UT_ASSERT_EQ(certificate_wal_calls, 0);
	sample_header.tt_slots[0].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, 0));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	sample_header.tt_slots[0].flags = 0;
	/* Only the original producer allocates a seal generation and advances. */
	UT_ASSERT(cluster_ctrc_origin_next_open_shared(&selected));
	UT_ASSERT_EQ(memcmp(&selected, &origin->key, sizeof(selected)), 0);
	UT_ASSERT(ctrc_cleaner_terminal_sample_exact(&selected, NULL, NULL));
	UT_ASSERT(cluster_ctrc_origin_begin_seal_shared(&selected));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_SEALING);
	UT_ASSERT_EQ(origin->seal_generation, 1);
	UT_ASSERT_EQ(CtrcShared->global_seal_generation, 2);
	UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
	(void)ctrc_origin_dispatchable_locked(origin);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&certificate));
	UT_ASSERT(ctrc_cleaner_publish_certificate(&certificate));
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT(cluster_ctrc_reuse_handoff_complete(&sample_header, 0));
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(held_count, 0);
}

static void
invalidate_pool_open(unsigned fault, ClusterCtrcOriginEntry *origin, uint64 ack_index)
{
	switch (fault) {
	case 0:
		origin->grant_generation = 0;
		break;
	case 1:
		origin->seal_generation = 1;
		break;
	case 2:
		origin->close_dispatched_bitmap = 1;
		break;
	case 3:
		origin->close_confirmed_bitmap = 1;
		break;
	case 4:
		origin->ack_bitmap = 1;
		break;
	case 5:
		origin->close_request_id[0] = 501;
		break;
	case 6:
		origin->touched_count++;
		break;
	case 7:
		origin->touched[0].boot_incarnation++;
		/* A different but structurally valid incarnation is not malformed. */
		origin->touched[0].capability_record_generation = 0;
		break;
	case 8:
		origin->touched[1].boot_incarnation = 1102;
		break;
	case 9:
		origin->reserved8[0] = 1;
		break;
	case 10:
		CtrcShared->global_seal_generation = UINT64_MAX;
		break;
	case 11:
		CtrcShared->global_seal_generation = 0;
		break;
	case 12:
		ctrc_origin_ack_entries()[ack_index].crc32c = 1;
		break;
	case 13:
		origin->key.xid++;
		break;
	case 14:
		origin->key.slot_wrap++;
		break;
	case 15:
		origin->state = CTRC_ORIGIN_BLOCKED;
		break;
	}
}

UT_TEST(test_pool_open_missing_preseal_evidence_retains_all_bytes)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned fault = 0; fault < 16; fault++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry *origin, before;
		ClusterCtrcLocalReleaseAckV1 acks_before[4];
		UndoSegmentHeaderData header_before;
		uint64 seal_generation;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_open(index, 1));
		origin = &ctrc_origin_entries()[index];
		invalidate_pool_open(fault, origin, index * 4);
		before = *origin;
		header_before = sample_header;
		seal_generation = CtrcShared->global_seal_generation;
		memcpy(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before));
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT(observation.unproven_reasons != 0);
		UT_ASSERT_EQ(memcmp(&before, origin, sizeof(before)), 0);
		UT_ASSERT_EQ(
			memcmp(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before)), 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(CtrcShared->global_seal_generation, seal_generation);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_open_requires_original_terminal_sampler_and_runnable_owner)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned fault = 0; fault < 17; fault++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;
		TTSlot *slot;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_open(index, 1));
		before = ctrc_origin_entries()[index];
		slot = &sample_header.tt_slots[before.key.slot_offset];
		switch (fault) {
		case 0:
			sample_commit = false;
			break;
		case 1:
			sample_active = true;
			break;
		case 2:
			sample_abort = true;
			break;
		case 3:
			sample_admitted = false;
			break;
		case 4:
			sample_recheck = false;
			break;
		case 5:
			sample_native_drift = true;
			break;
		case 6:
			slot->xid++;
			break;
		case 7:
			slot->wrap++;
			break;
		case 8:
			slot->status = TT_SLOT_ACTIVE;
			break;
		case 9:
			slot->commit_scn = InvalidScn;
			break;
		case 10:
			sample_header.segment_id++;
			break;
		case 11:
			sample_header.owner_instance++;
			break;
		case 12:
			sample_header.wrap_count++;
			break;
		case 13:
			slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
			break;
		case 14:
			slot->flags = 0x80;
			break;
		case 15:
			slot->status = TT_SLOT_ABORTED;
			break;
		case 16:
			pool_owner_available = false;
			break;
		}
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT(observation.unproven_reasons != 0);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static unsigned open_drift_kind;
static void
open_change_during_scur_release(void)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	ClusterCtrcParticipantIdentity participant = origin->touched[0];
	uint32 grant;

	if (held_count != 0)
		abort();
	switch (open_drift_kind) {
	case 0:
		if (!cluster_ctrc_origin_begin_seal_shared(&origin->key))
			abort();
		break;
	case 1:
		origin->key.xid++;
		break;
	case 2:
		origin->touched[0].boot_incarnation++;
		break;
	case 3:
		participant.node_id = 1;
		participant.boot_incarnation = 1102;
		if (cluster_ctrc_origin_record_touched(origin, &participant, CTRC_PROOF_ACTIVE, &grant)
			!= CLUSTER_CTRC_TOUCH_RECORDED)
			abort();
		break;
	case 4:
		origin->grant_generation++;
		break;
	case 5:
		ctrc_origin_ack_entries()[index * 4].crc32c = 1;
		break;
	case 6:
		CtrcShared->global_seal_generation = UINT64_MAX;
		break;
	case 7:
		CtrcShared->global_seal_generation = 0;
		break;
	case 8:
		pool_owner_generation++;
		break;
	case 9:
		pool_coordinator_generation++;
		break;
	case 10:
		sample_current_segment = 2;
		break;
	case 11:
		pool_owner_available = false;
		break;
	case 12:
		CtrcShared->global_seal_generation++;
		break;
	}
}

UT_TEST(test_pool_open_rechecks_original_phase_membership_generation_and_owner)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (open_drift_kind = 0; open_drift_kind < 13; open_drift_kind++) {
		ClusterCapacityObservationV1 observation;
		ClusterCapacityReasonV1 expected = CLUSTER_CAPACITY_SUPPLY_RECHECK;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_open(index, 1));
		sample_release_hook = open_change_during_scur_release;
		if (open_drift_kind == 11)
			expected = CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
		else if (open_drift_kind == 12)
			expected = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 expected);
		UT_ASSERT(sample_release_hook == NULL);
		UT_ASSERT_EQ(sample_holds, 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_open_shares_original_cursor_and_sixteen_total_samples)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	UndoSegmentHeaderData good_header;
	unsigned holds_before;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(17);
	UT_ASSERT(seed_pool_open(17, 1));
	good_header = sample_header;
	for (unsigned index = 1; index <= 16; index++) {
		switch (index % 5) {
		case 0:
			(void)ctrc_origin_dispatchable_locked(seed_origin(index, 0));
			break;
		case 1:
			UT_ASSERT(seed_pool_open(index, 1));
			break;
		case 2:
			UT_ASSERT(seed_pool_seal_ready(index, true));
			break;
		case 3:
			UT_ASSERT(seed_pool_seal_ready(index, false));
			break;
		case 4:
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
	UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_OPEN);
	UT_ASSERT_EQ(sample_holds - holds_before, 17);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_bad_open_does_not_hide_independent_certificate_work)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginEntry before;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(2);
	UT_ASSERT(seed_pool_open(1, 1));
	ctrc_origin_entries()[1].grant_generation = 0;
	before = ctrc_origin_entries()[1];
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[1], sizeof(before)), 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_open_error_keeps_original_sampler_cleanup)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned participants = 0; participants < 2; participants++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;
		volatile bool caught = false;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_open(index, participants));
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

/* Keep the actual frozen ACK producer, then construct the origin's complete
 * membership before its seal. Only original close/ACK edges select a phase. */
static bool
seed_pool_close_work(unsigned index, unsigned stage, bool partial_ack)
{
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	ClusterCtrcCloseDispatch dispatch;
	uint64 participant, receipt;
	unsigned participants = partial_ack ? 2 : 1;

	MemSet(origin, 0, sizeof(*origin));
	if (!seed_frozen_certificate(index, &dispatch, &participant, &receipt)
		|| !seed_pool_open(index, participants)
		|| !cluster_ctrc_origin_begin_seal_entry(origin, 41 + index))
		return false;
	if (partial_ack
		&& (!cluster_ctrc_origin_arm_close_entry(origin, 0, 501)
			|| !cluster_ctrc_origin_ack_land_shared(501,
													&ctrc_participant_ack_entries()[participant])))
		return false;
	if (stage != 0) {
		for (unsigned node = partial_ack ? 1 : 0; node < participants; node++)
			if (!cluster_ctrc_origin_arm_close_entry(origin, node, 501 + node)
				|| !cluster_ctrc_origin_note_close_reply_entry(origin, node, 501 + node,
															   CTRC_SEAL_REPLY_PENDING_DRAIN))
				return false;
	}
	if (stage == 2 && !cluster_ctrc_origin_begin_cleaning_entry(origin))
		return false;
	return origin->state
			   == (stage == 0	? CTRC_ORIGIN_SEALING
				   : stage == 1 ? CTRC_ORIGIN_SEALED
								: CTRC_ORIGIN_CLEANING)
		   && origin->ack_bitmap != origin->touched_bitmap;
}

UT_TEST(test_pool_observes_original_close_work_with_missing_required_ack)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	for (unsigned stage = 0; stage < 3; stage++) {
		for (unsigned scenario = 0; scenario < 4; scenario++) {
			ClusterCapacityObservationV1 observation;
			ClusterCtrcOriginEntry before;
			ClusterCtrcLocalReleaseAckV1 acks_before[4];
			UndoSegmentHeaderData header_before;
			uint64 stats_before[CTRC_STAT_COUNT];
			uint64 request_generation;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_close_work(index, stage, scenario % 2 != 0));
			before = ctrc_origin_entries()[index];
			sample_header.tt_slots[before.key.slot_offset].first_undo_block.raw[0] = 1;
			if (scenario >= 2) {
				sample_header.tt_slots[before.key.slot_offset].status = TT_SLOT_ABORTED;
				sample_header.tt_slots[before.key.slot_offset].commit_scn = InvalidScn;
				sample_commit = false;
				sample_abort = true;
			}
			header_before = sample_header;
			memcpy(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before));
			request_generation = CtrcShared->global_request_generation;
			for (int stat = 0; stat < CTRC_STAT_COUNT; stat++)
				stats_before[stat] = cluster_ctrc_stat_get(stat);
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
			UT_ASSERT_EQ(observation.phase, before.state);
			UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
			UT_ASSERT_EQ(memcmp(&observation.key, &before.key, sizeof(before.key)), 0);
			UT_ASSERT_EQ(observation.pool_positions,
						 CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
			UT_ASSERT_EQ(observation.owner_worker, 7);
			UT_ASSERT_EQ(observation.owner_generation, 71);
			UT_ASSERT_EQ(observation.coordinator_generation, 83);
			UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
			UT_ASSERT_EQ(
				memcmp(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before)), 0);
			UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
			UT_ASSERT_EQ(CtrcShared->global_request_generation, request_generation);
			for (int stat = 0; stat < CTRC_STAT_COUNT; stat++)
				UT_ASSERT_EQ(cluster_ctrc_stat_get(stat), stats_before[stat]);
			UT_ASSERT_EQ(certificate_wal_calls, 0);
			UT_ASSERT_EQ(certificate_flush_calls, 0);
			UT_ASSERT_EQ(sample_holds, 1);
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

UT_TEST(test_pool_close_observation_preserves_actual_dispatch_and_certificate_gates)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned stage = 0; stage < 3; stage++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginCertificateSnapshot certificate;
		ClusterCtrcCloseDispatch dispatch, retry;
		ClusterCtrcOriginEntry *origin;
		uint64 request_generation;

		(void)reset_pool_sample(index);
		UT_ASSERT(seed_pool_close_work(index, stage, true));
		origin = &ctrc_origin_entries()[index];
		request_generation = CtrcShared->global_request_generation;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT_EQ(CtrcShared->global_request_generation, request_generation);
		UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
		UT_ASSERT(!ctrc_cleaner_publish_certificate(&certificate));
		sample_header.tt_slots[origin->key.slot_offset].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(!cluster_ctrc_reuse_handoff_complete(&sample_header, origin->key.slot_offset));
		sample_header.tt_slots[origin->key.slot_offset].flags = 0;
		UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
		UT_ASSERT_EQ(dispatch.suboperation, CTRC_SEAL_CLOSE_AND_CLEAN);
		UT_ASSERT_EQ(memcmp(&dispatch.key, &origin->key, sizeof(dispatch.key)), 0);
		UT_ASSERT_EQ(dispatch.participant.node_id, 1);
		UT_ASSERT_EQ(dispatch.participant.boot_incarnation, 1102);
		UT_ASSERT_EQ(dispatch.request_id, stage == 0 ? request_generation : 502);
		UT_ASSERT_EQ(origin->state, stage == 0 ? CTRC_ORIGIN_SEALING : CTRC_ORIGIN_CLEANING);
		request_generation = CtrcShared->global_request_generation;
		UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&retry));
		UT_ASSERT_EQ(memcmp(&dispatch, &retry, sizeof(dispatch)), 0);
		UT_ASSERT_EQ(CtrcShared->global_request_generation, request_generation);
		UT_ASSERT_EQ(origin->ack_bitmap, 1);
		UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
		/* Even an impossible CERTIFYING phase cannot replace the missing ACK. */
		origin->state = CTRC_ORIGIN_CERTIFYING;
		UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(index, &certificate));
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(certificate_flush_calls, 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_close_needs_new_request_capacity_only_for_unarmed_work)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned stage = 0; stage < 3; stage++) {
		for (unsigned armed = 0; armed < 2; armed++) {
			ClusterCapacityObservationV1 observation;
			ClusterCtrcCloseDispatch dispatch;
			ClusterCtrcOriginEntry before;
			ClusterCapacityReasonV1 expected = stage != 0 || armed != 0
												   ? CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE
												   : CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
			uint64 refusals;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_close_work(index, stage, false));
			if (stage == 0 && armed != 0)
				UT_ASSERT(
					cluster_ctrc_origin_arm_close_entry(&ctrc_origin_entries()[index], 0, 501));
			before = ctrc_origin_entries()[index];
			CtrcShared->global_request_generation = UINT64_MAX;
			refusals = CtrcShared->full_refusal_count;
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				expected);
			UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
			UT_ASSERT_EQ(CtrcShared->full_refusal_count, refusals);
			CtrcShared->global_request_generation = 0;
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
			CtrcShared->global_request_generation = UINT64_MAX;
			UT_ASSERT_EQ(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch),
						 expected == CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
			if (expected == CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE)
				UT_ASSERT_EQ(dispatch.request_id, 501);
			else {
				UT_ASSERT_EQ(ctrc_origin_entries()[index].state, CTRC_ORIGIN_BLOCKED);
				UT_ASSERT_EQ(CtrcShared->full_refusal_count, refusals + 1);
			}
			UT_ASSERT_EQ(CtrcShared->global_request_generation, UINT64_MAX);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

static void
invalidate_pool_close_ack(unsigned fault, ClusterCtrcOriginEntry *origin, uint64 ack_index)
{
	switch (fault) {
	case 0:
		ctrc_origin_ack_entries()[ack_index].crc32c ^= 1;
		break;
	case 1:
		MemSet(&ctrc_origin_ack_entries()[ack_index], 0, sizeof(ClusterCtrcLocalReleaseAckV1));
		break;
	case 2:
		origin->ack_bitmap = 0;
		break;
	case 3:
		origin->ack_bitmap |= 2;
		break;
	case 4:
		origin->close_dispatched_bitmap &= ~UINT32_C(1);
		break;
	case 5:
		origin->close_confirmed_bitmap &= ~UINT32_C(1);
		break;
	case 6:
		origin->close_request_id[0] = 0;
		break;
	case 7:
		origin->close_request_id[3] = 999;
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
		origin->seal_generation++;
		break;
	case 12:
		origin->seal_generation = 0;
		break;
	case 13:
		origin->seal_generation = UINT64_MAX;
		break;
	case 14:
		origin->reserved8[0] = 1;
		break;
	case 15:
		origin->key.xid++;
		break;
	case 16:
		ctrc_origin_ack_entries()[ack_index + 1].crc32c = 1;
		break;
	case 17:
		origin->close_dispatched_bitmap |= 8;
		break;
	case 18:
		origin->close_confirmed_bitmap |= 8;
		break;
	case 19:
		origin->ack_bitmap |= 8;
		break;
	case 20:
		origin->touched[3].boot_incarnation = 1104;
		break;
	case 21:
		origin->state = CTRC_ORIGIN_BLOCKED;
		break;
	}
}

UT_TEST(test_pool_close_partial_ack_and_metadata_must_be_exact)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned stage = 0; stage < 3; stage++) {
		for (unsigned fault = 0; fault < 22; fault++) {
			ClusterCapacityObservationV1 observation;
			ClusterCtrcOriginEntry before;
			ClusterCtrcLocalReleaseAckV1 acks_before[4];
			UndoSegmentHeaderData header_before;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_close_work(index, stage, true));
			invalidate_pool_close_ack(fault, &ctrc_origin_entries()[index], index * 4);
			before = ctrc_origin_entries()[index];
			header_before = sample_header;
			memcpy(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before));
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
			UT_ASSERT(observation.unproven_reasons != 0);
			UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
			UT_ASSERT_EQ(
				memcmp(acks_before, &ctrc_origin_ack_entries()[index * 4], sizeof(acks_before)), 0);
			UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
			UT_ASSERT_EQ(certificate_wal_calls, 0);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

static void
invalidate_pool_close_sample(unsigned fault, TTSlot *slot)
{
	switch (fault) {
	case 0:
		sample_commit = false;
		break;
	case 1:
		sample_active = true;
		break;
	case 2:
		sample_abort = true;
		break;
	case 3:
		sample_admitted = false;
		break;
	case 4:
		sample_recheck = false;
		break;
	case 5:
		sample_native_drift = true;
		break;
	case 6:
		slot->xid++;
		break;
	case 7:
		slot->wrap++;
		break;
	case 8:
		slot->status = TT_SLOT_ACTIVE;
		break;
	case 9:
		slot->commit_scn = InvalidScn;
		break;
	case 10:
		sample_header.wrap_count++;
		break;
	case 11:
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		break;
	case 12:
		slot->flags = 0x80;
		break;
	case 13:
		pool_owner_available = false;
		break;
	case 14:
		slot->status = TT_SLOT_ABORTED;
		break;
	}
}

UT_TEST(test_pool_close_requires_canonical_native_admission_and_original_owner)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned stage = 0; stage < 3; stage++) {
		for (unsigned fault = 0; fault < 15; fault++) {
			ClusterCapacityObservationV1 observation;
			ClusterCtrcOriginEntry before;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_close_work(index, stage, true));
			before = ctrc_origin_entries()[index];
			invalidate_pool_close_sample(fault, &sample_header.tt_slots[before.key.slot_offset]);
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
			UT_ASSERT(observation.unproven_reasons != 0);
			UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[index], sizeof(before)), 0);
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
			UT_ASSERT_EQ(certificate_wal_calls, 0);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

static unsigned close_drift_kind;
static void
close_change_during_scur_release(void)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	uint64 participant;

	if (held_count != 0)
		abort();
	switch (close_drift_kind) {
	case 0:
		if (!ctrc_participant_index(&origin->key, 0, &participant)
			|| !cluster_ctrc_origin_arm_close_entry(origin, 0, 501)
			|| !cluster_ctrc_origin_ack_land_shared(501,
													&ctrc_participant_ack_entries()[participant]))
			abort();
		break;
	case 1:
		origin->seal_generation++;
		break;
	case 2:
		origin->close_request_id[0]++;
		break;
	case 3:
		ctrc_origin_ack_entries()[index * 4 + 1].crc32c = 1;
		break;
	case 4:
		origin->key.xid++;
		break;
	case 5:
		pool_owner_generation++;
		break;
	case 6:
		pool_coordinator_generation++;
		break;
	case 7:
		sample_current_segment = 2;
		break;
	case 8:
		CtrcShared->global_request_generation = UINT64_MAX;
		break;
	case 9:
		CtrcShared->global_request_generation = 0;
		break;
	case 10:
		CtrcShared->global_request_generation++;
		break;
	case 11:
		pool_owner_available = false;
		break;
	}
}

UT_TEST(test_pool_close_rechecks_real_ack_phase_requests_and_owner_after_sample)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned stage = 0; stage < 3; stage++) {
		for (close_drift_kind = 0; close_drift_kind < 12; close_drift_kind++) {
			ClusterCapacityObservationV1 observation;
			ClusterCapacityReasonV1 expected = CLUSTER_CAPACITY_SUPPLY_RECHECK;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_close_work(index, stage, false));
			sample_release_hook = close_change_during_scur_release;
			if (close_drift_kind == 10 || (close_drift_kind == 8 && stage != 0))
				expected = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
			else if (close_drift_kind == 11)
				expected = CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;
			UT_ASSERT_EQ(
				cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				expected);
			UT_ASSERT(sample_release_hook == NULL);
			UT_ASSERT_EQ(sample_holds, 1);
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
			UT_ASSERT_EQ(held_count, 0);
		}
	}
}

UT_TEST(test_pool_close_work_shares_all_phase_cursor_and_total_sample_budget)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	UndoSegmentHeaderData good_header;
	unsigned holds_before;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(17);
	UT_ASSERT(seed_pool_close_work(17, 2, true));
	good_header = sample_header;
	for (unsigned index = 1; index <= 16; index++) {
		switch (index % 8) {
		case 0:
			(void)ctrc_origin_dispatchable_locked(seed_origin(index, 0));
			break;
		case 1:
			UT_ASSERT(seed_pool_open(index, 1));
			break;
		case 2:
			UT_ASSERT(seed_pool_seal_ready(index, true));
			break;
		case 3:
			UT_ASSERT(seed_pool_seal_ready(index, false));
			break;
		case 4:
			UT_ASSERT(seed_pool_notification(index));
			break;
		case 5:
			UT_ASSERT(seed_pool_close_work(index, 0, true));
			break;
		case 6:
			UT_ASSERT(seed_pool_close_work(index, 1, true));
			break;
		case 7:
			UT_ASSERT(seed_pool_close_work(index, 2, true));
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
	UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_CLEANING);
	UT_ASSERT_EQ(sample_holds - holds_before, 17);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_bad_close_ack_does_not_hide_independent_original_work)
{
	ClusterCtrcOriginCertificateSnapshot calibration;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginEntry before;
	ClusterCtrcLocalReleaseAckV1 ack_before;

	(void)reset_capacity_sample();
	UT_ASSERT(ctrc_origin_next_certificate_snapshot_shared(&calibration));
	key = reset_pool_sample(2);
	UT_ASSERT(seed_pool_close_work(1, 0, true));
	ctrc_origin_ack_entries()[4].crc32c ^= 1;
	before = ctrc_origin_entries()[1];
	ack_before = ctrc_origin_ack_entries()[4];
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[1], sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&ack_before, &ctrc_origin_ack_entries()[4], sizeof(ack_before)), 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_close_error_keeps_original_sampler_cleanup)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned stage = 0; stage < 3; stage++) {
		for (unsigned partial = 0; partial < 2; partial++) {
			ClusterCapacityObservationV1 observation;
			ClusterCtrcOriginEntry before;
			volatile bool caught = false;

			(void)reset_pool_sample(index);
			UT_ASSERT(seed_pool_close_work(index, stage, partial != 0));
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
}

/* The actual publisher removes the empty-required origin. There is no
 * origin phase setter or positive release verdict supplied to the observer. */
static bool
seed_current_gc_supply(unsigned index, bool committed)
{
	ClusterCtrcTxnKeyV1 key = reset_pool_sample(index);
	ClusterCtrcOriginCertificateSnapshot snapshot;
	unsigned slot = key.slot_offset;

	sample_current_segment = key.segment_id;
	if (!committed) {
		sample_commit = false;
		sample_abort = true;
		sample_header.tt_slots[slot].status = TT_SLOT_ABORTED;
		sample_header.tt_slots[slot].commit_scn = InvalidScn;
	}
	if (!ctrc_origin_certificate_snapshot_index_locked(index, &snapshot)
		|| !ctrc_cleaner_publish_certificate(&snapshot)
		|| !ctrc_bytes_zero(&ctrc_origin_entries()[index], sizeof(ClusterCtrcOriginEntry)))
		return false;
	gc_keys[slot] = key;
	gc_owners[slot].segment_id = key.segment_id;
	gc_owners[slot].slot_offset = slot;
	gc_owners[slot].xid = key.xid;
	gc_owners[slot].wrap = key.slot_wrap;
	gc_owners[slot].status = committed ? CTS_COMMITTED : CTS_ABORTED;
	gc_owners[slot].commit_scn = committed ? 1000 : InvalidScn;
	gc_bindings[slot] = 17;
	gc_available[slot] = true;
	return true;
}

UT_TEST(test_current_gc_discovers_original_release_after_origin_reclamation)
{
	for (unsigned variant = 0; variant < 8; variant++) {
		unsigned index = (variant & 4 ? 255 * 48 : 0) + (variant & 2 ? 47 : 0);
		unsigned slot = index % TT_SLOTS_PER_SEGMENT;
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		UndoSegmentHeaderData before;
		uint64 stats[CTRC_STAT_COUNT];
		unsigned holds;

		UT_ASSERT(seed_current_gc_supply(index, (variant & 1) != 0));
		before = sample_header;
		holds = sample_holds;
		for (unsigned stat = 0; stat < CTRC_STAT_COUNT; stat++)
			stats[stat] = cluster_ctrc_stat_get(stat);
		/* Inclusive H equals commit; no retained certificate or armed key. */
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(sample_current_segment, 1000,
														 test_cluster_epoch, &continuation),
					 CLUSTER_CTRC_CAPACITY_WAIT);
		UT_ASSERT_EQ(memcmp(&continuation, &gc_keys[slot], sizeof(continuation)), 0);
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(memcmp(&before, &sample_header, sizeof(before)), 0);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[index], sizeof(ClusterCtrcOriginEntry)));
		UT_ASSERT_EQ(gc_bindings[slot], 17);
		for (unsigned stat = 0; stat < CTRC_STAT_COUNT; stat++)
			UT_ASSERT_EQ(stats[stat], cluster_ctrc_stat_get(stat));
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static SCN
gc_invalid_input(unsigned fault)
{
	SCN horizon = 1000;

	switch (fault) {
	case 0:
		gc_available[0] = false;
		break;
	case 1:
		gc_owners[0].status = CTS_ACTIVE;
		break;
	case 2:
		gc_owners[0].status = CTS_FREE;
		break;
	case 3:
		gc_owners[0].wrap = TT_WRAP_MAX;
		gc_keys[0].slot_wrap = TT_WRAP_MAX;
		break;
	case 4:
		horizon = 999;
		break;
	case 5:
		gc_owners[0].xid++;
		break;
	case 6:
		gc_owners[0].slot_offset++;
		break;
	case 7:
		gc_owners[0].segment_id++;
		break;
	case 8:
		gc_bindings[0] = 0;
		break;
	case 9:
		gc_bindings[0] = UINT64_MAX;
		break;
	case 10:
		gc_keys[0].root_generation++;
		break;
	case 11:
		gc_keys[0].reserved[0] = 1;
		break;
	case 12:
		ctrc_origin_entries()[0].state = CTRC_ORIGIN_BLOCKED;
		break;
	case 13:
		sample_header.tt_slots[0].flags = 0;
		break;
	case 14:
		sample_header.tt_slots[0].status = TT_SLOT_ABORTED;
		sample_header.tt_slots[0].commit_scn = 0;
		sample_commit = false;
		sample_abort = true;
		break;
	case 15:
		sample_header.tt_slots[0].commit_scn = 999;
		break;
	case 16:
		sample_active = true;
		break;
	case 17:
		sample_commit = false;
		break;
	case 18:
		sample_abort = true;
		break;
	case 19:
		sample_native_drift = true;
		break;
	case 20:
		sample_admitted = false;
		break;
	case 21:
		sample_recheck = false;
		break;
	case 22:
		sample_header.wrap_count++;
		break;
	case 23:
		sample_header.tt_slots[0].first_undo_block.raw[0] = 1;
		break;
	case 24:
		gc_keys[0].admission_record_generation++;
		break;
	case 25:
		gc_owners[0].status = CTS_ABORTED;
		break;
	case 26:
		gc_owners[0].wrap++;
		break;
	}
	return horizon;
}

UT_TEST(test_current_gc_missing_source_release_horizon_or_terminal_is_not_supply)
{
	for (unsigned fault = 0; fault < 27; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterCtrcOriginEntry before;
		UndoSegmentHeaderData header_before;
		SCN horizon = 1000;

		UT_ASSERT(seed_current_gc_supply(0, true));
		horizon = gc_invalid_input(fault);
		before = ctrc_origin_entries()[0];
		header_before = sample_header;
		UT_ASSERT_EQ(
			cluster_ctrc_capacity_probe_current(1, horizon, test_cluster_epoch, &continuation),
			CLUSTER_CTRC_CAPACITY_REFUSE);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

static unsigned gc_drift;
static void
gc_change_after_sample(void)
{
	switch (gc_drift) {
	case 0:
		gc_bindings[0]++;
		break;
	case 1:
		gc_owners[0].xid++;
		break;
	case 2:
		gc_keys[0].root_generation++;
		break;
	case 3:
		gc_available[0] = false;
		break;
	case 4:
		gc_owners[0].commit_scn++;
		break;
	case 5:
		sample_current_segment = 2;
		break;
	case 6:
		test_cluster_epoch++;
		break;
	case 7:
		if (cluster_ctrc_origin_open_active(&ctrc_origin_entries()[0], &gc_keys[0], 7)
			!= CLUSTER_CTRC_ORIGIN_OPENED)
			abort();
		break;
	case 8:
		gc_owners[47].xid++;
		break;
	}
}

UT_TEST(test_current_gc_rechecks_original_key_entry_binding_and_handoff)
{
	for (gc_drift = 0; gc_drift < 9; gc_drift++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		unsigned holds;

		UT_ASSERT(seed_current_gc_supply(0, true));
		holds = sample_holds;
		sample_release_hook = gc_change_after_sample;
		UT_ASSERT_EQ(
			cluster_ctrc_capacity_probe_current(1, 1000, test_cluster_epoch, &continuation),
			gc_drift == 6	? CLUSTER_CTRC_CAPACITY_REFUSE
			: gc_drift == 8 ? CLUSTER_CTRC_CAPACITY_WAIT
							: CLUSTER_CTRC_CAPACITY_RETRY);
		UT_ASSERT_EQ(ctrc_bytes_zero(&continuation, sizeof(continuation)), gc_drift != 8);
		UT_ASSERT(sample_release_hook == NULL);
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_current_gc_and_certificates_share_original_per_position_budget)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	unsigned holds;

	UT_ASSERT(seed_current_gc_supply(0, true));
	for (unsigned slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++) {
		gc_keys[slot] = gc_keys[0];
		gc_keys[slot].slot_offset = slot;
		gc_keys[slot].xid = gc_keys[0].xid + slot;
		gc_owners[slot] = gc_owners[0];
		gc_owners[slot].slot_offset = slot;
		gc_owners[slot].xid = gc_keys[slot].xid;
		gc_bindings[slot] = 17;
		gc_available[slot] = true;
		sample_header.tt_slots[slot] = sample_header.tt_slots[0];
		sample_header.tt_slots[slot].xid = gc_keys[slot].xid;
		sample_header.tt_slots[slot].flags = slot == 47 ? TT_SLOT_FLAG_CTRC_RELEASE_PROVEN : 0;
		/* Even positions take the real certificate path and fail canonical
		 * identity; odd positions take GC. No position may do both reads. */
		if (slot < 47 && (slot & 1) == 0) {
			ClusterCtrcOriginEntry *origin = seed_origin(slot, 0);
			(void)ctrc_origin_dispatchable_locked(origin);
			sample_header.tt_slots[slot].xid++;
		}
	}
	holds = sample_holds;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 1000, test_cluster_epoch, &continuation),
				 CLUSTER_CTRC_CAPACITY_WAIT);
	UT_ASSERT_EQ(continuation.slot_offset, 47);
	UT_ASSERT_EQ(sample_holds, holds + TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_current_gc_error_preserves_original_current_and_admission_cleanup)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		volatile bool caught = false;
		unsigned holds;

		UT_ASSERT(seed_current_gc_supply(0, committed != 0));
		holds = sample_holds;
		sample_copy_error = true;
		PG_TRY();
		{
			(void)cluster_ctrc_capacity_probe_current(1, 1000, test_cluster_epoch, &continuation);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(gc_bindings[0], 17);
		UT_ASSERT_EQ(held_count, 0);
	}
}

/* Calibrate through the original selector, never by writing its cursor. */
static ClusterCtrcTxnKeyV1
reset_continuation_pool(unsigned index)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;

	(void)reset_capacity_sample();
	if (!ctrc_origin_next_certificate_snapshot_shared(&snapshot))
		abort();
	return reset_pool_sample(index);
}

static void
continuation_sample_slot(const ClusterCtrcTxnKeyV1 *key)
{
	TTSlot *slot = &sample_header.tt_slots[key->slot_offset];

	slot->xid = key->xid;
	slot->wrap = key->slot_wrap;
	slot->status = TT_SLOT_COMMITTED;
	slot->commit_scn = 1000;
}

UT_TEST(test_pool_continuation_outside_batch_is_reproved_within_original_budget)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(index), before = hint;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginEntry origin_before = ctrc_origin_entries()[index];
	UndoSegmentHeaderData header_before = sample_header;

	for (unsigned i = 1; i <= 16; i++)
		(void)ctrc_origin_dispatchable_locked(seed_origin(i, 0));
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch, &hint,
																  &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &hint, sizeof(hint)), 0);
	UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
	UT_ASSERT_EQ(sample_holds, 16);
	UT_ASSERT_EQ(CtrcCertificateCursor, 16);
	UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
	UT_ASSERT_EQ(memcmp(&origin_before, &ctrc_origin_entries()[index], sizeof(origin_before)), 0);
	UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_pool_continuation_never_preempts_ordinary_runnable_work)
{
	ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95);
	ClusterCtrcOriginEntry *ordinary = seed_origin(48, 0);
	ClusterCapacityObservationV1 observation;

	(void)ctrc_origin_dispatchable_locked(ordinary);
	continuation_sample_slot(&ordinary->key);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch, &hint,
																  &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&observation.key, &ordinary->key, sizeof(ordinary->key)), 0);
	UT_ASSERT_EQ(sample_holds, 1);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_pool_continuation_is_sampled_once_last_and_keeps_fair_successor)
{
	ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(55);
	ClusterCapacityObservationV1 observation;
	unsigned occurrences = 0;

	for (unsigned i = 48; i <= 64; i++) {
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[i];

		if (i != 55)
			(void)ctrc_origin_dispatchable_locked(seed_origin(i, 0));
		continuation_sample_slot(&origin->key);
	}
	sample_commit = false;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch, &hint,
																  &observation),
				 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_EQ(sample_holds, 16);
	/* The original native bracket samples twice per physical candidate. */
	UT_ASSERT_EQ(sample_native_calls, 32);
	UT_ASSERT_EQ(sample_native_order[31], hint.xid);
	for (unsigned i = 0; i < sample_native_calls; i++)
		occurrences += sample_native_order[i] == hint.xid;
	UT_ASSERT_EQ(occurrences, 2);
	UT_ASSERT_EQ(CtrcCertificateCursor, 64);
	sample_commit = true;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(observation.key.xid, ctrc_origin_entries()[64].key.xid);
	UT_ASSERT_EQ(sample_holds, 17);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_pool_continuation_reclaimed_or_replaced_key_rechecks_without_cached_proof)
{
	for (unsigned replaced = 0; replaced < 2; replaced++) {
		unsigned index = TT_SLOTS_PER_SEGMENT * 3;
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(index), before = hint;
		ClusterCtrcOriginCertificateSnapshot snapshot;
		ClusterCapacityObservationV1 observation;
		unsigned holds;

		UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(index, &snapshot));
		UT_ASSERT(ctrc_cleaner_publish_certificate(&snapshot));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[index], sizeof(ClusterCtrcOriginEntry)));
		if (replaced) {
			ClusterCtrcTxnKeyV1 replacement = hint;

			replacement.xid++;
			UT_ASSERT_EQ(
				cluster_ctrc_origin_open_active(&ctrc_origin_entries()[index], &replacement, 7),
				CLUSTER_CTRC_ORIGIN_OPENED);
		}
		holds = sample_holds;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch,
																	  &hint, &observation),
					 CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
		/* The retired old hint now spends its one reserved GC sample;
		 * the replacement still spends one ordinary sample. Neither grants reuse. */
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

UT_TEST(test_pool_continuation_bad_hint_cannot_authorize_or_hide_independent_work)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95), before;
		ClusterCtrcOriginEntry *ordinary = seed_origin(48, 0);
		ClusterCapacityObservationV1 observation;

		(void)ctrc_origin_dispatchable_locked(ordinary);
		continuation_sample_slot(&ordinary->key);
		switch (fault) {
		case 0:
			hint.format_version++;
			break;
		case 1:
			hint.origin_node_id = 1;
			hint.owner_instance = 2;
			hint.segment_id = 257;
			break;
		case 2:
			hint.cluster_epoch--;
			break;
		case 3:
			hint.xid++;
			break;
		case 4:
			MemSet(&hint, 0, sizeof(hint));
			break;
		}
		before = hint;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch,
																	  &hint, &observation),
					 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT_EQ(memcmp(&observation.key, &ordinary->key, sizeof(ordinary->key)), 0);
		UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
		UT_ASSERT_EQ(sample_holds, 1);
	}
}

UT_TEST(test_pool_continuation_rebuilds_actual_phase_and_preserves_exact_drift_checks)
{
	unsigned index = TT_SLOTS_PER_SEGMENT * 3;
	for (unsigned phase = 0; phase < 2; phase++) {
		for (pool_drift_kind = 0; pool_drift_kind < 6; pool_drift_kind++) {
			ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(index);
			ClusterCapacityObservationV1 observation;

			if (phase == 0)
				UT_ASSERT(seed_pool_open(index, 1));
			else
				UT_ASSERT(seed_pool_close_work(index, 0, true));
			if (pool_drift_kind < 5)
				sample_release_hook = pool_change_during_scur_release;
			UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(
							 1, 500, test_cluster_epoch, &hint, &observation),
						 pool_drift_kind < 5 ? CLUSTER_CAPACITY_SUPPLY_RECHECK
											 : CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
			UT_ASSERT_EQ(observation.phase, phase == 0 ? CTRC_ORIGIN_OPEN : CTRC_ORIGIN_SEALING);
			UT_ASSERT_EQ(sample_holds, 1);
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
		}
	}
}

UT_TEST(test_pool_continuation_invalid_hint_cannot_bypass_sixteen_ordinary_candidates)
{
	unsigned index = CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT - 1;
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(index);
		ClusterCapacityObservationV1 observation;

		for (unsigned i = 1; i <= 16; i++)
			(void)ctrc_origin_dispatchable_locked(seed_origin(i, 0));
		switch (fault) {
		case 0:
			hint.format_version++;
			break;
		case 1:
			hint.origin_node_id = 1;
			hint.owner_instance = 2;
			hint.segment_id = 257;
			break;
		case 2:
			hint.cluster_epoch--;
			break;
		case 3:
			hint.root_id = 0;
			break;
		}
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch,
																	  &hint, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT_EQ(sample_holds, 16);
		UT_ASSERT_EQ(CtrcCertificateCursor, 17);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

UT_TEST(test_pool_continuation_error_keeps_original_sampler_and_hint_cleanup)
{
	ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95), before = hint;
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginEntry origin_before = ctrc_origin_entries()[95];
	volatile bool caught = false;

	sample_copy_error = true;
	PG_TRY();
	{
		(void)cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch, &hint,
															   &observation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
	UT_ASSERT_EQ(memcmp(&origin_before, &ctrc_origin_entries()[95], sizeof(origin_before)), 0);
	UT_ASSERT_EQ(sample_holds, 1);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(held_count, 0);
}

/* Change only the allocator/epoch/owner boundary after an actual inventory
 * unlock or SCUR release. The observer still runs its complete real scan. */
static uint32 final_scope_current;
static unsigned final_scope_unlocks;
static bool final_scope_epoch_change, final_scope_owner_missing;

static void
pool_final_scope_change(void)
{
	if (held_count != 0 || certificate_current)
		abort();
	sample_current_segment = final_scope_current;
	if (final_scope_epoch_change)
		test_cluster_epoch++;
	if (final_scope_owner_missing)
		pool_owner_available = false;
}

static bool
pool_final_scope_unlock(LWLock *lock, LWLockMode mode pg_attribute_unused(), bool acquiring)
{
	if (acquiring || lock != &CtrcShared->origin_lock || --final_scope_unlocks != 0)
		return false;
	test_lock_leave(lock);
	external_lock_hook = NULL;
	pool_final_scope_change();
	return true;
}

static void
arm_pool_final_scope_change(uint32 current, unsigned unlocks, bool epoch, bool owner_missing)
{
	final_scope_current = current;
	final_scope_unlocks = unlocks;
	final_scope_epoch_change = epoch;
	final_scope_owner_missing = owner_missing;
	if (unlocks != 0)
		external_lock_hook = pool_final_scope_unlock;
	else
		sample_release_hook = pool_final_scope_change;
}

UT_TEST(test_pool_empty_census_rechecks_current_at_actual_scan_boundaries)
{
	const unsigned unlocks[] = { 1, 96, 192 };
	const uint32 currents[] = { 2, 128, 256 };

	for (unsigned i = 0; i < lengthof(unlocks); i++) {
		ClusterCapacityObservationV1 observation;
		uint64 cursor;

		(void)reset_continuation_pool(95);
		MemSet(&ctrc_origin_entries()[95], 0, sizeof(ClusterCtrcOriginEntry));
		cursor = CtrcCertificateCursor;
		arm_pool_final_scope_change(currents[i], unlocks[i], false, false);
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(observation.reason, CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(observation.pool_positions, CTRC_CLEANER_ORIGINS);
		UT_ASSERT_EQ(CtrcCertificateCursor, (cursor + 1) % CTRC_CLEANER_ORIGINS);
		UT_ASSERT(external_lock_hook == NULL);
		UT_ASSERT_EQ(sample_current_segment, currents[i]);
		UT_ASSERT(ctrc_bytes_zero(&observation.key, sizeof(observation.key)));
		UT_ASSERT_EQ(sample_holds, 0);
		UT_ASSERT_EQ(sample_enters, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_failed_canonical_sample_rechecks_current_after_release)
{
	for (unsigned phase = 0; phase < 4; phase++) {
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;
		UndoSegmentHeaderData header_before;
		unsigned holds;

		(void)reset_continuation_pool(95);
		if (phase == 1)
			UT_ASSERT(seed_pool_open(95, 1));
		if (phase == 2)
			UT_ASSERT(seed_pool_close_work(95, 0, true));
		if (phase == 3)
			UT_ASSERT(seed_pool_notification(95));
		before = ctrc_origin_entries()[95];
		header_before = sample_header;
		holds = sample_holds;
		sample_commit = false;
		arm_pool_final_scope_change(256, 0, false, false);
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
					 CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_CANONICAL) != 0);
		UT_ASSERT(sample_release_hook == NULL);
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[95], sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&header_before, &sample_header, sizeof(header_before)), 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_owner_refusal_rechecks_current_without_canonical_acquisition)
{
	ClusterCapacityObservationV1 observation;
	ClusterCtrcOriginEntry before;

	(void)reset_continuation_pool(95);
	before = ctrc_origin_entries()[95];
	arm_pool_final_scope_change(2, 192, false, true);
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 500, test_cluster_epoch, &observation),
				 CLUSTER_CAPACITY_SUPPLY_RECHECK);
	UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_OWNER) != 0);
	UT_ASSERT(external_lock_hook == NULL);
	UT_ASSERT_EQ(observation.pool_positions, CTRC_CLEANER_ORIGINS);
	UT_ASSERT_EQ(sample_holds, 0);
	UT_ASSERT_EQ(sample_enters, 0);
	UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[95], sizeof(before)), 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_pool_invalid_final_current_cannot_use_a_reclaimed_hint_recheck)
{
	const uint32 currents[] = { 0, CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, UINT32_MAX };

	for (unsigned i = 0; i < lengthof(currents); i++) {
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95), before = hint;
		ClusterCapacityObservationV1 observation;

		MemSet(&ctrc_origin_entries()[95], 0, sizeof(ClusterCtrcOriginEntry));
		arm_pool_final_scope_change(currents[i], 192, false, false);
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, test_cluster_epoch,
																	  &hint, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_IDENTITY) != 0);
		UT_ASSERT_EQ(observation.reason, CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
		UT_ASSERT(external_lock_hook == NULL);
		UT_ASSERT_EQ(sample_holds, 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_final_epoch_failure_precedes_current_or_hint_recheck)
{
	for (unsigned reclaimed = 0; reclaimed < 2; reclaimed++) {
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95), before = hint;
		ClusterCapacityObservationV1 observation;
		uint64 epoch = test_cluster_epoch;

		if (reclaimed)
			MemSet(&ctrc_origin_entries()[95], 0, sizeof(ClusterCtrcOriginEntry));
		arm_pool_final_scope_change(reclaimed ? 0 : 2, 192, true, false);
		UT_ASSERT_EQ(
			cluster_ctrc_capacity_probe_pool_continuation_v1(1, 500, epoch, &hint, &observation),
			CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_IDENTITY) != 0);
		UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
		UT_ASSERT(external_lock_hook == NULL);
		UT_ASSERT_EQ(sample_holds, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_unchanged_final_scope_preserves_independent_owner_precedence)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95), before = hint;
		ClusterCapacityObservationV1 observation;
		ClusterCapacityReasonV1 expected = CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER;

		if (mode < 3)
			MemSet(&ctrc_origin_entries()[95], 0, sizeof(ClusterCtrcOriginEntry));
		if (mode == 1)
			expected = CLUSTER_CAPACITY_SUPPLY_RECHECK;
		if (mode == 2) {
			ClusterCtrcOriginEntry *ordinary = seed_origin(48, 0);

			(void)ctrc_origin_dispatchable_locked(ordinary);
			continuation_sample_slot(&ordinary->key);
			expected = CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
		}
		if (mode == 3)
			pool_owner_available = false;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(
						 1, 500, test_cluster_epoch, mode == 0 ? NULL : &hint, &observation),
					 expected);
		UT_ASSERT_EQ(sample_current_segment, 1);
		UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
		UT_ASSERT_EQ(sample_holds, mode == 1 || mode == 2 ? 1 : 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static unsigned final_scope_current_reads;
static void
pool_epoch_changes_during_final_current_read(void)
{
	if (++final_scope_current_reads == 2) {
		sample_current_read_hook = NULL;
		test_cluster_epoch++;
	}
}

UT_TEST(test_pool_final_epoch_check_follows_the_current_snapshot)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterCtrcTxnKeyV1 hint = reset_continuation_pool(95), before = hint;
		ClusterCapacityObservationV1 observation;
		uint64 epoch = test_cluster_epoch;

		if (mode != 1)
			MemSet(&ctrc_origin_entries()[95], 0, sizeof(ClusterCtrcOriginEntry));
		final_scope_current_reads = 0;
		sample_current_read_hook = pool_epoch_changes_during_final_current_read;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(
						 1, 500, epoch, mode == 2 ? &hint : NULL, &observation),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_IDENTITY) != 0);
		UT_ASSERT(sample_current_read_hook == NULL);
		UT_ASSERT_EQ(final_scope_current_reads, 2);
		UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(hint)), 0);
		UT_ASSERT_EQ(sample_holds, mode != 0 ? 1 : 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

/* Retire through the actual publisher, retaining only the caller's old key.
 * Every terminal slot is independently checked by the real retention object. */
static ClusterCtrcTxnKeyV1
reset_retired_pool_gc(unsigned index)
{
	ClusterCtrcTxnKeyV1 key = reset_continuation_pool(index);
	ClusterCtrcOriginCertificateSnapshot snapshot;

	if (!ctrc_origin_certificate_snapshot_index_locked(index, &snapshot)
		|| !ctrc_cleaner_publish_certificate(&snapshot)
		|| !ctrc_bytes_zero(&ctrc_origin_entries()[index], sizeof(ClusterCtrcOriginEntry)))
		abort();
	sample_header.segment_state = SEGMENT_COMMITTED;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		TTSlot *slot = &sample_header.tt_slots[i];

		slot->xid = i == key.slot_offset ? key.xid : 10000 + i;
		slot->wrap = i == key.slot_offset ? key.slot_wrap : 1;
		slot->status = i == key.slot_offset || i % 2 == 0 ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		slot->commit_scn = slot->status == TT_SLOT_COMMITTED ? 1000 : InvalidScn;
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	}
	return key;
}

UT_TEST(test_pool_gc_retired_key_observes_original_whole_segment_work)
{
	const unsigned indices[] = { 48, 6144, CTRC_CLEANER_ORIGINS - 1 };

	for (unsigned i = 0; i < lengthof(indices); i++) {
		ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(indices[i]), before = key;
		ClusterCapacityObservationV1 observation;
		UndoSegmentHeaderData header_before = sample_header;
		unsigned holds = sample_holds;
		uint64 progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);

		for (unsigned pass = 0; pass < 3; pass++) {
			UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(
							 1, 1000, test_cluster_epoch, &key, &observation),
						 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
			UT_ASSERT_EQ(observation.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
			UT_ASSERT_EQ(observation.phase, CTRC_ORIGIN_EMPTY);
			UT_ASSERT_EQ(memcmp(&observation.key, &key, sizeof(key)), 0);
			UT_ASSERT_EQ(observation.owner_worker, (key.segment_id - 1) % 8);
			UT_ASSERT_EQ(observation.coordinator_generation, pool_coordinator_generation);
			UT_ASSERT_EQ(observation.owner_generation, pool_owner_generation);
			UT_ASSERT_EQ(observation.pool_positions, CTRC_CLEANER_ORIGINS);
			UT_ASSERT_EQ(sample_holds, holds + pass + 1);
		}
		UT_ASSERT_EQ(memcmp(&key, &before, sizeof(key)), 0);
		UT_ASSERT_EQ(memcmp(&sample_header, &header_before, sizeof(header_before)), 0);
		UT_ASSERT(
			ctrc_bytes_zero(&ctrc_origin_entries()[indices[i]], sizeof(ClusterCtrcOriginEntry)));
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

static void
pool_gc_corrupt_slot(unsigned fault)
{
	TTSlot *slot = &sample_header.tt_slots[46];

	switch (fault) {
	case 0:
		slot->status = TT_SLOT_ACTIVE;
		break;
	case 1:
		slot->flags = TT_FLAGS_RESERVED;
		break;
	case 2:
		slot->flags |= 0x80;
		break;
	case 3:
		slot->first_undo_block.raw[0] = 1;
		break;
	case 4:
		slot->xid = InvalidTransactionId;
		break;
	case 5:
		slot->wrap = TT_WRAP_INVALID;
		break;
	case 6:
		slot->commit_scn = InvalidScn;
		break;
	case 7:
		slot->commit_scn = 1001;
		break;
	case 8:
		slot->status = TT_SLOT_ABORTED;
		break;
	case 9:
		slot->status = 0xff;
		break;
	case 10:
		slot->status = TT_SLOT_UNUSED;
		break;
	case 11:
		slot->status = TT_SLOT_RECYCLABLE;
		break;
	case 12:
		sample_header.segment_state = SEGMENT_ACTIVE;
		break;
	case 13:
		sample_header.segment_state = SEGMENT_RECYCLABLE;
		break;
	case 14:
		sample_header.wrap_count++;
		break;
	case 15:
		sample_header.tt_slots_count--;
		break;
	case 16:
		sample_header.owner_instance++;
		break;
	case 17:
		sample_commit = false;
		break;
	case 18:
		sample_abort = true;
		break;
	case 19:
		sample_active = true;
		break;
	case 20:
		sample_native_drift = true;
		break;
	case 21:
		sample_admitted = false;
		break;
	case 22:
		sample_recheck = false;
		break;
	}
}

UT_TEST(test_pool_gc_single_release_never_proves_all_slots_or_native_scope)
{
	for (unsigned fault = 0; fault < 23; fault++) {
		ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(95), before = key;
		ClusterCapacityObservationV1 observation;
		UndoSegmentHeaderData header_before;
		unsigned holds = sample_holds;

		pool_gc_corrupt_slot(fault);
		header_before = sample_header;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch,
																	  &key, &observation),
					 CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT((observation.unproven_reasons & CLUSTER_CAPACITY_UNPROVEN_CANONICAL) != 0);
		UT_ASSERT_EQ(memcmp(&sample_header, &header_before, sizeof(header_before)), 0);
		UT_ASSERT_EQ(memcmp(&key, &before, sizeof(key)), 0);
		UT_ASSERT(sample_holds <= holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_gc_preserves_original_record_seal_and_strict_drain_horizon)
{
	const SCN seals[] = { 800, 800, 800, 800, 0, 1000 };
	const SCN drains[] = { 900, 1000, 1001, 799, 900, 1000 };

	for (unsigned i = 0; i < lengthof(seals); i++) {
		ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(95);
		ClusterCapacityObservationV1 observation;
		unsigned holds = sample_holds;

		sample_header.tail_block = 1;
		UndoSegmentHeader_set_record_seal_upper_scn(&sample_header, seals[i]);
		UndoSegmentHeader_set_record_drain_upper_scn(&sample_header, drains[i]);
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch,
																	  &key, &observation),
					 i == 0 ? CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE
							: CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

UT_TEST(test_pool_gc_requires_original_optional_policy_record_cursor_and_owner)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(95);
		ClusterCapacityObservationV1 observation;
		unsigned holds = sample_holds;

		switch (fault) {
		case 0:
			cluster_undo_cleaner_enabled = false;
			break;
		case 1:
			cluster_undo_retention_horizon_enabled = false;
			break;
		case 2:
			sample_maintenance_cut = true;
			break;
		case 3:
			sample_record_segment = 0;
			break;
		case 4:
			sample_record_segment = key.segment_id;
			break;
		case 5:
			sample_record_segment = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
			break;
		case 6:
			pool_owner_available = false;
			break;
		case 7:
			sample_current_segment = key.segment_id;
			break;
		case 8:
			sample_current_segment = 0;
			break;
		}
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch,
																	  &key, &observation),
					 fault == 8 ? CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER
								: CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(sample_holds, holds);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

static unsigned pool_gc_drift;
static ClusterCtrcTxnKeyV1 pool_gc_drift_key;
static void
pool_gc_changes_after_sample(void)
{
	if (held_count != 0 || certificate_current)
		abort();
	switch (pool_gc_drift) {
	case 0:
		sample_record_segment = 3;
		break;
	case 1:
		sample_record_segment = pool_gc_drift_key.segment_id;
		break;
	case 2:
		pool_owner_generation++;
		break;
	case 3:
		pool_coordinator_generation++;
		break;
	case 4:
		pool_owner_available = false;
		break;
	case 5:
		sample_current_segment = 256;
		break;
	case 6:
		sample_current_segment = 0;
		break;
	case 7:
		sample_maintenance_cut = true;
		break;
	case 8:
		cluster_undo_retention_horizon_enabled = false;
		break;
	case 9:
		cluster_undo_cleaner_enabled = false;
		break;
	case 10:
		(void)seed_origin(95, 0);
		break;
	case 11:
		test_cluster_epoch++;
		break;
	}
}

UT_TEST(test_pool_gc_rechecks_original_sources_after_its_single_physical_sample)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(95), before = key;
		ClusterCapacityObservationV1 observation;
		unsigned holds = sample_holds;
		uint64 epoch = test_cluster_epoch;

		pool_gc_drift = fault;
		pool_gc_drift_key = key;
		sample_release_hook = pool_gc_changes_after_sample;
		UT_ASSERT_EQ(
			cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, epoch, &key, &observation),
			fault == 6 || fault == 11 ? CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER
									  : CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT(sample_release_hook == NULL);
		UT_ASSERT_EQ(memcmp(&key, &before, sizeof(key)), 0);
		UT_ASSERT_EQ(sample_holds, holds + 1);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_pool_gc_uses_all_slot_original_publisher_handoff)
{
	for (unsigned released = 0; released < 2; released++) {
		ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(95);
		ClusterCtrcOriginEntry *neighbor = seed_origin(94, 0);
		ClusterCapacityObservationV1 observation;
		ClusterCtrcOriginEntry before;

		neighbor->state = released ? CTRC_ORIGIN_RELEASE_PROVEN : CTRC_ORIGIN_BLOCKED;
		sample_header.tt_slots[46].xid = neighbor->key.xid;
		sample_header.tt_slots[46].wrap = neighbor->key.slot_wrap;
		before = *neighbor;
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch,
																	  &key, &observation),
					 released ? CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE
							  : CLUSTER_CAPACITY_SUPPLY_RECHECK);
		UT_ASSERT_EQ(memcmp(&before, neighbor, sizeof(before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

UT_TEST(test_pool_gc_shares_last_hint_budget_and_yields_to_independent_work)
{
	ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(CTRC_CLEANER_ORIGINS - 1);
	ClusterCapacityObservationV1 observation;
	unsigned holds = sample_holds, occurrences = 0;

	for (unsigned i = 1; i <= 16; i++)
		(void)ctrc_origin_dispatchable_locked(seed_origin(i, 0));
	sample_native_calls = 0;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch, &key,
																  &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(sample_holds, holds + 16);
	UT_ASSERT_EQ(CtrcCertificateCursor, 16);
	UT_ASSERT_EQ(sample_native_order[sample_native_calls - 1], key.xid);
	for (unsigned i = 0; i < sample_native_calls; i++)
		occurrences += sample_native_order[i] == key.xid;
	UT_ASSERT_EQ(occurrences, 2);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	key = reset_retired_pool_gc(95);
	(void)ctrc_origin_dispatchable_locked(seed_origin(48, 0));
	continuation_sample_slot(&ctrc_origin_entries()[48].key);
	sample_header.tt_slots[0].flags = TT_FLAGS_RESERVED;
	holds = sample_holds;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch, &key,
																  &observation),
				 CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(observation.key.xid, ctrc_origin_entries()[48].key.xid);
	UT_ASSERT_EQ(sample_holds, holds + 1);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_pool_gc_error_keeps_original_sampler_cleanup_and_exact_hint)
{
	ClusterCtrcTxnKeyV1 key = reset_retired_pool_gc(95), before = key;
	ClusterCapacityObservationV1 observation;
	UndoSegmentHeaderData header_before = sample_header;
	unsigned holds = sample_holds;
	volatile bool caught = false;

	sample_copy_error = true;
	PG_TRY();
	{
		(void)cluster_ctrc_capacity_probe_pool_continuation_v1(1, 1000, test_cluster_epoch, &key,
															   &observation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(memcmp(&key, &before, sizeof(key)), 0);
	UT_ASSERT_EQ(memcmp(&sample_header, &header_before, sizeof(header_before)), 0);
	UT_ASSERT_EQ(sample_holds, holds + 1);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(held_count, 0);
}

/* Keep native input independent of subsequently corrupted canonical bytes.
 * The expected old key remains test-side and is never passed to the observer. */
static ClusterCtrcTxnKeyV1
reset_unarmed_segment(unsigned index)
{
	ClusterCtrcTxnKeyV1 expected = reset_retired_pool_gc(index);

	segment_inputs = true;
	segment_physical_id = expected.segment_id;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		segment_native_xids[i] = sample_header.tt_slots[i].xid;
		segment_native_states[i] = sample_header.tt_slots[i].status;
	}
#ifdef CLUSTER_CTRC_CAPACITY_SEGMENT_V1
	CtrcCapacitySegmentCursor = 0;
#endif
	return expected;
}

static ClusterCapacityReasonV1
probe_unarmed_segment(ClusterCapacityObservationV1 *out)
{
	return cluster_ctrc_capacity_probe_pool_segment_v1(1, 1000, test_cluster_epoch, NULL, out);
}

/* Compose the actual producer with the original cleaner consumer and wait.
 * Context/floor and CV scheduling are explicit boundaries; no boundary
 * supplies the observation or a precomputed capacity decision. */
static unsigned capacity_sleeps, capacity_cancels;
static bool capacity_subscribed, capacity_cancel;
static uint32 capacity_wait_event, capacity_sleep_event;
uint32 *my_wait_event_info = &capacity_wait_event;
int cluster_lmon_main_loop_interval = 2000;

static bool
undo_cleaner_capacity_context_safe(void)
{
	return held_count == 0 && !native_cleaner_lock_held && sample_admission_debt == 0
		   && sample_holds == sample_releases;
}

static bool
capacity_cleaner_lock(LWLock *lock, LWLockMode mode pg_attribute_unused(), bool acquire)
{
	if (lock != &native_cleaner_state.lwlock) {
		if (native_cleaner_lock_held)
			abort();
		return false;
	}
	if (held_count != 0 || native_cleaner_lock_held == acquire)
		abort();
	native_cleaner_lock_held = acquire;
	return true;
}

static bool
undo_cleaner_capacity_floor(ClusterUndoHorizonFloor *floor)
{
	floor->scn = 1000;
	floor->epoch = test_cluster_epoch;
	return true;
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{
	if (capacity_subscribed || !undo_cleaner_capacity_context_safe())
		abort();
	capacity_subscribed = true;
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(), long timeout, uint32 event)
{
	if (!capacity_subscribed || !undo_cleaner_capacity_context_safe()
		|| timeout != Max(cluster_lmon_main_loop_interval, 200))
		abort();
	capacity_sleeps++;
	capacity_sleep_event = event;
	pgstat_report_wait_start(event);
	if (capacity_cancel)
		pg_re_throw();
	pgstat_report_wait_end();
	return true;
}

bool
ConditionVariableCancelSleep(void)
{
	capacity_subscribed = false;
	capacity_cancels++;
	return true;
}

int
geterrcode(void)
{
	return capacity_cancel ? ERRCODE_QUERY_CANCELED : ERRCODE_INTERNAL_ERROR;
}

#include "test_cluster_ctrc_capacity_wait.inc"

static void
reset_segment_wait(void)
{
	(void)reset_unarmed_segment(48);
	cluster_enabled = true;
	external_lock_hook = capacity_cleaner_lock;
	MemSet(&native_cleaner_state, 0, sizeof(native_cleaner_state));
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		UndoCleanerWorkerState *worker = &native_cleaner_state.workers[i];
		worker->status = UNDO_CLEANER_READY;
		worker->latch = (Latch *)&native_cleaner_state;
		worker->incarnation_generation = i == 0 ? 83 : 71;
	}
	capacity_sleeps = capacity_cancels = 0;
	capacity_subscribed = capacity_cancel = false;
	capacity_wait_event = 41;
	capacity_sleep_event = 0;
}

UT_TEST(test_retired_admission_observation_reaches_original_cleaner_cv)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };

	reset_segment_wait();
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
	UT_ASSERT_EQ(capacity_sleeps, 1);
	UT_ASSERT_EQ(capacity_sleep_event, WAIT_EVENT_CLUSTER_UNDO_CAPACITY_WAIT);
	UT_ASSERT_EQ(capacity_cancels, 1);
	UT_ASSERT(!capacity_subscribed);
	UT_ASSERT_EQ(native_cleaner_state.capacity_wait_entered, 1);
	UT_ASSERT_EQ(native_cleaner_state.capacity_wait_repolled, 1);
	UT_ASSERT_EQ(native_cleaner_state.capacity_wait_refused_proof, 0);
	UT_ASSERT_EQ(capacity_wait_event, 41);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_original_cleaner_rejects_missing_retired_admission_identity)
{
	ClusterCapacityObservationV1 observation;
	unsigned worker = UINT_MAX;

	reset_segment_wait();
	UT_ASSERT_EQ(probe_unarmed_segment(&observation), CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT(undo_cleaner_capacity_segment_worker(1, test_cluster_epoch, &observation, &worker));
	UT_ASSERT_EQ(worker, 1);
	UT_ASSERT_EQ(observation.segment.formation_epoch, 23);
	UT_ASSERT_EQ(observation.segment.admission_record_generation, 29);
	observation.segment.formation_epoch = 0;
	UT_ASSERT(!undo_cleaner_capacity_segment_worker(1, test_cluster_epoch, &observation, &worker));
	observation.segment.formation_epoch = 23;
	observation.segment.admission_record_generation = 0;
	UT_ASSERT(!undo_cleaner_capacity_segment_worker(1, test_cluster_epoch, &observation, &worker));
	UT_ASSERT_EQ(capacity_sleeps, 0);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_holds, sample_releases);
}

UT_TEST(test_retired_admission_refusal_never_sleeps_or_retains_debt)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };

		reset_segment_wait();
		if (fault == 0)
			sample_admitted = false;
		if (fault == 1)
			sample_recheck = false;
		if (fault == 2)
			sample_release_failure = true;
		if (fault == 3)
			segment_root_drift = true;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(capacity_sleeps, 0);
		UT_ASSERT_EQ(capacity_cancels, 1);
		UT_ASSERT(!capacity_subscribed);
		UT_ASSERT_EQ(native_cleaner_state.capacity_wait_entered, 0);
		UT_ASSERT_EQ(native_cleaner_state.capacity_wait_refused_proof, 1);
		UT_ASSERT_EQ(sample_admission_debt, 0);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_retired_admission_error_and_cv_cancel_keep_original_cleanup)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		volatile bool caught = false;

		reset_segment_wait();
		sample_copy_error = fault == 0;
		capacity_cancel = fault == 1;
		PG_TRY();
		{
			(void)cluster_undo_cleaner_wait_for_capacity(1, &continuation);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(capacity_sleeps, fault);
		UT_ASSERT_EQ(capacity_cancels, 1);
		UT_ASSERT(!capacity_subscribed);
		UT_ASSERT_EQ(pg_atomic_read_u64(&native_cleaner_state.capacity_wait_cancelled), fault);
		UT_ASSERT_EQ(sample_admission_debt, 0);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_unarmed_segment_fair_walk_keeps_zero_key_and_sixteen_get_budget)
{
	const unsigned indices[] = { 48, 6144, CTRC_CLEANER_ORIGINS - 1 };

	for (unsigned i = 0; i < lengthof(indices); i++) {
		ClusterCtrcTxnKeyV1 expected = reset_unarmed_segment(indices[i]);
		ClusterCapacityObservationV1 out;
		UndoSegmentHeaderData before = sample_header;
		bool found = false;

		for (unsigned pass = 0; pass < 20; pass++) {
			unsigned holds = sample_holds;
			found = probe_unarmed_segment(&out) == CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
			UT_ASSERT(sample_holds - holds <= 16);
			if (found)
				break;
		}
		UT_ASSERT(found);
		UT_ASSERT(ctrc_bytes_zero(&out.key, sizeof(out.key)));
		UT_ASSERT_EQ(out.scope, CLUSTER_CAPACITY_SCOPE_SEGMENT);
		UT_ASSERT_EQ(out.phase, CTRC_ORIGIN_EMPTY);
		UT_ASSERT_EQ(out.owner_worker, (expected.segment_id - 1) % 8);
#ifdef CLUSTER_CTRC_CAPACITY_SEGMENT_V1
		UT_ASSERT_EQ(out.segment.segment_id, expected.segment_id);
		UT_ASSERT_EQ(out.segment.owner_instance, expected.owner_instance);
		UT_ASSERT_EQ(out.segment.segment_generation, expected.segment_generation);
		UT_ASSERT_EQ(out.segment.current_segment, 1);
		UT_ASSERT_EQ(out.segment.current_binding_generation, 9);
		UT_ASSERT_EQ(out.segment.epoch, test_cluster_epoch);
		UT_ASSERT_EQ(out.segment.root_id, expected.root_id);
		UT_ASSERT_EQ(out.segment.root_generation, expected.root_generation);
		UT_ASSERT_EQ(out.segment.system_identifier, expected.system_identifier);
		UT_ASSERT_EQ(out.segment.origin_boot_incarnation, expected.origin_boot_incarnation);
		UT_ASSERT_EQ(out.segment.formation_epoch, expected.formation_epoch);
		UT_ASSERT_EQ(out.segment.admission_record_generation, expected.admission_record_generation);
#endif
		for (unsigned s = 0; s < lengthof(segment_reads); s++)
			UT_ASSERT(segment_reads[s] <= 1);
		UT_ASSERT_EQ(sample_native_calls, 2 * TT_SLOTS_PER_SEGMENT);
		UT_ASSERT_EQ(memcmp(&sample_header, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_unarmed_segment_requires_all48_native_release_and_horizon)
{
	for (unsigned slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++) {
		for (unsigned fault = 0; fault < 5; fault++) {
			ClusterCapacityObservationV1 out;
			(void)reset_unarmed_segment(48);
			if (fault == 0)
				sample_header.tt_slots[slot].flags = 0;
			if (fault == 1)
				sample_header.tt_slots[slot].status = TT_SLOT_ACTIVE;
			if (fault == 2)
				segment_native_states[slot] = TT_SLOT_ACTIVE;
			if (fault == 3)
				sample_header.tt_slots[slot].first_undo_block.raw[0] = 1;
			if (fault == 4)
				sample_header.tt_slots[slot].commit_scn = 1001;
			UT_ASSERT(probe_unarmed_segment(&out) != CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
			UT_ASSERT(sample_holds <= 17); /* Publisher's one get plus pool16. */
			UT_ASSERT_EQ(sample_holds, sample_releases);
			UT_ASSERT_EQ(sample_enters, sample_leaves);
		}
	}
}

UT_TEST(test_unarmed_segment_keeps_policy_record_owner_and_canonical_gates)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterCapacityObservationV1 out;
		(void)reset_unarmed_segment(48);
		switch (fault) {
		case 0:
			cluster_undo_cleaner_enabled = false;
			break;
		case 1:
			cluster_undo_retention_horizon_enabled = false;
			break;
		case 2:
			sample_maintenance_cut = true;
			break;
		case 3:
			sample_record_segment = 2;
			break;
		case 4:
			sample_record_segment = 0;
			break;
		case 5:
			pool_owner_available = false;
			break;
		case 6:
			sample_admitted = false;
			break;
		case 7:
			sample_recheck = false;
			break;
		case 8:
			sample_header.owner_instance++;
			break;
		case 9:
			sample_header.segment_id++;
			break;
		case 10:
			sample_header.tt_slots_count--;
			break;
		case 11:
			sample_header.segment_state = SEGMENT_ACTIVE;
			break;
		case 12:
			sample_header.wrap_count = 0;
			break;
		case 13:
			sample_header.wrap_count = UINT32_MAX;
			break;
		}
		UT_ASSERT(probe_unarmed_segment(&out) != CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

static unsigned segment_drift;
static unsigned segment_scope_reads;
static void
unarmed_segment_binding_changes_at_pool_finish(void)
{
	if (++segment_scope_reads == 5) {
		segment_current_binding++;
		sample_current_read_hook = NULL;
	}
}

static void
unarmed_segment_changes_after_sample(void)
{
	if (segment_drift == 0)
		segment_current_binding++;
	if (segment_drift == 1)
		sample_current_segment = 3;
	if (segment_drift == 2)
		test_cluster_epoch++;
	if (segment_drift == 3)
		sample_record_segment = 2;
	if (segment_drift == 4)
		pool_owner_generation++;
	if (segment_drift == 5)
		pool_coordinator_generation++;
	if (segment_drift == 6)
		sample_recheck = false;
	if (segment_drift == 7)
		segment_root_drift = true;
}

UT_TEST(test_unarmed_segment_rechecks_identity_and_original_sources)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterCapacityObservationV1 out;
		(void)reset_unarmed_segment(48);
		segment_drift = fault;
		if (fault < 8)
			sample_release_hook = unarmed_segment_changes_after_sample;
		if (fault == 8)
			segment_root_drift = true;
		if (fault == 9)
			segment_generation_drift = true;
		if (fault == 10)
			sample_native_drift = true;
		if (fault == 11) {
			segment_scope_reads = 0;
			sample_current_read_hook = unarmed_segment_binding_changes_at_pool_finish;
		}
		UT_ASSERT(probe_unarmed_segment(&out) != CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
#ifdef CLUSTER_CTRC_CAPACITY_SEGMENT_V1
		UT_ASSERT(ctrc_bytes_zero(&out.segment, sizeof(out.segment)));
#endif
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
	test_cluster_epoch = 13;
}

UT_TEST(test_unarmed_segment_dense_failed_origins_cannot_starve_gc)
{
	ClusterCapacityObservationV1 out;
	bool found = false;
	(void)reset_unarmed_segment(6144);
	for (unsigned i = 0; i < 48; i++)
		(void)ctrc_origin_dispatchable_locked(seed_origin(i, 0));
	for (unsigned pass = 0; pass < 130; pass++) {
		unsigned holds = sample_holds;
		found = probe_unarmed_segment(&out) == CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE;
		UT_ASSERT(sample_holds - holds <= 16);
		if (found)
			break;
	}
	UT_ASSERT(found);
	UT_ASSERT(ctrc_bytes_zero(&out.key, sizeof(out.key)));
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

UT_TEST(test_unarmed_segment_error_cleans_original_guard_and_admission)
{
	ClusterCapacityObservationV1 out;
	volatile bool caught = false;
	unsigned holds;
	(void)reset_unarmed_segment(48);
	holds = sample_holds;
	sample_copy_error = true;
	PG_TRY();
	{
		(void)probe_unarmed_segment(&out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(sample_holds, holds + 1);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_segment_probe_shares_hint_budget_and_samples_exact_hint_last)
{
	ClusterCtrcTxnKeyV1 hint = reset_retired_pool_gc(CTRC_CLEANER_ORIGINS - 1), before = hint;
	ClusterCapacityObservationV1 out;
	unsigned holds = sample_holds;

	/* The original key sampler remains a separate path. Only this test's
	 * physical boundary permits every logical read, so the hint can succeed. */
	for (unsigned i = 0; i < 16; i++)
		(void)ctrc_origin_dispatchable_locked(seed_origin(i, 0));
	sample_native_calls = 0;
	UT_ASSERT_EQ(
		cluster_ctrc_capacity_probe_pool_segment_v1(1, 1000, test_cluster_epoch, &hint, &out),
		CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
	UT_ASSERT_EQ(memcmp(&out.key, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&hint, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(sample_native_order[sample_native_calls - 1], hint.xid);
	UT_ASSERT(sample_holds - holds <= 16);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
}

static UndoSegmentHeaderData
retain_reason_header(void)
{
	UndoSegmentHeaderData header;

	MemSet(&header, 0, sizeof(header));
	header.segment_state = SEGMENT_COMMITTED;
	header.segment_id = 2;
	header.owner_instance = 1;
	header.wrap_count = 1;
	header.tt_slots_count = TT_SLOTS_PER_SEGMENT;
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		header.tt_slots[i].status = TT_SLOT_COMMITTED;
		header.tt_slots[i].xid = 10000 + i;
		header.tt_slots[i].wrap = 1;
		header.tt_slots[i].commit_scn = 1000;
		header.tt_slots[i].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	}
	return header;
}

UT_TEST(test_retain_reasons_distinguish_history_missing_and_unfinished_work)
{
	const uint32 expected[]
		= { CLUSTER_UNDO_RETAIN_RELEASE_NOT_DURABLE, CLUSTER_UNDO_RETAIN_REFERENCE_PENDING,
			CLUSTER_UNDO_RETAIN_HORIZON_MISSING,	 CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,
			CLUSTER_UNDO_RETAIN_HISTORY_PIN,		 CLUSTER_UNDO_RETAIN_ACTIVE_OR_PREPARED,
			CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,		 CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,
			CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,		 CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,
			CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,		 CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,
			CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,		 CLUSTER_UNDO_RETAIN_IDENTITY_OR_IO,
			CLUSTER_UNDO_RETAIN_SEAL_OR_DRAIN,		 CLUSTER_UNDO_RETAIN_SEAL_OR_DRAIN,
			CLUSTER_UNDO_RETAIN_HISTORY_PIN,		 CLUSTER_UNDO_RETAIN_HISTORY_PIN,
			CLUSTER_UNDO_RETAIN_HORIZON_MISSING };

	for (unsigned fault = 0; fault < lengthof(expected); fault++) {
		UndoSegmentHeaderData header = retain_reason_header(), before;
		SCN horizon = 1000;
		uint32 reasons = UINT32_MAX;

		switch (fault) {
		case 0:
			header.tt_slots[0].flags = 0;
			break;
		case 1:
			header.tt_slots[0].first_undo_block.raw[0] = 1;
			break;
		case 2:
			horizon = InvalidScn;
			break;
		case 3:
			header.tt_slots[0].commit_scn = InvalidScn;
			break;
		case 4:
			header.tt_slots[0].commit_scn = 1001;
			break;
		case 5:
			header.tt_slots[0].status = TT_SLOT_ACTIVE;
			break;
		case 6:
			header.tt_slots[0].xid = InvalidTransactionId;
			break;
		case 7:
			header.tt_slots[0].wrap = TT_WRAP_INVALID;
			break;
		case 8:
			header.tt_slots[0].status = 0x7f;
			break;
		case 9:
			header.tt_slots[0].flags = 0x80;
			break;
		case 10:
			header.tt_slots[0].status = TT_SLOT_ABORTED;
			break;
		case 11:
			header.tt_slots[0].status = TT_SLOT_UNUSED;
			break;
		case 13:
			header.segment_state = SEGMENT_ACTIVE;
			break;
		case 14:
			header.tail_block = 1;
			break;
		case 15:
		case 16:
		case 17:
		case 18:
			header.tail_block = 1;
			UndoSegmentHeader_set_record_seal_upper_scn(&header, 900);
			UndoSegmentHeader_set_record_drain_upper_scn(&header, fault == 15	? 899
																  : fault == 17 ? 1001
																  : fault == 18 ? 950
																				: 1000);
			if (fault == 18)
				horizon = InvalidScn;
			break;
		}
		before = header;
		UT_ASSERT(!cluster_undo_segment_recyclable_for_mode_with_reasons(
			fault == 12 ? NULL : &header, horizon, true, &reasons));
		UT_ASSERT_EQ(reasons, expected[fault]);
		UT_ASSERT_EQ(memcmp(&header, &before, sizeof(header)), 0);
	}
}

UT_TEST(test_retain_reasons_preserve_peer_nonpeer_and_record_equality)
{
	for (unsigned peer = 0; peer < 2; peer++) {
		UndoSegmentHeaderData header = retain_reason_header();
		uint32 reasons = UINT32_MAX;

		UT_ASSERT_EQ(cluster_undo_segment_recyclable_for_mode_with_reasons(&header, 1000, peer != 0,
																		   &reasons),
					 peer != 0);
		UT_ASSERT_EQ(reasons, peer ? 0 : CLUSTER_UNDO_RETAIN_HISTORY_PIN);
		UT_ASSERT(cluster_undo_segment_recyclable_for_mode_with_reasons(&header, 1001, peer != 0,
																		&reasons));
		UT_ASSERT_EQ(reasons, 0);
		UT_ASSERT(
			cluster_undo_segment_recyclable_for_mode_with_reasons(&header, 1001, peer != 0, NULL));
		for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			header.tt_slots[i].status = TT_SLOT_ABORTED;
			header.tt_slots[i].commit_scn = InvalidScn;
		}
		UT_ASSERT(cluster_undo_segment_recyclable_for_mode_with_reasons(&header, InvalidScn,
																		peer != 0, &reasons));
		UT_ASSERT_EQ(reasons, 0);
		if (!peer) {
			header.tt_slots[0].flags = 0x80;
			header.tt_slots[0].first_undo_block.raw[0] = 1;
			header.tt_slots[0].commit_scn = 900;
			UT_ASSERT(cluster_undo_segment_recyclable_for_mode_with_reasons(&header, InvalidScn,
																			false, &reasons));
			UT_ASSERT_EQ(reasons, 0);
		}
		header.tail_block = 1;
		UndoSegmentHeader_set_record_seal_upper_scn(&header, 900);
		UndoSegmentHeader_set_record_drain_upper_scn(&header, 1000);
		UT_ASSERT(!cluster_undo_segment_recyclable_for_mode_with_reasons(&header, 1000, peer != 0,
																		 &reasons));
		UT_ASSERT_EQ(reasons, CLUSTER_UNDO_RETAIN_HISTORY_PIN);
		UT_ASSERT(cluster_undo_segment_recyclable_for_mode_with_reasons(&header, 1001, peer != 0,
																		&reasons));
		UT_ASSERT_EQ(reasons, 0);
	}
}

UT_TEST(test_retain_reasons_cover_each_physical_slot_and_combined_causes)
{
	for (unsigned slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++) {
		UndoSegmentHeaderData header = retain_reason_header();
		uint32 reasons;

		header.tt_slots[slot].flags = 0;
		header.tt_slots[slot].first_undo_block.raw[0] = 1;
		header.tt_slots[slot].commit_scn = 1001;
		header.tt_slots[(slot + 1) % TT_SLOTS_PER_SEGMENT].status = TT_SLOT_ACTIVE;
		header.tail_block = 1;
		UT_ASSERT(
			!cluster_undo_segment_recyclable_for_mode_with_reasons(&header, 1000, true, &reasons));
		UT_ASSERT_EQ(reasons,
					 CLUSTER_UNDO_RETAIN_RELEASE_NOT_DURABLE | CLUSTER_UNDO_RETAIN_REFERENCE_PENDING
						 | CLUSTER_UNDO_RETAIN_HISTORY_PIN | CLUSTER_UNDO_RETAIN_ACTIVE_OR_PREPARED
						 | CLUSTER_UNDO_RETAIN_SEAL_OR_DRAIN);
	}
}

UT_TEST(test_capacity_segment_reports_actual_retention_rejection_without_owner)
{
	const uint32 expected[]
		= { CLUSTER_UNDO_RETAIN_RELEASE_NOT_DURABLE, CLUSTER_UNDO_RETAIN_REFERENCE_PENDING,
			CLUSTER_UNDO_RETAIN_HISTORY_PIN, CLUSTER_UNDO_RETAIN_ACTIVE_OR_PREPARED,
			CLUSTER_UNDO_RETAIN_SEAL_OR_DRAIN };

	for (unsigned fault = 0; fault < lengthof(expected); fault++) {
		ClusterCapacityObservationV1 out;
		unsigned holds;

		(void)reset_unarmed_segment(48);
		holds = sample_holds;
		if (fault == 0)
			sample_header.tt_slots[46].flags = 0;
		if (fault == 1)
			sample_header.tt_slots[46].first_undo_block.raw[0] = 1;
		if (fault == 2)
			sample_header.tt_slots[46].commit_scn = 1001;
		if (fault == 3)
			sample_header.tt_slots[46].status = TT_SLOT_ACTIVE;
		if (fault == 4)
			sample_header.tail_block = 1;
		UT_ASSERT_EQ(probe_unarmed_segment(&out), CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT_EQ(observed_retain_reasons(&out), expected[fault]);
		UT_ASSERT(ctrc_bytes_zero(&out.segment, sizeof(out.segment)));
		UT_ASSERT(sample_holds - holds <= 16);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
}

UT_TEST(test_capacity_retention_diagnostics_discard_unproven_sample_identity)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterCapacityObservationV1 out;
		(void)reset_unarmed_segment(48);
		sample_header.tt_slots[46].commit_scn = 1001;
		if (fault == 0)
			segment_root_drift = true;
		if (fault == 1)
			segment_generation_drift = true;
		if (fault >= 2 && fault < 5) {
			segment_drift = fault == 2 ? 0 : fault == 3 ? 2 : 6;
			sample_release_hook = unarmed_segment_changes_after_sample;
		}
		if (fault == 5)
			sample_release_failure = true;
		UT_ASSERT(probe_unarmed_segment(&out) != CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE);
		UT_ASSERT_EQ(observed_retain_reasons(&out), 0);
		UT_ASSERT_EQ(sample_holds, sample_releases);
		UT_ASSERT_EQ(sample_enters, sample_leaves);
	}
	test_cluster_epoch = 13;
}

int
main(void)
{
	UT_PLAN(94);
	UT_RUN(test_retired_admission_observation_reaches_original_cleaner_cv);
	UT_RUN(test_original_cleaner_rejects_missing_retired_admission_identity);
	UT_RUN(test_retired_admission_refusal_never_sleeps_or_retains_debt);
	UT_RUN(test_retired_admission_error_and_cv_cancel_keep_original_cleanup);
	UT_RUN(test_retain_reasons_distinguish_history_missing_and_unfinished_work);
	UT_RUN(test_retain_reasons_preserve_peer_nonpeer_and_record_equality);
	UT_RUN(test_retain_reasons_cover_each_physical_slot_and_combined_causes);
	UT_RUN(test_capacity_segment_reports_actual_retention_rejection_without_owner);
	UT_RUN(test_capacity_retention_diagnostics_discard_unproven_sample_identity);
	UT_RUN(test_unarmed_segment_fair_walk_keeps_zero_key_and_sixteen_get_budget);
	UT_RUN(test_unarmed_segment_requires_all48_native_release_and_horizon);
	UT_RUN(test_unarmed_segment_keeps_policy_record_owner_and_canonical_gates);
	UT_RUN(test_unarmed_segment_rechecks_identity_and_original_sources);
	UT_RUN(test_unarmed_segment_dense_failed_origins_cannot_starve_gc);
	UT_RUN(test_unarmed_segment_error_cleans_original_guard_and_admission);
	UT_RUN(test_segment_probe_shares_hint_budget_and_samples_exact_hint_last);
	UT_RUN(test_pool_gc_retired_key_observes_original_whole_segment_work);
	UT_RUN(test_pool_gc_single_release_never_proves_all_slots_or_native_scope);
	UT_RUN(test_pool_gc_preserves_original_record_seal_and_strict_drain_horizon);
	UT_RUN(test_pool_gc_requires_original_optional_policy_record_cursor_and_owner);
	UT_RUN(test_pool_gc_rechecks_original_sources_after_its_single_physical_sample);
	UT_RUN(test_pool_gc_uses_all_slot_original_publisher_handoff);
	UT_RUN(test_pool_gc_shares_last_hint_budget_and_yields_to_independent_work);
	UT_RUN(test_pool_gc_error_keeps_original_sampler_cleanup_and_exact_hint);
	UT_RUN(test_pool_empty_census_rechecks_current_at_actual_scan_boundaries);
	UT_RUN(test_pool_failed_canonical_sample_rechecks_current_after_release);
	UT_RUN(test_pool_owner_refusal_rechecks_current_without_canonical_acquisition);
	UT_RUN(test_pool_invalid_final_current_cannot_use_a_reclaimed_hint_recheck);
	UT_RUN(test_pool_final_epoch_failure_precedes_current_or_hint_recheck);
	UT_RUN(test_pool_unchanged_final_scope_preserves_independent_owner_precedence);
	UT_RUN(test_pool_final_epoch_check_follows_the_current_snapshot);
	UT_RUN(test_pool_continuation_outside_batch_is_reproved_within_original_budget);
	UT_RUN(test_pool_continuation_never_preempts_ordinary_runnable_work);
	UT_RUN(test_pool_continuation_is_sampled_once_last_and_keeps_fair_successor);
	UT_RUN(test_pool_continuation_reclaimed_or_replaced_key_rechecks_without_cached_proof);
	UT_RUN(test_pool_continuation_bad_hint_cannot_authorize_or_hide_independent_work);
	UT_RUN(test_pool_continuation_rebuilds_actual_phase_and_preserves_exact_drift_checks);
	UT_RUN(test_pool_continuation_invalid_hint_cannot_bypass_sixteen_ordinary_candidates);
	UT_RUN(test_pool_continuation_error_keeps_original_sampler_and_hint_cleanup);
	UT_RUN(test_current_gc_discovers_original_release_after_origin_reclamation);
	UT_RUN(test_current_gc_missing_source_release_horizon_or_terminal_is_not_supply);
	UT_RUN(test_current_gc_rechecks_original_key_entry_binding_and_handoff);
	UT_RUN(test_current_gc_and_certificates_share_original_per_position_budget);
	UT_RUN(test_current_gc_error_preserves_original_current_and_admission_cleanup);
	UT_RUN(test_pool_observes_original_close_work_with_missing_required_ack);
	UT_RUN(test_pool_close_observation_preserves_actual_dispatch_and_certificate_gates);
	UT_RUN(test_pool_close_needs_new_request_capacity_only_for_unarmed_work);
	UT_RUN(test_pool_close_partial_ack_and_metadata_must_be_exact);
	UT_RUN(test_pool_close_requires_canonical_native_admission_and_original_owner);
	UT_RUN(test_pool_close_rechecks_real_ack_phase_requests_and_owner_after_sample);
	UT_RUN(test_pool_close_work_shares_all_phase_cursor_and_total_sample_budget);
	UT_RUN(test_pool_bad_close_ack_does_not_hide_independent_original_work);
	UT_RUN(test_pool_close_error_keeps_original_sampler_cleanup);
	UT_RUN(test_pool_observes_original_open_terminal_before_seal);
	UT_RUN(test_pool_open_observation_cannot_seal_publish_or_grant_reuse);
	UT_RUN(test_pool_open_missing_preseal_evidence_retains_all_bytes);
	UT_RUN(test_pool_open_requires_original_terminal_sampler_and_runnable_owner);
	UT_RUN(test_pool_open_rechecks_original_phase_membership_generation_and_owner);
	UT_RUN(test_pool_open_shares_original_cursor_and_sixteen_total_samples);
	UT_RUN(test_pool_bad_open_does_not_hide_independent_certificate_work);
	UT_RUN(test_pool_open_error_keeps_original_sampler_cleanup);
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
