/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retained_cut.c
 *	  Retention lower census (S07): completion per source, the PAGE/SPACE
 *	  dependency rule, keyless SIDE classes, the fixed-memory sketch, the
 *	  history spool and the checkpointer driver.
 *
 *	  The product source is compiled into this test.  The retained input
 *	  scope, the record decoders, the temporary file and the ROOT publisher
 *	  are fixtures: each fixture record names its source, its LSN range,
 *	  the pages it changes (with their before-tokens), its SIDE owners and
 *	  an optional SPACE contribution.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wal_retained_cut.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>

static int log_count;

#undef ereport
#define ereport(elevel, ...)                                                                       \
	do {                                                                                           \
		if ((elevel) >= ERROR)                                                                     \
			abort();                                                                               \
		if ((elevel) == LOG)                                                                       \
			log_count++;                                                                           \
	} while (0)

/* Small enough to overflow from a test. */
#define RETAINED_PREPARED_MAX 2

#include "../../backend/cluster/cluster_wal_retained_cut.c"
#include "../../backend/cluster/cluster_wal_retained_side.c"
#include "../../backend/cluster/cluster_wal_retained_generation.c"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

/* ---- backend globals and services the module links against ---- */
bool cluster_enabled = true;
bool cluster_shared_config = true;
volatile sig_atomic_t ShutdownRequestPending = false;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 CritSectionCount = 0;
AuxProcType MyAuxProcType = CheckpointerProcess;
bool log_checkpoints = false;
static TimestampTz fixture_now;

TimestampTz
GetCurrentTimestamp(void)
{
	return fixture_now += 1000;
}
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
ProcessInterrupts(void)
{}

void
pg_re_throw(void)
{
	abort();
}

void *
palloc(Size size)
{
	void *p = malloc(Max(size, 1));

	if (p == NULL)
		abort();
	return p;
}

void *
palloc0(Size size)
{
	void *p = calloc(1, Max(size, 1));

	if (p == NULL)
		abort();
	return p;
}

void
pfree(void *pointer)
{
	free(pointer);
}

int
errcode_for_file_access(void)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* SCN total order: local part, then node id (never the raw integer). */
int
scn_total_cmp(SCN a, SCN b)
{
	if (scn_local(a) != scn_local(b))
		return scn_local(a) < scn_local(b) ? -1 : 1;
	if (scn_node_id(a) != scn_node_id(b))
		return scn_node_id(a) < scn_node_id(b) ? -1 : 1;
	return 0;
}

bool
cluster_wal_claim_v2_ref_valid(const ClusterWalThreadClaimRefV2 *ref)
{
	return ref != NULL && ref->identity.origin_owner_incarnation != 0;
}

/* ---- temporary file: an in-memory byte array ---- */
struct BufFile {
	char *bytes;
	size_t length, position;
};
static int spool_creates;

BufFile *
BufFileCreateTemp(bool interXact pg_attribute_unused())
{
	spool_creates++;
	return calloc(1, sizeof(BufFile));
}

void
BufFileWrite(BufFile *file, const void *ptr, size_t size)
{
	file->bytes = realloc(file->bytes, file->length + size);
	memcpy(file->bytes + file->length, ptr, size);
	file->length += size;
}

int
BufFileSeek(BufFile *file, int fileno pg_attribute_unused(), off_t offset, int whence)
{
	UT_ASSERT_EQ(whence, SEEK_SET);
	file->position = offset;
	return 0;
}

void
BufFileReadExact(BufFile *file, void *ptr, size_t size)
{
	UT_ASSERT(file->position + size <= file->length);
	memcpy(ptr, file->bytes + file->position, size);
	file->position += size;
}

void
BufFileClose(BufFile *file)
{
	free(file->bytes);
	free(file);
}

/* ---- the retained input scope ---- */
#define MAX_ITEMS 6
#define MAX_RECORDS 700

typedef struct FixtureRecord {
	uint32 source;
	XLogRecPtr read, end;
	uint64 result_token;
	uint32 ncomp;
	Oid rel[2];
	uint8 before_kind[2];
	uint64 before[2];
	uint32 owners;
	Oid space_rel;
	uint8 space_mask;  /* 0: reservation (block 1); 3: structure change */
	bool space_create; /* the structure change is a CREATE */
	uint8 inc;		   /* result segment incarnation id, 0 means 1 */
	uint8 before_inc;  /* before incarnation id, 0 means inc */
	/* SIDE keys, as the undo and transaction decoders would report them. */
	uint8 rmid;		  /* 0: neither decoder applies */
	uint8 undo_kind;  /* ClusterUndoDecodedKind */
	uint8 xact_kind;  /* RfSideXactKindV1 */
	bool undecodable; /* the decoder refuses the record */
	bool full_image;
	uint32 segment;
	uint32 sub; /* TT slot or undo block */
	TransactionId xid;
} FixtureRecord;

static ClusterWalInputV1 items[MAX_ITEMS];
static uint32 nitems;
static FixtureRecord records[MAX_RECORDS];
static uint32 nrecords;
static const FixtureRecord *current;
static ClusterControlRootResult begin_result, census_result, revalidate_result, token_result;
static int scopes_open;
static ClusterWalSourceRef self_ref;

struct ClusterWalInputsV1 {
	int unused;
};
static ClusterWalInputsV1 the_scope;

ClusterControlRootResult
cluster_wal_inputs_begin_v1(const uint8 storage_uuid[16] pg_attribute_unused(),
							uint64 system_identifier pg_attribute_unused(),
							ClusterWalInputsV1 **out)
{
	if (begin_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return begin_result;
	scopes_open++;
	*out = &the_scope;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	if (*inputs != NULL)
		scopes_open--;
	*inputs = NULL;
}

uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs pg_attribute_unused())
{
	return nitems;
}

const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs pg_attribute_unused(), uint32 index)
{
	return index < nitems ? &items[index] : NULL;
}

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs pg_attribute_unused())
{
	return revalidate_result;
}

ClusterControlRootResult
cluster_wal_inputs_root_token_v1(ClusterWalInputsV1 *inputs pg_attribute_unused(),
								 ClusterControlRootFileToken *out)
{
	memset(out, 0, sizeof(*out));
	out->file_txn_seq = 77; /* written even when refused */
	return token_result;
}

/* Feed each fixture record as a decoded record of its source. */
ClusterControlRootResult
cluster_wal_inputs_census_v1(ClusterWalInputsV1 *inputs pg_attribute_unused(),
							 ClusterWalCensusVisitorV1 visitor, void *arg, uint64 *out_record_count,
							 RfPageProofDetailV1 *out_detail)
{
	static XLogReaderState reader;
	DecodedXLogRecord *decoded = calloc(1, sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock));

	*out_record_count = 0;
	*out_detail = RF_PAGE_PROOF_DETAIL_OK;
	if (census_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		free(decoded);
		return census_result;
	}
	for (uint32 i = 0; i < nrecords; i++) {
		const ClusterWalInputV1 *item = &items[records[i].source];
		RfContributorStreamCutV1 cut = { 0 };
		RfPageProofDetailV1 detail;

		current = &records[i];
		cut.failed_thread = item->source.claim.identity.origin_thread_id;
		cut.origin_owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		cut.timeline_id = item->source.timeline;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		reader.ReadRecPtr = records[i].read;
		reader.EndRecPtr = records[i].end;
		reader.record = decoded;
		decoded->header.xl_rmid = records[i].rmid;
		for (uint32 b = 0; b < records[i].ncomp; b++) {
			decoded->blocks[b].rlocator.spcOid = 1663;
			decoded->blocks[b].rlocator.dbOid = 5;
			decoded->blocks[b].rlocator.relNumber = records[i].rel[b];
			decoded->blocks[b].forknum = MAIN_FORKNUM;
			decoded->blocks[b].blkno = 0;
		}
		detail = visitor(&reader, &item->source, &cut, arg);
		if (detail != RF_PAGE_PROOF_DETAIL_OK) {
			*out_detail = detail;
			free(decoded);
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		}
		(*out_record_count)++;
	}
	free(decoded);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

RfPageProofDetailV1
rf_page_detached_preflight_v1(XLogReaderState *record pg_attribute_unused(),
							  bool space_active pg_attribute_unused(),
							  const RfDetachedOwnerOpsV1 *owner_ops pg_attribute_unused(),
							  RfDetachedRecordPlanV1 *plan)
{
	memset(plan, 0, sizeof(*plan));
	plan->result_token = current->result_token;
	plan->component_count = current->ncomp;
	for (uint32 b = 0; b < current->ncomp; b++) {
		plan->components[b].block_id = b;
		plan->components[b].owner = RF_DETACHED_COMPONENT_PAGE_CODEC;
		plan->components[b].page_class = RF_PAGE_CLASS_ORDINARY;
		plan->components[b].before_kind = current->before_kind[b];
		plan->components[b].before.mutation_token = current->before[b];
		plan->components[b].before.segment_incarnation[0] = current->before_inc != 0
																? current->before_inc
															: current->inc != 0 ? current->inc
																				: 1;
		plan->components[b].result.segment_incarnation[0] = current->inc != 0 ? current->inc : 1;
		plan->components[b].result.mutation_token = current->result_token;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_record_census_v1(const RfDetachedRecordPlanV1 *record_plan pg_attribute_unused(),
						 const RfPageOnlineRecordIdentityV1 *identity pg_attribute_unused(),
						 const RfContributorStreamCutV1 *cut pg_attribute_unused(),
						 uint64 database_incarnation pg_attribute_unused(),
						 RfSideCensusSpaceVisitorV1 visit_space, void *arg,
						 RfSideContributionOwnersV1 *out)
{
	memset(out, 0, sizeof(*out));
	out->owners = current->owners;
	if (current->space_rel != InvalidOid) {
		RfSideSpaceContributionV1 space = { 0 };

		space.result.key.locator.spcOid = 1663;
		space.result.key.locator.dbOid = 5;
		space.result.key.locator.relNumber = current->space_rel;
		space.result.incarnation[0] = current->inc != 0 ? current->inc : 1;
		space.result.state = current->space_create ? CLUSTER_SPACE_IDENTITY_LIVE
												   : CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		space.result.sequence = current->space_create ? 1 : 2;
		space.page_mask = current->space_mask != 0 ? current->space_mask : 2;
		if (space.page_mask & 1)
			space.result_token[0] = current->result_token;
		if (space.page_mask & 2)
			space.result_token[1] = current->result_token;
		out->owners |= RF_SIDE_CONTRIBUTION_SPACE;
		if (!visit_space(arg, &space))
			return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
cluster_undo_decode(XLogReaderState *record pg_attribute_unused(), ClusterUndoDecoded *out)
{
	memset(out, 0, sizeof(*out));
	if (current->undecodable)
		return false;
	out->kind = (ClusterUndoDecodedKind)current->undo_kind;
	out->segment_id = current->segment;
	out->slot_offset = (uint16)current->sub;
	out->block_no = current->sub;
	out->has_fpi = current->full_image;
	return true;
}

bool
rf_side_xact_decode_v1(XLogReaderState *record pg_attribute_unused(),
					   uint64 system_identifier pg_attribute_unused(),
					   uint16 origin_thread pg_attribute_unused(), RfSideXactOperationV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (current->undecodable)
		return false;
	out->kind = (RfSideXactKindV1)current->xact_kind;
	out->xid = current->xid;
	if (out->kind == RF_SIDE_XACT_COMMIT) {
		out->has_tt_delta = true;
		out->tt_delta.segment_id = current->segment;
		out->tt_delta.slot_offset = (uint16)current->sub;
	} else if (out->kind == RF_SIDE_XACT_PREPARE) {
		out->prepared_binding_count = 1;
		out->prepared_bindings[0].undo_segment_id = current->segment;
		out->prepared_bindings[0].slot_offset = (uint16)current->sub;
	}
	return true;
}

/* The fixture SIDE decoder accepts every record: the native fallback and
 * its bounded transaction-end check are never reached here (see the
 * records test for both). */
bool
rf_side_xact_completion_shape_v1(XLogReaderState *record pg_attribute_unused(),
								 bool commit pg_attribute_unused())
{
	abort();
}

/* ---- checkpointer driver services ---- */
static bool self_ref_valid = true;
static int publish_calls;
static ClusterControlRootResult publish_result;
static ClusterControlRootFileToken publish_token;
static XLogRecPtr publish_native, publish_lower;

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	*out = self_ref;
	return self_ref_valid;
}

ClusterControlRootResult
cluster_control_root_v3_retained_lower_publish(const ClusterControlRootIdentity *self,
											   const ClusterControlRootFileToken *census_token,
											   XLogRecPtr native_redo, XLogRecPtr lower,
											   ClusterControlRootSnapshot *out)
{
	UT_ASSERT(memcmp(self, &self_ref.claim.identity, sizeof(*self)) == 0);
	publish_calls++;
	publish_token = *census_token;
	publish_native = native_redo;
	publish_lower = lower;
	memset(out, 0, sizeof(*out));
	return publish_result;
}

/* ---- buffers' first records (R-A22) ---- */
static bool dirty_ok;
static ClusterPageWalDirtyFloorV1 dirty;
static int dirty_calls, scan_sequence, dirty_scanned_at, local_pi_scanned_at;

bool
cluster_page_wal_dirty_floor_v1(const ClusterWalSourceRef *source, ClusterPageWalDirtyFloorV1 *out)
{
	UT_ASSERT(memcmp(source, &self_ref, sizeof(*source)) == 0);
	dirty_calls++;
	dirty_scanned_at = ++scan_sequence;
	UT_ASSERT_EQ(scopes_open, 0);
	memset(out, 0, sizeof(*out));
	if (!dirty_ok)
		return false;
	*out = dirty;
	return true;
}

/* ---- local PI directory ---- */
static bool local_pi_ok;
static ClusterPcmLocalPiFloorV1 local_pi;
static int local_pi_calls, local_pi_scopes_at_call;

bool
cluster_pcm_local_pi_floor_v1(const ClusterWalSourceRef *source, ClusterPcmLocalPiFloorV1 *out)
{
	UT_ASSERT(memcmp(source, &self_ref, sizeof(*source)) == 0);
	local_pi_calls++;
	local_pi_scanned_at = ++scan_sequence;
	local_pi_scopes_at_call = scopes_open;
	memset(out, 0, sizeof(*out));
	if (!local_pi_ok)
		return false;
	*out = local_pi;
	return true;
}

/* ---- fixture builders ---- */
static void
reset(void)
{
	local_pi_ok = true;
	memset(&local_pi, 0, sizeof(local_pi));
	local_pi_calls = 0;
	dirty_ok = true;
	memset(&dirty, 0, sizeof(dirty));
	dirty_calls = scan_sequence = dirty_scanned_at = local_pi_scanned_at = 0;
	retained_presync_valid = false;
	local_pi_scopes_at_call = -1;
	memset(items, 0, sizeof(items));
	memset(records, 0, sizeof(records));
	nitems = nrecords = 0;
	begin_result = census_result = revalidate_result = token_result
		= CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	publish_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	publish_calls = 0;
	self_ref_valid = true;
	log_count = 0;
	spool_creates = 0;
	retained_logged = false;
	retained_structure_pins = 0;
}

/* thread, lifecycle, physical lower, native redo, validated tail. */
static uint32
add_item(uint16 thread, uint8 lifecycle, XLogRecPtr lower, XLogRecPtr redo, XLogRecPtr tail,
		 bool current_writer)
{
	ClusterWalInputV1 *item = &items[nitems];

	item->kind = CLUSTER_WAL_INPUT_CHECKPOINT;
	item->current = current_writer;
	item->source.claim.identity.system_identifier = 7;
	item->source.claim.identity.origin_thread_id = thread;
	item->source.claim.identity.origin_node_id = thread - 1;
	item->source.claim.identity.origin_owner_incarnation = 40 + thread;
	item->source.claim.database_incarnation = 3;
	item->source.claim.claim_sha256[0] = (uint8)thread;
	item->source.timeline = 1;
	item->checkpoint.identity = item->source.claim.identity;
	item->checkpoint.lifecycle = lifecycle;
	item->checkpoint.checkpoint_lower_lsn = lower;
	item->checkpoint.validated_tail_lsn_exclusive = tail;
	item->native_redo = redo;
	if (thread == 1 && current_writer)
		self_ref = item->source;
	return nitems++;
}

static FixtureRecord *
add_record(uint32 source, XLogRecPtr read, XLogRecPtr end, Oid rel, uint64 before, uint64 result)
{
	FixtureRecord *r = &records[nrecords++];

	r->source = source;
	r->read = read;
	r->end = end;
	r->result_token = result;
	if (rel != InvalidOid) {
		r->ncomp = 1;
		r->rel[0] = rel;
		r->before_kind[0] = RF_PAGE_STATE_PRESENT;
		r->before[0] = before;
	}
	return r;
}

static bool
v_zero(const void *ptr, size_t len)
{
	const uint8 *bytes = ptr;

	for (size_t i = 0; i < len; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static ClusterControlRootResult
compute(ClusterWalRetainedCutV1 *cut, RfPageProofDetailV1 *detail)
{
	ClusterControlRootResult result = cluster_wal_retained_cut_compute_v1(&self_ref, cut, detail);

	UT_ASSERT_EQ(scopes_open, 0);
	return result;
}

#include "test_cluster_wal_retained_cut_cases.h"

int
main(void)
{
	UT_PLAN(24);
	UT_RUN(test_retained_cut_moves_to_native_redo_without_obligations);
	UT_RUN(test_retained_cut_peer_obligation_keeps_successors_on_its_page);
	UT_RUN(test_retained_cut_hot_page_releases_predecessors);
	UT_RUN(test_retained_cut_other_incarnation_is_kept);
	UT_RUN(test_retained_cut_refuses_a_chain_that_does_not_advance);
	UT_RUN(test_retained_cut_keyless_side_classes);
	UT_RUN(test_retained_cut_space_obligation_keeps_space_history);
	UT_RUN(test_retained_cut_structure_change_pins_history);
	UT_RUN(test_retained_cut_completion_by_lifecycle);
	UT_RUN(test_retained_cut_never_moves_back);
	UT_RUN(test_retained_cut_refusals_zero_the_output);
	UT_RUN(test_retained_cut_spills_history_to_a_temp_file);
	UT_RUN(test_retained_sketch_is_conservative);
	UT_RUN(test_retained_cut_driver_publishes_only_an_advance);
	UT_RUN(test_retained_cut_local_pi_floor_holds_this_thread);
	UT_RUN(test_retained_cut_local_pi_floor_unavailable_refuses);
	UT_RUN(test_r_a22_dirty_floor_holds_this_thread);
	UT_RUN(test_r_a22_presync_snapshot_is_merged_once);
	UT_RUN(test_r_a22_peer_completion_is_its_published_lower);
	UT_RUN(test_retained_cut_side_tt_slots_are_keyed);
	UT_RUN(test_retained_cut_side_undo_blocks_are_keyed);
	UT_RUN(test_retained_cut_side_prepared_transactions_are_keyed);
	UT_RUN(test_retained_cut_side_unkeyed_records_are_kept_by_class);
	UT_RUN(test_retained_cut_older_generation_unneeded_only_when_no_obligation);
	UT_RUN(test_r_a20_cross_thread_publish_seq_proves_no_restart);
	UT_DONE();
	return ut_failed_count != 0;
}
