/* Author: SqlRush <sqlrush@gmail.com>
 * Execute the production FULL evaluator, exact resolver and native CID getters.
 * Origin I/O, admission, backend publication and xact membership are fixtures.
 */
#include "postgres.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_verdict.h"
extern ClusterUndoVerdictResult self_unused_history(int, uint32, TransactionId, uint32, SCN, bool);
extern int self_unused_xid_origin(TransactionId);
extern bool cluster_undo_horizon_read_admission_enforce(SCN read_scn);
int scratch_fixture_main(void);
extern NodeId self_unused_origin(UBA address);
extern ClusterTxOutcome self_unused_exact(const ClusterTxLocator *, ClusterTxResolveMode,
	ClusterTxResolution *, ClusterTxResolveReason *);
extern bool self_unused_itl_locator(Page, uint8, ClusterTxLocator *, ClusterTxResolveReason *);
#define cluster_undo_verdict_resolve self_unused_history
#define cluster_xid_origin_slot self_unused_xid_origin
#define main scratch_fixture_main
#define GetTopFullTransactionIdIfAny self_unused_top_full
#define cluster_tx_resolve_operation_head self_unused_operation_head
#define cluster_tx_resolve_retained_operation_head self_unused_retained_operation_head
#define TransactionIdIsCurrentTransactionId self_unused_current
#define HeapTupleHeaderGetCmin self_unused_cmin
#define HeapTupleHeaderGetCmax self_unused_cmax
#define cluster_tx_resolve_exact self_unused_exact
#define cluster_itl_get_tt_ref self_unused_itl_ref
#define cluster_tx_locator_from_itl self_unused_itl_locator
#define uba_origin_node_id self_unused_origin
#include "test_cluster_r4_scratch_resolver.c"
#undef main
#undef cluster_undo_verdict_resolve
#undef cluster_xid_origin_slot
#undef GetTopFullTransactionIdIfAny
#undef cluster_tx_resolve_operation_head
#undef cluster_tx_resolve_retained_operation_head
#undef TransactionIdIsCurrentTransactionId
#undef HeapTupleHeaderGetCmin
#undef HeapTupleHeaderGetCmax
#undef cluster_tx_resolve_exact
#undef cluster_itl_get_tt_ref
#undef cluster_tx_locator_from_itl
#undef uba_origin_node_id
#include "cluster/cluster_tt_local.h"
extern NodeId uba_origin_node_id(UBA address);
#include "../../backend/cluster/cluster_uba.c"

extern bool TransactionIdIsCurrentTransactionId(TransactionId xid);
extern CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tuple);
extern CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tuple);
extern CommandId self_real_cmin(HeapTupleHeader tuple);
extern CommandId self_real_cmax(HeapTupleHeader tuple);
extern ClusterTxOutcome cluster_tx_resolve_exact(const ClusterTxLocator *, ClusterTxResolveMode,
												 ClusterTxResolution *, ClusterTxResolveReason *);

volatile uint32 CritSectionCount = 0;
#include "test_cluster_owner_full_fixture.h"
static bool self_owner_full_pipe;
static TestOwnerFullFixture self_owner_full;
static bool self_peer_full_pipe;
static ClusterUndoVerdictResult self_peer_raw_verdict;
static bool self_current;
static bool self_child;
static int self_update_layout;
static int self_update_defect;
enum SelfHistoryDefect {
	SELF_HISTORY_OK,
	SELF_HISTORY_SEQUENCE,
	SELF_HISTORY_TOP_ERA,
	SELF_HISTORY_GENERATION,
	SELF_HISTORY_DISCARDED,
	SELF_HISTORY_WRITER_ERA,
	SELF_HISTORY_CID,
	SELF_HISTORY_PRIOR_SELF,
	SELF_HISTORY_PRIOR_FOREIGN,
	SELF_HISTORY_PRIOR_TT,
	SELF_HISTORY_PRIOR_OWNER,
	SELF_HISTORY_PRIOR_FREE,
	SELF_HISTORY_PRIOR_SCN,
	SELF_HISTORY_OLD_XMIN,
	SELF_HISTORY_ERROR,
	SELF_HISTORY_UNKNOWN,
	SELF_HISTORY_OLD_HOFF
};
static int self_mixed_mode;
static int self_mixed_defect;
static int self_history_records;
static bool self_history_cross_page;
static enum SelfHistoryDefect self_history_defect;
static int self_history_expected_calls;
static PGAlignedBlock self_history_pages[2];
static ClusterItlSlotData self_history_heads[24];
static int self_operation_defect;
static int self_operation_calls;
static int self_admission_enters;
static int self_admission_leaves;
static bool self_admission_held;
static bool self_binding_present;
static int self_binding_reads;
static int self_drift;
static int self_cid_calls;
static ClusterCanonicalTxnBinding self_binding;

static bool self_real_itl_ref(Page page, uint8 index, ClusterUndoTTSlotRef *out);
static bool self_real_itl_locator(Page page, uint8 index, ClusterTxLocator *out,
								  ClusterTxResolveReason *reason);
extern bool cluster_itl_get_tt_ref(Page page, uint8 index, ClusterUndoTTSlotRef *out);
extern bool cluster_tx_locator_from_itl(Page page, uint8 index, ClusterTxLocator *out,
										ClusterTxResolveReason *reason);
#define cluster_itl_get_tt_ref self_real_itl_ref
#define cluster_tx_locator_from_itl self_real_itl_locator
#include "test_cluster_scratch_itl_ref.inc"
#undef cluster_itl_get_tt_ref
#undef cluster_tx_locator_from_itl

bool
cluster_itl_get_tt_ref(Page page, uint8 index, ClusterUndoTTSlotRef *out)
{
	return self_owner_full_pipe ? self_real_itl_ref(page, index, out)
								: self_unused_itl_ref(page, index, out);
}

bool
cluster_tx_locator_from_itl(Page page, uint8 index, ClusterTxLocator *out,
							ClusterTxResolveReason *reason)
{
	return self_owner_full_pipe ? self_real_itl_locator(page, index, out, reason)
								: self_unused_itl_locator(page, index, out, reason);
}

bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	if (self_owner_full_pipe)
		return xid == self_owner_full.expected.top.xid
			   || xid == XidFromFullTransactionId(self_owner_full.expected.operation_fxid);
	return self_current && (xid == UT_RAW_XID || (self_child && xid == UT_RAW_XID + 1));
}

#define HeapTupleHeaderGetCmin self_real_cmin
#define HeapTupleHeaderGetCmax self_real_cmax
#include "../../backend/utils/time/combocid.c"
#undef HeapTupleHeaderGetCmin
#undef HeapTupleHeaderGetCmax

CommandId
HeapTupleHeaderGetCmin(HeapTupleHeader tuple)
{
	self_cid_calls++;
	return self_real_cmin(tuple);
}
CommandId
HeapTupleHeaderGetCmax(HeapTupleHeader tuple)
{
	self_cid_calls++;
	return self_real_cmax(tuple);
}

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	self_binding_reads++;
	if (!self_binding_present || xid != self_binding.xid)
		return false;
	*out = self_binding;
	return true;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
						 ClusterTxResolution *out, ClusterTxResolveReason *reason)
{
	ClusterTxOutcome outcome;

	if (self_owner_full_pipe) {
		const ClusterCanonicalTxnBinding *binding = &self_owner_full.expected.top;

		memset(out, 0, sizeof(*out));
		out->locator_echo = *locator;
		out->locator_echo.tt_wrap = binding->slot_wrap;
		out->top_xid = binding->xid;
		out->outcome = CLUSTER_TX_IN_PROGRESS;
		out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
		out->authority.origin_epoch = ut_current_epoch;
		out->authority.authority_scn = self_owner_full.read_scn;
		out->local_canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = binding->segment_id,
															   .xid = binding->xid,
															   .slot_offset = binding->slot_offset,
															   .wrap = binding->slot_wrap };
		out->local_canonical_generation = binding->segment_generation;
		out->local_canonical_valid = true;
		*reason = CLUSTER_TX_RESOLVE_NONE;
		return CLUSTER_TX_IN_PROGRESS;
	}
	outcome = self_unused_exact(locator, mode, out, reason);
	out->top_xid = UT_RAW_XID;
	out->authority.origin_epoch = ut_current_epoch;
	out->authority.authority_scn = UT_READ_SCN;
	/* Origin boundary supplies its independently sampled canonical address. */
	out->local_canonical.segment_id = UT_SELF_NODE * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	out->local_canonical.xid = UT_RAW_XID;
	out->local_canonical.slot_offset = 3;
	out->local_canonical.wrap = 7;
	out->local_canonical_generation = 2;
	out->local_canonical_valid = true;
	if (self_drift == 1)
		out->locator_echo.uba.raw[0]++;
	if (self_drift == 2)
		out->locator_echo.tt_wrap++;
	if (self_drift == 3)
		out->top_xid++;
	if (self_drift == 4)
		ut_current_epoch++;
	if (self_drift == 5)
		out->authority.origin_epoch++;
	if (self_drift == 19)
		out->local_canonical_valid = false;
	return outcome;
}

extern FullTransactionId GetTopFullTransactionIdIfAny(void);
extern ClusterTxOutcome cluster_tx_resolve_operation_head(
	const ClusterTxLocator *, const ClusterItlSlotData *, const BufferTag *,
	const ClusterSavepointOperation *, ClusterSavepointReaderIdentity *, ClusterTxResolution *,
	ClusterSavepointReaderRecord *, char[BLCKSZ], ClusterTxResolveReason *);

ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_EXISTING_OTHER;
}

bool
cluster_semantic_activation_recovered_open_current(void)
{
	return self_operation_defect != 17;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	self_admission_enters++;
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT(!self_admission_held);
	if (self_operation_defect == 15)
		return CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	if (self_operation_defect == 16)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	token->entered = true;
	self_admission_held = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token->entered && self_admission_held);
	self_admission_leaves++;
	token->entered = self_admission_held = false;
}

/* Exact production entry and recovered-open predicate, not an API mock. */
#include "test_cluster_scratch_operation_entry.inc"

FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	if (self_owner_full_pipe)
		return self_owner_full.expected.top_fxid;
	return self_child ? FullTransactionIdFromEpochAndXid(1, UT_RAW_XID) : InvalidFullTransactionId;
}

static void
self_operation_record(char data[BLCKSZ], const ClusterTxLocator *locator,
					  const ClusterItlSlotData *head, const BufferTag *tag, bool creator)
{
	UndoBlockHeader *block = (UndoBlockHeader *)data;
	UndoSlotDirEntry *directory;
	UndoRecordHeader *record = (UndoRecordHeader *)(data + 256);
	UndoItlHistoryTrailer history = { 0 };
	UndoOperationTrailer operation = { 0 };
	uint32 segment, number;
	uint16 slot, row;
	char *body = (char *)record + sizeof(*record);
	Size body_length;

	memset(data, 0, BLCKSZ);
	UT_ASSERT(uba_decode_record(locator->uba, &segment, &number, &slot, &row));
	block->magic = PGRAC_UNDO_BLOCK_MAGIC;
	block->block_version = UNDO_BLOCK_VERSION_1;
	block->slot_count = row + 1;
	directory = (UndoSlotDirEntry *)(data + BLCKSZ - ((size_t)row + 1) * sizeof(*directory));
	record->record_type = creator ? UNDO_RECORD_INSERT : UNDO_RECORD_DELETE;
	record->flags
		= UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION;
	record->xid = UT_RAW_XID + 1;
	record->origin_node_id = UT_SELF_NODE;
	record->tt_slot_segment_id = self_binding.segment_id;
	record->tt_slot_id = slot + 1;
	record->tt_wrap_plus1 = 8;
	record->write_scn = head->write_scn;
	record->target_locator = (RelFileLocator){ tag->spcOid, tag->dbOid, tag->relNumber };
	record->target_fork = tag->forkNum;
	record->target_block = tag->blockNum;
	record->target_offset = 1;
	if (creator) {
		UndoInsertPayload inserted = { SizeofHeapTupleHeader, 0 };
		memcpy(body, &inserted, sizeof(inserted));
		body_length = sizeof(inserted);
	} else {
		UndoDeletePayload deleted = { SizeofHeapTupleHeader, sizeof(deleted), 0 };
		HeapTupleHeader old = (HeapTupleHeader)(body + sizeof(deleted));
		memcpy(body, &deleted, sizeof(deleted));
		old->t_hoff = SizeofHeapTupleHeader;
		HeapTupleHeaderSetXmin(old, UT_RAW_XID);
		old->t_infomask = HEAP_XMAX_INVALID;
		body_length = sizeof(deleted) + SizeofHeapTupleHeader;
	}
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = tag->blockNum;
	history.entries[0].after_kind = ITL_FLAG_ACTIVE;
	history.entries[0].after_write_scn = head->write_scn;
	history.entries[0].prior.wrap = head->wrap;
	operation.version = UNDO_OPERATION_TRAILER_VERSION;
	operation.length = sizeof(operation);
	operation.effect_state = self_operation_defect == 4 ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
														: CLUSTER_SAVEPOINT_EFFECT_LIVE;
	operation.canonical_top_fxid = U64FromFullTransactionId(GetTopFullTransactionIdIfAny());
	operation.operation_fxid = operation.canonical_top_fxid + 1;
	operation.operation_sequence = 5;
	operation.canonical_segment_generation = 2;
	operation.command_id = self_operation_defect == 5 ? 3 : 2;
	operation.subid = TopSubTransactionId + 1;
	if (self_operation_defect == 1)
		record->target_offset++;
	if (self_operation_defect == 2) {
		record->xid++;
		operation.operation_fxid++;
	}
	if (self_operation_defect == 3) {
		operation.canonical_top_fxid += UINT64CONST(1) << 32;
		operation.operation_fxid += UINT64CONST(1) << 32;
	}
	if (self_operation_defect == 10)
		operation.operation_fxid += UINT64CONST(1) << 32;
	if (self_operation_defect == 11)
		record->flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	if (self_operation_defect == 12)
		history.entries[0].after_write_scn--;
	if (self_operation_defect == 14)
		record->target_locator.relNumber++;
	memcpy(body + body_length, &history, sizeof(history));
	memcpy(body + body_length + sizeof(history), &operation, sizeof(operation));
	record->payload_length = body_length + sizeof(history) + sizeof(operation);
	*directory = (UndoSlotDirEntry){ .record_offset = 256,
									 .record_length = sizeof(*record) + record->payload_length,
									 .record_type = record->record_type,
									 .flags = record->flags };
	block->free_offset = 256 + directory->record_length;
}

/* Replace only the operation body/targets; keep the original exact envelope. */
static void
self_update_record(char data[BLCKSZ], bool creator)
{
	UndoRecordHeader *record = (UndoRecordHeader *)(data + 256);
	UndoBlockHeader *block = (UndoBlockHeader *)data;
	UndoSlotDirEntry *directory
		= (UndoSlotDirEntry *)(data + BLCKSZ
							   - (size_t)block->slot_count * sizeof(UndoSlotDirEntry));
	char *body = (char *)record + sizeof(*record);
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	UndoUpdatePayload update = { 0 };
	HeapTupleHeaderData old = { 0 };
	uint16 decoded_length;
	Size length;

	memcpy(&operation, body + record->payload_length - sizeof(operation), sizeof(operation));
	memcpy(&history, body + record->payload_length - sizeof(operation) - sizeof(history),
		   sizeof(history));
	record->record_type = UNDO_RECORD_UPDATE;
	record->target_block = creator && self_update_layout == 2 ? 1 : 0;
	record->target_offset = creator && self_update_layout == 1 ? 2 : 1;
	update.new_block = !creator && self_update_layout == 2 ? 1 : 0;
	update.new_offset = !creator && self_update_layout == 1 ? 2 : 1;
	if (self_update_defect == 1) {
		if (creator)
			update.new_offset = 3;
		else
			record->target_offset = 3;
	}
	if (self_update_defect == 2) {
		if (creator)
			update.new_block = 2;
		else
			record->target_block = 2;
	}
	update.old_tuple_offset = sizeof(update);
	update.old_tuple_length = SizeofHeapTupleHeader;
	if (self_update_defect == 4)
		update.old_tuple_length += 4;
	if (self_update_defect == 7)
		update.old_tuple_length--;
	old.t_hoff = self_update_defect == 8 ? 0 : SizeofHeapTupleHeader;
	if (self_update_defect == 9)
		old.t_hoff++;
	old.t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(&old, self_update_defect == 3 ? UT_RAW_XID + 80 : FrozenTransactionId);
	ItemPointerSet(&old.t_ctid, record->target_block, record->target_offset);
	length = sizeof(update) + update.old_tuple_length;
	memset(body, 0x5a, length);
	memcpy(body, &update, sizeof(update));
	memcpy(body + sizeof(update), &old, Min(update.old_tuple_length, SizeofHeapTupleHeader));
	history.entries[0].block = record->target_block;
	if (update.new_block != record->target_block) {
		history.count = 2;
		history.entries[1] = history.entries[0];
		history.entries[1].block = update.new_block;
	}
	memcpy(body + length, &history, sizeof(history));
	memcpy(body + length + sizeof(history), &operation, sizeof(operation));
	record->payload_length = length + sizeof(history) + sizeof(operation);
	directory->record_type = UNDO_RECORD_UPDATE;
	directory->record_length = sizeof(*record) + record->payload_length;
	block->free_offset = directory->record_offset + directory->record_length;
	UT_ASSERT(cluster_undo_record_decode_operation_payload(record, body, record->payload_length,
														   &decoded_length, &history, &operation));
}

/* Keep the DATA fixture independent of the roots supplied by the consumer. */
static void
self_history_rewrite(char bytes[BLCKSZ], unsigned index, bool creator)
{
	UndoRecordHeader *record = (UndoRecordHeader *)(bytes + 256);
	char *body = (char *)record + sizeof(*record);
	char *operation_bytes = body + record->payload_length - sizeof(UndoOperationTrailer);
	UndoOperationTrailer operation;
	UndoItlHistoryTrailer history;
	bool last = index + 1 == self_history_records;
	bool addressed = last || self_history_defect == SELF_HISTORY_OLD_XMIN
					 || self_history_defect == SELF_HISTORY_OLD_HOFF;
	bool affected = index == 1;

	memcpy(&operation, operation_bytes, sizeof(operation));
	memcpy(&history, operation_bytes - sizeof(history), sizeof(history));
	operation.operation_sequence = self_history_records - index;
	if (!addressed && index % 2 == 0) {
		record->xid++;
		operation.operation_fxid++;
	} else if (!addressed) {
		if (creator && record->record_type == UNDO_RECORD_UPDATE) {
			UndoUpdatePayload update;
			memcpy(&update, body, sizeof(update));
			update.new_offset = 3;
			memcpy(body, &update, sizeof(update));
		} else
			record->target_offset = 3;
	}
	if (!last) {
		record->flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
		record->prev_uba = self_history_heads[index + 1].undo_segment_head;
		operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		for (unsigned i = 0; i < history.count; i++)
			if (history.entries[i].block == 0)
				history.entries[i].prior = self_history_heads[index + 1];
	}
	if (affected && self_history_defect == SELF_HISTORY_SEQUENCE)
		operation.operation_sequence++;
	if (affected && self_history_defect == SELF_HISTORY_TOP_ERA) {
		operation.canonical_top_fxid += UINT64CONST(1) << 32;
		operation.operation_fxid += UINT64CONST(1) << 32;
	}
	if (affected && self_history_defect == SELF_HISTORY_GENERATION)
		operation.canonical_segment_generation++;
	if (last && self_history_defect == SELF_HISTORY_DISCARDED)
		operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	if (last && self_history_defect == SELF_HISTORY_WRITER_ERA)
		operation.operation_fxid += UINT64CONST(1) << 32;
	if (last && self_history_defect == SELF_HISTORY_CID)
		operation.command_id++;
	if (index == 0 && addressed) {
		uint16 offset;
		operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
		if (record->record_type == UNDO_RECORD_UPDATE) {
			UndoUpdatePayload update;
			memcpy(&update, body, sizeof(update));
			offset = update.old_tuple_offset;
		} else {
			UndoDeletePayload deleted;
			memcpy(&deleted, body, sizeof(deleted));
			offset = deleted.full_tuple_offset;
		}
		if (self_history_defect == SELF_HISTORY_OLD_XMIN)
			HeapTupleHeaderSetXmin((HeapTupleHeader)(body + offset), UT_RAW_XID + 80);
		if (self_history_defect == SELF_HISTORY_OLD_HOFF)
			((HeapTupleHeader)(body + offset))->t_hoff = 0;
	}
	memcpy(operation_bytes - sizeof(history), &history, sizeof(history));
	memcpy(operation_bytes, &operation, sizeof(operation));
}

/* Corrupt a link while retaining a structurally valid record envelope. */
static void
self_history_prior_defect(char bytes[BLCKSZ])
{
	UndoRecordHeader *record = (UndoRecordHeader *)(bytes + 256);
	char *tail = (char *)record + sizeof(*record) + record->payload_length
				 - sizeof(UndoOperationTrailer) - sizeof(UndoItlHistoryTrailer);
	UndoItlHistoryTrailer history;
	uint32 segment = self_binding.segment_id;
	ClusterItlSlotData *prior;

	memcpy(&history, tail, sizeof(history));
	prior = &history.entries[history.entries[0].block == 0 ? 0 : 1].prior;
	if (self_history_defect == SELF_HISTORY_PRIOR_SELF)
		prior->undo_segment_head = self_history_heads[0].undo_segment_head;
	if (self_history_defect == SELF_HISTORY_PRIOR_FOREIGN)
		prior->undo_segment_head
			= uba_encode((UT_PEER_NODE * CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1, 3, 3, 63);
	if (self_history_defect == SELF_HISTORY_PRIOR_TT)
		prior->undo_segment_head = uba_encode(segment, 4, 4, 63);
	if (self_history_defect == SELF_HISTORY_PRIOR_OWNER) {
		prior->xid++;
		prior->wrap--;
	}
	if (self_history_defect == SELF_HISTORY_PRIOR_FREE) {
		memset(prior, 0, sizeof(*prior));
		prior->wrap = 7;
	}
	if (self_history_defect == SELF_HISTORY_PRIOR_SCN)
		prior->write_scn--;
	memcpy(tail, &history, sizeof(history));
}

static void
self_history_store(const char bytes[BLCKSZ], unsigned index)
{
	const UndoRecordHeader *record = (const UndoRecordHeader *)(bytes + 256);
	unsigned page_index = self_history_cross_page && index >= self_history_records / 2;
	UndoBlockHeader *block = (UndoBlockHeader *)self_history_pages[page_index].data;
	uint32 segment, number;
	uint16 slot, row;
	UndoSlotDirEntry *directory;

	UT_ASSERT(uba_decode_record(self_history_heads[index].undo_segment_head, &segment, &number,
								&slot, &row));
	directory = (UndoSlotDirEntry *)(self_history_pages[page_index].data + BLCKSZ
									 - ((size_t)row + 1) * sizeof(*directory));
	*directory = (UndoSlotDirEntry){ .record_offset = block->free_offset,
									 .record_length = sizeof(*record) + record->payload_length,
									 .record_type = record->record_type,
									 .flags = record->flags };
	UT_ASSERT(block->free_offset + directory->record_length
			  <= BLCKSZ - block->slot_count * sizeof(*directory));
	memcpy(self_history_pages[page_index].data + block->free_offset, record,
		   directory->record_length);
	block->free_offset += directory->record_length;
}

/* V1 top records are page-history links, never child-operation receipts. */
static void
self_mixed_record(char bytes[BLCKSZ], unsigned index)
{
	UndoRecordHeader *record = (UndoRecordHeader *)(bytes + 256);
	UndoSlotDirEntry *directory;
	UndoItlHistoryTrailer history;
	uint16 length;
	uint32 segment, block;
	uint16 tt, row;
	bool last = index + 1 == self_history_records;
	bool plain = !last
				 && (self_mixed_mode == 2 || (self_mixed_mode == 1 && index == 1)
					 || (self_mixed_mode == 3 && index % 2 == 0));

	if (last && self_mixed_defect == 7) {
		UndoOperationTrailer operation;
		char *tail = (char *)(record + 1) + record->payload_length - sizeof(operation);
		memcpy(&operation, tail, sizeof(operation));
		operation.operation_sequence = self_history_records;
		memcpy(tail, &operation, sizeof(operation));
	}
	if (!plain)
		return;
	record->flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	record->xid = UT_RAW_XID;
	record->payload_length -= sizeof(UndoOperationTrailer);
	UT_ASSERT(cluster_undo_record_decode_payload(record, record + 1, record->payload_length,
												 &length, &history));
	if (index == (self_mixed_mode == 1 ? 1 : 0)) {
		if (self_mixed_defect == 1)
			record->xid++;
		if (self_mixed_defect == 2)
			record->tt_wrap_plus1++;
		if (self_mixed_defect == 3)
			record->target_locator.relNumber++;
		if (self_mixed_defect == 4) {
			history.entries[history.entries[0].block == 0 ? 0 : 1].slot_index++;
			memcpy((char *)(record + 1) + length, &history, sizeof(history));
		}
		if (self_mixed_defect == 5)
			record->prev_uba = self_history_heads[index].undo_segment_head;
	}
	UT_ASSERT(uba_decode_record(self_history_heads[index].undo_segment_head, &segment, &block, &tt,
								&row));
	directory = (UndoSlotDirEntry *)(bytes + BLCKSZ - ((size_t)row + 1) * sizeof(*directory));
	directory->record_length = sizeof(*record) + record->payload_length;
	directory->flags = record->flags;
}

static void
self_history_build(bool creator, const BufferTag *tag)
{
	ClusterItlSlotData root = ClusterPageGetItlSlots(ut_visibility_page.data)[0];
	PGAlignedBlock bytes;
	ClusterTxLocator locator = { 0 };

	UT_ASSERT(self_history_records == 3 || self_history_records == 24);
	memset(self_history_pages, 0, sizeof(self_history_pages));
	for (unsigned i = 0; i < 2; i++) {
		UndoBlockHeader *block = (UndoBlockHeader *)self_history_pages[i].data;
		block->magic = PGRAC_UNDO_BLOCK_MAGIC;
		block->block_version = UNDO_BLOCK_VERSION_1;
		block->slot_count = 65;
		block->free_offset = 256;
	}
	for (unsigned i = 0; i < self_history_records; i++) {
		unsigned page_index = self_history_cross_page && i >= self_history_records / 2;
		unsigned row_index = page_index ? i - self_history_records / 2 : i;
		self_history_heads[i] = root;
		self_history_heads[i].write_scn -= i;
		self_history_heads[i].undo_segment_head
			= uba_encode(self_binding.segment_id, 4 - page_index, 3, 64 - row_index);
	}
	for (unsigned i = 0; i < self_history_records; i++) {
		ClusterSavepointReaderRecord decoded;
		UndoRecordHeader *record = (UndoRecordHeader *)(bytes.data + 256);
		locator.xid = root.xid;
		locator.uba = self_history_heads[i].undo_segment_head;
		self_operation_record(bytes.data, &locator, &self_history_heads[i], tag, creator);
		if (self_update_layout != 0)
			self_update_record(bytes.data, creator);
		self_history_rewrite(bytes.data, i, creator);
		if (i == 0)
			self_history_prior_defect(bytes.data);
		UT_ASSERT(cluster_undo_record_decode_operation_payload(
			record, bytes.data + 256 + sizeof(*record), record->payload_length,
			&decoded.body_length, &decoded.history, &decoded.operation));
		if (self_mixed_mode != 0)
			self_mixed_record(bytes.data, i);
		self_history_store(bytes.data, i);
	}
}

static void
self_history_copy(const ClusterTxLocator *locator, const ClusterItlSlotData *head,
				  char data[BLCKSZ])
{
	unsigned index = self_operation_calls - 1;
	unsigned page_index = self_history_cross_page && index >= self_history_records / 2;
	UT_ASSERT(index < self_history_records);
	UT_ASSERT(
		cluster_undo_record_uba_equal(locator->uba, self_history_heads[index].undo_segment_head));
	UT_ASSERT_EQ(locator->xid, self_history_heads[index].xid);
	UT_ASSERT_EQ(locator->itl_slot_index, 0);
	UT_ASSERT_EQ(locator->tt_wrap, index == 0 ? TT_WRAP_INVALID : 7);
	UT_ASSERT_EQ(head->wrap, self_history_heads[index].wrap);
	memcpy(data, self_history_pages[page_index].data, BLCKSZ);
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_own_operation_head_origin_admitted(
	const ClusterTxLocator *locator, const ClusterItlSlotData *head, const BufferTag *tag,
	const ClusterSavepointOperation *owner, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterSavepointReaderIdentity *identity,
	ClusterTxResolution *top, ClusterSavepointReaderRecord *operation, char data[BLCKSZ],
	ClusterTxResolveReason *reason)
{
	PGAlignedBlock record;
	ClusterTxLocator exact = *locator;
	HeapTupleHeader tuple = (HeapTupleHeader)(ut_visibility_page.data + 1024);
	size_t length;

	self_operation_calls++;
	UT_ASSERT(self_child);
	UT_ASSERT(self_admission_held && admission->entered);
	UT_ASSERT_EQ(mode, CLUSTER_TX_RESOLVE_VISIBILITY);
	if (self_owner_full_pipe) {
		memcpy(data, self_owner_full.undo.data, BLCKSZ);
		if (!cluster_savepoint_reader_discover_own_head(owner, head, tag, locator, data, identity))
			return CLUSTER_TX_UNKNOWN;
		exact.tt_wrap = identity->canonical.wrap;
		if (!cluster_savepoint_reader_extract_identity(identity, &exact, data, record.data, &length,
													   operation))
			return CLUSTER_TX_UNKNOWN;
		return cluster_tx_resolve_exact(&exact, mode, top, reason);
	}
	if (self_operation_defect == 18)
		pg_re_throw();
	if (self_history_records != 0) {
		if (self_operation_calls == 2 && self_history_defect == SELF_HISTORY_ERROR)
			pg_re_throw();
		if (self_operation_calls == 2 && self_history_defect == SELF_HISTORY_UNKNOWN)
			return CLUSTER_TX_UNKNOWN;
		self_history_copy(locator, head, data);
	} else {
		self_operation_record(data, locator, head, tag,
							  (tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
		if (self_update_layout != 0)
			self_update_record(data, (tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
	}
	if (!cluster_savepoint_reader_discover_own_head(owner, head, tag, locator, data, identity))
		return CLUSTER_TX_UNKNOWN;
	exact.tt_wrap = identity->canonical.wrap;
	if (!cluster_savepoint_reader_extract_identity(identity, &exact, data, record.data, &length,
												   operation))
		return CLUSTER_TX_UNKNOWN;
	if (self_history_records != 0) {
		/* The legacy canonical-status fixture only accepts its original root.
		 * Retain that independent top sample and echo the addressed DATA. */
		ClusterTxLocator sampled = *locator;
		sampled.uba = self_history_heads[0].undo_segment_head;
		sampled.tt_wrap = TT_WRAP_INVALID;
		(void)cluster_tx_resolve_exact(&sampled, CLUSTER_TX_RESOLVE_VISIBILITY, top, reason);
		top->locator_echo = *locator;
		top->locator_echo.tt_wrap = 7;
	} else
		(void)cluster_tx_resolve_exact(locator, CLUSTER_TX_RESOLVE_VISIBILITY, top, reason);
	if (self_operation_defect == 6)
		top->outcome = CLUSTER_TX_UNKNOWN;
	if (self_operation_defect == 7)
		top->outcome = CLUSTER_TX_PREPARED;
	if (self_operation_defect == 8)
		ut_current_epoch++;
	if (self_operation_defect == 9)
		self_binding.segment_generation++;
	if (self_operation_defect == 13)
		top->local_canonical_generation++;
	return top->outcome;
}

static void
self_reset_case(bool peer, int defect)
{
	static ComboCidKeyData pair = { 0, 2 };

	ut_reset(CLUSTER_TT_STATUS_IN_PROGRESS, InvalidScn);
	ut_exit_fixture = ut_exit_exact_proof = ut_full_scratch_fixture = true;
	ut_full_scratch_scenario = 2;
	ut_current_epoch = UT_CLUSTER_EPOCH;
	ut_exit_ref = ut_exact_peer_ref();
	ut_exit_ref.origin_node_id = peer ? UT_PEER_NODE : UT_SELF_NODE;
	ut_exit_ref.cluster_epoch = ut_current_epoch;
	ut_memo_hit = false;
	ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	ut_origin_verdict.commit_scn = InvalidScn;
	cluster_crossnode_runtime_visibility = true;
	self_current = true;
	self_binding_present = defect != 6;
	self_cid_calls = 0;
	self_operation_calls = 0;
	self_admission_enters = self_admission_leaves = 0;
	self_admission_held = false;
	cluster_shared_config = self_child;
	self_drift = defect <= 5 || defect == 19 ? defect : 0;
	memset(&self_binding, 0, sizeof(self_binding));
	self_binding.segment_id = UT_SELF_NODE * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	self_binding.segment_generation = 2;
	self_binding.xid = UT_RAW_XID;
	self_binding.slot_offset = 3;
	self_binding.slot_wrap = 7;
	self_binding.origin_instance = UT_SELF_NODE + 1;
	self_binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	self_binding.active_lsn = UT_ANCHOR_LSN;
	if (defect == 7)
		self_binding.slot_wrap++;
	if (defect == 8)
		self_binding.segment_generation = UINT32_MAX;
	if (defect == 9)
		self_binding.origin_instance++;
	if (defect == 10)
		self_binding.publish_state = CLUSTER_CANONICAL_TXN_RESERVED;
	if (defect == 11)
		self_binding.active_lsn = InvalidXLogRecPtr;
	if (defect == 16)
		self_binding.slot_offset++;
	if (defect == 17)
		self_binding.segment_id++;
	if (defect == 18)
		self_binding.segment_generation++;
	comboCids = &pair;
	usedComboCids = 1;
}

/* The queried tuple stays at LP1; same-page UPDATE also has the other role. */
static void
self_update_tuple(HeapTuple tuple, bool creator)
{
	HeapTupleHeader other = (HeapTupleHeader)(ut_visibility_page.data + 1088);
	HeapTupleHeader old = creator ? other : tuple->t_data;
	HeapTupleHeader newer = creator ? tuple->t_data : other;
	BlockNumber new_block = !creator && self_update_layout == 2 ? 1 : 0;
	OffsetNumber new_offset = !creator && self_update_layout == 1 ? 2 : 1;

	HeapTupleHeaderSetXmin(old, FrozenTransactionId);
	old->t_infomask = HEAP_XMIN_FROZEN;
	HeapTupleHeaderSetXmax(old, UT_RAW_XID + 1);
	old->t_choice.t_heap.t_field3.t_cid = 2;
	ItemPointerSet(&old->t_ctid, new_block, new_offset);
	HeapTupleHeaderSetXmin(newer, UT_RAW_XID + 1);
	newer->t_infomask = HEAP_XMAX_INVALID | HEAP_UPDATED;
	newer->t_choice.t_heap.t_field3.t_cid = 2;
	ItemPointerSet(&newer->t_ctid, new_block, new_offset);
	other->t_hoff = SizeofHeapTupleHeader;
	if (self_update_layout == 1) {
		PageHeader page = (PageHeader)ut_visibility_page.data;
		Size other_length = SizeofHeapTupleHeader + (creator && self_update_defect == 4 ? 4 : 0);

		page->pd_lower += sizeof(ItemIdData);
		ItemIdSetNormal(PageGetItemId(ut_visibility_page.data, 2), 1088, other_length);
		memset((char *)other + SizeofHeapTupleHeader, 0x5a, other_length - SizeofHeapTupleHeader);
		old->t_infomask2 |= HEAP_HOT_UPDATED;
		newer->t_infomask2 |= HEAP_ONLY_TUPLE;
	}
}

static void
self_setup_tuple(bool creator, bool combo, bool peer, int defect, HeapTuple tuple, BufferTag *tag)
{
	HeapTupleHeader header;
	PageHeader page_header;
	ClusterItlSlotData *slot;

	memset(ut_visibility_page.data, 0, BLCKSZ);
	page_header = (PageHeader)ut_visibility_page.data;
	page_header->pd_flags = PD_HAS_ITL;
	page_header->pd_lower = SizeOfPageHeaderData;
	page_header->pd_upper = 1024;
	page_header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	page_header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSN(ut_visibility_page.data, UT_ANCHOR_LSN);
	slot = ClusterPageGetItlSlots(ut_visibility_page.data);
	slot->flags = ITL_FLAG_ACTIVE;
	slot->xid = UT_RAW_XID;
	slot->wrap = 7;
	if (self_child)
		slot->write_scn = UT_READ_SCN - 1;
	slot->undo_segment_head = uba_encode(
		(peer ? UT_PEER_NODE : UT_SELF_NODE) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 4, 3, 64);
	header = (HeapTupleHeader)(ut_visibility_page.data + 1024);
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_itl_slot_idx = 0;
	header->t_infomask = creator || combo ? 0 : HEAP_XMIN_FROZEN;
	if (creator && !combo)
		header->t_infomask |= HEAP_XMAX_INVALID;
	if (combo)
		header->t_infomask |= HEAP_COMBOCID;
	if (!creator)
		header->t_infomask |= HEAP_UPDATED;
	HeapTupleHeaderSetXmin(header, self_child && creator ? UT_RAW_XID + 1 : UT_RAW_XID);
	HeapTupleHeaderSetXmax(header, creator && !combo ? InvalidTransactionId
								   : self_child		 ? UT_RAW_XID + 1
													 : UT_RAW_XID);
	header->t_choice.t_heap.t_field3.t_cid = combo ? 0 : 2;
	if (defect == 12)
		header->t_choice.t_heap.t_field3.t_cid = 999;
	if (defect == 13)
		comboCids = NULL;
	if (defect == 14)
		header->t_choice.t_heap.t_field3.t_cid = InvalidCommandId;
	if (defect == 15)
		header->t_infomask |= HEAP_MOVED_OFF;
	ItemPointerSet(&tuple->t_self, 0, 1);
	header->t_ctid = tuple->t_self;
	tuple->t_data = header;
	tuple->t_len = SizeofHeapTupleHeader;
	tuple->t_tableOid = FirstNormalObjectId;
	if (self_child) {
		page_header->pd_lower += sizeof(ItemIdData);
		ItemIdSetNormal(PageGetItemId(ut_visibility_page.data, 1), 1024, tuple->t_len);
		tag->spcOid = 1663;
		tag->dbOid = 5;
		tag->relNumber = 12345;
	}
	if (self_update_layout != 0)
		self_update_tuple(tuple, creator);
}

static void
self_case(bool creator, bool combo, CommandId curcid, bool peer, int defect, bool expected_visible,
		  bool expected_error)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	ClusterR4HotScratchTestContext context = { 0 };
	PGAlignedBlock before;
	volatile bool caught = false;
	volatile bool visible = false;

	self_reset_case(peer, defect);
	self_setup_tuple(creator, combo, peer, defect, &tuple, &context.tag);
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = UT_CLUSTER_EPOCH;
	snapshot.read_scn = UT_READ_SCN;
	snapshot.curcid = curcid;
	context.scratch_page = ut_visibility_page.data;
	context.already_full = true;
	context.read_scn = snapshot.read_scn;
	context.logical_root = tuple.t_self;
	context.tag.blockNum = 0;
	if (self_history_records != 0)
		self_history_build(creator, &context.tag);
	memcpy(before.data, ut_visibility_page.data, BLCKSZ);
	ut_error_armed = true;
	PG_TRY();
	{
		visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	ut_error_armed = false;
	UT_ASSERT_EQ(caught, expected_error);
	if (!caught)
		UT_ASSERT_EQ(visible, expected_visible);
	if ((peer || expected_error) && !(self_child && self_operation_defect == 5)
		&& self_history_defect != SELF_HISTORY_CID)
		UT_ASSERT_EQ(self_cid_calls, 0);
	if (self_history_records != 0) {
		UT_ASSERT_EQ(self_operation_calls, self_history_expected_calls);
		UT_ASSERT_EQ(self_admission_enters, self_history_expected_calls);
		UT_ASSERT_EQ(self_admission_leaves, self_history_expected_calls);
		UT_ASSERT(!self_admission_held);
		UT_ASSERT_EQ(self_cid_calls, !expected_error || self_history_defect == SELF_HISTORY_CID);
	} else if (self_child) {
		bool refused = self_operation_defect >= 15 && self_operation_defect <= 17;

		UT_ASSERT_EQ(self_operation_calls, refused ? 0 : 1);
		UT_ASSERT_EQ(self_admission_enters, self_operation_defect == 17 ? 0 : 1);
		UT_ASSERT_EQ(self_admission_leaves, refused ? 0 : 1);
		UT_ASSERT(!self_admission_held);
	}
	UT_ASSERT_EQ(memcmp(before.data, ut_visibility_page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(ut_calls.clog, 0);
	UT_ASSERT_EQ(ut_hint_mutations, 0);
	UT_ASSERT(!cluster_vis_resolve_in_flight());
	UT_ASSERT(PG_exception_stack == NULL);
	UT_ASSERT(ut_snapshot_scope == NULL);
	comboCids = NULL;
	usedComboCids = 0;
}

UT_TEST(test_existing_delete_current_command) { self_case(false,false,2,false,0,true,false); }
UT_TEST(test_existing_delete_next_command) { self_case(false,false,3,false,0,false,false); }
UT_TEST(test_update_old_version_next_command) { self_case(false,false,4,false,0,false,false); }
UT_TEST(test_creator_same_and_next_command) {
	self_case(true,false,2,false,0,false,false); self_case(true,false,3,false,0,true,false);
}
UT_TEST(test_combo_creator_deleter_command_boundaries) {
	self_case(true,true,0,false,0,false,false); self_case(true,true,1,false,0,true,false);
	self_case(true,true,2,false,0,true,false); self_case(true,true,3,false,0,false,false);
}
UT_TEST(test_foreign_equal_raw_xid_never_uses_local_cids) {
	self_case(false,false,3,true,0,true,false); self_case(true,true,3,true,0,false,false);
}
UT_TEST(test_exact_or_publication_drift_is_not_self) {
	for (int defect=1; defect<=11; defect++) self_case(false,false,3,false,defect,false,true);
	for (int defect=16; defect<=19; defect++) self_case(false,false,3,false,defect,false,true);
}
UT_TEST(test_unknown_combo_map_stays_fail_closed) { for (int defect=12; defect<=15; defect++) self_case(true,true,3,false,defect,false,true); }

UT_TEST(test_own_child_creator_command_boundaries)
{
	self_child = true;
	self_operation_defect = 0;
	self_case(true, false, 2, false, 0, false, false);
	self_case(true, false, 3, false, 0, true, false);
	self_child = false;
}
UT_TEST(test_own_child_delete_command_boundaries)
{
	self_child = true;
	self_operation_defect = 0;
	self_case(false, false, 2, false, 0, true, false);
	self_case(false, false, 3, false, 0, false, false);
	self_child = false;
}
UT_TEST(test_own_child_operation_identity_refusals)
{
	self_child = true;
	for (int defect = 1; defect <= 14; defect++) {
		if (defect == 5)
			continue;
		self_operation_defect = defect;
		self_case(true, false, 3, false, 0, false, true);
		self_case(false, false, 3, false, 0, false, true);
	}
	self_child = false;
	self_operation_defect = 0;
}
UT_TEST(test_own_child_operation_cid_matches_tuple)
{
	self_child = true;
	self_operation_defect = 5;
	self_case(true, false, 3, false, 0, false, true);
	UT_ASSERT_EQ(self_cid_calls, 1);
	self_case(false, false, 3, false, 0, false, true);
	UT_ASSERT_EQ(self_cid_calls, 1);
	self_child = false;
	self_operation_defect = 0;
}
UT_TEST(test_own_child_admission_refusals_and_error_unwind)
{
	self_child = true;
	for (int defect = 15; defect <= 18; defect++) {
		self_operation_defect = defect;
		self_case(true, false, 3, false, 0, false, true);
		self_case(false, false, 3, false, 0, false, true);
	}
	self_child = false;
	self_operation_defect = 0;
}

UT_TEST(test_own_child_update_new_version_commands)
{
	self_child = true;
	for (self_update_layout = 1; self_update_layout <= 2; self_update_layout++) {
		self_update_defect = 0;
		self_case(true, false, 2, false, 0, false, false);
		self_case(true, false, 3, false, 0, true, false);
		self_update_defect = 4;
		self_case(true, false, 3, false, 0, true, false);
	}
	self_child = false;
	self_update_layout = self_update_defect = 0;
}
UT_TEST(test_own_child_update_old_version_commands)
{
	self_child = true;
	for (self_update_layout = 1; self_update_layout <= 2; self_update_layout++) {
		self_case(false, false, 2, false, 0, true, false);
		self_case(false, false, 3, false, 0, false, false);
	}
	self_child = false;
	self_update_layout = 0;
}
UT_TEST(test_own_child_update_targets_are_role_specific)
{
	self_child = true;
	for (self_update_layout = 1; self_update_layout <= 2; self_update_layout++) {
		for (self_update_defect = 1; self_update_defect <= 2; self_update_defect++) {
			self_case(true, false, 3, false, 0, false, true);
			self_case(false, false, 3, false, 0, false, true);
		}
	}
	self_child = false;
	self_update_layout = self_update_defect = 0;
}
UT_TEST(test_own_child_update_old_image_is_checked)
{
	self_child = true;
	for (self_update_layout = 1; self_update_layout <= 2; self_update_layout++) {
		for (self_update_defect = 3; self_update_defect <= 4; self_update_defect++)
			self_case(false, false, 3, false, 0, false, true);
		for (self_update_defect = 7; self_update_defect <= 9; self_update_defect++) {
			self_case(true, false, 3, false, 0, false, true);
			self_case(false, false, 3, false, 0, false, true);
		}
	}
	self_child = false;
	self_update_layout = self_update_defect = 0;
}
UT_TEST(test_own_child_update_command_id_matches_tuple)
{
	self_child = true;
	self_operation_defect = 5;
	for (self_update_layout = 1; self_update_layout <= 2; self_update_layout++) {
		self_case(true, false, 3, false, 0, false, true);
		UT_ASSERT_EQ(self_cid_calls, 1);
		self_case(false, false, 3, false, 0, false, true);
		UT_ASSERT_EQ(self_cid_calls, 1);
	}
	self_child = false;
	self_operation_defect = self_update_layout = 0;
}

static void
self_history_case(bool creator, CommandId curcid, bool expected_visible, bool expected_error,
				  int expected_calls)
{
	self_child = true;
	self_history_expected_calls = expected_calls;
	self_case(creator, false, curcid, false, 0, expected_visible, expected_error);
	self_child = false;
}

UT_TEST(test_own_child_history_insert_delete_commands)
{
	for (int length = 3; length <= 24; length += 21) {
		self_history_records = length;
		for (int cross = 0; cross <= 1; cross++) {
			self_history_cross_page = cross;
			self_history_case(true, 2, false, false, length);
			self_history_case(true, 3, true, false, length);
			self_history_case(false, 2, true, false, length);
			self_history_case(false, 3, false, false, length);
		}
	}
	self_history_records = 0;
	self_history_cross_page = false;
}
UT_TEST(test_own_child_history_update_commands)
{
	for (int length = 3; length <= 24; length += 21) {
		self_history_records = length;
		for (self_update_layout = 1; self_update_layout <= 2; self_update_layout++) {
			for (int cross = 0; cross <= 1; cross++) {
				self_history_cross_page = cross;
				self_history_case(true, 2, false, false, length);
				self_history_case(true, 3, true, false, length);
				self_history_case(false, 2, true, false, length);
				self_history_case(false, 3, false, false, length);
			}
		}
	}
	self_history_records = self_update_layout = 0;
	self_history_cross_page = false;
}
UT_TEST(test_own_child_history_identity_and_disposition_refusals)
{
	self_history_records = 3;
	for (int cross = 0; cross <= 1; cross++) {
		self_history_cross_page = cross;
		for (self_history_defect = SELF_HISTORY_SEQUENCE; self_history_defect <= SELF_HISTORY_CID;
			 self_history_defect++) {
			int calls = self_history_defect <= SELF_HISTORY_GENERATION ? 2 : 3;
			self_history_case(true, 3, false, true, calls);
			self_history_case(false, 3, false, true, calls);
		}
	}
	self_history_records = 0;
	self_history_defect = SELF_HISTORY_OK;
	self_history_cross_page = false;
}
UT_TEST(test_own_child_history_prior_refusals)
{
	self_history_records = 3;
	for (int cross = 0; cross <= 1; cross++) {
		self_history_cross_page = cross;
		for (self_history_defect = SELF_HISTORY_PRIOR_SELF;
			 self_history_defect <= SELF_HISTORY_PRIOR_SCN; self_history_defect++) {
			int calls = self_history_defect == SELF_HISTORY_PRIOR_SCN ? 2 : 1;
			self_history_case(true, 3, false, true, calls);
			self_history_case(false, 3, false, true, calls);
		}
	}
	self_history_records = 0;
	self_history_defect = SELF_HISTORY_OK;
	self_history_cross_page = false;
}
UT_TEST(test_own_child_history_error_unwinds_every_read)
{
	self_history_records = 3;
	for (self_history_defect = SELF_HISTORY_ERROR; self_history_defect <= SELF_HISTORY_UNKNOWN;
		 self_history_defect++) {
		self_history_case(true, 3, false, true, 2);
		self_history_case(false, 3, false, true, 2);
	}
	self_history_records = 0;
	self_history_defect = SELF_HISTORY_OK;
}
UT_TEST(test_own_child_history_matching_bad_image_stops)
{
	self_history_records = 3;
	self_history_defect = SELF_HISTORY_OLD_XMIN;
	self_history_case(false, 3, false, true, 1);
	self_update_layout = 1;
	self_history_case(false, 3, false, true, 1);
	self_history_defect = SELF_HISTORY_OLD_HOFF;
	self_history_case(false, 3, false, true, 1);
	self_history_case(true, 3, false, true, 1);
	self_history_records = self_update_layout = 0;
	self_history_defect = SELF_HISTORY_OK;
}

UT_TEST(test_own_child_mixed_top_history_commands)
{
	for (int length = 3; length <= 24; length += 21) {
		self_history_records = length;
		for (self_mixed_mode = 1; self_mixed_mode <= 3; self_mixed_mode++)
			for (int cross = 0; cross < 2; cross++) {
				self_history_cross_page = cross;
				for (self_update_layout = 0; self_update_layout <= 2; self_update_layout++) {
					self_history_case(true, 2, false, false, length);
					self_history_case(true, 3, true, false, length);
					self_history_case(false, 2, true, false, length);
					self_history_case(false, 3, false, false, length);
				}
			}
	}
	self_history_records = self_update_layout = self_mixed_mode = 0;
	self_history_cross_page = false;
}
UT_TEST(test_own_child_mixed_top_history_refusals)
{
	self_history_records = 3;
	for (self_mixed_mode = 1; self_mixed_mode <= 2; self_mixed_mode++)
		for (int cross = 0; cross < 2; cross++) {
			self_history_cross_page = cross;
			for (self_mixed_defect = 1; self_mixed_defect <= 7; self_mixed_defect++) {
				int calls = self_mixed_defect == 7 ? 3 : self_mixed_mode == 1 ? 2 : 1;
				if (self_mixed_defect == 6 || (self_mixed_defect == 7 && self_mixed_mode == 2))
					continue;
				self_history_case(true, 3, false, true, calls);
				self_history_case(false, 3, false, true, calls);
			}
		}
	self_history_records = self_mixed_mode = self_mixed_defect = 0;
	self_history_cross_page = false;
}
UT_TEST(test_own_child_mixed_top_history_error_unwinds)
{
	self_history_records = 3;
	self_mixed_mode = 2;
	for (self_history_defect = SELF_HISTORY_ERROR; self_history_defect <= SELF_HISTORY_UNKNOWN;
		 self_history_defect++) {
		self_history_case(true, 3, false, true, 2);
		self_history_case(false, 3, false, true, 2);
	}
	self_history_records = self_mixed_mode = 0;
	self_history_defect = SELF_HISTORY_OK;
}

#include "test_cluster_scratch_nonowner.inc"

UT_TEST(test_real_owner_full_producer_reaches_native_self_cid_consumer)
{
	FILE *producer = popen("./test_cluster_r4_tx_outcome --owner-full-fixture", "r");
	bool received;
	int status;

	UT_ASSERT(producer != NULL);
	if (producer == NULL)
		return;
	received = fread(&self_owner_full, sizeof(self_owner_full), 1, producer) == 1;
	status = pclose(producer);
	UT_ASSERT(received && status == 0 && self_owner_full.magic == TEST_OWNER_FULL_MAGIC);
	if (!received || status != 0 || self_owner_full.magic != TEST_OWNER_FULL_MAGIC)
		return;
	for (unsigned next = 0; next < 2; next++) {
		HeapTupleData tuple = { 0 };
		SnapshotData snapshot = { 0 };
		ClusterR4HotScratchTestContext context = { 0 };
		Page page = (Page)ut_visibility_page.data;
		PGAlignedBlock before;
		volatile bool visible = false, caught = false;

		self_child = true;
		self_reset_case(false, 0);
		self_owner_full_pipe = true;
		cluster_node_id = self_owner_full.expected.top.origin_instance - 1;
		self_binding = self_owner_full.expected.top;
		memcpy(page, self_owner_full.full.data, BLCKSZ);
		memcpy(before.data, page, BLCKSZ);
		tuple.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 3));
		tuple.t_len = ItemIdGetLength(PageGetItemId(page, 3));
		tuple.t_self = tuple.t_data->t_ctid;
		tuple.t_tableOid = FirstNormalObjectId;
		snapshot.snapshot_type = SNAPSHOT_MVCC;
		snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
		snapshot.read_epoch = ut_current_epoch;
		snapshot.read_scn = self_owner_full.read_scn;
		snapshot.curcid = self_owner_full.expected.command_id + next;
		context.scratch_page = page;
		context.already_full = true;
		context.read_scn = snapshot.read_scn;
		context.logical_root = tuple.t_self;
		context.tag = self_owner_full.tag;
		ut_error_armed = true;
		PG_TRY();
		{
			visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		ut_error_armed = false;
		if (caught)
			printf("# owner FULL consumer rejected: %s; %s\n", ut_error_message, ut_error_detail);
		UT_ASSERT(!caught);
		UT_ASSERT_EQ(visible, next != 0);
		UT_ASSERT_EQ(self_cid_calls, 1);
		UT_ASSERT_EQ(self_operation_calls, 1);
		UT_ASSERT_EQ(self_admission_enters, self_admission_leaves);
		UT_ASSERT(!self_admission_held);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
		self_owner_full_pipe = false;
	}
	self_child = false;
	cluster_node_id = UT_SELF_NODE;
}

#include "test_cluster_scratch_peer.inc"

int
main(void)
{
	UT_PLAN(36);
	UT_RUN(test_existing_delete_current_command);
	UT_RUN(test_existing_delete_next_command);
	UT_RUN(test_update_old_version_next_command);
	UT_RUN(test_creator_same_and_next_command);
	UT_RUN(test_combo_creator_deleter_command_boundaries);
	UT_RUN(test_foreign_equal_raw_xid_never_uses_local_cids);
	UT_RUN(test_exact_or_publication_drift_is_not_self);
	UT_RUN(test_unknown_combo_map_stays_fail_closed);
	UT_RUN(test_own_child_creator_command_boundaries);
	UT_RUN(test_own_child_delete_command_boundaries);
	UT_RUN(test_own_child_operation_identity_refusals);
	UT_RUN(test_own_child_operation_cid_matches_tuple);
	UT_RUN(test_own_child_admission_refusals_and_error_unwind);
	UT_RUN(test_own_child_update_new_version_commands);
	UT_RUN(test_own_child_update_old_version_commands);
	UT_RUN(test_own_child_update_targets_are_role_specific);
	UT_RUN(test_own_child_update_old_image_is_checked);
	UT_RUN(test_own_child_update_command_id_matches_tuple);
	UT_RUN(test_own_child_history_insert_delete_commands);
	UT_RUN(test_own_child_history_update_commands);
	UT_RUN(test_own_child_history_identity_and_disposition_refusals);
	UT_RUN(test_own_child_history_prior_refusals);
	UT_RUN(test_own_child_history_error_unwinds_every_read);
	UT_RUN(test_own_child_history_matching_bad_image_stops);
	UT_RUN(test_own_child_mixed_top_history_commands);
	UT_RUN(test_own_child_mixed_top_history_refusals);
	UT_RUN(test_own_child_mixed_top_history_error_unwinds);
	UT_RUN(test_nonowner_live_child_outcomes_and_roles);
	UT_RUN(test_nonowner_history_mixed_live_roles);
	UT_RUN(test_nonowner_unknown_identity_discard_and_admission_refuse);
	UT_RUN(test_nonowner_history_identity_and_terminal_stability);
	UT_RUN(test_nonowner_initial_v1_never_fabricates_owner);
	UT_RUN(test_nonowner_page_prior_and_matched_bad_image_refuse);
	printf("# nonowner subcases=%d\n", nonowner_cases);
	UT_RUN(test_real_owner_full_producer_reaches_native_self_cid_consumer);
	UT_RUN(test_peer_full_raw_child_verdict_never_authorizes_operation);
	UT_RUN(test_peer_full_possible_child_refusals_cover_both_tuple_roles);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
