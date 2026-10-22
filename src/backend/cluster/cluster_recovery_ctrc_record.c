/*-------------------------------------------------------------------------
 * cluster_recovery_ctrc_record.c
 *    Field-coded recovery CTRC release payload, without a WAL writer.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "common/cryptohash.h"
#include "port/pg_crc32c.h"
#include "replication/origin.h"
#include "utils/memutils.h"

typedef struct CTRCGraphNodeWork {
	ClusterRecoveryCTRCGraphBytesV2 id;
	ClusterRecoveryCTRCGraphBytesV2 evidence;
	size_t original;
	bool owner;
	bool seen;
} CTRCGraphNodeWork;

typedef struct CTRCGraphEdgeWork {
	ClusterRecoveryCTRCGraphBytesV2 bytes;
	size_t from;
	size_t to;
	size_t next;
} CTRCGraphEdgeWork;

/* Bound the combined scratch before allocation, including multiplication. */
static bool
ctrc_graph_size_add(size_t *total, size_t count, size_t width)
{
	if (width == 0 || count > (MaxAllocSize - *total) / width)
		return false;
	*total += count * width;
	return true;
}

static int
ctrc_graph_bytes_cmp(const ClusterRecoveryCTRCGraphBytesV2 *a,
					 const ClusterRecoveryCTRCGraphBytesV2 *b)
{
	int cmp = memcmp(a->data, b->data, Min(a->length, b->length));

	return cmp != 0 ? cmp : (a->length > b->length) - (a->length < b->length);
}

static int
ctrc_graph_node_cmp(const void *a, const void *b)
{
	return ctrc_graph_bytes_cmp(&((const CTRCGraphNodeWork *)a)->id,
								&((const CTRCGraphNodeWork *)b)->id);
}

static int
ctrc_graph_edge_cmp(const void *a, const void *b)
{
	return ctrc_graph_bytes_cmp(&((const CTRCGraphEdgeWork *)a)->bytes,
								&((const CTRCGraphEdgeWork *)b)->bytes);
}

static int
ctrc_graph_rank_cmp(const void *a, const void *b)
{
	size_t left = *(const size_t *)a, right = *(const size_t *)b;

	return (left > right) - (left < right);
}

static uint8 *
ctrc_graph_put_bytes(uint8 *next, const ClusterRecoveryCTRCGraphBytesV2 *bytes)
{
	cluster_undo_tt_ctrc_put_u64_le(next, bytes->length);
	memcpy(next + 8, bytes->data, bytes->length);
	return next + 8 + bytes->length;
}

static bool
ctrc_graph_hash_u64(pg_cryptohash_ctx *hash, uint64 value)
{
	uint8 bytes[8];

	cluster_undo_tt_ctrc_put_u64_le(bytes, value);
	return pg_cryptohash_update(hash, bytes, sizeof(bytes)) == 0;
}

static bool
ctrc_graph_hash_bytes(pg_cryptohash_ctx *hash, const ClusterRecoveryCTRCGraphBytesV2 *bytes)
{
	return ctrc_graph_hash_u64(hash, bytes->length)
		   && pg_cryptohash_update(hash, bytes->data, bytes->length) == 0;
}

bool
cluster_recovery_ctrc_graph_hash_v2(const ClusterRecoveryCTRCReleaseV2 *record,
									uint64 system_identifier,
									const ClusterRecoveryCTRCGraphV2 *graph, uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-D0-OBLIGATION-GRAPH-V2";
	ClusterRecoveryCTRCReleaseV2 value;
	ClusterRecoveryCTRCGraphV2 view;
	CTRCGraphNodeWork *nodes = NULL;
	CTRCGraphEdgeWork *edges = NULL;
	size_t *workspace = NULL, *rank, *heads, *queue, *roots;
	uint8 *arena = NULL, *next;
	size_t scratch = 0, arena_size = 0, node_count = 0, edge_count = 0;
	size_t first = 0, last = 0;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES], digest[32];
	pg_cryptohash_ctx *hash = NULL;
	bool owner = false, ok = false;

	if (out == NULL)
		return false;
	if (record == NULL || system_identifier == 0 || graph == NULL)
		goto done;
	view = *graph;
	/* Count bounds precede any access to supplied arrays. No caller count,
	 * native pointer/index or struct padding enters the transcript. */
	if (view.node_count == 0 || view.root_count == 0 || view.root_count > view.node_count
		|| view.nodes == NULL || view.roots == NULL || (view.edge_count != 0 && view.edges == NULL)
		|| !ctrc_graph_size_add(&scratch, view.node_count, sizeof(*nodes))
		|| !ctrc_graph_size_add(&scratch, view.edge_count, sizeof(*edges))
		|| !ctrc_graph_size_add(&scratch, view.node_count, 3 * sizeof(size_t))
		|| !ctrc_graph_size_add(&scratch, view.root_count, sizeof(size_t)))
		goto done;
	for (size_t i = 0; i < view.node_count; i++) {
		const ClusterRecoveryCTRCGraphNodeV2 *node = &view.nodes[i];

		if ((node->kind != 1 && node->kind != 2) || node->key.data == NULL || node->key.length == 0
			|| node->evidence.data == NULL || node->evidence.length == 0
			|| node->key.length > MaxAllocSize - 18
			|| node->evidence.length > MaxAllocSize - 18 - node->key.length
			|| !ctrc_graph_size_add(&arena_size, 1, 10 + node->key.length))
			goto done;
	}
	for (size_t i = 0; i < view.edge_count; i++) {
		const ClusterRecoveryCTRCGraphEdgeV2 *edge = &view.edges[i];

		if (edge->kind < 1 || edge->kind > 4 || edge->from >= view.node_count
			|| edge->to >= view.node_count || !ctrc_graph_size_add(&arena_size, 1, 38)
			|| !ctrc_graph_size_add(&arena_size, 1, view.nodes[edge->from].key.length)
			|| !ctrc_graph_size_add(&arena_size, 1, view.nodes[edge->to].key.length))
			goto done;
	}
	if (!ctrc_graph_size_add(&scratch, 1, arena_size))
		goto done;
	for (size_t i = 0; i < view.root_count; i++)
		if (view.roots[i] >= view.node_count)
			goto done;

	/* Reuse the complete codec's field/identity checks, but not the graph
	 * digest being calculated. Both digest fields at 192..255 are excluded. */
	value = *record;
	memset(value.graph_sha256, 1, sizeof(value.graph_sha256));
	if (!cluster_recovery_ctrc_encode_v2(&value, system_identifier, payload, sizeof(payload)))
		goto done;
	nodes = palloc_extended(view.node_count * sizeof(*nodes), MCXT_ALLOC_NO_OOM);
	if (nodes == NULL)
		goto done;
	if (view.edge_count != 0) {
		edges = palloc_extended(view.edge_count * sizeof(*edges), MCXT_ALLOC_NO_OOM);
		if (edges == NULL)
			goto done;
	}
	workspace = palloc_extended((3 * view.node_count + view.root_count) * sizeof(size_t),
								MCXT_ALLOC_NO_OOM);
	if (workspace == NULL)
		goto done;
	arena = palloc_extended(arena_size, MCXT_ALLOC_NO_OOM);
	if (arena == NULL)
		goto done;
	rank = workspace;
	heads = rank + view.node_count;
	queue = heads + view.node_count;
	roots = queue + view.node_count;
	next = arena;
	for (size_t i = 0; i < view.node_count; i++) {
		const ClusterRecoveryCTRCGraphNodeV2 *node = &view.nodes[i];

		nodes[i].id = (ClusterRecoveryCTRCGraphBytesV2){ next, 10 + node->key.length };
		nodes[i].evidence = node->evidence;
		nodes[i].original = i;
		nodes[i].owner = node->kind == 2;
		nodes[i].seen = false;
		cluster_undo_tt_ctrc_put_u16_le(next, node->kind);
		next = ctrc_graph_put_bytes(next + 2, &node->key);
	}
	qsort(nodes, view.node_count, sizeof(*nodes), ctrc_graph_node_cmp);
	for (size_t i = 0; i < view.node_count; i++) {
		CTRCGraphNodeWork node = nodes[i];

		if (node_count != 0 && ctrc_graph_bytes_cmp(&nodes[node_count - 1].id, &node.id) == 0) {
			if (ctrc_graph_bytes_cmp(&nodes[node_count - 1].evidence, &node.evidence) != 0)
				goto done;
		} else
			nodes[node_count++] = node;
		rank[node.original] = node_count - 1;
	}
	for (size_t i = 0; i < view.root_count; i++)
		roots[i] = rank[view.roots[i]];
	qsort(roots, view.root_count, sizeof(*roots), ctrc_graph_rank_cmp);
	for (size_t i = 1; i < view.root_count; i++)
		if (roots[i - 1] == roots[i])
			goto done;
	for (size_t i = 0; i < view.edge_count; i++) {
		const ClusterRecoveryCTRCGraphEdgeV2 *edge = &view.edges[i];
		uint8 *start = next;

		edges[i].from = rank[edge->from];
		edges[i].to = rank[edge->to];
		cluster_undo_tt_ctrc_put_u16_le(next, edge->kind);
		next = ctrc_graph_put_bytes(next + 2, &nodes[edges[i].from].id);
		next = ctrc_graph_put_bytes(next, &nodes[edges[i].to].id);
		edges[i].bytes = (ClusterRecoveryCTRCGraphBytesV2){ start, next - start };
	}
	if (view.edge_count != 0)
		qsort(edges, view.edge_count, sizeof(*edges), ctrc_graph_edge_cmp);
	for (size_t i = 0; i < view.edge_count; i++)
		if (edge_count == 0
			|| ctrc_graph_bytes_cmp(&edges[edge_count - 1].bytes, &edges[i].bytes) != 0)
			edges[edge_count++] = edges[i];

	/* Directed iterative reachability permits legal reference cycles, without
	 * recursion or a traversal-order hash. Roots are producer-qualified. */
	for (size_t i = 0; i < node_count; i++)
		heads[i] = SIZE_MAX;
	for (size_t i = 0; i < edge_count; i++) {
		edges[i].next = heads[edges[i].from];
		heads[edges[i].from] = i;
	}
	for (size_t i = 0; i < view.root_count; i++) {
		nodes[roots[i]].seen = true;
		queue[last++] = roots[i];
	}
	while (first < last) {
		size_t from = queue[first++];

		for (size_t i = heads[from]; i != SIZE_MAX; i = edges[i].next) {
			size_t to = edges[i].to;

			if (!nodes[to].seen) {
				nodes[to].seen = true;
				queue[last++] = to;
			}
		}
	}
	for (size_t i = 0; i < node_count; i++)
		if (nodes[i].owner) {
			if (!nodes[i].seen)
				goto done;
			owner = true;
		}
	if (!owner)
		goto done;

	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL || pg_cryptohash_init(hash) != 0
		|| pg_cryptohash_update(hash, domain, sizeof(domain) - 1) != 0
		|| pg_cryptohash_update(hash, payload, 192) != 0
		|| pg_cryptohash_update(hash, payload + 256, 64) != 0
		|| !ctrc_graph_hash_u64(hash, view.root_count))
		goto done;
	for (size_t i = 0; i < view.root_count; i++)
		if (!ctrc_graph_hash_bytes(hash, &nodes[roots[i]].id))
			goto done;
	if (!ctrc_graph_hash_u64(hash, node_count))
		goto done;
	for (size_t i = 0; i < node_count; i++)
		if (!ctrc_graph_hash_u64(hash, nodes[i].id.length + 8 + nodes[i].evidence.length)
			|| pg_cryptohash_update(hash, nodes[i].id.data, nodes[i].id.length) != 0
			|| !ctrc_graph_hash_bytes(hash, &nodes[i].evidence))
			goto done;
	if (!ctrc_graph_hash_u64(hash, edge_count))
		goto done;
	for (size_t i = 0; i < edge_count; i++)
		if (!ctrc_graph_hash_bytes(hash, &edges[i].bytes))
			goto done;
	ok = pg_cryptohash_final(hash, digest, sizeof(digest)) == 0;

done:
	if (hash != NULL)
		pg_cryptohash_free(hash);
	if (arena != NULL)
		pfree(arena);
	if (workspace != NULL)
		pfree(workspace);
	if (edges != NULL)
		pfree(edges);
	if (nodes != NULL)
		pfree(nodes);
	/* Consume all aliased input before publication, including on refusal. */
	if (ok)
		memcpy(out, digest, sizeof(digest));
	else
		memset(out, 0, 32);
	return ok;
}

bool
cluster_recovery_ctrc_read_v2(const XLogReaderState *reader, ClusterRecoveryCTRCReleaseV2 *out)
{
	ClusterRecoveryCTRCReleaseV2 record;

	if (out == NULL)
		return false;
	/* Consume only the completed current record. EndRecPtr includes any WAL
	 * page headers; neither byte count nor a readahead buffer supplies its end. */
	if (reader == NULL || reader->record == NULL || reader->system_identifier == 0
		|| reader->ReadRecPtr == InvalidXLogRecPtr || reader->EndRecPtr <= reader->ReadRecPtr
		|| reader->record->lsn != reader->ReadRecPtr
		|| reader->record->next_lsn != reader->EndRecPtr
		|| XLogRecGetRmid(reader) != RM_CLUSTER_UNDO_ID
		|| XLogRecGetInfo(reader) != CLUSTER_RECOVERY_CTRC_WAL_INFO
		|| XLogRecHasAnyBlockRefs(reader) || XLogRecHasPageVersionEdge(reader)
		|| TransactionIdIsValid(XLogRecGetXid(reader))
		|| TransactionIdIsValid(XLogRecGetTopXid(reader))
		|| XLogRecGetOrigin(reader) != InvalidRepOriginId
		|| !cluster_recovery_ctrc_decode_v2((const uint8 *)XLogRecGetData(reader),
											XLogRecGetDataLen(reader), reader->system_identifier,
											&record)
		|| record.writer_thread != reader->cluster_expected_thread_id)
		goto refused;
	*out = record;
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_reference_v2(const XLogReaderState *reader, uint32 source_index,
								   const uint8 source_identity_digest[32], const uint8 *raw_record,
								   size_t raw_length, ClusterRecoveryCTRCReferenceV2 *out)
{
	ClusterRecoveryCTRCReleaseV2 record;
	ClusterRecoveryCTRCReferenceV2 reference;
	uint32 data_length;

	if (out == NULL)
		return false;
	if (raw_record == NULL
		|| raw_length
			   != SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + CLUSTER_RECOVERY_CTRC_BYTES
		|| !cluster_recovery_ctrc_read_v2(reader, &record))
		goto refused;
	/* The decoded header is only a header. Bind the supplied complete final
	 * raw snapshot to it and to the original reader's entire main data. */
	if (memcmp(raw_record, &reader->record->header, SizeOfXLogRecord) != 0
		|| raw_record[SizeOfXLogRecord] != XLR_BLOCK_ID_DATA_LONG)
		goto refused;
	memcpy(&data_length, raw_record + SizeOfXLogRecord + 1, sizeof(data_length));
	if (data_length != CLUSTER_RECOVERY_CTRC_BYTES
		|| memcmp(raw_record + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong,
				  XLogRecGetData(reader), CLUSTER_RECOVERY_CTRC_BYTES)
			   != 0)
		goto refused;

	memset(&reference, 0, sizeof(reference));
	if (!cluster_recovery_ctrc_record_hash_v2(source_identity_digest, reader->ReadRecPtr,
											  reader->EndRecPtr, raw_record, raw_length,
											  reference.raw_record_sha256))
		goto refused;
	reference.source_index = source_index;
	reference.start = reader->ReadRecPtr;
	reference.end = reader->EndRecPtr;
	reference.raw_length = (uint32)raw_length;
	/* All aliased raw/source input has been consumed before publication. */
	*out = reference;
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

/* record is codec-validated; page is the caller's private aligned snapshot. */
static ClusterRecoveryCTRCTargetMatchV2
ctrc_target_match(const ClusterRecoveryCTRCReleaseV2 *record, const PGAlignedBlock *page)
{
	const UndoSegmentHeaderData *hdr = (const UndoSegmentHeaderData *)page->data;
	const TTSlot *slot;

	if (!UndoSegmentHeader_identity_matches(page->data, record->segment_id,
											(uint8)record->target_owner)
		|| cluster_undo_segment_header_integrity(page->data) != CLUSTER_UNDO_HEADER_VALID
		|| hdr->wrap_count != record->segment_generation)
		return CLUSTER_RECOVERY_CTRC_TARGET_REFUSED;
	/* The codec has checked slot_offset and both complete native slot images. */
	slot = &hdr->tt_slots[record->slot_offset];
	if (memcmp(slot, &record->before, sizeof(TTSlot)) == 0)
		return CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE;
	if (memcmp(slot, &record->after, sizeof(TTSlot)) == 0)
		return CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER;
	return CLUSTER_RECOVERY_CTRC_TARGET_REFUSED;
}

ClusterRecoveryCTRCTargetMatchV2
cluster_recovery_ctrc_target_match_v2(const uint8 *payload, size_t payload_length,
									  uint64 system_identifier, const void *header_page,
									  size_t header_length)
{
	ClusterRecoveryCTRCReleaseV2 record;
	PGAlignedBlock copy;

	if (payload == NULL || header_page == NULL || header_length != BLCKSZ
		|| !cluster_recovery_ctrc_decode_v2(payload, payload_length, system_identifier, &record))
		return CLUSTER_RECOVERY_CTRC_TARGET_REFUSED;
	/* Immutable, aligned snapshot. Integrity is separate from original reader
	 * coverage/authority; even a valid newer generation is not obsolete proof. */
	memcpy(copy.data, header_page, BLCKSZ);
	return ctrc_target_match(&record, &copy);
}

bool
cluster_recovery_ctrc_target_image_v2(const uint8 *payload, size_t payload_length,
									  uint64 system_identifier, const void *header_page,
									  size_t header_length, void *out, size_t out_length)
{
	ClusterRecoveryCTRCReleaseV2 record;
	PGAlignedBlock copy;
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)copy.data;

	if (out == NULL)
		return false;
	if (out_length != BLCKSZ || payload == NULL || header_page == NULL || header_length != BLCKSZ
		|| !cluster_recovery_ctrc_decode_v2(payload, payload_length, system_identifier, &record))
		goto refused;
	memcpy(copy.data, header_page, BLCKSZ);
	/* Exact-after alone cannot discharge the typed idempotency obligation. */
	if (ctrc_target_match(&record, &copy) != CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE)
		goto refused;
	memcpy(&hdr->tt_slots[record.slot_offset], &record.after, sizeof(TTSlot));
	if (!cluster_undo_segment_header_seal(copy.data))
		goto refused;
	/* The complete input has been consumed before any aliased publication. */
	memcpy(out, copy.data, BLCKSZ);
	return true;

refused:
	memset(out, 0, Min(out_length, BLCKSZ));
	return false;
}

static bool
ctrc_nonzero_digest(const uint8 digest[32])
{
	static const uint8 zero[32] = { 0 };

	return memcmp(digest, zero, sizeof(zero)) != 0;
}

/* Structural checks only; caller digests are not owner completion proofs. */
static bool
ctrc_shape_valid(const ClusterRecoveryCTRCReleaseV2 *r)
{
	const TTSlot *before = &r->before;
	const TTSlot *after = &r->after;
	uint32 segment, block;
	uint16 slot, row;

	if (r->version != CLUSTER_RECOVERY_CTRC_VERSION || r->length != CLUSTER_RECOVERY_CTRC_BYTES
		|| r->flags != CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN || r->root_id == 0
		|| r->root_generation == 0 || r->formation_epoch == 0 || r->recovery_serial == 0
		|| r->database_incarnation == 0 || r->writer_boot == 0 || r->cohort_bitmap == 0
		|| r->origin_node_id > SCN_MAX_VALID_NODE_ID || r->writer_thread == 0
		|| r->writer_thread > UNDO_OWNER_INSTANCE_MAX || r->target_owner == 0
		|| r->target_owner > UNDO_OWNER_INSTANCE_MAX || r->origin_node_id + 1 != r->target_owner
		|| r->segment_id == 0 || r->segment_generation == UINT32_MAX
		|| (r->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != r->target_owner
		|| r->slot_offset >= TT_SLOTS_PER_SEGMENT || r->wrap == TT_WRAP_INVALID
		|| !TransactionIdIsNormal(r->xid)
		|| (r->terminal_outcome != TT_SLOT_COMMITTED && r->terminal_outcome != TT_SLOT_ABORTED)
		|| !ctrc_nonzero_digest(r->graph_sha256) || !ctrc_nonzero_digest(r->input_sha256))
		return false;
	if (before->xid != r->xid || before->wrap != r->wrap || before->status != r->terminal_outcome
		|| before->flags != TT_FLAGS_RESERVED
		|| (before->status == TT_SLOT_COMMITTED && before->commit_scn == InvalidScn)
		|| (before->status == TT_SLOT_ABORTED && before->commit_scn != InvalidScn)
		|| after->xid != before->xid || after->wrap != before->wrap
		|| after->status != before->status || after->flags != TT_SLOT_FLAG_CTRC_RELEASE_PROVEN
		|| after->commit_scn != before->commit_scn
		|| after->first_undo_block.raw[0] != before->first_undo_block.raw[0]
		|| after->first_undo_block.raw[1] != before->first_undo_block.raw[1])
		return false;
	if (!UBA_is_invalid(before->first_undo_block)
		&& (!uba_decode_record(before->first_undo_block, &segment, &block, &slot, &row)
			|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != r->target_owner
			|| slot != r->slot_offset))
		return false;
	return true;
}

static void
ctrc_put_slot(uint8 *bytes, const TTSlot *slot)
{
	cluster_undo_tt_ctrc_put_u32_le(bytes, slot->xid);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 4, slot->wrap);
	bytes[6] = slot->status;
	bytes[7] = slot->flags;
	cluster_undo_tt_ctrc_put_u64_le(bytes + 8, slot->commit_scn);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 16, slot->first_undo_block.raw[0]);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 24, slot->first_undo_block.raw[1]);
}

static void
ctrc_get_slot(const uint8 *bytes, TTSlot *slot)
{
	slot->xid = cluster_undo_tt_ctrc_get_u32_le(bytes);
	slot->wrap = cluster_undo_tt_ctrc_get_u16_le(bytes + 4);
	slot->status = bytes[6];
	slot->flags = bytes[7];
	slot->commit_scn = cluster_undo_tt_ctrc_get_u64_le(bytes + 8);
	slot->first_undo_block.raw[0] = cluster_undo_tt_ctrc_get_u64_le(bytes + 16);
	slot->first_undo_block.raw[1] = cluster_undo_tt_ctrc_get_u64_le(bytes + 24);
}

static bool
ctrc_sha256(const uint8 *bytes, size_t length, uint8 out[32])
{
	pg_cryptohash_ctx *hash = pg_cryptohash_create(PG_SHA256);
	bool ok;

	if (hash == NULL)
		return false;
	ok = pg_cryptohash_init(hash) == 0 && pg_cryptohash_update(hash, bytes, length) == 0
		 && pg_cryptohash_final(hash, out, 32) == 0;
	pg_cryptohash_free(hash);
	return ok;
}

static bool
ctrc_identity_sha256(const uint8 *bytes, uint64 system_identifier, uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-D0-IDENTITY-V2";
	uint8 transcript[sizeof(domain) - 1 + 34 + 48 + 40 + 32];
	uint8 *next = transcript;

	/* No NUL. T is system/dbinc/origin/segment/generation/slot/wrap/xid,
	 * followed by the exact ROOT-round/source fields and full input digest. */
	memcpy(next, domain, sizeof(domain) - 1);
	next += sizeof(domain) - 1;
	cluster_undo_tt_ctrc_put_u64_le(next, system_identifier);
	next += 8;
	memcpy(next, bytes + 40, 8);
	next += 8;
	memcpy(next, bytes + 96, 2);
	next += 2;
	memcpy(next, bytes + 108, 8);
	next += 8;
	memcpy(next, bytes + 102, 4);
	next += 4;
	memcpy(next, bytes + 116, 4);
	next += 4;
	memcpy(next, bytes + 8, 48);
	next += 48;
	memcpy(next, bytes + 88, 40);
	next += 40;
	memcpy(next, bytes + 256, 32);
	return ctrc_sha256(transcript, sizeof(transcript), out);
}

static bool
ctrc_payload_sha256(const uint8 *bytes, uint8 out[32])
{
	/* Exactly eleven ASCII bytes followed by five zero bytes. */
	static const uint8 domain[16] = "PGRAC-D0-V2";
	uint8 transcript[304];

	memcpy(transcript, domain, sizeof(domain));
	memcpy(transcript + 16, bytes, 192);
	memcpy(transcript + 208, bytes + 224, 96);
	return ctrc_sha256(transcript, sizeof(transcript), out);
}

bool
cluster_recovery_ctrc_record_hash_v2(const uint8 source_identity_digest[32], XLogRecPtr start,
									 XLogRecPtr end, const uint8 *raw_record, size_t raw_length,
									 uint8 out[32])
{
	static const uint8 domain[] = "PGRAC-D0-RECORD-V2";
	XLogRecord header;
	pg_crc32c crc;
	pg_cryptohash_ctx *hash;
	uint8 positions[20];
	uint8 digest[32];
	bool ok;

	if (out == NULL)
		return false;
	if (source_identity_digest == NULL || raw_record == NULL
		|| !ctrc_nonzero_digest(source_identity_digest) || raw_length < SizeOfXLogRecord
		|| raw_length > XLogRecordMaxSize || start == InvalidXLogRecPtr || end <= start
		|| end - start < raw_length)
		goto refuse;
	/* No aligned dereference of caller bytes, and no read beyond raw_length. */
	memcpy(&header, raw_record, SizeOfXLogRecord);
	if (header.xl_tot_len != raw_length)
		goto refuse;

	/* Same native CRC order as ValidXLogRecord: data, then header before CRC.
	 * This detects damage; it cannot establish reader/source authority. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, raw_record + SizeOfXLogRecord, raw_length - SizeOfXLogRecord);
	COMP_CRC32C(crc, raw_record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(header.xl_crc, crc))
		goto refuse;

	cluster_undo_tt_ctrc_put_u64_le(positions, start);
	cluster_undo_tt_ctrc_put_u64_le(positions + 8, end);
	cluster_undo_tt_ctrc_put_u32_le(positions + 16, (uint32)raw_length);
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		goto refuse;
	/* Do not hash a terminator, page headers or alignment padding. The final
	 * CRC remains inside raw_record, never replaced by this external SHA. */
	ok = pg_cryptohash_init(hash) == 0
		 && pg_cryptohash_update(hash, domain, sizeof(domain) - 1) == 0
		 && pg_cryptohash_update(hash, source_identity_digest, 32) == 0
		 && pg_cryptohash_update(hash, positions, sizeof(positions)) == 0
		 && pg_cryptohash_update(hash, raw_record, raw_length) == 0
		 && pg_cryptohash_final(hash, digest, sizeof(digest)) == 0;
	pg_cryptohash_free(hash);
	if (!ok)
		goto refuse;
	/* Publish last, including when output aliases source or raw bytes. */
	memcpy(out, digest, sizeof(digest));
	return true;

refuse:
	memset(out, 0, 32);
	return false;
}

bool
cluster_recovery_ctrc_encode_v2(const ClusterRecoveryCTRCReleaseV2 *record,
								uint64 system_identifier, uint8 *bytes, size_t length)
{
	ClusterRecoveryCTRCReleaseV2 value;

	if (bytes == NULL)
		return false;
	if (record == NULL || system_identifier == 0 || length != CLUSTER_RECOVERY_CTRC_BYTES) {
		memset(bytes, 0, Min(length, (size_t)CLUSTER_RECOVERY_CTRC_BYTES));
		return false;
	}
	/* Preserve a logical input that aliases the output buffer. */
	value = *record;
	memset(bytes, 0, length);
	if (!ctrc_shape_valid(&value))
		return false;
	cluster_undo_tt_ctrc_put_u16_le(bytes, value.version);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 2, value.length);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 4, value.flags);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 8, value.root_id);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 16, value.root_generation);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 24, value.formation_epoch);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 32, value.recovery_serial);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 40, value.database_incarnation);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 48, value.writer_boot);
	/* 56..71 and 120..127 stay zero: no self-referential WAL location. */
	cluster_undo_tt_ctrc_put_u64_le(bytes + 72, value.covered_history);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 80, value.highest_fpi_end);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 88, value.cohort_bitmap);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 96, value.origin_node_id);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 98, value.writer_thread);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 100, value.target_owner);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 102, value.slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(bytes + 104, value.wrap);
	bytes[106] = value.terminal_outcome;
	cluster_undo_tt_ctrc_put_u32_le(bytes + 108, value.segment_id);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 112, value.segment_generation);
	cluster_undo_tt_ctrc_put_u32_le(bytes + 116, value.xid);
	ctrc_put_slot(bytes + 128, &value.before);
	ctrc_put_slot(bytes + 160, &value.after);
	memcpy(bytes + 224, value.graph_sha256, 32);
	memcpy(bytes + 256, value.input_sha256, 32);
	if (!ctrc_identity_sha256(bytes, system_identifier, bytes + 288)
		|| !ctrc_payload_sha256(bytes, bytes + 192)) {
		memset(bytes, 0, length);
		return false;
	}
	return true;
}

bool
cluster_recovery_ctrc_decode_v2(const uint8 *bytes, size_t length, uint64 system_identifier,
								ClusterRecoveryCTRCReleaseV2 *out)
{
	ClusterRecoveryCTRCReleaseV2 value;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
	uint8 identity_sha256[32];
	uint8 payload_sha256[32];

	if (out == NULL)
		return false;
	if (bytes == NULL || length != sizeof(payload) || system_identifier == 0) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	/* Copy before clearing: the caller may decode in place. */
	memcpy(payload, bytes, sizeof(payload));
	memset(out, 0, sizeof(*out));
	if (cluster_undo_tt_ctrc_get_u64_le(payload + 56) != 0
		|| cluster_undo_tt_ctrc_get_u64_le(payload + 64) != 0 || payload[107] != 0
		|| cluster_undo_tt_ctrc_get_u64_le(payload + 120) != 0)
		return false;
	memset(&value, 0, sizeof(value));
	value.version = cluster_undo_tt_ctrc_get_u16_le(payload);
	value.length = cluster_undo_tt_ctrc_get_u16_le(payload + 2);
	value.flags = cluster_undo_tt_ctrc_get_u32_le(payload + 4);
	value.root_id = cluster_undo_tt_ctrc_get_u64_le(payload + 8);
	value.root_generation = cluster_undo_tt_ctrc_get_u64_le(payload + 16);
	value.formation_epoch = cluster_undo_tt_ctrc_get_u64_le(payload + 24);
	value.recovery_serial = cluster_undo_tt_ctrc_get_u64_le(payload + 32);
	value.database_incarnation = cluster_undo_tt_ctrc_get_u64_le(payload + 40);
	value.writer_boot = cluster_undo_tt_ctrc_get_u64_le(payload + 48);
	value.covered_history = cluster_undo_tt_ctrc_get_u64_le(payload + 72);
	value.highest_fpi_end = cluster_undo_tt_ctrc_get_u64_le(payload + 80);
	value.cohort_bitmap = cluster_undo_tt_ctrc_get_u64_le(payload + 88);
	value.origin_node_id = cluster_undo_tt_ctrc_get_u16_le(payload + 96);
	value.writer_thread = cluster_undo_tt_ctrc_get_u16_le(payload + 98);
	value.target_owner = cluster_undo_tt_ctrc_get_u16_le(payload + 100);
	value.slot_offset = cluster_undo_tt_ctrc_get_u16_le(payload + 102);
	value.wrap = cluster_undo_tt_ctrc_get_u16_le(payload + 104);
	value.terminal_outcome = payload[106];
	value.segment_id = cluster_undo_tt_ctrc_get_u32_le(payload + 108);
	value.segment_generation = cluster_undo_tt_ctrc_get_u32_le(payload + 112);
	value.xid = cluster_undo_tt_ctrc_get_u32_le(payload + 116);
	ctrc_get_slot(payload + 128, &value.before);
	ctrc_get_slot(payload + 160, &value.after);
	memcpy(value.graph_sha256, payload + 224, 32);
	memcpy(value.input_sha256, payload + 256, 32);
	if (!ctrc_shape_valid(&value)
		|| !ctrc_identity_sha256(payload, system_identifier, identity_sha256)
		|| memcmp(identity_sha256, payload + 288, 32) != 0
		|| !ctrc_payload_sha256(payload, payload_sha256)
		|| memcmp(payload_sha256, payload + 192, 32) != 0)
		return false;
	*out = value;
	return true;
}
#endif
