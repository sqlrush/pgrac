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
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "common/cryptohash.h"
#include "port/pg_crc32c.h"
#include "replication/origin.h"
#include "utils/memutils.h"

StaticAssertDecl(SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort
					 + sizeof(xl_undo_tt_slot_bind) == CLUSTER_RECOVERY_CTRC_BIND_RAW_BYTES,
				 "BIND reference requires the fixed native short-data shape");
StaticAssertDecl(SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort
					 + offsetof(xl_undo_tt_slot_bind, xid) == 42,
				 "BIND reference xid offset must remain fixed");
StaticAssertDecl(CLUSTER_RECOVERY_FILE_UNDO_SEGMENT == 7,
				 "DATA reference must retain the original undo namespace kind");

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
cluster_recovery_ctrc_read_graph_v2(const XLogReaderState *reader,
	const ClusterRecoveryCTRCGraphV2 *graph, ClusterRecoveryCTRCReleaseV2 *out)
{
	ClusterRecoveryCTRCReleaseV2 record;
	uint8 digest[32];

	if (out == NULL)
		return false;
	if (!cluster_recovery_ctrc_read_v2(reader, &record)
		|| !cluster_recovery_ctrc_graph_hash_v2(&record, reader->system_identifier, graph, digest)
		|| memcmp(digest, record.graph_sha256, sizeof(digest)) != 0) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	/* Graph inputs may alias out; consume everything before publication. */
	memcpy(out, &record, sizeof(record));
	return true;
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

bool
cluster_recovery_ctrc_wal_atom_v2(const XLogReaderState *reader, uint32 source_index,
								  const uint8 source_identity_digest[32], const uint8 *raw_record,
								  size_t raw_length, uint8 *out, size_t out_length)
{
	ClusterRecoveryCTRCReferenceV2 reference;
	uint8 atom[CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES];

	if (out == NULL)
		return false;
	if (out_length != sizeof(atom)
		|| !cluster_recovery_ctrc_reference_v2(reader, source_index, source_identity_digest,
											   raw_record, raw_length, &reference))
		goto refused;

	/* U16(kind) || B(R), with R field-coded rather than a native struct. */
	cluster_undo_tt_ctrc_put_u16_le(atom, 1);
	cluster_undo_tt_ctrc_put_u64_le(atom + 2, 56);
	cluster_undo_tt_ctrc_put_u32_le(atom + 10, reference.source_index);
	cluster_undo_tt_ctrc_put_u64_le(atom + 14, reference.start);
	cluster_undo_tt_ctrc_put_u64_le(atom + 22, reference.end);
	cluster_undo_tt_ctrc_put_u32_le(atom + 30, reference.raw_length);
	memcpy(atom + 34, reference.raw_record_sha256, 32);
	/* B(raw): original complete record, without intervening WAL page headers. */
	cluster_undo_tt_ctrc_put_u64_le(atom + 66, reference.raw_length);
	memcpy(atom + 74, raw_record, reference.raw_length);
	/* Consume all aliased raw/source input before publishing the atom. */
	memcpy(out, atom, sizeof(atom));
	return true;

refused:
	memset(out, 0, Min(out_length, sizeof(atom)));
	return false;
}

bool
cluster_recovery_ctrc_wal_references_v2(const XLogReaderState *reader, uint32 source_index,
										const uint8 source_identity_digest[32],
										const uint8 *raw_record, size_t raw_length,
										ClusterRecoveryCTRCWalReferencesV2 *out)
{
	static const uint16 xid_offsets[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT] = { 116, 128, 160 };
	ClusterRecoveryCTRCReleaseV2 record;
	ClusterRecoveryCTRCWalReferencesV2 references = { 0 };
	uint8 *key = references.keys[0];

	if (out == NULL)
		return false;
	if (!cluster_recovery_ctrc_read_v2(reader, &record)
		|| !cluster_recovery_ctrc_wal_atom_v2(reader, source_index, source_identity_digest,
											  raw_record, raw_length, references.evidence + 16,
											  CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES))
		goto refused;

	/* The validated target and both native slot images carry the same T. */
	cluster_undo_tt_ctrc_put_u64_le(key, reader->system_identifier);
	cluster_undo_tt_ctrc_put_u64_le(key + 8, record.database_incarnation);
	cluster_undo_tt_ctrc_put_u16_le(key + 16, record.origin_node_id);
	cluster_undo_tt_ctrc_put_u32_le(key + 18, record.segment_id);
	cluster_undo_tt_ctrc_put_u32_le(key + 22, record.segment_generation);
	cluster_undo_tt_ctrc_put_u16_le(key + 26, record.slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(key + 28, record.wrap);
	cluster_undo_tt_ctrc_put_u32_le(key + 30, record.xid);
	cluster_undo_tt_ctrc_put_u16_le(key + 34, 3); /* WAL carrier */
	cluster_undo_tt_ctrc_put_u64_le(key + 36, 56);
	memcpy(key + 44, references.evidence + 16 + 10, 56);
	for (unsigned i = 0; i < CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT; i++) {
		if (i != 0)
			memcpy(references.keys[i], key, 100);
		cluster_undo_tt_ctrc_put_u64_le(references.keys[i] + 100,
										SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong
											+ xid_offsets[i]);
		/* Each field is a singleton. The zero ordinal is not a filtered index. */
	}
	cluster_undo_tt_ctrc_put_u64_le(references.evidence, 1);
	cluster_undo_tt_ctrc_put_u64_le(references.evidence + 8, CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES);
	/* Consume raw/source aliases before publishing the complete group. */
	memcpy(out, &references, sizeof(references));
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_wal_reference_graph_nodes_v2(
	const ClusterRecoveryCTRCWalReferencesV2 *references,
	ClusterRecoveryCTRCGraphNodeV2 out[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT])
{
	static const uint64 field_offsets[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT] = {
		SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 116,
		SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 128,
		SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 160
	};
	ClusterRecoveryCTRCGraphNodeV2 nodes[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT];
	const uint8 *atom;

	if (references == NULL || out == NULL
		|| cluster_undo_tt_ctrc_get_u64_le(references->evidence) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(references->evidence + 8)
			!= CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES)
		goto refused;
	atom = references->evidence + 16;
	if (cluster_undo_tt_ctrc_get_u16_le(atom) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 2) != 56
		|| cluster_undo_tt_ctrc_get_u32_le(atom + 30)
			!= CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES - 74
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 66)
			!= CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES - 74)
		goto refused;
	for (unsigned i = 0; i < CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT; i++) {
		const uint8 *key = references->keys[i];

		if (cluster_undo_tt_ctrc_get_u16_le(key + 34) != 3
			|| cluster_undo_tt_ctrc_get_u64_le(key + 36) != 56
			|| memcmp(key + 44, atom + 10, 56) != 0
			|| cluster_undo_tt_ctrc_get_u64_le(key + 100) != field_offsets[i]
			|| cluster_undo_tt_ctrc_get_u64_le(key + 108) != 0
			|| (i != 0 && memcmp(key, references->keys[0], 100) != 0))
			goto refused;
		nodes[i] = (ClusterRecoveryCTRCGraphNodeV2){
			1,
			{ references->keys[i], CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES },
			{ references->evidence, CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_EVIDENCE_BYTES }
		};
	}
	memcpy(out, nodes, sizeof(nodes));
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(nodes));
	return false;
}

bool
cluster_recovery_ctrc_tt_reference_v2(XLogReaderState *reader,
										uint64 database_incarnation, uint32 source_index,
										const uint8 source_identity_digest[32],
										const uint8 *raw_record, size_t raw_length,
										ClusterRecoveryCTRCTTReferenceV2 *out)
{
	ClusterUndoDecoded decoded;
	ClusterUndoDecodedKind kind;
	ClusterRecoveryCTRCTTReferenceV2 reference = { 0 };
	size_t payload_length;
	uint8 *key = reference.key;
	uint8 *atom = reference.evidence + 16;

	if (out == NULL)
		return false;
	if (database_incarnation == 0 || raw_record == NULL || reader == NULL
		|| reader->record == NULL || reader->system_identifier == 0
		|| reader->cluster_expected_thread_id == 0 || reader->cluster_expected_thread_id > 128
		|| reader->ReadRecPtr == InvalidXLogRecPtr || reader->EndRecPtr <= reader->ReadRecPtr
		|| reader->record->lsn != reader->ReadRecPtr
		|| reader->record->next_lsn != reader->EndRecPtr
		|| XLogRecGetRmid(reader) != RM_CLUSTER_UNDO_ID
		|| XLogRecHasAnyBlockRefs(reader) || XLogRecHasPageVersionEdge(reader)
		|| TransactionIdIsValid(XLogRecGetTopXid(reader))
		|| XLogRecGetOrigin(reader) != InvalidRepOriginId)
		goto refused;
	/* Only these original payloads carry an exact segment generation. */
	switch (XLogRecGetInfo(reader)) {
	case XLOG_UNDO_TT_SLOT_BIND:
		payload_length = sizeof(xl_undo_tt_slot_bind);
		kind = CLUSTER_UNDO_KIND_TT_BIND;
		break;
	case XLOG_UNDO_TT_SLOT_ABORT:
		payload_length = sizeof(xl_undo_tt_slot_abort_exact);
		kind = CLUSTER_UNDO_KIND_TT_ABORT;
		break;
	case XLOG_UNDO_TT_SLOT_CTRC_RELEASE:
		payload_length = CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES;
		kind = CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
		break;
	default:
		goto refused;
	}
	if (raw_length != SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + payload_length
		|| XLogRecGetDataLen(reader) != payload_length
		|| !cluster_undo_decode(reader, &decoded) || !cluster_undo_inspect_pi(&decoded)
		|| decoded.kind != kind || decoded.instance != reader->cluster_expected_thread_id
		|| decoded.expected_generation == UINT32_MAX)
		goto refused;
	/* A decoded header alone does not bind the supplied complete raw record.
	 * The header xid is evidence, separate from this one payload xid field. */
	if (memcmp(raw_record, &reader->record->header, SizeOfXLogRecord) != 0
		|| raw_record[SizeOfXLogRecord] != XLR_BLOCK_ID_DATA_SHORT
		|| raw_record[SizeOfXLogRecord + 1] != payload_length
		|| memcmp(raw_record + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort,
				  XLogRecGetData(reader), payload_length) != 0
		|| !cluster_recovery_ctrc_record_hash_v2(source_identity_digest, reader->ReadRecPtr,
												reader->EndRecPtr, raw_record, raw_length, key + 68))
		goto refused;

	/* T34 || U16(WAL carrier) || B(R56) || U64(offset) || U64(ordinal). */
	cluster_undo_tt_ctrc_put_u64_le(key, reader->system_identifier);
	cluster_undo_tt_ctrc_put_u64_le(key + 8, database_incarnation);
	cluster_undo_tt_ctrc_put_u16_le(key + 16, decoded.instance);
	cluster_undo_tt_ctrc_put_u32_le(key + 18, decoded.segment_id);
	cluster_undo_tt_ctrc_put_u32_le(key + 22, decoded.expected_generation);
	cluster_undo_tt_ctrc_put_u16_le(key + 26, decoded.slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(key + 28, decoded.wrap);
	cluster_undo_tt_ctrc_put_u32_le(key + 30, decoded.xid);
	cluster_undo_tt_ctrc_put_u16_le(key + 34, 3);
	cluster_undo_tt_ctrc_put_u64_le(key + 36, 56);
	cluster_undo_tt_ctrc_put_u32_le(key + 44, source_index);
	cluster_undo_tt_ctrc_put_u64_le(key + 48, reader->ReadRecPtr);
	cluster_undo_tt_ctrc_put_u64_le(key + 56, reader->EndRecPtr);
	cluster_undo_tt_ctrc_put_u32_le(key + 64, (uint32)raw_length);
	cluster_undo_tt_ctrc_put_u64_le(key + 100, 42);
	/* The singleton ordinal stays zero; it is not a filtered field index. */
	cluster_undo_tt_ctrc_put_u16_le(atom, 1);
	cluster_undo_tt_ctrc_put_u64_le(atom + 2, 56);
	memcpy(atom + 10, key + 44, 56);
	cluster_undo_tt_ctrc_put_u64_le(atom + 66, raw_length);
	memcpy(atom + 74, raw_record, raw_length);
	cluster_undo_tt_ctrc_put_u64_le(reference.evidence, 1);
	cluster_undo_tt_ctrc_put_u64_le(reference.evidence + 8, 74 + raw_length);
	reference.evidence_length = 16 + 74 + raw_length;
	/* Consume all raw/source aliases before publishing the complete group. */
	memcpy(out, &reference, sizeof(reference));
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_tt_wal_inventory_v2(XLogReaderState *reader,
											uint64 database_incarnation, uint32 source_index,
											const uint8 source_identity_digest[32],
											const uint8 *raw_record, size_t raw_length,
											ClusterRecoveryCTRCTTReferenceV2 *out)
{
	ClusterRecoveryCTRCTTReferenceV2 inventory;

	if (out == NULL)
		return false;
	if (!cluster_recovery_ctrc_tt_reference_v2(reader, database_incarnation, source_index,
												source_identity_digest, raw_record, raw_length,
												&inventory)
		|| XLogRecGetXid(reader) != InvalidTransactionId)
		goto refused;
	/* The exact original carrier has no header xid and one payload xid.
	 * Never copy the payload T34 to an unresolved header field, even when
	 * both low32 values match. Consume aliases before publishing the group. */
	memcpy(out, &inventory, sizeof(inventory));
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_tt_abort_wal_inventory_v2(XLogReaderState *reader,
										uint64 database_incarnation, uint32 source_index,
										const uint8 source_identity_digest[32],
										const uint8 *raw_record, size_t raw_length,
										ClusterRecoveryCTRCTTAbortReferencesV2 *out)
{
	static const uint16 field_offsets[CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT] = {
		34 + 48, 34 + 56, 34 + 104, 34 + 120
	};
	ClusterRecoveryTTAbortV1 abort;
	ClusterRecoveryCTRCTTAbortReferencesV2 references = { 0 };
	uint8 *key = references.keys[0];
	uint8 *atom = references.evidence + 16;
	uint8 hash[32];

	if (out == NULL)
		return false;
	if (reader == NULL || reader->record == NULL || raw_record == NULL
		|| database_incarnation == 0 || reader->system_identifier == 0
		|| reader->cluster_expected_thread_id == 0
		|| reader->cluster_expected_thread_id > UNDO_OWNER_INSTANCE_MAX
		|| reader->ReadRecPtr == InvalidXLogRecPtr
		|| reader->EndRecPtr <= reader->ReadRecPtr
		|| reader->record->lsn != reader->ReadRecPtr
		|| reader->record->next_lsn != reader->EndRecPtr
		|| XLogRecGetRmid(reader) != RM_CLUSTER_UNDO_ID
		|| XLogRecGetInfo(reader) != XLOG_UNDO_RECOVERY_TT_ABORT
		|| XLogRecHasAnyBlockRefs(reader) || XLogRecHasPageVersionEdge(reader)
		|| TransactionIdIsValid(XLogRecGetTopXid(reader))
		|| XLogRecGetOrigin(reader) != InvalidRepOriginId
		|| XLogRecGetXid(reader) != InvalidTransactionId
		|| raw_length != CLUSTER_RECOVERY_CTRC_TT_ABORT_RAW_BYTES
		|| XLogRecGetDataLen(reader) != CLUSTER_RECOVERY_TT_ABORT_BYTES
		|| !cluster_recovery_tt_abort_decode_v1(
			raw_record + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort,
			CLUSTER_RECOVERY_TT_ABORT_BYTES, &abort)
		|| abort.target_thread != reader->cluster_expected_thread_id
		|| memcmp(raw_record, &reader->record->header, SizeOfXLogRecord) != 0
		|| raw_record[SizeOfXLogRecord] != XLR_BLOCK_ID_DATA_SHORT
		|| raw_record[SizeOfXLogRecord + 1] != CLUSTER_RECOVERY_TT_ABORT_BYTES
		|| memcmp(raw_record + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort,
				  XLogRecGetData(reader), CLUSTER_RECOVERY_TT_ABORT_BYTES) != 0
		|| !cluster_recovery_ctrc_record_hash_v2(source_identity_digest,
			reader->ReadRecPtr, reader->EndRecPtr, raw_record, raw_length, hash))
		goto refused;

	cluster_undo_tt_ctrc_put_u64_le(key, reader->system_identifier);
	cluster_undo_tt_ctrc_put_u64_le(key + 8, database_incarnation);
	cluster_undo_tt_ctrc_put_u16_le(key + 16, abort.target_thread);
	cluster_undo_tt_ctrc_put_u32_le(key + 18, abort.exact.segment_id);
	cluster_undo_tt_ctrc_put_u32_le(key + 22, abort.exact.segment_generation);
	cluster_undo_tt_ctrc_put_u16_le(key + 26, abort.exact.slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(key + 28, abort.exact.wrap);
	cluster_undo_tt_ctrc_put_u32_le(key + 30, abort.exact.xid);
	cluster_undo_tt_ctrc_put_u16_le(key + 34, 3);
	cluster_undo_tt_ctrc_put_u64_le(key + 36, 56);
	cluster_undo_tt_ctrc_put_u32_le(key + 44, source_index);
	cluster_undo_tt_ctrc_put_u64_le(key + 48, reader->ReadRecPtr);
	cluster_undo_tt_ctrc_put_u64_le(key + 56, reader->EndRecPtr);
	cluster_undo_tt_ctrc_put_u32_le(key + 64, (uint32) raw_length);
	memcpy(key + 68, hash, sizeof(hash));
	for (unsigned i = 0; i < CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT; i++) {
		if (i != 0)
			memcpy(references.keys[i], key, sizeof(references.keys[i]));
		cluster_undo_tt_ctrc_put_u64_le(references.keys[i] + 100, field_offsets[i]);
	}
	cluster_undo_tt_ctrc_put_u64_le(references.evidence, 1);
	cluster_undo_tt_ctrc_put_u64_le(references.evidence + 8,
		74 + CLUSTER_RECOVERY_CTRC_TT_ABORT_RAW_BYTES);
	cluster_undo_tt_ctrc_put_u16_le(atom, 1);
	cluster_undo_tt_ctrc_put_u64_le(atom + 2, 56);
	memcpy(atom + 10, key + 44, 56);
	cluster_undo_tt_ctrc_put_u64_le(atom + 66, raw_length);
	memcpy(atom + 74, raw_record, raw_length);
	references.evidence_length = CLUSTER_RECOVERY_CTRC_TT_ABORT_EVIDENCE_BYTES;
	memcpy(out, &references, sizeof(references));
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_tt_abort_graph_nodes_v2(
	const ClusterRecoveryCTRCTTAbortReferencesV2 *references,
	ClusterRecoveryCTRCGraphNodeV2 out[CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT])
{
	static const uint64 field_offsets[CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT] = {
		34 + 48, 34 + 56, 34 + 104, 34 + 120
	};
	ClusterRecoveryCTRCGraphNodeV2 nodes[CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT];
	const uint8 *atom;
	uint64 atom_length;
	uint64 raw_length;

	if (references == NULL || out == NULL
		|| references->evidence_length != CLUSTER_RECOVERY_CTRC_TT_ABORT_EVIDENCE_BYTES)
		goto refused;

	/* The inventory is one shared exact atom with four independent offsets. */
	if (cluster_undo_tt_ctrc_get_u64_le(references->evidence) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(references->evidence + 8)
			!= CLUSTER_RECOVERY_CTRC_TT_ABORT_ATOM_BYTES)
		goto refused;
	atom = references->evidence + 16;
	atom_length = cluster_undo_tt_ctrc_get_u64_le(references->evidence + 8);
	if (cluster_undo_tt_ctrc_get_u16_le(atom) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 2) != 56
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 66)
			!= CLUSTER_RECOVERY_CTRC_TT_ABORT_RAW_BYTES
		|| atom_length != 74 + cluster_undo_tt_ctrc_get_u64_le(atom + 66)
		|| memcmp(atom + 10, references->keys[0] + 44, 56) != 0)
		goto refused;
	raw_length = cluster_undo_tt_ctrc_get_u32_le(references->keys[0] + 64);
	if (raw_length != CLUSTER_RECOVERY_CTRC_TT_ABORT_RAW_BYTES)
		goto refused;

	for (unsigned i = 0; i < CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT; i++) {
		const uint8 *key = references->keys[i];

		if (cluster_undo_tt_ctrc_get_u16_le(key + 34) != 3
			|| cluster_undo_tt_ctrc_get_u64_le(key + 36) != 56
			|| cluster_undo_tt_ctrc_get_u32_le(key + 64) != raw_length
			|| cluster_undo_tt_ctrc_get_u64_le(key + 100) != field_offsets[i]
			|| (i != 0 && memcmp(key, references->keys[0], 100) != 0))
			goto refused;
		nodes[i] = (ClusterRecoveryCTRCGraphNodeV2){
			1,
			{ references->keys[i], sizeof(references->keys[i]) },
			{ references->evidence, references->evidence_length }
		};
	}
	memcpy(out, nodes, sizeof(nodes));
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(nodes));
	return false;
}

bool
cluster_recovery_ctrc_tt_reference_graph_node_v2(
	const ClusterRecoveryCTRCTTReferenceV2 *reference,
	ClusterRecoveryCTRCGraphNodeV2 *out)
{
	ClusterRecoveryCTRCGraphNodeV2 node;
	const uint8 *atom;
	size_t raw_length;

	if (reference == NULL || out == NULL
		|| reference->evidence_length < 16 + 74
		|| reference->evidence_length > CLUSTER_RECOVERY_CTRC_TT_EVIDENCE_MAX_BYTES
		|| cluster_undo_tt_ctrc_get_u16_le(reference->key + 34) != 3
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 36) != 56
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 100) != 42)
		goto refused;
	raw_length = reference->evidence_length - 16 - 74;
	if (raw_length == 0 || raw_length > CLUSTER_RECOVERY_CTRC_TT_RAW_MAX_BYTES
		|| cluster_undo_tt_ctrc_get_u32_le(reference->key + 64) != raw_length
		|| cluster_undo_tt_ctrc_get_u64_le(reference->evidence) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(reference->evidence + 8) != 74 + raw_length)
		goto refused;
	atom = reference->evidence + 16;
	if (cluster_undo_tt_ctrc_get_u16_le(atom) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 2) != 56
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 66) != raw_length
		|| memcmp(atom + 10, reference->key + 44, 56) != 0)
		goto refused;
	node = (ClusterRecoveryCTRCGraphNodeV2){
		1,
		{ reference->key, sizeof(reference->key) },
		{ reference->evidence, reference->evidence_length }
	};
	memcpy(out, &node, sizeof(node));
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_bind_graph_node_v2(
	const ClusterRecoveryCTRCBindReferenceV2 *reference,
	ClusterRecoveryCTRCGraphNodeV2 *out)
{
	ClusterRecoveryCTRCGraphNodeV2 node;
	const uint8 *atom;

	if (reference == NULL || out == NULL
		|| cluster_undo_tt_ctrc_get_u16_le(reference->key + 34) != 3
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 36) != 56
		|| cluster_undo_tt_ctrc_get_u32_le(reference->key + 64)
			!= CLUSTER_RECOVERY_CTRC_BIND_RAW_BYTES
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 100) != 42
		|| cluster_undo_tt_ctrc_get_u64_le(reference->evidence) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(reference->evidence + 8)
			!= CLUSTER_RECOVERY_CTRC_BIND_ATOM_BYTES)
		goto refused;
	atom = reference->evidence + 16;
	if (cluster_undo_tt_ctrc_get_u16_le(atom) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 2) != 56
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 66)
			!= CLUSTER_RECOVERY_CTRC_BIND_RAW_BYTES
		|| memcmp(atom + 10, reference->key + 44, 56) != 0)
		goto refused;
	node = (ClusterRecoveryCTRCGraphNodeV2){
		1,
		{ reference->key, sizeof(reference->key) },
		{ reference->evidence, sizeof(reference->evidence) }
	};
	memcpy(out, &node, sizeof(node));
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_tt_data_reference_graph_node_v2(
	const ClusterRecoveryCTRCTTDataReferenceV2 *reference,
	ClusterRecoveryCTRCGraphNodeV2 *out)
{
	ClusterRecoveryCTRCGraphNodeV2 node;
	const uint8 *atom;
	size_t id_length;
	uint16 slot_offset;
	uint64 atom_length;

	if (reference == NULL || out == NULL
		|| reference->key_length < 60
		|| reference->key_length > CLUSTER_RECOVERY_CTRC_TT_DATA_KEY_MAX_BYTES
		|| reference->evidence_length < 16
		|| reference->evidence_length > CLUSTER_RECOVERY_CTRC_TT_DATA_EVIDENCE_MAX_BYTES)
		goto refused;
	id_length = reference->key_length - 60;
	slot_offset = cluster_undo_tt_ctrc_get_u16_le(reference->key + 26);
	if (slot_offset >= TT_SLOTS_PER_SEGMENT
		|| cluster_undo_tt_ctrc_get_u16_le(reference->key + 34) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 36) != id_length
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 44 + id_length)
			!= offsetof(UndoSegmentHeaderData, tt_slots)
				+ slot_offset * sizeof(TTSlot) + offsetof(TTSlot, xid)
		|| cluster_undo_tt_ctrc_get_u64_le(reference->key + 52 + id_length) != slot_offset
		|| cluster_undo_tt_ctrc_get_u64_le(reference->evidence) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(reference->evidence + 8)
			!= reference->evidence_length - 16)
		goto refused;
	atom = reference->evidence + 16;
	atom_length = cluster_undo_tt_ctrc_get_u64_le(reference->evidence + 8);
	if (cluster_undo_tt_ctrc_get_u16_le(atom) != 2
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 2) != id_length + 16
		|| atom_length != 34 + id_length + BLCKSZ
		|| reference->evidence_length != 16 + 34 + id_length + BLCKSZ
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 10 + id_length) != 0
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 18 + id_length) != BLCKSZ
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 26 + id_length) != BLCKSZ
		|| memcmp(atom + 10, reference->key + 44, id_length) != 0)
		goto refused;
	node = (ClusterRecoveryCTRCGraphNodeV2){
		1,
		{ reference->key, reference->key_length },
		{ reference->evidence, reference->evidence_length }
	};
	memcpy(out, &node, sizeof(node));
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_bind_reference_v2(XLogReaderState *reader,
										  uint64 database_incarnation, uint32 source_index,
										  const uint8 source_identity_digest[32],
										  const uint8 *raw_record, size_t raw_length,
										  ClusterRecoveryCTRCBindReferenceV2 *out)
{
	ClusterRecoveryCTRCTTReferenceV2 tt;
	ClusterRecoveryCTRCBindReferenceV2 reference;

	if (out == NULL)
		return false;
	if (reader == NULL || reader->record == NULL
		|| XLogRecGetInfo(reader) != XLOG_UNDO_TT_SLOT_BIND
		|| !cluster_recovery_ctrc_tt_reference_v2(reader, database_incarnation, source_index,
												source_identity_digest, raw_record, raw_length, &tt))
		goto refused;
	memcpy(reference.key, tt.key, sizeof(reference.key));
	memcpy(reference.evidence, tt.evidence, sizeof(reference.evidence));
	memcpy(out, &reference, sizeof(reference));
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

/* Shared byte encoding does not broaden the selected-slot validation scope. */
typedef struct CTRCTTDataObjectV2 {
	ClusterRecoveryNamespaceEntry native;
	PGAlignedBlock page;
	uint8 id[46 + MAXPGPATH - 1];
	size_t id_length;
} CTRCTTDataObjectV2;

static bool
ctrc_tt_data_object(uint64 system_identifier, uint64 database_incarnation,
	const ClusterRecoveryNamespaceEntry *entry, const void *header_page, size_t header_length,
	CTRCTTDataObjectV2 *object)
{
	ClusterRecoveryNamespaceEntry *native = &object->native;
	const UndoSegmentHeaderData *hdr = (const UndoSegmentHeaderData *)object->page.data;
	RelFileLocator locator;
	ForkNumber forknum;
	char canonical[MAXPGPATH];
	const char *end;
	size_t path_length;
	int canonical_length;
	uint16 origin;

	if (system_identifier == 0 || database_incarnation == 0 || entry == NULL
		|| header_page == NULL || header_length != BLCKSZ)
		return false;
	/* Consume possibly unaligned/aliased input only through aligned snapshots.
	 * The caller still owns the actual reread and final ROOT/namespace checks. */
	memcpy(native, entry, sizeof(*native));
	end = memchr(native->path, '\0', sizeof(native->path));
	if (end == NULL || end == native->path || native->kind != CLUSTER_RECOVERY_FILE_UNDO_SEGMENT
		|| native->size < BLCKSZ || native->undo_owner_instance == 0
		|| native->undo_owner_instance > 128 || native->undo_segment == 0)
		return false;
	path_length = end - native->path;
	origin = native->undo_owner_instance - 1;
	canonical_length = snprintf(canonical, sizeof(canonical), "pg_undo/instance_%u/seg_%u.dat",
								origin, native->undo_segment);
	if (canonical_length < 0 || (size_t)canonical_length != path_length
		|| memcmp(canonical, native->path, path_length) != 0
		|| cluster_recovery_namespace_classify(native->path, &locator, &forknum)
			   != CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
		return false;
	memcpy(object->page.data, header_page, BLCKSZ);
	if (!UndoSegmentHeader_identity_matches(object->page.data, native->undo_segment,
										  native->undo_owner_instance)
		|| cluster_undo_segment_header_integrity(object->page.data) != CLUSTER_UNDO_HEADER_VALID
		|| hdr->wrap_count == UINT32_MAX)
		return false;

	/* DATA ID: original relative namespace, native file generation and block 0.
	 * stat/size are not a qualified base or durable owner completion proof. */
	object->id_length = 46 + path_length;
	cluster_undo_tt_ctrc_put_u64_le(object->id, system_identifier);
	cluster_undo_tt_ctrc_put_u64_le(object->id + 8, database_incarnation);
	cluster_undo_tt_ctrc_put_u16_le(object->id + 16, origin);
	cluster_undo_tt_ctrc_put_u32_le(object->id + 18, CLUSTER_RECOVERY_FILE_UNDO_SEGMENT);
	cluster_undo_tt_ctrc_put_u64_le(object->id + 22, path_length);
	memcpy(object->id + 30, native->path, path_length);
	cluster_undo_tt_ctrc_put_u64_le(object->id + 30 + path_length, hdr->wrap_count);
	cluster_undo_tt_ctrc_put_u64_le(object->id + 38 + path_length, 0);
	return true;
}

static bool
ctrc_tt_data_slot(const TTSlot *slot)
{
	return TransactionIdIsNormal(slot->xid) && slot->wrap != TT_WRAP_INVALID
		&& (slot->flags & ~TT_SLOT_FLAGS_KNOWN) == 0
		&& (slot->status == TT_SLOT_ACTIVE || slot->status == TT_SLOT_COMMITTED
			|| slot->status == TT_SLOT_ABORTED || slot->status == TT_SLOT_RECYCLABLE);
}

/* T34 || U16(DATA carrier) || B(DATA ID) || actual field offset/ordinal. */
static size_t
ctrc_tt_data_key(const CTRCTTDataObjectV2 *object, uint16 slot_offset, const TTSlot *slot,
	uint8 *key)
{
	const UndoSegmentHeaderData *hdr = (const UndoSegmentHeaderData *)object->page.data;

	/* DATA ID's first 18 bytes are exactly the T34 system/incarnation/origin. */
	memcpy(key, object->id, 18);
	cluster_undo_tt_ctrc_put_u32_le(key + 18, object->native.undo_segment);
	cluster_undo_tt_ctrc_put_u32_le(key + 22, hdr->wrap_count);
	cluster_undo_tt_ctrc_put_u16_le(key + 26, slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(key + 28, slot->wrap);
	cluster_undo_tt_ctrc_put_u32_le(key + 30, slot->xid);
	cluster_undo_tt_ctrc_put_u16_le(key + 34, 1);
	cluster_undo_tt_ctrc_put_u64_le(key + 36, object->id_length);
	memcpy(key + 44, object->id, object->id_length);
	cluster_undo_tt_ctrc_put_u64_le(key + 44 + object->id_length,
								  offsetof(UndoSegmentHeaderData, tt_slots)
									  + slot_offset * sizeof(TTSlot) + offsetof(TTSlot, xid));
	cluster_undo_tt_ctrc_put_u64_le(key + 52 + object->id_length, slot_offset);
	return 60 + object->id_length;
}

static size_t
ctrc_tt_data_evidence(const CTRCTTDataObjectV2 *object, uint8 *evidence)
{
	uint8 *atom = evidence + 16;
	size_t atom_length = 34 + object->id_length + BLCKSZ;

	/* A single atom2 covers the entire original object, including padding and
	 * native UBA128. It does not invent the separate frozen U64 UNDO carrier. */
	cluster_undo_tt_ctrc_put_u16_le(atom, 2);
	cluster_undo_tt_ctrc_put_u64_le(atom + 2, object->id_length + 16);
	memcpy(atom + 10, object->id, object->id_length);
	cluster_undo_tt_ctrc_put_u64_le(atom + 10 + object->id_length, 0);
	cluster_undo_tt_ctrc_put_u64_le(atom + 18 + object->id_length, BLCKSZ);
	cluster_undo_tt_ctrc_put_u64_le(atom + 26 + object->id_length, BLCKSZ);
	memcpy(atom + 34 + object->id_length, object->page.data, BLCKSZ);
	cluster_undo_tt_ctrc_put_u64_le(evidence, 1);
	cluster_undo_tt_ctrc_put_u64_le(evidence + 8, atom_length);
	return 16 + atom_length;
}

bool
cluster_recovery_ctrc_tt_data_reference_v2(
	uint64 system_identifier, uint64 database_incarnation,
	const ClusterRecoveryNamespaceEntry *entry, const void *header_page, size_t header_length,
	uint16 slot_offset, ClusterRecoveryCTRCTTDataReferenceV2 *out)
{
	CTRCTTDataObjectV2 object;
	ClusterRecoveryCTRCTTDataReferenceV2 reference = { 0 };
	const UndoSegmentHeaderData *hdr = (const UndoSegmentHeaderData *)object.page.data;

	if (out == NULL)
		return false;
	if (slot_offset >= TT_SLOTS_PER_SEGMENT
		|| !ctrc_tt_data_object(system_identifier, database_incarnation, entry, header_page,
								header_length, &object)
		|| !ctrc_tt_data_slot(&hdr->tt_slots[slot_offset]))
		goto refused;
	reference.key_length = ctrc_tt_data_key(&object, slot_offset, &hdr->tt_slots[slot_offset],
										  reference.key);
	reference.evidence_length = ctrc_tt_data_evidence(&object, reference.evidence);
	memcpy(out, &reference, sizeof(reference));
	return true;

refused:
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_tt_data_inventory_v2(
	uint64 system_identifier, uint64 database_incarnation,
	const ClusterRecoveryNamespaceEntry *entry, const void *header_page, size_t header_length,
	ClusterRecoveryCTRCTTDataInventoryV2 *out)
{
	CTRCTTDataObjectV2 object;
	ClusterRecoveryCTRCTTDataInventoryV2 inventory;
	ClusterRecoveryUndoScan *scan = NULL;
	ClusterRecoveryUndoEntry candidate;
	TTSlot zero;

	if (out == NULL)
		return false;
	memset(&inventory, 0, sizeof(inventory));
	memset(&zero, 0, sizeof(zero));
	if (!ctrc_tt_data_object(system_identifier, database_incarnation, entry, header_page,
							 header_length, &object)
		|| cluster_recovery_undo_scan_begin(object.page.data, BLCKSZ, object.native.undo_segment,
										   object.native.undo_owner_instance, &scan)
			   != CLUSTER_RECOVERY_UNDO_MORE)
		goto refused;

	/* The original scanner visits every physical slot, independent of bitmap,
	 * active count, FULL lifecycle or release flags. No semantic graph is
	 * inferred from this object's END or an empty reference list. */
	for (uint16 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		if (cluster_recovery_undo_scan_next(scan, &candidate) != CLUSTER_RECOVERY_UNDO_ENTRY
			|| candidate.slot_offset != i)
			goto refused;
		if (candidate.slot.status == TT_SLOT_UNUSED) {
			/* Original ACTIVE transition only accepts the complete zero UNUSED
			 * predecessor. Residual xid/wrap/UBA/padding must not disappear. */
			if (memcmp(&candidate.slot, &zero, sizeof(zero)) != 0)
				goto refused;
			continue;
		}
		if (!ctrc_tt_data_slot(&candidate.slot))
			goto refused;
		ctrc_tt_data_key(&object, i, &candidate.slot, inventory.keys[inventory.reference_count++]);
	}
	if (cluster_recovery_undo_scan_next(scan, &candidate) != CLUSTER_RECOVERY_UNDO_END)
		goto refused;
	inventory.key_length = 60 + object.id_length;
	inventory.evidence_length = ctrc_tt_data_evidence(&object, inventory.evidence);
	cluster_recovery_undo_scan_destroy(scan);
	memcpy(out, &inventory, sizeof(inventory));
	return true;

refused:
	cluster_recovery_undo_scan_destroy(scan);
	memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_tt_data_graph_nodes_v2(
	const ClusterRecoveryCTRCTTDataInventoryV2 *inventory,
	ClusterRecoveryCTRCGraphNodeV2 out[TT_SLOTS_PER_SEGMENT], size_t *out_count)
{
	ClusterRecoveryCTRCGraphNodeV2 nodes[TT_SLOTS_PER_SEGMENT];
	size_t id_length;
	uint64 atom_length;
	const uint8 *atom;

	memset(nodes, 0, sizeof(nodes));
	if (inventory == NULL || out == NULL || out_count == NULL
		|| inventory->reference_count > TT_SLOTS_PER_SEGMENT
		|| inventory->key_length < 60
		|| inventory->key_length > CLUSTER_RECOVERY_CTRC_TT_DATA_KEY_MAX_BYTES
		|| inventory->evidence_length < 16
		|| inventory->evidence_length > CLUSTER_RECOVERY_CTRC_TT_DATA_EVIDENCE_MAX_BYTES)
		goto refused;
	id_length = inventory->key_length - 60;
	if (cluster_undo_tt_ctrc_get_u64_le(inventory->evidence) != 1
		|| cluster_undo_tt_ctrc_get_u64_le(inventory->evidence + 8)
			!= inventory->evidence_length - 16)
		goto refused;
	atom = inventory->evidence + 16;
	atom_length = cluster_undo_tt_ctrc_get_u64_le(inventory->evidence + 8);
	if (cluster_undo_tt_ctrc_get_u16_le(atom) != 2
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 2) != id_length + 16
		|| atom_length != 34 + id_length + BLCKSZ
		|| cluster_undo_tt_ctrc_get_u64_le(atom + 26 + id_length) != BLCKSZ
		|| (inventory->reference_count > 0
			&& memcmp(atom + 10, inventory->keys[0] + 44, id_length) != 0))
		goto refused;

	for (size_t i = 0; i < inventory->reference_count; i++) {
		const uint8 *key = inventory->keys[i];

		if (cluster_undo_tt_ctrc_get_u16_le(key + 34) != 1
			|| cluster_undo_tt_ctrc_get_u64_le(key + 36) != id_length
			|| memcmp(key + 44, atom + 10, id_length) != 0
			|| cluster_undo_tt_ctrc_get_u64_le(key + 52 + id_length)
			!= cluster_undo_tt_ctrc_get_u16_le(key + 26))
			goto refused;
		nodes[i] = (ClusterRecoveryCTRCGraphNodeV2){
			1,
			{ inventory->keys[i], inventory->key_length },
			{ inventory->evidence, inventory->evidence_length }
		};
	}
	memcpy(out, nodes, sizeof(nodes));
	*out_count = inventory->reference_count;
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(nodes));
	if (out_count != NULL)
		*out_count = 0;
	return false;
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
static int
ctrc_owner_source_cmp(const void *a, const void *b)
{
	return memcmp(a, b, 4);
}

static int
ctrc_owner_effect_cmp(const void *a, const void *b)
{
	return memcmp(a, b, 62);
}

bool
cluster_recovery_ctrc_owner_key_v2(const ClusterRecoveryCTRCOwnerScopeV2 *scope,
	uint8 *out, size_t out_length)
{
	ClusterRecoveryCTRCOwnerScopeV2 value;
	const ClusterRecoveryTxnKey *key;
	uint8 *scratch = NULL;
	uint8 *sources, *effects;
	size_t length = 60;

	if (scope == NULL || out == NULL)
		goto refused;
	/* Logical containers may be unaligned or overlap the final output. */
	memcpy(&value, scope, sizeof(value));
	key = &value.transaction;
	if (value.owner_family < 1 || value.owner_family > 14
		|| key->system_identifier == 0 || key->database_incarnation == 0
		|| key->origin_node_id > SCN_MAX_VALID_NODE_ID || key->segment_id == 0
		|| (key->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != key->origin_node_id
		|| key->segment_generation == UINT32_MAX || key->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| key->slot_wrap == TT_WRAP_INVALID || !TransactionIdIsNormal(key->xid)
		|| (value.source_count != 0 && value.source_indexes == NULL)
		|| (value.effect_count != 0 && value.effects == NULL)
		|| value.source_count > MaxAllocSize / sizeof(uint32)
		|| value.effect_count > MaxAllocSize / sizeof(ClusterRecoveryCTRCOwnerEffectV2)
		|| !ctrc_graph_size_add(&length, value.source_count, 4)
		|| !ctrc_graph_size_add(&length, value.effect_count, 62) || out_length != length)
		goto refused;
	scratch = palloc_extended(length, MCXT_ALLOC_NO_OOM);
	if (scratch == NULL)
		goto refused;
	cluster_undo_tt_ctrc_put_u16_le(scratch, value.owner_family);
	cluster_undo_tt_ctrc_put_u64_le(scratch + 2, length - 10);
	cluster_undo_tt_ctrc_put_u64_le(scratch + 10, key->system_identifier);
	cluster_undo_tt_ctrc_put_u64_le(scratch + 18, key->database_incarnation);
	cluster_undo_tt_ctrc_put_u16_le(scratch + 26, key->origin_node_id);
	cluster_undo_tt_ctrc_put_u32_le(scratch + 28, key->segment_id);
	cluster_undo_tt_ctrc_put_u32_le(scratch + 32, key->segment_generation);
	cluster_undo_tt_ctrc_put_u16_le(scratch + 36, key->slot_offset);
	cluster_undo_tt_ctrc_put_u16_le(scratch + 38, key->slot_wrap);
	cluster_undo_tt_ctrc_put_u32_le(scratch + 40, key->xid);
	cluster_undo_tt_ctrc_put_u64_le(scratch + 44, value.source_count);
	sources = scratch + 52;
	for (size_t i = 0; i < value.source_count; i++) {
		uint32 source;

		memcpy(&source, (const uint8 *)value.source_indexes + i * sizeof(source), sizeof(source));
		cluster_undo_tt_ctrc_put_u32_le(sources + i * 4, source);
	}
	qsort(sources, value.source_count, 4, ctrc_owner_source_cmp);
	for (size_t i = 1; i < value.source_count; i++)
		if (memcmp(sources + (i - 1) * 4, sources + i * 4, 4) == 0)
			goto refused;
	cluster_undo_tt_ctrc_put_u64_le(sources + value.source_count * 4, value.effect_count);
	effects = sources + value.source_count * 4 + 8;
	for (size_t i = 0; i < value.effect_count; i++) {
		ClusterRecoveryCTRCOwnerEffectV2 effect;
		const ClusterRecoveryCTRCReferenceV2 *record = &effect.record;
		uint8 *encoded = effects + i * 62;

		memcpy(&effect, (const uint8 *)value.effects + i * sizeof(effect), sizeof(effect));
		if (record->start == InvalidXLogRecPtr || record->end <= record->start
			|| record->raw_length < SizeOfXLogRecord || record->raw_length > XLogRecordMaxSize
			|| record->end - record->start < record->raw_length
			|| !ctrc_nonzero_digest(record->raw_record_sha256))
			goto refused;
		cluster_undo_tt_ctrc_put_u32_le(encoded, record->source_index);
		if (bsearch(encoded, sources, value.source_count, 4, ctrc_owner_source_cmp) == NULL)
			goto refused;
		cluster_undo_tt_ctrc_put_u64_le(encoded + 4, record->start);
		cluster_undo_tt_ctrc_put_u64_le(encoded + 12, record->end);
		cluster_undo_tt_ctrc_put_u32_le(encoded + 20, record->raw_length);
		memcpy(encoded + 24, record->raw_record_sha256, 32);
		cluster_undo_tt_ctrc_put_u16_le(encoded + 56, effect.component_kind);
		cluster_undo_tt_ctrc_put_u32_le(encoded + 58, effect.component_ordinal);
	}
	qsort(effects, value.effect_count, 62, ctrc_owner_effect_cmp);
	for (size_t i = 1; i < value.effect_count; i++)
		if (memcmp(effects + (i - 1) * 62, effects + i * 62, 62) == 0)
			goto refused;
	/* No caller output is published before every input has been consumed. */
	memcpy(out, scratch, length);
	pfree(scratch);
	return true;

refused:
	if (scratch != NULL)
		pfree(scratch);
	if (out != NULL)
		memset(out, 0, Min(out_length, (size_t)60));
	return false;
}

/* Recheck the exact owner-key envelope before borrowing it as a graph id. */
static bool
ctrc_owner_key_shape_v2(const uint8 *owner_key, size_t owner_key_length)
{
	const uint8 *sources, *effects;
	uint64 source_count_u64, effect_count_u64;
	size_t source_count, effect_count, effects_offset;
	uint32 segment_id;

	if (owner_key == NULL || owner_key_length < 60 || owner_key_length > MaxAllocSize
		|| cluster_undo_tt_ctrc_get_u16_le(owner_key) < 1
		|| cluster_undo_tt_ctrc_get_u16_le(owner_key) > 14
		|| cluster_undo_tt_ctrc_get_u64_le(owner_key + 2) != owner_key_length - 10
		|| cluster_undo_tt_ctrc_get_u64_le(owner_key + 10) == 0
		|| cluster_undo_tt_ctrc_get_u64_le(owner_key + 18) == 0
		|| cluster_undo_tt_ctrc_get_u16_le(owner_key + 26) > SCN_MAX_VALID_NODE_ID
		|| (segment_id = cluster_undo_tt_ctrc_get_u32_le(owner_key + 28)) == 0
		|| (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE
			!= cluster_undo_tt_ctrc_get_u16_le(owner_key + 26)
		|| cluster_undo_tt_ctrc_get_u32_le(owner_key + 32) == UINT32_MAX
		|| cluster_undo_tt_ctrc_get_u16_le(owner_key + 36) >= TT_SLOTS_PER_SEGMENT
		|| cluster_undo_tt_ctrc_get_u16_le(owner_key + 38) == TT_WRAP_INVALID
		|| !TransactionIdIsNormal(cluster_undo_tt_ctrc_get_u32_le(owner_key + 40)))
		return false;
	source_count_u64 = cluster_undo_tt_ctrc_get_u64_le(owner_key + 44);
	if (source_count_u64 > (owner_key_length - 60) / sizeof(uint32))
		return false;
	source_count = (size_t) source_count_u64;
	effects_offset = 60 + source_count * sizeof(uint32);
	effect_count_u64 = cluster_undo_tt_ctrc_get_u64_le(owner_key + effects_offset - 8);
	if (effect_count_u64 > (owner_key_length - effects_offset) / 62)
		return false;
	effect_count = (size_t) effect_count_u64;
	if (effects_offset + effect_count * 62 != owner_key_length)
		return false;
	sources = owner_key + 52;
	effects = owner_key + effects_offset;
	for (size_t i = 1; i < source_count; i++)
		if (memcmp(sources + (i - 1) * sizeof(uint32),
				sources + i * sizeof(uint32), sizeof(uint32)) >= 0)
			return false;
	for (size_t i = 0; i < effect_count; i++) {
		const uint8 *effect = effects + i * 62;
		uint64 start = cluster_undo_tt_ctrc_get_u64_le(effect + 4);
		uint64 end = cluster_undo_tt_ctrc_get_u64_le(effect + 12);
		uint32 raw_length = cluster_undo_tt_ctrc_get_u32_le(effect + 20);

		if ((i != 0 && memcmp(effects + (i - 1) * 62, effect, 62) >= 0)
			|| bsearch(effect, sources, source_count, 4, ctrc_owner_source_cmp) == NULL
			|| start == InvalidXLogRecPtr || end <= start
			|| raw_length < SizeOfXLogRecord || raw_length > XLogRecordMaxSize
			|| end - start < raw_length || !ctrc_nonzero_digest(effect + 24))
			return false;
	}
	return true;
}

bool
cluster_recovery_ctrc_owner_graph_node_v2(const uint8 *owner_key,
	size_t owner_key_length, const ClusterRecoveryCTRCGraphBytesV2 *evidence,
	ClusterRecoveryCTRCGraphNodeV2 *out)
{
	ClusterRecoveryCTRCGraphNodeV2 node;
	ClusterRecoveryCTRCGraphBytesV2 witness;

	if (out == NULL || evidence == NULL || owner_key_length > MaxAllocSize - 18
		|| !ctrc_owner_key_shape_v2(owner_key, owner_key_length))
		goto refused;
	memcpy(&witness, evidence, sizeof(witness));
	if (witness.data == NULL || witness.length == 0
		|| witness.length > MaxAllocSize - 18 - owner_key_length)
		goto refused;
	node = (ClusterRecoveryCTRCGraphNodeV2){
		2, { owner_key, owner_key_length }, witness
	};
	memcpy(out, &node, sizeof(node));
	return true;

refused:
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}

bool
cluster_recovery_ctrc_owner_transaction_matches_v2(const uint8 *owner_key,
	size_t owner_key_length, const uint8 *payload, size_t payload_length,
	uint64 system_identifier)
{
	ClusterRecoveryCTRCReleaseV2 record;

	if (!ctrc_owner_key_shape_v2(owner_key, owner_key_length)
		|| !cluster_recovery_ctrc_decode_v2(payload, payload_length, system_identifier, &record))
		return false;
	/* T34 is encoded explicitly, never compared as a padded native struct.
	 * Same transaction is not proof of complete or applicable owner work. */
	return cluster_undo_tt_ctrc_get_u64_le(owner_key + 10) == system_identifier
		&& cluster_undo_tt_ctrc_get_u64_le(owner_key + 18) == record.database_incarnation
		&& cluster_undo_tt_ctrc_get_u16_le(owner_key + 26) == record.origin_node_id
		&& cluster_undo_tt_ctrc_get_u32_le(owner_key + 28) == record.segment_id
		&& cluster_undo_tt_ctrc_get_u32_le(owner_key + 32) == record.segment_generation
		&& cluster_undo_tt_ctrc_get_u16_le(owner_key + 36) == record.slot_offset
		&& cluster_undo_tt_ctrc_get_u16_le(owner_key + 38) == record.wrap
		&& cluster_undo_tt_ctrc_get_u32_le(owner_key + 40) == record.xid;
}

typedef struct CTRCRawEvidenceWork {
	ClusterRecoveryCTRCGraphBytesV2 atom;
	size_t identity_length;
} CTRCRawEvidenceWork;

static int
ctrc_raw_evidence_cmp(const void *a, const void *b)
{
	return ctrc_graph_bytes_cmp(&((const CTRCRawEvidenceWork *)a)->atom,
								&((const CTRCRawEvidenceWork *)b)->atom);
}

static bool
ctrc_raw_atom_identity_length_v2(const ClusterRecoveryCTRCGraphBytesV2 *atom,
	size_t *identity_length)
{
	uint64 subject_length, bytes_length;
	uint16 kind;

	if (atom->data == NULL || atom->length < 20 || atom->length > MaxAllocSize)
		return false;
	kind = cluster_undo_tt_ctrc_get_u16_le(atom->data);
	subject_length = cluster_undo_tt_ctrc_get_u64_le(atom->data + 2);
	if ((kind != 1 && kind != 2) || subject_length == 0
		|| subject_length > atom->length - 18)
		return false;
	*identity_length = 10 + (size_t)subject_length;
	bytes_length = cluster_undo_tt_ctrc_get_u64_le(atom->data + *identity_length);
	return bytes_length != 0 && bytes_length == atom->length - *identity_length - 8;
}

static bool
ctrc_raw_evidence_encode_v2(const ClusterRecoveryCTRCGraphBytesV2 *atoms,
	size_t atom_count, uint8 *out, size_t out_capacity, size_t *out_length,
	bool witness_ids)
{
	CTRCRawEvidenceWork *work = NULL;
	size_t scratch = 0, length = 8, unique = 0, zero = 0;
	uint8 *next;

	if (out == NULL || out_length == NULL || out_capacity < 8 || out_capacity > MaxAllocSize
		|| (witness_ids && atom_count == 0))
		goto refused;
	if (atom_count != 0) {
		if (atoms == NULL || atom_count > MaxAllocSize / sizeof(*atoms)
			|| !ctrc_graph_size_add(&scratch, atom_count, sizeof(*work)))
			goto refused;
		/* Bound all descriptors and byte copies before reading any envelope. */
		for (size_t i = 0; i < atom_count; i++) {
			ClusterRecoveryCTRCGraphBytesV2 atom;

			memcpy(&atom, (const uint8 *)atoms + i * sizeof(atom), sizeof(atom));
			if (atom.data == NULL || atom.length < 20
				|| !ctrc_graph_size_add(&scratch, 1, atom.length))
				goto refused;
		}
		work = palloc_extended(scratch, MCXT_ALLOC_NO_OOM);
		if (work == NULL)
			goto refused;
		next = (uint8 *)(work + atom_count);
		for (size_t i = 0; i < atom_count; i++) {
			ClusterRecoveryCTRCGraphBytesV2 atom;

			memcpy(&atom, (const uint8 *)atoms + i * sizeof(atom), sizeof(atom));
			memcpy(next, atom.data, atom.length);
			work[i].atom = (ClusterRecoveryCTRCGraphBytesV2){ next, atom.length };
			if (!ctrc_raw_atom_identity_length_v2(&work[i].atom, &work[i].identity_length))
				goto refused;
			next += atom.length;
		}
		qsort(work, atom_count, sizeof(*work), ctrc_raw_evidence_cmp);
		for (size_t i = 0; i < atom_count; i++) {
			CTRCRawEvidenceWork atom = work[i];

			if (unique != 0) {
				const CTRCRawEvidenceWork *previous = &work[unique - 1];

				if (ctrc_graph_bytes_cmp(&previous->atom, &atom.atom) == 0)
					continue;
				/* The encoded kind/subject is a prefix, so conflicts are adjacent. */
				if (previous->identity_length == atom.identity_length
					&& memcmp(previous->atom.data, atom.atom.data, atom.identity_length) == 0)
					goto refused;
			}
			if (!ctrc_graph_size_add(&length, 1, 8)
				|| !ctrc_graph_size_add(&length, 1,
					witness_ids ? atom.identity_length : atom.atom.length))
				goto refused;
			work[unique++] = atom;
		}
	}
	if (length > out_capacity)
		goto refused;
	/* Every input has been copied and checked before the first output byte. */
	cluster_undo_tt_ctrc_put_u64_le(out, unique);
	next = out + 8;
	for (size_t i = 0; i < unique; i++) {
		ClusterRecoveryCTRCGraphBytesV2 bytes = work[i].atom;

		/* The complete kind/subject prefix is length-delimited, so raw atom
		 * order also orders IDs. Conflicting raw bytes were rejected above. */
		if (witness_ids)
			bytes.length = work[i].identity_length;
		next = ctrc_graph_put_bytes(next, &bytes);
	}
	if (work != NULL)
		pfree(work);
	memcpy(out_length, &length, sizeof(length));
	return true;

refused:
	if (work != NULL)
		pfree(work);
	if (out != NULL)
		memset(out, 0, Min(out_capacity, (size_t)8));
	if (out_length != NULL)
		memcpy(out_length, &zero, sizeof(zero));
	return false;
}

bool
cluster_recovery_ctrc_raw_evidence_v2(const ClusterRecoveryCTRCGraphBytesV2 *atoms,
	size_t atom_count, uint8 *out, size_t out_capacity, size_t *out_length)
{
	return ctrc_raw_evidence_encode_v2(atoms, atom_count, out, out_capacity, out_length, false);
}

bool
cluster_recovery_ctrc_witness_ids_v2(const ClusterRecoveryCTRCGraphBytesV2 *atoms,
	size_t atom_count, uint8 *out, size_t out_capacity, size_t *out_length)
{
	return ctrc_raw_evidence_encode_v2(atoms, atom_count, out, out_capacity, out_length, true);
}

typedef struct CTRCRawEvidenceCursor {
	const uint8 *data;
	size_t length, position;
	uint64 remaining;
	ClusterRecoveryCTRCGraphBytesV2 previous;
	size_t previous_identity_length;
} CTRCRawEvidenceCursor;

static bool
ctrc_raw_evidence_cursor_v2(const uint8 *evidence, size_t evidence_length,
	CTRCRawEvidenceCursor *cursor)
{
	uint64 count;

	if (evidence == NULL || evidence_length < 8 || evidence_length > MaxAllocSize)
		return false;
	count = cluster_undo_tt_ctrc_get_u64_le(evidence);
	/* B(atom) requires eight length bytes and at least a 20-byte atom. */
	if (count == 0 || count > (evidence_length - 8) / 28)
		return false;
	*cursor = (CTRCRawEvidenceCursor){evidence, evidence_length, 8, count, {NULL, 0}, 0};
	return true;
}

static bool
ctrc_raw_evidence_next_v2(CTRCRawEvidenceCursor *cursor,
	ClusterRecoveryCTRCGraphBytesV2 *identity)
{
	ClusterRecoveryCTRCGraphBytesV2 atom;
	uint64 atom_length;
	size_t identity_length;

	if (cursor->remaining == 0 || cursor->length - cursor->position < 8)
		return false;
	atom_length = cluster_undo_tt_ctrc_get_u64_le(cursor->data + cursor->position);
	cursor->position += 8;
	if (atom_length > cursor->length - cursor->position)
		return false;
	atom = (ClusterRecoveryCTRCGraphBytesV2){cursor->data + cursor->position, (size_t)atom_length};
	if (!ctrc_raw_atom_identity_length_v2(&atom, &identity_length)
		|| (cursor->previous.data != NULL
			&& (ctrc_graph_bytes_cmp(&cursor->previous, &atom) >= 0
				|| (cursor->previous_identity_length == identity_length
					&& memcmp(cursor->previous.data, atom.data, identity_length) == 0))))
		return false;
	cursor->position += atom.length;
	cursor->remaining--;
	if (cursor->remaining == 0 && cursor->position != cursor->length)
		return false;
	cursor->previous = atom;
	cursor->previous_identity_length = identity_length;
	*identity = (ClusterRecoveryCTRCGraphBytesV2){atom.data, identity_length};
	return true;
}

bool
cluster_recovery_ctrc_witness_ids_from_evidence_v2(const uint8 *evidence,
	size_t evidence_length, uint8 *out, size_t out_capacity, size_t *out_length)
{
	uint8 *scratch = NULL, *next;
	CTRCRawEvidenceCursor cursor;
	uint64 count;
	size_t scratch_length = 0, zero = 0;

	if (out == NULL || out_length == NULL || out_capacity < 8 || out_capacity > MaxAllocSize
		|| !ctrc_raw_evidence_cursor_v2(evidence, evidence_length, &cursor))
		goto refused;
	count = cursor.remaining;
	for (unsigned pass = 0; pass < 2; pass++) {
		size_t length = 8;

		if (!ctrc_raw_evidence_cursor_v2(evidence, evidence_length, &cursor)
			|| cursor.remaining != count)
			goto refused;
		next = scratch;
		if (pass != 0) {
			cluster_undo_tt_ctrc_put_u64_le(next, count);
			next += 8;
		}
		for (uint64 i = 0; i < count; i++) {
			ClusterRecoveryCTRCGraphBytesV2 identity;

			if (!ctrc_raw_evidence_next_v2(&cursor, &identity)
				|| !ctrc_graph_size_add(&length, 1, 8)
				|| !ctrc_graph_size_add(&length, 1, identity.length)
				|| (pass != 0 && length > scratch_length))
				goto refused;
			if (pass != 0)
				next = ctrc_graph_put_bytes(next, &identity);
		}
		if (length > out_capacity
			|| (pass != 0 && length != scratch_length))
			goto refused;
		if (pass == 0) {
			scratch_length = length;
			scratch = palloc_extended(scratch_length, MCXT_ALLOC_NO_OOM);
			if (scratch == NULL)
				goto refused;
		}
	}
	/* Publish only after the entire canonical input has been consumed. */
	memcpy(out, scratch, scratch_length);
	pfree(scratch);
	memcpy(out_length, &scratch_length, sizeof(scratch_length));
	return true;

refused:
	if (scratch != NULL)
		pfree(scratch);
	if (out != NULL)
		memset(out, 0, Min(out_capacity, (size_t)8));
	if (out_length != NULL)
		memcpy(out_length, &zero, sizeof(zero));
	return false;
}

static bool
ctrc_witness_id_next_v2(const uint8 *ids, size_t length, size_t *position,
	ClusterRecoveryCTRCGraphBytesV2 *previous)
{
	ClusterRecoveryCTRCGraphBytesV2 id;
	uint64 id_length, subject_length;
	uint16 kind;

	if (length - *position < 8)
		return false;
	id_length = cluster_undo_tt_ctrc_get_u64_le(ids + *position);
	*position += 8;
	if (id_length < 11 || id_length > length - *position)
		return false;
	id = (ClusterRecoveryCTRCGraphBytesV2){ids + *position, (size_t)id_length};
	kind = cluster_undo_tt_ctrc_get_u16_le(id.data);
	subject_length = cluster_undo_tt_ctrc_get_u64_le(id.data + 2);
	if ((kind != 1 && kind != 2) || subject_length != id.length - 10
		|| (previous->data != NULL && ctrc_graph_bytes_cmp(previous, &id) >= 0))
		return false;
	*position += id.length;
	*previous = id;
	return true;
}

bool
cluster_recovery_ctrc_witness_ids_in_evidence_v2(const uint8 *witness_ids,
	size_t witness_ids_length, const uint8 *evidence, size_t evidence_length)
{
	CTRCRawEvidenceCursor cursor;
	ClusterRecoveryCTRCGraphBytesV2 wanted = {NULL, 0};
	uint64 remaining;
	size_t position = 8;
	bool matched = false;

	if (witness_ids == NULL || witness_ids_length < 8 || witness_ids_length > MaxAllocSize
		|| !ctrc_raw_evidence_cursor_v2(evidence, evidence_length, &cursor))
		return false;
	remaining = cluster_undo_tt_ctrc_get_u64_le(witness_ids);
	/* B(U16(kind)||B(nonempty subject)) needs at least nineteen bytes. */
	if (remaining == 0 || remaining > (witness_ids_length - 8) / 19
		|| remaining > cursor.remaining
		|| !ctrc_witness_id_next_v2(witness_ids, witness_ids_length, &position, &wanted))
		return false;
	remaining--;
	while (cursor.remaining != 0) {
		ClusterRecoveryCTRCGraphBytesV2 identity;
		int cmp;

		if (!ctrc_raw_evidence_next_v2(&cursor, &identity))
			return false;
		/* Matching the final witness never skips unselected raw validation. */
		if (matched)
			continue;
		cmp = ctrc_graph_bytes_cmp(&identity, &wanted);
		if (cmp > 0)
			return false;
		if (cmp == 0) {
			if (remaining == 0) {
				if (position != witness_ids_length)
					return false;
				matched = true;
			} else {
				if (!ctrc_witness_id_next_v2(witness_ids, witness_ids_length, &position, &wanted))
					return false;
				remaining--;
			}
		}
	}
	return matched;
}

bool
cluster_recovery_ctrc_cold_step_bytes_v2(const ClusterRecoveryCTRCColdStepV2 *step,
	uint8 *out, size_t out_length)
{
	ClusterRecoveryCTRCColdStepV2 value;
	uint8 bytes[72 + 18 * 64];
	uint64 seen = 0;
	size_t length, position = 72;

	if (step == NULL || out == NULL)
		goto refused;
	memcpy(&value, step, sizeof(value));
	if (value.formation_epoch == 0 || value.recovery_serial == 0
		|| value.cohort_bitmap == 0 || value.completed_bitmap != value.cohort_bitmap
		|| !ctrc_nonzero_digest(value.input_sha256) || value.members == NULL
		|| value.member_count == 0 || value.member_count > 64)
		goto refused;
	length = 72 + 18 * value.member_count;
	if (out_length != length)
		goto refused;
	cluster_undo_tt_ctrc_put_u64_le(bytes, value.formation_epoch);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 8, value.recovery_serial);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 16, value.cohort_bitmap);
	cluster_undo_tt_ctrc_put_u64_le(bytes + 24, value.completed_bitmap);
	memcpy(bytes + 32, value.input_sha256, sizeof(value.input_sha256));
	cluster_undo_tt_ctrc_put_u64_le(bytes + 64, value.member_count);
	for (size_t i = 0; i < value.member_count; i++) {
		ClusterRecoveryCTRCColdMemberV2 member;
		uint64 bit;
		uint8 *entry;

		memcpy(&member, (const uint8 *)value.members + i * sizeof(member), sizeof(member));
		if (member.node_id >= 64 || member.boot == 0 || member.admitted_incarnation == 0)
			goto refused;
		bit = UINT64_C(1) << member.node_id;
		if ((value.cohort_bitmap & bit) == 0 || (seen & bit) != 0)
			goto refused;
		seen |= bit;
		entry = bytes + 72 + 18 * member.node_id;
		cluster_undo_tt_ctrc_put_u16_le(entry, member.node_id);
		cluster_undo_tt_ctrc_put_u64_le(entry + 2, member.boot);
		cluster_undo_tt_ctrc_put_u64_le(entry + 10, member.admitted_incarnation);
	}
	if (seen != value.cohort_bitmap)
		goto refused;
	/* Compact indexed entries forwards: no unread higher-node entry is lost. */
	for (unsigned node = 0; node < 64; node++)
		if (seen & (UINT64_C(1) << node)) {
			memmove(bytes + position, bytes + 72 + 18 * node, 18);
			position += 18;
		}
	/* The descriptor and every member were consumed before touching out. */
	memcpy(out, bytes, length);
	return true;

refused:
	if (out != NULL)
		memset(out, 0, Min(out_length, (size_t)72));
	return false;
}

bool
cluster_recovery_ctrc_cold_step_matches_v2(const uint8 *bytes, size_t length,
	const uint8 *payload, size_t payload_length, uint64 system_identifier)
{
	ClusterRecoveryCTRCReleaseV2 record;
	uint64 cohort, count, seen = 0;
	uint16 previous = 0;

	if (bytes == NULL || length < 90 || length > 72 + 18 * 64)
		return false;
	cohort = cluster_undo_tt_ctrc_get_u64_le(bytes + 16);
	count = cluster_undo_tt_ctrc_get_u64_le(bytes + 64);
	if (cluster_undo_tt_ctrc_get_u64_le(bytes) == 0
		|| cluster_undo_tt_ctrc_get_u64_le(bytes + 8) == 0
		|| cohort == 0 || cluster_undo_tt_ctrc_get_u64_le(bytes + 24) != cohort
		|| !ctrc_nonzero_digest(bytes + 32) || count == 0 || count > 64
		|| length != 72 + 18 * (size_t)count)
		return false;
	for (size_t i = 0; i < (size_t)count; i++) {
		const uint8 *member = bytes + 72 + 18 * i;
		uint16 node = cluster_undo_tt_ctrc_get_u16_le(member);
		uint64 bit;

		if (node >= 64 || (i != 0 && node <= previous)
			|| cluster_undo_tt_ctrc_get_u64_le(member + 2) == 0
			|| cluster_undo_tt_ctrc_get_u64_le(member + 10) == 0)
			return false;
		bit = UINT64_C(1) << node;
		if ((cohort & bit) == 0)
			return false;
		seen |= bit;
		previous = node;
	}
	if (seen != cohort
		|| !cluster_recovery_ctrc_decode_v2(payload, payload_length, system_identifier, &record))
		return false;
	/* Only fields actually carried in atom4 are compared; current member ACKs
	 * and the original owner/graph predicates cannot be inferred from them. */
	return cluster_undo_tt_ctrc_get_u64_le(bytes) == record.formation_epoch
		&& cluster_undo_tt_ctrc_get_u64_le(bytes + 8) == record.recovery_serial
		&& cohort == record.cohort_bitmap && memcmp(bytes + 32, record.input_sha256, 32) == 0;
}

#endif
