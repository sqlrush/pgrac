/*-------------------------------------------------------------------------
 * cluster_recovery_reference_graph.c
 *   Read-only transaction-record dependencies for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_reference_graph.h"
#include "catalog/pg_tablespace_d.h"

#ifdef USE_PGRAC_CLUSTER

#define RECOVERY_GRAPH_NONE UINT32_MAX
#define RECOVERY_GRAPH_TICK 128

typedef enum RecoveryNodeState {
	RECOVERY_NODE_DISCOVERED,
	RECOVERY_NODE_IN_PROGRESS,
	RECOVERY_NODE_DONE
} RecoveryNodeState;

typedef struct RecoveryRecordNode {
	ClusterRecoveryTxnKey transaction;
	UBA address;
	ClusterRecoveryUndoRecord record;
	uint32 record_generation;
	uint32 predecessor;
	RecoveryNodeState state;
} RecoveryRecordNode;

struct ClusterRecoveryReferenceGraph {
	ClusterRecoveryGraphRead read;
	void *arg;
	RecoveryRecordNode *nodes;
	uint32 *buckets; /* node index + 1; equality always checks the full key */
	uint32 *stack;
	uint32 capacity;
	uint32 bucket_count;
	uint32 count;
	uint32 depth;
	uint32 root_count;
	uint32 next_root;
	uint32 next_verify;
	bool started;
	bool verifying;
	ClusterRecoveryGraphResult status;
	ClusterRecoveryUndoRecord scratch;
};

static ClusterRecoveryGraphResult
recovery_graph_fail(ClusterRecoveryReferenceGraph *graph, ClusterRecoveryGraphResult result)
{
	if (graph != NULL)
		graph->status = result;
	return result;
}

static bool
recovery_graph_key_valid(const ClusterRecoveryTxnKey *key)
{
	return key != NULL && key->system_identifier != 0 && key->database_incarnation != 0
		   && key->origin_node_id <= SCN_MAX_VALID_NODE_ID && key->segment_id != 0
		   && (key->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE == key->origin_node_id
		   && key->slot_offset < TT_SLOTS_PER_SEGMENT && key->slot_wrap != TT_WRAP_INVALID
		   && TransactionIdIsNormal(key->xid);
}

static bool
recovery_graph_key_equal(const ClusterRecoveryTxnKey *a, const ClusterRecoveryTxnKey *b)
{
	return a->system_identifier == b->system_identifier
		   && a->database_incarnation == b->database_incarnation && a->segment_id == b->segment_id
		   && a->segment_generation == b->segment_generation && a->xid == b->xid
		   && a->origin_node_id == b->origin_node_id && a->slot_offset == b->slot_offset
		   && a->slot_wrap == b->slot_wrap;
}

static bool
recovery_graph_address_matches(const ClusterRecoveryTxnKey *key, UBA address)
{
	uint32 segment, block;
	uint16 slot, row;

	return uba_decode_record(address, &segment, &block, &slot, &row)
		   && (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE == key->origin_node_id
		   && slot == key->slot_offset;
}

/* A process-local hash-table bucket only, never an identity/proof digest. */
static uint32
recovery_graph_bucket(const ClusterRecoveryTxnKey *key, UBA address, uint32 mask)
{
	const uint64 values[] = { key->system_identifier,
							  key->database_incarnation,
							  key->segment_id,
							  key->segment_generation,
							  key->xid,
							  key->origin_node_id,
							  key->slot_offset,
							  key->slot_wrap,
							  address.raw[0],
							  address.raw[1] };
	uint64 hash = UINT64CONST(14695981039346656037);

	for (unsigned i = 0; i < lengthof(values); i++)
		for (unsigned byte = 0; byte < 8; byte++) {
			hash ^= (values[i] >> (byte * 8)) & UINT64CONST(255);
			hash *= UINT64CONST(1099511628211);
		}
	return (uint32)hash & mask;
}

static ClusterRecoveryGraphResult
recovery_graph_node(ClusterRecoveryReferenceGraph *graph, const ClusterRecoveryTxnKey *key,
					UBA address, uint32 *index)
{
	uint32 bucket = recovery_graph_bucket(key, address, graph->bucket_count - 1);
	RecoveryRecordNode *node;

	while (graph->buckets[bucket] != 0) {
		uint32 candidate = graph->buckets[bucket] - 1;

		node = &graph->nodes[candidate];
		if (recovery_graph_key_equal(key, &node->transaction)
			&& cluster_undo_record_uba_equal(address, node->address)) {
			*index = candidate;
			return CLUSTER_RECOVERY_GRAPH_MORE;
		}
		bucket = (bucket + 1) & (graph->bucket_count - 1);
	}
	if (graph->count == graph->capacity)
		return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_CAPACITY);
	*index = graph->count++;
	node = &graph->nodes[*index];
	node->transaction = *key;
	node->address = address;
	node->predecessor = RECOVERY_GRAPH_NONE;
	graph->buckets[bucket] = *index + 1;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryGraphResult
cluster_recovery_tx_record_check(const ClusterRecoveryTxnKey *key, UBA address,
								 const ClusterRecoveryUndoRecord *record, uint32 record_generation)
{
	const UndoRecordHeader *header;
	uint32 segment, block;
	uint16 slot, row, body_length;
	UndoItlHistoryTrailer history;

	if (!recovery_graph_key_valid(key) || record == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	header = &record->header;
	if (!cluster_undo_record_uba_equal(address, record->address)
		|| !uba_decode_record(record->address, &segment, &block, &slot, &row)
		|| !recovery_graph_address_matches(key, record->address)
		|| header->origin_node_id != key->origin_node_id
		|| header->tt_slot_segment_id != key->segment_id
		|| header->tt_slot_id != (uint32)key->slot_offset + 1 || header->xid != key->xid
		|| header->tt_wrap_plus1 == 0 || header->tt_wrap_plus1 - 1 != key->slot_wrap
		|| (segment == key->segment_id && record_generation != key->segment_generation))
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	if (header->payload_length > sizeof(record->payload)
		|| record->directory.record_type != header->record_type
		|| record->directory.flags != header->flags
		|| record->directory.record_length != sizeof(*header) + header->payload_length
		|| header->target_locator.spcOid == InvalidOid || header->target_locator.relNumber == 0
		|| (header->target_locator.spcOid == GLOBALTABLESPACE_OID
				? header->target_locator.dbOid != InvalidOid
				: header->target_locator.dbOid == InvalidOid)
		|| header->target_fork < MAIN_FORKNUM || header->target_fork > MAX_FORKNUM
		|| header->target_block == InvalidBlockNumber
		|| header->target_offset == InvalidOffsetNumber
		|| !cluster_undo_record_decode_payload(header, (const char *)record->payload,
											   header->payload_length, &body_length, &history)
		|| record->body_length != body_length
		|| memcmp(&record->history, &history, sizeof(history)) != 0)
		return CLUSTER_RECOVERY_GRAPH_CORRUPT;
	if (!UBA_is_invalid(header->prev_uba) && !recovery_graph_address_matches(key, header->prev_uba))
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

static bool
recovery_graph_record_equal(const ClusterRecoveryUndoRecord *a, const ClusterRecoveryUndoRecord *b)
{
	return cluster_undo_record_uba_equal(a->address, b->address)
		   && memcmp(&a->block_header, &b->block_header, sizeof(a->block_header)) == 0
		   && memcmp(&a->directory, &b->directory, sizeof(a->directory)) == 0
		   && memcmp(&a->header, &b->header, sizeof(a->header)) == 0
		   && a->body_length == b->body_length
		   && memcmp(&a->history, &b->history, sizeof(a->history)) == 0
		   && memcmp(a->payload, b->payload, a->header.payload_length) == 0;
}

ClusterRecoveryGraphResult
cluster_recovery_reference_graph_begin(size_t memory_budget, ClusterRecoveryGraphRead read,
									   void *arg, ClusterRecoveryReferenceGraph **out)
{
	ClusterRecoveryReferenceGraph *graph;
	size_t available, capacity;
	uint32 buckets = 2;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || read == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (memory_budget <= sizeof(*graph))
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	available = memory_budget - sizeof(*graph);
	/* A power-of-two table needs fewer than four buckets per node, plus
	 * one DFS stack index. Include all storage, including the read scratch. */
	capacity = available / (sizeof(RecoveryRecordNode) + 5 * sizeof(uint32));
	if (capacity == 0 || capacity > UINT32_MAX / 4)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	while (buckets < 2 * capacity)
		buckets *= 2;
	graph = calloc(1, sizeof(*graph));
	if (graph == NULL)
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	graph->capacity = (uint32)capacity;
	graph->bucket_count = buckets;
	graph->nodes = calloc(capacity, sizeof(*graph->nodes));
	graph->stack = calloc(capacity, sizeof(*graph->stack));
	graph->buckets = calloc(buckets, sizeof(*graph->buckets));
	if (graph->nodes == NULL || graph->stack == NULL || graph->buckets == NULL) {
		cluster_recovery_reference_graph_destroy(graph);
		return CLUSTER_RECOVERY_GRAPH_CAPACITY;
	}
	graph->read = read;
	graph->arg = arg;
	*out = graph;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

bool
cluster_recovery_reference_graph_accepts_roots(const ClusterRecoveryReferenceGraph *graph)
{
	return graph != NULL && !graph->started && graph->status == CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryGraphResult
cluster_recovery_reference_graph_add(ClusterRecoveryReferenceGraph *graph,
									 const ClusterRecoveryTxnKey *transaction, UBA head)
{
	uint32 index;

	if (graph == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (graph->status != CLUSTER_RECOVERY_GRAPH_MORE)
		return graph->status;
	if (graph->started || !recovery_graph_key_valid(transaction))
		return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (!recovery_graph_address_matches(transaction, head))
		return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_IDENTITY);
	return recovery_graph_node(graph, transaction, head, &index);
}

static ClusterRecoveryGraphResult
recovery_graph_read_node(ClusterRecoveryReferenceGraph *graph, RecoveryRecordNode *node,
						 uint32 *generation)
{
	ClusterRecoveryGraphResult result;

	memset(&graph->scratch, 0, sizeof(graph->scratch));
	*generation = 0;
	result
		= graph->read(graph->arg, &node->transaction, node->address, &graph->scratch, generation);
	if (result == CLUSTER_RECOVERY_GRAPH_PENDING)
		return result;
	if (result != CLUSTER_RECOVERY_GRAPH_RECORD) {
		if (result < CLUSTER_RECOVERY_GRAPH_INVALID || result > CLUSTER_RECOVERY_GRAPH_CORRUPT)
			result = CLUSTER_RECOVERY_GRAPH_CORRUPT;
		return recovery_graph_fail(graph, result);
	}
	result = cluster_recovery_tx_record_check(&node->transaction, node->address, &graph->scratch,
											  *generation);
	return result == CLUSTER_RECOVERY_GRAPH_MORE ? result : recovery_graph_fail(graph, result);
}

ClusterRecoveryGraphResult
cluster_recovery_reference_graph_step(ClusterRecoveryReferenceGraph *graph,
									  ClusterRecoveryGraphRecord *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (graph == NULL || out == NULL)
		return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_INVALID);
	if (graph->status != CLUSTER_RECOVERY_GRAPH_MORE)
		return graph->status;
	if (!graph->started) {
		graph->started = true;
		graph->root_count = graph->count;
	}
	for (unsigned transition = 0; transition < RECOVERY_GRAPH_TICK; transition++) {
		RecoveryRecordNode *node;
		ClusterRecoveryGraphResult result;
		uint32 generation;

		if (graph->verifying) {
			if (graph->next_verify == graph->count) {
				graph->status = CLUSTER_RECOVERY_GRAPH_DRAINED;
				return graph->status;
			}
			node = &graph->nodes[graph->next_verify];
			result = recovery_graph_read_node(graph, node, &generation);
			if (result != CLUSTER_RECOVERY_GRAPH_MORE)
				return result;
			if (generation != node->record_generation
				|| !recovery_graph_record_equal(&node->record, &graph->scratch))
				return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_CHANGED);
			graph->next_verify++;
			return CLUSTER_RECOVERY_GRAPH_MORE;
		}
		if (graph->depth == 0) {
			if (graph->next_root == graph->root_count) {
				graph->verifying = true;
				continue;
			}
			if (graph->nodes[graph->next_root].state == RECOVERY_NODE_DONE) {
				graph->next_root++;
				continue;
			}
			graph->stack[graph->depth++] = graph->next_root++;
		}
		node = &graph->nodes[graph->stack[graph->depth - 1]];
		if (node->state == RECOVERY_NODE_DISCOVERED) {
			result = recovery_graph_read_node(graph, node, &generation);
			if (result != CLUSTER_RECOVERY_GRAPH_MORE)
				return result;
			node->record = graph->scratch;
			node->record_generation = generation;
			node->state = RECOVERY_NODE_IN_PROGRESS;
			out->transaction = node->transaction;
			out->record_segment_generation = generation;
			out->record = &node->record;
			return CLUSTER_RECOVERY_GRAPH_RECORD;
		}
		if (!UBA_is_invalid(node->record.header.prev_uba)) {
			UBA previous = node->record.header.prev_uba;
			uint32 current_segment, current_block, previous_segment, previous_block;
			uint16 current_slot, current_row, previous_slot, previous_row;
			RecoveryRecordNode *child;

			if (cluster_undo_record_uba_equal(previous, node->address))
				return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_CYCLE);
			/* The original allocator's same-segment order is strict. A
			 * segment rollover need not decrease its segment number. */
			if (!uba_decode_record(node->address, &current_segment, &current_block, &current_slot,
								   &current_row)
				|| !uba_decode_record(previous, &previous_segment, &previous_block, &previous_slot,
									  &previous_row)
				|| (previous_segment == current_segment
					&& (previous_block > current_block
						|| (previous_block == current_block && previous_row >= current_row))))
				return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_IDENTITY);
			if (node->predecessor == RECOVERY_GRAPH_NONE) {
				result
					= recovery_graph_node(graph, &node->transaction, previous, &node->predecessor);
				if (result != CLUSTER_RECOVERY_GRAPH_MORE)
					return result;
			}
			child = &graph->nodes[node->predecessor];
			if (child->state == RECOVERY_NODE_IN_PROGRESS)
				return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_CYCLE);
			if (child->state == RECOVERY_NODE_DISCOVERED) {
				Assert(graph->depth < graph->capacity);
				graph->stack[graph->depth++] = node->predecessor;
				continue;
			}
		}
		node->state = RECOVERY_NODE_DONE;
		graph->depth--;
	}
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

uint32
cluster_recovery_reference_graph_count(const ClusterRecoveryReferenceGraph *graph)
{
	return graph == NULL ? 0 : graph->count;
}

void
cluster_recovery_reference_graph_invalidate(ClusterRecoveryReferenceGraph *graph)
{
	if (graph != NULL && graph->status <= CLUSTER_RECOVERY_GRAPH_DRAINED)
		graph->status = CLUSTER_RECOVERY_GRAPH_INVALID;
}

void
cluster_recovery_reference_graph_destroy(ClusterRecoveryReferenceGraph *graph)
{
	if (graph != NULL) {
		free(graph->nodes);
		free(graph->buckets);
		free(graph->stack);
		free(graph);
	}
}

struct ClusterRecoveryGraphLookup {
	const ClusterRecoveryReferenceGraph *graph;
	uint64 system_identifier, database_incarnation;
	ClusterTxLocator locator;
	uint32 next, match;
	ClusterRecoveryGraphLookupResult status;
};

bool
cluster_recovery_reference_graph_observed(const ClusterRecoveryReferenceGraph *graph)
{
	return graph != NULL && graph->status == CLUSTER_RECOVERY_GRAPH_DRAINED;
}

ClusterRecoveryGraphResult
cluster_recovery_reference_graph_at(const ClusterRecoveryReferenceGraph *graph, uint32 index,
									ClusterRecoveryGraphRecord *out)
{
	const RecoveryRecordNode *node;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_reference_graph_observed(graph) || index > graph->count)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	if (index == graph->count)
		return CLUSTER_RECOVERY_GRAPH_DRAINED;
	node = &graph->nodes[index];
	if (node->state != RECOVERY_NODE_DONE)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	out->transaction = node->transaction;
	out->record_segment_generation = node->record_generation;
	out->record = &node->record;
	return CLUSTER_RECOVERY_GRAPH_RECORD;
}

ClusterRecoveryGraphResult
cluster_recovery_reference_graph_recheck_at(ClusterRecoveryReferenceGraph *graph, uint32 index)
{
	RecoveryRecordNode *node;
	ClusterRecoveryGraphResult result;
	uint32 generation;

	if (!cluster_recovery_reference_graph_observed(graph) || index > graph->count)
		return graph != NULL && graph->status > CLUSTER_RECOVERY_GRAPH_DRAINED
			? graph->status : CLUSTER_RECOVERY_GRAPH_INVALID;
	if (index == graph->count) return CLUSTER_RECOVERY_GRAPH_DRAINED;
	node = &graph->nodes[index];
	if (node->state != RECOVERY_NODE_DONE)
		return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_INVALID);
	result = recovery_graph_read_node(graph, node, &generation);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE) return result;
	if (!cluster_recovery_reference_graph_observed(graph)
		|| generation != node->record_generation
		|| !recovery_graph_record_equal(&node->record, &graph->scratch))
		return recovery_graph_fail(graph, CLUSTER_RECOVERY_GRAPH_CHANGED);
	return CLUSTER_RECOVERY_GRAPH_RECORD;
}

/* Use the edges resolved by the original walk, not a second traversal or
 * a caller-provided root list. Native UBA128 and both physical generations
 * stay in the original records; these indexes are only local adjacency. */
ClusterRecoveryGraphResult
cluster_recovery_reference_graph_dependency_at(const ClusterRecoveryReferenceGraph *graph,
	uint32 index, ClusterRecoveryGraphDependency *out)
{
	ClusterRecoveryGraphDependency current = {0};
	const RecoveryRecordNode *node, *previous;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_reference_graph_observed(graph)
		|| graph->root_count > graph->count || index > graph->count)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	current.count = graph->count;
	current.root_count = graph->root_count;
	current.index = index;
	if (index == graph->count) {
		*out = current;
		return CLUSTER_RECOVERY_GRAPH_DRAINED;
	}
	node = &graph->nodes[index];
	if (cluster_recovery_reference_graph_at(graph, index, &current.value)
			!= CLUSTER_RECOVERY_GRAPH_RECORD
		|| !cluster_undo_record_uba_equal(node->address, node->record.address))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	current.root = index < graph->root_count;
	if (UBA_is_invalid(node->record.header.prev_uba)) {
		if (node->predecessor != RECOVERY_GRAPH_NONE)
			return CLUSTER_RECOVERY_GRAPH_INVALID;
	} else {
		if (node->predecessor >= graph->count || node->predecessor == index)
			return CLUSTER_RECOVERY_GRAPH_INVALID;
		previous = &graph->nodes[node->predecessor];
		if (!recovery_graph_key_equal(&node->transaction, &previous->transaction)
			|| !cluster_undo_record_uba_equal(node->record.header.prev_uba, previous->address)
			|| !cluster_undo_record_uba_equal(previous->address, previous->record.address)
			|| cluster_recovery_reference_graph_at(graph, node->predecessor,
				&current.predecessor) != CLUSTER_RECOVERY_GRAPH_RECORD)
			return CLUSTER_RECOVERY_GRAPH_INVALID;
		current.predecessor_index = node->predecessor;
	}
	*out = current;
	return CLUSTER_RECOVERY_GRAPH_RECORD;
}

/* Join native stored heads to the original observed nodes, without adding
 * roots or doing another walk. The probe bound is the original table size. */
ClusterRecoveryGraphResult
cluster_recovery_reference_graph_dependency_find(const ClusterRecoveryReferenceGraph *graph,
	const ClusterRecoveryTxnKey *transaction, UBA address, ClusterRecoveryGraphDependency *out)
{
	uint32 bucket;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !cluster_recovery_reference_graph_observed(graph)
		|| !recovery_graph_key_valid(transaction)
		|| !recovery_graph_address_matches(transaction, address)
		|| graph->buckets == NULL || graph->bucket_count == 0
		|| (graph->bucket_count & (graph->bucket_count - 1)) != 0
		|| graph->count > graph->capacity || graph->root_count > graph->count)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	bucket = recovery_graph_bucket(transaction, address, graph->bucket_count - 1);
	for (uint32 probes = 0; probes < graph->bucket_count; probes++) {
		uint32 entry = graph->buckets[bucket];
		const RecoveryRecordNode *node;

		if (entry == 0 || entry > graph->count) return CLUSTER_RECOVERY_GRAPH_INVALID;
		node = &graph->nodes[entry - 1];
		if (recovery_graph_key_equal(transaction, &node->transaction)
			&& cluster_undo_record_uba_equal(address, node->address))
			return cluster_recovery_reference_graph_dependency_at(graph, entry - 1, out);
		bucket = (bucket + 1) & (graph->bucket_count - 1);
	}
	return CLUSTER_RECOVERY_GRAPH_INVALID;
}

ClusterRecoveryGraphLookupResult
cluster_recovery_reference_lookup_begin(const ClusterRecoveryReferenceGraph *graph,
										uint64 system_identifier, uint64 database_incarnation,
										const ClusterTxLocator *locator, size_t memory_budget,
										ClusterRecoveryGraphLookup **out)
{
	ClusterRecoveryGraphLookup *lookup;
	uint32 segment, block;
	uint16 slot, row;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_reference_graph_observed(graph) || system_identifier == 0
		|| database_incarnation == 0 || locator == NULL || !TransactionIdIsNormal(locator->xid)
		|| locator->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| locator->itl_kind == ITL_FLAG_FREE
		|| locator->itl_kind > ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI
		|| !uba_decode_record(locator->uba, &segment, &block, &slot, &row))
		return CLUSTER_RECOVERY_LOOKUP_INVALID;
	if (memory_budget < sizeof(*lookup))
		return CLUSTER_RECOVERY_LOOKUP_CAPACITY;
	lookup = calloc(1, sizeof(*lookup));
	if (lookup == NULL)
		return CLUSTER_RECOVERY_LOOKUP_CAPACITY;
	lookup->graph = graph;
	lookup->system_identifier = system_identifier;
	lookup->database_incarnation = database_incarnation;
	lookup->locator = *locator;
	lookup->match = RECOVERY_GRAPH_NONE;
	*out = lookup;
	return CLUSTER_RECOVERY_LOOKUP_MORE;
}

ClusterRecoveryGraphLookupResult
cluster_recovery_reference_lookup_next(ClusterRecoveryGraphLookup *lookup,
									   ClusterRecoveryGraphRecord *out)
{
	const ClusterRecoveryReferenceGraph *graph;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (lookup == NULL)
		return CLUSTER_RECOVERY_LOOKUP_INVALID;
	if (out == NULL)
		return lookup->status = CLUSTER_RECOVERY_LOOKUP_INVALID;
	if (lookup->status >= CLUSTER_RECOVERY_LOOKUP_INVALID)
		return lookup->status;
	graph = lookup->graph;
	if (!cluster_recovery_reference_graph_observed(graph))
		return lookup->status = CLUSTER_RECOVERY_LOOKUP_CHANGED;
	for (unsigned work = 0; lookup->next < graph->count && work < RECOVERY_GRAPH_TICK; work++) {
		uint32 index = lookup->next++;
		const RecoveryRecordNode *node = &graph->nodes[index];
		const ClusterRecoveryTxnKey *key = &node->transaction;
		ClusterTxLocator locator;
		ClusterTxResolveReason reason;

		if (key->system_identifier != lookup->system_identifier
			|| key->database_incarnation != lookup->database_incarnation
			|| key->xid != lookup->locator.xid
			|| !cluster_undo_record_uba_equal(node->address, lookup->locator.uba)
			|| (lookup->locator.tt_wrap != TT_WRAP_INVALID
				&& key->slot_wrap != lookup->locator.tt_wrap))
			continue;
		locator = lookup->locator;
		locator.tt_wrap = key->slot_wrap;
		/* Reuse the original member contract, including canonical-vs-physical
		 * segment identity. The page's own slot wrap is never the TT wrap. */
		if (node->state != RECOVERY_NODE_DONE
			|| !cluster_undo_record_validate_identity(&locator, node->address, &node->record.header,
													  &reason)
			|| lookup->match != RECOVERY_GRAPH_NONE)
			return lookup->status = CLUSTER_RECOVERY_LOOKUP_IDENTITY;
		lookup->match = index;
	}
	if (lookup->next < graph->count)
		return CLUSTER_RECOVERY_LOOKUP_MORE;
	if (lookup->match == RECOVERY_GRAPH_NONE)
		return lookup->status = CLUSTER_RECOVERY_LOOKUP_MISSING;
	out->transaction = graph->nodes[lookup->match].transaction;
	out->record_segment_generation = graph->nodes[lookup->match].record_generation;
	out->record = &graph->nodes[lookup->match].record;
	return lookup->status = CLUSTER_RECOVERY_LOOKUP_RECORD;
}

void
cluster_recovery_reference_lookup_destroy(ClusterRecoveryGraphLookup *lookup)
{
	free(lookup);
}
#endif
