/*-------------------------------------------------------------------------
 * cluster_mx_history_graph.c
 *    Iterative private history graph; each complete address is read once.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_mx_history_graph.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_conf.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/backendid.h"
#include "utils/memutils.h"

typedef struct HistoryNode {
	ClusterMxHistoryRequest request;
	ClusterMxHistoryRecord record;
	ClusterMxHistoryPeer peer;
	char *bytes;
	size_t ordinal, global_prev, page_prior;
	struct HistoryNode *parent;
	uint8 phase;
	bool fetched, done;
} HistoryNode;

struct ClusterMxHistoryGraph {
	MemoryContext context;
	ClusterMxHistoryCapture capture;
	HistoryNode **table, **nodes;
	size_t count, capacity;
	HistoryNode *top;
	uint8 root;
	bool awaiting, failed, complete;
};

static uint32
history_graph_crc(const void *bytes, size_t length)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, length);
	FIN_CRC32C(crc);
	return crc;
}

static size_t
history_graph_hash(UBA address)
{
	uint64 h = UINT64CONST(14695981039346656037);
	for (unsigned word = 0; word < 2; ++word)
		for (unsigned shift = 0; shift < 64; shift += 8) {
			h ^= (address.raw[word] >> shift) & 255;
			h *= UINT64CONST(1099511628211);
		}
	return (size_t)h;
}

static HistoryNode *
history_graph_find(const ClusterMxHistoryGraph *g, UBA address)
{
	size_t i;
	if (g->capacity == 0)
		return NULL;
	i = history_graph_hash(address) & (g->capacity - 1);
	while (g->table[i] != NULL) {
		if (cluster_undo_record_uba_equal(g->table[i]->request.locator.uba, address))
			return g->table[i];
		i = (i + 1) & (g->capacity - 1);
	}
	return NULL;
}

static bool
history_graph_grow(ClusterMxHistoryGraph *g)
{
	HistoryNode **table, **nodes;
	size_t capacity;
	if (g->capacity != 0 && g->count < g->capacity / 2)
		return true;
	if (g->capacity > MaxAllocSize / (2 * sizeof(*table)))
		return false;
	capacity = g->capacity == 0 ? 16 : 2 * g->capacity;
	table = MemoryContextAllocExtended(g->context, capacity * sizeof(*table), MCXT_ALLOC_NO_OOM);
	if (table == NULL)
		return false;
	nodes = MemoryContextAllocExtended(g->context, capacity * sizeof(*nodes), MCXT_ALLOC_NO_OOM);
	if (nodes == NULL) {
		pfree(table);
		return false;
	}
	memset(table, 0, capacity * sizeof(*table));
	for (size_t n = 0; n < g->count; ++n) {
		size_t i = history_graph_hash(g->nodes[n]->request.locator.uba) & (capacity - 1);
		while (table[i] != NULL)
			i = (i + 1) & (capacity - 1);
		table[i] = nodes[n] = g->nodes[n];
	}
	if (g->table)
		pfree(g->table);
	if (g->nodes)
		pfree(g->nodes);
	g->table = table;
	g->nodes = nodes;
	g->capacity = capacity;
	return true;
}

static bool
history_graph_follow(ClusterMxHistoryGraph *g, const ClusterMxHistoryRequest *request, size_t *edge)
{
	HistoryNode *node = history_graph_find(g, request->locator.uba);
	size_t i;
	if (node != NULL) {
		ClusterMxHistoryRecord record;
		ClusterSavepointReaderIdentity identity;
		/* OPEN nodes are exactly the active DFS ancestors. Shared DONE
		 * nodes instead recheck every new edge against their final bytes. */
		if (!node->done
			|| !cluster_mx_history_record_from_bytes(request, node->bytes,
													 node->record.record_length,
													 node->record.record_offset, &record)
			|| !cluster_mx_history_record_bind_peer(request, &record, &node->peer, &identity))
			return false;
		if (edge)
			*edge = node->ordinal;
		return true;
	}
	if (!history_graph_grow(g))
		return false;
	node = MemoryContextAllocExtended(g->context, sizeof(*node), MCXT_ALLOC_NO_OOM);
	if (node == NULL)
		return false;
	memset(node, 0, sizeof(*node));
	node->request = *request;
	node->ordinal = g->count;
	node->global_prev = node->page_prior = CMXH_NO_NODE;
	node->parent = g->top;
	i = history_graph_hash(request->locator.uba) & (g->capacity - 1);
	while (g->table[i] != NULL)
		i = (i + 1) & (g->capacity - 1);
	g->table[i] = g->nodes[g->count++] = node;
	if (edge)
		*edge = node->ordinal;
	g->top = node;
	return true;
}

static ClusterMxHistoryRequest
history_graph_base(const ClusterMxHistoryGraph *g, uint8 root)
{
	const ClusterMxHistoryCapture *c = &g->capture;
	ClusterMxHistoryRequest r = { 0 };
	r.collector_id = c->collector_id;
	r.capture_generation = c->capture_generation;
	r.backend_incarnation = c->backend_incarnation;
	r.feature_bit = c->admission.feature_bit;
	r.admission_generation = c->admission.record_generation;
	r.read_scn = c->read_scn;
	r.read_epoch = c->read_epoch;
	r.retained_floor = c->retained_floor;
	r.snapshot_identity = c->snapshot_identity;
	r.descriptor_hash = c->descriptor_hash;
	r.multixact_id = c->mxkey.multixact_id;
	r.member_count = c->nmembers;
	r.target_locator = BufTagGetRelFileLocator(&c->pcm.tag);
	r.target_fork = c->pcm.tag.forkNum;
	r.target_block = c->pcm.tag.blockNum;
	r.target_offset = ItemPointerGetOffsetNumber(&c->tid);
	r.requester_owner = c->requester_owner;
	r.root_ordinal = root;
	r.root = c->roots[root];
	return r;
}

static void
history_graph_head(ClusterMxHistoryRequest *r, const ClusterItlSlotData *head, uint8 slot)
{
	r->edge = *head;
	r->edge_itl_slot = slot;
	r->locator = (ClusterTxLocator){ .uba = head->undo_segment_head,
									 .xid = head->xid,
									 .tt_wrap = TT_WRAP_INVALID,
									 .itl_kind = head->flags,
									 .itl_slot_index = slot };
}

ClusterMxHistoryGraph *
cluster_mx_history_graph_create(const ClusterMxHistoryCapture *c, MemoryContext context)
{
	ClusterMxHistoryGraph *g;
	if (c == NULL || context == NULL || c->collector_id == 0 || c->collector_id == UINT64_MAX
		|| c->capture_generation == 0 || c->capture_generation == UINT64_MAX
		|| c->snapshot_identity == 0 || c->backend_incarnation == 0 || !SCN_VALID(c->read_scn)
		|| !SCN_VALID(c->retained_floor) || scn_local(c->retained_floor) > scn_local(c->read_scn)
		|| c->read_epoch == 0 || c->read_epoch > UINT32_MAX
		|| c->read_epoch != c->admission.formation_epoch || !c->admission.entered
		|| c->admission.feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| c->admission.side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| cluster_multixact_current_validate_descriptor(&c->mxkey, c->mxkey.origin_node_id,
														 c->read_epoch, c->members, c->nmembers,
														 c->nmembers)
			   != CMX_DESC_OK
		|| cluster_multixact_current_descriptor_hash(&c->mxkey, c->members, c->nmembers)
			   != c->descriptor_hash)
		return NULL;
	g = MemoryContextAllocExtended(context, sizeof(*g), MCXT_ALLOC_NO_OOM);
	if (g == NULL)
		return NULL;
	memset(g, 0, sizeof(*g));
	g->capture = *c;
	g->context = context;
	return g;
}

ClusterMxHistoryStep
cluster_mx_history_graph_next(ClusterMxHistoryGraph *g, ClusterMxHistoryRequest *request,
							  bool *needs_read)
{
	HistoryNode *node;
	ClusterMxHistoryRequest r;
	if (request)
		memset(request, 0, sizeof(*request));
	if (needs_read)
		*needs_read = false;
	if (g == NULL)
		return CMXH_UNKNOWN;
	if (request == NULL || needs_read == NULL)
		g->failed = true;
	if (g->failed)
		return CMXH_UNKNOWN;
	if (g->complete)
		return CMXH_COMPLETE;
	node = g->top;
	if (node == NULL) {
		const ClusterItlSlotData *root;
		if (g->root == CLUSTER_ITL_INITRANS_DEFAULT) {
			g->complete = true;
			return CMXH_COMPLETE;
		}
		root = &g->capture.roots[g->root];
		if (!cluster_undo_history_prior_valid(root))
			goto failed;
		if (root->flags == ITL_FLAG_FREE) {
			g->root++;
			return CMXH_PENDING;
		}
		r = history_graph_base(g, g->root++);
		r.edge_kind = CMXH_ROOT;
		history_graph_head(&r, root, r.root_ordinal);
		if (!history_graph_follow(g, &r, NULL))
			goto failed;
		return CMXH_PENDING;
	}
	if (!node->fetched) {
		g->awaiting = true;
		*request = node->request;
		*needs_read = true;
		return CMXH_PENDING;
	}
	r = history_graph_base(g, node->request.root_ordinal);
	r.parent = node->request.locator.uba;
	r.parent_data_generation = node->peer.data_generation;
	r.parent_record_crc = history_graph_crc(node->bytes, node->record.record_length);
	if (node->phase == 0) {
		node->phase++;
		if (UBA_is_invalid(node->record.header.prev_uba))
			return CMXH_PENDING;
		r.edge_kind = CMXH_GLOBAL_PREV;
		r.edge_itl_slot = 255;
		r.locator = (ClusterTxLocator){ .uba = node->record.header.prev_uba,
										.xid = node->peer.canonical.xid,
										.tt_wrap = node->peer.canonical.wrap,
										.itl_slot_index = 255 };
		r.flags = CLUSTER_MX_HISTORY_EXPECT_OWNER;
		r.expected_owner = node->peer.canonical;
		r.expected_owner_generation = node->peer.canonical_generation;
		r.expected_top_fxid = node->peer.top_fxid;
		if (!history_graph_follow(g, &r, &node->global_prev))
			goto failed;
		return CMXH_PENDING;
	}
	if (node->phase == 1) {
		const UndoItlHistoryEntry *prior = NULL;
		node->phase++;
		if (node->record.record_class == CMXH_ROLLBACK_CONTROL
			|| !RelFileLocatorEquals(node->record.header.target_locator, r.target_locator)
			|| node->record.header.target_fork != r.target_fork)
			return CMXH_PENDING;
		for (unsigned i = 0; i < node->record.payload.history.count; ++i) {
			const UndoItlHistoryEntry *entry = &node->record.payload.history.entries[i];
			if (entry->block != r.target_block)
				continue;
			if (prior != NULL)
				goto failed;
			prior = entry;
		}
		if (prior == NULL || prior->prior.flags == ITL_FLAG_FREE)
			return CMXH_PENDING;
		r.edge_kind = CMXH_PAGE_PRIOR;
		history_graph_head(&r, &prior->prior, prior->slot_index);
		if (!history_graph_follow(g, &r, &node->page_prior))
			goto failed;
		return CMXH_PENDING;
	}
	node->done = true;
	g->top = node->parent;
	return CMXH_PENDING;
failed:
	g->failed = true;
	return CMXH_UNKNOWN;
}

bool
cluster_mx_history_graph_accept(ClusterMxHistoryGraph *g, const ClusterMxHistoryPeerResult *result)
{
	HistoryNode *node;
	ClusterMxHistoryRequest r, expected;
	ClusterMxHistoryRequestWire wire;
	ClusterMxHistoryPeer peer;
	ClusterMxHistoryRecord record;
	ClusterSavepointReaderIdentity identity;
	PGAlignedBlock bytes;
	UndoBlockHeader header;
	static const uint8 zero[6];
	if (g == NULL)
		return false;
	node = g->top;
	if (g->failed || g->complete || !g->awaiting || node == NULL || node->fetched || result == NULL
		|| result->admitted != 1 || result->consumed != 0
		|| memcmp(result->zero, zero, sizeof(zero)) != 0
		|| !cluster_mx_history_request_decode(&result->request, 424, cluster_node_id,
											  uba_origin_node_id(node->request.locator.uba),
											  g->capture.read_epoch, &r)
		|| r.forward.requester_backend_id != MyBackendId)
		goto failed;
	expected = node->request;
	expected.forward = r.forward;
	expected.requester_capability_generation = r.requester_capability_generation;
	expected.origin_capability_generation = r.origin_capability_generation;
	if (!cluster_mx_history_request_encode(&expected, &wire)
		|| !cluster_mx_history_request_same(&wire, &result->request, false)
		|| !cluster_mx_history_peer_decode(&result->peer, &peer)
		|| peer.formation_epoch != r.read_epoch
		|| peer.admission_generation != r.admission_generation
		|| peer.data_crc != history_graph_crc(result->data, BLCKSZ)
		|| ClusterR4WireReadU64(result->auth) != peer.canonical_generation
		|| !SCN_VALID(ClusterR4WireReadU64(result->auth + 8))
		|| scn_local(ClusterR4WireReadU64(result->auth + 8)) < scn_local(r.read_scn)
		|| !cluster_mx_history_record_capture(&r, (const char *)result->data, bytes.data, &record)
		|| !cluster_mx_history_record_bind_peer(&r, &record, &peer, &identity))
		goto failed;
	memcpy(&header, result->data, sizeof(header));
	if (header.block_lsn != peer.data_lsn)
		goto failed;
	node->bytes = MemoryContextAllocExtended(g->context, record.record_length, MCXT_ALLOC_NO_OOM);
	if (node->bytes == NULL)
		goto failed;
	memcpy(node->bytes, bytes.data, record.record_length);
	/* Keep the actual routed/landed echo for peer-only member assembly. */
	node->request = r;
	node->record = record;
	node->record.identity = identity;
	node->record.canonical_generation_known = true;
	node->peer = peer;
	node->fetched = true;
	g->awaiting = false;
	return true;
failed:
	g->failed = true;
	return false;
}

bool
cluster_mx_history_graph_view(const ClusterMxHistoryGraph *g, size_t ordinal,
							  ClusterMxHistoryNodeView *out)
{
	const HistoryNode *node;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (g == NULL || g->failed || !g->complete || ordinal >= g->count)
		return false;
	node = g->nodes[ordinal];
	if (!node->done)
		return false;
	*out = (ClusterMxHistoryNodeView){ .request = &node->request,
									   .record = &node->record,
									   .peer = &node->peer,
									   .bytes = node->bytes,
									   .global_prev = node->global_prev,
									   .page_prior = node->page_prior };
	return true;
}

size_t
cluster_mx_history_graph_count(const ClusterMxHistoryGraph *g)
{
	return g != NULL && !g->failed && g->complete ? g->count : 0;
}

void
cluster_mx_history_graph_destroy(ClusterMxHistoryGraph *g)
{
	if (g == NULL)
		return;
	for (size_t i = 0; i < g->count; ++i) {
		if (g->nodes[i]->bytes)
			pfree(g->nodes[i]->bytes);
		pfree(g->nodes[i]);
	}
	if (g->nodes)
		pfree(g->nodes);
	if (g->table)
		pfree(g->table);
	pfree(g);
}
#endif
