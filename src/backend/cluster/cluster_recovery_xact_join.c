/*-------------------------------------------------------------------------
 * cluster_recovery_xact_join.c
 *   Retained transaction effects and original-origin physical SLRU slots.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/clog.h"
#include "access/slru.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_xact_join.h"

#define XACT_JOIN_ROOT_TICK 128

typedef enum XactJoinStage {
	XACT_JOIN_FIND,
	XACT_JOIN_READ,
	XACT_JOIN_EMIT,
	XACT_JOIN_REREAD,
	XACT_JOIN_DONE
} XactJoinStage;

struct ClusterRecoveryXactJoin {
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryNativeScan *scan;
	ClusterRecoveryNativeEffect primary;
	uint8 root_digest[32];
	uint64 position, count;
	uint32 node, per_page, page_number, root_cursor, root_index, expected;
	bool parent;
	XactJoinStage stage;
	ClusterRecoveryXactJoinResult status;
	PGAlignedBlock page, reread;
};

static ClusterRecoveryXactJoinResult
xact_join_fail(ClusterRecoveryXactJoin *join, ClusterRecoveryXactJoinResult result)
{
	join->status = result;
	memset(&join->page, 0, sizeof(join->page));
	memset(&join->reread, 0, sizeof(join->reread));
	cluster_recovery_root_set_invalidate(join->roots);
	return result;
}

static bool
xact_join_current_xid(const ClusterRecoveryXactJoin *join, TransactionId *xid)
{
	const ClusterRecoveryNativeRecord *record = join->primary.record;
	uint64 ordinal;
	if (join->position >= join->count)
		return false;
	if (!join->parent && join->position == 0)
		*xid = record->top_xid;
	else {
		ordinal = join->position - !join->parent;
		/* Complete array bounds were checked before retaining this record. */
		memcpy(xid, record->payload + record->subxacts_offset + ordinal * sizeof(*xid),
			   sizeof(*xid));
	}
	return TransactionIdIsNormal(*xid);
}

static ClusterRecoveryXactJoinResult
xact_join_page_begin(ClusterRecoveryXactJoin *join)
{
	TransactionId xid;
	if (join->position == join->count) {
		join->stage = XACT_JOIN_DONE;
		return CLUSTER_RECOVERY_XACT_JOIN_DRAINED;
	}
	if (!xact_join_current_xid(join, &xid))
		return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_INVALID);
	join->page_number = xid / join->per_page;
	join->root_cursor = 0;
	join->root_index = UINT32_MAX;
	join->stage = XACT_JOIN_FIND;
	return CLUSTER_RECOVERY_XACT_JOIN_MORE;
}

ClusterRecoveryXactJoinResult
cluster_recovery_xact_join_begin(ClusterRecoveryRootSet *roots, ClusterRecoveryNativeScan *scan,
								 size_t memory_budget, ClusterRecoveryXactJoin **out)
{
	ClusterRecoveryNativeEffect primary;
	const ClusterRecoveryNativeRecord *record;
	ClusterRecoveryXactJoin *join;
	ClusterRecoveryXactJoinResult result = CLUSTER_RECOVERY_XACT_JOIN_INVALID;
	uint8 digest[32], opcode;
	bool parent;
	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest)
		|| cluster_recovery_native_scan_primary(scan, &primary) != CLUSTER_RECOVERY_NATIVE_EFFECT
		|| (primary.kind != CLUSTER_RECOVERY_NATIVE_TERMINAL
			&& primary.kind != CLUSTER_RECOVERY_NATIVE_ASSIGNMENT)
		|| primary.record == NULL)
		goto fail;
	record = primary.record;
	parent = primary.kind == CLUSTER_RECOVERY_NATIVE_ASSIGNMENT;
	opcode = record->header.xl_info & XLOG_XACT_OPMASK;
	if (record->header.xl_rmid != RM_XACT_ID
		|| (parent ? opcode != XLOG_XACT_ASSIGNMENT
				   : opcode != XLOG_XACT_COMMIT && opcode != XLOG_XACT_ABORT)
		|| !TransactionIdIsNormal(record->top_xid)
		|| (!parent && record->top_xid != record->header.xl_xid) || primary.bytes != record->payload
		|| primary.length != record->payload_length || record->payload == NULL
		|| record->subxacts_offset > record->payload_length
		|| record->nsubxacts
			   > (record->payload_length - record->subxacts_offset) / sizeof(TransactionId)
		|| record->source.claim.identity.origin_node_id < 0
		|| record->source.claim.identity.origin_node_id >= 128
		|| !cluster_recovery_root_set_storage_matches(
			roots, record->source.claim.identity.system_identifier,
			record->source.claim.database_incarnation, record->source.claim.identity.storage_uuid))
		goto fail;
	if (memory_budget < sizeof(*join) + 1024) {
		result = CLUSTER_RECOVERY_XACT_JOIN_CAPACITY;
		goto fail;
	}
	join = calloc(1, sizeof(*join));
	if (join == NULL) {
		result = CLUSTER_RECOVERY_XACT_JOIN_CAPACITY;
		goto fail;
	}
	join->roots = roots;
	join->scan = scan;
	join->primary = primary;
	memcpy(join->root_digest, digest, sizeof(digest));
	join->parent = parent;
	join->node = record->source.claim.identity.origin_node_id;
	join->count = (uint64)record->nsubxacts + !parent;
	join->per_page = parent ? CLUSTER_NATIVE_SUBTRANS_PER_PAGE : CLUSTER_NATIVE_CLOG_PER_PAGE;
	join->expected = parent						  ? record->top_xid
					 : opcode == XLOG_XACT_COMMIT ? TRANSACTION_STATUS_COMMITTED
												  : TRANSACTION_STATUS_ABORTED;
	result = xact_join_page_begin(join);
	if (result > CLUSTER_RECOVERY_XACT_JOIN_DRAINED) {
		free(join);
		goto fail;
	}
	*out = join;
	return CLUSTER_RECOVERY_XACT_JOIN_MORE;
fail:
	cluster_recovery_root_set_invalidate(roots);
	return result;
}

static ClusterRecoveryXactJoinResult
xact_join_find(ClusterRecoveryXactJoin *join)
{
	uint8 family = join->parent ? CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS
								: CLUSTER_RECOVERY_NATIVE_FAMILY_XACT;
	uint32 count = cluster_recovery_root_set_count(join->roots);
	unsigned visited = 0;
	while (join->root_cursor < count && visited++ < XACT_JOIN_ROOT_TICK) {
		uint32 index = join->root_cursor++;
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(join->roots, index);
		if (entry == NULL)
			return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
		if (entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU
			|| entry->file.native_origin_node != join->node || entry->file.native_family != family
			|| entry->file.native_segment != join->page_number / SLRU_PAGES_PER_SEGMENT)
			continue;
		if (join->root_index != UINT32_MAX)
			return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_INVALID);
		join->root_index = index;
		if (!(entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) || entry->file.size % BLCKSZ != 0
			|| entry->file.size / BLCKSZ > SLRU_PAGES_PER_SEGMENT
			|| entry->file.size / BLCKSZ <= join->page_number % SLRU_PAGES_PER_SEGMENT
			|| entry->required_blocks > entry->file.size / BLCKSZ)
			return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_BASE_REQUIRED);
	}
	if (join->root_cursor == count) {
		if (join->root_index == UINT32_MAX)
			return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_BASE_REQUIRED);
		join->stage = XACT_JOIN_READ;
	}
	return CLUSTER_RECOVERY_XACT_JOIN_MORE;
}

ClusterRecoveryXactJoinResult
cluster_recovery_xact_join_next(ClusterRecoveryXactJoin *join, ClusterRecoveryXactJoinEntry *out)
{
	ClusterRecoveryNativeEffect current;
	uint8 digest[32];
	TransactionId xid, next;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (join == NULL)
		return CLUSTER_RECOVERY_XACT_JOIN_INVALID;
	if (join->status > CLUSTER_RECOVERY_XACT_JOIN_DRAINED)
		return join->status;
	if (out == NULL)
		return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_INVALID);
	if (!cluster_recovery_root_set_digest(join->roots, digest)
		|| memcmp(join->root_digest, digest, sizeof(digest)) != 0
		|| cluster_recovery_native_scan_primary(join->scan, &current)
			   != CLUSTER_RECOVERY_NATIVE_EFFECT
		|| current.record != join->primary.record || current.bytes != join->primary.bytes
		|| current.length != join->primary.length || current.kind != join->primary.kind)
		return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
	if (join->stage == XACT_JOIN_DONE)
		return CLUSTER_RECOVERY_XACT_JOIN_DRAINED;
	if (join->stage == XACT_JOIN_FIND)
		return xact_join_find(join);
	if (join->stage == XACT_JOIN_READ || join->stage == XACT_JOIN_REREAD) {
		bool reread = join->stage == XACT_JOIN_REREAD;
		ClusterRecoveryRootsResult read = cluster_recovery_root_set_read(
			join->roots, join->root_index,
			(uint64)(join->page_number % SLRU_PAGES_PER_SEGMENT) * BLCKSZ,
			reread ? join->reread.data : join->page.data, BLCKSZ);
		if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
			return xact_join_fail(join, read == CLUSTER_RECOVERY_ROOTS_IO
											? CLUSTER_RECOVERY_XACT_JOIN_IO
											: CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
		if (!reread) {
			join->stage = XACT_JOIN_EMIT;
			return CLUSTER_RECOVERY_XACT_JOIN_MORE;
		}
		if (memcmp(join->page.data, join->reread.data, BLCKSZ) != 0)
			return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_CHANGED);
		return xact_join_page_begin(join);
	}
	if (!xact_join_current_xid(join, &xid))
		return xact_join_fail(join, CLUSTER_RECOVERY_XACT_JOIN_INVALID);
	out->record = join->primary.record;
	out->xid = xid;
	out->is_parent = join->parent;
	out->is_top = !join->parent && join->position == 0;
	out->ordinal = out->is_top ? 0 : join->position - !join->parent;
	out->root_index = join->root_index;
	out->page_number = join->page_number;
	out->expected_value = join->expected;
	if (join->parent)
		memcpy(&out->observed_value,
			   join->page.data + (xid % join->per_page) * sizeof(TransactionId),
			   sizeof(TransactionId));
	else {
		uint32 slot = xid % join->per_page;
		out->observed_value = ((uint8)join->page.data[slot / 4] >> ((slot % 4) * 2)) & 3;
	}
	out->equal = out->expected_value == out->observed_value;
	join->position++;
	if (!xact_join_current_xid(join, &next) || next / join->per_page != join->page_number)
		join->stage = XACT_JOIN_REREAD;
	return CLUSTER_RECOVERY_XACT_JOIN_ENTRY;
}

void
cluster_recovery_xact_join_destroy(ClusterRecoveryXactJoin *join)
{
	free(join);
}
#endif
