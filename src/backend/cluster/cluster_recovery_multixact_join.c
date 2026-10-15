/*-------------------------------------------------------------------------
 * cluster_recovery_multixact_join.c
 *   Retained MultiXact CREATE bytes and physical origin candidates.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/multixact.h"
#include "access/slru.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_multixact_join.h"

#define MX_JOIN_ROOT_TICK 128

typedef enum MxJoinStage {
	MX_JOIN_FIND,
	MX_JOIN_READ,
	MX_JOIN_EMIT,
	MX_JOIN_REREAD,
	MX_JOIN_DONE
} MxJoinStage;

struct ClusterRecoveryMultixactJoin {
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryNativeScan *create;
	ClusterRecoveryNativeEffect primary;
	uint8 root_digest[32];
	uint32 mid, start, count, ordinal, node;
	uint32 page_number, root_cursor, root_index;
	bool offset_phase;
	MxJoinStage stage;
	ClusterRecoveryMultixactJoinResult status;
	PGAlignedBlock page, reread;
};

static ClusterRecoveryMultixactJoinResult
mx_join_fail(ClusterRecoveryMultixactJoin *join, ClusterRecoveryMultixactJoinResult result)
{
	join->status = result;
	memset(&join->page, 0, sizeof(join->page));
	memset(&join->reread, 0, sizeof(join->reread));
	cluster_recovery_root_set_invalidate(join->roots);
	return result;
}

static void
mx_join_page_begin(ClusterRecoveryMultixactJoin *join)
{
	uint32 address = join->offset_phase ? join->mid : (uint32)(join->start + join->ordinal);
	uint32 per = join->offset_phase ? CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE
									: CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE;
	join->page_number = address / per;
	join->root_cursor = 0;
	join->root_index = UINT32_MAX;
	join->stage = MX_JOIN_FIND;
}

ClusterRecoveryMultixactJoinResult
cluster_recovery_multixact_join_begin(ClusterRecoveryRootSet *roots,
									  ClusterRecoveryNativeScan *create, size_t memory_budget,
									  ClusterRecoveryMultixactJoin **out)
{
	ClusterRecoveryMultixactJoin *join;
	ClusterRecoveryNativeEffect primary;
	xl_multixact_create header;
	uint8 digest[32];
	ClusterRecoveryMultixactJoinResult result = CLUSTER_RECOVERY_MX_JOIN_INVALID;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest)
		|| cluster_recovery_native_scan_primary(create, &primary) != CLUSTER_RECOVERY_NATIVE_EFFECT
		|| primary.kind != CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE || primary.record == NULL
		|| primary.bytes == NULL || primary.length < SizeOfMultiXactCreate)
		goto fail;
	if (!cluster_recovery_root_set_storage_matches(
			roots, primary.record->source.claim.identity.system_identifier,
			primary.record->source.claim.database_incarnation,
			primary.record->source.claim.identity.storage_uuid))
		goto fail;
	memcpy(&header, primary.bytes, SizeOfMultiXactCreate);
	/* The original scan already validates the complete native shape. Keep
	 * these bounds at the borrowed payload boundary too. */
	if (!MultiXactIdIsValid(header.mid) || header.moff == 0 || header.nmembers <= 0
		|| (size_t)header.nmembers
			   > (primary.length - SizeOfMultiXactCreate) / sizeof(MultiXactMember)
		|| primary.length
			   != SizeOfMultiXactCreate + (size_t)header.nmembers * sizeof(MultiXactMember)
		|| primary.record->source.claim.identity.origin_node_id < 0
		|| primary.record->source.claim.identity.origin_node_id >= 128)
		goto fail;
	if (memory_budget < sizeof(*join) + 1024) {
		result = CLUSTER_RECOVERY_MX_JOIN_CAPACITY;
		goto fail;
	}
	join = calloc(1, sizeof(*join));
	if (join == NULL) {
		result = CLUSTER_RECOVERY_MX_JOIN_CAPACITY;
		goto fail;
	}
	join->roots = roots;
	join->create = create;
	join->primary = primary;
	memcpy(join->root_digest, digest, 32);
	join->mid = header.mid;
	join->start = header.moff;
	join->count = header.nmembers;
	join->node = primary.record->source.claim.identity.origin_node_id;
	join->offset_phase = true;
	mx_join_page_begin(join);
	*out = join;
	return CLUSTER_RECOVERY_MX_JOIN_MORE;
fail:
	cluster_recovery_root_set_invalidate(roots);
	return result;
}

static ClusterRecoveryMultixactJoinResult
mx_join_find(ClusterRecoveryMultixactJoin *join)
{
	uint8 family = join->offset_phase ? CLUSTER_RECOVERY_NATIVE_FAMILY_MX_OFFSETS
									  : CLUSTER_RECOVERY_NATIVE_FAMILY_MX_MEMBERS;
	uint32 count = cluster_recovery_root_set_count(join->roots);
	unsigned visited = 0;
	while (join->root_cursor < count && visited++ < MX_JOIN_ROOT_TICK) {
		uint32 index = join->root_cursor++;
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(join->roots, index);
		if (entry == NULL)
			return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
		if (entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU
			|| entry->file.native_origin_node != join->node || entry->file.native_family != family
			|| entry->file.native_segment != join->page_number / SLRU_PAGES_PER_SEGMENT)
			continue;
		if (join->root_index != UINT32_MAX)
			return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_INVALID);
		join->root_index = index;
		if (!(entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) || entry->file.size % BLCKSZ != 0
			|| entry->file.size / BLCKSZ > SLRU_PAGES_PER_SEGMENT
			|| entry->file.size / BLCKSZ <= join->page_number % SLRU_PAGES_PER_SEGMENT
			|| entry->required_blocks > entry->file.size / BLCKSZ)
			return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_BASE_REQUIRED);
	}
	if (join->root_cursor == count) {
		if (join->root_index == UINT32_MAX)
			return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_BASE_REQUIRED);
		join->stage = MX_JOIN_READ;
	}
	return CLUSTER_RECOVERY_MX_JOIN_MORE;
}

ClusterRecoveryMultixactJoinResult
cluster_recovery_multixact_join_next(ClusterRecoveryMultixactJoin *join,
									 ClusterRecoveryMultixactJoinEntry *out)
{
	ClusterRecoveryNativeEffect current;
	uint8 digest[32];
	ClusterRecoveryRootsResult read;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (join == NULL)
		return CLUSTER_RECOVERY_MX_JOIN_INVALID;
	if (join->status > CLUSTER_RECOVERY_MX_JOIN_DRAINED)
		return join->status;
	if (out == NULL)
		return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_INVALID);
	if (!cluster_recovery_root_set_digest(join->roots, digest)
		|| memcmp(join->root_digest, digest, 32) != 0
		|| cluster_recovery_native_scan_primary(join->create, &current)
			   != CLUSTER_RECOVERY_NATIVE_EFFECT
		|| current.record != join->primary.record || current.bytes != join->primary.bytes
		|| current.length != join->primary.length || current.kind != join->primary.kind)
		return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
	if (join->stage == MX_JOIN_DONE)
		return CLUSTER_RECOVERY_MX_JOIN_DRAINED;
	if (join->stage == MX_JOIN_FIND)
		return mx_join_find(join);
	if (join->stage == MX_JOIN_READ || join->stage == MX_JOIN_REREAD) {
		bool reread = join->stage == MX_JOIN_REREAD;
		read = cluster_recovery_root_set_read(join->roots, join->root_index,
											  (uint64)(join->page_number % SLRU_PAGES_PER_SEGMENT)
												  * BLCKSZ,
											  reread ? join->reread.data : join->page.data, BLCKSZ);
		if (read != CLUSTER_RECOVERY_ROOTS_BYTES)
			return mx_join_fail(join, read == CLUSTER_RECOVERY_ROOTS_IO
										  ? CLUSTER_RECOVERY_MX_JOIN_IO
										  : CLUSTER_RECOVERY_MX_JOIN_CHANGED);
		if (!reread) {
			join->stage = MX_JOIN_EMIT;
			return CLUSTER_RECOVERY_MX_JOIN_MORE;
		}
		if (memcmp(join->page.data, join->reread.data, BLCKSZ) != 0)
			return mx_join_fail(join, CLUSTER_RECOVERY_MX_JOIN_CHANGED);
		if (!join->offset_phase && join->ordinal == join->count) {
			join->stage = MX_JOIN_DONE;
			return CLUSTER_RECOVERY_MX_JOIN_DRAINED;
		}
		join->offset_phase = false;
		mx_join_page_begin(join);
		return CLUSTER_RECOVERY_MX_JOIN_MORE;
	}
	out->record = join->primary.record;
	out->mid = join->mid;
	out->root_index = join->root_index;
	out->page_number = join->page_number;
	out->is_offset = join->offset_phase;
	if (join->offset_phase) {
		out->address = join->mid;
		out->expected_value = join->start;
		memcpy(&out->observed_value,
			   join->page.data + (join->mid % CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE) * sizeof(uint32),
			   sizeof(uint32));
		join->stage = MX_JOIN_REREAD;
	} else {
		MultiXactMember expected;
		uint32 flags, slot, group;
		out->ordinal = join->ordinal;
		out->address = (uint32)(join->start + join->ordinal);
		slot = out->address % CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE;
		group = slot / CLUSTER_NATIVE_MX_GROUP_MEMBERS;
		memcpy(&expected,
			   (const char *)join->primary.bytes + SizeOfMultiXactCreate
				   + (size_t)join->ordinal * sizeof(expected),
			   sizeof(expected));
		memcpy(&flags, join->page.data + group * CLUSTER_NATIVE_MX_GROUP_BYTES, sizeof(flags));
		memcpy(&out->observed_value,
			   join->page.data + group * CLUSTER_NATIVE_MX_GROUP_BYTES + sizeof(uint32)
				   + (slot % CLUSTER_NATIVE_MX_GROUP_MEMBERS) * sizeof(TransactionId),
			   sizeof(TransactionId));
		out->expected_value = expected.xid;
		out->expected_status = expected.status;
		out->observed_status
			= (flags >> ((slot % CLUSTER_NATIVE_MX_GROUP_MEMBERS) * CLUSTER_NATIVE_MX_FLAG_BITS))
			  & 255;
		join->ordinal++;
		if (join->ordinal == join->count
			|| (uint32)(join->start + join->ordinal) / CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE
				   != join->page_number)
			join->stage = MX_JOIN_REREAD;
	}
	out->equal = out->expected_value == out->observed_value
				 && out->expected_status == out->observed_status;
	return CLUSTER_RECOVERY_MX_JOIN_ENTRY;
}

void
cluster_recovery_multixact_join_destroy(ClusterRecoveryMultixactJoin *join)
{
	free(join);
}
#endif
