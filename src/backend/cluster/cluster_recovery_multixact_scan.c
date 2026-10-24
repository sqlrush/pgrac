/*-------------------------------------------------------------------------
 * cluster_recovery_multixact_scan.c
 *   Physical MultiXact candidates from the original observed root union.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/backend/cluster/cluster_recovery_multixact_scan.c
 * NOTES
 *   Read-only physical observations do not authorize recovery completion.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/slru.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_multixact_scan.h"
#ifdef USE_PGRAC_CLUSTER

enum MxPhysicalStage { MX_SELECT, MX_READ, MX_SLOTS, MX_PADDING, MX_REREAD };

struct ClusterRecoveryMultixactScan {
	ClusterRecoveryRootSet *roots;
	uint8 root_digest[32];
	uint32 next_root, root_index, slot;
	uint64 pages, page;
	uint32 per_page;
	bool members;
	const ClusterRecoveryRootEntry *entry;
	enum MxPhysicalStage stage;
	ClusterRecoveryMultixactResult status;
	ClusterRecoveryMultixactDiag diag;
	PGAlignedBlock current, recheck;
};

static ClusterRecoveryMultixactResult
mx_physical_fail(ClusterRecoveryMultixactScan *scan, ClusterRecoveryMultixactResult status)
{
	if (scan != NULL) {
		if (scan->status > CLUSTER_RECOVERY_MX_DRAINED)
			return scan->status;
		scan->status = status;
		memset(&scan->current, 0, sizeof(scan->current));
		memset(&scan->recheck, 0, sizeof(scan->recheck));
		cluster_recovery_root_set_invalidate(scan->roots);
	}
	return status;
}

static bool
mx_physical_roots_current(const ClusterRecoveryMultixactScan *scan)
{
	uint8 digest[32];
	return scan != NULL && cluster_recovery_root_set_digest(scan->roots, digest)
		   && memcmp(scan->root_digest, digest, sizeof(digest)) == 0;
}

ClusterRecoveryMultixactResult
cluster_recovery_multixact_scan_begin(ClusterRecoveryRootSet *roots, size_t memory_budget,
									  ClusterRecoveryMultixactScan **out)
{
	ClusterRecoveryMultixactScan *scan;
	uint8 digest[32];
	ClusterRecoveryMultixactResult status = CLUSTER_RECOVERY_MX_INVALID;
	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest))
		goto fail;
	/* Include allocator bookkeeping; the root reader has its separate budget. */
	status = CLUSTER_RECOVERY_MX_CAPACITY;
	if (memory_budget < sizeof(*scan) + 1024)
		goto fail;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		goto fail;
	scan->roots = roots;
	memcpy(scan->root_digest, digest, sizeof(digest));
	*out = scan;
	return CLUSTER_RECOVERY_MX_MORE;
fail:
	cluster_recovery_root_set_invalidate(roots);
	return status;
}

static void
mx_physical_identity(const ClusterRecoveryMultixactScan *scan, ClusterRecoveryMultixactEntry *out)
{
	const ClusterRecoveryNamespaceEntry *file = &scan->entry->file;
	out->root_index = scan->root_index;
	out->file_kind = file->kind;
	out->family = file->native_family;
	out->origin_node
		= file->kind == CLUSTER_RECOVERY_FILE_ORIGIN_SLRU ? file->native_origin_node : UINT32_MAX;
	out->segment = file->native_segment;
	out->file_size = file->size;
}

static ClusterRecoveryMultixactResult
mx_physical_select(ClusterRecoveryMultixactScan *scan, ClusterRecoveryMultixactEntry *out)
{
	uint32 count = cluster_recovery_root_set_count(scan->roots);
	for (unsigned work = 0; work < 128 && scan->next_root < count; ++work) {
		const ClusterRecoveryRootEntry *entry;
		scan->root_index = scan->next_root++;
		entry = cluster_recovery_root_set_entry(scan->roots, scan->root_index);
		if (entry == NULL)
			return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_CHANGED);
		if ((entry->file.kind != CLUSTER_RECOVERY_FILE_NATIVE_SLRU
			 && entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
			|| (entry->file.native_family != CLUSTER_RECOVERY_NATIVE_FAMILY_MX_OFFSETS
				&& entry->file.native_family != CLUSTER_RECOVERY_NATIVE_FAMILY_MX_MEMBERS)) {
			scan->diag.skipped_roots++;
			continue;
		}
		if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0)
			return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_BASE_REQUIRED);
		if (entry->file.size % BLCKSZ != 0 || entry->file.size / BLCKSZ > SLRU_PAGES_PER_SEGMENT)
			return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_CORRUPT);
		if (entry->required_blocks > entry->file.size / BLCKSZ)
			return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_BASE_REQUIRED);
		scan->entry = entry;
		scan->pages = entry->file.size / BLCKSZ;
		scan->page = 0;
		scan->members = entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_MX_MEMBERS;
		scan->per_page = scan->members ? CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE
									   : CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE;
		scan->stage = MX_READ;
		scan->diag.files++;
		mx_physical_identity(scan, out);
		out->kind = CLUSTER_RECOVERY_MX_FILE;
		return CLUSTER_RECOVERY_MX_ENTRY;
	}
	if (scan->next_root == count) {
		scan->status = CLUSTER_RECOVERY_MX_DRAINED;
		return scan->status;
	}
	return CLUSTER_RECOVERY_MX_MORE;
}

static ClusterRecoveryMultixactResult
mx_physical_read(ClusterRecoveryMultixactScan *scan, char *buffer)
{
	ClusterRecoveryRootsResult result = cluster_recovery_root_set_read(
		scan->roots, scan->root_index, scan->page * BLCKSZ, buffer, BLCKSZ);
	if (result == CLUSTER_RECOVERY_ROOTS_BYTES)
		return CLUSTER_RECOVERY_MX_MORE;
	if (result == CLUSTER_RECOVERY_ROOTS_IO)
		return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_IO);
	if (result == CLUSTER_RECOVERY_ROOTS_CAPACITY)
		return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_CAPACITY);
	return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_CHANGED);
}

ClusterRecoveryMultixactResult
cluster_recovery_multixact_scan_next(ClusterRecoveryMultixactScan *scan,
									 ClusterRecoveryMultixactEntry *out)
{
	ClusterRecoveryMultixactResult result;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_INVALID);
	if (scan->status > CLUSTER_RECOVERY_MX_DRAINED)
		return scan->status;
	if (!mx_physical_roots_current(scan))
		return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_CHANGED);
	if (scan->status == CLUSTER_RECOVERY_MX_DRAINED)
		return scan->status;
	if (scan->stage == MX_READ && scan->page == scan->pages)
		scan->stage = MX_SELECT;
	if (scan->stage == MX_SELECT)
		return mx_physical_select(scan, out);
	if (scan->stage == MX_READ) {
		result = mx_physical_read(scan, scan->current.data);
		if (result != CLUSTER_RECOVERY_MX_MORE)
			return result;
		scan->slot = 0;
		scan->stage = MX_SLOTS;
		scan->diag.pages++;
		return CLUSTER_RECOVERY_MX_MORE;
	}
	if (scan->stage == MX_SLOTS && scan->slot == scan->per_page)
		scan->stage = MX_PADDING;
	if (scan->stage == MX_SLOTS) {
		mx_physical_identity(scan, out);
		out->slot = scan->slot++;
		out->page_number = (uint64)out->segment * SLRU_PAGES_PER_SEGMENT + scan->page;
		out->raw_index = out->page_number * scan->per_page + out->slot;
		out->addressable = out->raw_index <= UINT32_MAX;
		out->bytes = scan->current.data;
		out->length = BLCKSZ;
		if (scan->members) {
			uint32 group = out->slot / CLUSTER_NATIVE_MX_GROUP_MEMBERS;
			uint32 within = out->slot % CLUSTER_NATIVE_MX_GROUP_MEMBERS;
			uint32 flags, offset = group * CLUSTER_NATIVE_MX_GROUP_BYTES;
			memcpy(&flags, scan->current.data + offset, sizeof(flags));
			memcpy(&out->value,
				   scan->current.data + offset + sizeof(flags) + within * sizeof(TransactionId),
				   sizeof(TransactionId));
			out->raw_status = (flags >> (within * CLUSTER_NATIVE_MX_FLAG_BITS)) & 0xff;
			out->kind = CLUSTER_RECOVERY_MX_MEMBER;
			scan->diag.member_slots++;
		} else {
			memcpy(&out->value, scan->current.data + out->slot * sizeof(uint32), sizeof(uint32));
			out->kind = CLUSTER_RECOVERY_MX_OFFSET;
			scan->diag.offset_slots++;
		}
		if (!out->addressable)
			scan->diag.unaddressable_slots++;
		return CLUSTER_RECOVERY_MX_ENTRY;
	}
	if (scan->stage == MX_PADDING) {
		size_t used = scan->members
						  ? (BLCKSZ / CLUSTER_NATIVE_MX_GROUP_BYTES) * CLUSTER_NATIVE_MX_GROUP_BYTES
						  : BLCKSZ;
		scan->stage = MX_REREAD;
		if (used < BLCKSZ) {
			mx_physical_identity(scan, out);
			out->kind = CLUSTER_RECOVERY_MX_PADDING;
			out->page_number = (uint64)out->segment * SLRU_PAGES_PER_SEGMENT + scan->page;
			out->bytes = scan->current.data + used;
			out->length = BLCKSZ - used;
			scan->diag.padding_bytes += out->length;
			return CLUSTER_RECOVERY_MX_ENTRY;
		}
	}
	result = mx_physical_read(scan, scan->recheck.data);
	if (result != CLUSTER_RECOVERY_MX_MORE)
		return result;
	if (memcmp(scan->current.data, scan->recheck.data, BLCKSZ) != 0)
		return mx_physical_fail(scan, CLUSTER_RECOVERY_MX_CHANGED);
	scan->diag.reread_pages++;
	scan->page++;
	scan->stage = MX_READ;
	return CLUSTER_RECOVERY_MX_MORE;
}

bool
cluster_recovery_multixact_scan_diag(const ClusterRecoveryMultixactScan *scan,
									 ClusterRecoveryMultixactDiag *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || scan == NULL || scan->status > CLUSTER_RECOVERY_MX_DRAINED
		|| !mx_physical_roots_current(scan))
		return false;
	*out = scan->diag;
	return true;
}

void
cluster_recovery_multixact_scan_destroy(ClusterRecoveryMultixactScan *scan)
{
	free(scan);
}
#endif
