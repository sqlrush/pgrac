/*-------------------------------------------------------------------------
 * cluster_recovery_slru_scan.c
 *   Raw native transaction slots from the original observed root union.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/backend/cluster/cluster_recovery_slru_scan.c
 * NOTES
 *   Read-only physical observations do not authorize recovery completion.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/slru.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_slru_scan.h"
#ifdef USE_PGRAC_CLUSTER

/* Geometry is also asserted by each original native producer. */
StaticAssertDecl(sizeof(TransactionId) == sizeof(uint32), "native parent slot");
StaticAssertDecl(sizeof(int64) + sizeof(RepOriginId) == 10, "packed commit timestamp slot");

enum SlruPhysicalStage { SLRU_SELECT, SLRU_READ, SLRU_SLOTS, SLRU_PADDING, SLRU_REREAD };

struct ClusterRecoverySlruScan {
	ClusterRecoveryRootSet *roots;
	uint8 root_digest[32];
	uint32 next_root, root_index, slot;
	uint64 pages, page;
	uint32 per_page;
	size_t used;
	const ClusterRecoveryRootEntry *entry;
	enum SlruPhysicalStage stage;
	ClusterRecoverySlruResult status;
	ClusterRecoverySlruDiag diag;
	PGAlignedBlock current, recheck;
};

static ClusterRecoverySlruResult
slru_physical_fail(ClusterRecoverySlruScan *scan, ClusterRecoverySlruResult status)
{
	if (scan != NULL) {
		if (scan->status > CLUSTER_RECOVERY_SLRU_DRAINED)
			return scan->status;
		scan->status = status;
		memset(&scan->current, 0, sizeof(scan->current));
		memset(&scan->recheck, 0, sizeof(scan->recheck));
		cluster_recovery_root_set_invalidate(scan->roots);
	}
	return status;
}

static bool
slru_physical_roots_current(const ClusterRecoverySlruScan *scan)
{
	uint8 digest[32];
	return scan != NULL && cluster_recovery_root_set_digest(scan->roots, digest)
		   && memcmp(scan->root_digest, digest, sizeof(digest)) == 0;
}

ClusterRecoverySlruResult
cluster_recovery_slru_scan_begin(ClusterRecoveryRootSet *roots, size_t memory_budget,
								 ClusterRecoverySlruScan **out)
{
	ClusterRecoverySlruScan *scan;
	uint8 digest[32];
	ClusterRecoverySlruResult status = CLUSTER_RECOVERY_SLRU_INVALID;
	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest))
		goto fail;
	/* Include allocator bookkeeping; the root reader has its separate budget. */
	status = CLUSTER_RECOVERY_SLRU_CAPACITY;
	if (memory_budget < sizeof(*scan) + 1024)
		goto fail;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		goto fail;
	scan->roots = roots;
	memcpy(scan->root_digest, digest, sizeof(digest));
	*out = scan;
	return CLUSTER_RECOVERY_SLRU_MORE;
fail:
	cluster_recovery_root_set_invalidate(roots);
	return status;
}

static void
slru_physical_identity(const ClusterRecoverySlruScan *scan, ClusterRecoverySlruEntry *out)
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

static ClusterRecoverySlruResult
slru_physical_select(ClusterRecoverySlruScan *scan, ClusterRecoverySlruEntry *out)
{
	uint32 count = cluster_recovery_root_set_count(scan->roots);
	for (unsigned work = 0; work < 128 && scan->next_root < count; ++work) {
		const ClusterRecoveryRootEntry *entry;
		scan->root_index = scan->next_root++;
		entry = cluster_recovery_root_set_entry(scan->roots, scan->root_index);
		if (entry == NULL)
			return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_CHANGED);
		if ((entry->file.kind != CLUSTER_RECOVERY_FILE_NATIVE_SLRU
			 && entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
			|| (entry->file.native_family != CLUSTER_RECOVERY_NATIVE_FAMILY_XACT
				&& entry->file.native_family != CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS
				&& entry->file.native_family != CLUSTER_RECOVERY_NATIVE_FAMILY_COMMIT_TS)) {
			scan->diag.skipped_roots++;
			continue;
		}
		if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0)
			return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_BASE_REQUIRED);
		if (entry->file.size % BLCKSZ != 0 || entry->file.size / BLCKSZ > SLRU_PAGES_PER_SEGMENT)
			return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_CORRUPT);
		if (entry->required_blocks > entry->file.size / BLCKSZ)
			return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_BASE_REQUIRED);
		scan->entry = entry;
		scan->pages = entry->file.size / BLCKSZ;
		scan->page = 0;
		switch (entry->file.native_family) {
		case CLUSTER_RECOVERY_NATIVE_FAMILY_XACT:
			scan->per_page = CLUSTER_NATIVE_CLOG_PER_PAGE;
			scan->used = BLCKSZ;
			break;
		case CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS:
			scan->per_page = CLUSTER_NATIVE_SUBTRANS_PER_PAGE;
			scan->used = scan->per_page * sizeof(TransactionId);
			break;
		default: /* native commit timestamp: packed time and replication origin */
			scan->per_page = CLUSTER_NATIVE_COMMIT_TS_PER_PAGE;
			scan->used = scan->per_page * (sizeof(int64) + sizeof(RepOriginId));
			break;
		}
		scan->stage = SLRU_READ;
		scan->diag.files++;
		slru_physical_identity(scan, out);
		out->kind = CLUSTER_RECOVERY_SLRU_FILE;
		return CLUSTER_RECOVERY_SLRU_ENTRY;
	}
	if (scan->next_root == count) {
		scan->status = CLUSTER_RECOVERY_SLRU_DRAINED;
		return scan->status;
	}
	return CLUSTER_RECOVERY_SLRU_MORE;
}

static ClusterRecoverySlruResult
slru_physical_read(ClusterRecoverySlruScan *scan, char *buffer)
{
	ClusterRecoveryRootsResult result = cluster_recovery_root_set_read(
		scan->roots, scan->root_index, scan->page * BLCKSZ, buffer, BLCKSZ);
	if (result == CLUSTER_RECOVERY_ROOTS_BYTES)
		return CLUSTER_RECOVERY_SLRU_MORE;
	if (result == CLUSTER_RECOVERY_ROOTS_IO)
		return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_IO);
	if (result == CLUSTER_RECOVERY_ROOTS_CAPACITY)
		return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_CAPACITY);
	return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_CHANGED);
}

ClusterRecoverySlruResult
cluster_recovery_slru_scan_next(ClusterRecoverySlruScan *scan, ClusterRecoverySlruEntry *out)
{
	ClusterRecoverySlruResult result;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_INVALID);
	if (scan->status > CLUSTER_RECOVERY_SLRU_DRAINED)
		return scan->status;
	if (!slru_physical_roots_current(scan))
		return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_CHANGED);
	if (scan->status == CLUSTER_RECOVERY_SLRU_DRAINED)
		return scan->status;
	if (scan->stage == SLRU_READ && scan->page == scan->pages)
		scan->stage = SLRU_SELECT;
	if (scan->stage == SLRU_SELECT)
		return slru_physical_select(scan, out);
	if (scan->stage == SLRU_READ) {
		result = slru_physical_read(scan, scan->current.data);
		if (result != CLUSTER_RECOVERY_SLRU_MORE)
			return result;
		scan->slot = 0;
		scan->stage = SLRU_SLOTS;
		scan->diag.pages++;
		return CLUSTER_RECOVERY_SLRU_MORE;
	}
	if (scan->stage == SLRU_SLOTS && scan->slot == scan->per_page)
		scan->stage = SLRU_PADDING;
	if (scan->stage == SLRU_SLOTS) {
		slru_physical_identity(scan, out);
		out->slot = scan->slot++;
		out->page_number = (uint64)out->segment * SLRU_PAGES_PER_SEGMENT + scan->page;
		out->raw_xid = out->page_number * scan->per_page + out->slot;
		out->addressable = out->raw_xid <= UINT32_MAX;
		out->bytes = scan->current.data;
		out->length = BLCKSZ;
		switch (out->family) {
		case CLUSTER_RECOVERY_NATIVE_FAMILY_XACT:
			out->raw_status
				= ((uint8)scan->current.data[out->slot / 4] >> ((out->slot % 4) * 2)) & 3;
			out->kind = CLUSTER_RECOVERY_SLRU_CLOG;
			scan->diag.clog_slots++;
			break;
		case CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS:
			memcpy(&out->raw_parent, scan->current.data + out->slot * sizeof(TransactionId),
				   sizeof(TransactionId));
			out->kind = CLUSTER_RECOVERY_SLRU_SUBTRANS;
			scan->diag.subtrans_slots++;
			break;
		default: {
			/* commit_ts.c copies 8+2 bytes, not sizeof its padded struct. */
			const char *slot
				= scan->current.data + out->slot * (sizeof(int64) + sizeof(RepOriginId));
			memcpy(&out->raw_timestamp, slot, sizeof(out->raw_timestamp));
			memcpy(&out->raw_replication_origin, slot + sizeof(int64), sizeof(RepOriginId));
			out->kind = CLUSTER_RECOVERY_SLRU_COMMIT_TS;
			scan->diag.commit_ts_slots++;
			break;
		}
		}
		if (!out->addressable)
			scan->diag.unaddressable_slots++;
		return CLUSTER_RECOVERY_SLRU_ENTRY;
	}
	if (scan->stage == SLRU_PADDING) {
		size_t used = scan->used;
		scan->stage = SLRU_REREAD;
		if (used < BLCKSZ) {
			slru_physical_identity(scan, out);
			out->kind = CLUSTER_RECOVERY_SLRU_PADDING;
			out->page_number = (uint64)out->segment * SLRU_PAGES_PER_SEGMENT + scan->page;
			out->bytes = scan->current.data + used;
			out->length = BLCKSZ - used;
			scan->diag.padding_bytes += out->length;
			return CLUSTER_RECOVERY_SLRU_ENTRY;
		}
	}
	result = slru_physical_read(scan, scan->recheck.data);
	if (result != CLUSTER_RECOVERY_SLRU_MORE)
		return result;
	if (memcmp(scan->current.data, scan->recheck.data, BLCKSZ) != 0)
		return slru_physical_fail(scan, CLUSTER_RECOVERY_SLRU_CHANGED);
	scan->diag.reread_pages++;
	scan->page++;
	scan->stage = SLRU_READ;
	return CLUSTER_RECOVERY_SLRU_MORE;
}

bool
cluster_recovery_slru_scan_diag(const ClusterRecoverySlruScan *scan, ClusterRecoverySlruDiag *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || scan == NULL || scan->status > CLUSTER_RECOVERY_SLRU_DRAINED
		|| !slru_physical_roots_current(scan))
		return false;
	*out = scan->diag;
	return true;
}

void
cluster_recovery_slru_scan_destroy(ClusterRecoverySlruScan *scan)
{
	free(scan);
}
#endif
