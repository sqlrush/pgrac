/*-------------------------------------------------------------------------
 * cluster_recovery_record_hash.c
 *    Full decoded WAL bytes in a canonical cold observation transcript.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_record_hash.h"
#ifdef USE_PGRAC_CLUSTER

static bool
record_hash_number(pg_cryptohash_ctx *hash, uint64 value)
{
	uint8 bytes[8];

	for (unsigned i = 0; i < sizeof(bytes); i++)
		bytes[i] = (uint8)(value >> (i * 8));
	return pg_cryptohash_update(hash, bytes, sizeof(bytes)) == 0;
}

static bool
record_hash_bytes(pg_cryptohash_ctx *hash, const void *bytes, size_t length)
{
	return (length == 0 || bytes != NULL) && record_hash_number(hash, length)
		   && (length == 0 || pg_cryptohash_update(hash, bytes, length) == 0);
}

bool
cluster_recovery_record_hash_update(pg_cryptohash_ctx *hash, const XLogReaderState *reader)
{
	static const uint8 domain[] = "PGRAC-E1-WAL-RECORD-V1";
	const DecodedXLogRecord *record;

	if (hash == NULL || reader == NULL || reader->record == NULL)
		return false;
	record = reader->record;
	if (record->max_block_id < -1 || record->max_block_id > XLR_MAX_BLOCK_ID
		|| (record->has_page_version_edge
			&& (record->page_version_edge.entry_count == 0
				|| record->page_version_edge.entry_count > XLR_PAGE_VERSION_EDGE_MAX_ENTRIES)))
		return false;
	/* Include NUL in the fixed domain; every scalar, including lengths, is
	 * an unsigned LE64. Original decoder validation is still mandatory. */
	if (pg_cryptohash_update(hash, domain, sizeof(domain)) != 0
		|| !record_hash_number(hash, record->lsn) || !record_hash_number(hash, record->next_lsn)
		|| !record_hash_number(hash, record->header.xl_tot_len)
		|| !record_hash_number(hash, record->header.xl_xid)
		|| !record_hash_number(hash, record->header.xl_prev)
		|| !record_hash_number(hash, record->header.xl_scn)
		|| !record_hash_number(hash, record->header.xl_info)
		|| !record_hash_number(hash, record->header.xl_rmid)
		|| !record_hash_number(hash, record->header.xl_crc)
		|| !record_hash_number(hash, record->record_origin)
		|| !record_hash_number(hash, record->toplevel_xid)
		|| !record_hash_bytes(hash, record->main_data, record->main_data_len)
		|| !record_hash_number(hash, record->max_block_id + 1))
		return false;
	for (int i = 0; i <= record->max_block_id; i++) {
		const DecodedBkpBlock *block = &record->blocks[i];

		if (!record_hash_number(hash, block->in_use))
			return false;
		if (!block->in_use)
			continue;
		if (!record_hash_number(hash, block->rlocator.spcOid)
			|| !record_hash_number(hash, block->rlocator.dbOid)
			|| !record_hash_number(hash, block->rlocator.relNumber)
			|| !record_hash_number(hash, (uint32)block->forknum)
			|| !record_hash_number(hash, block->blkno) || !record_hash_number(hash, block->flags)
			|| !record_hash_number(hash, block->component_ordinal)
			|| !record_hash_number(hash, block->has_image))
			return false;
		/* Absent image/data fields are decoder workspace, not record bytes. */
		if (block->has_image
			&& (block->bimg_len > BLCKSZ || !record_hash_number(hash, block->apply_image)
				|| !record_hash_number(hash, block->hole_offset)
				|| !record_hash_number(hash, block->hole_length)
				|| !record_hash_number(hash, block->bimg_info)
				|| !record_hash_bytes(hash, block->bkp_image, block->bimg_len)))
			return false;
		if (!record_hash_number(hash, block->has_data)
			|| (block->has_data && !record_hash_bytes(hash, block->data, block->data_len)))
			return false;
	}
	if (!record_hash_number(hash, record->has_page_version_edge))
		return false;
	if (record->has_page_version_edge) {
		const RfPageVersionEdgeV1 *edges = &record->page_version_edge;

		if (!record_hash_number(hash, edges->entry_count)
			|| !record_hash_number(hash, edges->result_token))
			return false;
		for (unsigned i = 0; i < edges->entry_count; i++) {
			const RfPageVersionEdgeEntryV1 *edge = &edges->entries[i];

			if (!record_hash_number(hash, edge->block_id)
				|| !record_hash_number(hash, edge->page_class)
				|| !record_hash_number(hash, edge->before_kind)
				|| !record_hash_number(hash, edge->result_kind)
				|| !record_hash_number(hash, edge->edge_flags)
				|| !record_hash_number(hash, edge->component_ordinal)
				|| pg_cryptohash_update(hash, edge->before.segment_incarnation, 16) != 0
				|| !record_hash_number(hash, edge->before.mutation_token)
				|| pg_cryptohash_update(hash, edge->result_incarnation, 16) != 0)
				return false;
		}
	}
	return true;
}
#endif
