/*-------------------------------------------------------------------------
 *
 * cluster_cr.c
 *	  pgrac own-instance Consistent Read (CR) block construction.
 *
 *	  Stage 3 第 13 sub-spec (spec-3.9).  Top-level CR machinery:
 *	    - backend-local 8 KB scratch slot + non-reentrant guard
 *	    - ClusterCRShared shmem region (9 atomic counters)
 *	    - 2-layer API: cluster_cr_lookup_or_construct (top, spec-3.10 cache
 *	      hook) / cluster_cr_construct_block (bottom, always constructs)
 *	    - chain walker driver (Step 3) + tuple remap + CR-image visibility
 *	      helper (Step 4.5)
 *
 *	  Inverse-apply helpers live in cluster_cr_apply.c (Step 4).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_guc.h" /* cluster_cr_chain_walk_max_steps, cluster_node_id */
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"


/*
 * ClusterCRShared -- per-instance shmem counters (spec-3.9 §2.5).
 *
 *	9 atomic counters, no LWLock (region lwlock_count = 0): counters are
 *	bumped with pg_atomic_fetch_add_u64 from the constructing backend,
 *	read lock-free by dump_state / pg_cluster_state.
 */
typedef struct ClusterCRShared {
	pg_atomic_uint64 cr_construct_count;
	pg_atomic_uint64 cr_snapshot_too_old_count;
	pg_atomic_uint64 cr_cross_instance_unsupported_count;
	pg_atomic_uint64 cr_corruption_count;
	pg_atomic_uint64 cr_chain_walk_steps_sum;
	pg_atomic_uint64 cr_inverse_insert_count;
	pg_atomic_uint64 cr_inverse_update_count;
	pg_atomic_uint64 cr_inverse_delete_count;
	pg_atomic_uint64 cr_inverse_itl_count;
} ClusterCRShared;

static ClusterCRShared *CRShared = NULL;

/*
 * Backend-local CR scratch slot (spec-3.9 Q3 / I-cr-1).
 *
 *	Single 8 KB reusable page allocated once in TopMemoryContext on first
 *	construction.  cluster_cr_construct_block returns a pointer into this
 *	slot; the pointer is valid only until the next construction call.
 *
 *	cr_in_progress is the non-reentrant guard (I-lock-3): nested CR
 *	construction in the same backend would clobber the shared scratch, so
 *	we Assert against it and keep the flag balanced across ereport via
 *	PG_TRY/PG_CATCH.
 */
static char *cr_scratch = NULL;
static bool cr_in_progress = false;


/* ============================================================
 * Shmem region (L206 5-step)
 * ============================================================ */

Size
cluster_cr_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCRShared));
}

void
cluster_cr_shmem_init(void)
{
	bool found;

	CRShared = ShmemInitStruct("ClusterCRShared", cluster_cr_shmem_size(), &found);

	if (!found) {
		pg_atomic_init_u64(&CRShared->cr_construct_count, 0);
		pg_atomic_init_u64(&CRShared->cr_snapshot_too_old_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cross_instance_unsupported_count, 0);
		pg_atomic_init_u64(&CRShared->cr_corruption_count, 0);
		pg_atomic_init_u64(&CRShared->cr_chain_walk_steps_sum, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_insert_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_update_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_delete_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_itl_count, 0);
	}
}

static const ClusterShmemRegion cluster_cr_region = {
	.name = "pgrac cluster cr counters",
	.size_fn = cluster_cr_shmem_size,
	.init_fn = cluster_cr_shmem_init,
	.lwlock_count = 0, /* atomic counters only; no LWLock */
	.owner_subsys = "cluster_cr",
	.reserved_flags = 0,
};

void
cluster_cr_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_region);
}


/* ============================================================
 * Counter accessors (spec-3.9 §2.1)
 * ============================================================ */

#define CR_COUNTER_ACCESSOR(fn, field)                                                             \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		if (CRShared == NULL)                                                                      \
			return 0;                                                                              \
		return pg_atomic_read_u64(&CRShared->field);                                               \
	}

CR_COUNTER_ACCESSOR(cluster_cr_construct_count, cr_construct_count)
CR_COUNTER_ACCESSOR(cluster_cr_snapshot_too_old_count, cr_snapshot_too_old_count)
CR_COUNTER_ACCESSOR(cluster_cr_cross_instance_unsupported_count,
					cr_cross_instance_unsupported_count)
CR_COUNTER_ACCESSOR(cluster_cr_corruption_count, cr_corruption_count)
CR_COUNTER_ACCESSOR(cluster_cr_chain_walk_steps_sum, cr_chain_walk_steps_sum)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_insert_count, cr_inverse_insert_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_update_count, cr_inverse_update_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_delete_count, cr_inverse_delete_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_itl_count, cr_inverse_itl_count)


/* ============================================================
 * Scratch slot helpers
 * ============================================================ */

/*
 * Ensure the backend-local scratch page is allocated (once, in
 * TopMemoryContext so it survives the lifetime of the backend).
 */
static void
cr_scratch_ensure(void)
{
	if (cr_scratch == NULL)
		cr_scratch = MemoryContextAllocZero(TopMemoryContext, BLCKSZ);
}


/* ============================================================
 * Chain walker driver (body lands in Step 3)
 * ============================================================ */

/*
 * cr_walk_and_apply -- walk ITL[itl_idx].undo_segment_head backward and
 *	inverse-apply every undo record newer than read_scn onto scratch_page.
 *
 *	Stop conditions + chain terminal taxonomy (spec-3.9 §3.1 I-chain-1..4):
 *	  - I-chain-1  write_scn <= read_scn : the unconditional normal stop;
 *	               this record + everything older is already in the snapshot.
 *	  - I-chain-2  invalid prev_uba (chain end) is a legal base state ONLY
 *	               when no record was applied (empty chain) or the last
 *	               applied record was an INSERT (the row was created after
 *	               read_scn and inverse-INSERT made it LP_UNUSED).
 *	  - I-chain-3  reaching chain end while still write_scn > read_scn after
 *	               an UPDATE/DELETE/ITL record => the older base needed to
 *	               reach read_scn is unreachable (most likely retention
 *	               recycled) => 53R9F snapshot_too_old (fail-closed, NEVER
 *	               silent-success).
 *	  - missing record (reader returns 0) => 53R9F.
 *	  - cross-instance origin => 53R9G.
 *	  - malformed UBA / short record / unknown record_type / step cap
 *	    exceeded => data_corrupted.
 *
 *	The inverse-apply bodies live in cluster_cr_apply.c (Step 4); the
 *	helpers return bool and the caller MUST ereport on false (I-fail-4).
 *
 *	Caller already memcpy'd the buffer page into scratch_page and holds the
 *	non-reentrant guard; this function performs no locking (I-lock-2).
 */
static void
cr_walk_and_apply(char *scratch_page, Buffer buf, SCN read_scn, int itl_idx)
{
	Page page = (Page)scratch_page;
	ClusterItlSlotData *slots;
	UBA uba;
	uint32 steps = 0;
	uint32 max_steps = (uint32)cluster_cr_chain_walk_max_steps;
	uint8 last_record_type = UNDO_RECORD_INVALID;
	PGAlignedBlock record_buf;

	(void)buf; /* page bytes already copied into scratch by the caller */

	if (itl_idx < 0 || itl_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR itl_idx %d out of range [0, %d)", itl_idx,
							   CLUSTER_ITL_INITRANS_DEFAULT)));

	if (!PageHasItl(page))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR target page has no ITL special area")));

	slots = ClusterPageGetItlSlots(page);
	uba = slots[itl_idx].undo_segment_head;

	while (!UBA_is_invalid(uba)) {
		UndoRecordHeader *hdr;
		size_t len;
		uint32 seg;
		uint32 blk;
		uint16 tt_off;
		uint16 row_off;

		if (++steps > max_steps)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR chain walk exceeded %u steps", max_steps),
							errhint("chain walk infinite loop suspected; raise "
									"cluster.cr_chain_walk_max_steps if a hot row legitimately "
									"has a longer in-snapshot undo chain.")));

		if (!uba_decode(uba, &seg, &blk, &tt_off, &row_off))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR encountered a malformed UBA in the undo chain")));

		len = cluster_undo_get_record(uba, record_buf.data, sizeof(record_buf.data));
		if (len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("snapshot too old: CR cannot read undo record at segment %u block %u "
							"(recycled or retention exceeded)",
							seg, blk),
					 errhint("Increase undo retention; spec-3.11 will enforce snapshot-age "
							 "retention policy.")));
		if (len < sizeof(UndoRecordHeader))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR read a short undo record (%zu < %zu bytes)", len,
								   sizeof(UndoRecordHeader))));

		hdr = (UndoRecordHeader *)record_buf.data;

		/* Own-instance CR only (spec-3.9 I-cr-5). */
		if (hdr->origin_node_id != (uint16)cluster_node_id)
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
							errmsg("cluster CR cross-instance UBA encountered "
								   "(origin_node_id=%u, local=%d)",
								   hdr->origin_node_id, cluster_node_id),
							errhint("spec-3.9 supports own-instance CR only; cross-instance CR "
									"is Stage 4 (Cache Fusion CR coordinator).")));

		/* I-chain-1: normal SCN stop. */
		if (hdr->write_scn <= read_scn)
			break;

		switch (hdr->record_type) {
		case UNDO_RECORD_INSERT: {
			const UndoInsertPayload *p
				= (const UndoInsertPayload *)(record_buf.data + sizeof(UndoRecordHeader));

			if (!cluster_cr_apply_insert_inverse(scratch_page, hdr, p))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR insert inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_insert_count, 1);
			break;
		}
		case UNDO_RECORD_UPDATE: {
			const UndoUpdatePayload *p
				= (const UndoUpdatePayload *)(record_buf.data + sizeof(UndoRecordHeader));
			const char *old_bytes = record_buf.data + p->old_tuple_offset;

			if ((size_t)p->old_tuple_offset + p->old_tuple_length > len)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR update payload old-tuple bytes out of bounds")));
			if (!cluster_cr_apply_update_inverse(scratch_page, hdr, p, old_bytes,
												 p->old_tuple_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR update inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_update_count, 1);
			break;
		}
		case UNDO_RECORD_DELETE: {
			const UndoDeletePayload *p
				= (const UndoDeletePayload *)(record_buf.data + sizeof(UndoRecordHeader));
			const char *full_bytes = record_buf.data + p->full_tuple_offset;

			if ((size_t)p->full_tuple_offset + p->full_tuple_length > len)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("cluster CR delete payload full-tuple bytes out of bounds")));
			if (!cluster_cr_apply_delete_inverse(scratch_page, hdr, p, full_bytes,
												 p->full_tuple_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR delete inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_delete_count, 1);
			break;
		}
		case UNDO_RECORD_ITL: {
			if (!cluster_cr_apply_itl_inverse(scratch_page, hdr, itl_idx))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR ITL inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_itl_count, 1);
			break;
		}
		default:
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR encountered unknown undo record type %u "
								   "(version skew or corruption)",
								   hdr->record_type)));
		}

		last_record_type = hdr->record_type;
		uba = hdr->prev_uba;
	}

	/*
	 * Chain terminal taxonomy (I-chain-2/3): if we left the loop via an
	 * invalid prev_uba (chain end) rather than the SCN stop, the scratch
	 * page is a legal read_scn base ONLY for an empty chain or an
	 * INSERT-rooted chain.  Any other terminator means the older base was
	 * unreachable -> fail-closed as snapshot_too_old, never silent success.
	 */
	if (UBA_is_invalid(uba) && last_record_type != UNDO_RECORD_INVALID
		&& last_record_type != UNDO_RECORD_INSERT)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
						errmsg("snapshot too old: CR undo chain ended before reaching the "
							   "read_scn base state"),
						errhint("The required older row version is no longer reachable; "
								"increase undo retention.")));

	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_chain_walk_steps_sum, steps);
}


/* ============================================================
 * 2-layer public API
 * ============================================================ */

const char *
cluster_cr_construct_block(Buffer buf, SCN read_scn, int itl_idx)
{
	const char *result;

	Assert(BufferIsValid(buf));
	Assert(!cr_in_progress); /* I-lock-3 non-reentrant */

	cr_scratch_ensure();

	/*
	 * I-lock-3 + I-fail-1: the guard MUST be balanced even if the chain
	 * walker ereport(ERROR)s, otherwise the next construction Asserts /
	 * the guard wedges this backend.  PG_TRY/PG_CATCH resets the flag and
	 * re-throws so the caller still sees the precise SQLSTATE.
	 */
	cr_in_progress = true;

	PG_TRY();
	{
		/* I-lock-1/2/4: caller holds the content lock; we only read the
		 * page bytes into backend-local scratch — no buffer lock, no WAL,
		 * no dirty.  The wait event covers the undo I/O of the chain walk
		 * so it is observable in pg_stat_activity (spec-3.9 §2 / TAP L8). */
		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_CR_CONSTRUCT);

		memcpy(cr_scratch, BufferGetPage(buf), BLCKSZ);

		cr_walk_and_apply(cr_scratch, buf, read_scn, itl_idx);

		pgstat_report_wait_end();

		if (CRShared != NULL)
			pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);

		result = cr_scratch;
	}
	PG_CATCH();
	{
		pgstat_report_wait_end();
		cr_in_progress = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	cr_in_progress = false;
	return result;
}

const char *
cluster_cr_lookup_or_construct(Buffer buf, SCN read_scn, int itl_idx)
{
	/*
	 * spec-3.9: fall-through to construction.  spec-3.10 inserts the CR
	 * cache lookup here (miss → construct + cache, hit → cached image),
	 * without touching the visibility caller.
	 */
	return cluster_cr_construct_block(buf, read_scn, itl_idx);
}


/* ============================================================
 * Tuple remap (CR scratch page → HeapTupleData wrapper)
 * ============================================================ */

bool
cluster_cr_remap_tuple(const char *cr_page, OffsetNumber off, HeapTupleData *out_htup)
{
	Page page = (Page)cr_page;
	ItemId itemid;

	if (cr_page == NULL || out_htup == NULL)
		return false;
	if (off < FirstOffsetNumber || off > PageGetMaxOffsetNumber(page))
		return false;

	itemid = PageGetItemId(page, off);
	if (!ItemIdIsNormal(itemid))
		return false; /* LP_UNUSED / LP_DEAD / LP_REDIRECT: CR-removed or not a tuple */

	out_htup->t_len = ItemIdGetLength(itemid);
	out_htup->t_data = (HeapTupleHeader)PageGetItem(page, itemid);
	out_htup->t_tableOid = InvalidOid;
	ItemPointerSetOffsetNumber(&out_htup->t_self, off);
	/* Block number of t_self is filled by the caller (it knows the buffer's
	 * block); CR scratch alone does not carry it. */
	return true;
}
