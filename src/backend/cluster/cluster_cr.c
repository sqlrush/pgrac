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
#include "cluster/cluster_inject.h"
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
 * Test injection hooks (spec-3.9 Step 7; SKIP-style precondition)
 * ============================================================ */

/*
 * cr_check_error_injections -- if a CR error injection point is armed, raise
 *	the CR code's OWN precise SQLSTATE (NOT the framework's generic XX000).
 *	Called at the top of the chain walker so it fires deterministically
 *	regardless of the actual undo chain (spec-3.9 v0.4 F8/F10).
 */
static void
cr_check_error_injections(void)
{
	uint64 param = 0;

	if (cluster_cr_injection_armed("cr_snapshot_too_old", &param))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
						errmsg("snapshot too old: CR cannot reconstruct (injected; segment %u)",
							   (uint32)param),
						errhint("test injection cr_snapshot_too_old; disarm with "
								"cluster_inject_fault('cr_snapshot_too_old','none',0).")));

	if (cluster_cr_injection_armed("cr_cross_instance", &param))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
				 errmsg("cluster CR cross-instance UBA (injected; origin_node_id=%u, local=%d)",
						(uint32)param, cluster_node_id),
				 errhint("test injection cr_cross_instance; spec-3.9 is own-instance only.")));

	if (cluster_cr_injection_armed("cr_corruption", &param)) {
		const char *kind = (param == 1)	  ? "uba_decode"
						   : (param == 2) ? "crc"
						   : (param == 3) ? "unknown_record_type"
						   : (param == 4) ? "chain_break"
										  : "unknown";

		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR undo chain corruption (injected; kind=%s)", kind),
						errhint("test injection cr_corruption param 1..4.")));
	}
}


/* ============================================================
 * Chain walker driver (body lands in Step 3)
 * ============================================================ */

/*
 * cr_walk_and_apply -- walk ITL[itl_idx].undo_segment_head backward and
 *	inverse-apply every undo record newer than read_scn onto scratch_page.
 *
 *	Stop conditions + chain terminal taxonomy (spec-3.9 §3.1 I-chain-1..4):
 *	  - I-chain-1  write_scn not later than read_scn: the unconditional
 *	               normal stop; this record + everything older is already in
 *	               the snapshot.
 *	  - I-chain-2  invalid prev_uba (chain end) is a legal base state ONLY
 *	               when no record was applied (empty chain) or the last
 *	               applied record was an INSERT (the row was created after
 *	               read_scn and inverse-INSERT made it LP_UNUSED).
 *	  - I-chain-3  reaching chain end while still newer than read_scn after an
 *	               UPDATE/DELETE/ITL record => the older base needed to reach
 *	               read_scn is unreachable (most likely retention recycled)
 *	               => 53R9F snapshot_too_old (fail-closed, NEVER
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
	PGAlignedBlock record_buf;

	(void)buf; /* page bytes already copied into scratch by the caller */

	/* spec-3.9 Step 7: deterministic error injection (raises CR's own
	 * SQLSTATE).  Checked FIRST so L4/L5/L6 fire regardless of page state. */
	cr_check_error_injections();

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
		if (scn_time_cmp(hdr->write_scn, read_scn) <= 0)
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
			/* payload offsets are PAYLOAD-relative (heapam.c sets them to
			 * sizeof(...Payload)); base them at the payload start, not the
			 * record start which includes the UndoRecordHeader. */
			const char *old_bytes = (const char *)p + p->old_tuple_offset;

			if (sizeof(UndoRecordHeader) + (size_t)p->old_tuple_offset + p->old_tuple_length > len)
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
			/* payload-relative offset (see UPDATE branch). */
			const char *full_bytes = (const char *)p + p->full_tuple_offset;

			if (sizeof(UndoRecordHeader) + (size_t)p->full_tuple_offset + p->full_tuple_length
				> len)
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
			const UndoItlPayload *p
				= (const UndoItlPayload *)(record_buf.data + sizeof(UndoRecordHeader));

			if (len < sizeof(UndoRecordHeader) + sizeof(UndoItlPayload))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR ITL undo record too short")));
			if (!cluster_cr_apply_itl_inverse(scratch_page, hdr, p))
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

		uba = hdr->prev_uba;
	}

	/*
	 * Chain terminal taxonomy (revised after verify-linkdb-first; flag for
	 * codereview — supersedes the v0.2 F4 I-chain-2/3 assumption).
	 *
	 *   The ITL undo_segment_head chain is PER-TRANSACTION (it threads every
	 *   undo record written under that ITL slot's xact), NOT per-row.  Each
	 *   record fully restores the prior physical state of its target tuple,
	 *   so once we have inverse-applied every record on this chain whose
	 *   only records newer than read_scn, the scratch page reflects the read_scn state
	 *   for the tuples this xact touched — and a clean `invalid prev_uba`
	 *   simply means the xact's undo is exhausted.  That is a LEGITIMATE
	 *   terminal, NOT "older base unreachable".
	 *
	 *   The v0.2 F4 audit assumed a per-row chain where chain-end before the
	 *   SCN stop implied a truncated history.  With the real per-xact chain,
	 *   a genuine truncation (retention recycled an older record we still
	 *   needed) instead surfaces as cluster_undo_get_record() returning 0 in
	 *   the loop above -> 53R9F there.  So clean chain-end is success and we
	 *   do not raise here.
	 */
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_chain_walk_steps_sum, steps);
}


/* ============================================================
 * 2-layer public API
 * ============================================================ */

const char *
cluster_cr_construct_block(Buffer buf, SCN read_scn, int itl_idx)
{
	/* Init silences cppcheck uninitvar: the only non-throwing exit assigns
	 * result in the PG_TRY body; PG_CATCH always PG_RE_THROWs, but cppcheck
	 * cannot reason through the longjmp. */
	const char *result = NULL;

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

		/* spec-3.9 Step 7: deterministic wait-event window for TAP L8.
		 * pg_usleep with the armed microsecond param under the wait event. */
		{
			uint64 delay_us = 0;

			if (cluster_cr_injection_armed("cr_construct_delay_us", &delay_us) && delay_us > 0)
				pg_usleep((long)delay_us);
		}

		memcpy(cr_scratch, BufferGetPage(buf), BLCKSZ);

		cr_walk_and_apply(cr_scratch, buf, read_scn, itl_idx);

		pgstat_report_wait_end();

		if (CRShared != NULL)
			pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);

		result = cr_scratch;
	}
	PG_CATCH();
	{
		/*
		 * spec-3.9 Step 6: centralized error-taxonomy counter bump.  Every
		 * failure path in cr_walk_and_apply ereports with a precise SQLSTATE
		 * (53R9F / 53R9G / data_corrupted, I-fail-1); reading it here with
		 * geterrcode() bumps the matching counter exactly once per failed
		 * construction without touching the ~15 ereport sites.  Injection-
		 * forced failures (Step 7) flow through here too.
		 */
		if (CRShared != NULL) {
			int sqlerr = geterrcode();

			if (sqlerr == ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD)
				pg_atomic_fetch_add_u64(&CRShared->cr_snapshot_too_old_count, 1);
			else if (sqlerr == ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_cross_instance_unsupported_count, 1);
			else if (sqlerr == ERRCODE_DATA_CORRUPTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_corruption_count, 1);
		}

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


/* ============================================================
 * Tuple-level cluster visibility helpers (spec-3.9 Step 4.5)
 *
 *   spec-3.9 §5 places cluster_visibility_decide_tuple in the generic
 *   cluster visibility helper layer and cluster_visibility_decide_cr_tuple
 *   in cluster_cr.c.  linkdb has no standalone generic cluster_visibility.c
 *   (only cluster_visibility_inject.c), so both helpers live here for now;
 *   FLAG FOR USER CODEREVIEW — if a dedicated cluster_visibility.c is
 *   wanted, decide_tuple moves there unchanged.
 *
 *   Both decide visibility WITHOUT PG-native ProcArray/CLOG (AD-012 例外 9
 *   / spec-3.9 I-fail-2/3).  They use SCN-based decisions and the tuple
 *   header committed/invalid bits.
 *
 *   VISIBILITY SEMANTICS (MVP — flag for codereview):
 *     - decide_tuple is for the 3-tier fast-path exits, where the block is
 *       already at/before read_scn so the physical tuple IS the read_scn
 *       version.  When the tuple's ITL slot carries a valid commit_scn it
 *       defers to cluster_visibility_decide_by_scn(commit_scn, read_scn);
 *       otherwise it uses the tuple's xmin-committed / xmax-invalid bits.
 *     - decide_cr_tuple is for a CR image already reconstructed to read_scn:
 *       post-read_scn changes are undone, so the row is VISIBLE iff its
 *       image xmin is committed/frozen and its xmax is invalid.  No buffer,
 *       no hint-bit writes (I-lock-4).
 * ============================================================ */

/*
 * Pure tuple-header visibility approximation (no syscall, no ProcArray).
 *   VISIBLE iff xmin is committed/frozen AND xmax is invalid.
 */
static ClusterVisibilityDecision
cr_decide_by_infomask(HeapTupleHeader htup)
{
	bool xmin_committed = (htup->t_infomask & HEAP_XMIN_COMMITTED) != 0;
	bool xmax_invalid = (htup->t_infomask & HEAP_XMAX_INVALID) != 0
						|| !TransactionIdIsValid(HeapTupleHeaderGetRawXmax(htup));

	if (xmin_committed && xmax_invalid)
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}

ClusterVisibilityDecision
cluster_visibility_decide_tuple(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tup = htup->t_data;

	/*
	 * If the tuple's ITL slot carries an authoritative commit_scn, decide by
	 * SCN against the snapshot read_scn (cluster semantics).  Else fall back
	 * to the tuple-header committed/invalid bits.  Never consult
	 * ProcArray/CLOG (I-fail-2).
	 */
	if (BufferIsValid(buffer)) {
		Page page = BufferGetPage(buffer);

		if (PageHasItl(page) && tup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& tup->t_itl_slot_idx < CLUSTER_ITL_INITRANS_DEFAULT) {
			const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[tup->t_itl_slot_idx];

			if (SCN_VALID(slot->commit_scn) && SCN_VALID(snapshot->read_scn))
				return cluster_visibility_decide_by_scn(slot->commit_scn, snapshot->read_scn);
		}
	}

	return cr_decide_by_infomask(tup);
}

ClusterVisibilityDecision
cluster_visibility_decide_cr_tuple(HeapTuple htup, Snapshot snapshot)
{
	HeapTupleHeader tup = htup->t_data;

	(void)snapshot; /* CR image already reconstructed to read_scn */

	/*
	 * The CR image tuple is the row as of read_scn: the chain walker has
	 * undone every change newer than read_scn.  Reasoning about
	 * visibility on the reconstructed image:
	 *
	 *   - A tuple that was INSERTED after read_scn was inverse-INSERTed to
	 *     LP_UNUSED, so it never reaches here (cluster_cr_remap_tuple returns
	 *     false -> the gate caller treats it invisible).  Therefore any tuple
	 *     present in the image existed at read_scn.
	 *   - A tuple DELETED after read_scn was inverse-DELETEd, restoring the
	 *     pre-delete image with xmax cleared -> visible.
	 *   - A tuple DELETED at/before read_scn keeps its xmax in the image
	 *     (the delete is not undone, SCN stop) -> invisible.
	 *
	 * So this image-level decision reduces to "is the reconstructed tuple
	 * still live (xmax invalid) as of read_scn".  We deliberately do NOT
	 * require the HEAP_XMIN_COMMITTED hint bit: it is a lazy optimization that
	 * is often unset in a captured undo image, and the chain walk already
	 * encodes the xmin-after-read_scn case via inverse-INSERT.
	 *
	 * The xmin-side (creation) check — a tuple still present in the image whose
	 * inserting xact committed AFTER read_scn (the UPDATE new-version and the
	 * H6 insert-then-late-commit cases) — is handled by the caller gate
	 * (cluster_cr_satisfies_mvcc, spec-3.9 Hardening L214) using the tuple's
	 * live ITL slot write_scn, because that needs the buffer/slot which this
	 * image-only helper deliberately does not take (I-lock-4 / I-fail-3).
	 */
	if (tup->t_infomask & HEAP_XMAX_INVALID)
		return CLUSTER_VISIBILITY_VISIBLE;
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tup)))
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}


/* ============================================================
 * MVCC 3-tier short-circuit gate (spec-3.9 Step 5)
 * ============================================================ */

bool
cluster_cr_satisfies_mvcc(HeapTuple htup, Snapshot snapshot, Buffer buffer, bool *out_visible)
{
	HeapTupleHeader tup = htup->t_data;
	Page page;
	PageHeader phdr;
	uint8 itl_idx;
	const ClusterItlSlotData *slot;
	const char *cr_page;
	HeapTupleData cr_htup;
	ClusterVisibilityDecision decision;

	/*
	 * Master switch (default on after spec-3.9 Hardening v1.0.1).  Turning it
	 * off is a diagnostic escape hatch; normal cluster snapshots take the CR
	 * path when the page/ITL SCN gates prove the tuple was modified after
	 * read_scn.
	 */
	if (!cluster_cr_mvcc_gate)
		return false;

	if (!cluster_enabled || !BufferIsValid(buffer)
		|| snapshot->cluster_source != (uint8)SNAPSHOT_SOURCE_CLUSTER)
		return false;

	page = BufferGetPage(buffer);
	if (!PageHasItl(page))
		return false;

	itl_idx = tup->t_itl_slot_idx;
	if (itl_idx == CLUSTER_ITL_SLOT_UNALLOCATED || itl_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;

	phdr = (PageHeader)page;

	/* Tier 1 (page gate): block already at/before snapshot -> not our case;
	 * the existing visibility path / PG-native body handles it. */
	if (!SCN_VALID(phdr->pd_block_scn) || !SCN_VALID(snapshot->read_scn))
		return false;
	if (scn_time_cmp(phdr->pd_block_scn, snapshot->read_scn) <= 0)
		return false;

	slot = &ClusterPageGetItlSlots(page)[itl_idx];

	/* Tier 2 (ITL gate): this tuple's own change is already in the snapshot. */
	if (!SCN_VALID(slot->write_scn))
		return false;
	if (scn_time_cmp(slot->write_scn, snapshot->read_scn) <= 0)
		return false;

	/* Tier 3 (own-instance only): remote tuples are resolved by the existing
	 * spec-3.2/3.3 remote-xid block; cross-instance CR is Stage 4. */
	if (UBA_is_invalid(slot->undo_segment_head))
		return false;
	if ((int32)uba_origin_node_id(slot->undo_segment_head) != cluster_node_id)
		return false;

	/*
	 * Gate fired: construct the read_scn block image and decide on the
	 * historical tuple.  cluster_cr_lookup_or_construct never returns NULL —
	 * it ereports the precise SQLSTATE on failure (I-fail-1).
	 */
	cr_page = cluster_cr_lookup_or_construct(buffer, snapshot->read_scn, itl_idx);

	if (!cluster_cr_remap_tuple(cr_page, ItemPointerGetOffsetNumber(&htup->t_self), &cr_htup)) {
		/* CR-removed (inverse-INSERT made it LP_UNUSED): the row did not
		 * exist at read_scn -> invisible. */
		*out_visible = false;
		return true;
	}

	/*
	 * spec-3.9 Hardening (L214): xmin-side (creation) visibility.
	 *
	 * cluster_visibility_decide_cr_tuple is xmax-only and assumes every tuple
	 * still present in the CR image existed at read_scn.  That holds for a
	 * fresh INSERT (inverse-INSERTed to LP_UNUSED -> remap false above) and a
	 * DELETE (inverse-DELETE clears xmax), but NOT for the NEW physical
	 * version produced by an UPDATE: the walker inverse-applies the update
	 * onto the OLD tuple's slot, leaving the new version present in the image
	 * with xmin = the updating xact.  If that xact committed after read_scn
	 * the new version did not exist at the snapshot and must be invisible, or
	 * the snapshot would see BOTH the old and the new value (caught by t/215
	 * L10).  This also closes the H6 "inserted-before-read_scn-but-committed-
	 * after" case.
	 *
	 * The tuple's own ITL slot is the writer that produced this version, and
	 * the tier-2 gate above already established slot.write_scn is newer than read_scn.  If
	 * that post-snapshot writer IS the image tuple's creator (slot.xid ==
	 * xmin) the row was CREATED after the snapshot -> invisible.  We key off
	 * write_scn, NOT commit_scn: under delayed cleanout a just-committed slot
	 * is still ACTIVE with commit_scn = InvalidScn, but write_scn is always
	 * stamped at write time, and commit_scn is not earlier than write_scn, so a
	 * write_scn newer than read_scn already implies the creator committed after
	 * the snapshot.  For
	 * a delete-marked old tuple slot.xid == xmax (the deleter), not xmin, so a
	 * genuine inverse-DELETE / inverse-UPDATE restore is left untouched.
	 */
	{
		TransactionId cr_xmin = HeapTupleHeaderGetRawXmin(cr_htup.t_data);

		if (TransactionIdIsValid(cr_xmin) && cr_xmin == slot->xid) {
			*out_visible = false;
			return true;
		}
	}

	decision = cluster_visibility_decide_cr_tuple(&cr_htup, snapshot);
	*out_visible = (decision == CLUSTER_VISIBILITY_VISIBLE);
	return true;
}
