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
#include "access/transam.h"	   /* spec-3.11 D6/C1b: TransactionIdDidCommit, TransactionIdIsNormal */
#include "access/xact.h"	   /* spec-3.19 D3: TransactionIdIsCurrentTransactionId */
#include "storage/procarray.h" /* spec-3.19 D3: TransactionIdIsInProgress */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h" /* spec-3.20 D3.A: RelFileLocatorEquals (F8 block-scope) */
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_cr_cache.h"
#include "cluster/cluster_guc.h" /* cluster_cr_chain_walk_max_steps, cluster_node_id */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_itl.h" /* spec-3.21: cluster_itl_get_tt_ref (xmax overlay key) */
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_durable.h"			/* spec-3.11 D6: watermark by-xid resolve */
#include "cluster/cluster_visibility_resolve.h" /* spec-3.21: cluster_vis_cr_xmax_verdict */
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
	/* spec-3.10 D5: CR block cache (4 counters, 9 -> 13 cr-category rows). */
	pg_atomic_uint64 cr_cache_hit_count;
	pg_atomic_uint64 cr_cache_miss_count;
	pg_atomic_uint64 cr_cache_evict_count;
	pg_atomic_uint64 cr_cache_install_count;
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
		pg_atomic_init_u64(&CRShared->cr_cache_hit_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_miss_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_evict_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_install_count, 0);
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
CR_COUNTER_ACCESSOR(cluster_cr_cache_hit_count, cr_cache_hit_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_miss_count, cr_cache_miss_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_evict_count, cr_cache_evict_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_install_count, cr_cache_install_count)


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
 * cr_walk_chain -- walk one per-transaction undo chain from start_uba backward
 *	and inverse-apply every undo record newer than read_scn onto scratch_page.
 *	*steps is a running total across all candidate chains in one construction
 *	(spec-3.10); cluster_cr_construct_block_into calls this once per chain.
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
cr_walk_chain(char *scratch_page, UBA start_uba, SCN read_scn,
			  const ClusterCRCandidateChain *chains, int nchains, uint32 *steps, uint32 max_steps,
			  RelFileLocator cur_locator, ForkNumber cur_fork, BlockNumber cur_block)
{
	UBA uba = start_uba;
	PGAlignedBlock record_buf;

	while (!UBA_is_invalid(uba)) {
		UndoRecordHeader *hdr;
		size_t len;
		uint32 seg;
		uint32 blk;
		uint16 tt_off;
		uint16 row_off;

		if (++(*steps) > max_steps)
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
					 errhint("Own-instance retention (cluster.undo_retention_horizon_enabled) "
							 "keeps undo for active readers; this fires only past the best-"
							 "effort capacity bound or when the reader's read_scn predates the "
							 "horizon.")));
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

		/*
		 * spec-3.20 D3.B: every physical INSERT/UPDATE/DELETE/ITL undo record is
		 * written with a valid target relation (cluster_undo_record.c sets it
		 * from the heap relation's locator).  A missing/invalid target locator is
		 * undo corruption, NOT a skippable cross-block record -- fail closed
		 * rather than silently drop it (which would hide the corruption and could
		 * leave a post-read_scn version unrolled).
		 */
		if (!RelFileNumberIsValid(hdr->target_locator.relNumber))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR undo record has an invalid target relation"),
							errhint("undo chain corruption suspected; retry the transaction.")));

		/*
		 * spec-3.20 D3.A (F8): block-scope filter.  The prev_uba chain is
		 * transaction-GLOBAL -- a single txn (e.g. a TPC-B statement updating
		 * pgbench_accounts + _tellers + _branches + inserting _history) chains
		 * undo records across multiple relations/forks/blocks.  We are
		 * reconstructing ONE block (cur_locator/cur_fork/cur_block), so a record
		 * targeting any other relation/fork/block must NOT be inverse-applied to
		 * this scratch page: doing so lands a foreign old image at target_offset
		 * (DIFFLEN -> fail-closed, or -- worse -- a same-length SILENT overwrite
		 * that returns a wrong CR image, the spec-3.20 D0 P0/8.A finding).
		 *
		 * Skip-apply but KEEP walking prev_uba: the global chain can hold a
		 * deeper record that DOES belong to this block (records between read_scn
		 * and this block's head write_scn for other blocks are interleaved).
		 * Stopping here would truncate this block's legitimate history.  The
		 * header already carries the full physical target (HC213).
		 */
		if (!RelFileLocatorEquals(hdr->target_locator, cur_locator) || hdr->target_fork != cur_fork
			|| hdr->target_block != cur_block) {
			uba = hdr->prev_uba;
			continue;
		}

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

		/*
		 * Keep the transient CR image close to the read_scn shape.  A hot row can
		 * have many post-read_scn intermediate versions on the same page
		 * (pgbench_tellers is the small-table repro): if the walk re-adds every
		 * intermediate image and defers pruning to the very end, the scratch page
		 * can temporarily overflow or present a foreign NORMAL tuple at a later
		 * restore offset.  Any tuple whose xmin is one of the candidate xids did
		 * not exist at read_scn, so it is safe to remove immediately after each
		 * inverse step, then compact before the next older undo record.
		 */
		(void)cluster_cr_prune_post_snapshot_versions(scratch_page, chains, nchains);
		PageRepairFragmentation((Page)scratch_page);

		uba = hdr->prev_uba;
	}

	/*
	 * Clean chain-end is a LEGITIMATE terminal: the ITL undo_segment_head chain
	 * is PER-TRANSACTION (threads every undo record under that slot's xact, NOT
	 * per-row), so an invalid prev_uba simply means the xact's undo is
	 * exhausted and the tuples it touched are back at read_scn.  A genuine
	 * truncation (retention recycled a still-needed older record) instead
	 * surfaces as cluster_undo_get_record() == 0 -> 53R9F inside the loop.
	 * *steps is a running total across all candidate chains; the caller
	 * (cluster_cr_construct_block_into) accumulates cr_chain_walk_steps_sum
	 * once after every chain completes.
	 */
}


/*
 * cr_resolve_kept_tuples_durable -- spec-3.11 D6/C4/C5.
 *
 * Called only when the block's ITL recycle watermark exceeds read_scn: a
 * completed DATA ITL slot whose write_scn is newer than the snapshot was
 * recycled out of this block, so cluster_cr_collect_candidate_chains above may
 * not have captured that writer and a post-read_scn tuple version it created
 * could survive the candidate prune as a false-visible (spec-3.10 §v0.5 A).
 *
 * For every still-NORMAL tuple whose xmin is NOT one of the live candidate
 * xids, resolve xmin's commit_scn from the durable TT by xid (spec-3.11 D2):
 *
 *   - committed (CLOG-confirmed -- C1b) and commit_scn is later than read_scn:
 *     the evicted post-read_scn creator -> prune (LP_UNUSED).
 *   - committed and commit_scn is not later than read_scn: a legitimate
 *     pre-read_scn version -> keep.
 *   - not committed per CLOG (aborted / still in flight at this read): the
 *     creator's row was not visible at read_scn -> prune.
 *   - durable lookup miss / ambiguous: cannot prove either way -> fail closed
 *     (53R9F).  Never leave a possibly-post-read_scn version visible (规则 8.A).
 *
 * This retires the spec-3.10 blanket fail-closed for every kept tuple whose
 * durable slot is still resolvable; only a tuple whose own slot was already
 * recycled (lookup miss) still fails closed -- spec-3.12 retention shrinks that
 * window.  Own-instance only (the watermark is a local-block property; remote
 * origins resolve via Cache Fusion in Stage 4).
 *
 * Mutates dst_page (LP_UNUSED marks); the caller leaves them unrepaired (the CR
 * image is read-only and visibility scans skip LP_UNUSED, matching prune-AFTER).
 */
static void
cr_resolve_kept_tuples_durable(char *dst_page, SCN read_scn, const ClusterCRCandidateChain *chains,
							   int nchains)
{
	Page page = (Page)dst_page;
	OffsetNumber off;
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
		ItemId lp = PageGetItemId(page, off);
		HeapTupleHeader htup;
		TransactionId xmin;
		SCN durable_scn = InvalidScn;
		bool is_candidate = false;
		int c;

		if (!ItemIdIsNormal(lp))
			continue;

		htup = (HeapTupleHeader)PageGetItem(page, lp);
		xmin = HeapTupleHeaderGetRawXmin(htup);

		/* Frozen / bootstrap xids predate any cluster snapshot -> visible. */
		if (!TransactionIdIsNormal(xmin))
			continue;

		/* xmin already covered by the candidate prune/walk -> nothing to do. */
		for (c = 0; c < nchains; c++) {
			if (chains[c].xid == xmin) {
				is_candidate = true;
				break;
			}
		}
		if (is_candidate)
			continue;

		/*
		 * xmin fell outside the (possibly incomplete) candidate set.  Resolve it
		 * durably by xid; a miss / ambiguity is unresolvable -> fail closed.
		 */
		if (!cluster_tt_slot_durable_lookup_by_xid(xmin, &durable_scn))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
							errmsg("cluster CR cannot reconstruct block: durable TT slot for "
								   "writer xid %u is unavailable after ITL slot reuse",
								   xmin),
							errhint("retry the transaction with a fresh snapshot")));

		/*
		 * C1b (规则 8.A): the durable slot is stamped at pre-commit, so a
		 * COMMITTED stamp alone does not prove the xact committed.  Confirm via
		 * CLOG: an xact that stamped then aborted (or is still in flight at this
		 * read) was not visible at read_scn -> prune.
		 */
		if (!TransactionIdDidCommit(xmin)) {
			ItemIdSetUnused(lp);
			continue;
		}

		/* Committed after the snapshot -> evicted post-read_scn creator -> prune. */
		if (scn_time_cmp(durable_scn, read_scn) > 0)
			ItemIdSetUnused(lp);
		/* else committed at/before read_scn -> legitimate pre-read_scn -> keep. */
	}
}


/* ============================================================
 * 2-layer public API
 * ============================================================ */

/*
 * cluster_cr_construct_block_into -- full-block CR into a caller-provided
 *	BLCKSZ destination (the CR cache victim slot in spec-3.10, or the backend
 *	scratch via the public wrapper).  memcpy the live page, then inverse-apply
 *	EVERY candidate ITL chain (write_scn newer than read_scn) in write_scn-DESC order so
 *	the whole block is rolled back to read_scn (Oracle block-level CR).
 *
 *	Candidate chains are snapshotted BEFORE any inverse-apply (cluster_cr_apply
 *	itl_inverse mutates the page's ITL slots; re-reading heads mid-walk would
 *	corrupt selection — spec-3.10 D1).  Order is newest-first because each
 *	per-transaction chain's inverse is an unconditional old-image restore and a
 *	row touched by txB then txC must be peeled C->B (spec-3.10 Q10).
 *
 *	I-lock-3 non-reentrant + I-fail-1 balanced guard (PG_TRY resets cr_in_progress
 *	and re-throws the precise SQLSTATE).  Returns dst_page.
 */
static const char *
cluster_cr_construct_block_into(Buffer buf, SCN read_scn, char *dst_page)
{
	/* Init silences cppcheck uninitvar across the PG_RE_THROW longjmp. */
	const char *result = NULL;

	Assert(BufferIsValid(buf));
	Assert(dst_page != NULL);
	Assert(!cr_in_progress); /* I-lock-3 non-reentrant */

	cr_in_progress = true;

	PG_TRY();
	{
		Page page;
		const ClusterItlSlotData *slots;
		ClusterCRCandidateChain chains[CLUSTER_ITL_INITRANS_DEFAULT];
		int nchains;
		uint32 steps = 0;
		uint32 max_steps = (uint32)cluster_cr_chain_walk_max_steps;
		bool watermark_exceeds = false; /* spec-3.11 D6: durable resolve gate */

		/* I-lock-1/2/4: caller holds the content lock; we only read page bytes
		 * into dst — no buffer lock, no WAL, no dirty.  The wait event covers
		 * the undo I/O of the chain walks (spec-3.9 §2 / TAP L8). */
		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_CR_CONSTRUCT);

		/* deterministic wait-event window for TAP L8 (armed µs sleep). */
		{
			uint64 delay_us = 0;

			if (cluster_cr_injection_armed("cr_construct_delay_us", &delay_us) && delay_us > 0)
				pg_usleep((long)delay_us);
		}

		memcpy(dst_page, BufferGetPage(buf), BLCKSZ);

		/* spec-3.9 Step 7: injection-forced taxonomy fires FIRST (TAP L4/L5/L6),
		 * before any page inspection, so it is page-state-independent. */
		cr_check_error_injections();

		page = (Page)dst_page;
		if (!PageHasItl(page))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR target page has no ITL special area")));

		/*
		 * spec-3.10 §v0.5 / spec-3.11 D6: slot-reuse gate.  If a completed DATA
		 * ITL slot whose write_scn is newer than this reader's snapshot was
		 * recycled out of this block (its undo-chain anchor overwritten), the
		 * per-page candidate set may be incomplete and a post-read_scn tuple
		 * version could survive the candidate prune as a false-visible.
		 *
		 * spec-3.10 failed the whole block closed (53R9F) here.  spec-3.11 D6
		 * instead defers to cr_resolve_kept_tuples_durable() after the candidate
		 * prune + walk: each kept tuple whose xmin is not a live candidate is
		 * resolved against the durable TT by xid, so only a tuple whose own
		 * durable slot was already recycled still fails closed (§v0.5 A).
		 */
		{
			SCN recycle_wm = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;

			watermark_exceeds = SCN_VALID(recycle_wm) && scn_time_cmp(recycle_wm, read_scn) > 0;
		}

		/* Snapshot candidate chains BEFORE mutation, then peel newest-first. */
		slots = ClusterPageGetItlSlots(page);
		nchains = cluster_cr_collect_candidate_chains(slots, read_scn, chains,
													  CLUSTER_ITL_INITRANS_DEFAULT);
		if (nchains > 1)
			qsort(chains, nchains, sizeof(chains[0]), cluster_cr_chain_cmp_by_write_scn_desc);

		/*
		 * If the recycle watermark says the candidate set may be incomplete,
		 * resolve evicted post-read_scn tuple creators before walking undo.
		 * Otherwise a recycled-slot tuple can remain NORMAL at the target
		 * offset and block an older UPDATE/DELETE restore with a length/identity
		 * mismatch.  The post-walk call below remains as the final fail-closed
		 * guard for tuples materialized by the chain walk itself.
		 */
		if (watermark_exceeds) {
			if (cluster_tt_durable_lookup)
				cr_resolve_kept_tuples_durable(dst_page, read_scn, chains, nchains);
			else
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
								errmsg("cluster CR cannot reconstruct block: ITL slot reused "
									   "after snapshot"),
								errhint("retry the transaction with a fresh snapshot")));
		}

		/*
		 * spec-3.10 §v0.6: prune post-read_scn versions BOTH before and after
		 * the inverse-apply (v0.4 pruned only after).
		 *
		 * Chain revert clears xmax but does not remove the new physical version
		 * an UPDATE produced (heap_update emits only UNDO_RECORD_UPDATE, no
		 * INSERT-undo for the new version); the prune marks LP_UNUSED every
		 * tuple whose xmin is a post-read_scn candidate (new versions, reusing
		 * INSERTs, standalone INSERTs -- see its header + §3.3).
		 *
		 * prune-FIRST + PageRepairFragmentation (§v0.6): frees the line pointer
		 * a later INSERT reused for one of those tuples and reclaims its space,
		 * so the inverse-apply can re-add the old image at its read_scn offnum
		 * even though PG's prune horizon (decoupled from read_scn -- AD-012)
		 * reused that offset after the snapshot.  INSERT-inverse is idempotent
		 * on the already-freed slot.  See cluster_cr_apply.c
		 * cr_restore_full_image / cr_readd_at_offnum (§v0.6 B1-B5).
		 *
		 * prune-AFTER (retained from v0.4): a row updated more than once on this
		 * block has intermediate versions whose pre-images the walk restores
		 * (the older txn's UNDO_RECORD_UPDATE old image IS the newer version);
		 * those carry a candidate xmin, so a second prune strips them, leaving
		 * exactly the read_scn version (Q10 write_scn-DESC peel -- t/216 L4b).
		 */
		(void)cluster_cr_prune_post_snapshot_versions(dst_page, chains, nchains);
		PageRepairFragmentation((Page)dst_page);

		/*
		 * spec-3.20 D3.A (F8): the physical identity of the block we are
		 * reconstructing, so cr_walk_chain can drop undo records the same
		 * transaction wrote against OTHER relations/forks/blocks.
		 */
		{
			RelFileLocator cur_locator;
			ForkNumber cur_fork;
			BlockNumber cur_block;

			BufferGetTag(buf, &cur_locator, &cur_fork, &cur_block);

			for (int i = 0; i < nchains; i++)
				cr_walk_chain(dst_page, chains[i].undo_segment_head, read_scn, chains, nchains,
							  &steps, max_steps, cur_locator, cur_fork, cur_block);
		}

		(void)cluster_cr_prune_post_snapshot_versions(dst_page, chains, nchains);

		/*
		 * spec-3.11 D6/C4: if a completed post-read_scn DATA slot was evicted
		 * (watermark > read_scn), the candidate set above may miss its writer.
		 * Resolve every kept tuple whose xmin is not a live candidate against the
		 * durable TT by xid, pruning evicted post-read_scn versions and failing
		 * closed only on an unresolvable (recycled) slot.
		 *
		 * cluster.tt_durable_lookup=off (C6 fallback) reverts to the spec-3.10
		 * overlay-only behavior: any watermark > read_scn fails the whole block
		 * closed (53R9F) without consulting the durable TT.
		 */
		if (watermark_exceeds)
			cr_resolve_kept_tuples_durable(dst_page, read_scn, chains, nchains);

		pgstat_report_wait_end();

		if (CRShared != NULL) {
			pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);
			pg_atomic_fetch_add_u64(&CRShared->cr_chain_walk_steps_sum, steps);
		}

		result = dst_page;
	}
	PG_CATCH();
	{
		/*
		 * spec-3.9 Step 6: centralized error-taxonomy counter bump.  Every
		 * failure path ereports a precise SQLSTATE (53R9F / 53R9G /
		 * data_corrupted); geterrcode() bumps the matching counter once per
		 * failed construction.  Injection-forced failures flow through here too.
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
cluster_cr_construct_block(Buffer buf, SCN read_scn)
{
	/* Public fallback: construct into the backend-local scratch.  Used by the
	 * test SRF and as the cache-disabled path (spec-3.10 §3.1). */
	cr_scratch_ensure();
	return cluster_cr_construct_block_into(buf, read_scn, cr_scratch);
}

/*
 * cr_build_cache_key -- CR cache identity for `buf` at `read_scn`.  base_page_lsn
 * is the live page LSN (bumped by every WAL-logged physical change incl.
 * HOT-prune / VACUUM), the version guard that forces a miss after a relayout
 * (spec-3.10 §3.2).  memset zeroes padding so the key compares cleanly.
 */
static ClusterCRCacheKey
cr_build_cache_key(Buffer buf, SCN read_scn)
{
	ClusterCRCacheKey key;

	memset(&key, 0, sizeof(key));
	BufferGetTag(buf, &key.rlocator, &key.forknum, &key.blockno);
	key.read_scn = read_scn;
	key.base_page_lsn = PageGetLSN(BufferGetPage(buf));
	return key;
}

const char *
cluster_cr_lookup_or_construct(Buffer buf, SCN read_scn)
{
	/*
	 * spec-3.10 D3: probe the backend-local CR cache; on a hit serve the
	 * immutable cached image (no construction).  On a miss, reserve a victim
	 * slot, construct the full-block CR directly into it (two-phase: a throwing
	 * construction never commits the slot), then commit + return.  Disabled
	 * (max_blocks == 0): lookup always misses, victim_slot returns the fallback
	 * buffer, so this degrades to plain construction.
	 */
	ClusterCRCacheKey key = cr_build_cache_key(buf, read_scn);
	const char *hit;
	char *slot;
	bool evicted = false;

	hit = cluster_cr_cache_lookup(&key);
	if (hit != NULL) {
		if (CRShared != NULL)
			pg_atomic_fetch_add_u64(&CRShared->cr_cache_hit_count, 1);
		return hit;
	}
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_miss_count, 1);

	slot = cluster_cr_cache_victim_slot(&key, &evicted);
	if (evicted && CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_evict_count, 1);

	/* Construct INTO the reserved slot.  ereports on failure with the slot
	 * left uncommitted (invalid -> never served). */
	(void)cluster_cr_construct_block_into(buf, read_scn, slot);

	cluster_cr_cache_commit_slot();
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_install_count, 1);
	return slot;
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

/*
 * cluster_cr_resolve_xmax_commit_scn -- resolve the EXACT commit_scn of a
 * committed own-instance deleter (cr_xmax) recorded on a CR image, for the
 * spec-3.21 xmax-side visibility decision.  Returns true + *out_scn on success.
 *
 *	Used only for the committed-at/before-read_scn branch: the committed-AFTER-
 *	read_scn case is decided VISIBLE in the caller via the live slot (tier-2 has
 *	already proved the live deleter's write_scn > read_scn, hence commit_scn >
 *	read_scn -- the sound direction of write_scn, NOT the P1-a inverse).  So this
 *	resolver is reached only when the LIVE slot was recycled to a newer writer.
 *
 *	Exact-key source order (spec-3.21 D2; never a CLOG/write_scn proxy -- P1-a):
 *	  1. the CR SCRATCH page ITL slot at itl_idx, IFF slot.xid == cr_xmax;
 *	  2. the BOC / spec-3.6 overlay by exact key (scratch ITL ref + local_xid);
 *	  3. the durable TT by exact xid -- survives an ITL slot recycle.
 *	A slot/xid mismatch or InvalidScn at every source -> false, and the caller
 *	fails closed (53R9F); rule 8.A: an unresolved deleter is NEVER treated as a
 *	committed-before-read_scn delete (which would false-hide a live row).
 */
static bool
cluster_cr_resolve_xmax_commit_scn(const char *cr_page, uint8 itl_idx, TransactionId cr_xmax,
								   SCN *out_scn)
{
	Page page = (Page)cr_page; /* read-only ITL access on the scratch image */

	*out_scn = InvalidScn;

	/* Sources 1+2 require the scratch slot to still hold cr_xmax exactly. */
	if (itl_idx != CLUSTER_ITL_SLOT_UNALLOCATED && itl_idx < CLUSTER_ITL_INITRANS_DEFAULT
		&& PageHasItl(page)) {
		const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[itl_idx];

		if (slot->xid == cr_xmax) {
			ClusterUndoTTSlotRef ref;

			/*
			 * Source 1: the rolled-back scratch slot's own commit stamp.  NB: a
			 * lock-only ITL undo record for this same slot index, inverse-applied
			 * during the chain walk, can restore prev_commit_scn (= InvalidScn)
			 * over cr_xmax's later-stamped commit_scn while leaving slot.xid ==
			 * cr_xmax (cluster_cr_apply_itl_inverse restores commit_scn/flags/
			 * undo_segment_head but not xid).  In that case this and Source 2
			 * (whose ref came from the same rolled-back slot) both miss, and
			 * Source 3 (durable scan by exact xid) is the true authoritative
			 * fallback.  Never a wrong scn -- a stale InvalidScn just falls through.
			 */
			if (SCN_VALID(slot->commit_scn)) {
				*out_scn = slot->commit_scn;
				return true;
			}

			/* Source 2: BOC / overlay by the exact key (ref + local_xid). */
			if (cluster_itl_get_tt_ref(page, itl_idx, &ref)) {
				ClusterTTStatusKey key;
				ClusterTTStatusResult result;

				memset(&key, 0, sizeof(key));
				key.origin_node_id = ref.origin_node_id;
				key.undo_segment_id = ref.undo_segment_id;
				key.tt_slot_id = ref.tt_slot_id;
				key.cluster_epoch = ref.cluster_epoch;
				key.local_xid = cr_xmax;

				if (cluster_tt_status_lookup_exact(&key, &result) && result.authoritative
					&& (result.status == CLUSTER_TT_STATUS_COMMITTED
						|| result.status == CLUSTER_TT_STATUS_CLEANED_OUT)
					&& SCN_VALID(result.commit_scn)) {
					*out_scn = result.commit_scn;
					return true;
				}
			}
		}
	}

	/* Source 3: durable TT by exact xid (survives ITL slot recycle). */
	if (cluster_tt_slot_durable_lookup_by_xid(cr_xmax, out_scn) && SCN_VALID(*out_scn))
		return true;

	*out_scn = InvalidScn;
	return false; /* unresolved -> caller fails closed (53R9F). */
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
	 * spec-3.19 D3: live-tuple xmin guard (fail-closed toward invisible).
	 *
	 * The CR image occupant of this offset can differ from the LIVE occupant
	 * after HOT line-pointer reuse + 8-slot ITL recycling, so the historical-
	 * image decision below must NOT be applied to a live tuple that is not
	 * itself a committed, fully-finished version.  Without this guard the gate
	 * reports the historical version's visibility for a live version whose own
	 * xmin HeapTupleSatisfiesUpdate then rejects -> TM_Invisible ("attempted to
	 * update invisible tuple", spec-3.19 D0/D1).
	 *
	 * The verdict must match what HeapTupleSatisfiesUpdate uses, NOT a CLOG-only
	 * test: the disagreement window is the COMMIT-IN-PROGRESS gap.  A committing
	 * xact sets its CLOG commit bit (TransactionIdDidCommit -> true) BEFORE it
	 * leaves the ProcArray (ProcArrayEndTransaction).  During that gap the native
	 * SatisfiesUpdate path still sees the writer via TransactionIdIsInProgress and
	 * returns TM_Invisible (D0 captured xmin{commit=1 inprog=1}).  So the live
	 * version is a valid visible update target only when its xmin is BOTH
	 * committed AND no longer in progress; otherwise (still in progress, in the
	 * commit gap, or aborted) it is visible to no snapshot -> invisible.
	 *
	 * Own-instance only (tier-3 above): the live xmin's CLOG + ProcArray state is
	 * local-authoritative.  Our own write is excluded (self-modification is
	 * handled by the native path).  The construct-time prune already drops
	 * post-read_scn *committed* versions; this guard closes the in-progress /
	 * commit-gap / aborted versions the prune cannot key off commit_scn.  The
	 * snapshot's correct older version is found by the normal chain walk.
	 */
	{
		TransactionId live_xmin = HeapTupleHeaderGetRawXmin(tup);

		if (TransactionIdIsNormal(live_xmin) && !TransactionIdIsCurrentTransactionId(live_xmin)
			&& (TransactionIdIsInProgress(live_xmin) || !TransactionIdDidCommit(live_xmin))) {
			*out_visible = false;
			return true;
		}
	}

	/*
	 * Gate fired: construct the read_scn block image (full-block CR, spec-3.10)
	 * and decide on the historical tuple.  cluster_cr_lookup_or_construct never
	 * returns NULL — it ereports the precise SQLSTATE on failure (I-fail-1).
	 * itl_idx is no longer passed (full-block rolls back every candidate chain);
	 * the queried tuple's live `slot` (already resolved above) still drives the
	 * tier-2 + xmin-side checks.
	 */
	cr_page = cluster_cr_lookup_or_construct(buffer, snapshot->read_scn);

	if (!cluster_cr_remap_tuple(cr_page, ItemPointerGetOffsetNumber(&htup->t_self), &cr_htup)) {
		/* CR-removed (inverse-INSERT made it LP_UNUSED): the row did not
		 * exist at read_scn -> invisible. */
		*out_visible = false;
		return true;
	}

	/*
	 * spec-3.9 Hardening (L214): xmin-side (creation) visibility.
	 *
	 * spec-3.10 NOTE: this per-tuple check is now SUPERSEDED by the construct-
	 * time cluster_cr_prune_post_snapshot_versions() (full-block CR), which
	 * marks LP_UNUSED every post-read_scn-created tuple BEFORE remap — so any
	 * tuple reaching here has a pre-read_scn creator and cr_xmin != slot->xid,
	 * i.e. this branch never fires (verified across t/215 + t/216).  Kept as a
	 * zero-cost defense-in-depth backstop; removable in a future cleanup
	 * (spec-3.10 §v0.4 D-A).  L214 alone could NOT handle same-row-multi-update
	 * (a doubly-updated tuple's live slot is its latest modifier, not creator).
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
	if (decision == CLUSTER_VISIBILITY_VISIBLE) {
		/* xmax invalid -> the row was never deleted -> visible. */
		*out_visible = true;
		return true;
	}

	/*
	 * spec-3.21 D1: the CR image carries a VALID xmax.  The pre-3.21 code
	 * returned invisible here ("any valid xmax -> deleted at read_scn"), but a
	 * valid xmax only proves SOME xact wrote a delete/lock mark -- not that the
	 * delete COMMITTED at/before read_scn.  When the deleter is lock-only,
	 * in-progress, aborted, or committed AFTER read_scn, the row was LIVE at the
	 * snapshot and must be VISIBLE.  Mis-hiding it produced silent hot-row
	 * UPDATE 0 / lost updates (D0.6: 538 in-progress false-invisibles).  Resolve
	 * the deleter's commit-state vs read_scn instead (own-instance, tier-3).
	 */
	{
		HeapTupleHeader cr_tup = cr_htup.t_data;
		uint16 cr_infomask = cr_tup->t_infomask;
		TransactionId cr_xmax;
		ClusterTTStatus xmax_status;
		ClusterVisibilityDecision scn_decision = CLUSTER_VISIBILITY_UNKNOWN;

		/*
		 * A lock-only xmax never deletes the tuple (spec-3.21 P1-c).
		 * HEAP_XMAX_IS_LOCKED_ONLY also catches HEAP_LOCKED_UPGRADED (the legacy
		 * multi-locker pattern has HEAP_XMAX_LOCK_ONLY set), so it is handled
		 * here -- and HeapTupleGetUpdateXid below (reached only on the MULTI
		 * branch) is thus never called on a lock-only multi (its internal
		 * Assert(!LOCK_ONLY) holds).
		 */
		if (HEAP_XMAX_IS_LOCKED_ONLY(cr_infomask)) {
			*out_visible = true;
			return true;
		}

		/* MultiXact: decode the UPDATE member; a lockers-only multi has no
		 * update xid -> visible.  Never treat the multi value as a plain xid. */
		if (cr_infomask & HEAP_XMAX_IS_MULTI)
			cr_xmax = HeapTupleGetUpdateXid(cr_tup);
		else
			cr_xmax = HeapTupleHeaderGetRawXmax(cr_tup);

		if (!TransactionIdIsValid(cr_xmax) || TransactionIdIsCurrentTransactionId(cr_xmax)) {
			/* lockers-only multi, or our own delete (native handles self). */
			*out_visible = true;
			return true;
		}

		/* Own-instance authoritative classification of the deleting xact. */
		if (TransactionIdIsInProgress(cr_xmax))
			xmax_status = CLUSTER_TT_STATUS_IN_PROGRESS;
		else if (!TransactionIdDidCommit(cr_xmax))
			xmax_status = CLUSTER_TT_STATUS_ABORTED;
		else {
			/* Committed deleter: invisible IFF the delete is visible at read_scn. */
			SCN xmax_cscn;

			/*
			 * Live-slot shortcut: if the LIVE tuple's ITL slot still holds cr_xmax
			 * (the deleter), tier-2 above already proved slot->write_scn > read_scn,
			 * so commit_scn >= write_scn > read_scn -- the delete committed AFTER
			 * the snapshot -> the row was live at read_scn -> VISIBLE.  (Sound
			 * direction of write_scn; P1-a forbids only the inverse.)  This is the
			 * common RR-snapshot + concurrent-commit case (t/229 L6) whose deleter
			 * slot has not been recycled; its commit_scn need not be stamped yet.
			 */
			if (slot->xid == cr_xmax) {
				*out_visible = true;
				return true;
			}

			/*
			 * The live slot was recycled to a newer writer (slot->xid != cr_xmax):
			 * resolve cr_xmax's EXACT commit_scn by exact xid (P1-a: no proxy) and
			 * compare to read_scn, or fail closed (53R9F).
			 */
			if (!cluster_cr_resolve_xmax_commit_scn(cr_page, cr_tup->t_itl_slot_idx, cr_xmax,
													&xmax_cscn))
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
								errmsg("cluster CR cannot resolve commit_scn for deleting xmax %u",
									   cr_xmax),
								errhint("ITL slot recycled and no own-instance commit_scn-by-xid "
										"history source yet (Stage 4.1); retry with a fresh "
										"snapshot.")));
			xmax_status = CLUSTER_TT_STATUS_COMMITTED;
			scn_decision = cluster_visibility_decide_by_scn(xmax_cscn, snapshot->read_scn);
		}

		switch (cluster_vis_cr_xmax_verdict(xmax_status, scn_decision)) {
		case CVV_VISIBLE:
			*out_visible = true;
			return true;
		case CVV_INVISIBLE:
			*out_visible = false;
			return true;
		case CVV_FAILCLOSED_UNKNOWN:
		default:
			/*
			 * cluster_vis_cr_xmax_verdict only ever returns VISIBLE / INVISIBLE /
			 * FAILCLOSED_UNKNOWN; the wait/conflict/gone verdicts (CVV_BEING_
			 * MODIFIED / CVV_GONE_* / CVV_FAILCLOSED_CONFLICT) belong to the
			 * SatisfiesUpdate/Dirty forks, not this snapshot-read gate.  Any of
			 * them reaching here would be a verdict-table bug -> fail closed.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR xmax visibility unresolved for deleting xmax %u", cr_xmax),
					 errhint("deleter commit_scn could not be proven against read_scn; retry.")));
		}
	}

	pg_unreachable(); /* every verdict branch above returns or ereports */
}
