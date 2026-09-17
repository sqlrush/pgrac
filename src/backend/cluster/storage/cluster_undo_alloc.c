/*-------------------------------------------------------------------------
 *
 * cluster_undo_alloc.c
 *	  pgrac undo segment allocator (runtime API).
 *
 *	  Stage 1.22 entry points for backend-side undo segment management:
 *	    - cluster_undo_path_resolve(): pure path builder
 *	    - cluster_undo_segment_allocate(): create + init + WAL-protect
 *	      one segment file under $PGDATA/pg_undo/instance_<N>/seg_<id>.dat
 *
 *	  Allocator NOT called during initdb (initdb writes the seed segment
 *	  directly via libpgport in initdb.c -- frontend / no WAL).  Backend
 *	  allocator is exercised at:
 *	    - Stage 1.22: cluster_unit harness tests + cluster_tap L5/L6
 *	    - Stage 2+ (feature-117): on-demand at first transaction binding
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §2.3 + §D5.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_alloc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"	  /* cluster_undo_gcs_coherence */
#include "cluster/cluster_mode.h"	  /* cluster_peer_mode_enabled */
#include "common/file_perm.h"		  /* pg_mkdir_p / pg_dir_create_mode (shared subdir, D2-2) */
#include "cluster/cluster_scn.h"	  /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_undo_gcs.h" /* path intent + shared-root decision */
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_undo_record_api.h" /* reuse counter note (3.13) */
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_xlog.h" /* emit_segment_recycle (3.13 D3) */
#include "cluster/cluster_undo_retention.h"	   /* segment recyclable predicate */
#include "cluster/cluster_undo_segment_init.h" /* fresh header builder (D4 reuse) */
#include "access/xlog.h"					   /* XLogFlush (3.13 v0.3 (1)) */
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_shared_fs.h" /* undo namespace on shared root (D2-2) */
#include "cluster/storage/cluster_undo_buf.h"  /* invalidate_segment on reuse (3.18 D3.2) */
#include "cluster/storage/cluster_undo_xlog.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/fd.h"


/* spec-3.4b D2: forward decl of cluster.node_id GUC owned by cluster_guc.c. */
extern int cluster_node_id;


/*
 * cluster_undo_path_resolve
 *
 *   Pure path builder (no I/O, no errors apart from buffer overflow).
 *
 *   Hardening v1.0.4 P1-1 (directory naming separation):
 *     - The owner_instance VALUE in headers / WAL payloads is
 *       (cluster_node_id + 1), so 0 stays the "unallocated" sentinel.
 *     - The DIRECTORY NAME on disk uses cluster_node_id directly
 *       (= owner_instance - 1) so that single-node default
 *       (cluster_node_id = 0) lays out at pg_undo/instance_0/...
 *       matching the initdb seed segment (also at instance_0/).
 *       Otherwise allocator segments would land at instance_1/ and
 *       split from the seed, breaking the per-instance subdir
 *       invariant.
 *
 *   Caller MUST pass owner_instance in [1, UNDO_OWNER_INSTANCE_MAX];
 *   the assert catches sentinel-0 misuse.
 */
int
cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 owner_instance, uint32 segment_id,
						  char *buf, size_t buf_size)
{
	int ret;

	if (buf == NULL || buf_size == 0)
		return -1;
	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/*
	 * spec-5.22b D2-2 (threading strategy B): every caller derives intent via
	 * cluster_undo_intent_for_owner(owner), so in D2 RUNTIME_SHARED ⟺ own
	 * instance and MATERIALIZED_LOCAL ⟺ a foreign dead-origin owner.
	 *
	 * spec-5.22d D4-3: the dead-owner authority serve introduces exactly one
	 * sanctioned foreign-owner shared read -- RUNTIME_SHARED_AUTHORITY_BLOCK0,
	 * which by construction targets a foreign owner (owner != self).  Relax
	 * the invariant to admit that one case ONLY:
	 *   - a plain RUNTIME_SHARED with a foreign owner is still a loud
	 *     split-brain bug (it would silently read the shared copy of a
	 *     segment it does not own);
	 *   - AUTHORITY_BLOCK0 with owner == self is likewise rejected (a live
	 *     owner serves via the D6 path, never via the authority block0 route).
	 * The read stays block0-only by the call-site contract (§2.3/§3.6); this
	 * assert only governs the owner-vs-self / intent pairing.
	 */
	Assert(((intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED)
			== (owner_instance == (uint8)(cluster_node_id + 1)))
		   || (intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0
			   && owner_instance != (uint8)(cluster_node_id + 1)));

	/*
	 * spec-5.22e D5-4: the pairing above is a WRITER-EXCLUSION pillar (the
	 * D4 complete-scan prove and the D5 retention brake both argue "no
	 * foreign writer can exist in an owner's shared namespace"), so an
	 * Assert alone is not enough -- production builds must fail closed too
	 * (rule 12: Assert never substitutes for a runtime guard).  Callers
	 * resolve paths outside critical sections (the 53R9D discipline), so
	 * an ERROR here is safe.
	 */
	if (!((((intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED)
			== (owner_instance == (uint8)(cluster_node_id + 1)))
		   || (intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0
			   && owner_instance != (uint8)(cluster_node_id + 1)))))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("undo path intent/ownership violation: intent %d, owner %u, self node %d",
						(int)intent, (unsigned)owner_instance, cluster_node_id),
				 errhint("Foreign-owner shared undo is only reachable through the dead-owner "
						 "authority block0 route; anything else is a split-brain bug "
						 "(spec-5.22e writer-exclusion guard).")));

	/*
	 * RUNTIME_SHARED own undo migrates to the shared cluster_fs root only
	 * under peer-mode + cluster.undo_gcs_coherence; MATERIALIZED_LOCAL and
	 * every non-coherent mode stay on the local DataDir (P1-3 / 裁决 A).  An
	 * unset shared root on the shared branch fails closed (resolver -1).
	 */
	if (cluster_undo_path_uses_shared_root(intent, cluster_peer_mode_enabled(),
										   cluster_undo_gcs_coherence))
		return cluster_shared_fs_undo_path_resolve(owner_instance, segment_id, buf, buf_size);

	/* local DataDir: directory uses cluster_node_id (= owner_instance - 1) */
	ret = snprintf(buf, buf_size, "%s/pg_undo/instance_%u/seg_%u.dat", DataDir,
				   (unsigned)(owner_instance - 1), (unsigned)segment_id);
	if (ret < 0 || (size_t)ret >= buf_size)
		return -1;
	return 0;
}


/*
 * Ensure the parent directory pg_undo/instance_<N>/ exists.
 *
 * Stage 1.22: only instance_0/ is created at initdb time.  Backend
 * allocations to instance >= 2 (cross-instance, deferred to Stage 2+)
 * would also need the subdir.  For Stage 1.22 we keep this defensive:
 * the allocator never reaches instance != 1 because the public API
 * rejects it, but if a future caller does, mkdir() handles it.
 */
static void
ensure_instance_subdir(uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;

	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/*
	 * spec-5.22b D2-2: own-instance allocation under coherence lands the
	 * segment on the shared cluster_fs root, whose pg_undo/instance_<N> tree
	 * is not seeded by initdb -- create the whole path (idempotent).  Foreign
	 * owners never reach the allocator (single-writer), so only the shared
	 * branch differs from the pre-D2 local mkdir.
	 */
	if (cluster_undo_path_uses_shared_root(cluster_undo_intent_for_owner(owner_instance),
										   cluster_peer_mode_enabled(),
										   cluster_undo_gcs_coherence)) {
		if (cluster_shared_fs_undo_instance_dir_resolve(owner_instance, path, sizeof(path)) != 0)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("undo shared instance subdir unresolved: owner_instance=%u "
								   "(cluster.shared_data_dir unset)",
								   (unsigned)owner_instance)));
		if (pg_mkdir_p(path, pg_dir_create_mode) != 0 && errno != EEXIST)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create undo shared instance subdir \"%s\": %m", path)));
		return;
	}

	/* local DataDir: directory uses cluster_node_id (= owner_instance - 1); see
	 * cluster_undo_path_resolve docstring. */
	ret = snprintf(path, sizeof(path), "%s/pg_undo/instance_%u", DataDir,
				   (unsigned)(owner_instance - 1));
	if (ret < 0 || (size_t)ret >= sizeof(path))
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("undo instance subdir path too long: owner_instance=%u",
							   (unsigned)owner_instance)));

	if (mkdir(path, S_IRWXU) < 0 && errno != EEXIST)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create undo instance subdir \"%s\": %m", path)));
}

/*
 * Existing undo segment files are mutable: lifecycle state, block bitmap, and
 * durable TT slots all live in block 0 after allocation.  Idempotence therefore
 * means "the header identity is valid", not "the whole block still equals the
 * freshly allocated zero template".
 */
bool
cluster_undo_segment_header_identity_ok(const char *blockbuf, uint32 segment_id,
										uint8 owner_instance)
{
	PageHeader ph = (PageHeader)blockbuf;
	const UndoSegmentHeaderData *hdr = (const UndoSegmentHeaderData *)blockbuf;

	if ((ph->pd_flags & PD_UNDO_SEG_HEADER) == 0)
		return false;
	if (PageGetPageSize((Page)blockbuf) != BLCKSZ
		|| PageGetPageLayoutVersion((Page)blockbuf) != PG_PAGE_LAYOUT_VERSION)
		return false;

	if (hdr->segment_id != segment_id || hdr->segment_size_bytes != UNDO_SEGMENT_SIZE_BYTES
		|| hdr->owner_instance != owner_instance || hdr->tt_slots_count != TT_SLOTS_PER_SEGMENT)
		return false;

	switch (hdr->segment_state) {
	case SEGMENT_ALLOCATED:
	case SEGMENT_ACTIVE:
	case SEGMENT_COMMITTED:
	case SEGMENT_RECYCLABLE:
		return true;
	default:
		return false;
	}
}


/*
 * cluster_undo_segment_allocate
 *
 *   Validate the local owner and enter the current-authorized publisher.
 *   Existing exact images are load-only. New headers remain private until
 *   INIT WAL, complete file contents and no-replace publication are durable.
 *   Invalid existing files are never repaired by a live allocator.
 */
void
cluster_undo_segment_allocate(uint32 segment_id, uint8 owner_instance)
{
	ClusterUndoBlock0LogicalKey logical = { 0 };
	ClusterUndoBlock0Result result;

	/*
	 * spec-3.4b D2 multi-instance unlock.
	 *
	 *   The previous Stage 1.22 single-node restriction
	 *   ("owner_instance must equal CLUSTER_UNDO_DEFAULT_OWNER (1)")
	 *   is replaced with the multi-instance validation chain below.
	 */
	if (owner_instance == CLUSTER_UNDO_OWNER_INVALID || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("owner_instance %u out of range [1, %u]", (unsigned)owner_instance,
							   (unsigned)UNDO_OWNER_INSTANCE_MAX)));

	/* Spec-3.4b F2: segment 0 is the bootstrap-only sentinel. */
	if (segment_id == 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("segment_id 0 is bootstrap-only and not allocatable")));

	/* Spec-3.4b F4: exact-key alias guard against 16-bit truncation. */
	if (segment_id > UINT16_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("segment_id %u exceeds spec-3.4b limit of UINT16_MAX (%u)",
							   (unsigned)segment_id, (unsigned)UINT16_MAX),
						errhint("Stage 4/5 may bump ClusterUndoTTSlotRef.undo_segment_id "
								"and ClusterTTStatusKey.undo_segment_id to wider types.")));

	/*
	 * Spec-3.4b D2: a node may only allocate segments whose id falls
	 * within its reserved per-instance range, i.e. the derived
	 * owner_instance from segment_id must match the requested
	 * owner_instance.  Cross-instance mis-allocation is a programming
	 * bug -- raise PANIC so corruption is impossible.
	 */
	{
		uint32 derived_inst = ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1;

		if (derived_inst != (uint32)owner_instance)
			ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("segment_id %u maps to owner_instance %u, "
								   "but allocate was called with owner_instance %u",
								   (unsigned)segment_id, (unsigned)derived_inst,
								   (unsigned)owner_instance)));

		/* And that owner must be this node. */
		if ((int)owner_instance != cluster_node_id + 1)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("attempt to allocate segment owned by instance %u "
							"from cluster_node_id %d (own owner_instance %d)",
							(unsigned)owner_instance, cluster_node_id, cluster_node_id + 1)));
	}

	ensure_instance_subdir(owner_instance);
	logical.segment_id = segment_id;
	logical.owner_instance = owner_instance;
	result = cluster_undo_block0_current_live_owner_provision(&logical, 10000);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("undo segment first publication refused: instance=%u segment=%u result=%d",
						(unsigned)owner_instance, (unsigned)segment_id, (int)result)));
}


/*
 * cluster_undo_active_segment_for_node_or_create
 *
 *   spec-3.4b D2: return the segment_id of this node's active undo
 *   segment, creating it on first call.  See header for the encoding.
 */
uint32
cluster_undo_active_segment_for_node_or_create(int node_id)
{
	static int cached_node_id = -1;
	static uint32 cached_segment_id = 0;
	static bool cached_block0_resident = false;
	uint32 segment_id;
	uint8 owner_instance;
	ClusterUndoBlock0LogicalKey logical;

	if (node_id < 0 || node_id > SCN_MAX_VALID_NODE_ID)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("node_id %d out of range [0, %d] for undo segment selection",
							   node_id, SCN_MAX_VALID_NODE_ID)));

	owner_instance = (uint8)(node_id + 1);

	/* per_instance_slot = 0 (spec-3.4b MVP, single active segment per node) */
	segment_id = (uint32)node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	logical.segment_id = segment_id;
	logical.owner_instance = owner_instance;

	if (cached_node_id == node_id && cached_segment_id == segment_id) {
		if (!cached_block0_resident
			&& cluster_undo_block0_current_live_owner_ensure_resident(&logical, 10000)
				   == CLUSTER_UNDO_BLOCK0_OK)
			cached_block0_resident = true;
		return cached_segment_id;
	}

	/*
	 * cluster_undo_segment_allocate is idempotent under current authority.
	 * Calling it here means the first
	 * DML on this node lazily provisions the segment; subsequent calls in
	 * this backend return from the cache above.
	 */
	cluster_undo_segment_allocate(segment_id, owner_instance);

	cached_node_id = node_id;
	cached_segment_id = segment_id;
	/* Successful first publication already retained and admitted the exact
	 * image under current X; do not acquire the same authority twice. */
	cached_block0_resident = true;

	return segment_id;
}


/* ============================================================
 * spec-3.8 D1+D5+D6: Lifecycle state machine + tail_block + bitmap
 *
 *   Hardening v1.0.1:
 *	   H-1 first_active_block ≡ tail_block (linkdb SSOT field name)
 *	   H-2 wrap_count field doesn't exist;  TT slot wrap (per-slot)
 *	       preserved as-is per spec-1.20 SSOT;  segment-level
 *	       wrap_count deferred to spec-3.12 真 recycle
 *
 *   I/O pattern: open segment file → pread block 0 (8KB header) →
 *   modify field → pwrite block 0 → fsync.  Per spec §3.2 lock contract,
 *   these are lifecycle work and OK to perform under lifecycle_lock
 *   (caller's concern).
 * ============================================================ */

/*
 * spec-3.8 Fix #372:  lifecycle helpers now read/write the 8 KB segment
 * header via cluster_undo_smgr block I/O instead of inline BasicOpenFile +
 * pg_pread/pwrite.  The undo segment header occupies block 0 (8192 bytes),
 * so a header RMW becomes:  smgr_read_block(0) → modify hdr → smgr_write_block
 * (0, do_fsync=true).
 *
 * A PGAlignedBlock-sized scratch buffer is used so the temporary cast back
 * to UndoSegmentHeaderData stays alignment-safe.
 */

static bool
read_segment_header_via_smgr(uint32 segment_id, uint8 owner_instance, char *blockbuf,
							 UndoSegmentHeaderData **out_hdr)
{
	if (!cluster_undo_smgr_read_block(cluster_undo_intent_for_owner(owner_instance), segment_id,
									  owner_instance, 0, blockbuf))
		return false;
	*out_hdr = (UndoSegmentHeaderData *)blockbuf;
	return true;
}


typedef enum ClusterUndoLifecycleMutationKind {
	CLUSTER_UNDO_LIFECYCLE_MARK_ACTIVE = 1,
	CLUSTER_UNDO_LIFECYCLE_MARK_COMMITTED,
	CLUSTER_UNDO_LIFECYCLE_MARK_FULL,
	CLUSTER_UNDO_LIFECYCLE_INIT_TAIL,
	CLUSTER_UNDO_LIFECYCLE_MARK_BLOCK,
	CLUSTER_UNDO_LIFECYCLE_CLAIM_RANGE
} ClusterUndoLifecycleMutationKind;

/* Freeze one legal monotonic lifecycle successor, then let the sole live
 * block-zero current/content-X owner compare and publish it.  This function
 * may wait for 0xFB and therefore must run without lifecycle/content locks. */
static bool
cluster_undo_block0_current_live_owner_lifecycle_exact(uint32 segment_id, uint8 owner_instance,
													   ClusterUndoLifecycleMutationKind kind,
													   uint32 arg1, uint32 arg2)
{
	PGAlignedBlock predecessor;
	PGAlignedBlock successor;
	UndoSegmentHeaderData *before;
	UndoSegmentHeaderData *after;
	ClusterUndoBlock0LogicalKey key;
	ClusterUndoBlock0Generation expected;
	uint64 last;
	uint32 block;

	if (!read_segment_header_via_smgr(segment_id, owner_instance, predecessor.data, &before)
		|| !cluster_undo_segment_header_identity_ok(predecessor.data, segment_id, owner_instance)
		|| before->wrap_count == UINT32_MAX)
		return false;
	memcpy(successor.data, predecessor.data, BLCKSZ);
	after = (UndoSegmentHeaderData *)successor.data;

	switch (kind) {
	case CLUSTER_UNDO_LIFECYCLE_MARK_ACTIVE:
		if (!UndoSegmentState_can_become_active(before->segment_state))
			return false;
		after->segment_state = SEGMENT_ACTIVE;
		break;
	case CLUSTER_UNDO_LIFECYCLE_MARK_COMMITTED:
		if (before->segment_state != SEGMENT_ACTIVE && before->segment_state != SEGMENT_COMMITTED)
			return false;
		after->segment_state = SEGMENT_COMMITTED;
		break;
	case CLUSTER_UNDO_LIFECYCLE_MARK_FULL:
		if (before->segment_state != SEGMENT_ACTIVE)
			return false;
		after->segment_flags |= UNDO_SEGMENT_FLAG_FULL;
		break;
	case CLUSTER_UNDO_LIFECYCLE_INIT_TAIL:
		if (before->segment_state != SEGMENT_ACTIVE || arg1 != 1
			|| (before->tail_block != 0 && before->tail_block != arg1))
			return false;
		after->tail_block = arg1;
		break;
	case CLUSTER_UNDO_LIFECYCLE_MARK_BLOCK:
		if (before->segment_state != SEGMENT_ACTIVE || arg1 >= UNDO_BLOCKS_PER_SEGMENT)
			return false;
		(void)UndoSegmentBitmap_mark_used(after->free_block_bitmap, arg1);
		break;
	case CLUSTER_UNDO_LIFECYCLE_CLAIM_RANGE:
		last = (uint64)arg1 + arg2;
		if (before->segment_state != SEGMENT_ACTIVE || arg1 < 1 || arg2 == 0
			|| last > UNDO_BLOCKS_PER_SEGMENT)
			return false;
		/* An extent claim is intentionally not idempotent: accepting an
		 * already-used bit could hand the same extent to two local writers. */
		for (block = arg1; block < (uint32)last; block++) {
			uint32 byte_idx = block / 8;
			uint8 bit_mask = (uint8)(1u << (block % 8));

			if ((before->free_block_bitmap[byte_idx] & bit_mask) != 0)
				return false;
		}
		if (!UndoSegmentBitmap_mark_range_used(after->free_block_bitmap, arg1, arg2,
											   UNDO_BLOCKS_PER_SEGMENT))
			return false;
		break;
	default:
		return false;
	}

	key.segment_id = segment_id;
	key.owner_instance = owner_instance;
	expected.known = true;
	expected.value = before->wrap_count;
	if (cluster_undo_block0_current_live_owner_ensure_resident(&key, 10000)
		!= CLUSTER_UNDO_BLOCK0_OK)
		return false;
	return cluster_undo_block0_current_live_owner_mutate_exact(&key, &expected, predecessor.data,
															   successor.data, 10000)
		   == CLUSTER_UNDO_BLOCK0_OK;
}


/*
 * cluster_undo_segment_mark_active -- D1 ALLOCATED → ACTIVE transition.
 *
 *	Called on first successful record write into the segment.  Reads
 *	segment header,  flips segment_state to SEGMENT_ACTIVE,  writes back
 *	+ fsync.  Idempotent (already-ACTIVE returns true without rewrite).
 */
bool
cluster_undo_segment_mark_active(uint32 segment_id, uint8 owner_instance)
{
	return cluster_undo_block0_current_live_owner_lifecycle_exact(
		segment_id, owner_instance, CLUSTER_UNDO_LIFECYCLE_MARK_ACTIVE, 0, 0);
}


/*
 * cluster_undo_segment_mark_committed -- spec-3.12 D3 ACTIVE → COMMITTED.
 *
 *	Minimal lazy transition: when the TT-slot allocator rolls a drained segment
 *	away (no in-flight ACTIVE TT slot left) and that segment is no longer the
 *	record cursor's active segment, it moves to SEGMENT_COMMITTED ("all tx
 *	committed; awaits retention").  cluster_undo_segment_recyclable() then admits
 *	it once the horizon passes the segment's watermark; proactive reclaim of
 *	RECYCLABLE segments stays in spec-3.13.  Only ACTIVE (incl. FULL-but-ACTIVE,
 *	since FULL is a flag not a state) transitions; ALLOCATED / RECYCLABLE are a
 *	no-op (defensive).  Idempotent (already-COMMITTED returns true).
 */
bool
cluster_undo_segment_mark_committed(uint32 segment_id, uint8 owner_instance)
{
	return cluster_undo_block0_current_live_owner_lifecycle_exact(
		segment_id, owner_instance, CLUSTER_UNDO_LIFECYCLE_MARK_COMMITTED, 0, 0);
}


/*
 * cluster_undo_segment_try_mark_recyclable -- spec-3.13 D3.
 *
 *	COMMITTED -> RECYCLABLE through the generation-independent 0xFB XCUR and
 *	exact resident content-X owner.  The folded horizon remains bound to the
 *	caller's expected epoch; no lifecycle/content lock may be held here.
 */
ClusterUndoSegTryRecycle
cluster_undo_segment_try_mark_recyclable(uint32 segment_id, uint8 owner_instance, SCN horizon,
										 uint64 expected_epoch)
{
	ClusterUndoBlock0LogicalKey key;
	ClusterUndoBlock0RecycleResult result;

	if (segment_id == 0 || owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX
		|| !SCN_VALID(horizon) || (expected_epoch == 0 && cluster_conf_node_count() != 4))
		return CLUSTER_SEG_RECYCLE_READ_FAIL;
	key.owner_instance = owner_instance;
	key.segment_id = segment_id;
	result = cluster_undo_block0_current_live_owner_recycle_exact(&key, horizon, expected_epoch,
																  10000);
	switch (result) {
	case CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED:
		return CLUSTER_SEG_RECYCLE_ADVANCED;
	case CLUSTER_UNDO_BLOCK0_RECYCLE_ALREADY:
		return CLUSTER_SEG_RECYCLE_ALREADY;
	case CLUSTER_UNDO_BLOCK0_RECYCLE_NOT_COMMITTED:
		return CLUSTER_SEG_RECYCLE_NOT_COMMITTED;
	case CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED:
		return CLUSTER_SEG_RECYCLE_RETAINED;
	case CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED:
	default:
		return CLUSTER_SEG_RECYCLE_READ_FAIL;
	}
}


/*
 * cluster_undo_segment_reuse_in_place -- spec-3.13 D4.
 *
 *	Whole-segment rebirth of a RECYCLABLE segment: exact fresh block-0 image
 *	(SEGMENT_ALLOCATED, TT slots/bitmap/reserved zero) at generation old+1,
 *	serialized by the existing 0xFB XCUR/current resident owner.  Data blocks
 *	are NOT zeroed (lazy overwrite; C-R3/C-R4: no
 *	live reader can legally dereference into a recyclable segment, and
 *	block headers carry first_change_scn for generation telling).
 *	Caller holds no lifecycle/content lock.  Returns the
 *	segment_id, or 0 on I/O failure (caller falls back to fresh create).
 */
uint32
cluster_undo_segment_reuse_in_place(uint32 segment_id, uint8 owner_instance, uint32 old_generation)
{
	PGAlignedBlock page;
	UndoSegmentHeaderData *hdr;
	ClusterUndoBlock0LogicalKey key;
	ClusterUndoBlock0Generation expected;
	ClusterUndoBlock0Result result;

	if (segment_id == 0 || old_generation == UINT32_MAX || owner_instance < 1
		|| owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return 0;

	cluster_undo_segment_make_header_bytes(segment_id, owner_instance, page.data);
	hdr = (UndoSegmentHeaderData *)page.data;
	hdr->wrap_count = old_generation + 1;
	key.owner_instance = owner_instance;
	key.segment_id = segment_id;
	expected.known = true;
	expected.value = old_generation;
	result = cluster_undo_block0_current_live_owner_reuse_exact(&key, &expected, page.data, 10000);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return 0;

	/*
	 * spec-3.18 D3.2 (review finding 3): drop the reborn segment's old-
	 * generation cached data blocks from the undo buffer pool -- the pool keys
	 * blocks by (segment, block) with no generation, so a stale slot would
	 * otherwise keep hitting the same key (recyclable + WAL-protected -> drop
	 * without flush).
	 */
	cluster_undo_buf_invalidate_segment(segment_id, owner_instance);

	cluster_undo_record_note_segment_reuse();

	return segment_id;
}


/*
 * cluster_undo_segment_generation -- spec-3.13 D4 accessor (3.18 D1 undo
 * buffer keying contract: cache keys must be (segment, generation)).
 *
 *	Returns the on-disk wrap_count, or 0 when the header is unreadable /
 *	fails identity (callers treat 0 as \"generation zero or unknown\").
 */
uint32
cluster_undo_segment_generation(uint32 segment_id, uint8 owner_instance)
{
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;

	if (!read_segment_header_via_smgr(segment_id, owner_instance, blockbuf.data, &hdr))
		return 0;
	if (!cluster_undo_segment_header_identity_ok(blockbuf.data, segment_id, owner_instance))
		return 0;
	return hdr->wrap_count;
}


/*
 * cluster_undo_segment_mark_full -- D1 set UNDO_SEGMENT_FLAG_FULL.
 *
 *	Called when free_block_bitmap shows segment exhausted.  Per spec
 *	§3.3 I2:  state remains ACTIVE — fullness is a flag,  not a state
 *	transition.  Idempotent.
 */
bool
cluster_undo_segment_mark_full(uint32 segment_id, uint8 owner_instance)
{
	return cluster_undo_block0_current_live_owner_lifecycle_exact(
		segment_id, owner_instance, CLUSTER_UNDO_LIFECYCLE_MARK_FULL, 0, 0);
}


/*
 * cluster_undo_segment_tail_block_init -- D5 initial tail_block setup.
 *
 *	Per Hardening v1.0.1 H-1:  spec terminology first_active_block ≡
 *	linkdb tail_block (offset 48,  retention base).  Called after
 *	ALLOCATED → ACTIVE transition to set initial retention base to
 *	block 1 (block 0 is segment header).  Single update;  later
 *	advances are driven by cleaner (spec-3.12).
 */
bool
cluster_undo_segment_tail_block_init(uint32 segment_id, uint8 owner_instance,
									 BlockNumber initial_tail)
{
	return cluster_undo_block0_current_live_owner_lifecycle_exact(
		segment_id, owner_instance, CLUSTER_UNDO_LIFECYCLE_INIT_TAIL, initial_tail, 0);
}


/*
 * D6 free_block_bitmap helpers
 *
 *   Per spec §3.7 free_block_bitmap layout: 1024 bytes at offset 1656
 *   of UndoSegmentHeaderData (8192 blocks / 8 bits-per-byte = 1024 B).
 *   Bit set = block is in-use;  bit clear = block free.  Block 0 (segment
 *   header) bit always set (reserved).
 *
 *   Per spec §3.1: is_full() returns true when no usable block left
 *   (free count <= 1 margin for safety).
 */
bool
cluster_undo_segment_mark_block_used(uint32 segment_id, uint8 owner_instance, uint32 block_no)
{
	return cluster_undo_block0_current_live_owner_lifecycle_exact(
		segment_id, owner_instance, CLUSTER_UNDO_LIFECYCLE_MARK_BLOCK, block_no, 0);
}


/*
 * cluster_undo_segment_mark_block_range_used -- spec-3.18 D3.2 (A1).
 *
 *	Mark a whole extent [first_block, first_block+nblocks) used in ONE block-0
 *	read + ONE write + fsync (vs N for the per-block helper).  Idempotent;
 *	range out of bounds -> false (fail-closed; caller must not proceed).  Caller
 *	holds lifecycle_lock so concurrent block-0 mutations are serialized.
 */
bool
cluster_undo_segment_mark_block_range_used(uint32 segment_id, uint8 owner_instance,
										   uint32 first_block, uint32 nblocks)
{
	return cluster_undo_block0_current_live_owner_lifecycle_exact(
		segment_id, owner_instance, CLUSTER_UNDO_LIFECYCLE_CLAIM_RANGE, first_block, nblocks);
}


/*
 * cluster_undo_segment_first_free_block -- spec-3.18 D3.2 (B1).
 *
 *	Reconstruct the extent high-water on restart: the first free data block in
 *	the segment's bitmap (>= 1), or 0 when the segment is full or the bitmap is
 *	corrupt (a used block after a free one -> the kernel fail-closes to 0).
 *	Caller treats 0 as "this segment cannot host a new extent" -> autoextend /
 *	rollback to a fresh segment.
 */
uint32
cluster_undo_segment_first_free_block(uint32 segment_id, uint8 owner_instance)
{
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;

	if (!read_segment_header_via_smgr(segment_id, owner_instance, blockbuf.data, &hdr))
		return 0;

	return UndoSegmentBitmap_first_free_block(hdr->free_block_bitmap, UNDO_BLOCKS_PER_SEGMENT);
}


bool
cluster_undo_segment_is_full(uint32 segment_id, uint8 owner_instance)
{
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;

	if (!read_segment_header_via_smgr(segment_id, owner_instance, blockbuf.data, &hdr))
		return false;

	/* Pure-kernel decision (covered by cluster_unit T15). */
	return UndoSegmentBitmap_is_full(hdr->free_block_bitmap, UNDO_FREE_BITMAP_BYTES);
}


/*
 * cluster_undo_segment_read_state -- spec-3.18 D3.2 (review finding P1-C).
 *
 *	Return the segment's on-disk lifecycle state, or SEGMENT_INVALID when the
 *	header is unreadable.  Used by the extent claim to tell a freshly-created
 *	(ALLOCATED) segment -- which needs tail_block initialized -- from a restart-
 *	resumed (already ACTIVE) one whose cleaner-advanced tail_block must NOT be
 *	reset to 1.
 */
uint8
cluster_undo_segment_read_state(uint32 segment_id, uint8 owner_instance)
{
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;

	if (!read_segment_header_via_smgr(segment_id, owner_instance, blockbuf.data, &hdr))
		return (uint8)SEGMENT_INVALID;

	return hdr->segment_state;
}


/* ============================================================
 * spec-3.8 D2: Autoextend lazy at exhaustion
 *
 *   API: cluster_undo_segment_extend_or_create()
 *
 *   Per Hardening v1.0.1 H-1/H-2:  use existing segment-id encoding
 *   ((owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + slot + 1).
 *   Scans slot 0..max-1 to find first unused slot (file doesn't exist
 *   OR header not yet initialized).
 *
 *   Hard cap source:  cluster.undo_segments_max_per_instance GUC
 *   (declared in cluster_guc.h, registered in Step 7).  Effective cap =
 *   min(GUC value, CLUSTER_UNDO_SEGS_PER_INSTANCE = 256 encoding limit).
 *
 *   Lock contract: caller holds lifecycle_lock (NOT cursor_lock).
 *   File I/O happens under lifecycle_lock per spec §3.2.
 * ============================================================ */

bool
cluster_undo_segment_extend_or_create(uint8 owner_instance, ClusterUndoSegmentExtendPlan *plan)
{
	uint32 slot;
	uint32 base_segment_id;
	uint32 effective_cap;

	if (plan == NULL)
		return false;
	memset(plan, 0, sizeof(*plan));

	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return false;

	/*
	 * effective_cap = min(GUC, encoding limit 256).  Per spec-3.8 §3.6 F6
	 * SIGHUP race-safe contract:  if reloaded GUC value < current pool
	 * size,  use current pool as effective floor + WARNING (no retro-shrink).
	 *
	 * Caller MUST hold lifecycle_lock per spec §3.2 — this probe happens
	 * within that lock so current_pool_size is non-stale.
	 */
	{
		/*
		 * cluster_undo_segments_max_per_instance is declared in
		 * cluster/cluster_guc.h (included above), so no local extern is
		 * needed here.  PGRAC: D2-2 added that include for
		 * cluster_undo_gcs_coherence; dropping the redundant local extern
		 * avoids a shadowed-declaration static-analysis finding.
		 */
		int guc_val = cluster_undo_segments_max_per_instance;
		uint32 current_pool_size = 0;
		uint32 probe_slot;

		if (guc_val <= 0 || guc_val > (int)CLUSTER_UNDO_SEGS_PER_INSTANCE)
			effective_cap = CLUSTER_UNDO_SEGS_PER_INSTANCE;
		else
			effective_cap = (uint32)guc_val;

		/* Probe current pool size by counting existing segment files. */
		for (probe_slot = 0; probe_slot < CLUSTER_UNDO_SEGS_PER_INSTANCE; probe_slot++) {
			char path[MAXPGPATH];
			int path_ret;
			int probe_fd;
			uint32 probe_segment_id
				= (uint32)(owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + probe_slot + 1;

			path_ret
				= cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner_instance),
											owner_instance, probe_segment_id, path, sizeof(path));
			if (path_ret != 0)
				break;

			probe_fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
			if (probe_fd < 0)
				break;
			close(probe_fd);
			current_pool_size++;
		}

		/* No retro-shrink:  if configured GUC < current pool size,
		 * use current pool as floor + WARNING per spec §3.6 + R9. */
		if (guc_val > 0 && (uint32)guc_val < current_pool_size) {
			ereport(WARNING,
					(errmsg("cluster.undo_segments_max_per_instance (%d) is less than "
							"current pool size (%u); using current pool size as effective floor",
							guc_val, current_pool_size),
					 errhint("Reload value will not retro-shrink existing pool.")));
			effective_cap = current_pool_size;
		}
	}

	/* segment_id space for this owner_instance:
	 *   [base_segment_id, base_segment_id + effective_cap)
	 *
	 * Per existing encoding (cluster_undo_active_segment_for_node_or_create):
	 *   slot 0 segment_id = (owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1
	 */
	base_segment_id = (uint32)(owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	/* Iterate slots — find first one where the file doesn't yet exist. */
	for (slot = 0; slot < effective_cap; slot++) {
		char path[MAXPGPATH];
		int ret;
		int fd;
		uint32 new_segment_id = base_segment_id + slot;

		ret = cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner_instance),
										owner_instance, new_segment_id, path, sizeof(path));
		if (ret != 0)
			continue;

		fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
		if (fd >= 0) {
			/*
			 * spec-3.13 D4 reuse-first: an existing segment whose header says
			 * SEGMENT_RECYCLABLE is reborn in place (generation+1) instead of
			 * consuming a fresh slot.  Identity-checked read (L212); any
			 * doubt falls through to \"slot is taken\" (fail-closed: never
			 * overwrite a segment we cannot positively classify).
			 */
			PGAlignedBlock peek;
			ssize_t rb = pg_pread(fd, peek.data, BLCKSZ, 0);

			close(fd);
			if (rb == BLCKSZ
				&& cluster_undo_segment_header_identity_ok(peek.data, new_segment_id,
														   owner_instance)) {
				UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)peek.data;

				if (header->segment_state == SEGMENT_RECYCLABLE) {
					plan->segment_id = new_segment_id;
					plan->generation = header->wrap_count;
					plan->needs_reuse = true;
					return true;
				}
				if (header->segment_state == SEGMENT_ALLOCATED) {
					TTSlot empty_slots[TT_SLOTS_PER_SEGMENT] = { { 0 } };

					/* The state byte alone cannot authorize FREE/wrap0. A
					 * previous incarnation may have bound a TT before its first
					 * DATA write. Preserve any occupied slot, including ACTIVE. */
					if (memcmp(header->tt_slots, empty_slots, sizeof(empty_slots)) == 0) {
						plan->segment_id = new_segment_id;
						plan->generation = header->wrap_count;
						plan->needs_reuse = false;
						return true;
					}
				}
			}
			/* File exists — slot is taken. */
			continue;
		}

		if (errno != ENOENT) {
			/* Real I/O error reading slot N — abort autoextend. */
			return false;
		}

		/* Freeze the absent candidate only. Its caller must release the
		 * lifecycle lock before taking current authority and publishing. */
		plan->segment_id = new_segment_id;
		plan->generation = 0;
		plan->needs_reuse = false;
		plan->needs_provision = true;
		return true;
	}

	/* All slots taken — hard cap reached. */
	plan->at_hard_cap = true;
	return false;
}


/*
 * cluster_undo_segment_scan_max_existing -- spec-3.8 restart scan helper.
 *
 *	Scans owner_instance's segment pool to find the highest segment_id
 *	that has an existing file on disk.  Used by cluster_undo_record.c
 *	at post-restart first claim to resume to the most-recent active
 *	segment rather than going back to segment_id = 1.
 *
 *	Returns: highest segment_id whose file exists;  0 if no files
 *	(fresh init).  Caller MUST hold lifecycle_lock per spec §3.2.
 */
uint32
cluster_undo_segment_scan_max_existing(uint8 owner_instance)
{
	uint32 base_segment_id;
	uint32 slot;
	uint32 max_found = 0;

	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return 0;

	base_segment_id = (uint32)(owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	for (slot = 0; slot < CLUSTER_UNDO_SEGS_PER_INSTANCE; slot++) {
		char path[MAXPGPATH];
		int ret;
		int fd;
		uint32 probe_id = base_segment_id + slot;

		ret = cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner_instance),
										owner_instance, probe_id, path, sizeof(path));
		if (ret != 0)
			break;

		fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			break; /* gap or end of pool */
		close(fd);

		max_found = probe_id;
	}

	return max_found;
}


/*
 * cluster_undo_segment_file_exists -- spec-3.22 lock-free existence probe.
 *
 *	Resolves the segment path and checks access(F_OK).  Lock-free and side-effect
 *	free: existence is monotone (an allocated segment file is never deleted, only
 *	recycled in place), so the by-xid resolve can use this to distinguish an
 *	absent segment (sound scan skip) from an existing-but-unreadable one
 *	(incomplete scan -> SCAN_UNAVAILABLE).  Returns false on a bad path / range.
 */
bool
cluster_undo_segment_file_exists(uint8 owner_instance, uint32 segment_id)
{
	char path[MAXPGPATH];

	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return false;
	if (cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner_instance), owner_instance,
								  segment_id, path, sizeof(path))
		!= 0)
		return false;
	return access(path, F_OK) == 0;
}


/*
 * cluster_undo_segment_scan_resumable_active -- spec-3.18 D3.2 (review finding
 * 2).  Restart resume must pick the segment that is actually the live active
 * one, NOT merely the highest-numbered existing file:  reuse-first
 * (cluster_undo_segment_reuse_first_recyclable) can make the active segment a
 * LOW-numbered reborn slot while a HIGH-numbered one sits COMMITTED /
 * RECYCLABLE / ACTIVE-FULL.  Scan the headers and return the segment whose
 * state is SEGMENT_ACTIVE and whose FULL flag is clear -- the unique writable
 * active segment.  Returns 0 when none is resumable (caller uses the
 * ensured/created segment + autoextend), and fail-closes to 0 if it finds more
 * than one writable-active segment (the allocator keeps exactly one;  two means
 * a corrupt/ambiguous pool -- don't guess, force a fresh claim).
 */
uint32
cluster_undo_segment_scan_resumable_active(uint8 owner_instance)
{
	uint32 base_segment_id;
	uint32 slot;
	uint32 found = 0;

	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return 0;

	base_segment_id = (uint32)(owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	for (slot = 0; slot < CLUSTER_UNDO_SEGS_PER_INSTANCE; slot++) {
		PGAlignedBlock blockbuf;
		UndoSegmentHeaderData *hdr;
		uint32 probe_id = base_segment_id + slot;

		if (!read_segment_header_via_smgr(probe_id, owner_instance, blockbuf.data, &hdr))
			break; /* gap or end of pool (file absent / unreadable) */

		if (hdr->segment_state == SEGMENT_ACTIVE
			&& (hdr->segment_flags & UNDO_SEGMENT_FLAG_FULL) == 0) {
			if (found != 0)
				return 0; /* >1 writable-active -> ambiguous, fail-closed */
			found = probe_id;
		}
	}

	return found;
}


/* Read-only full-pool observation.  Unlike first-gap allocator probes, this
 * scans every native slot so restart reconstruction remains exact across gaps. */
ClusterUndoPoolObservationResult
cluster_undo_segment_observe_pool(uint8 owner_instance, ClusterUndoPoolObservation *out)
{
	uint32 base_segment_id;
	uint32 slot;
	uint32 allocated = 0;
	uint32 configured_cap;
	int guc_val = cluster_undo_segments_max_per_instance;

	if (out == NULL)
		return CLUSTER_UNDO_POOL_OBS_INVALID_OWNER;
	out->allocated_count = 0;
	out->configured_cap = 0;
	out->effective_cap = 0;
	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return CLUSTER_UNDO_POOL_OBS_INVALID_OWNER;

	base_segment_id = (uint32)(owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	for (slot = 0; slot < CLUSTER_UNDO_SEGS_PER_INSTANCE; slot++) {
		PGAlignedBlock blockbuf;
		char path[MAXPGPATH];
		uint32 segment_id = base_segment_id + slot;
		ssize_t nread;
		int fd;

		if (cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner_instance), owner_instance,
									  segment_id, path, sizeof(path))
			!= 0)
			return CLUSTER_UNDO_POOL_OBS_IO_ERROR;

		fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0) {
			if (errno == ENOENT)
				continue;
			return CLUSTER_UNDO_POOL_OBS_IO_ERROR;
		}
		nread = read(fd, blockbuf.data, BLCKSZ);
		close(fd);
		if (nread < 0)
			return CLUSTER_UNDO_POOL_OBS_IO_ERROR;
		if (nread != BLCKSZ
			|| !cluster_undo_segment_header_identity_ok(blockbuf.data, segment_id, owner_instance))
			return CLUSTER_UNDO_POOL_OBS_INVALID_HEADER;
		allocated++;
	}

	if (guc_val < 16)
		configured_cap = 16;
	else if (guc_val > (int)CLUSTER_UNDO_SEGS_PER_INSTANCE)
		configured_cap = CLUSTER_UNDO_SEGS_PER_INSTANCE;
	else
		configured_cap = (uint32)guc_val;

	out->allocated_count = allocated;
	out->configured_cap = configured_cap;
	out->effective_cap = Max(configured_cap, allocated);
	return CLUSTER_UNDO_POOL_OBS_OK;
}
