/*-------------------------------------------------------------------------
 *
 * cluster_xnode_profile.c
 *	  Cross-node performance profiling accumulators (spec-5.59 D1).
 *
 *	  Owns the "pgrac cluster xnode profile" shmem region: one
 *	  {total_nanos, n_events} accumulator per ClusterXnodeBucket, a
 *	  reset generation, the read-side amortization probe counters
 *	  (reship vs S-holder hit), and the HW-extend master-locality
 *	  split.  All mutators are gated on the cluster.xnode_profile GUC
 *	  (default off) and are pure atomic adds -- no locks, no ereport,
 *	  so they are safe from any instrumented hot path.
 *
 *	  The SQL surface is the existing pg_cluster_state SRF: cluster_debug.c
 *	  emits one 'xnode_profile' category row per key (no new pg_proc, no
 *	  catversion bump).  Reset has no SQL surface either: cluster_xp_reset()
 *	  is an internal C API driven by cluster_unit; the perf runner uses
 *	  fresh-boot + reset_generation deltas instead.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xnode_profile.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.59-two-node-crossnode-perf-profile.md (D1, §2.2 / §2.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/buf_internals.h"
#include "storage/shmem.h"

ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

/*
 * Dump key stems, one per bucket.  Keep in sync with ClusterXnodeBucket
 * (unit test U1 walks the enum and asserts a non-NULL, unique name for
 * every bucket).
 */
static const char *const cluster_xp_bucket_names[CLXP_NBUCKETS] = {
	[CLXP_W_GCS_X_REQUEST] = "w_gcs_x_request",
	[CLXP_W_GCS_X_RECEIVE] = "w_gcs_x_receive",
	[CLXP_W_GCS_X_INSTALL] = "w_gcs_x_install",
	[CLXP_W_GCS_X_INVALIDATE] = "w_gcs_x_invalidate",
	[CLXP_W_GES_ENQUEUE] = "w_ges_enqueue",
	[CLXP_W_GES_CONVERT] = "w_ges_convert",
	[CLXP_W_GES_WAIT] = "w_ges_wait",
	[CLXP_W_GES_WAKE] = "w_ges_wake",
	[CLXP_W_HW_EXTEND] = "w_hw_extend",
	[CLXP_R_GCS_S_REQUEST] = "r_gcs_s_request",
	[CLXP_R_GCS_S_RECEIVE] = "r_gcs_s_receive",
	[CLXP_R_READIMAGE_SHIP] = "r_readimage_ship",
	[CLXP_R_SHOLDER_CACHE_HIT] = "r_sholder_cache_hit",
	[CLXP_R_CR_CONSTRUCT] = "r_cr_construct",
	[CLXP_R_CR_CHAIN_WALK] = "r_cr_chain_walk",
	[CLXP_R_TT_VISIBILITY_RESOLVE] = "r_tt_visibility_resolve",
	[CLXP_I_INDEX_BLOCK_XFER] = "i_index_block_xfer",
	[CLXP_I_RIGHTMOST_LEAF_PING] = "i_rightmost_leaf_ping",
	[CLXP_C_SCN_COMMIT_ADVANCE] = "c_scn_commit_advance",
	[CLXP_C_SCN_BOC_BROADCAST] = "c_scn_boc_broadcast",
	[CLXP_C_COMMIT_UNDO_FLUSH] = "c_commit_undo_flush",
	[CLXP_C_COMMIT_ITL_STAMP] = "c_commit_itl_stamp",
	[CLXP_C_COMMIT_TT_STAMP] = "c_commit_tt_stamp",
	[CLXP_C_COMMIT_WAL_FLUSH] = "c_commit_wal_flush",
	[CLXP_C_COMMIT_QUORUM_READ] = "c_commit_quorum_read",
	[CLXP_IC_SEND_SERVICE] = "ic_send_service",
	[CLXP_IC_INBOUND_DISPATCH] = "ic_inbound_dispatch",
	[CLXP_LOCAL_UNDO_ITL_WAL] = "local_undo_itl_wal",
};

const char *
cluster_xp_bucket_name(ClusterXnodeBucket b)
{
	if ((int)b < 0 || b >= CLXP_NBUCKETS)
		return NULL;
	return cluster_xp_bucket_names[b];
}

/*
 * spec-7.4 D4 commit-latency histogram support: the μs upper edges (derived
 * from the single-source CLXP_HIST_EDGES_US macro so the classifier and the
 * dump labels never drift) and the component names the dump uses to build
 * "hist.<component>.le_<edge>us" / ".le_inf" keys.
 */
const uint32 cluster_xp_hist_edge_us[CLXP_HIST_NEDGES] = { CLXP_HIST_EDGES_US };

static const char *const cluster_xp_hist_component_names[CLXP_HIST_NCOMPONENTS] = {
	[CLXP_HIST_UNDO_FLUSH] = "commit_undo_flush",
	[CLXP_HIST_ITL_STAMP] = "commit_itl_stamp",
	[CLXP_HIST_TT_STAMP] = "commit_tt_stamp",
	[CLXP_HIST_WAL_FLUSH] = "commit_wal_flush",
	[CLXP_HIST_SCN_COMMIT_ADVANCE] = "commit_scn_advance",
};

const char *
cluster_xp_hist_component_name(ClusterXpHistComponent c)
{
	if ((int)c < 0 || c >= CLXP_HIST_NCOMPONENTS)
		return NULL;
	return cluster_xp_hist_component_names[c];
}

/* ============================================================
 * Shmem region.
 * ============================================================ */

Size
cluster_xnode_profile_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterXnodeProfileShared));
}

static void
cluster_xp_zero_all(ClusterXnodeProfileShared *ctl, bool init)
{
	int i;

	for (i = 0; i < CLXP_NBUCKETS; i++) {
		if (init) {
			pg_atomic_init_u64(&ctl->bucket[i].total_nanos, 0);
			pg_atomic_init_u64(&ctl->bucket[i].n_events, 0);
		} else {
			pg_atomic_write_u64(&ctl->bucket[i].total_nanos, 0);
			pg_atomic_write_u64(&ctl->bucket[i].n_events, 0);
		}
	}
	/* spec-7.4 D4: zero the per-commit-component μs latency histogram. */
	{
		int c;
		int b;

		for (c = 0; c < CLXP_HIST_NCOMPONENTS; c++) {
			for (b = 0; b < CLXP_HIST_NBUCKETS; b++) {
				if (init)
					pg_atomic_init_u64(&ctl->hist[c][b], 0);
				else
					pg_atomic_write_u64(&ctl->hist[c][b], 0);
			}
		}
	}
	if (init) {
		pg_atomic_init_u64(&ctl->reset_generation, 0);
		pg_atomic_init_u64(&ctl->read_reship_count, 0);
		pg_atomic_init_u64(&ctl->read_sholder_hit_count, 0);
		pg_atomic_init_u64(&ctl->hw_extend_local_count, 0);
		pg_atomic_init_u64(&ctl->hw_extend_remote_count, 0);
	} else {
		/* reset_generation is bumped by the caller, never zeroed */
		pg_atomic_write_u64(&ctl->read_reship_count, 0);
		pg_atomic_write_u64(&ctl->read_sholder_hit_count, 0);
		pg_atomic_write_u64(&ctl->hw_extend_local_count, 0);
		pg_atomic_write_u64(&ctl->hw_extend_remote_count, 0);
	}
}

void
cluster_xnode_profile_shmem_init(void)
{
	bool found;

	ClusterXnodeProfileCtl = (ClusterXnodeProfileShared *)ShmemInitStruct(
		"pgrac cluster xnode profile", cluster_xnode_profile_shmem_size(), &found);
	if (!found)
		cluster_xp_zero_all(ClusterXnodeProfileCtl, true);
}

static const ClusterShmemRegion cluster_xnode_profile_region = {
	.name = "pgrac cluster xnode profile",
	.size_fn = cluster_xnode_profile_shmem_size,
	.init_fn = cluster_xnode_profile_shmem_init,
	.lwlock_count = 0, /* atomics only */
	.owner_subsys = "cluster_xnode_profile",
	.reserved_flags = 0,
};

void
cluster_xnode_profile_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_xnode_profile_region);
}

/* ============================================================
 * Probe mutators (all GUC-gated, atomic-add only).
 * ============================================================ */

/*
 * cluster_xp_note_read -- read-side amortization probe (spec-5.59 §3.6).
 *
 *	Called once per cross-node read on the requester: was_reship means the
 *	read was served by a full read-image ship (no cached copy); false means
 *	it was served from a registered S-holder copy.
 */
void
cluster_xp_note_read(bool was_reship)
{
	if (likely(!cluster_xnode_profile_enabled) || ClusterXnodeProfileCtl == NULL)
		return;
	if (was_reship)
		pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->read_reship_count, 1);
	else
		pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->read_sholder_hit_count, 1);
}

/*
 * cluster_xp_note_hw_extend -- HW extend master-locality split (D2).
 */
void
cluster_xp_note_hw_extend(bool remote_master)
{
	if (likely(!cluster_xnode_profile_enabled) || ClusterXnodeProfileCtl == NULL)
		return;
	if (remote_master)
		pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->hw_extend_remote_count, 1);
	else
		pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->hw_extend_local_count, 1);
}

/*
 * cluster_xp_count -- count-only event (no timing), e.g. the optional
 *	nbtree rightmost-leaf ping probe.
 */
void
cluster_xp_count(ClusterXnodeBucket b)
{
	if (likely(!cluster_xnode_profile_enabled) || ClusterXnodeProfileCtl == NULL)
		return;
	Assert((int)b >= 0 && b < CLXP_NBUCKETS);
	pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->bucket[b].n_events, 1);
}

/* ============================================================
 * Relation-kind hint (D4 index axis; backend-local, profiling-only).
 * ============================================================ */

static BufferTag xp_relkind_hint_tag;
static bool xp_relkind_hint_is_index = false;
static bool xp_relkind_hint_valid = false;

/*
 * cluster_xp_relkind_hint_set -- caller (bufmgr, which has the Relation)
 *	tags the buffer it is about to read.  Consumed by exact-tag match in
 *	the GCS block layer; attribution is approximate by design (a
 *	profiling dimension, never correctness-bearing).
 */
void
cluster_xp_relkind_hint_set(const struct buftag *tag, bool is_index)
{
	if (likely(!cluster_xnode_profile_enabled)) {
		xp_relkind_hint_valid = false;
		return;
	}
	xp_relkind_hint_tag = *(const BufferTag *)tag;
	xp_relkind_hint_is_index = is_index;
	xp_relkind_hint_valid = true;
}

bool
cluster_xp_relkind_hint_is_index_for(const struct buftag *tag)
{
	if (!xp_relkind_hint_valid)
		return false;
	return xp_relkind_hint_is_index
		   && BufferTagsEqual(&xp_relkind_hint_tag, (const BufferTag *)tag);
}

/*
 * cluster_xp_reset -- zero every accumulator and bump reset_generation.
 *
 *	Internal C API (cluster_unit U3); no SQL surface.  Not GUC-gated so a
 *	test can reset regardless of the profiling switch.
 */
void
cluster_xp_reset(void)
{
	if (ClusterXnodeProfileCtl == NULL)
		return;
	cluster_xp_zero_all(ClusterXnodeProfileCtl, false);
	pg_atomic_fetch_add_u64(&ClusterXnodeProfileCtl->reset_generation, 1);
}
