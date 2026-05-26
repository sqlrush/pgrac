/*-------------------------------------------------------------------------
 *
 * cluster_subtrans.c
 *	  pgrac SUBTRANS cross-node visibility — implementation.
 *
 *	  spec-3.5 D5 (NEW;Stage 3 第 9 sub-spec).
 *
 *	  See cluster_subtrans.h for the public contract.  This file
 *	  implements eager origin-side emit (subcommit / subabort /
 *	  ensure_parent_binding) and reader-side lazy parent follow.
 *
 *	  All entry points L195 single-node fast path (cluster_conf_has_peers
 *	  false → no-op).  Recursive lookup_parent bounded by
 *	  cluster.subtrans_max_chain_depth (HC205).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.5-subtrans-cross-node-visibility.md (v0.3 FROZEN 2026-05-26)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_subtrans.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_elog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * Module-local counters.  Shmem-resident so cross-backend aggregation is
 * possible from pg_cluster_state (spec-3.4e D6 pattern).
 */
typedef struct ClusterSubtransShmem
{
	pg_atomic_uint64 chain_depth_exceeded_count;
	pg_atomic_uint64 xact_has_state_check_count;
} ClusterSubtransShmem;

static ClusterSubtransShmem *ClusterSubtransState = NULL;

/* ------------------------------------------------------------ */
/* shmem registration                                           */
/* ------------------------------------------------------------ */

Size
cluster_subtrans_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(sizeof(ClusterSubtransShmem));
}

void
cluster_subtrans_shmem_init(void)
{
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ClusterSubtransState = (ClusterSubtransShmem *) ShmemInitStruct(
		"ClusterSubtransState", MAXALIGN(sizeof(ClusterSubtransShmem)), &found);
	if (!found)
	{
		pg_atomic_init_u64(&ClusterSubtransState->chain_depth_exceeded_count, 0);
		pg_atomic_init_u64(&ClusterSubtransState->xact_has_state_check_count, 0);
	}
}

static const ClusterShmemRegion cluster_subtrans_region = {
	.name = "pgrac cluster subtrans state",
	.size_fn = cluster_subtrans_shmem_size,
	.init_fn = cluster_subtrans_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_subtrans",
	.reserved_flags = 0,
};

void
cluster_subtrans_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_subtrans_region);
}

/* ------------------------------------------------------------ */
/* private helpers                                              */
/* ------------------------------------------------------------ */

/*
 * Build an exact ClusterTTStatusKey for a local (this-node) xid via the
 * existing TT-local binding machinery.  Used both for child and parent
 * xids — raw xid alone is unsafe (HC180 + L203 reasoning).
 */
static bool
build_key_for_xid(TransactionId xid, ClusterTTStatusKey *out)
{
	uint32 seg;
	uint16 off;
	uint32 tt_id;

	memset(out, 0, sizeof(*out));

	if (!cluster_enabled || cluster_node_id < 0)
		return false;
	if (!TransactionIdIsNormal(xid))
		return false;

	/*
	 * Get-or-create binding:  ensures that a TT slot is reserved for this
	 * xid on this node.  ensure_parent_binding is the caller for the
	 * parent path;  subcommit emit also relies on the child having a
	 * binding already (heap DML created it).
	 */
	if (!cluster_tt_local_get_or_create_binding(xid, &seg, &off, &tt_id))
		return false;

	out->origin_node_id = (uint16) cluster_node_id;
	out->undo_segment_id = (uint16) seg;
	out->tt_slot_id = tt_id;
	out->cluster_epoch = (uint32) cluster_epoch_get_current();
	out->local_xid = xid;
	return true;
}

/* ------------------------------------------------------------ */
/* public API                                                   */
/* ------------------------------------------------------------ */

bool
cluster_subtrans_ensure_parent_binding(TransactionId		parent_xid,
									   ClusterTTStatusKey * parent_key_out)
{
	ClusterTTStatusKey key;

	if (parent_key_out == NULL)
		return false;
	memset(parent_key_out, 0, sizeof(*parent_key_out));

	if (!cluster_enabled || !cluster_conf_has_peers())
		return false;
	if (!TransactionIdIsNormal(parent_xid))
		return false;

	if (!build_key_for_xid(parent_xid, &key))
		return false;

	/*
	 * Install parent as IN_PROGRESS in the local overlay (HC203).  This
	 * makes sure remote readers performing lazy follow find an overlay
	 * entry rather than missing through to 53R97 prematurely.  Idempotent:
	 * install_local overwrites existing entry without bumping eviction.
	 */
	cluster_tt_status_install_local(&key, CLUSTER_TT_STATUS_IN_PROGRESS, InvalidScn);

	*parent_key_out = key;
	return true;
}

bool
cluster_subtrans_emit_subcommit(TransactionId child_xid, TransactionId parent_xid)
{
	ClusterTTStatusKey child_key;
	ClusterTTStatusKey parent_key;

	if (!cluster_enabled || !cluster_conf_has_peers())
		return false;
	if (!TransactionIdIsNormal(child_xid) || !TransactionIdIsNormal(parent_xid))
		return false;

	if (!cluster_subtrans_ensure_parent_binding(parent_xid, &parent_key))
		return false;
	if (!build_key_for_xid(child_xid, &child_key))
		return false;

	/* Install local SUBCOMMITTED + emit V3 hint to peers. */
	if (!cluster_tt_status_install_subcommitted(&child_key, &parent_key))
		return false;

	cluster_tt_status_hint_emit_subcommitted(&child_key, &parent_key);
	return true;
}

bool
cluster_subtrans_emit_subabort(TransactionId child_xid)
{
	ClusterTTStatusKey child_key;

	if (!cluster_enabled || !cluster_conf_has_peers())
		return false;
	if (!TransactionIdIsNormal(child_xid))
		return false;

	if (!build_key_for_xid(child_xid, &child_key))
		return false;

	/*
	 * ABORTED uses the existing V2 emit path (commit_scn=InvalidScn).
	 * install_local covers the local overlay entry.
	 */
	cluster_tt_status_install_local(&child_key, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
	cluster_tt_status_hint_emit(&child_key, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
	return true;
}

ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child_result, int depth_remaining)
{
	ClusterTTStatusResult cur;
	ClusterTTStatusKey next_key;
	int budget;

	memset(&cur, 0, sizeof(cur));

	if (child_result == NULL)
	{
		cur.status = CLUSTER_TT_STATUS_UNKNOWN;
		cur.authoritative = false;
		return cur;
	}

	/*
	 * If the result we were handed isn't SUBCOMMITTED, no follow needed.
	 * Returning a copy keeps caller paths uniform.
	 */
	cur = *child_result;
	if (cur.status != CLUSTER_TT_STATUS_SUBCOMMITTED || !cur.has_parent_key)
		return cur;

	budget = (depth_remaining > 0) ? depth_remaining : cluster_subtrans_max_chain_depth;
	if (budget <= 0)
		budget = 32; /* defensive */

	next_key = cur.parent_key;

	while (budget-- > 0)
	{
		ClusterTTStatusResult parent_res;

		if (!cluster_tt_status_lookup_exact(&next_key, &parent_res))
		{
			/*
			 * parent overlay miss → caller must fail-closed (53R97 per
			 * L199).  Return UNKNOWN authoritative=false.
			 */
			memset(&cur, 0, sizeof(cur));
			cur.status = CLUSTER_TT_STATUS_UNKNOWN;
			cur.authoritative = false;
			return cur;
		}

		if (ClusterSubtransState != NULL)
			pg_atomic_fetch_add_u64(&ClusterSubtransState->chain_depth_exceeded_count, 0);

		if (parent_res.status != CLUSTER_TT_STATUS_SUBCOMMITTED)
			return parent_res;

		/* Continue follow up the chain. */
		if (!parent_res.has_parent_key)
		{
			memset(&cur, 0, sizeof(cur));
			cur.status = CLUSTER_TT_STATUS_UNKNOWN;
			cur.authoritative = false;
			return cur;
		}
		next_key = parent_res.parent_key;
	}

	/* Depth exceeded → fail-closed UNKNOWN. */
	if (ClusterSubtransState != NULL)
		pg_atomic_fetch_add_u64(&ClusterSubtransState->chain_depth_exceeded_count, 1);
	memset(&cur, 0, sizeof(cur));
	cur.status = CLUSTER_TT_STATUS_UNKNOWN;
	cur.authoritative = false;
	return cur;
}

bool
cluster_subtrans_xact_has_state(TransactionId top_xid)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult res;

	if (!cluster_enabled || !cluster_conf_has_peers())
		return false;
	if (!TransactionIdIsNormal(top_xid))
		return false;

	if (ClusterSubtransState != NULL)
		pg_atomic_fetch_add_u64(&ClusterSubtransState->xact_has_state_check_count, 1);

	/*
	 * Build the canonical key from the local TT-local binding (peek; do
	 * not allocate new binding from a probing call).  If there is no
	 * binding at all, the xact has never stamped a cluster overlay entry
	 * — guard short-circuits to false.
	 */
	{
		uint32 seg;
		uint16 off;
		uint32 tt_id;
		uint32 epoch;

		if (!cluster_tt_local_peek_binding(top_xid, &seg, &off, &tt_id, &epoch))
			return false;
		memset(&key, 0, sizeof(key));
		key.origin_node_id = (uint16) cluster_node_id;
		key.undo_segment_id = (uint16) seg;
		key.tt_slot_id = tt_id;
		key.cluster_epoch = epoch;
		key.local_xid = top_xid;
	}

	if (!cluster_tt_status_lookup_exact(&key, &res))
		return false;

	/*
	 * Any installed overlay state for this xid counts as "has state":
	 * IN_PROGRESS (parent placeholder), SUBCOMMITTED, COMMITTED, ABORTED,
	 * CLEANED_OUT.  PREPARE TRANSACTION guard (D10) treats any of these
	 * as a positive signal -- the wire fan-out has already propagated
	 * something the cluster reader needs the spec to finalize.
	 */
	return res.authoritative;
}

uint64
cluster_subtrans_get_chain_depth_exceeded_count(void)
{
	if (ClusterSubtransState == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterSubtransState->chain_depth_exceeded_count);
}

uint64
cluster_subtrans_get_xact_has_state_check_count(void)
{
	if (ClusterSubtransState == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterSubtransState->xact_has_state_check_count);
}

#else /* !USE_PGRAC_CLUSTER */

bool
cluster_subtrans_emit_subcommit(TransactionId child_xid, TransactionId parent_xid)
{
	(void) child_xid;
	(void) parent_xid;
	return false;
}

bool
cluster_subtrans_emit_subabort(TransactionId child_xid)
{
	(void) child_xid;
	return false;
}

ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child_result, int depth_remaining)
{
	ClusterTTStatusResult r;

	(void) depth_remaining;
	memset(&r, 0, sizeof(r));
	if (child_result != NULL)
		r = *child_result;
	return r;
}

bool
cluster_subtrans_xact_has_state(TransactionId top_xid)
{
	(void) top_xid;
	return false;
}

bool
cluster_subtrans_ensure_parent_binding(TransactionId parent_xid,
									   ClusterTTStatusKey *parent_key_out)
{
	(void) parent_xid;
	if (parent_key_out != NULL)
		memset(parent_key_out, 0, sizeof(*parent_key_out));
	return false;
}

uint64
cluster_subtrans_get_chain_depth_exceeded_count(void)
{
	return 0;
}

uint64
cluster_subtrans_get_xact_has_state_check_count(void)
{
	return 0;
}

#endif /* USE_PGRAC_CLUSTER */
