/*-------------------------------------------------------------------------
 *
 * cluster_ges.c
 *	  GES (Global Enqueue Service) request protocol skeleton — spec-2.13.
 *
 *	  Implements the 2 ICMsgType handler stubs (GES_REQUEST=4 /
 *	  GES_REPLY=5), 2 atomic defer counters, 2 read accessors, and the
 *	  cluster_ges shmem region lifecycle.
 *
 *	  See cluster_ges.h for the protocol contract and skeleton scope.
 *	  See spec-2.13-ges-request-protocol-skeleton.md (frozen v0.2) for
 *	  design rationale.
 *
 *	  AD-002 PCM vs GES 分工 + AD-011 不移植 LC/RC Lock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Skeleton phase: 2 handler stubs永远 DEFER (no state change beyond
 *	  the atomic counter bump);  caller-side (spec-2.14+) replaces the
 *	  DEFER body with real routing / grant / convert logic.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ges.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/* ============================================================
 * Shmem region state (Q3.1=B independent region).
 * ============================================================ */

static ClusterGesSharedState *cluster_ges_state = NULL;


/* ============================================================
 * Shmem region lifecycle (mirror cluster_scn pattern).
 * ============================================================ */

Size
cluster_ges_shmem_size(void)
{
	return sizeof(ClusterGesSharedState);
}

void
cluster_ges_shmem_init(void)
{
	bool found;

	cluster_ges_state = ShmemInitStruct("pgrac cluster ges", cluster_ges_shmem_size(), &found);
	if (!found) {
		/* spec-2.13 D3 init zero (Q4.1=A all-atomic, no LWLock). */
		pg_atomic_init_u64(&cluster_ges_state->request_defer_count, 0);
		pg_atomic_init_u64(&cluster_ges_state->reply_defer_count, 0);
	}
}

static const ClusterShmemRegion cluster_ges_region = {
	.name = "pgrac cluster ges",
	.size_fn = cluster_ges_shmem_size,
	.init_fn = cluster_ges_shmem_init,
	.lwlock_count = 0, /* spec-2.13: lock-free (L106 inherit) */
	.owner_subsys = "cluster_ges",
	.reserved_flags = 0,
};

void
cluster_ges_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ges_region);
}


/* ============================================================
 * Handler stubs (spec-2.13 D2).
 *
 *	Skeleton phase: increment defer counter + DEBUG2 log + return.
 *	NO state change beyond the atomic counter (caller MUST fall back
 *	to PG-native lock manager when stub fires).
 *
 *	Handler 4 硬约束 (spec-2.3 Q6) enforced by this stub body:
 *	  (1) no block / no LWLock wait — atomic only;
 *	  (2) no catalog SQL — bare counter bump + elog;
 *	  (3) no ERROR / FATAL — only Assert (debug-build only) + DEBUG2;
 *	  (4) bounded shmem only — single 8-byte field touched.
 * ============================================================ */

void
cluster_ges_request_handler(const ClusterICEnvelope *env, const void *payload pg_attribute_unused())
{
	Assert(env != NULL);
	Assert(cluster_ges_state != NULL);

	pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);

	ereport(DEBUG2, (errmsg_internal("cluster_ges: GES_REQUEST received from "
									 "peer %u; skeleton DEFER (spec-2.16 真激活)",
									 env->source_node_id)));
}

void
cluster_ges_reply_handler(const ClusterICEnvelope *env, const void *payload pg_attribute_unused())
{
	Assert(env != NULL);
	Assert(cluster_ges_state != NULL);

	pg_atomic_fetch_add_u64(&cluster_ges_state->reply_defer_count, 1);

	ereport(DEBUG2, (errmsg_internal("cluster_ges: GES_REPLY received from "
									 "peer %u; skeleton DEFER (spec-2.16 真激活)",
									 env->source_node_id)));
}


/* ============================================================
 * Counter accessors (spec-2.13 D4).
 *
 *	Used by cluster_debug emit_row to surface counters in
 *	pg_cluster_state (category='ges').
 * ============================================================ */

uint64
cluster_ges_request_defer_count(void)
{
	Assert(cluster_ges_state != NULL);
	return pg_atomic_read_u64(&cluster_ges_state->request_defer_count);
}

uint64
cluster_ges_reply_defer_count(void)
{
	Assert(cluster_ges_state != NULL);
	return pg_atomic_read_u64(&cluster_ges_state->reply_defer_count);
}
