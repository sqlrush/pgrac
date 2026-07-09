/*-------------------------------------------------------------------------
 *
 * cluster_conf.h
 *	  pgrac cluster topology configuration (Stage 0.19).
 *
 *	  This header declares the in-memory representation of the cluster
 *	  topology -- the list of nodes that make up the cluster -- and the
 *	  loader / accessor API.  Source of truth at startup is the
 *	  pgrac.conf file (path from cluster.config_file GUC); the loader
 *	  parses it once and writes the result into the shmem-backed
 *	  ClusterConfShmem control block, which all backends share.
 *
 *	  Stage 0.19 is the framework only: it ships a parser that handles
 *	  the file when present, falls back to a single-node degraded
 *	  topology when absent (so existing single-node tests keep
 *	  passing), and exposes the result via the pg_cluster_nodes SQL
 *	  view.  Real readers -- Interconnect / Heartbeat / Reconfig /
 *	  Recovery Coordinator / GES master selection -- arrive in
 *	  Stage 2+ and consume ClusterConfShmem unchanged.
 *
 *	  See docs/cluster-conf-design.md for the full design rationale and
 *	  Stage evolution path; specs/spec-0.19-conf-framework.md for the
 *	  stage-0 scope and exit criteria.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_conf.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  cluster_conf_* / ClusterConf* / cluster_get_nodes (the C-level
 *	  surface) are available only when configured with --enable-cluster
 *	  (USE_PGRAC_CLUSTER defined); call sites must be guarded.  The
 *	  cluster_get_nodes SRF function symbol is provided unconditionally
 *	  (pg_proc.dat references it from both build modes; see the body
 *	  in cluster_conf.c which #ifdefs the implementation and returns
 *	  zero rows in disable-cluster builds).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONF_H
#define CLUSTER_CONF_H

#include "fmgr.h"


/*
 * Hard upper bound on the cluster size.  Anchored to AD-012 example 10
 * which fixes the cluster.node_id range to 0..127, so the topology
 * table must accommodate up to 128 distinct ids.  Picking a static
 * cap (instead of a dynamic alloc) lets the shmem layout be
 * pre-computed and the EXEC_BACKEND attach path stay trivial.
 */
#define CLUSTER_MAX_NODES 128

/*
 * Field-length budgets.  Generous compared to the strings we expect in
 * practice, but chosen to keep ClusterNodeInfo aligned to a power of
 * two so the array element offset is a simple multiplication.
 */
#define CLUSTER_NODE_HOSTNAME_LEN 64
#define CLUSTER_NODE_ADDR_LEN 128
#define CLUSTER_NODE_RDMA_ADDR_LEN 64
#define CLUSTER_NODE_RDMA_GID_LEN 40
#define CLUSTER_NODE_REGION_LEN 64
#define CLUSTER_CONF_NAME_LEN 64

/*
 * On-shmem magic.  ASCII "CONF" little-endian = 0x434F4E46.  Used by
 * cluster_conf_load to assert it is initialising a fresh region (or by
 * EXEC_BACKEND children to confirm they attached to the right
 * structure).
 */
#define PGRAC_CLUSTER_CONF_MAGIC ((uint32)0x434F4E46)


/*
 * Node role.  Stored as int32 in shmem so the layout stays stable
 * across compilers regardless of how they size enums.  At Stage 0.19
 * the value is informational only; Stage 2+ ADG (Active Data Guard)
 * dispatch will branch on it.
 */
typedef enum ClusterNodeRole {
	CLUSTER_ROLE_PRIMARY = 0,
	CLUSTER_ROLE_STANDBY = 1,
	CLUSTER_ROLE_ARBITER = 2
} ClusterNodeRole;


/*
 * Per-node entry in the topology table.  Layout is part of the shmem
 * ABI and must not change inside a stable release; new fields land
 * by appending (which Stage 2+ will do for last_heartbeat_at / state
 * / weight, see docs/cluster-conf-design.md §5.2).
 */
typedef struct ClusterNodeInfo {
	int32 node_id; /* -1 if slot empty (post-validate fills) */
	char hostname[CLUSTER_NODE_HOSTNAME_LEN];
	char interconnect_addr[CLUSTER_NODE_ADDR_LEN];
	char public_addr[CLUSTER_NODE_ADDR_LEN];
	ClusterNodeRole role;
	char region[CLUSTER_NODE_REGION_LEN];
	char rdma_addr[CLUSTER_NODE_RDMA_ADDR_LEN];
	char rdma_gid[CLUSTER_NODE_RDMA_GID_LEN];
	uint16 rdma_port;
	uint16 rdma_pkey;
	uint32 rdma_qkey;

	/*
	 * PGRAC: spec-7.2 D2 — DATA-plane (LMS<->LMS) listener address,
	 * host:port.  Optional: empty means this node offers no DATA plane
	 * (the CONTROL plane on interconnect_addr is unaffected).  Never
	 * derived from interconnect_addr by offset — adjacent-port
	 * collisions on single-host deployments make an implicit default
	 * unsound (spec-7.2 r1-F2), so it is declared explicitly or the
	 * data plane stays off (fail-closed once the GCS block family
	 * flips to the DATA plane).  Appended per the layout-append rule.
	 */
	char data_addr[CLUSTER_NODE_ADDR_LEN];
} ClusterNodeInfo;


/*
 * Top-level container in shmem.  Sized to ~128 KiB (accepted overhead
 * given the simplicity it buys; see docs/cluster-conf-design.md §3.2 —
 * budget raised from 64 KiB when spec-7.2 D2 appended data_addr).
 */
typedef struct ClusterConf {
	uint32 magic;
	char cluster_name[CLUSTER_CONF_NAME_LEN];
	int32 node_count;
	ClusterNodeInfo nodes[CLUSTER_MAX_NODES];
} ClusterConf;


/*
 * The cluster_conf surface (loader, accessors, helper functions) is
 * present only in --enable-cluster builds.  See file-header NOTES for
 * the disable-cluster contract and the SRF declaration below.
 */
#ifdef USE_PGRAC_CLUSTER

/* Process-local pointer to the shmem region.  Set in cluster_init_shmem. */
extern ClusterConf *ClusterConfShmem;

/*
 * Shmem region helpers.  Stage 1.3: registered via
 * cluster_shmem_register_region; cluster_request_shmem and
 * cluster_init_shmem dispatch by table.  See cluster_shmem.c
 * cluster_conf_region descriptor.
 *
 *	cluster_conf_shmem_size -- bytes to reserve via RequestAddinShmemSpace
 *	                           (size_fn for the registry).
 *	cluster_conf_shmem_init -- ShmemInitStruct + zero-fill on first attach;
 *	                           must run before cluster_conf_load (init_fn
 *	                           for the registry).
 */
extern Size cluster_conf_shmem_size(void);
extern void cluster_conf_shmem_init(void);

/*
 * cluster_conf_load -- read pgrac.conf (cluster.config_file GUC) and
 *	populate ClusterConfShmem.  Falls back to a single-node degraded
 *	topology when the file is absent (LOG, not FATAL) so single-node
 *	dev / regress runs do not need a conf file.  ereports FATAL on
 *	syntax errors, schema violations, or cluster.node_id /
 *	pgrac.conf disagreement.
 *
 *	Called from cluster_init_shmem (cluster_shmem.c) after the conf
 *	region is allocated and before cluster_ic_init runs (Stage 2+ tier
 *	vtables consume the topology via this SSOT).
 */
extern void cluster_conf_load(void);

/*
 * Lookup helper: O(node_count) scan through ClusterConfShmem->nodes.
 * Returns NULL if node_id is not in the topology.  Future readers
 * (Interconnect / Heartbeat) will use this rather than re-implementing
 * the scan.
 */
extern const ClusterNodeInfo *cluster_conf_lookup_node(int32 node_id);

/* Returns ClusterConfShmem->node_count, with NULL safety. */
extern int cluster_conf_node_count(void);

/*
 * Hot-path peer predicate.  Use this instead of cluster_conf_node_count()
 * in per-tuple / per-transaction gates so single-node fallback avoids an
 * out-of-line function call.  The topology is postmaster-static after
 * cluster_conf_load(); reconfig changes liveness, not the declared count.
 */
static inline bool
cluster_conf_has_peers(void)
{
	return ClusterConfShmem != NULL && ClusterConfShmem->node_count > 1;
}

/* Bidirectional role <-> string mapping.  Used by parser and by SRF. */
extern bool cluster_conf_role_from_string(const char *str, ClusterNodeRole *out);
extern const char *cluster_conf_role_to_string(ClusterNodeRole role);

#endif /* USE_PGRAC_CLUSTER */


/*
 * cluster_get_nodes -- SRF backing pg_cluster_nodes view.
 *
 *	Emits one row per non-empty slot in ClusterConfShmem->nodes:
 *	    node_id           int4  -- 0..127
 *	    hostname          text  -- NULL if not set in conf
 *	    interconnect_addr text  -- "host:port" (empty in single-node
 *	                               degraded mode)
 *	    public_addr       text  -- NULL if not set in conf
 *	    role              text  -- "primary" / "standby" / "arbiter"
 *	    region            text  -- NULL if not set in conf
 *	    is_self           bool  -- (node_id == cluster_node_id)
 *
 *	Stage 0.19 returns 1 row in single-node degraded mode, or
 *	node_count rows from the parsed topology.  Column shape is a
 *	stable contract from 0.19 onward; Stage 2+ may append columns
 *	(last_heartbeat_at, state, weight, ...).  In --disable-cluster
 *	builds the SRF returns zero rows (same convention as
 *	cluster_get_wait_events).
 */
extern Datum cluster_get_nodes(PG_FUNCTION_ARGS);

#endif /* CLUSTER_CONF_H */
