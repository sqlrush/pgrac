/*-------------------------------------------------------------------------
 *
 * cluster_cr_coordinator_stat.h
 *	  pgrac cross-instance CR/current read-path coordinator boundary (spec-5.57).
 *
 *	  spec-5.57 makes the "cross-node table content read is unavailable" wall an
 *	  EXPLICIT, observable, frozen read-path coordinator boundary (CR-9) instead
 *	  of six scattered fail-closed + forward-link sites.  This module owns the
 *	  boundary's two non-data-plane surfaces:
 *
 *	   1. the pure origin CLASSIFIER -- given a 0-based origin node derived from a
 *	      UBA (uba_origin_node_id), decide which of the three CR origin classes
 *	      (§0.1) it is, plus an invalid bucket:
 *	        - OWN                 : origin == this instance (normal construct)
 *	        - MATERIALIZED_REMOTE : merged-materialized remote, served from the
 *	                                local pg_undo/instance_<origin> tree (class②,
 *	                                already shipped, spec-4.5a D8)
 *	        - RUNTIME_REMOTE      : runtime-warm remote (class③) -- the net-new
 *	                                boundary; data plane (remote undo fetch) is
 *	                                Stage 6 (#119), so this is fail-closed 53R9G
 *	                                until then (rule 8.A: never false-visible)
 *	        - INVALID             : not a derivable in-membership owner
 *
 *	   2. an INDEPENDENT, small, fixed-size observability counter region (one
 *	      atomic per ClusterCrCoordCounter), deliberately SEPARATE from the
 *	      spec-5.51 ClusterCRShared struct (held substrate) and from every other
 *	      CR region -- purely additive observability for pg_cluster_state's
 *	      'cr_coord' category.  Always allocated (a handful of uint64s) so the
 *	      region count is deterministic.  Advisory only: corruption/staleness
 *	      affects observability, never a visibility verdict.
 *
 *	  CORRECTNESS BOUNDARY (rule 8.A): the fail-closed 53R9G boundary itself lives
 *	  in the CR walker (cluster_cr.c) and is NON-DEGRADABLE -- it fires under any
 *	  GUC value.  The cluster.cross_instance_cr_coordinator GUC only gates the
 *	  observability surface (counter bumps / dump / probe / LOG-once), never the
 *	  fail-closed itself.  This module ships NO data-plane code (AD-013): no wire
 *	  struct, no remote undo fetch -- that is the Stage 6 boundary (§1.3).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.57-cross-instance-cr-current-coordinator.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr_coordinator_stat.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_COORDINATOR_STAT_H
#define CLUSTER_CR_COORDINATOR_STAT_H

#ifndef FRONTEND

#include "postgres.h"

#include "cluster/cluster_scn.h" /* NodeId, SCN_NODE_ID_VALID */

/*
 * ClusterCrCoordMode -- GUC cluster.cross_instance_cr_coordinator (spec-5.57 D3,
 *   §2.2).  A single enum expressing the boundary's monotone observability state.
 *   It NEVER gates the fail-closed 53R9G boundary (8.A non-degradable); it only
 *   gates the D3 observability surface:
 *     off      -- counters/dump-bumps/probe/LOG-once off (boundary still fires)
 *     boundary -- DEFAULT: fail-closed boundary + D3 counters
 *     forward  -- contract placeholder: still fail-closed (data plane is Stage 6),
 *                 plus a LOG-once telling the operator the data plane is Stage 6
 */
typedef enum ClusterCrCoordMode {
	CR_COORD_MODE_OFF = 0,
	CR_COORD_MODE_BOUNDARY, /* DEFAULT */
	CR_COORD_MODE_FORWARD,
} ClusterCrCoordMode;

extern int cluster_cross_instance_cr_coordinator; /* enum-backing GUC int */
extern bool cluster_cross_instance_cr_probe;	  /* D0 measure-leg GUC */

/*
 * ClusterCrCoordOriginClass -- classification of a CR undo record's origin
 *   (the heart of the read-path coordinator boundary, §0.1).  See the file
 *   header for the four classes.
 */
typedef enum ClusterCrCoordOriginClass {
	CR_COORD_ORIGIN_OWN = 0,
	CR_COORD_ORIGIN_MATERIALIZED_REMOTE,
	CR_COORD_ORIGIN_RUNTIME_REMOTE, /* class③ -- fail-closed (Stage 6 data plane) */
	CR_COORD_ORIGIN_INVALID,
} ClusterCrCoordOriginClass;

/*
 * ClusterCrCoordCounter -- the four frozen observability counters (§2.6).  The
 *   array bound is CR_COORD_COUNTER__COUNT.  All advisory (no correctness role).
 */
typedef enum ClusterCrCoordCounter {
	CR_COORD_CROSS_INSTANCE_CR_REFUSED = 0, /* class③ CR construct refused (boundary) */
	CR_COORD_REMOTE_UNDO_READ_REFUSED,		/* the remote-undo-read leg refused (W3) */
	CR_COORD_MATERIALIZED_REMOTE_SERVED,	/* class② record served from local tree */
	CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE, /* D0 probe-mode class③ hit */
	CR_COORD_COUNTER__COUNT,
} ClusterCrCoordCounter;

/*
 * spec-5.57 D4 (version selection + CR fabrication contract; frozen, NO data
 * plane here):
 *   - Version selection (§2.3/§3.3): the target CR image for (BufferTag,
 *     read_scn) is the current block image (shipped by the Owner, WAL-before-
 *     ship preserved) with all undo records satisfying
 *     (write_scn later than read_scn, OR ITL.status==ACTIVE) inverse-applied via
 *     scn_time_cmp.  read_scn is
 *     a GLOBAL Lamport SCN (AD-008), so the Requester's read_scn alone fixes the
 *     version cluster-wide; own-instance undo is read locally (shipped),
 *     remote-origin undo is the Stage 6 (#119) data plane (fail-closed here).
 *   - Fabrication position (Q1-A): CR is fabricated on the REQUESTER side (the
 *     Owner only ships the current image; it does NOT build a CR block).  This
 *     matches pgrac's existing requester-local CR model (cluster_cr.c
 *     cr_walk_chain) and AD-002 ("CR is a snapshot-layer construct").  Holder-
 *     side fabrication (Oracle LMS style) is unverified for Oracle and is left
 *     as a Stage 6 Smart-Fusion option (§10), not the frozen direction.
 *   - Three roles (§2.1): Requester <-> Master (existing GRD/PCM master routing,
 *     cluster_grd_lookup_master) <-> Owner.  No new coordinator process (AD-013).
 */

/*
 * cluster_cr_coordinator_classify_origin -- pure boundary classifier.  Takes a
 *   0-based origin node id (as derived by uba_origin_node_id) and returns its
 *   ClusterCrCoordOriginClass against this instance's cluster_node_id and the
 *   spec-4.5a materialized-origin oracle.  No I/O, safe under critical section.
 *   InvalidNodeId / out-of-membership origins return CR_COORD_ORIGIN_INVALID
 *   (never silently 'own' -- L10/L69 boundary semantics).
 */
extern ClusterCrCoordOriginClass cluster_cr_coordinator_classify_origin(NodeId origin_node);

/*
 * origin(0-based) <-> owner_instance(1-based) namespace conversion (L48).  The
 *   undo segment directory uses the 1-based owner_instance; the CR walker and
 *   membership use the 0-based origin/cluster_node_id.  These are the single
 *   SSOT for the conversion so the two namespaces are never silently mixed.
 */
extern int cluster_cr_coordinator_origin_to_owner_instance(NodeId origin_node);
extern NodeId cluster_cr_coordinator_owner_instance_to_origin(int owner_instance);

/* Observability counter region (independent shmem; advisory; §2.6). */
extern Size cluster_cr_coordinator_shmem_size(void);
extern void cluster_cr_coordinator_shmem_init(void);
extern void cluster_cr_coordinator_shmem_register(void);
extern void cluster_cr_coordinator_stat_bump(ClusterCrCoordCounter counter);
extern uint64 cluster_cr_coordinator_stat_count(ClusterCrCoordCounter counter);

/* Frozen counter key string (shared by the pg_cluster_state dump + tests). */
extern const char *cluster_cr_coordinator_counter_key(ClusterCrCoordCounter counter);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_COORDINATOR_STAT_H */
