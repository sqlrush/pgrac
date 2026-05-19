/*-------------------------------------------------------------------------
 *
 * cluster_wait_events.h
 *	  pgrac cluster wait event class IDs (13 classes for 83 cluster
 *	  wait events: 46 from stage 0.11 + 5 added by stage 1.1 for
 *	  cluster_shared_fs (SharedFs class) + 5 added by stage 1.10 for
 *	  ClusterStartupPhase{0..4}Wait (StartupPhase class) + 4 added by
 *	  stages 1.11-1.14 for LMON/LCK/DIAG/Cluster Stats main loops
 *	  (BgProc class).
 *
 *	  Updated by spec-2.33 D9 (GCS block-shipping wait-event expansion).
 *
 *	  This header is the single source of truth for the upper-byte
 *	  class identifiers used by the WaitEventCluster enum defined in
 *	  src/include/utils/wait_event.h.  Every cluster wait event value
 *	  is composed as:
 *
 *	      <PG_WAIT_CLUSTER_CATEGORY> | <event_index_in_category>
 *
 *	  i.e. the upper byte selects the category (visible to users via
 *	  pg_stat_activity.wait_event_type) and the lower bits enumerate
 *	  events within that category.
 *
 *	  ID space allocation (per docs/wait-events-design.md §14.1):
 *
 *	      0x10000000   Cluster: GES           (subsystem #8)
 *	      0x11000000   Cluster: PCM           (subsystem #6)
 *	      0x12000000   Cluster: BufferShip    (subsystem #5)
 *	      0x13000000   Cluster: SCN           (subsystem #7)
 *	      0x14000000   Cluster: Reconfig      (#14 / #20)
 *	      0x15000000   Cluster: Recovery      (#86)
 *	      0x16000000   Cluster: Sinval        (subsystem #9)
 *	      0x17000000   Cluster: Interconnect  (AD-007)
 *	      0x18000000   Cluster: Undo          (AD-010)
 *	      0x19000000   Cluster: ADG           (#95)
 *	      0x1a000000   Cluster: SharedFs      (spec-1.1)
 *
 *	  Why the gap between PG (0x01..0x0A) and pgrac (0x10..0x19):
 *
 *	  PostgreSQL upstream may add new wait classes in future releases
 *	  (e.g. 0x0B, 0x0C ...).  We deliberately leave 0x0B..0x0F as a
 *	  buffer zone so that pgrac never collides with an upstream class
 *	  added later.  Starting at 0x10 also gives a clean visual
 *	  separation when reading hex dumps of wait_event_info values.
 *
 *	  Why each category gets its own class ID instead of one shared
 *	  PG_WAIT_CLUSTER class:
 *
 *	  pg_stat_activity.wait_event_type is rendered straight from the
 *	  upper byte by pgstat_get_wait_event_type().  Giving each cluster
 *	  category its own ID lets users filter directly:
 *
 *	      SELECT count(*) FROM pg_stat_activity
 *	          WHERE wait_event_type = 'Cluster: GES';
 *
 *	  Without per-category IDs, users would have to LIKE-match the
 *	  event name, which is brittle.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_wait_events.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The macros below are intentionally PG-free (no PG headers
 *	  included), so unit tests in src/test/cluster_unit/ can include
 *	  this header standalone.
 *
 *	  Stage 0.11 only registers identifiers; pgstat_report_wait_start/
 *	  pgstat_report_wait_end call sites land in the spec for each
 *	  owning subsystem (e.g. spec-1.X-ges-skeleton wires the GES
 *	  events).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAIT_EVENTS_H
#define CLUSTER_WAIT_EVENTS_H


/* ============================================================
 * Class IDs (upper byte) for the 10 cluster wait categories.
 *
 *	The lower 24 bits of each class ID are zero; the WaitEventCluster
 *	enum in wait_event.h sets the lower bits per event.
 * ============================================================ */
#define PG_WAIT_CLUSTER_GES 0x10000000U
#define PG_WAIT_CLUSTER_PCM 0x11000000U
#define PG_WAIT_CLUSTER_BUFFERSHIP 0x12000000U
#define PG_WAIT_CLUSTER_SCN 0x13000000U
#define PG_WAIT_CLUSTER_RECONFIG 0x14000000U
#define PG_WAIT_CLUSTER_RECOVERY 0x15000000U
#define PG_WAIT_CLUSTER_SINVAL 0x16000000U
#define PG_WAIT_CLUSTER_INTERCONNECT 0x17000000U
#define PG_WAIT_CLUSTER_UNDO 0x18000000U
#define PG_WAIT_CLUSTER_ADG 0x19000000U
#define PG_WAIT_CLUSTER_SHAREDFS 0x1a000000U
#define PG_WAIT_CLUSTER_STARTUP_PHASE 0x1b000000U /* spec-1.10 (2026-05-03) */
#define PG_WAIT_CLUSTER_BGPROC 0x1c000000U		  /* spec-1.11 Sprint B (2026-05-04) */


#endif /* CLUSTER_WAIT_EVENTS_H */
