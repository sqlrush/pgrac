/*-------------------------------------------------------------------------
 *
 * cluster_drm_scan.h
 *	  LMON-side DRM decision scan driver (spec-7.6 wave 6.3c).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_drm_scan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_DRM_SCAN_H
#define CLUSTER_DRM_SCAN_H

/*
 * Periodic DRM decision scan, driven from the LMON main-loop tick.  It self-
 * gates to cluster.drm_scan_interval_ms and is a cheap no-op when
 * cluster.drm_enabled is off, so both LMON drain sites can call it every tick.
 *
 * Wave 6.3c PROPOSES only (INV-DRM9 pure): it reconciles the sample rate, rolls
 * each candidate shard's tumbling window, runs the pure decision predicate on
 * the completed-window counts and counts per-reason verdicts + auto-actionable
 * proposals for observability.  It never mutates lock/master/GRD state — the
 * live single-shard remaster executor is a later wave (6.3d).
 */
extern void cluster_drm_lmon_scan_tick(void);

#endif /* CLUSTER_DRM_SCAN_H */
