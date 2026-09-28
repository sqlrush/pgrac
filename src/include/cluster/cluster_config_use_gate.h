/*-------------------------------------------------------------------------
 *
 * cluster_config_use_gate.h
 *    Ephemeral native producer cut, not configuration or DATA authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_config_use_gate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONFIG_USE_GATE_H
#define CLUSTER_CONFIG_USE_GATE_H

#include "port/atomics.h"

typedef struct ClusterConfigUseGate {
	pg_atomic_uint64 state;
} ClusterConfigUseGate;

typedef struct ClusterConfigUseGateState {
	bool closed;
	uint32 epoch;
	uint32 owners;
} ClusterConfigUseGateState;

/* Only initialize before children exist / after all old children retire.
 * The single controller must separately prove membership and application.
 * A cut cookie proves only which LOCAL close is held, never a CF selection.
 */
extern void cluster_config_use_gate_init(ClusterConfigUseGate *gate);
extern ClusterConfigUseGateState cluster_config_use_gate_read(ClusterConfigUseGate *gate);
extern bool cluster_config_use_gate_close(ClusterConfigUseGate *gate, uint32 *cut);
extern bool cluster_config_use_gate_open(ClusterConfigUseGate *gate, uint32 cut);
/* continuation only reserves a counted provisional owner. Caller MUST check
 * the exact still-held native leader before doing work; otherwise leave.
 * Each success has one process-local, nontransferable matching leave. */
extern bool cluster_config_use_gate_enter(ClusterConfigUseGate *gate, bool continuation,
										  uint32 *epoch);
extern bool cluster_config_use_gate_leave(ClusterConfigUseGate *gate);

/* Native-family mapping only, not an authority assertion. Runtime publisher
 * must separately own the configuration episode. Never exposed through SQL.
 */
extern ClusterConfigUseGate *cluster_shared_config_delivery_native_gate(void);
extern bool cluster_shared_config_delivery_work_pending(void);

/* Original native lifecycle consumers; no caller-set role or epoch. */
extern void cluster_shared_config_use_enter(void);
extern void cluster_shared_config_use_xact_start(void);
extern void cluster_shared_config_use_xact_end(void);
extern void cluster_shared_config_use_idle(void);
extern void cluster_shared_config_use_exit(void);
/* Original native session obligations, including catalog cleanup at exit. */
extern bool cluster_shared_config_use_session_owned(void);

#endif
