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
#include "cluster/cluster_shared_config.h"

typedef struct ClusterConfigUseGate {
	pg_atomic_uint64 state;
} ClusterConfigUseGate;

typedef struct ClusterConfigUseGateState {
	bool closed;
	uint32 epoch;
	uint32 owners;
} ClusterConfigUseGateState;

typedef struct ClusterConfigUseTarget {
	uint32 epoch;
	uint32 node_id;
	ClusterSharedConfigRef ref;
	ClusterSharedConfigActive common;
} ClusterConfigUseTarget;

/* Single controller only, while exact CLOSED has zero owners. The target is
 * immutable until all readers leave; OPEN's atomic publication follows bind.
 * This is a local common-use floor, not a CF/member/service certificate. */
extern bool cluster_config_use_target_bind(ClusterConfigUseGate *gate,
										   ClusterConfigUseTarget *target, uint32 cut, uint32 node,
										   const ClusterSharedConfigRef *ref,
										   const ClusterSharedConfigActive *common);
/* Caller owns a counted reservation throughout this read and dependent use.
 * An unbound target is not proof. Later default-only/pending generations can
 * preserve the exact actual common values; this is not their application ACK. */
extern bool cluster_config_use_target_matches(const ClusterConfigUseTarget *target, uint32 epoch,
											  const ClusterSharedConfigRegistration *actual);

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
extern ClusterConfigUseTarget *cluster_shared_config_delivery_native_target(void);
extern bool cluster_shared_config_delivery_work_pending(void);

/* Separate fresh-maintenance producer cut. The real terminal receipt pipeline
 * is not stopped by this gate. NULL/failed is not an empty producer proof.
 * Controller must retain its global episode and wake cleaners after OPEN. */
extern ClusterConfigUseGate *cluster_shared_config_delivery_cleaner_gate(bool *failed);
extern bool cluster_shared_config_cleaner_begin(void);
/* Only a complete original pass retires ownership. ERROR keeps a sticky
 * failed family until native all-old-child retirement; never an idle ACK. */
extern void cluster_shared_config_cleaner_end(bool completed);

/* Original native lifecycle consumers; no caller-set role or epoch. */
extern void cluster_shared_config_use_enter(void);
extern void cluster_shared_config_use_xact_start(void);
extern void cluster_shared_config_use_xact_end(void);
extern void cluster_shared_config_use_idle(void);
extern void cluster_shared_config_use_exit(void);
/* Original native session obligations, including catalog cleanup at exit. */
extern bool cluster_shared_config_use_session_owned(void);

#endif
