/* PGRAC: complete retained WAL metadata under one ROOT and WALR read scope.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_INPUTS_H
#define CLUSTER_WAL_INPUTS_H

#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_thread_recovery_fabric.h"

#define CLUSTER_WAL_INPUTS_MAX 128

typedef enum ClusterWalInputKindV1 {
	CLUSTER_WAL_INPUT_CHECKPOINT = 1,
	CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL = 2
} ClusterWalInputKindV1;

typedef struct ClusterWalInputV1 {
	ClusterWalInputKindV1 kind;
	bool current;
	ClusterWalSourceRef source;
	ClusterControlRootSnapshot checkpoint;
	XLogRecPtr checkpoint_start;
	XLogRecPtr native_redo;
	/* Only the terminal alternative uses these fields. It has no checkpoint. */
	XLogRecPtr first_segment;
	ClusterWalStartupObservation terminal;
} ClusterWalInputV1;

typedef struct ClusterWalInputsV1 ClusterWalInputsV1;

/* Release every read pin while a background job waits for a peer. Suspended
 * scopes cannot expose inputs, read WAL or qualify use of an existing plan.
 * Resume reacquires the same sorted pin set and compares the complete original
 * ROOT token. A changed token is terminal for this scope; lock contention is
 * retryable. Neither operation resamples an already fixed live endpoint. */
extern ClusterControlRootResult cluster_wal_inputs_suspend_v1(ClusterWalInputsV1 *inputs);
extern ClusterControlRootResult cluster_wal_inputs_resume_v1(ClusterWalInputsV1 *inputs);

/* Native background worker, bgwriter or checkpointer I/O context only;
 * never LMON/LMS dispatch. CF and WALR use their existing native owners.
 * Read all present origins, including non-serving/current/history/terminal
 * generations. No ALIVE filter or partial result. Pending initialization
 * returns RECONFIG_WAIT. Acquire distinct sorted WALR-S outside CF, then
 * select all immutable claim references/anchors under the unchanged CF-S
 * ROOT token. Read the claims outside CF while retaining the entire WALR-S
 * set, then recheck the exact ROOT token before returning any inputs.
 * Outputs are metadata only: live complete ends, physical WAL validation,
 * directory cuts, DATA and retirement still need their original owners.
 * The caller's ResourceOwner must remain current through release. */
extern ClusterControlRootResult cluster_wal_inputs_begin_v1(const uint8 storage_uuid[16],
															uint64 system_identifier,
															ClusterWalInputsV1 **out);
extern ClusterControlRootResult cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs);
extern uint32 cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs);
extern const ClusterWalInputV1 *cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index);

/* Physically visit one selected retained source outside CF, under this exact
 * ROOT/WALR scope. OPEN returns WAIT without invalidating the scope: its original live writer must first confirm
 * a complete end after the directory cut. Terminal input is fully reclassified
 * and compared with the selected terminal. Callbacks are provisional; any
 * failure invalidates the scope and clears output. The caller must release
 * the scope on failure/ERROR and revalidate the complete scope before using
 * the assembled proof. No callback may release or mutate this scope. */
extern ClusterControlRootResult
cluster_wal_inputs_visit_retained_v1(ClusterWalInputsV1 *inputs, uint32 index,
									 ClusterWalRecordVisitor visitor, void *arg,
									 ClusterWalTailObservation *out);

/* Original local OPEN writer only. Invoke first after capturing the exact
 * directory cut for this job. Keeps a fixed native reserved end in this scope,
 * explicitly flushes this original writer through that end, then decodes
 * without CF. No pending sample from another job is borrowed. RECONFIG_WAIT
 * before visiting retains the reservation; any visited
 * failure invalidates the scope. Remote sources require their own writer
 * transport and return RECONFIG_WAIT here, never use the receiver's Flush. */
extern ClusterControlRootResult
cluster_wal_inputs_visit_live_local_v1(ClusterWalInputsV1 *inputs, uint32 index,
									   ClusterWalRecordVisitor visitor, void *arg,
									   ClusterWalTailObservation *out);

/* Fix a local OPEN endpoint before constructing an immutable contribution
 * plan. Once accepted, retries/visits reuse that exact complete end and still
 * recheck the original writer. Metadata only, no physical validation yet. */
extern ClusterControlRootResult
cluster_wal_inputs_prepare_live_local_v1(ClusterWalInputsV1 *inputs, uint32 index,
										 XLogRecPtr *out_complete_end);

/* Same cut/reader rules, with original-writer CONTROL observation for foreign
 * OPEN sources. Pending returns WAIT; accepted endpoints never move. These
 * calls are still background I/O owners, never CONTROL dispatch. */
extern ClusterControlRootResult cluster_wal_inputs_prepare_live_v1(ClusterWalInputsV1 *inputs,
																   uint32 index,
																   XLogRecPtr *out_complete_end);
extern ClusterControlRootResult cluster_wal_inputs_visit_live_v1(ClusterWalInputsV1 *inputs,
																 uint32 index,
																 ClusterWalRecordVisitor visitor,
																 void *arg,
																 ClusterWalTailObservation *out);

/* Read every selected source into one sealed contribution graph. All retained
 * records remain obligations, including records before native redo. This is
 * NOT a replay plan: it grants no recovery/mutation/PI/ROOT/GC authority. The
 * scope must remain held and be revalidated before consuming the result. A
 * foreign OPEN source pending its original writer's cut returns WAIT with no
 * partial output. Explicit empty terminals remain participants and are read.
 * The fabric destructor owns the output; release it before the input scope.
 * detail reports a PAGE/SIDE refusal; root errors are returned directly. */
extern ClusterControlRootResult
cluster_wal_inputs_contributions_v1(ClusterWalInputsV1 *inputs, bool space_active,
									ClusterThreadRecoveryFabricPlanV1 **out_plan,
									uint64 *out_record_count, RfPageProofDetailV1 *out_detail);
/* Local prior-executor retirement only. Consume original prior-exit evidence
 * and the ROOT-selected native INSTALL predecessor chain under this scope.
 * This does not prove DATA coverage, remove PI, or permit WAL reclamation. */
extern bool cluster_wal_inputs_local_predecessor_retired_v1(ClusterWalInputsV1 *inputs,
															const ClusterWalSourceRef *predecessor,
															const ClusterWalSourceRef *writer);
extern void cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs);

#endif
