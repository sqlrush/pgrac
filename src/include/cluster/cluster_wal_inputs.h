/* PGRAC: complete retained WAL metadata under one ROOT and WALR read scope.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_INPUTS_H
#define CLUSTER_WAL_INPUTS_H

#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_wal_retention.h"
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
	CheckPoint native_checkpoint; /* complete original immutable-anchor input */
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

/* Abandon a failed proof permanently and release all WAL pins/remote requests.
 * Only compare its original ROOT to schedule a retry: OK means keep waiting;
 * STALE_TOKEN means discard this object and begin a fresh scope. This works
 * on a stale scope, but never restores it or grants access to its sources. */
extern ClusterControlRootResult cluster_wal_inputs_wait_failed_v1(ClusterWalInputsV1 *inputs);

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
/* Startup in recovery only, before IR. Same complete ROOT/anchor/claim and
 * WALR-S selection as begin_v1, but this scope cannot sample/flush OPEN
 * writers, request peer cuts, build live contribution plans or prove local
 * predecessor retirement. OPEN metadata remains visible; retained visitation
 * returns WAIT for OPEN without invalidating the scope. No membership filter,
 * no lower-versus-native-redo filter and no discovery of unselected archives.
 *
 * This read scope is not an execution pin or a recovery permission. Copy
 * metadata only while held. Release before acquiring an execution pin, then
 * revalidate the entire selected ROOT using that original execution owner
 * before IR; copied tokens alone confer no DATA/ROOT/PI/GC authority. */
extern ClusterControlRootResult cluster_wal_inputs_cold_begin_v1(const uint8 storage_uuid[16],
																 uint64 system_identifier,
																 ClusterWalInputsV1 **out);
struct ClusterRecoveryStartV1;
struct ClusterColdTopTxCensusEntryV1;
struct ClusterRecoveryTTAbortV1;
/* Post-EOR Startup only, bound to its original installed-writer start resource
 * and an exact original ACTIVE census entry. Reacquire all original ROOT
 * inputs and WALR read pins; reject another cut or changed subject/start.
 * The caller's ResourceOwner remains current through release. Suspend/resume
 * rechecks the start and input version as well as ROOT. Retained-only: OPEN
 * visitation still waits, and live writer/flush/retirement APIs stay refused.
 * This grants no loser, C0, checkpoint, DATA or reclamation authority. */
extern ClusterControlRootResult cluster_wal_inputs_begin_tx_closure_v1(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, ClusterWalInputsV1 **out);
struct ClusterColdCtrcWorkV1;
/* Original post-EOR Startup and its current retained CTRC work only. Reacquire
 * the complete ROOT-selected input set and WALR read pins, reselect every
 * original source (including INSTALL history), and bind the original census
 * ROOT, owner and work generation. No ACTIVE subject is required. Suspend/
 * resume and reads reject re-prepared work even at the same allocation address.
 * Cold census may fix and natively flush only this exact original Startup
 * owner's installed local OPEN writer after selection. Its end never moves;
 * native writer/fence/timeline, physical prefix and whole-scope checks remain.
 * Any other OPEN source waits before callbacks or local flush. General live
 * APIs remain refused. No C0/path, DATA, discharge or completion right. */
extern ClusterControlRootResult cluster_wal_inputs_begin_ctrc_census_v1(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 **out);
/* Borrow the original CTRC scope's read-only shared namespace descriptor.
 * Scope creation records the configured directory name/object before original
 * ROOT selection. The first call opens that same directory without following
 * its final symlink, verifies its owned directory identity and rechecks the
 * original ROOT. Later calls compare the same descriptor and directory name;
 * they never reopen or rebase. The original Start/work/owner/pin must remain
 * current. Suspended scopes refuse. Destroy dependents before releasing the
 * scope, which alone closes the descriptor. Failure sets *root_fd to -1.
 * This supplies neither isolation/freeze nor DATA/write/completion authority;
 * complete physical enumeration and all later semantic proofs remain required.
 */
extern ClusterControlRootResult cluster_wal_inputs_ctrc_namespace_v1(
	ClusterWalInputsV1 *inputs, int *root_fd);
/* Exact current C0 candidate only, inside the original TX_CLOSURE read scope.
 * Resolve through the shared undo namespace without borrowing a redo duty.
 * This provisional path grants no write authority: retain target exclusion
 * and revalidate the whole source ROOT before and after descriptor I/O. */
extern bool cluster_wal_inputs_tx_abort_path_v1(ClusterWalInputsV1 *inputs,
	const struct ClusterRecoveryTTAbortV1 *record, char *out, size_t out_size);
/* Local scope/candidate checks only; no ROOT read or new permission. The
 * caller still revalidates the whole selected input outside native locks. */
extern bool cluster_wal_inputs_tx_abort_current_v1(ClusterWalInputsV1 *inputs,
	const struct ClusterRecoveryTTAbortV1 *record);

/* One native IR attempt for this exact original C0 candidate. Select only
 * the original COMPLETE checkpoint/history, with the original isolation owner
 * and whole ROOT/WALR scope. RECONFIG_WAIT means local contention, never a
 * terminal elapsed deadline. An uncertain release retains this entire scope.
 * This grants neither PAGE nor ordinary RR replay authority. */
extern ClusterControlRootResult cluster_wal_inputs_tx_abort_acquire_v1(
	ClusterWalInputsV1 *inputs, const struct ClusterRecoveryTTAbortV1 *record);
/* No I/O: current exact candidate plus this scope's actual held TX IR. */
extern bool cluster_wal_inputs_tx_abort_authorized_v1(ClusterWalInputsV1 *inputs,
	const struct ClusterRecoveryTTAbortV1 *record);
struct ClusterRecoverySerialGuard;
/* Borrow only this original scope's actual TX IR and complete native WALR
 * owner, without acquiring or transferring either. Retain the original scope
 * and Start until every consumer has retired. This purpose is distinct from
 * an ordinary RR retention pin; PAGE/stable-base, DATA, FPI, ROOT publishing
 * and discharge still require their own consumers and proofs. */
typedef struct ClusterRecoveryTxRetentionV1 {
	ClusterWalInputsV1 *scope;
	struct ClusterRecoverySerialGuard *serial;
	ClusterWalReadPinOwnerV1 input_owner;
} ClusterRecoveryTxRetentionV1;
/* No I/O, allocation or caller output pointer: return the complete view by
 * value, or an all-zero value on refusal. A caller assignment must use its
 * own disjoint storage. No borrowed owner is overwritten by this producer. */
extern ClusterRecoveryTxRetentionV1 cluster_wal_inputs_tx_retention_v1(
	ClusterWalInputsV1 *inputs, const struct ClusterRecoveryTTAbortV1 *record);
/* No I/O; locate the exact active scope and original native incarnation before
 * inspecting scope/IR pointers. This never grants PAGE or ordinary RR rights. */
extern bool cluster_wal_inputs_tx_retention_current_v1(const ClusterRecoveryTxRetentionV1 *view);
/* Original TX subject only: this value freezes the exact COMPLETE current or
 * selected history input and its whole ROOT beside the borrowed native owners.
 * The replay token remains the immutable old RR token, never current write
 * authority. Retain the original scope/Start; caller storage must be disjoint.
 * This is not an RR retention pin, PAGE batch, replay/ROOT publication or DATA
 * permission. Other contributing origins still need their own authorities. */
typedef struct ClusterRecoverySubjectAuthorityV1 {
	ClusterRecoveryTxRetentionV1 retention;
	ClusterControlRootFileToken root;
	ClusterWalInputV1 selected;
	ClusterControlRootReadToken replay_token;
} ClusterRecoverySubjectAuthorityV1;
/* No I/O/allocation/acquire; all-zero on refusal. Only the original native TX
 * scope can supply the selected subject. No caller-provided COMPLETE record. */
extern ClusterRecoverySubjectAuthorityV1 cluster_wal_inputs_tx_subject_v1(
	const ClusterRecoveryTxRetentionV1 *view);
/* No I/O: compare the complete frozen value against that same original scope
 * and actual native holders, locating its incarnation before dereferencing any
 * borrowed owner. Across I/O, revalidate the original whole ROOT scope outside
 * native locks as well. This never reselects a different generation/cut. */
extern bool cluster_wal_inputs_tx_subject_current_v1(
	const ClusterRecoverySubjectAuthorityV1 *authority);
extern bool cluster_wal_inputs_tx_subject_covers_window_v1(
	const ClusterRecoverySubjectAuthorityV1 *authority, uint16 thread,
	XLogRecPtr lower, XLogRecPtr upper);
struct ClusterRecoverySerialRequest;
/* IR purpose callbacks: validate includes original whole ROOT I/O outside CF;
 * current is no-wait and never calls the IR holder revalidation recursively. */
extern ClusterControlRootResult cluster_wal_inputs_tx_serial_validate_v1(
	ClusterWalInputsV1 *inputs, const struct ClusterRecoverySerialRequest *request);
extern bool cluster_wal_inputs_tx_serial_current_v1(ClusterWalInputsV1 *inputs,
	const struct ClusterRecoverySerialRequest *request);

/* Startup in recovery, under the caller's existing execution WALR retention
 * owner. Select one exact CLOSED/RECOVERY_REQUIRED/RECOVERY_COMPLETE checkpoint
 * from the original whole ROOT, validate its native anchor and physical claim,
 * and recheck ROOT after claim I/O outside CF. Acquires no WALR scope or recovery
 * permission. The caller must revalidate its original execution owner before
 * and after the call; output is provisional metadata, never an endpoint proof. */
extern ClusterControlRootResult cluster_wal_inputs_cold_checkpoint_v1(
	const ClusterControlRootSnapshot *expected, const ClusterControlRootFileToken *root_token,
	ClusterWalRecoveryCheckpointV1 *out);
/* Original execution owner only; sync exact retained WAL/claim/namespace and
 * reselect the same native checkpoint and whole ROOT after I/O outside CF.
 * Acquires no scope and grants no endpoint or recovery authority. */
extern ClusterControlRootResult
cluster_wal_inputs_cold_sync_v1(const ClusterControlRootSnapshot *expected,
								const ClusterControlRootFileToken *root_token,
								ClusterWalTailObservation *out);
/* Metadata token of the whole selected ROOT, not a publication capability.
 * Like at_v1, this requires the same live scope/owner; call revalidate_v1
 * before consuming provisional results. Invalid scopes produce no token. */
extern ClusterControlRootResult cluster_wal_inputs_root_token_v1(ClusterWalInputsV1 *inputs,
																 ClusterControlRootFileToken *out);
extern ClusterControlRootResult cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs);
extern uint32 cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs);
extern const ClusterWalInputV1 *cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index);

/* Provisional metadata from this held online scope: the exact source has a
 * durable RECOVERY_COMPLETE covering its sealed tail. This does not validate
 * WAL bytes, clear existing PI, prove another generation retired, or permit
 * source reclamation. A census must still decode the whole selected input
 * and revalidate ROOT before using the result. No I/O or CF acquisition. */
extern bool cluster_wal_inputs_recovered_prefix_v1(ClusterWalInputsV1 *inputs,
												   const ClusterWalSourceRef *source,
												   XLogRecPtr *out_end);

/* Provisional executor retirement for an entire selected origin. The current
 * generation must be RECOVERY_COMPLETE; every retained checkpoint predecessor
 * must form a continuous installed terminal chain. A lone CLOSED generation
 * is never an executor-exit proof. The caller still needs the complete decoded
 * input, DATA ancestry, membership/boot and final master cut. No I/O, mutation
 * or WAL reclamation; revalidate this same held scope before consumption. */
extern bool cluster_wal_inputs_recovered_owner_v1(ClusterWalInputsV1 *inputs, int32 node,
												  ClusterWalSourceRef *out);

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

/* Fixed-memory census over the same complete input set and immutable live
 * ends. All callbacks are provisional additions, never DATA/retirement or
 * service-completion proofs. A callback refusal invalidates the scope. No
 * full PAGE/SIDE graph is materialized; final physical/source/ROOT checks are
 * identical to contributions_v1. The original caller retains every source. */
typedef RfPageProofDetailV1 (*ClusterWalCensusVisitorV1)(XLogReaderState *record,
														 const ClusterWalSourceRef *source,
														 const RfContributorStreamCutV1 *cut,
														 void *arg);
extern ClusterControlRootResult cluster_wal_inputs_census_v1(ClusterWalInputsV1 *inputs,
															 ClusterWalCensusVisitorV1 visitor,
															 void *arg, uint64 *out_record_count,
															 RfPageProofDetailV1 *out_detail);

/* Cold-only, read-only enumeration of every selected source. completed is
 * NULL before its physical visit and non-NULL after the original reader has
 * verified its entire cut. Explicit empty initializer terminals also receive
 * both callbacks; they are not missing participants. Each event is provisional
 * until the complete census returns OK and the caller revalidates its scope.
 * No callback may release or mutate the scope, apply effects or publish a
 * completion. The caller must discard all additions on failure/ERROR. */
typedef RfPageProofDetailV1 (*ClusterWalCensusSourceVisitorV1)(
	const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
	const ClusterWalTailObservation *completed, void *arg);
/* Requires an original Startup cold scope. OPEN sources return WAIT before
 * callbacks, except the original CTRC scope's exact installed local writer
 * and private fixed native cut described above. No peer live-cut request or
 * online PI/replay authority. The original physical, claim, ROOT, pin and final
 * all-source checks remain mandatory. An empty callback list alone proves
 * nothing about namespace coverage or recovery completion. */
extern ClusterControlRootResult
cluster_wal_inputs_cold_census_v1(ClusterWalInputsV1 *inputs, ClusterWalCensusVisitorV1 visitor,
								  ClusterWalCensusSourceVisitorV1 source_visitor, void *arg,
								  uint64 *out_record_count, RfPageProofDetailV1 *out_detail);
/* Local prior-executor retirement only. Consume original prior-exit evidence
 * and the ROOT-selected native INSTALL predecessor chain under this scope.
 * This does not prove DATA coverage, remove PI, or permit WAL reclamation. */
extern bool cluster_wal_inputs_local_predecessor_retired_v1(ClusterWalInputsV1 *inputs,
															const ClusterWalSourceRef *predecessor,
															const ClusterWalSourceRef *writer);
extern void cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs);

#endif
