/*-------------------------------------------------------------------------
 *
 * cluster_external_fence.h
 *	  Provider-neutral external I/O fence semantic ABI (RF-ROOT P4).
 *
 *	  Oracle documents failure isolation, GES/LMON recovery coordination and
 *	  IPMI integration, but not these bytes or predicates.  The exact layouts
 *	  below are the approved PGRAC adaptation.  No production provider is
 *	  selected: provider id 0 remains UNAVAILABLE and bit 24 remains inactive.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_EXTERNAL_FENCE_H
#define CLUSTER_EXTERNAL_FENCE_H

#include "cluster/cluster_control_root.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_recovery_duty.h"

#define PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED UINT32_C(1)
#define PGRAC_EXTERNAL_FENCE_PREDICATE_REJOIN_ON UINT32_C(2)
#define PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1 UINT32_C(1)
#define PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_MS UINT32_C(5000)
#define PGRAC_EXTERNAL_FENCE_DIGEST_BYTES UINT32_C(32)
#define PGRAC_EXTERNAL_FENCE_NEED_V1_BYTES UINT32_C(96)
#define PGRAC_EXTERNAL_FENCE_BINDING_V1_BYTES UINT32_C(104)
#define PGRAC_EXTERNAL_FENCE_REJOIN_OFFER_V1_BYTES UINT32_C(40)
#define PGRAC_EXTERNAL_FENCE_REJOIN_NEED_V1_BYTES UINT32_C(104)
#define PGRAC_EXTERNAL_FENCE_REJOIN_BINDING_V1_BYTES UINT32_C(112)
#define PGRAC_EXTERNAL_FENCE_REJOIN_FRAME_V1_BYTES UINT32_C(256)
#define PGRAC_EXTERNAL_FENCE_ACQUIRE_TIMEOUT_DEFAULT_MS 120000

StaticAssertDecl(PGRAC_IC_HELLO_CAP_CONTROL_ROOT_V1 == UINT32_C(0x00080000),
				 "unexpected control-root v1 HELLO capability");
StaticAssertDecl(PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1 == (UINT64_C(1) << 24),
				 "unexpected external-fence root feature bit");

typedef enum PgracExternalFenceVerdict {
	PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED = 1,
	PGRAC_EXTERNAL_FENCE_REJECTED = 2,
	PGRAC_EXTERNAL_FENCE_UNKNOWN = 3,
	PGRAC_EXTERNAL_FENCE_UNAVAILABLE = 4
} PgracExternalFenceVerdict;

typedef enum PgracExternalFenceDenyReason {
	PGRAC_EXTERNAL_FENCE_DENY_NONE = 0,
	PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT = 1,
	PGRAC_EXTERNAL_FENCE_DENY_EPISODE_NOT_CURRENT = 2,
	PGRAC_EXTERNAL_FENCE_DENY_VICTIM_NOT_AUTHORIZED = 3,
	PGRAC_EXTERNAL_FENCE_DENY_STORAGE_IDENTITY = 4,
	PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG = 5,
	PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH = 6,
	PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL = 7,
	PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_REJECTED = 8,
	PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_UNKNOWN = 9,
	PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE = 10,
	PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT = 11,
	PGRAC_EXTERNAL_FENCE_DENY_JOURNAL = 12,
	PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH = 13,
	PGRAC_EXTERNAL_FENCE_DENY_EXPIRED = 14,
	PGRAC_EXTERNAL_FENCE_DENY_DAEMON_RESTARTED = 15,
	PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED = 16,
	PGRAC_EXTERNAL_FENCE_DENY_MAPPING_CHANGED = 17,
	PGRAC_EXTERNAL_FENCE_DENY_MIXED_VERSION = 18,
	PGRAC_EXTERNAL_FENCE_DENY_REJOIN_INVALIDATED = 19,
	PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE = 20,
	PGRAC_EXTERNAL_FENCE_DENY_ROOT_NOT_COMPLETE = 21,
	PGRAC_EXTERNAL_FENCE_DENY_ROOT_STALE = 22,
	PGRAC_EXTERNAL_FENCE_DENY_REJOIN_OFFER_MISMATCH = 23,
	PGRAC_EXTERNAL_FENCE_DENY_JOIN_CANDIDATE_MISMATCH = 24,
	PGRAC_EXTERNAL_FENCE_DENY_JOIN_NOT_READY = 25,
	PGRAC_EXTERNAL_FENCE_DENY_REJOIN_CONSUMED = 26,
	PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_MISSING = 27,
	PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_STALE = 28,
	PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH = 29,
	PGRAC_EXTERNAL_FENCE_DENY_IO_NOT_DRAINED = 30,
	PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR = 31
} PgracExternalFenceDenyReason;

typedef enum PgracExternalFenceRejoinStatus {
	PGRAC_EXTERNAL_FENCE_REJOIN_PENDING = 0,
	PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED = 1,
	PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT = 2,
	PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER = 3,
	PGRAC_EXTERNAL_FENCE_REJOIN_READY = 4,
	PGRAC_EXTERNAL_FENCE_REJOIN_REJECTED = 5,
	PGRAC_EXTERNAL_FENCE_REJOIN_UNKNOWN = 6,
	PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE = 7,
	PGRAC_EXTERNAL_FENCE_REJOIN_STALE = 8,
	PGRAC_EXTERNAL_FENCE_REJOIN_CONSUMED = 9
} PgracExternalFenceRejoinStatus;

typedef enum PgracExternalFenceNeedSetResult {
	PGRAC_EXTERNAL_FENCE_NEED_SET_OK = 0,
	PGRAC_EXTERNAL_FENCE_NEED_SET_BAD_ARGUMENT = 1,
	PGRAC_EXTERNAL_FENCE_NEED_SET_DUTY_INVALID = 2,
	PGRAC_EXTERNAL_FENCE_NEED_SET_CAPABILITY_UNAVAILABLE = 3,
	PGRAC_EXTERNAL_FENCE_NEED_SET_FENCE_AUTHORITY_UNAVAILABLE = 4,
	PGRAC_EXTERNAL_FENCE_NEED_SET_MEMBERSHIP_UNSTABLE = 5,
	PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_COUNT_INVALID = 6,
	PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_INCAR_UNPROVEN = 7,
	PGRAC_EXTERNAL_FENCE_NEED_SET_ORIGINAL_OWNER_MISSING = 8,
	PGRAC_EXTERNAL_FENCE_NEED_SET_STORAGE_UNAVAILABLE = 9
} PgracExternalFenceNeedSetResult;

typedef struct PgracExternalFenceWriterV1 {
	int32 node_id;
	uint32 reserved0;
	uint64 incarnation;
} PgracExternalFenceWriterV1;

typedef struct PgracExternalFenceWriterSetDigest {
	uint8 bytes[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
} PgracExternalFenceWriterSetDigest;

typedef struct PgracExternalFenceNeedV1 {
	uint64 system_identifier;
	ClusterRecoveryDutyDigest canonical_duty_digest;
	int32 victim_node_id;
	uint32 reserved0;
	uint64 victim_incarnation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceNeedV1;

typedef struct PgracExternalFenceBindingV1 {
	uint64 system_identifier;
	ClusterRecoveryDutyDigest canonical_duty_digest;
	int32 victim_node_id;
	uint32 reserved0;
	uint64 victim_incarnation;
	uint64 target_mapping_generation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceBindingV1;

typedef struct PgracExternalFenceRejoinOfferV1 {
	uint8 operation_id[16];
	int32 old_node_id;
	uint32 reserved20;
	uint64 old_incarnation;
	uint64 candidate_incarnation;
} PgracExternalFenceRejoinOfferV1;

typedef struct PgracExternalFenceRejoinNeedV1 {
	uint64 system_identifier;
	uint8 rejoin_gate_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	int32 old_node_id;
	uint32 reserved44;
	uint64 old_incarnation;
	uint64 candidate_incarnation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceRejoinNeedV1;

typedef struct PgracExternalFenceRejoinBindingV1 {
	uint64 system_identifier;
	uint8 rejoin_gate_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	int32 old_node_id;
	uint32 reserved44;
	uint64 old_incarnation;
	uint64 candidate_incarnation;
	uint64 target_mapping_generation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceRejoinBindingV1;

/* STOP04 §2.4.1: coordinator-LMON semantic snapshots.  These values are
 * process-local inputs to the opaque rejoin authority; they are never a disk,
 * wire or shared-memory ABI. */
typedef struct ClusterReconfigRejoinFailureSnapshotV1 {
	uint32 reconfig_kind;
	uint32 reserved0;
	uint64 event_id;
	uint64 new_epoch;
	uint64 cssd_dead_generation;
	uint8 dead_bitmap[16];
	uint8 survivor_bitmap[16];
	int32 old_node_id;
	uint32 reserved68;
	uint64 old_incarnation;
} ClusterReconfigRejoinFailureSnapshotV1;

typedef struct ClusterGrdRejoinClearSnapshotV1 {
	uint64 episode_epoch;
	uint64 dead_bitmap_hash;
	uint8 survivor_bitmap[16];
} ClusterGrdRejoinClearSnapshotV1;

typedef struct ClusterReconfigRejoinPendingSnapshotV1 {
	uint32 reconfig_kind;
	uint32 reserved0;
	uint64 event_id;
	uint64 old_epoch;
	uint64 new_epoch;
	uint64 cssd_dead_generation;
	uint8 dead_bitmap[16];
	uint8 join_bitmap[16];
	int32 node_id;
	uint32 reserved76;
	uint64 candidate_incarnation;
	uint64 observed_slot_generation;
} ClusterReconfigRejoinPendingSnapshotV1;

StaticAssertDecl(sizeof(ClusterRecoveryDutyDigest) == 32,
				 "recovery duty digest must remain 32 bytes");
StaticAssertDecl(sizeof(PgracExternalFenceWriterV1) == 16,
				 "external fence writer v1 must remain 16 bytes");
StaticAssertDecl(offsetof(PgracExternalFenceWriterV1, node_id) == 0, "writer node offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceWriterV1, reserved0) == 4,
				 "writer reserved offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceWriterV1, incarnation) == 8,
				 "writer incarnation offset changed");
StaticAssertDecl(sizeof(PgracExternalFenceWriterSetDigest) == 32,
				 "external fence writer-set digest must remain 32 bytes");
StaticAssertDecl(sizeof(PgracExternalFenceNeedV1) == 96,
				 "external fence need v1 must remain 96 bytes");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, system_identifier) == 0,
				 "need system identifier offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, canonical_duty_digest) == 8,
				 "need duty digest offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, victim_node_id) == 40,
				 "need victim node offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, reserved0) == 44,
				 "need reserved offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, victim_incarnation) == 48,
				 "need victim incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, protected_set_digest) == 56,
				 "need protected-set digest offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, predicate_id) == 88,
				 "need predicate id offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceNeedV1, predicate_version) == 92,
				 "need predicate version offset changed");
StaticAssertDecl(sizeof(PgracExternalFenceBindingV1) == 104,
				 "external fence binding v1 must remain 104 bytes");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, system_identifier) == 0,
				 "binding system identifier offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, canonical_duty_digest) == 8,
				 "binding duty digest offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, victim_node_id) == 40,
				 "binding victim node offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, reserved0) == 44,
				 "binding reserved offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, victim_incarnation) == 48,
				 "binding victim incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, target_mapping_generation) == 56,
				 "binding mapping generation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, protected_set_digest) == 64,
				 "binding protected-set digest offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, predicate_id) == 96,
				 "binding predicate id offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceBindingV1, predicate_version) == 100,
				 "binding predicate version offset changed");
StaticAssertDecl(sizeof(PgracExternalFenceRejoinOfferV1) == 40,
				 "external fence rejoin offer v1 must remain 40 bytes");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinOfferV1, operation_id) == 0,
				 "rejoin offer operation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinOfferV1, old_node_id) == 16,
				 "rejoin offer node offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinOfferV1, reserved20) == 20,
				 "rejoin offer reserved offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinOfferV1, old_incarnation) == 24,
				 "rejoin offer old incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinOfferV1, candidate_incarnation) == 32,
				 "rejoin offer candidate incarnation offset changed");
StaticAssertDecl(sizeof(PgracExternalFenceRejoinNeedV1) == 104,
				 "external fence rejoin need v1 must remain 104 bytes");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, system_identifier) == 0,
				 "rejoin need system identifier offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, rejoin_gate_digest) == 8,
				 "rejoin need gate digest offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, old_node_id) == 40,
				 "rejoin need node offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, reserved44) == 44,
				 "rejoin need reserved offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, old_incarnation) == 48,
				 "rejoin need old incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, candidate_incarnation) == 56,
				 "rejoin need candidate incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, protected_set_digest) == 64,
				 "rejoin need protected-set offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, predicate_id) == 96,
				 "rejoin need predicate offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinNeedV1, predicate_version) == 100,
				 "rejoin need predicate version offset changed");
StaticAssertDecl(sizeof(PgracExternalFenceRejoinBindingV1) == 112,
				 "external fence rejoin binding v1 must remain 112 bytes");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, system_identifier) == 0,
				 "rejoin binding system identifier offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, rejoin_gate_digest) == 8,
				 "rejoin binding gate digest offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, old_node_id) == 40,
				 "rejoin binding node offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, reserved44) == 44,
				 "rejoin binding reserved offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, old_incarnation) == 48,
				 "rejoin binding old incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, candidate_incarnation) == 56,
				 "rejoin binding candidate incarnation offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, target_mapping_generation) == 64,
				 "rejoin binding mapping offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, protected_set_digest) == 72,
				 "rejoin binding protected-set offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, predicate_id) == 104,
				 "rejoin binding predicate offset changed");
StaticAssertDecl(offsetof(PgracExternalFenceRejoinBindingV1, predicate_version) == 108,
				 "rejoin binding predicate version offset changed");
StaticAssertDecl(sizeof(ClusterReconfigRejoinFailureSnapshotV1) == 80,
				 "rejoin failure snapshot v1 size changed");
StaticAssertDecl(offsetof(ClusterReconfigRejoinFailureSnapshotV1, old_incarnation) == 72,
				 "rejoin failure old incarnation offset changed");
StaticAssertDecl(sizeof(ClusterGrdRejoinClearSnapshotV1) == 32,
				 "rejoin GRD clear snapshot v1 size changed");
StaticAssertDecl(offsetof(ClusterGrdRejoinClearSnapshotV1, survivor_bitmap) == 16,
				 "rejoin GRD clear survivor offset changed");
StaticAssertDecl(sizeof(ClusterReconfigRejoinPendingSnapshotV1) == 96,
				 "rejoin pending snapshot v1 size changed");
StaticAssertDecl(offsetof(ClusterReconfigRejoinPendingSnapshotV1, observed_slot_generation) == 88,
				 "rejoin pending observed generation offset changed");

typedef struct PgracExternalFenceAdmissionV1 PgracExternalFenceAdmissionV1;
typedef struct PgracExternalFenceNeedSetV1 PgracExternalFenceNeedSetV1;
typedef struct PgracExternalFenceAdmissionSetV1 PgracExternalFenceAdmissionSetV1;
typedef struct PgracExternalFenceRejoinOpV1 PgracExternalFenceRejoinOpV1;
typedef struct PgracExternalFenceRejoinAuthorityClearV1 PgracExternalFenceRejoinAuthorityClearV1;

extern bool cluster_reconfig_get_observed_slot_coherent(int32 node_id, uint64 *out_incarnation,
														uint64 *out_generation);
extern bool
cluster_reconfig_rejoin_failure_snapshot(int32 old_node_id, uint64 old_incarnation,
										 ClusterReconfigRejoinFailureSnapshotV1 *out_failure);
extern bool cluster_grd_rejoin_clear_snapshot(const ClusterReconfigRejoinFailureSnapshotV1 *failure,
											  ClusterGrdRejoinClearSnapshotV1 *out_clear);
extern bool
cluster_reconfig_rejoin_pending_snapshot(const ClusterReconfigRejoinFailureSnapshotV1 *failure,
										 uint64 candidate_incarnation,
										 ClusterReconfigRejoinPendingSnapshotV1 *out_pending);
extern bool
cluster_reconfig_rejoin_pending_ready(const ClusterReconfigRejoinPendingSnapshotV1 *pending);

/* STOP04 §11.7 current-package policy.  This remains false until a later
 * explicit provider selection and deployment certification authorizes bit24.
 * Keeping the check as a production callsite leaves the approved LMON handoff
 * dormant without weakening ordinary online join. */
extern bool cluster_external_fence_runtime_active(void);
extern bool
cluster_external_fence_rejoin_protected_set_digest(uint8 out[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES]);

extern PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_start_async(int timeout_ms, PgracExternalFenceRejoinOpV1 **out_op);
extern PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_poll_nowait(PgracExternalFenceRejoinOpV1 *op,
										  PgracExternalFenceDenyReason *reason);
extern const PgracExternalFenceRejoinOfferV1 *
cluster_external_fence_rejoin_offer(const PgracExternalFenceRejoinOpV1 *op);
extern PgracExternalFenceRejoinStatus cluster_external_fence_rejoin_authority_clear_build(
	const PgracExternalFenceRejoinOpV1 *offered_op,
	const ClusterReconfigRejoinFailureSnapshotV1 *failure,
	const ClusterGrdRejoinClearSnapshotV1 *grd_clear,
	PgracExternalFenceRejoinAuthorityClearV1 **out_clear, PgracExternalFenceDenyReason *reason);
extern PgracExternalFenceRejoinStatus cluster_external_fence_rejoin_authorize_on_async(
	PgracExternalFenceRejoinOpV1 *op, PgracExternalFenceRejoinAuthorityClearV1 **authority_clear,
	const ClusterControlRootIdentity *old_identity,
	const ClusterControlRootSnapshot *complete_snapshot,
	const ClusterControlRootReadToken *complete_token, const uint8 protected_set_digest[32],
	PgracExternalFenceDenyReason *reason);
extern PgracExternalFenceRejoinStatus cluster_external_fence_rejoin_refresh_on_async(
	PgracExternalFenceRejoinOpV1 *op, const ClusterReconfigRejoinPendingSnapshotV1 *pending,
	PgracExternalFenceDenyReason *reason);
extern bool
cluster_external_fence_rejoin_revalidate_root(PgracExternalFenceRejoinOpV1 *op,
											  ClusterControlRootSnapshot *out_fresh_snapshot,
											  PgracExternalFenceDenyReason *reason);
extern bool cluster_external_fence_rejoin_consume_nowait(
	PgracExternalFenceRejoinOpV1 *op, const ClusterReconfigRejoinPendingSnapshotV1 *current_pending,
	const ClusterJoinCommitMarker *committed_candidate, PgracExternalFenceDenyReason *reason);
extern const PgracExternalFenceRejoinBindingV1 *
cluster_external_fence_rejoin_binding(const PgracExternalFenceRejoinOpV1 *op);
extern void cluster_external_fence_rejoin_release(PgracExternalFenceRejoinOpV1 **op);
extern void cluster_external_fence_rejoin_authority_clear_release(
	PgracExternalFenceRejoinAuthorityClearV1 **authority_clear);

/* STOP04 §11.10: the public set ABI is keyed by the exact full duty and borrows
 * one formation object from build through final no-wait revalidation.  There is
 * no scalar generation argument or hidden root/formation fetch. */
extern PgracExternalFenceNeedSetResult
cluster_external_fence_need_set_build(const ClusterRecoveryDutyKey *duty,
									  const ClusterFormationWitnessV1 *formation,
									  PgracExternalFenceNeedSetV1 **out);
extern bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason);
extern PgracExternalFenceVerdict
cluster_external_fence_admit_set_wait(const PgracExternalFenceNeedSetV1 *needs,
									  const ClusterFormationWitnessV1 *formation, int timeout_ms,
									  PgracExternalFenceAdmissionSetV1 **out);
extern bool cluster_external_fence_revalidate_set_nowait(
	const PgracExternalFenceAdmissionSetV1 *admissions, const PgracExternalFenceNeedSetV1 *needs,
	const ClusterFormationWitnessV1 *formation, PgracExternalFenceDenyReason *reason);

extern PgracExternalFenceVerdict
cluster_external_fence_admit_wait(const PgracExternalFenceNeedV1 *need, int timeout_ms,
								  PgracExternalFenceAdmissionV1 **out);
extern bool cluster_external_fence_revalidate_nowait(const PgracExternalFenceAdmissionV1 *admission,
													 const PgracExternalFenceNeedV1 *current,
													 PgracExternalFenceDenyReason *reason);
extern const PgracExternalFenceBindingV1 *
cluster_external_fence_admission_binding(const PgracExternalFenceAdmissionV1 *admission);
extern void cluster_external_fence_admission_release(PgracExternalFenceAdmissionV1 *admission);

extern uint32 cluster_external_fence_need_set_count(const PgracExternalFenceNeedSetV1 *set);
extern const PgracExternalFenceNeedV1 *
cluster_external_fence_need_set_at(const PgracExternalFenceNeedSetV1 *set, uint32 index);
extern const PgracExternalFenceWriterSetDigest *
cluster_external_fence_need_set_digest(const PgracExternalFenceNeedSetV1 *set);
extern void cluster_external_fence_need_set_release(PgracExternalFenceNeedSetV1 **set);

extern uint32
cluster_external_fence_admission_set_count(const PgracExternalFenceAdmissionSetV1 *set);
extern const PgracExternalFenceBindingV1 *
cluster_external_fence_admission_set_binding_at(const PgracExternalFenceAdmissionSetV1 *set,
												uint32 index);
extern const PgracExternalFenceWriterSetDigest *
cluster_external_fence_admission_set_digest(const PgracExternalFenceAdmissionSetV1 *set);
extern void cluster_external_fence_admission_set_release(PgracExternalFenceAdmissionSetV1 **set);

/* Diagnostic only; never authority. */
extern PgracExternalFenceDenyReason cluster_external_fence_last_deny_reason(void);

#endif /* CLUSTER_EXTERNAL_FENCE_H */
