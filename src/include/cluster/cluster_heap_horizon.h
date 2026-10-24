/* Shared heap's conservative native freeze boundary.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_HEAP_HORIZON_H
#define CLUSTER_HEAP_HORIZON_H

#include "access/transam.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"

/* Before heap mutation, cap a local proposal at the immutable activation
 * floor: another slot can still issue an XID below this node's proposal.
 * Not a complete cluster freeze/reclamation horizon. Refusal preserves out.
 * The caller excludes temporary relations. Nonshared mode is native. */
extern bool cluster_heap_freeze_cutoff_v1(TransactionId proposed, TransactionId *out);

/* Shared permanent heap's age policies, using one current durable legal-next
 * FullXid observation. The caller supplies its existing conservative ceiling;
 * an available published global candidate may only lower it. An unavailable
 * candidate preserves the old v1 policy. Failure preserves both outputs; the
 * caller must scan without advancing its freeze boundary. This grants no
 * tuple freeze, producer handoff, or truncation authority. */
extern bool cluster_heap_freeze_policy_v1(TransactionId ceiling, TransactionId frozen, int min_age,
										  int table_age, TransactionId *freeze_limit,
										  bool *aggressive);

/* Same policy and single legal-next sample, retaining the exact FullXid
 * result instead of projecting away its epoch. Both outputs are unchanged
 * on refusal. This is a conservative policy observation, not F/H admission. */
extern bool cluster_heap_freeze_policy_full_v1(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, FullTransactionId *freeze_limit, bool *aggressive);

/* Same original legal-next cut and allocation fence; still not F/H authority. */
typedef struct ClusterHeapFreezePolicySampleV1 {
	FullTransactionId legal_next;
	FullTransactionId freeze_limit;
	ClusterXidStripeSampleFence allocation_fence;
	bool aggressive;
} ClusterHeapFreezePolicySampleV1;

/* Failure preserves out. Keep the returned witness unchanged. The current
 * check never samples nextXid again; call without page/census locks. */
extern bool cluster_heap_freeze_policy_sample_v1(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, ClusterHeapFreezePolicySampleV1 *out);
extern bool cluster_heap_freeze_policy_sample_current_v1(
	const ClusterHeapFreezePolicySampleV1 *sample);

/*
 * Frozen 2.14 M1 waterline report.  The wire image is deliberately encoded
 * field-by-field in little-endian order; sizeof(ClusterHeapFreezeReportV2)
 * is not part of the ABI.  The report reuses the undo-horizon interconnect
 * message number while advertising the V2 capability bit.
 */
#define CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION 2
#define CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES 120

#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION 0
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH 2
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS 4
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID 8
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RECORD_GENERATION 16
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FORMATION_EPOCH 24
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_INCARNATION 32
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REPORT_SEQUENCE 40
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FUTURE_FULL_XID 48
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ACTIVE_FULL_XID_MIN 56
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_HORIZON_SCN 64
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REQUESTED_FULL_XID 72
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN 80
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS 84
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_F_SAMPLE_ID 88
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_SAMPLE_CLOCK_SCN 96
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND 104
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0 108
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN 112

typedef enum ClusterHeapFreezeReportV2ContributorKind {
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN = 0,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK = 1,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT = 2,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED = 3,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION = 4
} ClusterHeapFreezeReportV2ContributorKind;

typedef struct ClusterHeapFreezeReportV2 {
	uint16 version;
	uint16 length;
	uint32 flags;
	uint64 sysid;
	uint64 record_generation;
	uint64 formation_epoch;
	uint64 incarnation;
	uint64 report_sequence;
	uint64 future_full_xid;
	uint64 active_full_xid_min;
	uint64 horizon_scn;
	uint64 requested_full_xid;
	uint32 origin;
	uint32 sender_interval_ms;
	uint64 f_sample_id;
	uint64 h_sample_clock_scn;
	uint32 h_contributor_kind;
	uint32 reserved0;
	uint64 h_contributor_token;
} ClusterHeapFreezeReportV2;

extern bool cluster_heap_freeze_report_v2_encode(const ClusterHeapFreezeReportV2 *report,
												 uint8 out[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES]);
extern bool cluster_heap_freeze_report_v2_decode(const uint8 *bytes, size_t length,
												 ClusterHeapFreezeReportV2 *out);

/* Volatile receive observation only, not permission to advance a floor.
 * F has no age expiry; consumers must still enforce H freshness, backward
 * clock and regression checks and capture/recheck the full fixed cohort.
 * A failed bounded read zeroes out. Original semantic/connection publishers
 * retire the identity; reconnect alone cannot reset sequence monotonicity. */
typedef struct ClusterHeapFreezeReportV2View {
	ClusterHeapFreezeReportV2 report;
	uint64 recv_at_us;
	bool horizon_regression_flagged;
} ClusterHeapFreezeReportV2View;

extern bool cluster_heap_freeze_report_v2_sample(int32 peer, ClusterHeapFreezeReportV2View *out);

typedef enum ClusterHeapFreezeCohortStatus {
	CLUSTER_HEAP_FREEZE_COHORT_READY = 0,
	CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE,
	CLUSTER_HEAP_FREEZE_COHORT_H_STALE,
	CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH,
	CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED,
	CLUSTER_HEAP_FREEZE_COHORT_INVALID
} ClusterHeapFreezeCohortStatus;

/* Same receive publication identity used by the original fold and watermark;
 * covered is maintenance intent coverage, never freeze permission. */
typedef struct ClusterHeapFreezeCohortPeerObservation {
	uint64 recv_at_us;
	uint32 interval_ms;
	uint32 generation;
	uint32 seq;
	uint64 covered;
} ClusterHeapFreezeCohortPeerObservation;

/* Read-only observation of the entire admitted cohort, never a freeze or
 * truncation certificate. Producer protection closure is still required
 * before these values may authorize a global floor. F and H use different
 * domains: F is a FullXid lower bound, H is an SCN history bound. */
typedef struct ClusterHeapFreezeCohortObservation {
	uint64 future_full_xid_max;	   /* request ceiling, never a freeze floor */
	uint64 requested_full_xid_max; /* intent only; may exceed the proven ceiling */
	uint64 active_full_xid_min;
	uint64 horizon_scn;
	uint64 record_generation;
	uint64 formation_epoch;
	uint64 admitted_members_lo;
	uint64 admitted_members_hi;
	/* Same local completed sample; remote reports do not prove identity coverage. */
	uint64 local_unqualified_entries;
	/* Original completed local scan, retained verbatim for the final local
	 * registration-cut check. No remote/provenance/retention authority. */
	ClusterCtrcActiveScan local_registration_cut;
	/* Original fixed-cohort cut, retained verbatim for the existing current
	 * getter. Keep unchanged; not a producer-retention certificate. */
	ClusterSemanticActiveSampleFence admission_fence;
	/* Original local H from the same single history sample used by the fold. */
	uint64 source_local_horizon_scn;
	/* Kept outside the peer identity type, which is also stored in shmem. */
	uint64 source_peer_horizon_scn[CLUSTER_MAX_NODES];
	uint64 source_local_sample_id;
	uint64 source_started_at_us;
	uint32 source_interval_ms;
	ClusterHeapFreezeCohortPeerObservation source_peers[CLUSTER_MAX_NODES];
} ClusterHeapFreezeCohortObservation;

/* Pure match of retained, separately current-checked inputs. These are
 * candidate bounds only, not a positive F/H or producer-retention certificate.
 * The selected Min(policy,F) must remain strictly inside the original
 * policy legal-next past half-space; a higher ignored F does not qualify
 * or invalidate that selected bound. The retained H must equal the original
 * SCN-time minimum of its local sample and exactly the required peer sources,
 * preserving the original fold order for equal-time values.
 * No nextXid/H sample, current fetch, lock or identity callback is performed.
 * Failure preserves *out; the caller still owns the original admission and
 * must qualify provenance/retention before any native plan or shared write. */
typedef struct ClusterHeapFreezeObservedBoundsV1 {
	FullTransactionId freeze_limit;
	uint64 horizon_scn;
} ClusterHeapFreezeObservedBoundsV1;

extern bool cluster_heap_freeze_observed_bounds_v1(
	const ClusterHeapFreezePolicySampleV1 *policy,
	const ClusterHeapFreezeCohortObservation *cohort,
	const ClusterSemanticAdmissionToken *admission, int local_node_id,
	ClusterHeapFreezeObservedBoundsV1 *out);

/* Bounded reads, fixed required set, current connection/admission rechecks.
 * Only READY writes out; every refusal preserves it. An old completed F
 * remains usable with fresh H; F itself has no age expiry. */
extern ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_observe(ClusterHeapFreezeCohortObservation *out);

/* Keep the original observation unchanged. Recheck its completed local sample,
 * exact peer publications/connections and H age after local identity checks;
 * never resample N/H or refold reports. Call without page/census locks.
 * Replacing a source publication invalidates this combined observation; there
 * is no age expiry on F itself. This still grants no retention/freeze authority. */
extern ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_current(const ClusterHeapFreezeCohortObservation *expected);

/*
 * A published, identity-bound candidate from one READY fixed-cohort fold.
 * This is a volatile observation for the 2.4 producer/consumer handoff: it
 * is not a freeze certificate, native truncation permission, or proof that
 * every retained FullXid producer has completed its handoff.  The reader
 * rechecks the worker sample, original required peer publications/connections
 * and semantic fence before accepting it. A changed source retires this
 * combined candidate until a new complete fold; F itself has no age expiry.
 */
typedef struct ClusterHeapFreezeWatermark {
	uint64 future_full_xid_max;
	uint64 active_full_xid_min;
	uint64 horizon_scn;
	uint64 publication_sequence;
	uint64 f_sample_id;
	uint64 published_at_us;
	uint64 record_generation;
	uint64 formation_epoch;
	uint64 admitted_members_lo;
	uint64 admitted_members_hi;
} ClusterHeapFreezeWatermark;

/* Bounded, read-only sample of the latest READY global candidate. */
extern bool cluster_heap_freeze_watermark_sample(ClusterHeapFreezeWatermark *out);

/* Best-effort explicit FREEZE intent, selected from a complete current
 * cohort's observed future maximum. One volatile, identity-scoped local
 * slot coalesces requests; the original LMON V2 cadence carries it and the
 * original QVOTEC consumer alone handles durability. No voting-disk IO or
 * synchronous peer request here. True means queued, never freeze permission. */
extern bool cluster_heap_freeze_request_maintenance(void);

#endif
