/* Conservative shared heap freeze metadata, before any page mutation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "storage/lwlock.h"

static inline void
heap_freeze_report_v2_put_le16(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)(value & 0xFF);
	dst[1] = (uint8)((value >> 8) & 0xFF);
}

static inline void
heap_freeze_report_v2_put_le32(uint8 *dst, uint32 value)
{
	int i;

	for (i = 0; i < 4; i++)
		dst[i] = (uint8)((value >> (8 * i)) & 0xFF);
}

static inline void
heap_freeze_report_v2_put_le64(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		dst[i] = (uint8)((value >> (8 * i)) & 0xFF);
}

static inline uint16
heap_freeze_report_v2_get_le16(const uint8 *src)
{
	return (uint16)src[0] | ((uint16)src[1] << 8);
}

static inline uint32
heap_freeze_report_v2_get_le32(const uint8 *src)
{
	return (uint32)src[0] | ((uint32)src[1] << 8) | ((uint32)src[2] << 16)
		   | ((uint32)src[3] << 24);
}

static inline uint64
heap_freeze_report_v2_get_le64(const uint8 *src)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= (uint64)src[i] << (8 * i);
	return value;
}

static bool
heap_freeze_report_v2_kind_valid(uint32 kind)
{
	return kind <= CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION;
}

static bool
heap_freeze_report_v2_full_xid_valid(uint64 value)
{
	/* A nonzero epoch does not make a special raw XID normal. */
	return value != UINT64_MAX
		&& TransactionIdIsNormal(XidFromFullTransactionId(FullTransactionIdFromU64(value)));
}

static bool
heap_freeze_report_v2_valid(const ClusterHeapFreezeReportV2 *report)
{
	if (report == NULL || report->version != CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION
		|| report->length != CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES || report->flags != 0
		|| report->reserved0 != 0
		|| !heap_freeze_report_v2_kind_valid(report->h_contributor_kind)
		|| report->sender_interval_ms < CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS
		|| report->sender_interval_ms > CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS)
		return false;
	if (report->sysid == 0 || report->record_generation == 0 || report->formation_epoch == 0
		|| report->incarnation == 0 || report->report_sequence == 0 || report->f_sample_id == 0)
		return false;
	if (!SCN_VALID((SCN)report->horizon_scn)
		|| report->horizon_scn == CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED
		|| !SCN_VALID((SCN)report->h_sample_clock_scn))
		return false;
	if (!heap_freeze_report_v2_full_xid_valid(report->future_full_xid)
		|| !heap_freeze_report_v2_full_xid_valid(report->active_full_xid_min)
		|| report->active_full_xid_min > report->future_full_xid)
		return false;
	if (report->requested_full_xid != 0
		&& !heap_freeze_report_v2_full_xid_valid(report->requested_full_xid))
		return false;
	return true;
}

bool
cluster_heap_freeze_report_v2_encode(const ClusterHeapFreezeReportV2 *report,
								 uint8 out[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES])
{
	uint8 image[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	if (out == NULL || !heap_freeze_report_v2_valid(report))
		return false;
	memset(image, 0, sizeof(image));
	heap_freeze_report_v2_put_le16(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION,
								  report->version);
	heap_freeze_report_v2_put_le16(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH,
								  report->length);
	heap_freeze_report_v2_put_le32(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS,
								  report->flags);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID,
								  report->sysid);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RECORD_GENERATION,
								  report->record_generation);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FORMATION_EPOCH,
								  report->formation_epoch);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_INCARNATION,
								  report->incarnation);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REPORT_SEQUENCE,
								  report->report_sequence);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FUTURE_FULL_XID,
								  report->future_full_xid);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ACTIVE_FULL_XID_MIN,
								  report->active_full_xid_min);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_HORIZON_SCN,
								  report->horizon_scn);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REQUESTED_FULL_XID,
								  report->requested_full_xid);
	heap_freeze_report_v2_put_le32(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN,
								  report->origin);
	heap_freeze_report_v2_put_le32(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS,
								  report->sender_interval_ms);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_F_SAMPLE_ID,
								  report->f_sample_id);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_SAMPLE_CLOCK_SCN,
								  report->h_sample_clock_scn);
	heap_freeze_report_v2_put_le32(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND,
								  report->h_contributor_kind);
	heap_freeze_report_v2_put_le32(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0,
								  report->reserved0);
	heap_freeze_report_v2_put_le64(image + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN,
								  report->h_contributor_token);
	memcpy(out, image, sizeof(image));
	return true;
}

bool
cluster_heap_freeze_report_v2_decode(const uint8 *bytes, size_t length,
							 ClusterHeapFreezeReportV2 *out)
{
	ClusterHeapFreezeReportV2 decoded;

	if (bytes == NULL || out == NULL || length != CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES
		|| heap_freeze_report_v2_get_le16(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION)
			   != CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION
		|| heap_freeze_report_v2_get_le16(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH)
			   != CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES
		|| heap_freeze_report_v2_get_le32(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS) != 0
		|| heap_freeze_report_v2_get_le32(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0) != 0)
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.version = heap_freeze_report_v2_get_le16(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION);
	decoded.length = heap_freeze_report_v2_get_le16(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH);
	decoded.flags = heap_freeze_report_v2_get_le32(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS);
	decoded.sysid = heap_freeze_report_v2_get_le64(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID);
	decoded.record_generation = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RECORD_GENERATION);
	decoded.formation_epoch = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FORMATION_EPOCH);
	decoded.incarnation = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_INCARNATION);
	decoded.report_sequence = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REPORT_SEQUENCE);
	decoded.future_full_xid = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FUTURE_FULL_XID);
	decoded.active_full_xid_min = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ACTIVE_FULL_XID_MIN);
	decoded.horizon_scn = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_HORIZON_SCN);
	decoded.requested_full_xid = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REQUESTED_FULL_XID);
	decoded.origin = heap_freeze_report_v2_get_le32(bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN);
	decoded.sender_interval_ms = heap_freeze_report_v2_get_le32(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS);
	decoded.f_sample_id = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_F_SAMPLE_ID);
	decoded.h_sample_clock_scn = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_SAMPLE_CLOCK_SCN);
	decoded.h_contributor_kind = heap_freeze_report_v2_get_le32(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND);
	decoded.reserved0 = heap_freeze_report_v2_get_le32(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0);
	decoded.h_contributor_token = heap_freeze_report_v2_get_le64(
		bytes + CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN);
	if (!heap_freeze_report_v2_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}

bool
cluster_heap_freeze_cutoff_v1(TransactionId proposed, TransactionId *out)
{
	uint64 floor, epoch, generation, next, candidate;
	FullTransactionId next_full, proposed_full;
	TransactionId cutoff;

	if (out == NULL)
		return false;
	if (!cluster_shared_config) {
		*out = proposed;
		return true;
	}
	if (!cluster_enabled || !cluster_xid_striping || !TransactionIdIsNormal(proposed)
		|| !cluster_xid_stripe_get_activation(&floor, &epoch, &generation) || floor == 0
		|| epoch == 0 || generation == 0)
		return false;
	next_full = ReadNextFullTransactionId();
	next = U64FromFullTransactionId(next_full);
	if (!FullTransactionIdIsValid(next_full)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(next_full))
		|| (next >= floor ? next - floor : floor - next) >= UINT64_C(0x80000000))
		return false;
	proposed_full = cluster_xid_widen(proposed, next_full);
	if (!FullTransactionIdIsValid(proposed_full))
		return false;
	/* A full activation boundary may fall on raw 0..2 at epoch carry.
	 * Those special IDs cannot be issued by any stripe. */
	if ((uint32)floor < FirstNormalTransactionId)
		floor += FirstNormalTransactionId - (uint32)floor;
	candidate = Min(U64FromFullTransactionId(proposed_full), floor);
	cutoff = (TransactionId)candidate;
	if (!TransactionIdIsNormal(cutoff))
		return false;
	*out = cutoff;
	return true;
}

/*
 * Derive both VACUUM age policies from one original durable allocation
 * promise at the local nextXid cut. A lagging raw nextXid is not the next
 * legal striped allocation. The native policy keeps its existing deletion
 * ceiling; the independently proved shared policy uses the FullXid age bound.
 * An available global candidate may only lower either policy. Neither
 * observation is a terminal or freeze certificate.
 */
static bool
cluster_heap_freeze_policy_sample_common(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, bool native_ceiling, ClusterHeapFreezePolicySampleV1 *out)
{
	ClusterXidStripeSampleFence fence = {0};
	ClusterHeapFreezePolicySampleV1 sample;
	ClusterHeapFreezeWatermark watermark;
	FullTransactionId legal_next, frozen_full, ceiling_full;
	uint64 next, oldest, bound, candidate;
	bool sampled, have_watermark;

	if (out == NULL || min_age < 0 || table_age < 0
		|| !TransactionIdIsNormal(ceiling) || !TransactionIdIsNormal(frozen)
		|| !cluster_shared_config || !cluster_enabled || !cluster_xid_striping
		|| ShmemVariableCache == NULL)
		return false;

	LWLockAcquire(XidGenLock, LW_SHARED);
	sampled = cluster_xid_stripe_sample_future(ShmemVariableCache->nextXid, &legal_next, &fence);
	LWLockRelease(XidGenLock);
	if (!sampled || !FullTransactionIdIsValid(legal_next)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(legal_next)))
		return false;

	/* The bounded local reader must run outside XidGenLock. Refusal keeps
	 * the old v1 policy, never granting row-freeze authority. Recheck the
	 * original allocation fence after the additional observation. */
	have_watermark = cluster_heap_freeze_watermark_sample(&watermark);
	if (!cluster_xid_stripe_sample_fence_current(&fence))
		return false;

	frozen_full = cluster_xid_widen(frozen, legal_next);
	ceiling_full = cluster_xid_widen(ceiling, legal_next);
	if (!FullTransactionIdIsValid(frozen_full) || !FullTransactionIdIsValid(ceiling_full))
		return false;
	next = U64FromFullTransactionId(legal_next);
	oldest = U64FromFullTransactionId(frozen_full);
	bound = U64FromFullTransactionId(ceiling_full);
	if ((!native_ceiling && next == UINT64_MAX)
		|| oldest > next || bound > next || next - oldest >= UINT64CONST(0x80000000)
		|| next - bound >= UINT64CONST(0x80000000))
		return false;
	/*
	 * Saturate before subtraction at the first valid FullXid, and skip
	 * raw 0..2 within later epochs. The native policy must retain its ceiling.
	 */
	candidate = next > (uint64)min_age ? next - (uint64)min_age : FirstNormalTransactionId;
	candidate = Max(candidate, FirstNormalTransactionId);
	if ((TransactionId)candidate < FirstNormalTransactionId)
		candidate += FirstNormalTransactionId - (TransactionId)candidate;
	if (native_ceiling)
		candidate = Min(candidate, bound);
	if (have_watermark)
		candidate = Min(candidate, watermark.active_full_xid_min);
	/* A more restrictive global F can be older than the representable raw
	 * window. Do not turn that FullXid into an ambiguous raw cutoff. */
	if (candidate < FirstNormalTransactionId || !TransactionIdIsNormal((TransactionId)candidate)
		|| candidate > next || next - candidate >= UINT64CONST(0x80000000))
		return false;
	memset(&sample, 0, sizeof(sample));
	sample.legal_next = legal_next;
	sample.freeze_limit = FullTransactionIdFromU64(candidate);
	sample.allocation_fence = fence;
	sample.aggressive = next - oldest >= (uint64)table_age;
	*out = sample;
	return true;
}

bool
cluster_heap_freeze_policy_sample_v1(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, ClusterHeapFreezePolicySampleV1 *out)
{
	return cluster_heap_freeze_policy_sample_common(ceiling, frozen,
		min_age, table_age, true, out);
}

bool
cluster_heap_freeze_shared_policy_sample_v1(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, ClusterHeapFreezePolicySampleV1 *out)
{
	return cluster_heap_freeze_policy_sample_common(ceiling, frozen,
		min_age, table_age, false, out);
}

bool
cluster_heap_freeze_policy_full_v1(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, FullTransactionId *freeze_limit, bool *aggressive)
{
	ClusterHeapFreezePolicySampleV1 sample;

	if (freeze_limit == NULL || aggressive == NULL
		|| !cluster_heap_freeze_policy_sample_v1(ceiling, frozen, min_age, table_age, &sample))
		return false;
	*freeze_limit = sample.freeze_limit;
	*aggressive = sample.aggressive;
	return true;
}

/* The raw compatibility entry projects only a successfully checked FullXid;
 * it shares the computation and never takes a second legal-next sample. */
bool
cluster_heap_freeze_policy_v1(TransactionId ceiling, TransactionId frozen, int min_age,
	int table_age, TransactionId *freeze_limit, bool *aggressive)
{
	FullTransactionId full;
	bool scan;

	if (freeze_limit == NULL || aggressive == NULL
		|| !cluster_heap_freeze_policy_full_v1(ceiling, frozen, min_age, table_age, &full, &scan))
		return false;
	*freeze_limit = XidFromFullTransactionId(full);
	*aggressive = scan;
	return true;
}

bool
cluster_heap_freeze_policy_sample_current_v1(const ClusterHeapFreezePolicySampleV1 *sample)
{
	if (sample == NULL || !cluster_shared_config || !cluster_enabled || !cluster_xid_striping
		|| !FullTransactionIdIsValid(sample->legal_next)
		|| !FullTransactionIdIsValid(sample->freeze_limit)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(sample->legal_next))
		|| !TransactionIdIsNormal(XidFromFullTransactionId(sample->freeze_limit))
		|| U64FromFullTransactionId(sample->legal_next) == UINT64_MAX
		|| U64FromFullTransactionId(sample->freeze_limit) == UINT64_MAX
		|| FullTransactionIdFollows(sample->freeze_limit, sample->legal_next)
		|| U64FromFullTransactionId(sample->legal_next) - U64FromFullTransactionId(sample->freeze_limit)
		   >= UINT64CONST(0x80000000))
		return false;
	return cluster_xid_stripe_sample_fence_current(&sample->allocation_fence);
}

bool
cluster_heap_freeze_observed_bounds_v1(
	const ClusterHeapFreezePolicySampleV1 *policy,
	const ClusterHeapFreezeCohortObservation *cohort,
	const ClusterSemanticAdmissionToken *admission, int local_node_id,
	ClusterHeapFreezeObservedBoundsV1 *out)
{
	ClusterHeapFreezeObservedBoundsV1 bounds = { 0 };
	SCN source_horizon;
	uint64 source_min, source_future;

	if (policy == NULL || cohort == NULL || admission == NULL || out == NULL
		|| !admission->entered || admission->record_generation == 0
		|| admission->formation_epoch == 0 || local_node_id < 0 || local_node_id >= 128
		|| !FullTransactionIdIsValid(policy->legal_next)
		|| !FullTransactionIdIsValid(policy->freeze_limit)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(policy->legal_next))
		|| !TransactionIdIsNormal(XidFromFullTransactionId(policy->freeze_limit))
		|| U64FromFullTransactionId(policy->legal_next) == UINT64_MAX
		|| U64FromFullTransactionId(policy->freeze_limit) == UINT64_MAX
		|| FullTransactionIdFollows(policy->freeze_limit, policy->legal_next)
		|| U64FromFullTransactionId(policy->legal_next)
			- U64FromFullTransactionId(policy->freeze_limit) >= UINT64CONST(0x80000000)
		|| cohort->record_generation != admission->record_generation
		|| cohort->formation_epoch != admission->formation_epoch
		|| cohort->admission_fence.record_generation != cohort->record_generation
		|| cohort->admission_fence.formation_epoch != cohort->formation_epoch
		|| cohort->admission_fence.admitted_members_lo != cohort->admitted_members_lo
		|| cohort->admission_fence.admitted_members_hi != cohort->admitted_members_hi
		|| cohort->admission_fence.local_node_id != local_node_id
		|| cohort->local_unqualified_entries != 0
		|| !SCN_VALID((SCN)cohort->horizon_scn) || cohort->horizon_scn == UINT64_MAX
		|| !TransactionIdIsNormal((TransactionId)cohort->active_full_xid_min)
		|| !TransactionIdIsNormal((TransactionId)cohort->future_full_xid_max)
		|| cohort->future_full_xid_max == UINT64_MAX
		|| cohort->active_full_xid_min > cohort->future_full_xid_max
		|| ((local_node_id < 64
			 ? cohort->admitted_members_lo >> local_node_id
			 : cohort->admitted_members_hi >> (local_node_id - 64)) & 1) == 0)
		return false;

	/* Reproduce H from the same saved local cut and original required peer
	 * publications, in the original fold order and SCN time domain. Values
	 * from outside that fixed cohort cannot change this bound. This is a
	 * pure source match, not contributor-lifetime or retention authority. */
	/* Reproduce F from the same original completed local cut and required
	 * peer report inputs. A caller's aggregate cannot replace a missing or
	 * inconsistent source, even when the policy would ignore its higher F.
	 * Numeric equality still grants no producer-lifetime/retention authority. */
	source_min = U64FromFullTransactionId(cohort->local_registration_cut.min_full);
	source_future = U64FromFullTransactionId(cohort->local_registration_cut.future_full);
	if (cohort->local_registration_cut.status != CTRC_ACTIVE_SCAN_COMPLETE
		|| cohort->local_registration_cut.unqualified_entries != 0
		|| !TransactionIdIsNormal((TransactionId)source_min)
		|| !TransactionIdIsNormal((TransactionId)source_future)
		|| source_min == UINT64_MAX || source_future == UINT64_MAX
		|| source_min > source_future)
		return false;

	source_horizon = (SCN)cohort->source_local_horizon_scn;
	if (!SCN_VALID(source_horizon) || source_horizon == UINT64_MAX
		|| !SCN_VALID((SCN)cohort->source_local_history.sample_clock_scn)
		|| scn_time_cmp(source_horizon, (SCN)cohort->source_local_history.sample_clock_scn) > 0
		|| (uint32)cohort->source_local_history.contributor_kind
			> CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION)
		return false;
	for (int peer = 0; peer < CLUSTER_MAX_NODES; peer++)
	{
		uint64 members = peer < 64 ? cohort->admitted_members_lo : cohort->admitted_members_hi;
		SCN peer_horizon;
		uint64 peer_min, peer_future;

		if (peer == local_node_id || (members & (UINT64CONST(1) << (peer % 64))) == 0)
			continue;
		peer_min = cohort->source_peer_active_full_xid_min[peer];
		peer_future = cohort->source_peer_future_full_xid[peer];
		if (!TransactionIdIsNormal((TransactionId)peer_min)
			|| !TransactionIdIsNormal((TransactionId)peer_future)
			|| peer_min == UINT64_MAX || peer_future == UINT64_MAX
			|| peer_min > peer_future)
			return false;
		source_min = Min(source_min, peer_min);
		source_future = Max(source_future, peer_future);
		peer_horizon = (SCN)cohort->source_peer_horizon_scn[peer];
		if (!SCN_VALID(peer_horizon) || peer_horizon == UINT64_MAX
			|| !SCN_VALID((SCN)cohort->source_peer_history[peer].sample_clock_scn)
			|| scn_time_cmp(peer_horizon,
				(SCN)cohort->source_peer_history[peer].sample_clock_scn) > 0
			|| (uint32)cohort->source_peer_history[peer].contributor_kind
				> CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION)
			return false;
		if (scn_time_cmp(peer_horizon, source_horizon) < 0)
			source_horizon = peer_horizon;
	}
	if (source_min != cohort->active_full_xid_min
		|| source_future != cohort->future_full_xid_max
		|| (uint64)source_horizon != cohort->horizon_scn)
		return false;

	/* F only caps the original policy; H remains in its independent SCN
	 * domain. Neither numeric match supplies producer-retention authority. */
	bounds.freeze_limit = FullTransactionIdFromU64(
		Min(U64FromFullTransactionId(policy->freeze_limit), cohort->active_full_xid_min));
	/* The policy alone may fit N_legal while an older selected F does not.
	 * Check the final minimum before any consumer can narrow it to native
	 * XID semantics. A higher, ignored F must not reject the policy bound. */
	if (U64FromFullTransactionId(policy->legal_next)
		- U64FromFullTransactionId(bounds.freeze_limit) >= UINT64CONST(0x80000000))
		return false;
	bounds.horizon_scn = (uint64)source_horizon;
	memcpy(out, &bounds, sizeof(bounds));
	return true;
}
