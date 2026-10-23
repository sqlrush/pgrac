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
	return FullTransactionIdIsValid(FullTransactionIdFromU64(value));
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
 * legal striped allocation. The cutoff still obeys the caller's existing
 * conservative ceiling. An available global candidate may only lower it;
 * neither observation is a terminal or freeze certificate.
 */
bool
cluster_heap_freeze_policy_full_v1(TransactionId ceiling, TransactionId frozen,
	int min_age, int table_age, FullTransactionId *freeze_limit, bool *aggressive)
{
	ClusterXidStripeSampleFence fence;
	ClusterHeapFreezeWatermark watermark;
	FullTransactionId legal_next, frozen_full, ceiling_full;
	uint64 next, oldest, bound, candidate;
	bool sampled, have_watermark;

	if (freeze_limit == NULL || aggressive == NULL || min_age < 0 || table_age < 0
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
	if (oldest > next || bound > next || next - oldest >= UINT64CONST(0x80000000)
		|| next - bound >= UINT64CONST(0x80000000))
		return false;
	/*
	 * Saturate before subtraction at the first valid FullXid, and skip
	 * raw 0..2 within later epochs. Never move the original ceiling.
	 */
	candidate = next > (uint64)min_age ? next - (uint64)min_age : FirstNormalTransactionId;
	candidate = Max(candidate, FirstNormalTransactionId);
	if ((TransactionId)candidate < FirstNormalTransactionId)
		candidate += FirstNormalTransactionId - (TransactionId)candidate;
	candidate = Min(candidate, bound);
	if (have_watermark)
		candidate = Min(candidate, watermark.active_full_xid_min);
	/* A more restrictive global F can be older than the representable raw
	 * window. Do not turn that FullXid into an ambiguous raw cutoff. */
	if (candidate < FirstNormalTransactionId || !TransactionIdIsNormal((TransactionId)candidate)
		|| candidate > next || next - candidate >= UINT64CONST(0x80000000))
		return false;
	*freeze_limit = FullTransactionIdFromU64(candidate);
	*aggressive = next - oldest >= (uint64)table_age;
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
