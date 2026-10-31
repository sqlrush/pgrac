/*-------------------------------------------------------------------------
 * cluster_cold_recovery_xid.c
 *    Ordered original-source native counter inputs; no replay authority.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_cold_recovery.h"

static bool
cold_xid_source_valid(const ClusterColdParticipantV1 *source)
{
	return source != NULL && source->thread_id != 0
		   && source->thread_id <= CLUSTER_COLD_MAX_PARTICIPANTS && source->reserved_zero == 0
		   && source->timeline != 0 && source->owner_incarnation != 0
		   && source->physical_lower != InvalidXLogRecPtr
		   && source->native_redo >= source->physical_lower
		   && source->tail_end >= source->native_redo;
}

static bool
cold_xid_source_same(const ClusterColdParticipantV1 *a, const ClusterColdParticipantV1 *b)
{
	return cold_xid_source_valid(b) && a->thread_id == b->thread_id
		   && a->timeline == b->timeline && a->owner_incarnation == b->owner_incarnation
		   && a->physical_lower == b->physical_lower && a->native_redo == b->native_redo
		   && a->tail_end == b->tail_end;
}

static bool
cold_xid_fail(ClusterColdXidProgressV1 *progress)
{
	if (progress != NULL) {
		progress->failed = true;
		progress->next_xid = 0;
	}
	return false;
}

/* Same advancement as AdvanceNextFullTransactionIdPastXid, without shared
 * allocator access. Every header is observed, including a special raw xid.
 * Reject ambiguous half-range inputs and 64-bit rollover rather than
 * fabricating an era. This is an input refusal, never a loser decision. */
static bool
cold_xid_advance(uint64 *next, TransactionId xid)
{
	TransactionId current = (TransactionId)*next;
	uint32 epoch = (uint32)(*next >> 32);

	/* Native TransactionIdFollowsOrEquals uses numeric ordering if either
	 * operand is special. The current counter is always a normal xid. */
	if (!TransactionIdIsNormal(xid))
		return true;
	if ((uint32)(xid - current) == UINT32_C(0x80000000))
		return false;
	if (NormalTransactionIdPrecedes(xid, current))
		return true;
	TransactionIdAdvance(xid);
	if (xid < current) {
		if (epoch == UINT32_MAX)
			return false;
		epoch++;
	}
	*next = ((uint64)epoch << 32) | xid;
	return true;
}

bool
cluster_cold_xid_begin_v1(ClusterColdXidProgressV1 *progress,
						  const ClusterColdParticipantV1 *source,
						  uint64 checkpoint_next_xid, uint64 expected_records)
{
	if (progress == NULL)
		return false;
	memset(progress, 0, sizeof(*progress));
	if (!cold_xid_source_valid(source)
		|| !TransactionIdIsNormal((TransactionId)checkpoint_next_xid)
		|| (expected_records == 0 && source->native_redo != source->tail_end)
		|| (expected_records != 0
			&& (source->native_redo == source->tail_end
				|| expected_records > source->tail_end - source->native_redo)))
		return cold_xid_fail(progress);
	progress->source = *source;
	progress->next_xid = checkpoint_next_xid;
	progress->expected_records = expected_records;
	progress->last_end = source->native_redo;
	progress->begun = true;
	return true;
}

bool
cluster_cold_xid_record_v1(ClusterColdXidProgressV1 *progress,
						   const ClusterColdParticipantV1 *source,
						   const ClusterColdRecordV1 *record,
						   const ClusterColdXidInputV1 *input)
{
	uint64 next;

	if (progress == NULL || !progress->begun || progress->failed
		|| !cold_xid_source_same(&progress->source, source) || record == NULL || input == NULL
		|| !input->complete || !TransactionIdIsNormal((TransactionId)progress->next_xid)
		|| progress->records >= progress->expected_records
		|| record->read_rec_ptr < progress->last_end || record->end_rec_ptr <= record->read_rec_ptr
		|| record->end_rec_ptr > progress->source.tail_end
		|| record->prev_rec_ptr >= record->read_rec_ptr
		|| (progress->records != 0 && record->prev_rec_ptr != progress->last_read)
		|| record->reserved_zero != 0 || (record->record_flags & ~CLUSTER_COLD_RECORD_KNOWN_FLAGS) != 0
		|| (record->record_flags & CLUSTER_COLD_RECORD_UNSUPPORTED) != 0
		|| ((record->record_flags & CLUSTER_COLD_RECORD_STRUCTURAL) != 0
			&& !(record->rmid == RM_RELMAP_ID && record->relmap_image != NULL
				&& record->component_count == 0 && record->space_count == 0))
		|| (input->payload_xid != InvalidTransactionId
			&& !TransactionIdIsNormal(input->payload_xid))
		|| (input->checkpoint_next_xid != 0
			&& !TransactionIdIsNormal((TransactionId)input->checkpoint_next_xid))
		|| (input->checkpoint_shutdown && input->checkpoint_next_xid == 0))
		return cold_xid_fail(progress);
	next = progress->next_xid;
	if (!cold_xid_advance(&next, input->record_xid)
		|| (input->payload_xid != InvalidTransactionId && !cold_xid_advance(&next, input->payload_xid)))
		return cold_xid_fail(progress);
	if (input->checkpoint_next_xid != 0
		&& (input->checkpoint_shutdown || input->checkpoint_next_xid > next))
		next = input->checkpoint_next_xid;
	progress->next_xid = next;
	progress->records++;
	progress->last_read = record->read_rec_ptr;
	progress->last_end = record->end_rec_ptr;
	return true;
}

bool
cluster_cold_xid_candidate_v1(const ClusterColdXidProgressV1 *progress,
							  const ClusterColdParticipantV1 *source, uint64 *next_xid)
{
	if (next_xid == NULL)
		return false;
	*next_xid = 0;
	if (progress == NULL || !progress->begun || progress->failed
		|| !cold_xid_source_same(&progress->source, source)
		|| progress->records != progress->expected_records
		|| progress->last_end != progress->source.tail_end
		|| !TransactionIdIsNormal((TransactionId)progress->next_xid))
		return false;
	*next_xid = progress->next_xid;
	return true;
}
#endif
