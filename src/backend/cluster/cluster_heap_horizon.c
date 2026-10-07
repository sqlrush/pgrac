/* Conservative shared heap freeze metadata, before any page mutation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"

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
