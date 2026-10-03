/*-------------------------------------------------------------------------
 * cluster_page_data.h
 *    Exact DATA completion from the resident current holder.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_DATA_H
#define CLUSTER_PAGE_DATA_H

#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_page_online_plan.h"
#include "cluster/cluster_wal_source.h"

typedef struct ClusterPageDataTargetV1 {
	uint64 database_incarnation;
	RfPageIdentityV1 identity;
	RfPageVersionV1 version;
} ClusterPageDataTargetV1;

typedef struct ClusterPageDataReceiptV1 ClusterPageDataReceiptV1;

/* Background write endpoint, with no caller buffer locks. Borrows only a
 * resident, unfenced current-X; resident SPACE0 stays locked across write,
 * fsync and exact post-read; DATA content is unlocked after the native write
 * and reacquired conditionally to verify the same WAL/version after readback.
 * SPACE0/1 require a decoded typed identity; SPACE0 is its own identity hold
 * and stays locked once across I/O. Tombstoned identities qualify only SPACE
 * metadata, never ordinary DATA or completion of a structural side effect.
 * Busy/missing/stale returns false, never takes
 * ownership or creates storage. The exact native WAL binding must name the
 * selected local writer generation or carry its original writer's native
 * flush certification. A receiver's thread id/LSN cannot certify foreign WAL.
 *
 * On success *out (NULL on entry) owns a receipt in CurrentMemoryContext.
 * Failure leaves *out unchanged. ERROR releases this call's locks/pins/I/O
 * and propagates. A receipt proves only the exact requested DATA version;
 * it never grants checkpoint publication or WAL retirement. */
extern bool cluster_bufmgr_write_page_data_v1(const ClusterPageDataTargetV1 *target,
											  ClusterPageDataReceiptV1 **out);
extern bool cluster_page_data_receipt_read_v1(const ClusterPageDataReceiptV1 *receipt,
											  ClusterPageDataTargetV1 *out);
extern void cluster_page_data_receipt_free_v1(ClusterPageDataReceiptV1 **receipt);

struct ClusterThreadRecoveryFabricPlanV1;
struct ClusterPageWalBindingV1;
struct ClusterSpaceStructureChange;

/* Resolve a structural SPACE0 binding to the original TRUNCATE or ordinary
 * COMMIT-DROP ending retired_incarnation in this receiver's sealed input.
 * Full original claim/record and retained SPACE ancestry are required.
 * Returns the actual decoded change; refusal preserves output. This pure
 * query proves neither physical durability, KO, per-PI PAGE ancestry nor
 * retirement, and never grants disposal from an untrusted wire value. */
extern bool cluster_page_structural_record_v1(const struct ClusterPageWalBindingV1 *binding,
	const uint8 retired_incarnation[16], const struct ClusterThreadRecoveryFabricPlanV1 *plan,
	struct ClusterSpaceStructureChange *out);

/* Pure per-PI MAIN/VM ancestry in that same sealed input. The entire old
 * incarnation's PAGE chain must precede the exact structural end, and pi
 * must match an original full source/record within it. This query is not a
 * DATA receipt, KO completion, durability proof or physical-disposal grant. */
extern bool cluster_page_structural_ancestor_v1(const struct ClusterPageWalBindingV1 *binding,
	const struct ClusterPageWalBindingV1 *pi, const struct ClusterThreadRecoveryFabricPlanV1 *plan);

/* Bind typed SPACE DATA to its exact original contribution and complete
 * retained ancestry. Ordinary PAGE receipts keep their existing proof path.
 * The caller must retain this immutable plan until all receipts/ACKs using
 * it are freed. Remote notices bind to the receiver's own retained plan;
 * no pointer or ancestry claim is accepted from the wire. Refusal leaves
 * the receipt unchanged. This grants no checkpoint or WAL retirement. */
extern bool cluster_page_data_bind_plan_v1(ClusterPageDataReceiptV1 *receipt,
	const struct ClusterThreadRecoveryFabricPlanV1 *plan);

/* Combine actual DATA completions with one sealed, retained PAGE input.
 * Sources are the original full claims in the plan's participant order;
 * receipts are unique and ordered by the plan's target identity. Each must
 * name an exact result record in that source, never a larger numeric token.
 * This is only a PAGE constraint: the caller still owns source retention,
 * SIDE obligations and ROOT publication. Failure leaves prefixes unchanged. */
extern bool cluster_page_data_prefix_v1(const RfPageOnlinePlanV1 *plan,
										const ClusterWalSourceRef *sources,
										uint32 participant_count,
										const ClusterPageDataReceiptV1 *const *receipts,
										uint32 receipt_count, RfPageContributionPrefixV1 *prefixes);

#endif /* CLUSTER_PAGE_DATA_H */
