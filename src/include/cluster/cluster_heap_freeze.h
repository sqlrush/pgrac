/*-------------------------------------------------------------------------
 * cluster_heap_freeze.h
 *    Caller-owned inputs for shared heap freeze preparation.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HEAP_FREEZE_H
#define CLUSTER_HEAP_FREEZE_H

#include "cluster/cluster_space_identity.h"
#include "storage/bufpage.h"

/*
 * An unproven physical input, not an eligibility or retention certificate.
 * The full image retains page version/LSN, tuple headers and all physical
 * ITL/UBA fields without deriving a transaction identity from a raw XID.
 * Reuse is bounded to one image in the original VACUUM context. No borrowed
 * page pointer survives capture; no durable or wire representation is implied.
 */
typedef struct ClusterHeapFreezePageInputV1
{
	ClusterSpaceIdentity identity;
	BlockNumber block;
	OffsetNumber max_offset;
	uint16 normal_tuples;
	uint16 multi_tuples;
	PGAlignedBlock image;
} ClusterHeapFreezePageInputV1;

/*
 * Caller supplies a previously validated live SPACE identity and already
 * holds the source page's original share/cleanup content lock. Source and
 * output must not overlap. No allocation, I/O, buffer acquisition or proof
 * lookup occurs here. Failure leaves output unchanged. A successful copy
 * grants no current/fence/admission authority after the content lock is freed.
 */
extern bool cluster_heap_freeze_capture_page_v1(const ClusterSpaceIdentity *identity,
											  BlockNumber block, Page page,
											  ClusterHeapFreezePageInputV1 *out);

#endif /* CLUSTER_HEAP_FREEZE_H */
