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

#include "access/htup_details.h"
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
 * Physical row inputs only. The tuple-named ITL can describe a later updater,
 * not the creator. Its bytes, including FREE state, never prove provenance.
 * Missing or out-of-range slot indexes remain unproven rows in the batch.
 */
typedef struct ClusterHeapFreezeTupleInputV1
{
	OffsetNumber offset;
	uint16 tuple_offset;
	uint16 tuple_length;
	TransactionId raw_xmin;
	TransactionId raw_xmax;
	ItemPointerData ctid;
	uint16 infomask;
	uint16 infomask2;
	uint8 header_length;
	uint8 itl_slot_index;
	bool has_carrier;
	ClusterItlSlotData carrier;
} ClusterHeapFreezeTupleInputV1;

/* One caller-owned batch, reused with the captured image for each page. */
typedef struct ClusterHeapFreezePageBatchV1
{
	ClusterSpaceIdentity identity;
	BlockNumber block;
	XLogRecPtr page_lsn;
	SCN page_version;
	uint16 tuple_count;
	uint16 multi_tuples;
	ClusterHeapFreezeTupleInputV1 tuples[MaxHeapTuplesPerPage];
} ClusterHeapFreezePageBatchV1;

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

/*
 * Expand the immutable image in physical offset order, without resolving or
 * coalescing raw XIDs. Input and output must not overlap. No allocation, I/O,
 * proof lookup or mutation occurs. Failure leaves output unchanged. Success
 * grants no transaction, retention, freeze or final identity-fence authority.
 */
extern bool cluster_heap_freeze_prepare_page_v1(const ClusterHeapFreezePageInputV1 *input,
											  ClusterHeapFreezePageBatchV1 *out);

#endif /* CLUSTER_HEAP_FREEZE_H */
