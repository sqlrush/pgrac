/* Explicit CLOG WAL observations; not file ownership or recovery coverage. */
#ifndef CLUSTER_CLOG_WAL_H
#define CLUSTER_CLOG_WAL_H

#include "access/clog_file.h"

#define CLUSTER_CLOG_SEGMENT_WAL_BYTES_V2 (128 + CLUSTER_CLOG_FILE_HEADER_BYTES)
#define CLUSTER_CLOG_PAGE_WAL_BYTES_V2 (128 + CLUSTER_CLOG_FRAME_BYTES)
#define CLUSTER_CLOG_TRUNCATE_WAL_BYTES_V2 (128 + CLUSTER_RECOVERY_CLOG_TRUNCATE_PAYLOAD_V2_BYTES)

typedef struct ClusterClogWalObservationV2
{
	uint8 info;
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 page;
	uint32 cutoff_page;
	TransactionId oldest_xid;
	Oid oldest_database;
	FullTransactionId oldest_full_xid;
	/* Borrowed complete header/frame; valid only while the input is retained. */
	const uint8 *image;
	Size image_bytes;
} ClusterClogWalObservationV2;

/* Normalized rmgr info only. Exact framing, complete identity and body CRCs
 * are checked before publishing out. Refusal preserves input and output;
 * overlapping input/output is refused. These self-consistent observations
 * still require original control/custody/source validation before replay. */
extern bool cluster_clog_wal_decode_v2(uint8 info, const void *bytes, Size length,
									 ClusterClogWalObservationV2 *out);

StaticAssertDecl(CLUSTER_CLOG_SEGMENT_WAL_BYTES_V2 == 8320, "CLOG segment WAL extent");
StaticAssertDecl(CLUSTER_CLOG_PAGE_WAL_BYTES_V2 == 8384, "CLOG page WAL extent");
StaticAssertDecl(CLUSTER_CLOG_TRUNCATE_WAL_BYTES_V2 == 152, "CLOG truncate WAL extent");

#endif
