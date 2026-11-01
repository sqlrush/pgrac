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

/* Reconstruct a private page frame from a selected native full-image record.
 * The original replay owner supplies the exact expected registration and actual
 * record end, and retains control/source/file custody. Optional current bytes
 * are a complete, same-birth frame containing effects already replayed by that
 * owner. They must be supplied whenever such effects exist; NULL/0 means there
 * are no already-replayed bytes to merge, not permission to discard a page.
 *
 * Compatible CLOG states are joined without downgrading terminal/subcommitted
 * entries. COMMITTED versus ABORTED refuses the whole result. Watermarks are
 * retained separately from the actual image position; neither proves coverage.
 * Success produces only a private, checksummed frame. It grants no write,
 * recovery, checkpoint or transaction authority and performs no I/O. The owner
 * must account for all required terminal redo before publishing a repaired page.
 * Exact lengths are required; refusal preserves every output/input byte, and
 * the output must not overlap any input. The native redo entry is not enabled
 * by this helper.
 */
extern bool cluster_clog_wal_page_restore_v2(const ClusterClogFileBindingV2 *expected,
										  const void *record, Size record_length,
										  XLogRecPtr record_end,
										  const void *current_frame, Size current_length,
										  void *out, Size length);

StaticAssertDecl(CLUSTER_CLOG_SEGMENT_WAL_BYTES_V2 == 8320, "CLOG segment WAL extent");
StaticAssertDecl(CLUSTER_CLOG_PAGE_WAL_BYTES_V2 == 8384, "CLOG page WAL extent");
StaticAssertDecl(CLUSTER_CLOG_TRUNCATE_WAL_BYTES_V2 == 152, "CLOG truncate WAL extent");

#endif
