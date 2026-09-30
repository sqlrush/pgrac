/* PGRAC: read-only, exact-generation recovery WAL observation.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#ifndef CLUSTER_WAL_TAIL_H
#define CLUSTER_WAL_TAIL_H

#include "catalog/pg_control.h"
#include "cluster/cluster_wal_source.h"

typedef struct ClusterWalTailObservation {
	XLogRecPtr complete_end;
	XLogRecPtr last_record_start;
	pg_crc32c last_record_crc;
	uint64 records;
	/* Exact claim namespace, published only after input validation succeeds. */
	uint64 database_incarnation;
} ClusterWalTailObservation;

struct XLogReaderState;
typedef bool (*ClusterWalRecordVisitor)(struct XLogReaderState *reader, void *arg);

/* Read-only provisional records: a visitor must not publish or mutate shared
 * state. Later input/revalidation failure invalidates every record it saw.
 * The exact checkpoint and last-record cut must match the selected root;
 * missing required records and any complete suffix outside that cut refuse.
 * Author: SqlRush <sqlrush@gmail.com> */
extern ClusterControlRootResult
cluster_wal_tail_visit_sealed(const char *wal_root, const ClusterWalSourceRef *ref,
							  int segment_size, const ClusterControlRootSnapshot *sealed,
							  XLogRecPtr checkpoint_start, ClusterWalRecordVisitor visitor,
							  void *arg, ClusterWalTailObservation *out);

/* Resolve the selected v3 claim/anchor under CF-S, scan without CF, then
 * reobserve the exact root token/record. Caller retains and revalidates its
 * separate IR/fencing/WALR owner bundle. This supplies input, not permission. */
extern ClusterControlRootResult cluster_control_root_recovery_visit(
	const ClusterControlRootSnapshot *expected, const ClusterControlRootReadToken *token,
	ClusterWalRecordVisitor visitor, void *arg, ClusterWalTailObservation *out);

/* Startup-only physical input, never a close/recovery/serving permission.
 * Zero maxima mean no PARAMETER_CHANGE record was observed; the caller must
 * also retain the predecessor/history/configuration requirements. */
typedef struct ClusterWalStartupObservation {
	ClusterWalTailObservation tail;
	/* The first actual checkpoint, never copied from a predecessor root. */
	XLogRecPtr checkpoint_start;
	XLogRecPtr checkpoint_end;
	pg_crc32c checkpoint_crc;
	uint8 checkpoint_info;
	CheckPoint checkpoint;
	uint64 checkpoint_records;
	uint64 fpw_records;
	uint64 parameter_records;
	uint64 unsupported_records;
	bool fpw_disabled;
	int max_connections;
	int max_worker_processes;
	int max_wal_senders;
	int max_prepared_xacts;
	int max_locks_per_xact;
} ClusterWalStartupObservation;

/* Read the root-selected independent stream from its fresh segment boundary.
 * This reports every complete native record, including a zero-record result.
 * Only the caller's selected initializer owner and native-side census can
 * qualify EMPTY. The first record must have no predecessor link; there is
 * no archive/local/other-generation fallback.
 * Caller owns immutable, nonaliasing input plus retention/isolation and root
 * revalidation; provisional bootstrap reads grant none of those permissions.
 * Author: SqlRush <sqlrush@gmail.com> */
extern ClusterControlRootResult cluster_wal_startup_observe(const char *wal_root,
															const ClusterWalSourceRef *ref,
															int segment_size,
															XLogRecPtr first_segment,
															ClusterWalStartupObservation *out);

/* Recovery-owner-only physical sync of the observed stream, actual claim
 * and directory entries. This grants no writer or recovery permission.
 * Caller must own/revalidate isolation, WALR and purpose-bound IR before and
 * after this operation. */
extern ClusterControlRootResult cluster_wal_startup_sync(const char *wal_root,
														 const ClusterWalSourceRef *ref,
														 int segment_size, XLogRecPtr first_segment,
														 ClusterWalStartupObservation *out);

/* Caller supplies a root-selected immutable reference and exact checkpoint
 * record start, NOT an arbitrary point at which to search for a later record.
 * This validates physical input only: the owner must hold/revalidate isolation,
 * retention and recovery serialization before consuming or publishing it.
 * No replay, authority publication, local-timeline or legacy-path fallback.
 * All outputs are cleared on refusal; ERROR/cancellation releases owned FDs.
 */
extern ClusterControlRootResult cluster_wal_tail_observe(const char *wal_root,
														 const ClusterWalSourceRef *ref,
														 int segment_size, XLogRecPtr scan_lower,
														 XLogRecPtr minimum_end,
														 ClusterWalTailObservation *out);

/* PGRAC: also bind the selected checkpoint record inside the scanned
 * prefix. A CRC-valid but different checkpoint cannot seal this root.
 * Author: SqlRush <sqlrush@gmail.com> */
extern ClusterControlRootResult
cluster_wal_tail_observe_checkpoint(const char *wal_root, const ClusterWalSourceRef *ref,
									int segment_size, XLogRecPtr scan_lower, XLogRecPtr minimum_end,
									XLogRecPtr checkpoint_start, pg_crc32c checkpoint_crc,
									ClusterWalTailObservation *out);

#endif /* CLUSTER_WAL_TAIL_H */
