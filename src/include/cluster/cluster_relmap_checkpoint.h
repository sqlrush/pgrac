/* Original checkpoint native issuance window. Structural bytes alone are
 * never a qualified base, terminal, completion, or release permission. */
#ifndef CLUSTER_RELMAP_CHECKPOINT_H
#define CLUSTER_RELMAP_CHECKPOINT_H
#include "catalog/pg_control.h"
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_wal_writer.h"
#define CLUSTER_RELMAP_CHECKPOINT_WINDOW_SIZE 160

typedef struct ClusterRelmapCheckpointWindow {
	uint64 system_identifier, database_incarnation;
	uint64 epoch, admitted_incarnation, boot_incarnation;
	uint64 native_floor, last_issued, allocation_next;
	uint16 origin;
} ClusterRelmapCheckpointWindow;

/* Exact complete native CheckPoint bytes, actual native record addresses and
 * an original issuance observation. No caller boolean grants qualification.
 * Nonoverlapping refusal clears all output; overlap preserves inputs. */
extern bool cluster_relmap_checkpoint_window_encode_v1(
	const ClusterRelmapCheckpointWindow *window, const CheckPoint *checkpoint, XLogRecPtr start,
	XLogRecPtr end, uint8 out[CLUSTER_RELMAP_CHECKPOINT_WINDOW_SIZE]);
extern bool cluster_relmap_checkpoint_window_decode_v1(
	const uint8 bytes[CLUSTER_RELMAP_CHECKPOINT_WINDOW_SIZE], const CheckPoint *checkpoint,
	XLogRecPtr start, XLogRecPtr end, const ClusterControlRootIdentity *identity, uint64 dbinc,
	ClusterRelmapCheckpointWindow *out);
/* Ephemeral original Checkpointer sample; not a persistence receipt. Capture
 * before checkpoint critical work, finish after the actual WAL flush and all
 * native locks/critical sections are released. Unknown stays all-zero/v2. */
typedef struct ClusterRelmapCheckpointSampleV1 {
	ClusterWalWriterToken writer;
	ClusterRelmapCheckpointWindow window;
} ClusterRelmapCheckpointSampleV1;
extern bool cluster_relmap_recovery_checkpoint_window_capture(TimeLineID timeline,
															  ClusterRelmapCheckpointSampleV1 *out);
extern bool
cluster_relmap_recovery_checkpoint_window_finish(const ClusterRelmapCheckpointSampleV1 *sample,
												 const CheckPoint *checkpoint, XLogRecPtr start,
												 XLogRecPtr end, uint8 out[160]);
#endif
