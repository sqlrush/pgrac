/* Test-only original Checkpointer fixture bridge. Native ownership, locks
 * and WAL/ROOT durability remain controlled below the linked algorithms. */
#ifndef TEST_CLUSTER_COLD_CHECKPOINT_PUBLISHER_H
#define TEST_CLUSTER_COLD_CHECKPOINT_PUBLISHER_H
#include "cluster/cluster_relmap_checkpoint.h"

typedef struct TestColdCheckpointPublisherResult {
	bool captured, finished;
	uint64 before_issued, after_issued, allocation_next;
	unsigned locks_remaining, interrupt_errors, assertion_failed;
} TestColdCheckpointPublisherResult;

extern TestColdCheckpointPublisherResult
test_cold_checkpoint_publish(const ClusterWalSourceRef *source, const CheckPoint *checkpoint,
							 XLogRecPtr start, XLogRecPtr end, uint64 post_issued, unsigned fault,
							 uint8 out[160]);

/* Controlled original-writer observations, fixed before fixture WAL creation.
 * These values provide no production writer/admission qualification. */
typedef struct TestColdCheckpointWriterV1 {
	uint64 epoch, boot_incarnation;
} TestColdCheckpointWriterV1;

extern TestColdCheckpointPublisherResult test_cold_checkpoint_publish_identity(
	const ClusterWalSourceRef *source, const CheckPoint *checkpoint, XLogRecPtr start,
	XLogRecPtr end, uint64 post_issued, unsigned fault,
	const TestColdCheckpointWriterV1 *writer, uint8 out[160]);
#endif
