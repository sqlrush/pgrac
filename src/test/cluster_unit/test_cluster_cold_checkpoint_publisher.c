/* Original issuance tracker and Checkpointer beside the original Startup.
 * Retain the complete existing fixture and its controlled leaf providers. */
#include "test_cluster_cold_checkpoint_publisher_namespace.h"
int d3_startup_checkpoint_existing_main(void);
#define main d3_startup_checkpoint_existing_main
#include "test_cluster_relmap_recovery.c"
#undef main
#include "test_cluster_cold_checkpoint_publisher.h"

TestColdCheckpointPublisherResult
test_cold_checkpoint_publish(const ClusterWalSourceRef *source, const CheckPoint *checkpoint,
							 XLogRecPtr start, XLogRecPtr end, uint64 post_issued, unsigned fault,
							 uint8 out[160])
{
	TestColdCheckpointPublisherResult result = { 0 };
	ClusterRelmapCheckpointSampleV1 sample;
	uint64 next = U64FromFullTransactionId(checkpoint->nextXid);

	setup();
	ut_current_failed = 0;
	memset(out, 0, 160);
	original.writer.ref = *source;
	original.writer.epoch = 7 + source->claim.identity.origin_node_id;
	variables.nextXid = checkpoint->nextXid;
	variables.oldestClogXid = (TransactionId)(next - 21);
	/* These are actual issued-publish calls in the original tracker, with
	 * its existing controlled exclusive-XidGenLock assertion provider. */
	publish(next - 1);
	if (fault == 1)
		MyBackendType = B_STARTUP;
	if (fault == 2)
		variables.clusterIssuedState = 0;
	result.captured
		= cluster_relmap_recovery_checkpoint_window_capture(checkpoint->ThisTimeLineID, &sample);
	if (result.captured) {
		result.before_issued = sample.window.last_issued;
		/* Reservation deliberately exceeds the independent actual sample.
		 * Neither producer nor consumer may call this issuance. */
		variables.nextXid = FullTransactionIdFromU64(post_issued + 0x1000);
		if (post_issued != next - 1)
			publish(post_issued);
		if (fault == 3)
			variables.oldestClogXid++;
		if (fault == 4)
			native_writer_current = false;
		if (fault == 5)
			native_boot++;
		if (fault == 6)
			variables.clusterIssuedState = 2;
		if (fault == 7)
			native_change_check = native_writer_checks + 2;
		result.finished = cluster_relmap_recovery_checkpoint_window_finish(&sample, checkpoint,
																		   start, end, out);
	}
	result.after_issued = U64FromFullTransactionId(variables.clusterLastIssuedXid);
	result.allocation_next = U64FromFullTransactionId(variables.nextXid);
	result.locks_remaining = depth;
	result.interrupt_errors = interrupt_errors;
	result.assertion_failed = ut_current_failed;
	return result;
}

TestColdCheckpointPublisherResult
test_cold_checkpoint_publish_identity(const ClusterWalSourceRef *source,
									  const CheckPoint *checkpoint, XLogRecPtr start,
									  XLogRecPtr end, uint64 post_issued, unsigned fault,
									  const TestColdCheckpointWriterV1 *writer, uint8 out[160])
{
	TestColdCheckpointPublisherResult result = { 0 };
	ClusterRelmapCheckpointSampleV1 sample;
	uint64 next = U64FromFullTransactionId(checkpoint->nextXid);

	setup();
	ut_current_failed = 0;
	memset(out, 0, 160);
	if (writer == NULL)
		return result;
	original.writer.ref = *source;
	original.writer.epoch = writer->epoch;
	native_boot = writer->boot_incarnation;
	variables.nextXid = checkpoint->nextXid;
	variables.oldestClogXid = (TransactionId)(next - 21);
	publish(next - 1);
	if (fault == 1)
		MyBackendType = B_STARTUP;
	if (fault == 2)
		variables.clusterIssuedState = 0;
	result.captured
		= cluster_relmap_recovery_checkpoint_window_capture(checkpoint->ThisTimeLineID, &sample);
	if (result.captured) {
		result.before_issued = sample.window.last_issued;
		variables.nextXid = FullTransactionIdFromU64(post_issued + 0x1000);
		if (post_issued != next - 1)
			publish(post_issued);
		if (fault == 3)
			variables.oldestClogXid++;
		if (fault == 4)
			native_writer_current = false;
		if (fault == 5)
			native_boot++;
		if (fault == 6)
			variables.clusterIssuedState = 2;
		if (fault == 7)
			native_change_check = native_writer_checks + 2;
		result.finished = cluster_relmap_recovery_checkpoint_window_finish(&sample, checkpoint,
																		   start, end, out);
	}
	result.after_issued = U64FromFullTransactionId(variables.clusterLastIssuedXid);
	result.allocation_next = U64FromFullTransactionId(variables.nextXid);
	result.locks_remaining = depth;
	result.interrupt_errors = interrupt_errors;
	result.assertion_failed = ut_current_failed;
	return result;
}
