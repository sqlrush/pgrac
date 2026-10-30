/* These original service/stop fixtures construct no PI business batch.
 * Execute the actual public release wrapper against its empty batch. An
 * unexpected business header aborts; real pending PI/ACK is covered by
 * test_cluster_wal_inputs_job_pi, not fabricated here.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_PI_EMPTY_BATCH_FIXTURE_H
#define TEST_CLUSTER_PI_EMPTY_BATCH_FIXTURE_H
#include "cluster/cluster_pi_writeback.h"
static void *wb_batch;
static ResourceOwner wb_scan_owner;
static void
wb_batch_release(void)
{
	if (wb_batch != NULL)
		abort();
}
#include "test_cluster_pi_empty_batch_release.inc"
#endif
