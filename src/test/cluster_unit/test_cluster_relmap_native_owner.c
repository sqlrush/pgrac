/* Original relmap owner/native writer beside the original cold reader.
 * Retain all existing fixture providers and assertions.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_owner_namespace.h"
int d3_owner_existing_main(void);
#define main d3_owner_existing_main
#include "test_cluster_wal_writer.c"
#undef main
#include "test_cluster_relmap_native_owner.h"

static TestRelmapNativeOwnerResult
relmap_owner_bridge_result(bool captured, bool matched)
{
	return (TestRelmapNativeOwnerResult){ captured, matched, relmap_assign_calls,
		sync_calls, rename_calls, ut_current_failed };
}

TestRelmapNativeOwnerResult
test_relmap_native_owner_capture(const ClusterWalSourceRef *source, uint64 top,
								uint64 selected_epoch, unsigned fault,
								ClusterRelmapOwnerSampleV2 *out)
{
	bool captured;
	cluster_node_id = 0;
	ut_current_failed = 0;
	relmap_owner_fixture();
	/* These are the original fixture's independent provider inputs. The
	 * linked owner/native writer must perform every original qualification. */
	ref = *source;
	cluster_node_id = source->claim.identity.origin_node_id;
	epoch = selected_epoch;
	incarnation = source->claim.identity.origin_owner_incarnation;
	native_ctl.InsertTimeLineID = source->timeline;
	relmap_top_full = top;
	if (fault == 1) MyBackendType = B_STARTUP;
	if (fault == 2) relmap_local_lock = true;
	if (fault == 3) relmap_global_x = false;
	if (fault == 4) relmap_in_transaction = false;
	if (fault == 5) have_ref = false;
	if (fault == 6) active = false;
	if (fault == 7) member = false;
	if (fault == 8) self_fenced = true;
	if (fault == 9) relmap_admission_delta = 1;
	if (fault == 10) incarnation++;
	captured = cluster_relmap_owner_capture_v2(out);
	return relmap_owner_bridge_result(captured, false);
}

TestRelmapNativeOwnerResult
test_relmap_native_owner_check(const ClusterRelmapOwnerSampleV2 *sample, unsigned fault)
{
	/* Local-lock matching is allowed, but must never assign a new FullXid. */
	if (fault == 0) relmap_local_lock = true;
	if (fault == 1) epoch++;
	if (fault == 2) incarnation++;
	if (fault == 3) relmap_top_full += UINT64CONST(1) << 32;
	if (fault == 4) ref.claim.claim_sha256[0]++;
	if (fault == 5) MyProcPid++;
	if (fault == 6) relmap_global_x = false;
	if (fault == 7) ref.timeline++;
	return relmap_owner_bridge_result(false, cluster_relmap_owner_matches_v2(sample));
}

void
test_relmap_native_owner_done(void)
{
	/* This directory belongs only to the included writer fixture. Original
	 * cold ROOT/WAL/pins and their separate cleanup remain with the caller. */
	if (scratch[0] != '\0') {
		if (!rmtree(scratch, true))
			abort();
		scratch[0] = '\0';
	}
	if (cluster_wal_threads_dir != NULL) {
		pfree(cluster_wal_threads_dir);
		cluster_wal_threads_dir = NULL;
	}
	if (DataDir != NULL) {
		pfree(DataDir);
		DataDir = NULL;
	}
}
