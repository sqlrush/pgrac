/* Original KO/provider chain linked to the real WB proof and initialization.
 * PCM/GES/WAL are explicit stateful boundaries; WB closure is not replaced.
 * Author: SqlRush <sqlrush@gmail.com> */
#define Z_REAL_WB_PROOF 1
#define Z_DROP_NO_MAIN 1
#include "test_cluster_drop_z.c"

UT_TEST(test_stop_two_objects_use_production_wb_empty_proof)
{
 const char *reason;
 if (!z_setup(true)) return;
 ko_stop_requested = true;
 for (unsigned i=0;i<32;i++) UT_ASSERT(cluster_ko_shared_native_poll_v2());
 UT_ASSERT_EQ(z_selected[0],16);
 UT_ASSERT_EQ(z_selected[1],16);
 UT_ASSERT_EQ(main_syncs,2);
 UT_ASSERT_EQ(dir_syncs,2);
 UT_ASSERT_EQ(external_fds,0);
 UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason),CLUSTER_NORMAL_STOP_PENDING);
 z_release_ax();
 UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason),CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_after_cut_preserves_work_without_io)
{
 const char *reason;
 if (!z_setup(true)) return;
 ko_stop_requested = true;
 ko_continue_allowed = false;
 for (unsigned i=0;i<8;i++) UT_ASSERT(!cluster_ko_shared_native_poll_v2());
 assert_no_drop_io();
 UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason),CLUSTER_NORMAL_STOP_PENDING);
 ko_continue_allowed = true;
 for (unsigned i=0;i<32;i++) UT_ASSERT(cluster_ko_shared_native_poll_v2());
 UT_ASSERT_EQ(main_syncs,2);
 z_release_ax();
 UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason),CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_rejects_new_native_reservation_without_losing_old_work)
{
 ClusterKoCompletionV2 fresh={0};
 if(!z_setup(false)) return;
 ko_stop_requested=true;
 MyBackendType=B_BACKEND;
 UT_ASSERT(!ko_native_reserve(&fresh));
 UT_ASSERT(!DsaPointerIsValid(fresh.native_allocation));
 MyBackendType=B_CHECKPOINTER;
 for(unsigned i=0;i<16;i++) UT_ASSERT(cluster_ko_shared_native_poll_v2());
 UT_ASSERT_EQ(main_syncs,1);
}

#ifndef Z_WB_NO_MAIN
int main(void)
{
 setvbuf(stdout,NULL,_IOLBF,0);
 UT_PLAN(6);
 UT_RUN(test_stop_rejects_new_native_reservation_without_losing_old_work);
 UT_RUN(test_stop_two_objects_use_production_wb_empty_proof);
 UT_RUN(test_stop_after_cut_preserves_work_without_io);
 UT_RUN(test_z_real_io_failure_retains_exact_context_and_error);
 UT_RUN(test_z_changed_context_never_relabels_old_prefix_or_retires_new_identity);
 UT_RUN(test_z_pending_object_cannot_starve_other_object_or_claim_idle);
 UT_DONE();
 return ut_failed_count != 0;
}

#endif
