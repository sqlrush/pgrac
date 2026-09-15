/* A148 stable-primary eligibility, not ADG recovery/lease certification. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_clean_leave.h"
#include "../../backend/cluster/cluster_mrp.c"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
BackendType MyBackendType = B_LMON;
bool cluster_enabled = true;
bool cluster_enable_adg = false;
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
static ClusterMrpSharedState region;
void
ExceptionalCondition(const char *cond, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", cond, file, line);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT(strcmp(name, "pgrac cluster mrp") == 0);
	UT_ASSERT_EQ(size, MAXALIGN(sizeof(region)));
	*found = false;
	return &region;
}
static void
fresh_region(void)
{
	cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
	cluster_enable_adg = false;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	cluster_mrp_shmem_init();
}
UT_TEST(test_original_primary_init_and_observational_history)
{
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	fresh_region();
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	cluster_mrp_publish_recovery_thread(4); /* original Startup metadata, not active redo */
	cluster_mrp_note_primary_thread_count(4);
	pg_atomic_write_u64(&region.error_count, 9);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_mrp_recovery_thread(), 4);
	cluster_enable_adg = true; /* primary still must not launch MRP */
	UT_ASSERT(!cluster_mrp_should_start());
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_wrong_owner_or_standby_never_admitted)
{
	fresh_region();
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	MyBackendType = B_LMON;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	cluster_dg_role = CLUSTER_DG_ROLE_STANDBY;
	cluster_mrp_shmem_init(); /* disabled standby is still outside normal primary */
	UT_ASSERT(!cluster_mrp_should_start());
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	cluster_enable_adg = true;
	cluster_mrp_shmem_init();
	UT_ASSERT(cluster_mrp_should_start());
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_native_pid_and_active_authority_cannot_be_observational)
{
	fresh_region();
	for (uint32 state = CLUSTER_MRP_NOT_STARTED; state < CLUSTER_MRP_DISABLED; state++) {
		cluster_mrp_set_state((ClusterMrpState)state);
		UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	}
	cluster_mrp_set_state(CLUSTER_MRP_DISABLED);
	pg_atomic_write_u32(&region.pid, 17); /* native publication boundary, no process launch */
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	pg_atomic_write_u32(&region.pid, 0);
	pg_atomic_write_u32(&region.apply_master_term_valid, 1);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u32(&region.apply_master_term_valid), 1);
}
UT_TEST(test_mailbox_publication_and_completion_are_distinct)
{
	fresh_region();
	/* Exact original atomic publication boundary; no voting I/O or lease
	 * acquisition executed. Result publisher below is the original body. */
	pg_atomic_write_u64(&region.pending_apply_lease_seq, 3);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u64(&region.pending_apply_lease_seq, 2);
	pg_atomic_write_u64(&region.apply_lease_request_seq, 1);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_mrp_qvotec_inflight_apply_lease_seq = 1;
	cluster_mrp_apply_lease_publish_result(CLUSTER_MRP_APPLY_LEASE_SUBMIT_ACK);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	pg_atomic_write_u64(&region.apply_master_token_seq, 1);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u64(&region.apply_lease_completion_seq, 2);
	UT_ASSERT_EQ(cluster_mrp_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(pg_atomic_read_u64(&region.apply_master_token_seq), 1);
}
int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_original_primary_init_and_observational_history);
	UT_RUN(test_wrong_owner_or_standby_never_admitted);
	UT_RUN(test_native_pid_and_active_authority_cannot_be_observational);
	UT_RUN(test_mailbox_publication_and_completion_are_distinct);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
