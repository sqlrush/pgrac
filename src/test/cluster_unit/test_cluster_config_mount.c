/*-------------------------------------------------------------------------
 *
 * test_cluster_config_mount.c
 *    Original LMON mount observation carrier, including real fork visibility.
 *    Family setup and the unrelated incoming-slot retirement are fixtures;
 *    this is not root, membership or live cluster admission certification.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_mount.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <sys/wait.h>
#include <unistd.h>
#include "miscadmin.h"
#include "cluster/cluster_config_members.h"

/* Compile the production carrier; unrelated functions are dead-stripped. */
#include "../../backend/cluster/cluster_shared_config_delivery.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool IsPostmasterEnvironment = true, IsUnderPostmaster;
BackendType MyBackendType = B_INVALID;
int MyProcPid;
pid_t PostmasterPid;

bool
cluster_shared_config_delivery_slot_retire(ClusterSharedConfigDeliverySlot *slot, int32 pid)
{
	UT_ASSERT(slot == &delivery_family->incoming && pid > 0);
	return true;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected Assert %s at %s:%d\n", condition, file, line);
	abort();
}

static void
setup(void)
{
	if (delivery_family != NULL)
		UT_ASSERT_EQ(munmap(delivery_family, sizeof(*delivery_family)), 0);
	delivery_family
		= mmap(NULL, sizeof(*delivery_family), PROT_READ | PROT_WRITE, PG_MMAP_FLAGS, -1, 0);
	if (delivery_family == MAP_FAILED)
		abort();
	MyProcPid = PostmasterPid = getpid();
	MyBackendType = B_INVALID;
	IsUnderPostmaster = false;
	delivery_family->postmaster_pid = PostmasterPid;
	delivery_generation = 1;
	pg_atomic_init_u64(&delivery_family->generation, 1);
	pg_atomic_init_u32(&delivery_family->lmon_pid, 0);
	pg_atomic_init_u64(&delivery_family->mount_sequence, 0);
}

static ClusterConfigMountProof
proof(void)
{
	ClusterConfigMountProof value = { 0 };
	value.result = CLUSTER_CONFIG_MOUNT_MATCH;
	value.self_incarnation = 123;
	value.key.epoch = 7;
	value.common.version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	return value;
}

UT_TEST(real_fork_publication_and_reap_retire_receipt)
{
	int go[2], ready[2], status;
	char byte = 1;
	pid_t child;
	ClusterConfigMountProof expected = proof(), observed;
	setup();
	UT_ASSERT_EQ(pipe(go), 0);
	UT_ASSERT_EQ(pipe(ready), 0);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		close(go[1]);
		close(ready[0]);
		MyProcPid = getpid();
		IsUnderPostmaster = true;
		MyBackendType = B_LMON;
		if (read(go[0], &byte, 1) != 1)
			_exit(2);
		cluster_shared_config_mount_publish(&expected);
		if (write(ready[1], &byte, 1) != 1)
			_exit(3);
		_exit(0);
	}
	close(go[0]);
	close(ready[1]);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	cluster_shared_config_delivery_lmon_started(child);
	UT_ASSERT_EQ(write(go[1], &byte, 1), 1);
	UT_ASSERT_EQ(read(ready[0], &byte, 1), 1);
	UT_ASSERT(cluster_shared_config_mount_observe(&observed));
	UT_ASSERT_EQ(memcmp(&expected, &observed, sizeof(expected)), 0);
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	cluster_shared_config_delivery_lmon_reaped(child);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	/* Even a reused numeric PID cannot inherit the reaped lifetime's proof. */
	cluster_shared_config_delivery_lmon_started(child);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	close(go[1]);
	close(ready[0]);
}

UT_TEST(wrong_owner_or_torn_lifetime_never_publishes_match)
{
	ClusterConfigMountProof expected = proof(), observed;
	setup();
	cluster_shared_config_mount_publish(&expected);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	pg_atomic_write_u32(&delivery_family->lmon_pid, MyProcPid);
	MyBackendType = B_BACKEND;
	cluster_shared_config_mount_publish(&expected);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	MyBackendType = B_LMON;
	cluster_shared_config_mount_publish(&expected);
	UT_ASSERT(cluster_shared_config_mount_observe(&observed));
	MyBackendType = B_LOGGER;
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	MyBackendType = B_STARTUP;
	pg_atomic_write_u64(&delivery_family->mount_sequence, 3);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	MyBackendType = B_LMON;
	cluster_shared_config_mount_publish(&expected);
	UT_ASSERT_EQ(pg_atomic_read_u64(&delivery_family->mount_sequence), PG_UINT64_MAX);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
}

UT_TEST(new_shmem_generation_rejects_old_family_child)
{
	ClusterConfigMountProof expected = proof(), observed;
	setup();
	pg_atomic_write_u32(&delivery_family->lmon_pid, MyProcPid);
	MyBackendType = B_LMON;
	cluster_shared_config_mount_publish(&expected);
	UT_ASSERT(cluster_shared_config_mount_observe(&observed));
	pg_atomic_write_u64(&delivery_family->generation, 2);
	UT_ASSERT(!cluster_shared_config_mount_observe(&observed));
	expected.result = CLUSTER_CONFIG_MOUNT_MISMATCH;
	cluster_shared_config_mount_publish(&expected);
	UT_ASSERT_EQ(delivery_family->mount.result, CLUSTER_CONFIG_MOUNT_MATCH);
}

UT_TEST(parent_profile_is_not_an_application_census)
{
	ClusterSharedConfigRef selected = { 0 };
	ClusterSharedConfigActive common;
	setup();
	selected.identity.system_identifier = 19;
	selected.identity.database_incarnation = 2;
	selected.identity.generation = 3;
	selected.identity.configured[0] = 3;
	selected.identity.storage_uuid[0] = 4;
	selected.identity.authority_uuid[0] = 5;
	delivery_family->startup_identity = selected.identity;
	delivery_family->startup_node_id = 0;
	delivery_family->startup_common = proof().common;
	/* Later ordinary targets do not change static actual startup values. */
	selected.identity.generation++;
	UT_ASSERT(cluster_shared_config_parent_profile(&selected, 0, &common));
	UT_ASSERT_EQ(common.version, CLUSTER_SHARED_CONFIG_COMMON_VERSION);
	UT_ASSERT(!cluster_shared_config_parent_profile(&selected, 1, &common));
	selected.identity.authority_uuid[0]++;
	UT_ASSERT(!cluster_shared_config_parent_profile(&selected, 0, &common));
	UT_ASSERT_EQ(common.version, 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(real_fork_publication_and_reap_retire_receipt);
	UT_RUN(wrong_owner_or_torn_lifetime_never_publishes_match);
	UT_RUN(new_shmem_generation_rejects_old_family_child);
	UT_RUN(parent_profile_is_not_an_application_census);
	if (delivery_family != NULL)
		munmap(delivery_family, sizeof(*delivery_family));
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
