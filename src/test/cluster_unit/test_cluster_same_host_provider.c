/* Real capsule I/O and cold NeedSet/admission; only host/proc observations
 * are controlled.  The original daemon fixture supplies formation owners. */
#define main same_host_daemon_fixture_main
int same_host_daemon_fixture_main(void);
#include "test_cluster_external_fence.c"
#undef main
#include "cluster/cluster_same_host_fence.h"
#include <sys/stat.h>

static ClusterSameHostProcessV1 observed_host;
static ClusterSameHostProbeV1 observed_probe;
static uint64 observed_ticks;
static bool host_read_ok;

bool cluster_same_host_test_current(ClusterSameHostProcessV1 *out);
ClusterSameHostProbeV1 cluster_same_host_test_probe(uint64 pid, uint64 *ticks);

bool
cluster_same_host_test_current(ClusterSameHostProcessV1 *out)
{
	*out = observed_host;
	return host_read_ok;
}

ClusterSameHostProbeV1
cluster_same_host_test_probe(uint64 pid, uint64 *ticks)
{
	*ticks = observed_ticks;
	return pid == (uint64)getpid() ? observed_probe : CLUSTER_SAME_HOST_PROCESS_UNREADABLE;
}

typedef struct LocalProviderFixture {
	char directory[128];
	ClusterRecoveryDutyKey duty;
	ClusterSameHostKeyV1 keys[4];
	PgracExternalFenceNeedSetV1 *needs;
	PgracExternalFenceAdmissionSetV1 *admissions;
} LocalProviderFixture;

static const ClusterFormationWitnessV1 *const local_formation
	= (const ClusterFormationWitnessV1 *)(uintptr_t)1;

static void
local_path(LocalProviderFixture *f, int node, bool file, char path[512])
{
	if (file)
		snprintf(path, 512, "%s/thread_%d/pgrac_process_%llu.identity", f->directory,
				 node + 1, (unsigned long long)f->keys[node].incarnation);
	else
		snprintf(path, 512, "%s/thread_%d", f->directory, node + 1);
}

static void
local_cleanup(LocalProviderFixture *f)
{
	char path[512];
	cluster_external_fence_admission_set_release(&f->admissions);
	cluster_external_fence_need_set_release(&f->needs);
	for (int n = 0; n < 4; n++) {
		local_path(f, n, true, path);
		(void)unlink(path);
		local_path(f, n, false, path);
		(void)rmdir(path);
	}
	(void)rmdir(f->directory);
	cluster_wal_threads_dir = NULL;
	stub_same_host_available = false;
}

static bool
local_setup(LocalProviderFixture *f)
{
	char path[512];
	memset(f, 0, sizeof(*f));
	strcpy(f->directory, "/tmp/pgrac-local-fence-XXXXXX");
	if (mkdtemp(f->directory) == NULL) return false;
	cluster_wal_threads_dir = f->directory;
	prepare_cold_formation(&f->duty);
	stub_runtime_active = false;
	stub_same_host_available = true;
	host_read_ok = true;
	memset(&observed_host, 0, sizeof(observed_host));
	memset(observed_host.machine_id, 1, 16);
	memset(observed_host.boot_id, 2, 16);
	observed_host.postmaster_pid = getpid();
	observed_host.start_ticks = 100;
	observed_host.pid_namespace = 11;
	observed_probe = CLUSTER_SAME_HOST_PROCESS_PRESENT;
	observed_ticks = 100;
	if (cluster_external_fence_need_set_build_cold(&f->duty, local_formation, &f->needs)
		!= PGRAC_EXTERNAL_FENCE_NEED_SET_OK || cluster_external_fence_need_set_count(f->needs) != 4)
		goto fail;
	for (int n = 0; n < 4; n++) {
		const PgracExternalFenceNeedV1 *need = cluster_external_fence_need_set_at(f->needs, n);
		ClusterSameHostKeyV1 *key = &f->keys[n];
		key->system_identifier = need->system_identifier;
		key->origin_node_id = need->victim_node_id;
		key->incarnation = need->victim_incarnation;
		memcpy(key->protected_set_digest, need->protected_set_digest, 32);
		local_path(f, n, false, path);
		if (mkdir(path, 0700) != 0
			|| !cluster_same_host_fence_record_writer_v1(f->directory, key, getpid())) goto fail;
	}
	return true;
fail:
	local_cleanup(f);
	return false;
}

static void
local_expect(LocalProviderFixture *f, PgracExternalFenceVerdict expected)
{
	PgracExternalFenceDenyReason reason;
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(f->needs, local_formation, 1000,
				 &f->admissions), expected);
	if (expected == PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED) {
		UT_ASSERT(f->admissions != NULL);
		UT_ASSERT_EQ(cluster_external_fence_admission_set_count(f->admissions), 4);
		UT_ASSERT(cluster_external_fence_revalidate_set_nowait(f->admissions, f->needs,
				 local_formation, &reason));
		for (int n = 0; n < 4; n++) {
			const PgracExternalFenceBindingV1 *b
				= cluster_external_fence_admission_set_binding_at(f->admissions, n);
			UT_ASSERT(b != NULL);
			if (b != NULL) {
				UT_ASSERT_EQ(b->victim_node_id, n);
				UT_ASSERT_EQ(b->victim_incarnation, f->keys[n].incarnation);
			}
		}
	} else UT_ASSERT(f->admissions == NULL);
}

UT_TEST(test_local_exited_full_set_and_nowait)
{
	LocalProviderFixture f;
	PgracExternalFenceDenyReason reason;
	bool ready = local_setup(&f);
	UT_ASSERT(ready);
	if (!ready) return;
	UT_ASSERT(cluster_same_host_fence_record_writer_v1(f.directory, &f.keys[0], getpid()));
	observed_ticks = 101;
	UT_ASSERT(!cluster_same_host_fence_record_writer_v1(f.directory, &f.keys[0], getpid()));
	observed_probe = CLUSTER_SAME_HOST_PROCESS_MISSING;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	/* Exclusion is immutable; the final no-wait check must perform no OS read. */
	host_read_ok = false;
	UT_ASSERT(cluster_external_fence_revalidate_set_nowait(f.admissions, f.needs,
			 local_formation, &reason));
	stub_cold_classification_result = CLUSTER_FORMATION_WITNESS_UNSTABLE;
	UT_ASSERT(!cluster_external_fence_revalidate_set_nowait(f.admissions, f.needs,
			 local_formation, &reason));
	local_cleanup(&f);
}

UT_TEST(test_local_live_rejected)
{
	LocalProviderFixture f; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_REJECTED);
	local_cleanup(&f);
}

UT_TEST(test_other_machine_unavailable)
{
	LocalProviderFixture f; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	observed_host.machine_id[0]++;
	observed_probe = CLUSTER_SAME_HOST_PROCESS_MISSING;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	local_cleanup(&f);
}

UT_TEST(test_local_pid_reuse_excluded)
{
	LocalProviderFixture f; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	observed_ticks++;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	local_cleanup(&f);
}

UT_TEST(test_local_reboot_excluded)
{
	LocalProviderFixture f; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	observed_host.boot_id[0]++;
	observed_probe = CLUSTER_SAME_HOST_PROCESS_UNREADABLE;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	local_cleanup(&f);
}

UT_TEST(test_last_missing_capsule_clears_partial_set)
{
	LocalProviderFixture f; char path[512]; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	local_path(&f, 3, true, path);
	UT_ASSERT_EQ(unlink(path), 0);
	observed_probe = CLUSTER_SAME_HOST_PROCESS_MISSING;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	local_cleanup(&f);
}

UT_TEST(test_local_read_error_unavailable)
{
	LocalProviderFixture f; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	observed_probe = CLUSTER_SAME_HOST_PROCESS_UNREADABLE;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	host_read_ok = false;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	local_cleanup(&f);
}

UT_TEST(test_local_pid_namespace_mismatch_unavailable)
{
	LocalProviderFixture f; bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	observed_host.pid_namespace++;
	observed_probe = CLUSTER_SAME_HOST_PROCESS_MISSING;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	local_cleanup(&f);
}

UT_TEST(test_corrupt_capsule_unavailable)
{
	LocalProviderFixture f; char path[512]; uint8 changed = 0; int fd;
	bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	local_path(&f, 0, true, path);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(write(fd, &changed, 1), 1);
		UT_ASSERT_EQ(close(fd), 0);
	}
	observed_probe = CLUSTER_SAME_HOST_PROCESS_MISSING;
	local_expect(&f, PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	local_cleanup(&f);
}

UT_TEST(test_same_host_does_not_activate_online_or_single_victim)
{
	LocalProviderFixture f; PgracExternalFenceNeedSetV1 *online = NULL;
	PgracExternalFenceAdmissionV1 *single = NULL;
	bool ready = local_setup(&f);
	UT_ASSERT(ready); if (!ready) return;
	observed_probe = CLUSTER_SAME_HOST_PROCESS_MISSING;
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&f.duty, local_formation, &online),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_CAPABILITY_UNAVAILABLE);
	UT_ASSERT(online == NULL);
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(cluster_external_fence_need_set_at(f.needs, 0),
				 1000, &single), PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(single == NULL);
	local_cleanup(&f);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_local_exited_full_set_and_nowait);
	UT_RUN(test_local_live_rejected);
	UT_RUN(test_other_machine_unavailable);
	UT_RUN(test_local_pid_reuse_excluded);
	UT_RUN(test_local_reboot_excluded);
	UT_RUN(test_last_missing_capsule_clears_partial_set);
	UT_RUN(test_local_read_error_unavailable);
	UT_RUN(test_local_pid_namespace_mismatch_unavailable);
	UT_RUN(test_corrupt_capsule_unavailable);
	UT_RUN(test_same_host_does_not_activate_online_or_single_victim);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
