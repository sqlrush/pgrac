/* User-approved same-host process exclusion; no remote-host admission. */
#include "postgres.h"
#include "cluster/cluster_same_host_fence.h"
#include "unit_test.h"
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

UT_DEFINE_GLOBALS();

static bool hidden_proc;
static int kernel_pid_rc, kernel_pid_errno;

static int
probe_open(const char *path, int flags, ...)
{
	mode_t mode = 0;
	if (hidden_proc && strcmp(path, "/proc/42/stat") == 0) {
		errno = ENOENT;
		return -1;
	}
	if (flags & O_CREAT) {
		va_list args;
		va_start(args, flags);
		mode = va_arg(args, int);
		va_end(args);
	}
	return open(path, flags, mode);
}

static int
probe_kill(pid_t pid, int signal)
{
	if (hidden_proc) {
		errno = kernel_pid_errno;
		return kernel_pid_rc;
	}
	return kill(pid, signal);
}

#define open probe_open
#define kill probe_kill
#include "../../backend/cluster/cluster_same_host_fence.c"
#undef open
#undef kill

static ClusterSameHostProcessV1
old_process(void)
{
	ClusterSameHostProcessV1 old = { .postmaster_pid = 42, .start_ticks = 100, .pid_namespace = 11 };

	memset(old.machine_id, 1, sizeof(old.machine_id));
	memset(old.boot_id, 2, sizeof(old.boot_id));
	return old;
}

UT_TEST(test_same_host_exited)
{
	ClusterSameHostProcessV1 old = old_process();

	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &old,
				 CLUSTER_SAME_HOST_PROCESS_MISSING, 0), CLUSTER_SAME_HOST_WRITE_EXCLUDED);
}

UT_TEST(test_same_host_live)
{
	ClusterSameHostProcessV1 old = old_process();

	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &old,
				 CLUSTER_SAME_HOST_PROCESS_PRESENT, 100), CLUSTER_SAME_HOST_STILL_ALIVE);
}

UT_TEST(test_remote_host)
{
	ClusterSameHostProcessV1 old = old_process(), current = old;

	current.machine_id[0]++;
	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &current,
				 CLUSTER_SAME_HOST_PROCESS_MISSING, 0), CLUSTER_SAME_HOST_UNAVAILABLE);
}

UT_TEST(test_pid_reuse)
{
	ClusterSameHostProcessV1 old = old_process();

	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &old,
				 CLUSTER_SAME_HOST_PROCESS_PRESENT, 101), CLUSTER_SAME_HOST_WRITE_EXCLUDED);
}

UT_TEST(test_host_reboot)
{
	ClusterSameHostProcessV1 old = old_process(), current = old;

	current.boot_id[0]++;
	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &current,
				 CLUSTER_SAME_HOST_PROCESS_UNREADABLE, 0), CLUSTER_SAME_HOST_WRITE_EXCLUDED);
}

UT_TEST(test_missing_identity_fields)
{
	ClusterSameHostProcessV1 valid = old_process();

	for (int missing = 0; missing < 5; missing++)
	{
		ClusterSameHostProcessV1 old = valid;

		if (missing == 0) memset(old.machine_id, 0, sizeof(old.machine_id));
		if (missing == 1) memset(old.boot_id, 0, sizeof(old.boot_id));
		if (missing == 2) old.postmaster_pid = 0;
		if (missing == 3) old.start_ticks = 0;
		if (missing == 4) old.pid_namespace = 0;
		UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &valid,
					 CLUSTER_SAME_HOST_PROCESS_MISSING, 0), CLUSTER_SAME_HOST_UNAVAILABLE);
	}
}

UT_TEST(test_process_read_failed)
{
	ClusterSameHostProcessV1 old = old_process();

	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &old,
				 CLUSTER_SAME_HOST_PROCESS_UNREADABLE, 0), CLUSTER_SAME_HOST_UNAVAILABLE);
}

UT_TEST(test_hidden_proc_is_not_process_death)
{
	uint64 ticks;
	hidden_proc = true;
	kernel_pid_rc = 0;
	kernel_pid_errno = 0;
	UT_ASSERT_EQ(same_host_probe(42, &ticks), CLUSTER_SAME_HOST_PROCESS_UNREADABLE);
	kernel_pid_rc = -1;
	kernel_pid_errno = EPERM;
	UT_ASSERT_EQ(same_host_probe(42, &ticks), CLUSTER_SAME_HOST_PROCESS_UNREADABLE);
	kernel_pid_errno = EIO;
	UT_ASSERT_EQ(same_host_probe(42, &ticks), CLUSTER_SAME_HOST_PROCESS_UNREADABLE);
	kernel_pid_errno = ESRCH;
	UT_ASSERT_EQ(same_host_probe(42, &ticks), CLUSTER_SAME_HOST_PROCESS_MISSING);
	hidden_proc = false;
}

UT_TEST(test_pid_namespace_mismatch)
{
	ClusterSameHostProcessV1 old = old_process(), current = old;

	current.pid_namespace++;
	UT_ASSERT_EQ(cluster_same_host_process_excluded_v1(&old, &current,
				 CLUSTER_SAME_HOST_PROCESS_MISSING, 0), CLUSTER_SAME_HOST_UNAVAILABLE);
}

UT_TEST(test_proc_starttime_field)
{
	char text[512], *p = text;
	uint64 ticks = 99;

	p += sprintf(p, "42 (pm ) name) S");
	for (int field = 4; field <= 21; field++) p += sprintf(p, " 0");
	strcpy(p, " 100 0");
	UT_ASSERT(cluster_same_host_process_start_v1(text, 42, &ticks));
	UT_ASSERT_EQ(ticks, 100);
	UT_ASSERT(!cluster_same_host_process_start_v1(text, 43, &ticks));
	UT_ASSERT_EQ(ticks, 0);
	strcpy(p, " 0");
	UT_ASSERT(!cluster_same_host_process_start_v1(text, 42, &ticks));
	strcpy(p, " 18446744073709551616");
	UT_ASSERT(!cluster_same_host_process_start_v1(text, 42, &ticks));
	*p = '\0';
	UT_ASSERT(!cluster_same_host_process_start_v1(text, 42, &ticks));
}

UT_TEST(test_capsule_codec_binding)
{
	ClusterSameHostRecordV1 r = { .key = { .system_identifier = 99, .origin_node_id = 1,
		.incarnation = 7 } }, out;
	ClusterSameHostKeyV1 wrong;
	uint8 bytes[160];

	r.process = old_process();
	memset(r.key.protected_set_digest, 3, sizeof(r.key.protected_set_digest));
	UT_ASSERT(cluster_same_host_record_encode_v1(&r, bytes));
	UT_ASSERT(cluster_same_host_record_decode_v1(bytes, sizeof(bytes), &r.key, &out));
	UT_ASSERT(cluster_same_host_record_matches_v1(&out, &r.key));
	UT_ASSERT_EQ(out.process.postmaster_pid, 42);
	for (size_t i = 0; i < sizeof(bytes); i++) {
		bytes[i] ^= 1;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_same_host_record_decode_v1(bytes, sizeof(bytes), &r.key, &out));
		UT_ASSERT_EQ(out.key.system_identifier, 0);
		bytes[i] ^= 1;
	}
	wrong = r.key; wrong.incarnation++;
	UT_ASSERT(!cluster_same_host_record_decode_v1(bytes, sizeof(bytes), &wrong, &out));
	wrong = r.key; wrong.protected_set_digest[0]++;
	UT_ASSERT(!cluster_same_host_record_decode_v1(bytes, sizeof(bytes), &wrong, &out));
	UT_ASSERT(!cluster_same_host_record_decode_v1(bytes, sizeof(bytes) - 1, &r.key, &out));
	memset(bytes, 0, sizeof(bytes));
	UT_ASSERT(!cluster_same_host_record_decode_v1(bytes, sizeof(bytes), &r.key, &out));
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_same_host_exited);
	UT_RUN(test_same_host_live);
	UT_RUN(test_remote_host);
	UT_RUN(test_pid_reuse);
	UT_RUN(test_host_reboot);
	UT_RUN(test_missing_identity_fields);
	UT_RUN(test_process_read_failed);
	UT_RUN(test_hidden_proc_is_not_process_death);
	UT_RUN(test_pid_namespace_mismatch);
	UT_RUN(test_proc_starttime_field);
	UT_RUN(test_capsule_codec_binding);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
