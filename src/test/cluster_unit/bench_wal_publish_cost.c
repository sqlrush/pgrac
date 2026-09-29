/*-------------------------------------------------------------------------
 *
 * bench_wal_publish_cost.c
 *    Optional real-file production WAL publisher component measurements.
 *
 * Runtime authority and lock ownership are fixture inputs. This executable
 * is not linked into postgres and does not measure SQL commit latency.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/bench_wal_publish_cost.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <time.h>
#include <unistd.h>

static uint64 measured_sync_ns;
static int measured_sync_calls;
static int cost_fsync(int fd);
int publisher_correctness_main(void);

/* Reuse real record construction, codecs and publisher, not a fsync model. */
#define fsync cost_fsync
#define main publisher_correctness_main
#include "test_cluster_wal_durable_publish.c"
#undef main
#undef fsync

typedef struct CostSample {
	uint64 wal_ns;
	uint64 publish_ns;
	uint64 sync_ns;
	uint64 covered_end;
	int sync_calls;
} CostSample;

static uint64
cost_now(void)
{
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		abort();
	return (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
}

static int
cost_fsync(int fd)
{
	uint64 start = cost_now();
	int result = fsync(fd);
	int saved_errno = errno;
	measured_sync_ns += cost_now() - start;
	++measured_sync_calls;
	errno = saved_errno;
	return result;
}

static bool
cost_number(const char *text, unsigned maximum, unsigned *out)
{
	char *end;
	unsigned long n;
	if (text[0] < '1' || text[0] > '9')
		return false;
	errno = 0;
	n = strtoul(text, &end, 10);
	if (errno != 0 || *end != '\0' || n > maximum)
		return false;
	*out = n;
	return true;
}

static void
cost_sync_path(const char *path, bool directory)
{
	int fd = open(path, (directory ? O_RDONLY | O_DIRECTORY : O_RDWR) | O_NOFOLLOW);
	if (fd < 0 || fsync(fd) != 0 || close(fd) != 0)
		abort();
}

static void
cost_sync_wal(XLogRecPtr first, XLogRecPtr end)
{
	XLogSegNo low, high;
	XLByteToSeg(first, low, wal_segment_size);
	XLByteToSeg(end - 1, high, wal_segment_size);
	for (XLogSegNo seg = low; seg <= high; ++seg) {
		char name[MAXFNAMELEN], path[MAXPGPATH];
		XLogFileName(name, ref.timeline, seg, wal_segment_size);
		snprintf(path, sizeof(path), "%s/%s", generation, name);
		cost_sync_path(path, false);
	}
}

int
main(int argc, char **argv)
{
	unsigned groups, records;
	bool publish;
	char *parent = NULL;
	char path[MAXPGPATH];
	struct stat initial, current;
	ClusterWalDurablePrefix previous;
	CostSample *samples;

	if (argc != 5 || (strcmp(argv[2], "pgwp") != 0 && strcmp(argv[2], "wal-only") != 0)
		|| !cost_number(argv[3], 4096, &groups)
		|| !cost_number(argv[4], 1024, &records)
		/* Leave room for the fixture's generation, WAL and temporary filenames. */
		|| strlen(argv[1]) >= MAXPGPATH - 256 || (parent = realpath(argv[1], NULL)) == NULL
		|| strlen(parent) >= MAXPGPATH - 256 || stat(parent, &current) != 0
		|| !S_ISDIR(current.st_mode)) {
		fprintf(
			stderr,
			"usage: %s EXISTING_SCRATCH_PARENT pgwp|wal-only GROUPS[1..4096] RECORDS[1..1024]\n",
			argv[0]);
		free(parent);
		return 2;
	}
	publish = strcmp(argv[2], "pgwp") == 0;
	scratch_parent = parent;
	samples = calloc(groups, sizeof(*samples));
	if (samples == NULL) {
		free(parent);
		return 1;
	}
	CritSectionCount = 0;
	cluster_wal_durable_publish_init();
	fixture();
	previous = base_record();
	if (ut_current_failed || lstat(scratch, &initial) != 0 || !S_ISDIR(initial.st_mode))
		abort();

	/* Establish the initial namespace outside all measured intervals. */
	cost_sync_wal(previous.record_start, previous.exclusive_end);
	cost_sync_path(prefix_path, false);
	cost_sync_path(generation, true);
	snprintf(path, sizeof(path), "%s/durable_prefix", generation);
	cost_sync_path(path, true);
	snprintf(path, sizeof(path), "%s/thread_1", cluster_wal_threads_dir);
	cost_sync_path(path, true);
	cost_sync_path(cluster_wal_threads_dir, true);
	cost_sync_path(DataDir, true);
	cost_sync_path(scratch, true);

	for (unsigned group = 0; group < groups; ++group) {
		ClusterWalDurablePrefix next = previous;
		uint64 start;
		XLogRecPtr covered = 0;
		CostSample *sample = &samples[group];

		for (unsigned record = 0; record < records; ++record) {
			XLogRecPtr position = next.exclusive_end;
			if (position % XLOG_BLCKSZ == 0)
				position += position % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD;
			next = record_write(position, next.record_start, 24);
		}
		start = cost_now();
		cost_sync_wal(previous.exclusive_end, next.exclusive_end);
		sample->wal_ns = cost_now() - start;
		if (publish) {
			measured_sync_ns = 0;
			measured_sync_calls = 0;
			start = cost_now();
			if (cluster_wal_durable_publish(1, next.exclusive_end, previous.exclusive_end, &covered)
					!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| covered != next.exclusive_end)
				abort();
			sample->publish_ns = cost_now() - start;
			sample->sync_ns = measured_sync_ns;
			sample->sync_calls = measured_sync_calls;
			sample->covered_end = covered;
		}
		previous = next;
	}

	/* Remove only the exact temporary directory created by this invocation. */
	if (lstat(scratch, &current) != 0 || !S_ISDIR(current.st_mode)
		|| initial.st_dev != current.st_dev || initial.st_ino != current.st_ino
		|| !rmtree(scratch, true))
		abort();
	printf("{\"boundary\":\"production_publisher_fixture\",\"sql_commit_measurement\":false,"
		   "\"mode\":\"%s\",\"cassert\":%s,\"samples\":[",
		   argv[2],
#ifdef USE_ASSERT_CHECKING
		   "true"
#else
		   "false"
#endif
	);
	for (unsigned i = 0; i < groups; ++i) {
		CostSample *sample = &samples[i];
		printf("%s{\"group\":%u,\"records\":%u,\"wal_fsync_us\":%.3f,\"publish_us\":%.3f,"
			   "\"publish_fsync_us\":%.3f,\"publish_fsync_calls\":%d,\"covered_end\":" UINT64_FORMAT
			   "}",
			   i ? "," : "", i, records, sample->wal_ns / 1000.0, sample->publish_ns / 1000.0,
			   sample->sync_ns / 1000.0, sample->sync_calls, sample->covered_end);
	}
	puts("]}");
	free(samples);
	free(parent);
	return 0;
}
