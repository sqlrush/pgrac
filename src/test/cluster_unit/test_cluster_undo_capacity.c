#include "postgres.h"
#include "cluster/cluster_undo_capacity.h"
#include "storage/ipc.h"
#include "unit_test.h"

#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

UT_DEFINE_GLOBALS();
static ClusterUndoCapacityChangeFence state;
static pg_on_exit_callback exit_callback;
static Datum exit_arg;
static unsigned exit_registrations;

void
on_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	exit_callback = function;
	exit_arg = arg;
	exit_registrations++;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "%s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static void
reset(void)
{
	cluster_undo_capacity_fence_init(&state);
	cluster_undo_capacity_fence_attach(&state);
}

typedef struct CapacityProcessShared
{
	ClusterUndoCapacityChangeFence fence;
	volatile uint64 payload[2];
} CapacityProcessShared;

/* Only this fixture's transport is simulated. Each child executes the actual
 * writer core and the original registered exit callback in its own process. */
static void
capacity_process_writer(CapacityProcessShared *shared, unsigned index, int input, int output)
{
	bool outer = false, inner = false;
	char command, reply;
	ssize_t count;

	cluster_undo_capacity_fence_attach(&shared->fence);
	while ((count = read(input, &command, 1)) == 1)
	{
		reply = 'K';
		switch (command)
		{
			case 'B':
				outer = cluster_undo_capacity_write_scope_begin();
				if (!outer || exit_registrations != 1)
					reply = 'F';
				break;
			case 'N':
				inner = cluster_undo_capacity_write_scope_begin();
				if (!inner || exit_registrations != 1)
					reply = 'F';
				break;
			case 'M':
				cluster_undo_capacity_write_mark();
				shared->payload[index]++;
				break;
			case 'O':
				cluster_undo_capacity_write_scope_end(outer);
				outer = false;
				break;
			case 'X':
				if (exit_callback == NULL)
					reply = 'F';
				else
				{
					exit_callback(0, exit_arg);
					exit_callback(0, exit_arg);
					outer = inner = false;
				}
				break;
			default:
				reply = 'F';
				break;
		}
		pg_memory_barrier();
		if (write(output, &reply, 1) != 1)
			break;
	}
	/* Also clean up after a parent assertion/transport failure. The callback
 * retires this child's lazy registration, not another writer's work. */
	if (exit_callback != NULL)
		exit_callback(0, exit_arg);
	close(input);
	close(output);
	_exit(count == 0 ? 0 : 1);
}

static bool
capacity_process_command(int input, int output, char command)
{
	struct pollfd ready = { output, POLLIN, 0 };
	char reply;

	if (write(input, &command, 1) != 1 || poll(&ready, 1, 5000) <= 0
		|| read(output, &reply, 1) != 1)
		return false;
	pg_memory_barrier();
	return reply == 'K';
}

static bool
capacity_process_reap(pid_t pid)
{
	int status;

	/* Bound failure cleanup, without using timing to arrange the test cuts.
 * Only an exact child created below may be killed on fixture failure. */
	for (unsigned attempt = 0; attempt < 500; attempt++)
	{
		pid_t result = waitpid(pid, &status, WNOHANG);

		if (result == pid)
			return WIFEXITED(status) && WEXITSTATUS(status) == 0;
		if (result < 0 && errno != EINTR)
			return false;
		(void) poll(NULL, 0, 10);
	}
	(void) kill(pid, SIGKILL);
	while (waitpid(pid, &status, 0) < 0 && errno == EINTR)
		;
	return false;
}

static bool
capacity_process_overlap(bool exit_nested)
{
	CapacityProcessShared *shared;
	int commands[2][2] = {{-1, -1}, {-1, -1}};
	int replies[2][2] = {{-1, -1}, {-1, -1}};
	pid_t children[2] = {-1, -1};
	void (*previous_sigpipe)(int);
	uint64 old = 0, next = 909;
	bool ok = false;
	const char *failed = NULL;

#define PROCESS_CHECK(condition) \
	do { if (!(condition)) { failed = #condition; goto cleanup; } } while (0)

	shared = mmap(NULL, sizeof(*shared), PROT_READ | PROT_WRITE,
				  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (shared == MAP_FAILED)
		return false;
	MemSet(shared, 0, sizeof(*shared));
	cluster_undo_capacity_fence_init(&shared->fence);
	cluster_undo_capacity_fence_attach(&shared->fence);
	previous_sigpipe = signal(SIGPIPE, SIG_IGN);
	PROCESS_CHECK(previous_sigpipe != SIG_ERR);
	PROCESS_CHECK(cluster_undo_capacity_change_sample(&old) && old == 1);
	for (unsigned i = 0; i < 2; i++)
		PROCESS_CHECK(pipe(commands[i]) == 0 && pipe(replies[i]) == 0);
	for (unsigned i = 0; i < 2; i++)
	{
		children[i] = fork();
		PROCESS_CHECK(children[i] >= 0);
		if (children[i] == 0)
		{
			for (unsigned j = 0; j < 2; j++)
			{
				close(commands[j][1]);
				close(replies[j][0]);
				if (j != i)
				{
					close(commands[j][0]);
					close(replies[j][1]);
				}
			}
			capacity_process_writer(shared, i, commands[i][0], replies[i][1]);
		}
	}
	for (unsigned i = 0; i < 2; i++)
	{
		close(commands[i][0]); commands[i][0] = -1;
		close(replies[i][1]); replies[i][1] = -1;
		PROCESS_CHECK(capacity_process_command(commands[i][1], replies[i][0], 'B'));
	}
	/* Read-only scopes are lazy even when they belong to real other PIDs. */
	PROCESS_CHECK(cluster_undo_capacity_change_current(old));
	PROCESS_CHECK(capacity_process_command(commands[0][1], replies[0][0], 'M'));
	PROCESS_CHECK(pg_atomic_read_u64(&shared->fence.inflight_writers) == 1);
	PROCESS_CHECK(!cluster_undo_capacity_change_sample(&next) && next == 909);
	PROCESS_CHECK(capacity_process_command(commands[1][1], replies[1][0], 'M'));
	PROCESS_CHECK(pg_atomic_read_u64(&shared->fence.inflight_writers) == 2);
	PROCESS_CHECK(!cluster_undo_capacity_change_sample(&next) && next == 909);
	if (exit_nested)
	{
		PROCESS_CHECK(capacity_process_command(commands[0][1], replies[0][0], 'N'));
		PROCESS_CHECK(capacity_process_command(commands[0][1], replies[0][0], 'M'));
		PROCESS_CHECK(pg_atomic_read_u64(&shared->fence.inflight_writers) == 2);
		PROCESS_CHECK(capacity_process_command(commands[0][1], replies[0][0], 'O'));
		PROCESS_CHECK(pg_atomic_read_u64(&shared->fence.inflight_writers) == 2);
		PROCESS_CHECK(!cluster_undo_capacity_change_sample(&next) && next == 909);
		PROCESS_CHECK(capacity_process_command(commands[0][1], replies[0][0], 'X'));
	}
	else
		PROCESS_CHECK(capacity_process_command(commands[0][1], replies[0][0], 'O'));
	/* One writer finishing (even twice via exit) cannot retire the other. */
	PROCESS_CHECK(pg_atomic_read_u64(&shared->fence.inflight_writers) == 1);
	PROCESS_CHECK(!cluster_undo_capacity_change_sample(&next) && next == 909);
	PROCESS_CHECK(!cluster_undo_capacity_change_current(old));
	PROCESS_CHECK(capacity_process_command(commands[1][1], replies[1][0], 'O'));
	PROCESS_CHECK(pg_atomic_read_u64(&shared->fence.inflight_writers) == 0);
	PROCESS_CHECK(cluster_undo_capacity_change_sample(&next) && next == 5);
	PROCESS_CHECK(!cluster_undo_capacity_change_current(old));
	PROCESS_CHECK(shared->payload[0] == (exit_nested ? 2 : 1) && shared->payload[1] == 1);
	ok = true;
cleanup:
	for (unsigned i = 0; i < 2; i++)
		for (unsigned j = 0; j < 2; j++)
		{
			if (commands[i][j] >= 0) close(commands[i][j]);
			if (replies[i][j] >= 0) close(replies[i][j]);
		}
	for (unsigned i = 0; i < 2; i++)
		if (children[i] > 0 && !capacity_process_reap(children[i]))
			ok = false;
	if (previous_sigpipe != SIG_ERR)
		(void) signal(SIGPIPE, previous_sigpipe);
	/* The observing parent never began a writer scope. Detach before unmap. */
	cluster_undo_capacity_fence_attach(NULL);
	munmap(shared, sizeof(*shared));
	if (!ok)
		fprintf(stderr, "capacity process overlap (%s): %s\n",
				exit_nested ? "nested exit" : "normal", failed ? failed : "child cleanup");
#undef PROCESS_CHECK
	return ok;
}

UT_TEST(test_two_actual_writers_keep_negative_observation_invalid_until_both_retire)
{
	UT_ASSERT(capacity_process_overlap(false));
}

UT_TEST(test_actual_writer_exit_preserves_another_process_and_nested_guard)
{
	UT_ASSERT(capacity_process_overlap(true));
}

UT_TEST(test_lazy_read_scopes_do_not_prevent_cross_pass_observation)
{
	uint64 sequence;
	bool first, second;

	reset();
	UT_ASSERT(cluster_undo_capacity_change_sample(&sequence));
	first = cluster_undo_capacity_write_scope_begin();
	second = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_scope_end(first);
	UT_ASSERT(cluster_undo_capacity_change_current(sequence));
	cluster_undo_capacity_write_scope_end(second);
	UT_ASSERT(cluster_undo_capacity_change_current(sequence));
}

UT_TEST(test_actual_mutation_invalidates_before_and_after_visibility)
{
	uint64 old, next;
	bool outer, retained_guard;

	reset();
	UT_ASSERT(cluster_undo_capacity_change_sample(&old));
	outer = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	UT_ASSERT(!cluster_undo_capacity_change_sample(&next));
	retained_guard = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 1);
	/* The outer wrapper can return while its original XCUR guard lives. */
	cluster_undo_capacity_write_scope_end(outer);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&next));
	cluster_undo_capacity_write_scope_end(retained_guard);
	UT_ASSERT(cluster_undo_capacity_change_sample(&next));
	UT_ASSERT(next > old);
	UT_ASSERT(!cluster_undo_capacity_change_current(old));
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 0);
}

UT_TEST(test_two_writers_never_use_sequence_parity_as_stability)
{
	uint64 sequence = 99;
	bool own;

	reset();
	/* Model a second process's real shared registration using the same
	 * atomic transition, while this process executes the real scope core. */
	pg_atomic_fetch_add_u64(&state.inflight_writers, 1);
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	own = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 2);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	cluster_undo_capacity_write_scope_end(own);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	pg_atomic_fetch_sub_u64(&state.inflight_writers, 1);
	UT_ASSERT(cluster_undo_capacity_change_sample(&sequence));
}

UT_TEST(test_exhaustion_permanently_invalidates_without_refusing_writers)
{
	uint64 sequence;
	bool entered;

	reset();
	pg_atomic_write_u64(&state.change_sequence, UINT64_MAX - 1);
	entered = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	cluster_undo_capacity_write_scope_end(entered);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.change_sequence), UINT64_MAX);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 0);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	entered = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	cluster_undo_capacity_write_scope_end(entered);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.change_sequence), UINT64_MAX);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
}

UT_TEST(test_original_exit_callback_retires_only_own_registration_once)
{
	uint64 sequence;

	reset();
	(void)cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	(void)cluster_undo_capacity_write_scope_begin();
	/* Another process's registration and mutation remain outstanding. */
	pg_atomic_fetch_add_u64(&state.inflight_writers, 1);
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	UT_ASSERT_EQ(exit_registrations, 1);
	UT_ASSERT(exit_callback != NULL);
	exit_callback(0, exit_arg);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 1);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	exit_callback(0, exit_arg);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 1);
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	pg_atomic_fetch_sub_u64(&state.inflight_writers, 1);
	UT_ASSERT(cluster_undo_capacity_change_sample(&sequence));
}

int
main(void)
{
	UT_PLAN(7);
	/* Fork from the pre-writer state, as postmaster does. The original five
	 * cases keep their bodies and relative order after these new cuts. */
	UT_RUN(test_two_actual_writers_keep_negative_observation_invalid_until_both_retire);
	UT_RUN(test_actual_writer_exit_preserves_another_process_and_nested_guard);
	UT_RUN(test_lazy_read_scopes_do_not_prevent_cross_pass_observation);
	UT_RUN(test_actual_mutation_invalidates_before_and_after_visibility);
	UT_RUN(test_two_writers_never_use_sequence_parity_as_stability);
	UT_RUN(test_exhaustion_permanently_invalidates_without_refusing_writers);
	UT_RUN(test_original_exit_callback_retires_only_own_registration_once);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
