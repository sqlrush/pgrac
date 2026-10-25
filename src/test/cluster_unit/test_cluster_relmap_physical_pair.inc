/* Original producer/singleton plus real temporary dual-copy authority files.
 * Native transaction/WAL, lower GES and notification transport are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

static char physical_root[MAXPGPATH];
static bool physical_finishing;
static ClusterRelmapRetentionDirectory *physical_directory;
static ClusterRelmapV2Image physical_initial[2];
static ClusterRelmapRetentionKey physical_write_key;
static uint64 physical_write_end;

static void
physical_expect_pin(const ClusterRelmapV2Namespace *identity, uint64 generation,
	const ClusterRelmapV2Owner *owner)
{
	memset(&physical_write_key, 0, sizeof(physical_write_key));
	physical_write_key.identity = *identity;
	physical_write_key.generation = generation;
	physical_write_key.origin = owner->node;
	physical_write_key.top_full_xid = owner->top_full_xid;
	physical_write_key.membership_epoch = owner->membership_epoch;
	physical_write_key.admitted_incarnation = owner->admitted_incarnation;
	physical_write_key.boot_incarnation = owner->boot_incarnation;
	physical_write_end = owner->wal_end;
}

static void
physical_path(char path[MAXPGPATH], unsigned slot, const char *leaf)
{
	int n = slot == 0
		? snprintf(path, MAXPGPATH, "%s/global/%s", physical_root, leaf)
		: snprintf(path, MAXPGPATH, "%s/base/%u/%s", physical_root, MyDatabaseId, leaf);

	UT_ASSERT(n > 0 && n < MAXPGPATH);
}

static void
physical_pair_equals(unsigned slot, const ClusterRelmapV2Image *expected)
{
	static const char *const names[] = { "pgrac_relmap_authority", "pgrac_relmap_authority.bak" };
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE], actual[CLUSTER_RELMAP_V2_FILE_SIZE];

	UT_ASSERT_EQ(cluster_relmap_v2_encode(expected, wire, sizeof(wire)), CLUSTER_RELMAP_V2_VALID);
	for (unsigned copy = 0; copy < 2; copy++)
	{
		char path[MAXPGPATH];
		struct stat st;
		int fd;

		physical_path(path, slot, names[copy]);
		fd = open(path, O_RDONLY | O_NOFOLLOW);
		UT_ASSERT(fd >= 0);
		if (fd < 0)
			continue;
		UT_ASSERT_EQ(fstat(fd, &st), 0);
		UT_ASSERT_EQ(st.st_size, sizeof(actual));
		UT_ASSERT_EQ(read(fd, actual, sizeof(actual)), sizeof(actual));
		UT_ASSERT(memcmp(actual, wire, sizeof(actual)) == 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

static void
physical_clean_pairs(void)
{
	for (unsigned slot = 0; slot < 2; slot++)
	{
		ClusterRelmapV2Image expected = physical_initial[slot];

		if ((postcommit_mask & (1U << slot)) && terminal_publish)
		{
			expected.committed_generation = 8;
			expected.committed = terminal_images[slot].pending;
		}
		physical_pair_equals(slot, &expected);
	}
}

static void
physical_observe(unsigned event)
{
	UT_ASSERT(cluster_relmap_authority_x_held() && !v2_retention_held && CritSectionCount == 0);
	if (event == RELMAP_PHYSICAL_WRITE)
	{
		bool staged = false;

		UT_ASSERT_EQ(local_held, !physical_finishing);
		for (unsigned i = 0; i < lengthof(physical_directory->pins); i++)
		{
			const ClusterRelmapRetentionPin *pin = &physical_directory->pins[i];

			if (pin->occupied && pin->may_be_staged
				&& pin->key.identity.system_identifier == physical_write_key.identity.system_identifier
				&& pin->key.identity.database_incarnation == physical_write_key.identity.database_incarnation
				&& pin->key.identity.shared == physical_write_key.identity.shared
				&& pin->key.identity.dbid == physical_write_key.identity.dbid
				&& pin->key.generation == physical_write_key.generation
				&& pin->key.origin == physical_write_key.origin
				&& pin->key.top_full_xid == physical_write_key.top_full_xid
				&& pin->key.membership_epoch == physical_write_key.membership_epoch
				&& pin->key.admitted_incarnation == physical_write_key.admitted_incarnation
				&& pin->key.boot_incarnation == physical_write_key.boot_incarnation
				&& pin->wal_end == physical_write_end)
				staged = true;
		}
		UT_ASSERT(staged);
	}
}

static ClusterRelmapV2PairResult
physical_read(const ClusterRelmapV2Namespace *identity, ClusterRelmapV2View *out)
{
	unsigned slot = identity->shared ? 0 : 1;

	terminal_preparing_slot = slot;
	terminal_reads[slot]++;
	return test_relmap_physical_v2_read(identity, out);
}

static bool
physical_write(const ClusterRelmapV2Namespace *identity, uint64 generation,
	const ClusterRelmapV2Owner *owner, const RelMapFile *pending,
	ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	TestRelmapPhysicalStats before = test_relmap_physical_stats(), after;
	bool ok;

	UT_ASSERT(directory == physical_directory && lock == v2_retention_lock);
	UT_ASSERT_EQ(flushes, inserts);
	UT_ASSERT_EQ(owner->wal_end, 1000 + inserts);
	v2_stage_calls++;
	physical_expect_pin(identity, generation, owner);
	ok = test_relmap_physical_v2_write_pending(identity, generation, owner, pending, directory, lock);
	after = test_relmap_physical_stats();
	v2_stage_writes += after.writes - before.writes;
	return ok;
}

static bool
physical_check(const ClusterRelmapV2Image *image)
{
	unsigned slot = image->identity.shared ? 0 : 1;

	if (!test_relmap_physical_v2_check_staged(image))
		return false;
	terminal_images[slot] = *image;
	terminal_checks[slot]++;
	return true;
}

static ClusterRelmapV2ClearResult
physical_clear(const ClusterRelmapV2Image *image, bool publish)
{
	unsigned slot = image->identity.shared ? 0 : 1;
	ClusterRelmapV2ClearResult result;

	UT_ASSERT(physical_finishing && publish == terminal_publish && terminal_calls < 2);
	terminal_trace[terminal_calls++] = slot;
	physical_expect_pin(&image->identity, image->pending_generation, &image->pending_owner);
	result = publish ? test_relmap_physical_v2_publish(image)
		: test_relmap_physical_v2_discard_pending(image);
	if (result == CLUSTER_RELMAP_V2_CLEARED_POSTREAD)
		terminal_disk_cleared[slot] = true;
	return result;
}

static void
physical_send(const SharedInvalidationMessage *msgs, int n)
{
	physical_clean_pairs();
	postcommit_send(msgs, n);
}

static ClusterSinvalAckResult
physical_ack(const SharedInvalidationMessage *msgs, int n)
{
	physical_clean_pairs();
	return postcommit_ack(msgs, n);
}

static void
physical_before_release(void)
{
	physical_clean_pairs();
	native_lock_before_release();
}

static ClusterRelmapRetentionDirectory *
physical_fixture(bool next_era, unsigned mask, bool publish)
{
	static const char *const names[] = { "pgrac_relmap_authority", "pgrac_relmap_authority.bak" };
	TestRelmapNativeLockConfig config = native_lock_configuration();
	char path[MAXPGPATH];

	config.before_release = physical_before_release;
	physical_directory = native_lock_fixture(next_era, mask, publish, &config);
	UT_ASSERT(strlcpy(physical_root, "/tmp/pgrac_relmap_producer_XXXXXX", sizeof(physical_root))
		< sizeof(physical_root));
	UT_ASSERT(mkdtemp(physical_root) != NULL);
	UT_ASSERT(snprintf(path, sizeof(path), "%s/global", physical_root) < sizeof(path));
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	UT_ASSERT(snprintf(path, sizeof(path), "%s/base", physical_root) < sizeof(path));
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	UT_ASSERT(snprintf(path, sizeof(path), "%s/base/%u", physical_root, MyDatabaseId) < sizeof(path));
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	memset(physical_initial, 0, sizeof(physical_initial));
	for (unsigned slot = 0; slot < 2; slot++)
	{
		ClusterRelmapV2Image *image = &physical_initial[slot];
		uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
		int dir;

		image->identity.system_identifier = GetSystemIdentifier();
		image->identity.database_incarnation = writer_ref.claim.database_incarnation;
		image->identity.shared = slot == 0;
		image->identity.dbid = slot == 0 ? InvalidOid : MyDatabaseId;
		image->committed_generation = 7;
		image->committed = committed;
		UT_ASSERT_EQ(cluster_relmap_v2_encode(image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
		for (unsigned copy = 0; copy < 2; copy++)
		{
			int fd;

			physical_path(path, slot, names[copy]);
			fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(write(fd, bytes, sizeof(bytes)), sizeof(bytes));
			UT_ASSERT_EQ(fsync(fd), 0);
			UT_ASSERT_EQ(close(fd), 0);
		}
		physical_path(path, slot, ".");
		dir = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
		UT_ASSERT(dir >= 0);
		UT_ASSERT_EQ(fsync(dir), 0);
		UT_ASSERT_EQ(close(dir), 0);
	}
	physical_finishing = false;
	test_relmap_physical_reset(physical_root, physical_observe);
	v2_pair_observer = physical_read;
	v2_write_observer = physical_write;
	v2_prepared_pair_observer = physical_check;
	v2_terminal_observer = physical_clear;
	v2_send_observer = physical_send;
	v2_ack_observer = physical_ack;
	UT_ASSERT(cluster_relmap_authority_x_lock());
	return physical_directory;
}

static void
physical_cleanup(void)
{
	static const char *const names[] = { "pgrac_relmap_authority", "pgrac_relmap_authority.bak",
		"pgrac_relmap_authority.tmp", "pgrac_relmap_authority.bak.tmp" };
	char path[MAXPGPATH];

	test_relmap_physical_reset(NULL, NULL);
	for (unsigned slot = 0; slot < 2; slot++)
	{
		for (unsigned name = 0; name < lengthof(names); name++)
		{
			physical_path(path, slot, names[name]);
			UT_ASSERT(unlink(path) == 0 || errno == ENOENT);
		}
		physical_path(path, slot, ".");
		path[strlen(path) - 2] = '\0';
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	UT_ASSERT(snprintf(path, sizeof(path), "%s/base", physical_root) < sizeof(path));
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(physical_root), 0);
	physical_root[0] = '\0';
	native_lock_test_reset();
}

UT_TEST(v2_physical_pairs_finish_before_notification_and_unlock)
{
	for (unsigned which = 0; which < 12; which++)
	{
		unsigned mask = which % 3 + 1, count = mask == 3 ? 2 : 1;
		bool publish = which >= 6;
		ClusterRelmapRetentionDirectory *directory = physical_fixture((which / 3) % 2 != 0,
			mask, publish);
		ClusterRelmapRetentionDirectory before;
		TestRelmapPhysicalStats stats;
		TestRelmapNativeLockStats lock_stats;
		bool refused = precommit_error(false);

		UT_ASSERT(!refused);
		if (refused)
		{
			physical_cleanup();
			continue;
		}
		UT_ASSERT_EQ(active_shared_updates.num_mappings + active_local_updates.num_mappings, 0);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(inserts, count);
		UT_ASSERT_EQ(flushes, count);
		UT_ASSERT_EQ(xid_under_lock + xid_in_crit, 0);
		UT_ASSERT_EQ(preserves, 2 * count);
		for (unsigned slot = 0; slot < 2; slot++)
			physical_pair_equals(slot, mask & (1U << slot) ? &terminal_images[slot] : &physical_initial[slot]);
		stats = test_relmap_physical_stats();
		UT_ASSERT_EQ(stats.writes, 2 * count);
		UT_ASSERT_EQ(stats.file_syncs, 2 * count);
		UT_ASSERT_EQ(stats.directory_syncs, 2 * count);
		UT_ASSERT_EQ(stats.renames, 2 * count);
		UT_ASSERT(stats.reads > 0);
		before = *directory;
		physical_finishing = true;
		v2_terminal_context = publish;
		if (publish)
			cluster_relmap_producer_postcommit_v2();
		else
			cluster_relmap_producer_abort_v2();
		physical_clean_pairs();
		stats = test_relmap_physical_stats();
		UT_ASSERT_EQ(stats.writes, 4 * count);
		UT_ASSERT_EQ(stats.file_syncs, 4 * count);
		UT_ASSERT_EQ(stats.directory_syncs, 4 * count);
		UT_ASSERT_EQ(stats.renames, 4 * count);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(terminal_calls, count);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], publish ? count : 0);
		UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], publish ? count : 0);
		test_relmap_native_lock_stats(&lock_stats);
		UT_ASSERT_EQ(lock_stats.releases, 1);
		UT_ASSERT(lock_stats.exact_request && lock_stats.held_at_release);
		UT_ASSERT(!cluster_relmap_authority_x_held() && !cluster_relmap_v2_finish_entered);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		physical_cleanup();
	}
}

UT_TEST(v2_physical_stage_sync_refusal_keeps_original_batch)
{
	for (unsigned which = 0; which < 8; which++)
	{
		ClusterRelmapRetentionDirectory *directory = physical_fixture(which >= 4, 3, false);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 saved[2];
		TestRelmapPhysicalStats stats, after;
		TestRelmapNativeLockStats lock_stats;
		RelMapFile queues[4];
		unsigned sync_at = which & 2 ? 3 : 1;

		precommit_save_queues(queues);
		test_relmap_physical_fail_sync((which & 1) != 0, sync_at);
		UT_ASSERT(precommit_error(false));
		precommit_same_queues(queues);
		UT_ASSERT(cluster_relmap_v2_prepare_unfinished);
		UT_ASSERT_EQ(cluster_relmap_v2_arms[0].armed, sync_at == 3);
		UT_ASSERT(!cluster_relmap_v2_arms[1].armed);
		if (sync_at == 3)
			physical_pair_equals(0, &terminal_images[0]);
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		stats = test_relmap_physical_stats();
		physical_finishing = true;
		UT_ASSERT(abort_panic());
		UT_ASSERT(postcommit_panic());
		after = test_relmap_physical_stats();
		UT_ASSERT(memcmp(&stats, &after, sizeof(stats)) == 0);
		UT_ASSERT(memcmp(saved, cluster_relmap_v2_arms, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
		test_relmap_native_lock_stats(&lock_stats);
		UT_ASSERT_EQ(lock_stats.releases, 0);
		UT_ASSERT(cluster_relmap_authority_x_held());
		physical_cleanup();
	}
}

UT_TEST(v2_physical_second_finish_refusal_keeps_all_custody)
{
	for (unsigned which = 0; which < 8; which++)
	{
		bool publish = (which & 1) != 0;
		ClusterRelmapRetentionDirectory *directory = physical_fixture(which >= 4, 3, publish);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 saved[2];
		TestRelmapPhysicalStats stats, after;
		TestRelmapNativeLockStats lock_stats;
		ClusterRelmapV2View remaining;
		ClusterRelmapV2Image clean;
		bool refused = precommit_error(false);

		UT_ASSERT(!refused);
		if (refused)
		{
			physical_cleanup();
			continue;
		}
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		test_relmap_physical_reset(physical_root, physical_observe);
		test_relmap_physical_fail_sync((which & 2) != 0, 3);
		physical_finishing = true;
		v2_terminal_context = publish;
		UT_ASSERT(publish ? postcommit_panic() : abort_panic());
		native_lock_keeps_receipts(directory, &before, saved);
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT(terminal_disk_cleared[0] && !terminal_disk_cleared[1]);
		clean = physical_initial[0];
		if (publish)
		{
			clean.committed_generation = 8;
			clean.committed = saved[0].staged.pending;
		}
		physical_pair_equals(0, &clean);
		UT_ASSERT_EQ(test_relmap_physical_v2_read(&physical_initial[1].identity, &remaining),
			CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(remaining.pending_complete && remaining.has_pending);
		UT_ASSERT_EQ(remaining.pending_generation, 8);
		stats = test_relmap_physical_stats();
		UT_ASSERT(postcommit_panic());
		UT_ASSERT(abort_panic());
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
		after = test_relmap_physical_stats();
		UT_ASSERT(memcmp(&stats, &after, sizeof(stats)) == 0);
		native_lock_keeps_receipts(directory, &before, saved);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1]
			+ ack_attempts[0] + ack_attempts[1] + unlock_calls, 0);
		test_relmap_native_lock_stats(&lock_stats);
		UT_ASSERT_EQ(lock_stats.releases, 0);
		physical_cleanup();
	}
}
