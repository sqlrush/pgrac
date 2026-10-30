/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_pair.inc
 *     Retained allocation through original preparation and physical pairs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_pair.inc
 * NOTES
 *     Directory, native terminal authority, locks and WAL are controlled.
 *     This is a component path, not native transaction or lifetime proof.
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

bool enableFsync = true;
static char stage_pair_root[MAXPGPATH];
static ClusterRelmapV2Image stage_pair_initial[2], stage_pair_prepared[2];
static unsigned stage_pair_preserves, stage_pair_syncs, stage_pair_terminal_reads;
static unsigned stage_pair_drift_at_rename;
static bool stage_pair_finishing, stage_pair_terminal_ready, stage_pair_corrupt_preserve;
static ClusterRelmapRetentionKey stage_pair_write_key;
static XLogRecPtr stage_pair_write_end;
static bool (*stage_pair_terminal_reader)(const ClusterRelmapOwnerSampleV2 *, ClusterRelmapRecoveryNativeTerminal *);
static void (*stage_pair_terminal_write_observer)(void);

static void
retained_pair_path(char path[MAXPGPATH], unsigned slot, const char *leaf)
{
	int n = slot == 0
		? snprintf(path, MAXPGPATH, "%s/global/%s", stage_pair_root, leaf)
		: snprintf(path, MAXPGPATH, "%s/base/%u/%s", stage_pair_root, MyDatabaseId, leaf);

	UT_ASSERT(n > 0 && n < MAXPGPATH);
}

static void
retained_pair_observe(unsigned event)
{
	UT_ASSERT(caller_global_x && !held && !retention_held && CritSectionCount == 0);
	/* Stage's first committed-pair read precedes MappingLock. Writes and
	 * prepare validation run under it; terminal cleanup runs after it. */
	if (stage_pair_finishing || (event != RELMAP_PHYSICAL_READ && event != RELMAP_PHYSICAL_CLOSE))
		UT_ASSERT_EQ(retention_mapping_held, !stage_pair_finishing);
	if (stage_pair_finishing && event == RELMAP_PHYSICAL_WRITE && stage_pair_terminal_write_observer != NULL)
		stage_pair_terminal_write_observer();
	if (event == RELMAP_PHYSICAL_WRITE && !stage_pair_finishing) {
		ClusterRelmapRetentionDirectory *d = &relmap_runtime_shmem->directories[retention_origin];
		bool found = false;

		for (uint32 i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++) {
			ClusterRelmapRetentionPin *pin = &d->pins[i];
			if (memcmp(&pin->key, &stage_pair_write_key, sizeof(pin->key)) == 0
				&& pin->wal_end == stage_pair_write_end && pin->may_be_staged) found = true;
		}
		UT_ASSERT(found); /* Original stage precedes every attempted pair write. */
	}
	if (event == RELMAP_PHYSICAL_RENAME && stage_pair_finishing
		&& test_relmap_physical_stats().renames == stage_pair_drift_at_rename)
		caller_boot++;
}

static ClusterRelmapV2PairResult
retained_pair_read(const ClusterRelmapV2Namespace *identity, ClusterRelmapV2View *out)
{
	return test_relmap_physical_v2_read(identity, out);
}

static bool
retained_pair_write(const ClusterRelmapV2Namespace *identity, uint64 generation,
	const ClusterRelmapV2Owner *owner, const RelMapFile *pending,
	ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	unsigned slot = identity->shared ? 0 : 1;

	memset(&stage_pair_write_key, 0, sizeof(stage_pair_write_key));
	stage_pair_write_key.identity = *identity;
	stage_pair_write_key.generation = generation;
	stage_pair_write_key.origin = owner->node;
	stage_pair_write_key.top_full_xid = owner->top_full_xid;
	stage_pair_write_key.membership_epoch = owner->membership_epoch;
	stage_pair_write_key.admitted_incarnation = owner->admitted_incarnation;
	stage_pair_write_key.boot_incarnation = owner->boot_incarnation;
	stage_pair_write_end = owner->wal_end;
	stage_pair_writes++;
	stage_written[slot] = *owner;
	return test_relmap_physical_v2_write_pending(identity, generation, owner, pending, directory, lock);
}

bool
cluster_relmap_authority_v2_check_staged(const ClusterRelmapV2Image *image)
{
	bool ok = stage_physical_active && test_relmap_physical_v2_check_staged(image);

	if (ok) stage_pair_prepared[image->identity.shared ? 0 : 1] = *image;
	return ok;
}

ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_publish(const ClusterRelmapV2Image *image)
{
	return stage_physical_active ? test_relmap_physical_v2_publish(image)
		: CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
}

ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_discard_pending(const ClusterRelmapV2Image *image)
{
	return stage_physical_active ? test_relmap_physical_v2_discard_pending(image)
		: CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
}

void
RelationPreserveStorage(RelFileLocator locator, bool atCommit)
{
	UT_ASSERT(stage_physical_active && retention_mapping_held && !atCommit);
	UT_ASSERT_EQ(locator.relNumber, 900);
	UT_ASSERT_EQ(locator.spcOid, locator.dbOid == InvalidOid ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
	UT_ASSERT(locator.dbOid == InvalidOid || locator.dbOid == MyDatabaseId);
	stage_pair_preserves++;
	if (stage_pair_corrupt_preserve) {
		char path[MAXPGPATH];
		uint8 byte;
		int fd;

		retained_pair_path(path, locator.dbOid == InvalidOid ? 0 : 1, "pgrac_relmap_authority.bak");
		fd = open(path, O_RDWR | O_NOFOLLOW);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, &byte, 1, 32), 1);
		byte ^= 1;
		UT_ASSERT_EQ(pwrite(fd, &byte, 1, 32), 1);
		UT_ASSERT_EQ(fsync(fd), 0);
		UT_ASSERT_EQ(close(fd), 0);
		stage_pair_corrupt_preserve = false;
	}
}

void
ForceSyncCommit(void)
{
	UT_ASSERT(stage_physical_active && !retention_mapping_held && !held && !retention_held);
	stage_pair_syncs++;
}

static bool
retained_pair_terminal(const ClusterRelmapOwnerSampleV2 *sample)
{
	return stage_physical_active && relmap_owner_matches_v2(sample, false);
}

/* Controlled native COMMITTED fact: actual transaction commit WAL/CLOG
 * durability is deliberately outside this producer storage component test. */
bool
cluster_relmap_recovery_native_terminal_sync(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	const ClusterRelmapRetentionDirectory *d = &relmap_runtime_shmem->directories[retention_origin];

	if (!stage_physical_active) return retention_original_native_terminal_sync(sample, out);
	if (stage_pair_terminal_reader != NULL) return stage_pair_terminal_reader(sample, out);
	UT_ASSERT(stage_physical_active && stage_pair_finishing && caller_global_x);
	UT_ASSERT(!retention_mapping_held && !held && !retention_held && CritSectionCount == 0);
	stage_pair_terminal_reads++;
	if (!stage_pair_terminal_ready) return false;
	memset(out, 0, sizeof(*out));
	out->bounds.system_identifier = d->system_identifier;
	out->bounds.database_incarnation = d->database_incarnation;
	out->bounds.origin = sample->owner.node;
	out->bounds.membership_epoch = sample->owner.membership_epoch;
	out->bounds.admitted_incarnation = sample->owner.admitted_incarnation;
	out->bounds.boot_incarnation = sample->owner.boot_incarnation;
	out->bounds.native_floor = d->native_floor;
	out->bounds.last_issued = d->last_issued;
	out->top_full_xid = sample->owner.top_full_xid;
	out->status = TRANSACTION_STATUS_COMMITTED;
	return true;
}

static void
retained_pair_start(bool rollover)
{
	static const char *const names[] = { "pgrac_relmap_authority", "pgrac_relmap_authority.bak" };
	uint64 first = rollover ? UINT64_C(0x7fffffffe) : UINT64_C(0x500007ffe);
	char path[MAXPGPATH];

	retained_stage_start(first, true, false);
	retained_stage_pin(first - 1);
	memset(&shared_map, 0, sizeof(shared_map));
	memset(&local_map, 0, sizeof(local_map));
	memset(cluster_relmap_publish_arms, 0, sizeof(cluster_relmap_publish_arms));
	memset(cluster_relmap_v2_arms, 0, sizeof(cluster_relmap_v2_arms));
	cluster_relmap_v2_finish_entered = cluster_relmap_v2_prepare_unfinished = false;
	stage_pair_preserves = stage_pair_syncs = stage_pair_terminal_reads = stage_pair_drift_at_rename = 0;
	stage_pair_finishing = stage_pair_terminal_ready = stage_pair_corrupt_preserve = false;
	stage_pair_terminal_reader = NULL;
	stage_pair_terminal_write_observer = NULL;
	memset(stage_pair_initial, 0, sizeof(stage_pair_initial));
	memset(stage_pair_prepared, 0, sizeof(stage_pair_prepared));
	strlcpy(stage_pair_root, "/tmp/pgrac-retained-pair.XXXXXX", sizeof(stage_pair_root));
	UT_ASSERT(mkdtemp(stage_pair_root) != NULL);
	UT_ASSERT(snprintf(path, sizeof(path), "%s/global", stage_pair_root) < sizeof(path));
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	UT_ASSERT(snprintf(path, sizeof(path), "%s/base", stage_pair_root) < sizeof(path));
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	UT_ASSERT(snprintf(path, sizeof(path), "%s/base/%u", stage_pair_root, MyDatabaseId) < sizeof(path));
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	for (unsigned slot = 0; slot < 2; slot++) {
		ClusterRelmapV2Image *image = &stage_pair_initial[slot];
		uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
		int dir;

		image->identity = caller_namespace(slot == 0);
		image->committed_generation = stage_committed[slot];
		image->committed = stage_base;
		UT_ASSERT_EQ(cluster_relmap_v2_encode(image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
		for (unsigned copy = 0; copy < 2; copy++) {
			int fd;

			retained_pair_path(path, slot, names[copy]);
			fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(write(fd, bytes, sizeof(bytes)), sizeof(bytes));
			UT_ASSERT_EQ(fsync(fd), 0);
			UT_ASSERT_EQ(close(fd), 0);
		}
		retained_pair_path(path, slot, ".");
		dir = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
		UT_ASSERT(dir >= 0);
		UT_ASSERT_EQ(fsync(dir), 0);
		UT_ASSERT_EQ(close(dir), 0);
	}
	stage_physical_active = true;
	retention_terminal_match = retained_pair_terminal;
	test_relmap_physical_reset(stage_pair_root, retained_pair_observe);
}

static void
retained_pair_expect(unsigned slot, bool pending, bool published)
{
	static const char *const names[] = { "pgrac_relmap_authority", "pgrac_relmap_authority.bak" };
	ClusterRelmapV2Image expected = stage_pair_initial[slot];
	uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE], actual[CLUSTER_RELMAP_V2_FILE_SIZE];

	if (pending) expected = stage_pair_prepared[slot];
	if (published) {
		expected.committed_generation = stage_pair_prepared[slot].pending_generation;
		expected.committed = stage_pair_prepared[slot].pending;
	}
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&expected, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
	for (unsigned copy = 0; copy < 2; copy++) {
		char path[MAXPGPATH];
		struct stat st;
		int fd;

		retained_pair_path(path, slot, names[copy]);
		fd = open(path, O_RDONLY | O_NOFOLLOW);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(fstat(fd, &st), 0);
		UT_ASSERT_EQ(st.st_size, sizeof(actual));
		UT_ASSERT_EQ(read(fd, actual, sizeof(actual)), sizeof(actual));
		UT_ASSERT_EQ(memcmp(bytes, actual, sizeof(bytes)), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

static void
retained_pair_stop(void)
{
	static const char *const names[] = { "pgrac_relmap_authority", "pgrac_relmap_authority.bak",
		"pgrac_relmap_authority.tmp", "pgrac_relmap_authority.bak.tmp" };
	char path[MAXPGPATH];

	UT_ASSERT(!held && !retention_held && !retention_mapping_held && caller_global_x);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(lock_errors | native_publish_order_errors | caller_owner_lock_errors, 0);
	test_relmap_physical_reset(NULL, NULL);
	stage_physical_active = false;
	retention_terminal_match = NULL;
	for (unsigned slot = 0; slot < 2; slot++) {
		for (unsigned name = 0; name < lengthof(names); name++) {
			retained_pair_path(path, slot, names[name]);
			if (unlink(path) != 0) UT_ASSERT_EQ(errno, ENOENT);
		}
		retained_pair_path(path, slot, ".");
		/* rmdir does not accept a trailing dot on macOS. */
		path[strlen(path) - 2] = '\0';
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	UT_ASSERT(snprintf(path, sizeof(path), "%s/base", stage_pair_root) < sizeof(path));
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(stage_pair_root), 0);
	retained_clog_stop();
}

UT_TEST(retained_pair_native_prepare_and_finish_both_namespaces)
{
	for (unsigned which = 0; which < 4; which++) {
		bool publish = (which & 1) != 0;
		ClusterRelmapRetentionPin pins[3];
		XidNativeClogState state;
		TestRelmapPhysicalStats before, after;

		retained_pair_start((which & 2) != 0);
		UT_ASSERT(cluster_relmap_producer_arm_v2(true, &stage_updates));
		UT_ASSERT(cluster_relmap_producer_arm_v2(false, &stage_updates));
		UT_ASSERT_EQ(stage_pair_preserves, 2); UT_ASSERT_EQ(stage_pair_syncs, 2);
		UT_ASSERT_EQ(stage_inserts, 2); UT_ASSERT_EQ(native_publish_count, 1);
		UT_ASSERT_EQ(cluster_relmap_v2_arms[0].sample.owner.top_full_xid,
			cluster_relmap_v2_arms[1].sample.owner.top_full_xid);
		UT_ASSERT_EQ(shared_map.mappings[0].mapfilenumber, 900);
		UT_ASSERT_EQ(local_map.mappings[0].mapfilenumber, 900);
		retained_pair_expect(0, true, false);
		retained_pair_expect(1, true, false);
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		test_xid_native_clog_state(&state);
		retained_clog_birth(&state, which & 2 ? 4 : 7,
			which & 2 ? UINT64_C(0x800000003) : UINT64_C(0x500008000));
		stage_pair_finishing = true;
		stage_pair_terminal_ready = publish;
		UT_ASSERT(cluster_relmap_producer_finish_v2(publish));
		retained_pair_expect(0, false, publish);
		retained_pair_expect(1, false, publish);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
		UT_ASSERT(caller_global_x); /* The original caller still owns release. */
		before = test_relmap_physical_stats();
		UT_ASSERT(!cluster_relmap_producer_finish_v2(publish));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(!publish));
		after = test_relmap_physical_stats();
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		retained_pair_stop();
	}
}

UT_TEST(retained_pair_native_commit_refuses_without_terminal_fact)
{
	ClusterRelmapRetentionPin pins[3];
	TestRelmapPhysicalStats before, after;

	retained_pair_start(false);
	UT_ASSERT(cluster_relmap_producer_arm_v2(true, &stage_updates));
	UT_ASSERT(cluster_relmap_producer_arm_v2(false, &stage_updates));
	memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
	stage_pair_finishing = true;
	before = test_relmap_physical_stats();
	UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
	UT_ASSERT(cluster_relmap_v2_finish_entered);
	UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
	UT_ASSERT_EQ(stage_pair_terminal_reads, 1);
	UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
	after = test_relmap_physical_stats();
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
	retained_pair_expect(0, true, false);
	retained_pair_expect(1, true, false);
	retained_pair_stop();
}

UT_TEST(retained_pair_native_partial_prepare_keeps_custody_and_input)
{
	for (unsigned which = 0; which < 2; which++) {
		ClusterRelmapRetentionPin first;
		TestRelmapPhysicalStats before, after;

		retained_pair_start(false);
		UT_ASSERT(cluster_relmap_producer_arm_v2(true, &stage_updates));
		first = relmap_runtime_shmem->directories[4].pins[1];
		if (which == 0)
			test_relmap_physical_fail_sync(false, test_relmap_physical_stats().file_syncs + 2);
		else
			stage_pair_corrupt_preserve = true;
		UT_ASSERT(!cluster_relmap_producer_arm_v2(false, &stage_updates));
		UT_ASSERT(cluster_relmap_v2_prepare_unfinished);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT_EQ(stage_pair_syncs, 1);
		UT_ASSERT_EQ(local_map.num_mappings, 0);
		UT_ASSERT_EQ(memcmp(&first, &relmap_runtime_shmem->directories[4].pins[1], sizeof(first)), 0);
		UT_ASSERT(relmap_runtime_shmem->directories[4].pins[2].may_be_staged);
		retained_pair_expect(0, true, false);
		before = test_relmap_physical_stats();
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT(!cluster_relmap_producer_arm_v2(false, &stage_updates));
		after = test_relmap_physical_stats();
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT_EQ(native_publish_count, 1);
		retained_pair_stop();
	}
}

UT_TEST(retained_pair_native_partial_finish_is_one_shot)
{
	for (unsigned which = 0; which < 4; which++) {
		bool publish = (which & 1) != 0;
		ClusterRelmapRetentionPin pins[3];
		TestRelmapPhysicalStats before, after;

		retained_pair_start(false);
		UT_ASSERT(cluster_relmap_producer_arm_v2(true, &stage_updates));
		UT_ASSERT(cluster_relmap_producer_arm_v2(false, &stage_updates));
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		stage_pair_finishing = true; stage_pair_terminal_ready = publish;
		test_relmap_physical_fail_sync(false,
			test_relmap_physical_stats().file_syncs + ((which & 2) ? 3 : 1));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(publish));
		UT_ASSERT(cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
		if (which & 2) retained_pair_expect(0, false, publish);
		before = test_relmap_physical_stats();
		UT_ASSERT(!cluster_relmap_producer_finish_v2(publish));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(!publish));
		after = test_relmap_physical_stats();
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		retained_pair_stop();
	}
}

UT_TEST(retained_pair_native_late_owner_drift_keeps_all_debt)
{
	for (unsigned publish = 0; publish < 2; publish++) {
		ClusterRelmapRetentionPin pins[3];
		TestRelmapPhysicalStats before, after;

		retained_pair_start(false);
		UT_ASSERT(cluster_relmap_producer_arm_v2(true, &stage_updates));
		UT_ASSERT(cluster_relmap_producer_arm_v2(false, &stage_updates));
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		stage_pair_finishing = true; stage_pair_terminal_ready = publish;
		stage_pair_drift_at_rename = test_relmap_physical_stats().renames + 2;
		UT_ASSERT(!cluster_relmap_producer_finish_v2(publish));
		UT_ASSERT(cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
		retained_pair_expect(0, false, publish);
		retained_pair_expect(1, true, false);
		before = test_relmap_physical_stats();
		UT_ASSERT(!cluster_relmap_producer_finish_v2(!publish));
		after = test_relmap_physical_stats();
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT_EQ(native_publish_count, 1);
		retained_pair_stop();
	}
}
