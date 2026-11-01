/*-------------------------------------------------------------------------
 * test_cluster_relmap_control.c
 *    Original control selection, authority paths and complete local receipts.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_control.c
 * NOTES
 *    Real ROOT/registry/pair files; original CF, LWLock and native config
 *    leaves are controlled. This is component evidence, not admission.
 *-------------------------------------------------------------------------
 */
#define TEST_CONTROL_RELMAP_CONSUMER 1
#include "test_cluster_control_relmap_selection.c"
#include "cluster/cluster_relmap_control.h"
#include "cluster/cluster_relmap_authority.h"
#include "utils/relmapper.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"

static LWLockPadded local_locks[256];
LWLockPadded *MainLWLockArray = local_locks;
bool enableFsync = true;
volatile uint32 InterruptHoldoffCount;
volatile uint32 CritSectionCount;
/* Assertions remain fatal in this standalone fixture. */
void ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "TRAP: %s at %s:%d\n", condition, file, line);
	abort();
}
static bool mapping_held, control_held, release_requires_holdoff;
static unsigned mapping_acquires, pair_reads, pair_syncs, hook_at;
static void (*after_pair)(void);
extern bool control_original_read(const ClusterRecoveryFileIdentityV2 *, ClusterRelmapFilePairV2 *);
extern bool control_original_checkpoint(const ClusterRecoveryFileIdentityV2 *, ClusterRelmapFilePairV2 *);
bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock != RelationMappingLock || mode != LW_SHARED || mapping_held || !cf_held)
		abort();
	mapping_held = true;
	mapping_acquires++;
	return true;
}
void LWLockRelease(LWLock *lock)
{
	if (lock != RelationMappingLock || !mapping_held)
		abort();
	if (release_requires_holdoff && !InterruptHoldoffCount) abort();
	mapping_held = false;
}
bool LWLockHeldByMe(LWLock *lock)
{
	return (lock == RelationMappingLock && mapping_held) || (lock == ControlFileLock && control_held);
}
static void pair_hook(void)
{
	if (after_pair && pair_reads + pair_syncs == hook_at)
		after_pair();
}
bool cluster_relmap_authority_managed_read_v2(const ClusterRecoveryFileIdentityV2 *id,
	ClusterRelmapFilePairV2 *out)
{
	bool ok = control_original_read(id, out);
	pair_reads++;
	pair_hook();
	return ok;
}
bool cluster_relmap_authority_managed_checkpoint_v2(const ClusterRecoveryFileIdentityV2 *id,
	ClusterRelmapFilePairV2 *out)
{
	bool ok = control_original_checkpoint(id, out);
	pair_syncs++;
	pair_hook();
	return ok;
}
static void consumer_begin(unsigned count)
{
	char path[200];
	inventory_begin(count);
	snprintf(path, sizeof(path), "%s/base", selection_path);
	if (mkdir(path, 0700)) abort();
	for (unsigned i = 0; i < count; i++) {
		ClusterRelmapV2Image image = {0};
		uint8 bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
		int dir;
		if (i) {
			snprintf(path, sizeof(path), "%s/base/%u", selection_path, completed[i].database_oid);
			if (mkdir(path, 0700)) abort();
		} else snprintf(path, sizeof(path), "%s/global", selection_path);
		dir = open(path, O_RDONLY | O_DIRECTORY);
		image.identity = (ClusterRelmapV2Namespace){11, 12, completed[i].database_oid, !i};
		image.committed_generation = 9;
		image.committed.magic = RELMAPPER_FILEMAGIC;
		image.committed.num_mappings = 1;
		image.committed.mappings[0] = (RelMapping){1259, 40000 + i};
		INIT_CRC32C(image.committed.crc);
		COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
		FIN_CRC32C(image.committed.crc);
		if (dir < 0 || !cluster_relmap_file_encode_v2(&completed[i], &image, bytes, sizeof(bytes))) abort();
		for (unsigned c = 0; c < 2; c++) {
			int fd = openat(dir, c ? "pgrac_relmap_authority.bak" : "pgrac_relmap_authority",
				O_WRONLY | O_CREAT | O_EXCL, 0600);
			if (fd < 0 || write(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd)) abort();
		}
		if (close(dir)) abort();
	}
	mapping_held = control_held = release_requires_holdoff = false;
	mapping_acquires = pair_reads = pair_syncs = hook_at = 0;
	after_pair = NULL;
	enableFsync = true;
}
static void consumer_end(unsigned count)
{
	char path[200];
	for (unsigned i = 0; i < count; i++) {
		int dir;
		if (i) snprintf(path, sizeof(path), "%s/base/%u", selection_path, completed[i].database_oid);
		else snprintf(path, sizeof(path), "%s/global", selection_path);
		dir = open(path, O_RDONLY | O_DIRECTORY);
		if (dir < 0) abort();
		unlinkat(dir, "pgrac_relmap_authority", 0);
		unlinkat(dir, "pgrac_relmap_authority.bak", 0);
		if (close(dir) || (i && rmdir(path))) abort();
	}
	snprintf(path, sizeof(path), "%s/base", selection_path);
	if (rmdir(path)) abort();
	selection_end();
}
static bool checkpoint(ClusterRelmapCheckpointReceiptV2 **fresh)
{
	return CheckPointRelationMapManagedV2(&expected_config, 65536, 1024 * 1024, fresh);
}
#include "cluster/cluster_cf_enqueue.h"
static unsigned cf_locks, cf_unlocks, legacy_reads;
static bool cf_lock_fail, cf_release_fail;
bool cluster_cf_lock(LOCKMODE mode)
{
	if (mode != ShareLock || cf_held || mapping_held) abort();
	cf_locks++;
	if (cf_lock_fail) return false;
	cf_held = true; return true;
}
ClusterCfReleaseResult cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	if (mode != ShareLock || !cf_held || mapping_held) abort();
	cf_unlocks++; cf_held = false;
	return cf_release_fail ? CLUSTER_CF_RELEASE_UNCONFIRMED : CLUSTER_CF_RELEASE_CONFIRMED;
}
static void cluster_read_relmap_from_authority(RelMapFile *map, const char *path,
	bool lock_held, int elevel)
{
	(void)path; (void)lock_held; (void)elevel;
	legacy_reads++;
	memset(map, 0, sizeof(*map)); map->num_mappings = 7;
}
/* Standalone ERROR leaf; complete production wrappers are extracted below. */
#undef ereport
#define ereport(level, ...) pg_re_throw()
#include "test_cluster_relmap_control_entries.inc"
UT_TEST(cache_uses_registered_origin_and_original_shared_and_cross_database_paths)
{
	RelMapFile map;
	consumer_begin(3);
	UT_ASSERT(RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, &map));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(RelationMapReadManagedV2(&expected_config, 65536, 1002, DEFAULTTABLESPACE_OID, &map));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40002);
	UT_ASSERT_EQ(pair_reads, 2);
	UT_ASSERT(!mapping_held && cf_held);
	UT_ASSERT(!RelationMapReadManagedV2(&expected_config, 65536, 1002, GLOBALTABLESPACE_OID, &map));
	UT_ASSERT_EQ(pair_reads, 2);
	consumer_end(3);
}
UT_TEST(complete_checkpoint_exceeds_64_and_preserves_all_pair_and_source_copies)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterRelmapCheckpointSummaryV2 summary;
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	ClusterRecoveryCheckpointSourceV2 source;
	ClusterControlRelmapSelectionV2 *fresh = NULL;
	consumer_begin(73);
	UT_ASSERT(checkpoint(&r));
	UT_ASSERT(cluster_relmap_checkpoint_summary_v2(r, &summary));
	UT_ASSERT_EQ(summary.control.logical_count, 73);
	UT_ASSERT_EQ(summary.control.source_count, 2);
	UT_ASSERT_EQ(pair_syncs, 73);
	UT_ASSERT_EQ(mapping_acquires, 1);
	for (unsigned i = 0; i < 73; i++) {
		UT_ASSERT(cluster_relmap_checkpoint_file_at_v2(r, i, &id, &pair));
		UT_ASSERT(identity_same(&id, &completed[i]));
		UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40000 + i);
	}
	UT_ASSERT(cluster_relmap_checkpoint_source_at_v2(r, 1, &source));
	UT_ASSERT_EQ(source.replay_end, sources[1].replay_end);
	UT_ASSERT(!cluster_relmap_checkpoint_file_at_v2(r, 73, &id, &pair));
	UT_ASSERT(!cluster_relmap_checkpoint_source_at_v2(r, 2, &source));
	/* A later publisher takes a new CF interval and a new original selection. */
	cf_sequence++;
	UT_ASSERT(inventory_read(&fresh));
	UT_ASSERT(cluster_relmap_checkpoint_recheck_v2(r, fresh));
	cluster_control_root_relmap_selection_destroy_v4(&fresh);
	cluster_relmap_checkpoint_destroy_v2(&r);
	UT_ASSERT(r == NULL && cf_held && !mapping_held && fcntl(selection_dir, F_GETFD) >= 0);
	cluster_relmap_checkpoint_destroy_v2(&r);
	consumer_end(73);
}
UT_TEST(whole_receipt_budget_refuses_before_any_native_file_io)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	consumer_begin(73);
	UT_ASSERT(!CheckPointRelationMapManagedV2(&expected_config, 65536, 1024, &r));
	UT_ASSERT(r == NULL && pair_syncs == 0 && mapping_acquires == 0);
	UT_ASSERT(!CheckPointRelationMapManagedV2(&expected_config, 256, 1024 * 1024, &r));
	UT_ASSERT(r == NULL && pair_syncs == 0);
	consumer_end(73);
}
UT_TEST(missing_last_backup_refuses_the_complete_batch_without_partial_receipt)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	char path[200];
	consumer_begin(73);
	snprintf(path, sizeof(path), "%s/base/1072/pgrac_relmap_authority.bak", selection_path);
	UT_ASSERT(unlink(path) == 0);
	UT_ASSERT(!checkpoint(&r));
	UT_ASSERT(r == NULL && pair_syncs == 73 && !mapping_held && cf_held);
	consumer_end(73);
}
static void drift_root(void) { selection_change(); }
static void drift_cf(void) { cf_sequence++; }
static void late_config_error(void) { config_error = true; }
static void error_inside_pair(void) { release_requires_holdoff = true; pg_re_throw(); }
UT_TEST(cache_late_control_drift_preserves_output_without_legacy_fallback)
{
	RelMapFile map, before;
	consumer_begin(2);
	memset(&map, 0xa5, sizeof(map)); before = map;
	after_pair = drift_root; hook_at = 1;
	UT_ASSERT(!RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, &map));
	UT_ASSERT(memcmp(&map, &before, sizeof(map)) == 0 && !mapping_held && cf_held);
	UT_ASSERT_EQ(pair_reads, 1);
	consumer_end(2);
}
UT_TEST(checkpoint_late_control_drift_and_cf_reacquire_refuse)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	consumer_begin(2);
	after_pair = drift_cf; hook_at = 2;
	UT_ASSERT(!checkpoint(&r));
	UT_ASSERT(r == NULL && !mapping_held && cf_held);
	consumer_end(2);
	consumer_begin(2);
	after_pair = drift_root; hook_at = 2;
	UT_ASSERT(!checkpoint(&r));
	UT_ASSERT(r == NULL && !mapping_held && cf_held);
	consumer_end(2);
}
UT_TEST(error_after_native_io_cleans_only_local_state_and_mapping_lock)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	RelMapFile map, before;
	volatile bool caught = false;
	consumer_begin(2);
	after_pair = late_config_error; hook_at = 2;
	PG_TRY(); { (void)checkpoint(&r); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && r == NULL && cf_held && !mapping_held);
	consumer_end(2);
	consumer_begin(2);
	caught = false;
	memset(&map, 0xa5, sizeof(map)); before = map;
	after_pair = late_config_error; hook_at = 1;
	PG_TRY(); { (void)RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, &map); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && cf_held && !mapping_held && memcmp(&map, &before, sizeof(map)) == 0);
	consumer_end(2);
	consumer_begin(2);
	caught = false;
	after_pair = error_inside_pair; hook_at = 1;
	PG_TRY(); { (void)checkpoint(&r); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && r == NULL && !mapping_held && cf_held && InterruptHoldoffCount == 0);
	consumer_end(2);
}
UT_TEST(entered_mapping_lock_and_missing_original_cf_refuse_without_acquiring)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	RelMapFile map;
	consumer_begin(1);
	mapping_held = true;
	UT_ASSERT(!checkpoint(&r));
	UT_ASSERT(!RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, &map));
	UT_ASSERT(mapping_held && mapping_acquires == 0 && pair_reads + pair_syncs == 0);
	mapping_held = false; cf_held = false;
	UT_ASSERT(!checkpoint(&r));
	UT_ASSERT(!RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, &map));
	UT_ASSERT(!cf_held && mapping_acquires == 0);
	cf_held = true;
	consumer_end(1);
}
UT_TEST(output_aliases_and_nonfresh_receipts_refuse_before_effects)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterRelmapCheckpointSummaryV2 summary;
	ClusterRelmapFilePairV2 pair;
	consumer_begin(2);
	UT_ASSERT(!RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, (void *)&expected_config));
	UT_ASSERT(!CheckPointRelationMapManagedV2(&expected_config, 65536, 1024 * 1024, (void *)&expected_config));
	UT_ASSERT(pair_reads + pair_syncs == 0);
	UT_ASSERT(checkpoint(&r));
	UT_ASSERT(!checkpoint(&r));
	UT_ASSERT(!cluster_relmap_checkpoint_summary_v2(r, (void *)r));
	UT_ASSERT(!cluster_relmap_checkpoint_file_at_v2(r, 0, (void *)&pair, &pair));
	UT_ASSERT(cluster_relmap_checkpoint_summary_v2(r, &summary));
	cluster_relmap_checkpoint_destroy_v2(&r);
	consumer_end(2);
}
UT_TEST(new_control_reference_mismatch_poison_cannot_be_revived)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterControlRelmapSelectionV2 *fresh = NULL;
	ClusterRelmapCheckpointSummaryV2 summary;
	consumer_begin(2);
	UT_ASSERT(checkpoint(&r));
	inventory[0].image_digest[0] ^= 1; inventory_write();
	UT_ASSERT(inventory_read(&fresh));
	UT_ASSERT(!cluster_relmap_checkpoint_recheck_v2(r, fresh));
	UT_ASSERT(!cluster_relmap_checkpoint_summary_v2(r, &summary));
	cluster_control_root_relmap_selection_destroy_v4(&fresh);
	inventory[0].image_digest[0] ^= 1; inventory_write();
	UT_ASSERT(inventory_read(&fresh));
	UT_ASSERT(!cluster_relmap_checkpoint_recheck_v2(r, fresh));
	cluster_control_root_relmap_selection_destroy_v4(&fresh);
	cluster_relmap_checkpoint_destroy_v2(&r);
	consumer_end(2);
}
UT_TEST(publisher_recheck_error_poison_retains_original_custody)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterControlRelmapSelectionV2 *fresh = NULL;
	ClusterRelmapCheckpointSummaryV2 summary;
	volatile bool caught = false;
	consumer_begin(2);
	UT_ASSERT(checkpoint(&r));
	UT_ASSERT(inventory_read(&fresh));
	config_error = true;
	PG_TRY(); { (void)cluster_relmap_checkpoint_recheck_v2(r, fresh); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	config_error = false;
	UT_ASSERT(caught && cf_held && !mapping_held);
	UT_ASSERT(!cluster_relmap_checkpoint_summary_v2(r, &summary));
	cluster_control_root_relmap_selection_destroy_v4(&fresh);
	cluster_relmap_checkpoint_destroy_v2(&r);
	consumer_end(2);
}
UT_TEST(control_file_lock_refuses_before_selection_or_pair_io)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	RelMapFile map;
	bool read_ok, checkpoint_ok, no_effect;
	consumer_begin(1);
	control_held = true;
	read_ok = RelationMapReadManagedV2(&expected_config, 65536, 0, GLOBALTABLESPACE_OID, &map);
	checkpoint_ok = checkpoint(&r);
	no_effect = mapping_acquires == 0 && pair_reads + pair_syncs == 0 && dependencies == 0;
	cluster_relmap_checkpoint_destroy_v2(&r);
	control_held = false;
	consumer_end(1);
	UT_ASSERT(!read_ok && !checkpoint_ok && no_effect);
}

uint64 GetSystemIdentifier(void);
static uint64 actual_system = 11;
uint64 GetSystemIdentifier(void) { return actual_system; }
UT_TEST(format_requires_complete_selected_registration_before_dispatch)
{
	ClusterControlRelmapFormatV2 f;
	consumer_begin(2);
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &f) == 0);
	UT_ASSERT(f.root_version == 4 && !memcmp(&f.config, &expected_config, sizeof(f.config)));
	UT_ASSERT(pair_reads == 0 && pair_syncs == 0 && mapping_acquires == 0);
	consumer_end(2);
}
UT_TEST(format_unknown_or_partial_never_becomes_legacy)
{
	ClusterControlRelmapFormatV2 f, before;
	int fd;
	consumer_begin(2);
	memset(&f, 0xa5, sizeof(f)); before = f;
	fd = openat(selection_dir, "pgrac_control_root", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, 66048) == 0 && close(fd) == 0);
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &f) != 0);
	UT_ASSERT(!memcmp(&f, &before, sizeof(f)) && pair_reads == 0);
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	reg.coverage.file_count = 1; inventory[0].copy_count = 1; inventory_write();
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &f) != 0);
	UT_ASSERT(!memcmp(&f, &before, sizeof(f)));
	consumer_end(2);
}
UT_TEST(format_missing_cf_late_change_and_error_preserve_output)
{
	ClusterControlRelmapFormatV2 f, before;
	volatile bool caught = false;
	consumer_begin(2);
	memset(&f, 0xa5, sizeof(f)); before = f;
	cf_held = false;
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &f) != 0 && dependencies == 0);
	cf_held = true; config_hook = selection_change;
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &f) != 0);
	UT_ASSERT(!memcmp(&f, &before, sizeof(f)));
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	config_error = true;
	PG_TRY(); { (void)cluster_control_root_relmap_format_locked_v2(65536, &f); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	config_error = false;
	UT_ASSERT(caught && cf_held && !memcmp(&f, &before, sizeof(f)));
	consumer_end(2);
}
UT_TEST(actual_cache_control_dispatch_uses_db_tablespace_and_cf_order)
{
	RelMapFile map;
	consumer_begin(3);
	cf_locks = cf_unlocks = legacy_reads = 0; cf_lock_fail = cf_release_fail = false;
	cf_held = false;
	cluster_read_relmap_from_control(&map, "not-an-identity", 1002, DEFAULTTABLESPACE_OID, ERROR);
	UT_ASSERT(map.mappings[0].mapfilenumber == 40002 && cf_locks == 1 && cf_unlocks == 1);
	UT_ASSERT(!cf_held && !mapping_held && legacy_reads == 0);
	cf_held = true;
	cluster_read_relmap_from_control(&map, "global", 0, GLOBALTABLESPACE_OID, ERROR);
	UT_ASSERT(map.mappings[0].mapfilenumber == 40000 && cf_locks == 1 && cf_unlocks == 1 && cf_held);
	consumer_end(3);
}
UT_TEST(actual_cache_managed_failure_never_uses_legacy_or_changes_output)
{
	RelMapFile map, before;
	volatile bool caught = false;
	consumer_begin(2);
	cf_locks = cf_unlocks = legacy_reads = 0; cf_release_fail = cf_lock_fail = false;
	memset(&map, 0x5a, sizeof(map)); before = map;
	cf_held = false;
	PG_TRY(); { cluster_read_relmap_from_control(&map, "global", 1001, GLOBALTABLESPACE_OID, ERROR); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !memcmp(&map, &before, sizeof(map)) && !legacy_reads);
	UT_ASSERT(cf_locks == 1 && cf_unlocks == 1 && !cf_held && !mapping_held);
	cf_held = true; consumer_end(2);
}
UT_TEST(actual_cache_release_uncertainty_preserves_output_and_error_cleanup)
{
	RelMapFile map, before;
	volatile bool caught = false;
	consumer_begin(2);
	cf_locks = cf_unlocks = legacy_reads = 0; cf_lock_fail = false; cf_release_fail = true;
	memset(&map, 0x5a, sizeof(map)); before = map; cf_held = false;
	PG_TRY(); { cluster_read_relmap_from_control(&map, "global", 0, GLOBALTABLESPACE_OID, ERROR); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !memcmp(&map, &before, sizeof(map)) && !legacy_reads && cf_unlocks == 1);
	cf_release_fail = false; cf_held = true; consumer_end(2);
}
UT_TEST(actual_checkpoint_local_custody_releases_only_owned_cf)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterRelmapCheckpointSummaryV2 summary;
	consumer_begin(73);
	cf_locks = cf_unlocks = legacy_reads = 0; cf_lock_fail = cf_release_fail = false;
	cf_held = false;
	UT_ASSERT(CheckPointRelationMapControlV2(&r));
	UT_ASSERT(!cf_held && !mapping_held && cf_locks == 1 && cf_unlocks == 1 && pair_syncs == 73);
	UT_ASSERT(cluster_relmap_checkpoint_summary_v2(r, &summary) && summary.control.logical_count == 73);
	cluster_relmap_checkpoint_destroy_v2(&r);
	cf_held = true;
	UT_ASSERT(CheckPointRelationMapControlV2(&r) && cf_held && cf_locks == 1 && cf_unlocks == 1);
	cluster_relmap_checkpoint_destroy_v2(&r);
	consumer_end(73);
}
UT_TEST(actual_checkpoint_late_error_and_unconfirmed_release_return_no_receipt)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	volatile bool caught = false;
	consumer_begin(2);
	cf_locks = cf_unlocks = legacy_reads = 0; cf_lock_fail = false; cf_release_fail = true;
	cf_held = false;
	UT_ASSERT(!CheckPointRelationMapControlV2(&r) && !r && !cf_held && cf_unlocks == 1);
	cf_release_fail = false; cf_held = false; config_error = true;
	PG_TRY(); { (void)CheckPointRelationMapControlV2(&r); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	config_error = false;
	UT_ASSERT(caught && !r && !cf_held && !mapping_held && cf_unlocks == 2);
	cf_held = true; consumer_end(2);
}
UT_TEST(actual_control_unknown_format_does_not_reach_old_cache_or_checkpoint)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	RelMapFile map;
	volatile bool caught = false;
	consumer_begin(1);
	legacy_reads = 0; cf_lock_fail = cf_release_fail = false;
	config_ok = false;
	PG_TRY(); { cluster_read_relmap_from_control(&map, "global", 0, GLOBALTABLESPACE_OID, ERROR); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !legacy_reads && cf_held && !CheckPointRelationMapControlV2(&r) && !r);
	config_ok = true; consumer_end(1);
}
UT_TEST(exact_legacy_control_keeps_original_reader_and_no_managed_receipt)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterControlRelmapFormatV2 format;
	RelMapFile map;
	consumer_begin(1);
	UT_ASSERT(cluster_control_root_v3_encode(&root) == 0);
	for (unsigned i = 0; i < 2; i++) {
		int fd = openat(selection_dir, i ? "pgrac_control_root.bak" : "pgrac_control_root", O_WRONLY | O_TRUNC);
		UT_ASSERT(fd >= 0 && write(fd, root.bytes, sizeof(root.bytes)) == sizeof(root.bytes) && close(fd) == 0);
	}
	legacy_reads = cf_locks = cf_unlocks = 0;
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &format) == 0 && format.root_version == 3);
	cluster_read_relmap_from_control(&map, "global", 0, GLOBALTABLESPACE_OID, ERROR);
	UT_ASSERT(map.num_mappings == 7 && legacy_reads == 1 && !pair_reads);
	UT_ASSERT(CheckPointRelationMapControlV2(&r) && !r && !pair_syncs && cf_held);
	consumer_end(1);
}
UT_TEST(native_system_and_complete_budget_remain_independent_of_target)
{
	ClusterControlRelmapFormatV2 format;
	consumer_begin(2);
	actual_system = 12;
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(65536, &format) != 0);
	actual_system = 11;
	UT_ASSERT(cluster_control_root_relmap_format_locked_v2(256, &format) != 0);
	UT_ASSERT(pair_reads == 0 && pair_syncs == 0 && mapping_acquires == 0);
	consumer_end(2);
}
UT_TEST(actual_callers_refuse_before_cf_if_mapping_or_control_is_held)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	RelMapFile map;
	volatile bool caught = false;
	consumer_begin(1);
	cf_locks = cf_unlocks = legacy_reads = 0; cf_held = false; mapping_held = true;
	PG_TRY(); { cluster_read_relmap_from_control(&map, "global", 0, GLOBALTABLESPACE_OID, ERROR); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !CheckPointRelationMapControlV2(&r) && !r && !cf_locks);
	mapping_held = false; control_held = true;
	UT_ASSERT(!CheckPointRelationMapControlV2(&r) && !cf_locks && !legacy_reads);
	control_held = false; cf_held = true; consumer_end(1);
}
int main(void)
{
	UT_PLAN(43);
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_RUN(test_control_selects_complete_original_file_identity);
	UT_RUN(test_control_selection_refuses_missing_custody_and_dependency);
	UT_RUN(test_control_selection_late_root_and_error_preserve_output);
	UT_RUN(test_control_selection_valid_outer_hash_cannot_hide_noncanonical_source);
	UT_RUN(test_control_selection_key_and_named_directory_remain_original);
	UT_RUN(test_control_selection_ambiguous_births_refuse_without_latest_rule);
	UT_RUN(test_complete_decoded_vector_exceeds_active_pin_capacity);
	UT_RUN(test_complete_inventory_origin_lookup_and_sources);
	UT_RUN(test_inventory_requires_original_custody_and_fresh_output);
	UT_RUN(test_inventory_rejects_ambiguous_birth_origin_and_incomplete_copy);
	UT_RUN(test_inventory_requires_complete_configured_source_set_and_shared_address);
	UT_RUN(test_complete_receipt_membership_refuses_partial_extra_and_wrong_identity);
	UT_RUN(test_post_io_root_registry_error_and_cf_reacquire_cannot_revive);
	UT_RUN(test_post_io_named_directory_remains_original);
	UT_RUN(test_actual_pair_sync_readback_then_complete_control_recheck);
	UT_RUN(test_copied_checkpoint_reference_does_not_revive_cf_authority);
	UT_RUN(cache_uses_registered_origin_and_original_shared_and_cross_database_paths);
	UT_RUN(complete_checkpoint_exceeds_64_and_preserves_all_pair_and_source_copies);
	UT_RUN(whole_receipt_budget_refuses_before_any_native_file_io);
	UT_RUN(missing_last_backup_refuses_the_complete_batch_without_partial_receipt);
	UT_RUN(cache_late_control_drift_preserves_output_without_legacy_fallback);
	UT_RUN(checkpoint_late_control_drift_and_cf_reacquire_refuse);
	UT_RUN(error_after_native_io_cleans_only_local_state_and_mapping_lock);
	UT_RUN(entered_mapping_lock_and_missing_original_cf_refuse_without_acquiring);
	UT_RUN(output_aliases_and_nonfresh_receipts_refuse_before_effects);
	UT_RUN(new_control_reference_mismatch_poison_cannot_be_revived);
	UT_RUN(publisher_recheck_error_poison_retains_original_custody);
	UT_RUN(control_file_lock_refuses_before_selection_or_pair_io);
	UT_RUN(format_requires_complete_selected_registration_before_dispatch);
	UT_RUN(format_unknown_or_partial_never_becomes_legacy);
	UT_RUN(format_missing_cf_late_change_and_error_preserve_output);
	UT_RUN(actual_cache_control_dispatch_uses_db_tablespace_and_cf_order);
	UT_RUN(actual_cache_managed_failure_never_uses_legacy_or_changes_output);
	UT_RUN(actual_cache_release_uncertainty_preserves_output_and_error_cleanup);
	UT_RUN(actual_checkpoint_local_custody_releases_only_owned_cf);
	UT_RUN(actual_checkpoint_late_error_and_unconfirmed_release_return_no_receipt);
	UT_RUN(actual_control_unknown_format_does_not_reach_old_cache_or_checkpoint);
	UT_RUN(exact_legacy_control_keeps_original_reader_and_no_managed_receipt);
	UT_RUN(native_system_and_complete_budget_remain_independent_of_target);
	UT_RUN(actual_callers_refuse_before_cf_if_mapping_or_control_is_held);
	UT_DONE();
	return ut_failed_count != 0;
}
