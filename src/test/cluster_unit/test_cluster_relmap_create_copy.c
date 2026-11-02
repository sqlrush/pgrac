/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_copy.c
 *    Original control reservation and real source/target relmap files.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_copy.c
 * NOTES
 *    CF, global X and native owner leaves are controlled; file I/O is real.
 *-------------------------------------------------------------------------
 */
#define TEST_CONTROL_CLOG_PREPARATION 1
#define TEST_CONTROL_RECOVERY_CREATE 1
#include "test_cluster_control_native_reservations.c"
#undef main
#include "cluster/cluster_relmap_control.h"
#include "cluster/cluster_relmap_authority.h"
#include "storage/fd.h"
#include "access/xloginsert.h"
#include "access/xlog.h"
#include "common/cluster_relmap_create.h"

extern bool cluster_relmap_control_copy_create_v2(ClusterControlNativeRelmapCreateV2 *, Oid, Oid);
static bool copy_x = true;
bool cluster_relmap_authority_x_held(void);
bool cluster_relmap_authority_x_held(void) { return copy_x; }
bool enableFsync = true;
volatile uint32 InterruptHoldoffCount;
static unsigned copy_syncs, fail_sync, drift_sync, copy_closes, fail_close;
static bool read_error, release_error, release_error_after;
static unsigned wal_begins, wal_registers, wal_inserts, wal_flushes;
static unsigned wal_error_phase, wal_drift;
static XLogRecPtr wal_result;
static uint8 wal_rmid, wal_info;
static bool wal_recovery, wal_watch_target, wal_bad_order;
static unsigned char wal_bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
static ClusterControlNativeRelmapCreateV2 *wal_creator;
static int target_dir(void);
static bool recovery_sync_error, recovery_payload_drift;
static XLogReaderState *recovery_crc_drift;

static void wal_target_still_absent(void)
{
	int d;
	if (!wal_watch_target) return;
	d=target_dir();
	if (d<0 || faccessat(d,"pgrac_relmap_authority",F_OK,0)==0
		|| faccessat(d,"pgrac_relmap_authority.bak",F_OK,0)==0) wal_bad_order=true;
	if (d>=0 && close(d)) abort();
}
bool RecoveryInProgress(void) { return wal_recovery; }
void XLogBeginInsert(void)
{
	++wal_begins; wal_target_still_absent();
	if (wal_error_phase==1) { InterruptHoldoffCount=0; pg_re_throw(); }
}
void XLogRegisterData(char *data, uint32 length)
{
	if (length!=sizeof(wal_bytes) || wal_begins!=1 || wal_registers) abort();
	++wal_registers; memcpy(wal_bytes,data,length);
}
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)
{
	if (wal_registers!=1 || wal_inserts) abort();
	++wal_inserts; wal_rmid=rmid; wal_info=info; wal_target_still_absent();
	if (wal_error_phase==2) { InterruptHoldoffCount=0; pg_re_throw(); }
	return wal_result;
}
void XLogFlush(XLogRecPtr end)
{
	if (end!=wal_result || wal_inserts!=1 || wal_flushes) abort();
	++wal_flushes; wal_target_still_absent();
	if (wal_error_phase==3) { InterruptHoldoffCount=0; pg_re_throw(); }
	if (wal_drift==1) ++cf_sequence;
	if (wal_drift==2) ++wal_creator->expected.creation_generation;
	if (wal_drift==3) wal_creator->batch_uuid[15]^=1;
}

static int copy_sync(int fd)
{
	++copy_syncs;
	if (wal_watch_target && wal_flushes!=1) wal_bad_order=true;
	if (copy_syncs == drift_sync) ++cf_sequence;
	if (recovery_payload_drift) { recovery_payload_drift=false; wal_bytes[64+128]^=1; }
	if (recovery_crc_drift) { recovery_crc_drift->record->header.xl_crc^=1; recovery_crc_drift=NULL; }
	if (recovery_sync_error) { InterruptHoldoffCount=0; pg_re_throw(); }
	return copy_syncs == fail_sync ? -1 : fsync(fd);
}
static int copy_close(int fd)
{
	int result = close(fd);
	return ++copy_closes == fail_close ? -1 : result;
}
#define close copy_close
#define pg_fsync copy_sync
#include "../../common/cluster_relmap_file_io.c"
#undef pg_fsync
#undef close
bool copy_original_read(const ClusterRecoveryFileIdentityV2 *, ClusterRelmapFilePairV2 *);
#define cluster_relmap_authority_managed_read_v2 copy_original_read
#include "../../backend/cluster/cluster_relmap_authority.c"
#undef cluster_relmap_authority_managed_read_v2
bool cluster_relmap_authority_managed_read_v2(const ClusterRecoveryFileIdentityV2 *id,
	ClusterRelmapFilePairV2 *out)
{
	bool ok = copy_original_read(id,out);
	if (read_error) { InterruptHoldoffCount=0; pg_re_throw(); }
	return ok;
}

bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return lock == RelationMappingLock && held_family_lock == lock && mode == LW_EXCLUSIVE;
}
bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock != RelationMappingLock || mode != LW_EXCLUSIVE || held_family_lock
		|| !cf_held || !batch_exclusive || !copy_x) abort();
	held_family_lock = lock;
	++InterruptHoldoffCount;
	return true;
}
void LWLockRelease(LWLock *lock)
{
	if (lock != RelationMappingLock || held_family_lock != lock || !InterruptHoldoffCount) abort();
	if (release_error) { release_error=false; InterruptHoldoffCount=0; pg_re_throw(); }
	held_family_lock = NULL;
	--InterruptHoldoffCount;
	if (release_error_after) { release_error_after=false; InterruptHoldoffCount=0; pg_re_throw(); }
}
void ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr,"TRAP: %s at %s:%d\n",condition,file,line);
	abort();
}
static void copy_begin(void)
{
	ClusterRelmapV2Image image = {0};
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	char path[MAXPGPATH];
	int d;
	table_reset(); owner_begin();
	InterruptHoldoffCount=0; held_family_lock=NULL;
	copy_x = enableFsync = true;
	copy_syncs = fail_sync = drift_sync = copy_closes = fail_close = 0;
	read_error=release_error=release_error_after=false;
	wal_begins=wal_registers=wal_inserts=wal_flushes=0;
	wal_error_phase=wal_drift=0; wal_result=UINT64_C(0x70000A8);
	wal_recovery=wal_watch_target=wal_bad_order=false; wal_creator=NULL;
	recovery_sync_error=recovery_payload_drift=false;
	recovery_crc_drift=NULL;
	wal_rmid=wal_info=0; memset(wal_bytes,0,sizeof(wal_bytes));
	snprintf(path, sizeof(path), "%s/base", selection_path);
	if (mkdir(path,0700)) abort();
	snprintf(path,sizeof(path), "%s/base/1001", selection_path);
	if (mkdir(path,0700)) abort();
	d = open(path,O_RDONLY|O_DIRECTORY);
	image.identity = (ClusterRelmapV2Namespace){11,12,1001,false};
	image.committed_generation = 9;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0] = (RelMapping){1259,40001};
	INIT_CRC32C(image.committed.crc);
	COMP_CRC32C(image.committed.crc,&image.committed,offsetof(RelMapFile,crc));
	FIN_CRC32C(image.committed.crc);
	if (d<0 || !cluster_relmap_file_encode_v2(&completed[1],&image,bytes,sizeof(bytes))) abort();
	for (int i=0;i<2;i++) {
		int fd=openat(d,i?"pgrac_relmap_authority.bak":"pgrac_relmap_authority",O_WRONLY|O_CREAT|O_EXCL,0600);
		if (fd<0 || write(fd,bytes,sizeof(bytes))!=sizeof(bytes) || close(fd)) abort();
	}
	if (close(d)) abort();
	snprintf(path,sizeof(path), "%s/base/2000",selection_path);
	if (mkdir(path,0700)) abort();
}
static int target_dir(void)
{
	char path[MAXPGPATH];
	snprintf(path,sizeof(path),"%s/base/2000",selection_path);
	return open(path,O_RDONLY|O_DIRECTORY);
}
static void copy_end(ClusterControlNativeRelmapCreateV2 **c)
{
	char path[MAXPGPATH];
	cluster_control_native_relmap_create_destroy_v2(c);
	for (unsigned i=0;i<2;i++) {
		int d;
		snprintf(path,sizeof(path),"%s/base/%u",selection_path,i?2000:1001);
		d=open(path,O_RDONLY|O_DIRECTORY);
		if(d<0) abort();
		unlinkat(d,"pgrac_relmap_authority",0); unlinkat(d,"pgrac_relmap_authority.bak",0);
		if(close(d)||rmdir(path)) abort();
	}
	snprintf(path,sizeof(path),"%s/base",selection_path);
	if(rmdir(path)) abort();
	batch_end();
}
static bool reserve_copy(ClusterControlNativeRelmapCreateV2 **c)
{
	return cluster_control_native_files_reserve_relmap_create_locked_v2(2000,1663,c)==0;
}
static bool target_available(void)
{
	ClusterControlNativeFileBorrowV2 b={0};
	bool ok=cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_RELMAP_V2,2000,1663,0,&b)==0;
	if(ok && !cluster_control_native_file_release_v2(&b)) abort();
	return ok;
}
UT_TEST(copy_source_committed_map_into_independent_registered_pair)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapFilePairV2 pair;
	ClusterControlNativeTableInfoV2 info;
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	UT_ASSERT(cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(target_available());
	UT_ASSERT(cluster_control_native_files_info_v2(&info) && info.borrowed==0);
	d=target_dir();
	UT_ASSERT(cluster_relmap_file_pair_read_v2(d,&c->expected,&pair));
	UT_ASSERT(pair.view.committed_generation==1 && !pair.view.has_pending);
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber,40001);
	UT_ASSERT(!cluster_relmap_file_pair_read_v2(d,&completed[1],&pair));
	UT_ASSERT(close(d)==0 && root_renames==4 && copy_syncs==3);
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(copy_syncs==3 && !held_family_lock && !InterruptHoldoffCount);
	copy_end(&c);
}
UT_TEST(existing_target_never_overwritten_and_attempt_never_retried)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	char v=0;
	int d,fd;
	copy_begin(); UT_ASSERT(reserve_copy(&c)); d=target_dir();
	fd=openat(d,"pgrac_relmap_authority.bak",O_WRONLY|O_CREAT|O_EXCL,0600);
	UT_ASSERT(fd>=0 && write(fd,"X",1)==1 && close(fd)==0);
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0 && !target_available());
	fd=openat(d,"pgrac_relmap_authority.bak",O_RDONLY);
	UT_ASSERT(read(fd,&v,1)==1 && v=='X' && close(fd)==0);
	UT_ASSERT(unlinkat(d,"pgrac_relmap_authority.bak",0)==0);
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(copy_syncs==0 && close(d)==0 && root_renames==2);
	copy_end(&c);
}
UT_TEST(guards_and_copied_carrier_precede_native_effects)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL,copied;
	copy_begin(); UT_ASSERT(reserve_copy(&c)); copied=*c;
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(&copied,1001,1663));
	copy_x=false;
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	copy_x=true;
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(copy_syncs==0 && root_renames==2 && !target_available());
	copy_end(&c);
	for(unsigned f=0;f<5;f++) {
		copy_begin(); UT_ASSERT(reserve_copy(&c));
		if(f==0) cf_held=false;
		if(f==1) held_family_lock=RelationMappingLock;
		if(f==2) CritSectionCount=1;
		if(f==3) enableFsync=false;
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,f==4?2000:1001,1663));
		cf_held=true; held_family_lock=NULL; CritSectionCount=0; enableFsync=true;
		UT_ASSERT(!target_available() && root_renames==2 && copy_syncs==0);
		copy_end(&c);
	}
}
UT_TEST(sync_failure_preserves_files_reservation_and_original_borrows_retire)
{
	for(unsigned f=1;f<=3;f++) {
		ClusterControlNativeRelmapCreateV2 *c=NULL;
		ClusterControlNativeTableInfoV2 info;
		unsigned before;
		int d;
		copy_begin(); UT_ASSERT(reserve_copy(&c)); fail_sync=f;
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
		before=copy_syncs; fail_sync=0;
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
		UT_ASSERT(copy_syncs==before && !target_available() && root_renames==2);
		UT_ASSERT(cluster_control_native_files_info_v2(&info) && info.borrowed==0 && info.reservation_count==3);
		d=target_dir();
		UT_ASSERT(faccessat(d,"pgrac_relmap_authority",F_OK,0)==0 && close(d)==0);
		UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
		copy_end(&c);
	}
}
UT_TEST(late_cf_change_cannot_publish_durable_pair_or_retry)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapFilePairV2 pair;
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c)); drift_sync=3;
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(!target_available() && root_renames==2 && copy_syncs==3);
	d=target_dir(); UT_ASSERT(cluster_relmap_file_pair_read_v2(d,&c->expected,&pair));
	UT_ASSERT(close(d)==0 && !cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
	copy_end(&c);
}
static void copy_error_after_primary(void) { config_error=true; }
UT_TEST(error_during_complete_keeps_attempt_and_creation_duty)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	volatile bool caught=false;
	copy_begin(); UT_ASSERT(reserve_copy(&c)); rename_hook=copy_error_after_primary;
	PG_TRY(); { (void)cluster_relmap_control_copy_create_v2(c,1001,1663); }
	PG_CATCH(); { caught=true; } PG_END_TRY();
	config_error=false; rename_hook=NULL;
	printf("# complete ERROR caught=%u renames=%u available=%u\n", caught, root_renames, target_available());
	UT_ASSERT(caught && !target_available() && root_renames==4);
	UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663) && root_renames==4);
	copy_end(&c);
}
UT_TEST(missing_source_copy_never_creates_target)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	char path[MAXPGPATH];
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	snprintf(path,sizeof(path),"%s/base/1001/pgrac_relmap_authority.bak",selection_path);
	UT_ASSERT(unlink(path)==0);
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	d=target_dir(); UT_ASSERT(faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0 && close(d)==0);
	UT_ASSERT(root_renames==2 && !target_available());
	copy_end(&c);
}
UT_TEST(error_under_mapping_lock_and_late_close_retain_debt)
{
	for(unsigned f=0;f<2;f++) {
		ClusterControlNativeRelmapCreateV2 *c=NULL;
		ClusterControlNativeTableInfoV2 info;
		volatile bool caught=false;
		volatile bool ok=false;
		copy_begin(); UT_ASSERT(reserve_copy(&c));
		read_error = f==0;
		/* Source pair closes twice, then target first copy close. */
		fail_close = f==1 ? 3 : 0;
		PG_TRY(); { ok=cluster_relmap_control_copy_create_v2(c,1001,1663); }
		PG_CATCH(); { caught=true; } PG_END_TRY();
		read_error=release_error=false; fail_close=0;
		UT_ASSERT(!ok && caught==(f==0) && !target_available() && root_renames==2);
		UT_ASSERT(cluster_control_native_files_info_v2(&info) && info.borrowed==0 && info.reservation_count==3);
		UT_ASSERT(!held_family_lock && !InterruptHoldoffCount && cf_held && copy_x);
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
		copy_end(&c);
	}
}
UT_TEST(secondary_release_error_keeps_original_outer_cleanup_duty)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterControlNativeTableInfoV2 info;
	volatile bool caught=false;
	copy_begin(); UT_ASSERT(reserve_copy(&c)); read_error=release_error=true;
	PG_TRY(); { (void)cluster_relmap_control_copy_create_v2(c,1001,1663); }
	PG_CATCH(); { caught=true; } PG_END_TRY();
	read_error=release_error=false;
	UT_ASSERT(caught && held_family_lock==RelationMappingLock && InterruptHoldoffCount==0);
	UT_ASSERT(cluster_control_native_files_info_v2(&info) && info.borrowed==0);
	UT_ASSERT(cf_held && copy_x && root_renames==2 && !target_available());
	UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
	/* Original outer ERROR owner does LWLockReleaseAll, not a second retail
	 * release from this consumer. Restore exactly its per-lock holdoff. */
	HOLD_INTERRUPTS(); LWLockRelease(RelationMappingLock);
	UT_ASSERT(!held_family_lock && !InterruptHoldoffCount);
	copy_end(&c);
}
UT_TEST(replay_storage_preserves_original_reserved_creation_owner)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapFilePairV2 source, pair;
	ClusterControlNativeTableInfoV2 info;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	UT_ASSERT(copy_original_read(&completed[1], &source));
	UT_ASSERT(cluster_relmap_authority_managed_replay_create_v2(&c->expected,
		&source.view.committed, &pair));
	UT_ASSERT_EQ(pair.view.committed_generation, 1);
	UT_ASSERT(cluster_relmap_authority_managed_replay_create_v2(&c->expected,
		&source.view.committed, &pair));
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40001);
	LWLockRelease(RelationMappingLock);
	UT_ASSERT(root_renames == 2 && !target_available() && !c->private_attempted
		&& !c->private_copy_attempted && cf_held && copy_x && !InterruptHoldoffCount);
	UT_ASSERT(cluster_control_native_files_info_v2(&info) && info.reservation_count == 3);
	copy_end(&c);
}
UT_TEST(replay_storage_lock_refusal_and_late_failure_never_complete_control)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapFilePairV2 source = {0}, pair, saved;
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	memset(&pair, 0xa5, sizeof(pair)); saved = pair;
	UT_ASSERT(!cluster_relmap_authority_managed_replay_create_v2(&c->expected,
		&source.view.committed, &pair));
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	UT_ASSERT(copy_original_read(&completed[1], &source));
	copy_x = false;
	UT_ASSERT(!cluster_relmap_authority_managed_replay_create_v2(&c->expected,
		&source.view.committed, &pair));
	copy_x = true;
	d = target_dir();
	UT_ASSERT(d >= 0 && faccessat(d, "pgrac_relmap_authority", F_OK, 0) != 0 && close(d) == 0);
	copy_syncs = 0; fail_sync = 2;
	UT_ASSERT(!cluster_relmap_authority_managed_replay_create_v2(&c->expected,
		&source.view.committed, &pair));
	UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	LWLockRelease(RelationMappingLock);
	UT_ASSERT(root_renames == 2 && !target_available() && cf_held && copy_x
		&& !InterruptHoldoffCount && !c->private_attempted && !c->private_copy_attempted);
	copy_end(&c);
}
UT_TEST(creation_wal_is_flushed_before_either_target_and_matches_installed_pair)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapCreateReplayV1 replay;
	ClusterRelmapFilePairV2 pair;
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c)); wal_creator=c; wal_watch_target=true;
	UT_ASSERT(cluster_relmap_control_copy_create_v2(c,1001,1663));
	UT_ASSERT(wal_begins==1 && wal_registers==1 && wal_inserts==1 && wal_flushes==1 && !wal_bad_order);
	UT_ASSERT(wal_rmid==RM_RELMAP_ID && wal_info==CLUSTER_RELMAP_CREATE_WAL_INFO_V1);
	UT_ASSERT(cluster_relmap_create_rebuild_v1(wal_bytes,sizeof(wal_bytes),&c->expected,c->batch_uuid,wal_result,&replay));
	d=target_dir(); UT_ASSERT(d>=0 && cluster_relmap_file_pair_read_v2(d,&c->expected,&pair));
	UT_ASSERT(!memcmp(&replay.record.committed,&pair.view.committed,sizeof(RelMapFile)));
	UT_ASSERT(pair.view.committed_generation==1 && !pair.view.has_pending && target_available());
	UT_ASSERT(close(d)==0); copy_end(&c);
}

UT_TEST(invalid_native_insert_end_never_creates_or_retries_target)
{
	for(unsigned i=0;i<2;i++) {
		ClusterControlNativeRelmapCreateV2 *c=NULL;
		ClusterControlNativeTableInfoV2 info;
		int d;
		copy_begin(); UT_ASSERT(reserve_copy(&c)); wal_result=i?UINT64_MAX:0;
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
		UT_ASSERT(wal_inserts==1 && !wal_flushes && !copy_syncs && !target_available());
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663) && wal_inserts==1);
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0 && close(d)==0);
		UT_ASSERT(cluster_control_native_files_info_v2(&info) && !info.borrowed && info.reservation_count==3);
		copy_end(&c);
	}
}

UT_TEST(wal_error_at_each_native_stage_retains_original_creation_debt)
{
	for(unsigned phase=1;phase<=3;phase++) {
		ClusterControlNativeRelmapCreateV2 *c=NULL;
		ClusterControlNativeTableInfoV2 info;
		volatile bool caught=false;
		int d;
		copy_begin(); UT_ASSERT(reserve_copy(&c)); wal_error_phase=phase;
		PG_TRY(); { (void)cluster_relmap_control_copy_create_v2(c,1001,1663); }
		PG_CATCH(); { caught=true; } PG_END_TRY();
		UT_ASSERT(caught && c->private_copy_attempted && !c->private_attempted);
		UT_ASSERT(!copy_syncs && !target_available() && root_renames==2);
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663) && wal_begins==1);
		UT_ASSERT(!held_family_lock && !InterruptHoldoffCount && cf_held && copy_x);
		UT_ASSERT(cluster_control_native_files_info_v2(&info) && !info.borrowed && info.reservation_count==3);
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0 && close(d)==0);
		copy_end(&c);
	}
}

UT_TEST(flushed_record_late_owner_identity_or_batch_change_cannot_create)
{
	for(unsigned drift=1;drift<=3;drift++) {
		ClusterControlNativeRelmapCreateV2 *c=NULL;
		ClusterControlNativeTableInfoV2 info;
		int d;
		copy_begin(); UT_ASSERT(reserve_copy(&c)); wal_creator=c; wal_drift=drift;
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
		UT_ASSERT(wal_inserts==1 && wal_flushes==1 && !copy_syncs && !target_available());
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663) && wal_inserts==1);
		UT_ASSERT(cluster_control_native_files_info_v2(&info) && !info.borrowed && info.reservation_count==3);
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0 && close(d)==0);
		UT_ASSERT(!held_family_lock && !InterruptHoldoffCount); copy_end(&c);
	}
}

UT_TEST(recovery_and_preexisting_authority_refusals_enter_no_creation_wal)
{
	for(unsigned reason=0;reason<3;reason++) {
		ClusterControlNativeRelmapCreateV2 *c=NULL;
		int d;
		copy_begin(); UT_ASSERT(reserve_copy(&c));
		if(reason==0) wal_recovery=true;
		if(reason==1) copy_x=false;
		if(reason==2) cf_held=false;
		UT_ASSERT(!cluster_relmap_control_copy_create_v2(c,1001,1663));
		UT_ASSERT(!wal_begins && !wal_inserts && !wal_flushes && !copy_syncs && !target_available());
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0 && close(d)==0);
		wal_recovery=false; copy_x=true; cf_held=true; copy_end(&c);
	}
}

static void creation_native_record(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	memset(reader,0,sizeof(*reader)); memset(decoded,0,sizeof(*decoded));
	reader->ReadRecPtr=UINT64_C(0x7000000); reader->EndRecPtr=wal_result;
	reader->record=decoded;
	reader->system_identifier=11; reader->cluster_expected_thread_id=1; reader->seg.ws_tli=1;
	decoded->size=sizeof(*decoded); decoded->lsn=reader->ReadRecPtr; decoded->next_lsn=reader->EndRecPtr;
	decoded->header.xl_rmid=RM_RELMAP_ID; decoded->header.xl_info=CLUSTER_RELMAP_CREATE_WAL_INFO_V1;
	decoded->header.xl_crc=0x98765432;
	decoded->main_data=(char *)wal_bytes; decoded->main_data_len=sizeof(wal_bytes); decoded->max_block_id=-1;
}
UT_TEST(native_creation_input_uses_original_end_and_independent_reserved_batch)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapCreateReplayV1 replay;
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	ClusterRelmapFilePairV2 source;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	LWLockAcquire(RelationMappingLock,LW_EXCLUSIVE);
	UT_ASSERT(copy_original_read(&completed[1],&source));
	LWLockRelease(RelationMappingLock);
	UT_ASSERT(cluster_relmap_create_encode_v1(&c->expected,c->batch_uuid,&source.view.committed,wal_bytes,sizeof(wal_bytes)));
	creation_native_record(&reader,&decoded);
	UT_ASSERT(cluster_relmap_control_create_redo_input_v1(&reader,&c->expected,c->batch_uuid,&replay));
	UT_ASSERT(replay.actual_end==reader.EndRecPtr && !memcmp(&replay.record.committed,&source.view.committed,sizeof(RelMapFile)));
	UT_ASSERT(!target_available() && !c->private_attempted && !c->private_copy_attempted && !copy_syncs);
	copy_end(&c);
}
UT_TEST(native_creation_input_rejects_wrong_envelope_and_output_alias)
{
	ClusterControlNativeRelmapCreateV2 *c=NULL;
	ClusterRelmapCreateReplayV1 replay,saved;
	ClusterRelmapFilePairV2 source;
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	LWLockAcquire(RelationMappingLock,LW_EXCLUSIVE); UT_ASSERT(copy_original_read(&completed[1],&source));
	LWLockRelease(RelationMappingLock);
	UT_ASSERT(cluster_relmap_create_encode_v1(&c->expected,c->batch_uuid,&source.view.committed,wal_bytes,sizeof(wal_bytes)));
	memset(&replay,0xa5,sizeof(replay)); saved=replay;
	for(unsigned f=0;f<9;f++) {
		creation_native_record(&reader,&decoded);
		if(f==0) decoded.header.xl_rmid=RM_XACT_ID;
		if(f==1) decoded.header.xl_info=0;
		if(f==2) decoded.max_block_id=0;
		if(f==3) decoded.has_page_version_edge=true;
		if(f==4) --decoded.main_data_len;
		if(f==5) decoded.main_data=NULL;
		if(f==6) ++reader.EndRecPtr;
		if(f==7) ++reader.ReadRecPtr;
		if(f==8) reader.record=NULL;
		UT_ASSERT(!cluster_relmap_control_create_redo_input_v1(&reader,&c->expected,c->batch_uuid,&replay));
		UT_ASSERT(!memcmp(&replay,&saved,sizeof(saved)));
	}
	creation_native_record(&reader,&decoded);
	UT_ASSERT(!cluster_relmap_control_create_redo_input_v1(&reader,&c->expected,c->batch_uuid,(ClusterRelmapCreateReplayV1 *)&reader));
	UT_ASSERT(!cluster_relmap_control_create_redo_input_v1(&reader,&c->expected,c->batch_uuid,(ClusterRelmapCreateReplayV1 *)&decoded));
	UT_ASSERT(!cluster_relmap_control_create_redo_input_v1(&reader,&c->expected,c->batch_uuid,(ClusterRelmapCreateReplayV1 *)wal_bytes));
	UT_ASSERT(!memcmp(&replay,&saved,sizeof(saved)) && !target_available() && !copy_syncs);
	copy_end(&c);
}

#include "test_cluster_relmap_create_replay.inc"
#include "test_cluster_relmap_recovery_create.inc"
#include "test_cluster_relmap_create_receipt.inc"
#include "test_cluster_relmap_create_owner.inc"

int main(int argc,char **argv)
{
	(void)argc; (void)argv;
	UT_RUN(copy_source_committed_map_into_independent_registered_pair);
	UT_RUN(existing_target_never_overwritten_and_attempt_never_retried);
	UT_RUN(guards_and_copied_carrier_precede_native_effects);
	UT_RUN(sync_failure_preserves_files_reservation_and_original_borrows_retire);
	UT_RUN(late_cf_change_cannot_publish_durable_pair_or_retry);
	UT_RUN(error_during_complete_keeps_attempt_and_creation_duty);
	UT_RUN(missing_source_copy_never_creates_target);
	UT_RUN(error_under_mapping_lock_and_late_close_retain_debt);
	UT_RUN(secondary_release_error_keeps_original_outer_cleanup_duty);
	UT_RUN(replay_storage_preserves_original_reserved_creation_owner);
	UT_RUN(replay_storage_lock_refusal_and_late_failure_never_complete_control);
	UT_RUN(native_creation_input_uses_original_end_and_independent_reserved_batch);
	UT_RUN(native_creation_input_rejects_wrong_envelope_and_output_alias);
	UT_RUN(creation_wal_is_flushed_before_either_target_and_matches_installed_pair);
	UT_RUN(invalid_native_insert_end_never_creates_or_retries_target);
	UT_RUN(wal_error_at_each_native_stage_retains_original_creation_debt);
	UT_RUN(flushed_record_late_owner_identity_or_batch_change_cannot_create);
	UT_RUN(recovery_and_preexisting_authority_refusals_enter_no_creation_wal);
	UT_RUN(creation_record_replays_missing_pair_before_original_control_confirmation);
	UT_RUN(creation_record_exact_pair_replay_preserves_both_inodes_before_confirmation);
	UT_RUN(creation_record_wrong_binding_or_end_never_enters_file_replay);
	UT_RUN(creation_replay_partial_sync_and_late_control_failure_keep_original_debt);
	UT_RUN(recovery_creation_installs_registered_pair_and_replays_available_without_replacement);
	UT_RUN(recovery_creation_accepts_exact_unconfirmed_pair_without_overwrite);
	UT_RUN(recovery_creation_wrong_binding_or_record_has_no_file_effect);
	UT_RUN(recovery_creation_partial_pair_and_sync_failure_keep_debt_and_refuse_retry);
	UT_RUN(recovery_creation_late_cf_or_payload_change_cannot_publish);
	UT_RUN(recovery_creation_error_cleans_only_local_mapping_and_poisoned_authorization);
	UT_RUN(recovery_creation_confirmation_error_preserves_durable_pair_and_debt);
	UT_RUN(recovery_creation_original_lock_and_fsync_guards_precede_effects);
	UT_RUN(recovery_creation_requires_the_original_sealed_native_frame);
	UT_RUN(creation_receipt_keeps_real_pair_and_complete_original_sources);
	UT_RUN(creation_receipt_small_budget_refuses_before_target_effects);
	UT_RUN(creation_receipt_nonempty_and_input_aliases_never_execute);
	UT_RUN(creation_receipt_getters_reject_aliases_without_changing_saved_inputs);
	UT_RUN(creation_receipt_false_and_error_never_publish_or_enable_retry);
	UT_RUN(creation_receipt_saved_observation_does_not_claim_currentness);
	UT_RUN(creation_owner_first_step_keeps_receipt_until_original_plan_destroy);
	UT_RUN(creation_owner_duplicate_never_resyncs_or_confirms);
	UT_RUN(creation_owner_whole_batch_and_original_step_precede_mutation);
	UT_RUN(creation_owner_false_error_and_provider_reentry_lock_the_attempt);
	UT_RUN(creation_owner_late_refusal_keeps_the_confirmed_receipt_and_debt);
	UT_RUN(creation_owner_capacity_refuses_before_authority_or_native_io);
	UT_RUN(creation_owner_saved_storage_rejects_plan_output_aliases);
	UT_DONE(); return ut_failed_count!=0;
}
