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
#include "test_cluster_control_native_reservations.c"
#undef main
#include "cluster/cluster_relmap_control.h"
#include "cluster/cluster_relmap_authority.h"
#include "storage/fd.h"

extern bool cluster_relmap_control_copy_create_v2(ClusterControlNativeRelmapCreateV2 *, Oid, Oid);
static bool copy_x = true;
bool cluster_relmap_authority_x_held(void);
bool cluster_relmap_authority_x_held(void) { return copy_x; }
bool enableFsync = true;
volatile uint32 InterruptHoldoffCount;
static unsigned copy_syncs, fail_sync, drift_sync, copy_closes, fail_close;
static bool read_error, release_error;
static int copy_sync(int fd)
{
	++copy_syncs;
	if (copy_syncs == drift_sync) ++cf_sequence;
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
	read_error=release_error=false;
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
	UT_DONE(); return ut_failed_count!=0;
}
