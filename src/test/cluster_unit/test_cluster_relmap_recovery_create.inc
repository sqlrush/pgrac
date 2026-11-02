/*-------------------------------------------------------------------------
 * test_cluster_relmap_recovery_create.inc
 *    Original recovery authorization, real relmap pairs and control receipts.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_recovery_create.inc
 * NOTES
 *    Original Startup authority is controlled; ROOT/registry/file I/O is real.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_control_relmap_recovery.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"

BackendType MyBackendType;
static ClusterColdTypedV1 redo_typed;
static struct ClusterRecoveryFencePlan *const redo_fence = (void *)0x1234;
static ClusterWalSourceRef redo_source;
static unsigned redo_failed;
static bool redo_owner_error;
static ClusterColdRecordFrameV1 redo_frame;
static ClusterColdPlanV1 redo_plan;

bool create_owner_original_frame_count(const ClusterColdPlanV1 *, uint32 *);
bool create_owner_original_frame_at(const ClusterColdPlanV1 *, uint32, ClusterColdRecordFrameV1 *);

bool cluster_cold_plan_input_frame_count_v1(const ClusterColdPlanV1 *plan, uint32 *count)
{
	if (plan!=&redo_plan) return create_owner_original_frame_count(plan,count);
	*count=1; return true;
}
bool cluster_cold_plan_input_frame_v1(const ClusterColdPlanV1 *plan, uint32 index,
	ClusterColdRecordFrameV1 *out)
{
	if (plan!=&redo_plan) return create_owner_original_frame_at(plan,index,out);
	if (index) return false;
	*out=redo_frame; return true;
}

bool
cluster_cold_typed_relmap_create_record_held_v2(ClusterColdTypedV1 *t,
	struct ClusterRecoveryFencePlan *f, uint32 p, const ClusterWalSourceRef *s,
	XLogRecPtr read, XLogRecPtr end)
{
	return t==&redo_typed && f==redo_fence && p==0 && wal_recovery && !redo_failed
		&& read==UINT64_C(0x7000000) && end==UINT64_C(0x70000A8)
		&& s && !memcmp(s,&redo_source,sizeof(*s));
}
bool
cluster_cold_typed_relmap_create_record_current_v2(ClusterColdTypedV1 *t,
	struct ClusterRecoveryFencePlan *f, uint32 p, const ClusterWalSourceRef *s,
	XLogRecPtr read, XLogRecPtr end)
{
	/* Production current brackets ROOT/profile I/O and must stay outside MappingLock. */
	if (held_family_lock) abort();
	if (redo_owner_error) { InterruptHoldoffCount=0; pg_re_throw(); }
	return cluster_cold_typed_relmap_create_record_held_v2(t,f,p,s,read,end);
}
void
cluster_cold_typed_relmap_create_failed_v2(ClusterColdTypedV1 *t)
{
	if (t!=&redo_typed) abort();
	++redo_failed;
}
static void recovery_copy_enter(void)
{
	memset(&redo_typed,0,sizeof(redo_typed));
	memset(&redo_plan,0,sizeof(redo_plan)); redo_plan.magic=CLUSTER_COLD_PLAN_MAGIC;
	redo_typed.plan=&redo_plan;
	memset(&redo_frame,0,sizeof(redo_frame));
	redo_frame.read_rec_ptr=UINT64_C(0x7000000); redo_frame.end_rec_ptr=UINT64_C(0x70000A8);
	redo_frame.record_crc=0x98765432; redo_frame.rmid=RM_RELMAP_ID;
	redo_frame.info=CLUSTER_RELMAP_CREATE_WAL_INFO_V1;
	redo_source=sources[0].source;
	redo_failed=0; redo_owner_error=false;
	wal_recovery=true; MyBackendType=B_STARTUP;
	owner_ok=false; /* Runtime creator must not authorize this episode. */
}
static void recovery_copy_leave(void)
{
	wal_recovery=false; MyBackendType=B_INVALID; owner_ok=true;
}
static bool recovery_copy_apply(ClusterControlNativeRelmapCreateV2 *old, XLogReaderState *reader)
{
	return cluster_relmap_control_recovery_create_v2(&redo_typed,redo_fence,0,&redo_source,
		reader,&old->expected,old->batch_uuid);
}
UT_TEST(recovery_creation_installs_registered_pair_and_replays_available_without_replacement)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	ClusterRelmapFilePairV2 pair;
	XLogReaderState reader; DecodedXLogRecord decoded;
	struct stat before[2],after;
	const char *names[2]={"pgrac_relmap_authority","pgrac_relmap_authority.bak"};
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded));
	recovery_copy_enter();
	UT_ASSERT(recovery_copy_apply(old,&reader));
	UT_ASSERT(target_available() && root_renames==4 && !redo_failed);
	UT_ASSERT(!old->private_attempted && !old->private_copy_attempted);
	d=target_dir(); UT_ASSERT(d>=0 && cluster_relmap_file_pair_read_v2(d,&old->expected,&pair));
	UT_ASSERT(!memcmp(pair.bytes[0],wal_bytes+64,sizeof(pair.bytes[0])));
	UT_ASSERT(!memcmp(pair.bytes[1],wal_bytes+64,sizeof(pair.bytes[1])));
	for(unsigned i=0;i<2;i++) UT_ASSERT(fstatat(d,names[i],&before[i],0)==0);
	UT_ASSERT(recovery_copy_apply(old,&reader));
	for(unsigned i=0;i<2;i++) {
		UT_ASSERT(fstatat(d,names[i],&after,0)==0);
		UT_ASSERT(after.st_dev==before[i].st_dev && after.st_ino==before[i].st_ino);
	}
	UT_ASSERT(root_renames==4 && !redo_failed && !wal_inserts && !wal_flushes);
	UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount && close(d)==0);
	recovery_copy_leave(); copy_end(&old);
}
UT_TEST(recovery_creation_accepts_exact_unconfirmed_pair_without_overwrite)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	ClusterRelmapCreateReplayV1 replay; ClusterRelmapFilePairV2 pair;
	XLogReaderState reader; DecodedXLogRecord decoded;
	struct stat before,after; int d;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded));
	UT_ASSERT(cluster_relmap_control_create_redo_input_v1(&reader,&old->expected,old->batch_uuid,&replay));
	LWLockAcquire(RelationMappingLock,LW_EXCLUSIVE);
	UT_ASSERT(cluster_relmap_authority_managed_replay_create_v2(&old->expected,&replay.record.committed,&pair));
	LWLockRelease(RelationMappingLock);
	d=target_dir(); UT_ASSERT(d>=0 && fstatat(d,"pgrac_relmap_authority",&before,0)==0);
	recovery_copy_enter(); UT_ASSERT(recovery_copy_apply(old,&reader));
	UT_ASSERT(fstatat(d,"pgrac_relmap_authority",&after,0)==0 && after.st_ino==before.st_ino);
	UT_ASSERT(target_available() && !redo_failed && root_renames==4 && close(d)==0);
	recovery_copy_leave(); copy_end(&old);
}
UT_TEST(recovery_creation_wrong_binding_or_record_has_no_file_effect)
{
	for(unsigned which=0;which<5;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded; int d;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		if(which==0) old->batch_uuid[0]^=1;
		if(which==1) old->expected.creation_generation++;
		if(which==2) redo_source.claim.identity.origin_owner_incarnation++;
		if(which==3) reader.EndRecPtr++;
		if(which==4) decoded.header.xl_rmid=RM_XACT_ID;
		UT_ASSERT(!recovery_copy_apply(old,&reader));
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0);
		UT_ASSERT(!copy_syncs && root_renames==2 && !target_available() && close(d)==0);
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(recovery_creation_partial_pair_and_sync_failure_keep_debt_and_refuse_retry)
{
	for(unsigned which=0;which<3;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded;
		unsigned syncs; int d;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); d=target_dir(); UT_ASSERT(d>=0);
		if(which==0) { int fd=openat(d,"pgrac_relmap_authority.bak",O_CREAT|O_EXCL|O_WRONLY,0600);
			UT_ASSERT(fd>=0 && write(fd,"X",1)==1 && close(fd)==0); }
		if(which==1) fail_sync=2;
		if(which==2) fail_close=copy_closes+1;
		recovery_copy_enter(); UT_ASSERT(!recovery_copy_apply(old,&reader));
		UT_ASSERT(redo_failed && !target_available() && root_renames==2);
		syncs=copy_syncs; fail_sync=fail_close=0;
		UT_ASSERT(!recovery_copy_apply(old,&reader) && copy_syncs==syncs);
		UT_ASSERT(faccessat(d,"pgrac_relmap_authority.bak",F_OK,0)==0 && close(d)==0);
		UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(recovery_creation_late_cf_or_payload_change_cannot_publish)
{
	for(unsigned which=0;which<3;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded; int d;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		if(which==0) drift_sync=3;
		if(which==1) recovery_payload_drift=true;
		if(which==2) recovery_crc_drift=&reader;
		UT_ASSERT(!recovery_copy_apply(old,&reader));
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)==0);
		UT_ASSERT(redo_failed && root_renames==2 && !target_available() && close(d)==0);
		UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(recovery_creation_error_cleans_only_local_mapping_and_poisoned_authorization)
{
	for(unsigned which=0;which<4;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded; volatile bool caught=false;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		if(which==0) recovery_sync_error=true;
		if(which==1) release_error=true;
		if(which==2) redo_owner_error=true;
		if(which==3) release_error_after=true;
		PG_TRY(); { (void)recovery_copy_apply(old,&reader); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY();
		recovery_sync_error=redo_owner_error=false;
		UT_ASSERT(caught && redo_failed && root_renames==2 && !target_available());
		UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(recovery_creation_confirmation_error_preserves_durable_pair_and_debt)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded; volatile bool caught=false; int d;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
	rename_hook=copy_error_after_primary;
	PG_TRY(); { (void)recovery_copy_apply(old,&reader); }
	PG_CATCH(); { caught=true; }
	PG_END_TRY();
	config_error=false; rename_hook=NULL;
	d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)==0);
	UT_ASSERT(caught && redo_failed && !held_family_lock && !InterruptHoldoffCount);
	UT_ASSERT(cf_held && copy_x && !wal_inserts && close(d)==0);
	recovery_copy_leave(); copy_end(&old);
}
UT_TEST(recovery_creation_original_lock_and_fsync_guards_precede_effects)
{
	for(unsigned which=0;which<5;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		if(which==0) copy_x=false;
		if(which==1) enableFsync=false;
		if(which==2) batch_exclusive=false;
		if(which==3) MyBackendType=B_BACKEND;
		if(which==4) held_family_lock=XidGenLock;
		UT_ASSERT(!recovery_copy_apply(old,&reader));
		UT_ASSERT(!copy_syncs && root_renames==2);
		copy_x=enableFsync=batch_exclusive=true; held_family_lock=NULL;
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(recovery_creation_requires_the_original_sealed_native_frame)
{
	for(unsigned which=0;which<8;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		ClusterRelmapCreateWalV1 input;
		XLogReaderState reader; DecodedXLogRecord decoded;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		if(which==0) reader.cluster_record_crc_failed=true;
		if(which==1) {
			UT_ASSERT(cluster_relmap_create_decode_v1(wal_bytes,sizeof(wal_bytes),&input));
			input.committed.mappings[0].mapfilenumber++;
			INIT_CRC32C(input.committed.crc);
			COMP_CRC32C(input.committed.crc,&input.committed,offsetof(RelMapFile,crc));
			FIN_CRC32C(input.committed.crc);
			UT_ASSERT(cluster_relmap_create_encode_v1(&input.identity,input.batch_uuid,
				&input.committed,wal_bytes,sizeof(wal_bytes)));
			decoded.header.xl_crc++;
		}
		if(which==2) decoded.header.xl_xid++;
		if(which==3) decoded.toplevel_xid++;
		if(which==4) reader.system_identifier++;
		if(which==5) reader.cluster_expected_thread_id++;
		if(which==6) reader.seg.ws_tli++;
		if(which==7) redo_frame.record_crc++;
		UT_ASSERT(!recovery_copy_apply(old,&reader));
		UT_ASSERT(redo_failed && !copy_syncs && root_renames==2 && !target_available());
		recovery_copy_leave(); copy_end(&old);
	}
}
