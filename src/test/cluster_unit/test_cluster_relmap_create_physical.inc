/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_physical.inc
 *    Read-only original creation receipts through real files and ROOT.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_physical.inc
 * NOTES
 *    Original source and lock leaves controlled; planner/registry/files real.
 *-------------------------------------------------------------------------
 */
static ClusterRelmapRecoveryCreateReceiptV2 *physical_setup(
	ClusterControlNativeRelmapCreateV2 **creator,XLogReaderState *reader,DecodedXLogRecord *decoded)
{
	if(!owner_setup(creator,reader,decoded) || !owner_apply(reader,65536)) return NULL;
	owner_source_calls=0;
	owner_physical_receipt=cold_relmap_create_at(redo_typed.plan,0)->receipt;
	return owner_physical_receipt;
}
static bool physical_current(ClusterRelmapRecoveryCreateReceiptV2 *receipt)
{
	return cluster_relmap_recovery_create_physical_recheck_v2(receipt,&redo_typed,redo_fence);
}
static void physical_target_fault(ClusterControlNativeRelmapCreateV2 *creator,unsigned fault)
{
	const char *name=fault==1 ? "pgrac_relmap_authority.bak" : "pgrac_relmap_authority";
	int d=target_dir(),fd; unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapV2Image image={0};
	if(d<0) abort();
	if(fault==0) { if(unlinkat(d,name,0)) abort(); }
	else {
		fd=openat(d,name,O_RDWR|O_NOFOLLOW); if(fd<0) abort();
		if(fault==1) { if(pread(fd,bytes,sizeof(bytes),0)!=sizeof(bytes)) abort(); bytes[170]^=1; }
		else {
			image.identity=(ClusterRelmapV2Namespace){creator->expected.system_identifier,
				creator->expected.database_incarnation,creator->expected.database_oid,false};
			image.committed_generation=fault==2 ? 2 : 1;
			image.committed=owner_declaration.creation.committed;
			if(fault==3) {
				image.committed.mappings[0].mapfilenumber++;
				INIT_CRC32C(image.committed.crc);
				COMP_CRC32C(image.committed.crc,&image.committed,offsetof(RelMapFile,crc));
				FIN_CRC32C(image.committed.crc);
			}
			if(!cluster_relmap_file_encode_v2(&creator->expected,&image,bytes,sizeof(bytes))) abort();
		}
		if(fault==4) { if(ftruncate(fd,64)) abort(); }
		else if(pwrite(fd,bytes,sizeof(bytes),0)!=sizeof(bytes)) abort();
		if(close(fd)) abort();
		/* Two valid, equal but different images must still fail the original
		 * receipt comparison, rather than only a pair/CRC consistency test. */
		if(fault==2 || fault==3) {
			fd=openat(d,"pgrac_relmap_authority.bak",O_RDWR|O_NOFOLLOW);
			if(fd<0 || pwrite(fd,bytes,sizeof(bytes),0)!=sizeof(bytes) || close(fd)) abort();
		}
	}
	if(close(d)) abort();
}
UT_TEST(creation_physical_recheck_reads_only_original_pair_and_sources)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
	Size before=redo_typed.plan->memory_used; unsigned syncs=copy_syncs,renames=root_renames;
	UT_ASSERT(receipt && physical_current(receipt) && physical_current(receipt));
	UT_ASSERT(copy_syncs==syncs && root_renames==renames && redo_typed.plan->memory_used==before);
	UT_ASSERT(!redo_failed && !redo_typed.plan->relmap_failed && cf_held && copy_x
		&& !held_family_lock && !InterruptHoldoffCount);
	owner_done(&creator);
}
UT_TEST(creation_physical_recheck_rejects_missing_changed_and_later_images)
{
	for(unsigned fault=0;fault<5;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
		unsigned syncs=copy_syncs,renames=root_renames;
		UT_ASSERT(receipt); physical_target_fault(creator,fault);
		UT_ASSERT(!physical_current(receipt) && redo_failed && redo_typed.plan->relmap_failed);
		UT_ASSERT(!physical_current(receipt) && copy_syncs==syncs && root_renames==renames
			&& cold_relmap_create_at(redo_typed.plan,0)->receipt==receipt);
		UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
		owner_done(&creator);
	}
}
UT_TEST(creation_physical_recheck_source_and_saved_coordinates_fail_sticky)
{
	for(unsigned fault=0;fault<5;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
		ClusterRecoveryCheckpointFileV2 file; unsigned syncs=copy_syncs,renames=root_renames;
		UT_ASSERT(receipt && cluster_relmap_recovery_create_file_at_v2(receipt,0,&file));
		if(fault==0) owner_source_ok=false;
		if(fault==1) ++redo_typed.sources[0].claim.identity.root_lineage_seq;
		if(fault==2) ++redo_typed.participants[0].tail_end;
		if(fault==3) owner_fail_call=2;
		if(fault==4) ((ClusterRecoveryCheckpointFileSourceV2 *)file.sources)[1].redo_start++;
		UT_ASSERT(!physical_current(receipt) && redo_failed && redo_typed.plan->relmap_failed);
		UT_ASSERT(copy_syncs==syncs && root_renames==renames && !held_family_lock && !InterruptHoldoffCount);
		owner_source_ok=true; owner_fail_call=0;
		UT_ASSERT(!physical_current(receipt)); owner_done(&creator);
	}
}
UT_TEST(creation_physical_recheck_errors_preserve_receipt_and_clean_local_lock)
{
	for(unsigned fault=0;fault<4;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
		volatile bool caught=false; Size bytes=redo_typed.plan->memory_used; unsigned syncs=copy_syncs;
		UT_ASSERT(receipt);
		if(fault==0) read_error=true;
		if(fault==1) release_error=true;
		if(fault==2) release_error_after=true;
		if(fault==3) owner_source_error=true;
		PG_TRY(); { (void)physical_current(receipt); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY();
		read_error=owner_source_error=false;
		UT_ASSERT(caught && redo_failed && redo_typed.plan->relmap_failed
			&& !held_family_lock && !InterruptHoldoffCount && copy_syncs==syncs
			&& redo_typed.plan->memory_used==bytes);
		UT_ASSERT(!physical_current(receipt) && cf_held && copy_x); owner_done(&creator);
	}
}
UT_TEST(creation_physical_recheck_reentry_cannot_adopt_outer_success)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
	UT_ASSERT(receipt); owner_physical_reentry=true;
	UT_ASSERT(!physical_current(receipt) && redo_failed && redo_typed.plan->relmap_failed);
	UT_ASSERT(!physical_current(receipt) && !held_family_lock && !InterruptHoldoffCount);
	owner_done(&creator);
}
UT_TEST(creation_physical_recheck_capacity_precedes_providers_and_file_reads)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
	unsigned closes=copy_closes; UT_ASSERT(receipt);
	redo_typed.plan->memory_budget=redo_typed.plan->memory_used+16;
	UT_ASSERT(!physical_current(receipt) && redo_failed && redo_typed.plan->relmap_failed
		&& !owner_source_calls && copy_closes==closes);
	owner_done(&creator);
}
UT_TEST(creation_physical_recheck_requires_original_owner_and_custody)
{
	for(unsigned fault=0;fault<6;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
		unsigned syncs=copy_syncs,closes=copy_closes;
		UT_ASSERT(receipt);
		if(fault==0) cf_held=false;
		if(fault==1) copy_x=false;
		if(fault==2) enableFsync=false;
		if(fault==3) MyBackendType=B_INVALID;
		if(fault==4) redo_typed.tx_fence=NULL;
		if(fault==5) wal_recovery=false;
		UT_ASSERT(!physical_current(receipt) && redo_failed && redo_typed.plan->relmap_failed
			&& copy_syncs==syncs && copy_closes==closes);
		cf_held=copy_x=enableFsync=true; MyBackendType=B_STARTUP; redo_typed.tx_fence=redo_fence; wal_recovery=true;
		UT_ASSERT(!physical_current(receipt)); owner_done(&creator);
	}
}

UT_TEST(creation_confirmed_ready_and_repeat_are_read_only)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
	Size bytes=redo_typed.plan->memory_used; unsigned syncs=copy_syncs,renames=root_renames;
	UT_ASSERT(receipt && cluster_relmap_recovery_create_recheck_v2(receipt,&redo_typed,redo_fence));
	UT_ASSERT(owner_apply(&reader,65536) && cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence));
	UT_ASSERT(copy_syncs==syncs && root_renames==renames && redo_typed.plan->memory_used==bytes
		&& !redo_failed && cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
	owner_done(&creator);
}
UT_TEST(creation_confirmed_readiness_requires_all_original_receipts)
{
	for(unsigned fault=0;fault<3;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
		ColdRelmapCreateInput *entry;
		UT_ASSERT(owner_setup(&creator,&reader,&decoded)); entry=cold_relmap_create_at(redo_typed.plan,0);
		UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence) && !redo_failed);
		UT_ASSERT(owner_apply(&reader,65536));
		if(fault==0) entry->applied=false;
		if(fault==1) entry->captured=false;
		if(fault==2) owner_side_ok=false;
		UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence));
		UT_ASSERT(entry->receipt && cf_held && copy_x); owner_done(&creator);
	}
}
UT_TEST(creation_confirmed_duplicate_rejects_changed_record_and_control)
{
	for(unsigned fault=0;fault<3;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
		unsigned syncs=copy_syncs,renames=root_renames; UT_ASSERT(receipt);
		if(fault==0) wal_bytes[64+128]^=1;
		if(fault==1) ++cf_sequence;
		if(fault==2) physical_target_fault(creator,3);
		UT_ASSERT(!owner_apply(&reader,65536) && redo_failed && redo_typed.plan->relmap_failed);
		UT_ASSERT(copy_syncs==syncs && root_renames==renames && !held_family_lock && !InterruptHoldoffCount);
		owner_done(&creator);
	}
}
UT_TEST(creation_confirmed_full_recheck_keeps_late_refusal_sticky)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=physical_setup(&creator,&reader,&decoded);
	unsigned syncs=copy_syncs,renames=root_renames; UT_ASSERT(receipt);
	owner_fail_call=3;
	UT_ASSERT(!cluster_relmap_recovery_create_recheck_v2(receipt,&redo_typed,redo_fence)
		&& redo_failed && redo_typed.plan->relmap_failed);
	owner_fail_call=0;
	UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence)
		&& copy_syncs==syncs && root_renames==renames && cf_held && copy_x);
	owner_done(&creator);
}
