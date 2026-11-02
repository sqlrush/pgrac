/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_receipt.inc
 *    Retain the original successful creation receipts, without another replay.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_receipt.inc
 * NOTES
 *    Real original registry, paired files and hashes; episode leaves controlled.
 *-------------------------------------------------------------------------
 */
static bool
recovery_copy_capture(ClusterControlNativeRelmapCreateV2 *old, XLogReaderState *reader,
	Size budget, ClusterRelmapRecoveryCreateReceiptV2 **out)
{
	return cluster_relmap_control_recovery_create_capture_v2(&redo_typed,redo_fence,0,
		&redo_source,reader,&old->expected,old->batch_uuid,budget,out);
}

/* Missing digest/source ownership would return dangling scratch or the wrong
 * actual file receipts after the original executor has completed cleanup. */
UT_TEST(creation_receipt_keeps_real_pair_and_complete_original_sources)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL;
	ClusterRelmapRecoveryCreateObservationV2 observation;
	ClusterRecoveryCheckpointSourceV2 cut;
	ClusterRecoveryCheckpointFileV2 file;
	ClusterRelmapFilePairV2 pair;
	XLogReaderState reader; DecodedXLogRecord decoded;
	uint8 digest[32]; pg_sha256_ctx hash; unsigned syncs,renames; int d;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
	UT_ASSERT(recovery_copy_capture(old,&reader,65536,&receipt) && receipt);
	UT_ASSERT(target_available() && !redo_failed);
	UT_ASSERT(cluster_relmap_recovery_create_observation_v2(receipt,&observation));
	UT_ASSERT(observation.participant==0 && observation.source_count==2);
	UT_ASSERT(!memcmp(&observation.source,&redo_source,sizeof(redo_source)));
	UT_ASSERT(!memcmp(&observation.frame,&redo_frame,sizeof(redo_frame)));
	UT_ASSERT(observation.creation.actual_end==UINT64_C(0x70000A8));
	UT_ASSERT(!memcmp(&observation.creation.record.identity,&old->expected,sizeof(old->expected)));
	UT_ASSERT(!memcmp(observation.creation.record.batch_uuid,old->batch_uuid,16));
	d=target_dir(); UT_ASSERT(d>=0 && cluster_relmap_file_pair_read_v2(d,&old->expected,&pair));
	for(unsigned i=0;i<2;i++) {
		UT_ASSERT(cluster_relmap_recovery_create_source_at_v2(receipt,i,&cut));
		UT_ASSERT(!memcmp(&cut,&sources[i],sizeof(cut)));
		UT_ASSERT(cluster_relmap_recovery_create_file_at_v2(receipt,i,&file));
		UT_ASSERT(file.copy_index==i && file.copy_count==2 && file.synced_bytes==2304
			&& file.first_page==0 && file.page_count==1 && file.source_count==2);
		UT_ASSERT(!memcmp(&file.identity,&old->expected,sizeof(old->expected)));
		for(unsigned j=0;j<2;j++) {
			UT_ASSERT(file.sources[j].source_index==j);
			UT_ASSERT(file.sources[j].redo_start==sources[j].redo_start
				&& file.sources[j].replay_end==sources[j].replay_end);
		}
		/* Independent whole-file SHA input, not the executor's saved digest. */
		pg_sha256_init(&hash); pg_sha256_update(&hash,pair.bytes[i],sizeof(pair.bytes[i]));
		pg_sha256_final(&hash,digest);
		UT_ASSERT(!memcmp(digest,file.image_digest,32));
	}
	/* Saved complete sources own their bytes, rather than a live caller alias. */
	sources[1].source.claim.identity.root_lineage_seq++;
	UT_ASSERT(cluster_relmap_recovery_create_source_at_v2(receipt,1,&cut));
	UT_ASSERT(cut.source.claim.identity.root_lineage_seq+1==sources[1].source.claim.identity.root_lineage_seq);
	sources[1].source.claim.identity.root_lineage_seq--;
	syncs=copy_syncs; renames=root_renames;
	cluster_relmap_recovery_create_destroy_v2(&receipt);
	cluster_relmap_recovery_create_destroy_v2(&receipt);
	UT_ASSERT(!receipt && copy_syncs==syncs && root_renames==renames && !redo_failed);
	UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount && close(d)==0);
	recovery_copy_leave(); copy_end(&old);
}
UT_TEST(creation_receipt_small_budget_refuses_before_target_effects)
{
	const Size budgets[]={1,1600};
	for(unsigned i=0;i<lengthof(budgets);i++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded; int d;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		UT_ASSERT(!recovery_copy_capture(old,&reader,budgets[i],&receipt));
		d=target_dir(); UT_ASSERT(d>=0 && faccessat(d,"pgrac_relmap_authority",F_OK,0)!=0);
		UT_ASSERT(!receipt && !copy_syncs && root_renames==2 && !target_available() && close(d)==0);
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(creation_receipt_nonempty_and_input_aliases_never_execute)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL,*saved;
	XLogReaderState reader; DecodedXLogRecord decoded;
	unsigned syncs,renames;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
	{
		void *aliases[]={&redo_typed,&redo_plan,&redo_source,&reader,&decoded,
			wal_bytes,&old->expected,old->batch_uuid};
		for(unsigned i=0;i<lengthof(aliases);i++) {
			UT_ASSERT(!recovery_copy_capture(old,&reader,65536,(ClusterRelmapRecoveryCreateReceiptV2 **)aliases[i]));
			UT_ASSERT(!copy_syncs && root_renames==2 && !redo_failed);
		}
	}
	UT_ASSERT(recovery_copy_capture(old,&reader,65536,&receipt)); saved=receipt;
	syncs=copy_syncs; renames=root_renames;
	UT_ASSERT(!recovery_copy_capture(old,&reader,65536,&receipt));
	UT_ASSERT(receipt==saved && copy_syncs==syncs && root_renames==renames && !redo_failed);
	cluster_relmap_recovery_create_destroy_v2(&receipt);
	recovery_copy_leave(); copy_end(&old);
}
UT_TEST(creation_receipt_getters_reject_aliases_without_changing_saved_inputs)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL;
	ClusterRelmapRecoveryCreateObservationV2 observation,saved;
	ClusterRecoveryCheckpointSourceV2 cut,cut_saved;
	ClusterRecoveryCheckpointFileV2 file,file_saved;
	XLogReaderState reader; DecodedXLogRecord decoded;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
	UT_ASSERT(recovery_copy_capture(old,&reader,65536,&receipt));
	UT_ASSERT(cluster_relmap_recovery_create_observation_v2(receipt,&saved));
	UT_ASSERT(cluster_relmap_recovery_create_file_at_v2(receipt,0,&file_saved));
	UT_ASSERT(cluster_relmap_recovery_create_source_at_v2(receipt,0,&cut_saved));
	observation=saved; file=file_saved; cut=cut_saved;
	UT_ASSERT(!cluster_relmap_recovery_create_observation_v2(NULL,&observation));
	UT_ASSERT(!cluster_relmap_recovery_create_file_at_v2(receipt,2,&file));
	UT_ASSERT(!cluster_relmap_recovery_create_source_at_v2(receipt,2,&cut));
	UT_ASSERT(!memcmp(&observation,&saved,sizeof(saved)) && !memcmp(&file,&file_saved,sizeof(file))
		&& !memcmp(&cut,&cut_saved,sizeof(cut)));
	UT_ASSERT(!cluster_relmap_recovery_create_observation_v2(receipt,(void *)file.sources));
	UT_ASSERT(!cluster_relmap_recovery_create_file_at_v2(receipt,0,(void *)file.sources));
	UT_ASSERT(!cluster_relmap_recovery_create_source_at_v2(receipt,0,(void *)receipt));
	UT_ASSERT(!cluster_relmap_recovery_create_observation_v2(receipt,(void *)&redo_plan));
	UT_ASSERT(cluster_relmap_recovery_create_observation_v2(receipt,&observation)
		&& !memcmp(&observation,&saved,sizeof(saved)));
	cluster_relmap_recovery_create_destroy_v2(&receipt);
	recovery_copy_leave(); copy_end(&old);
}
UT_TEST(creation_receipt_false_and_error_never_publish_or_enable_retry)
{
	for(unsigned which=0;which<5;which++) {
		ClusterControlNativeRelmapCreateV2 *old=NULL;
		ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded; volatile bool caught=false;
		unsigned syncs;
		copy_begin(); UT_ASSERT(reserve_copy(&old));
		UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
		if(which==0) fail_sync=2;
		if(which==1) drift_sync=3;
		if(which==2) recovery_sync_error=true;
		if(which==3) release_error_after=true;
		if(which==4) rename_hook=copy_error_after_primary;
		PG_TRY(); { UT_ASSERT(!recovery_copy_capture(old,&reader,65536,&receipt)); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY();
		UT_ASSERT(caught==(which>=2) && !receipt && redo_failed);
		recovery_sync_error=config_error=false; rename_hook=NULL; fail_sync=drift_sync=0;
		syncs=copy_syncs;
		UT_ASSERT(!recovery_copy_capture(old,&reader,65536,&receipt));
		UT_ASSERT(!receipt && copy_syncs==syncs && cf_held && copy_x
			&& !held_family_lock && !InterruptHoldoffCount);
		recovery_copy_leave(); copy_end(&old);
	}
}
UT_TEST(creation_receipt_saved_observation_does_not_claim_currentness)
{
	ClusterControlNativeRelmapCreateV2 *old=NULL;
	ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL;
	ClusterRelmapRecoveryCreateObservationV2 before,after;
	XLogReaderState reader; DecodedXLogRecord decoded; unsigned syncs,renames;
	copy_begin(); UT_ASSERT(reserve_copy(&old));
	UT_ASSERT(creation_replay_record(old,&reader,&decoded)); recovery_copy_enter();
	UT_ASSERT(recovery_copy_capture(old,&reader,65536,&receipt));
	UT_ASSERT(cluster_relmap_recovery_create_observation_v2(receipt,&before));
	syncs=copy_syncs; renames=root_renames; cf_held=false;
	UT_ASSERT(cluster_relmap_recovery_create_observation_v2(receipt,&after)
		&& !memcmp(&before,&after,sizeof(before)));
	UT_ASSERT(copy_syncs==syncs && root_renames==renames && !redo_failed);
	/* No current/physical/confirmed claim is made by saved getters or destroy. */
	cluster_relmap_recovery_create_destroy_v2(&receipt); cf_held=true;
	UT_ASSERT(!receipt && !redo_failed && copy_syncs==syncs && root_renames==renames);
	recovery_copy_leave(); copy_end(&old);
}
