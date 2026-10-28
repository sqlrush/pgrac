/* Same original physical DATA namespace, after known owner writes.
 * Real files/native ROOT reader; source/locks and postread faults controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_current_calls, census_current_fault;
static ClusterRecoveryCensus *census_current_owner;
static ClusterRecoveryRootsResult
census_current_read(ClusterRecoveryRootSet *roots, uint32 index, uint64 offset,
	void *buffer, size_t length)
{
	ClusterRecoveryRootsResult result = cluster_recovery_root_set_read_current_relation(
		roots, index, offset, buffer, length);
	census_current_calls++;
	if (census_current_fault == 1) inputs_pin_current = false;
	if (census_current_fault == 2) pg_re_throw();
	if (census_current_fault == 3) census_current_owner->pages->entries[0].page_sha256[31]++;
	return result;
}
static uint32
census_current_setup(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH])
{
	ClusterRecoveryCensus *cursor = page_census_fixture(inputs, fd, path, true);
	uint32 at = UINT32_MAX;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	for (uint32 i = 0; i < cursor->page_count; i++)
		if (cursor->pages->entries[i].forknum == MAIN_FORKNUM
			&& cursor->pages->entries[i].reservation.identity.key.locator.relNumber == 16386
			&& cursor->pages->entries[i].blockno == 0) at = i;
	UT_ASSERT(at != UINT32_MAX);
	census_current_owner = cursor; census_current_calls = census_current_fault = 0;
	return at;
}
static void
census_current_cleanup(ClusterWalInputsV1 **inputs, int fd, const char *path)
{
	census_current_fault = 0; inputs_pin_current = true;
	page_census_done(census_current_owner, inputs, fd, path); census_current_owner = NULL;
}
UT_TEST(test_census_current_pages_keep_initial_baseline_after_owner_write)
{
	ClusterWalInputsV1 *inputs; int fd, file; char path[MAXPGPATH];
	uint32 at = census_current_setup(&inputs, &fd, path);
	ClusterRecoveryCensus *cursor = census_current_owner;
	ClusterRecoveryCensusPageV1 original = cursor->pages->entries[at];
	ClusterRecoveryCensusManifest saved = cursor->manifest;
	ClusterRecoveryCensusPageDigestV1 read;
	uint8 byte = 0x37;
	for (uint32 i = 0; i < cursor->page_count; i++) {
		read = cluster_recovery_census_page_digest_v1(cursor, i);
		UT_ASSERT(read.observed);
		UT_ASSERT(memcmp(read.sha256, cursor->pages->entries[i].page_sha256, 32) == 0);
	}
	file = openat(fd, "base/5/16386", O_RDWR); UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pwrite(file, &byte, 1, BLCKSZ-1), 1); UT_ASSERT_EQ(close(file), 0);
	read = cluster_recovery_census_page_digest_v1(cursor, at);
	UT_ASSERT(read.observed); UT_ASSERT(memcmp(read.sha256, original.page_sha256, 32) != 0);
	/* Unchanged neighbor shares the legally changed file timestamps. */
	read = cluster_recovery_census_page_digest_v1(cursor, at + 1);
	UT_ASSERT(read.observed);
	UT_ASSERT(memcmp(read.sha256, cursor->pages->entries[at+1].page_sha256, 32) == 0);
	UT_ASSERT(memcmp(&original, &cursor->pages->entries[at], sizeof(original)) == 0);
	UT_ASSERT(memcmp(&saved, &cursor->manifest, sizeof(saved)) == 0);
	/* The old strict reader still rejects the original timestamp change. */
	{ PGAlignedBlock data;
		UT_ASSERT_EQ(cluster_recovery_root_set_read(cursor->roots, original.root_index,
			0, data.data, BLCKSZ), CLUSTER_RECOVERY_ROOTS_CHANGED); }
	read = cluster_recovery_census_page_digest_v1(cursor, at); UT_ASSERT(!read.observed);
	census_current_cleanup(&inputs, fd, path);
}
UT_TEST(test_census_current_pages_refuse_replaced_short_or_unsafe_objects)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs; int fd, file; char path[MAXPGPATH];
		uint32 at = census_current_setup(&inputs, &fd, path);
		ClusterRecoveryCensusPageDigestV1 read, zero = {0};
		if (mode == 0) {
			PGAlignedBlock blank = {0};
			UT_ASSERT_EQ(renameat(fd,"base/5/16386",fd,"base/5/old"),0);
			file=openat(fd,"base/5/16386",O_CREAT|O_WRONLY,0600); UT_ASSERT(file>=0);
			for (unsigned j=0;j<8;j++) UT_ASSERT_EQ(write(file,blank.data,BLCKSZ),BLCKSZ);
			UT_ASSERT_EQ(close(file),0);
		} else {
			file=openat(fd,"base/5/16386",O_RDWR); UT_ASSERT(file>=0);
			if (mode==1) UT_ASSERT_EQ(ftruncate(file,7*BLCKSZ),0);
			else UT_ASSERT_EQ(fchmod(file,0640),0);
			UT_ASSERT_EQ(close(file),0);
		}
		read=cluster_recovery_census_page_digest_v1(census_current_owner,at);
		UT_ASSERT(memcmp(&read,&zero,sizeof(read))==0);
		UT_ASSERT_EQ(census_current_owner->status,CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		if (mode==0) UT_ASSERT_EQ(unlinkat(fd,"base/5/old",0),0);
		census_current_cleanup(&inputs,fd,path);
	}
}
UT_TEST(test_census_current_pages_preconditions_do_not_enter_or_poison)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	uint32 at = census_current_setup(&inputs,&fd,path);
	for (unsigned mode=0; mode<4; mode++) {
		ClusterRecoveryCensusPageDigestV1 read;
		ClusterRecoveryCensus *cursor=census_current_owner;
		if (mode==0) inventory_graph_caller_lock=true;
		if (mode==1) CritSectionCount=1;
		if (mode==2) ShutdownRequestPending=true;
		if (mode==3) cursor->status=CLUSTER_RECOVERY_CENSUS_MORE;
		read=cluster_recovery_census_page_digest_v1(cursor,at);
		UT_ASSERT(!read.observed); UT_ASSERT_EQ(census_current_calls,0);
		inventory_graph_caller_lock=false; CritSectionCount=0; ShutdownRequestPending=false;
		UT_ASSERT_EQ(cursor->status,mode==3?CLUSTER_RECOVERY_CENSUS_MORE:CLUSTER_RECOVERY_CENSUS_DRAINED);
		cursor->status=CLUSTER_RECOVERY_CENSUS_DRAINED;
	}
	UT_ASSERT(cluster_recovery_census_page_digest_v1(census_current_owner,at).observed);
	census_current_cleanup(&inputs,fd,path);
}
UT_TEST(test_census_current_pages_final_source_error_and_index_drift_are_sticky)
{
	for (unsigned fault=1;fault<=3;fault++) {
		ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
		uint32 at=census_current_setup(&inputs,&fd,path);
		ClusterRecoveryCensusPageDigestV1 read={0}; volatile bool caught=false;
		census_current_fault=fault;
		PG_TRY(); { read=cluster_recovery_census_page_digest_v1(census_current_owner,at); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY();
		UT_ASSERT(caught==(fault==2)); UT_ASSERT(!read.observed);
		UT_ASSERT_EQ(census_current_owner->status,CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs_pin_held && inputs!=NULL);
		census_current_fault=0; inputs_pin_current=true;
		UT_ASSERT(!cluster_recovery_census_page_digest_v1(census_current_owner,at).observed);
		census_current_cleanup(&inputs,fd,path);
	}
}
